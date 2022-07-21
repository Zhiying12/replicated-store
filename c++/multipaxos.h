#ifndef MULTI_PAXOS_H_
#define MULTI_PAXOS_H_

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <vector>

#include "json_fwd.h"
#include "kvstore.h"
#include "log.h"
#include "multipaxos.grpc.pb.h"

static const int64_t kIdBits = 0xff;
static const int64_t kRoundIncrement = kIdBits + 1;
static const int64_t kMaxNumPeers = 0xf;

using rpc_stub_t = std::unique_ptr<multipaxos::MultiPaxosRPC::Stub>;

struct rpc_peer_t {
  rpc_peer_t(int64_t id, rpc_stub_t stub) : id_(id), stub_(std::move(stub)) {}
  int64_t id_;
  rpc_stub_t stub_;
};

using rpc_server_t = std::unique_ptr<grpc::Server>;

class MultiPaxos : public multipaxos::MultiPaxosRPC::Service {
 public:
  MultiPaxos(Log* log, nlohmann::json const& config);
  MultiPaxos(MultiPaxos const& mp) = delete;
  MultiPaxos& operator=(MultiPaxos const& mp) = delete;
  MultiPaxos(MultiPaxos&& mp) = delete;
  MultiPaxos& operator=(MultiPaxos&& mp) = delete;

  int64_t NextBallot() {
    std::scoped_lock lock(mu_);
    auto old_ballot = ballot_;
    ballot_ += kRoundIncrement;
    ballot_ = (ballot_ & ~kIdBits) | id_;
    ready_ = false;
    DLOG(INFO) << id_ << " became a leader: ballot: " << old_ballot << " -> "
               << ballot_;
    cv_leader_.notify_one();
    return ballot_;
  }

  void SetBallot(int64_t ballot) {
    auto old_id = ballot_ & kIdBits;
    auto new_id = ballot & kIdBits;
    if ((old_id == id_ || old_id == kMaxNumPeers) && old_id != new_id) {
      DLOG(INFO) << id_ << " became a follower: ballot: " << ballot_ << " -> "
                 << ballot;
      cv_follower_.notify_one();
    }
    ballot_ = ballot;
  }

  int64_t Leader() const {
    std::scoped_lock lock(mu_);
    return ballot_ & kIdBits;
  }

  bool IsLeader() const {
    std::scoped_lock lock(mu_);
    return IsLeaderLockless();
  }

  bool IsLeaderLockless() const { return (ballot_ & kIdBits) == id_; }

  bool IsSomeoneElseLeader() const {
    std::scoped_lock lock(mu_);
    auto id = ballot_ & kIdBits;
    return id != id_ && id < kMaxNumPeers;
  }

  void WaitUntilLeader() {
    std::unique_lock lock(mu_);
    while (running_ && !IsLeaderLockless())
      cv_leader_.wait(lock);
  }

  void WaitUntilFollower() {
    std::unique_lock lock(mu_);
    while (running_ && IsLeaderLockless())
      cv_follower_.wait(lock);
  }

  void Start();
  void Stop();

  void HeartbeatThread();
  void PrepareThread();

  std::optional<int64_t> SendHeartbeats(int64_t global_last_executed);
  std::optional<std::vector<log_vector_t>> SendPrepares();

 private:
  grpc::Status Heartbeat(grpc::ServerContext*,
                         const multipaxos::HeartbeatRequest*,
                         multipaxos::HeartbeatResponse*) override;

  grpc::Status Prepare(grpc::ServerContext*,
                       const multipaxos::PrepareRequest*,
                       multipaxos::PrepareResponse*) override;

  grpc::Status Accept(grpc::ServerContext*,
                      const multipaxos::AcceptRequest*,
                      multipaxos::AcceptResponse*) override;

  std::atomic<bool> running_;
  bool ready_;
  int64_t ballot_;
  Log* log_;
  int64_t id_;
  long heartbeat_interval_;
  std::mt19937 engine_;
  std::uniform_int_distribution<int> dist_;
  std::string port_;
  std::atomic<long> last_heartbeat_;
  std::vector<rpc_peer_t> rpc_peers_;
  rpc_server_t rpc_server_;
  bool rpc_server_running_;
  std::condition_variable rpc_server_running_cv_;
  mutable std::mutex mu_;
  asio::thread_pool thread_pool_;

  std::condition_variable cv_leader_;
  std::condition_variable cv_follower_;

  std::thread heartbeat_thread_;
  std::thread prepare_thread_;
};

struct heartbeat_state_t {
  size_t num_rpcs_ = 0;
  std::vector<int64_t> responses_;
  std::mutex mu_;
  std::condition_variable cv_;
};

struct prepare_state_t {
  size_t num_rpcs_ = 0;
  std::vector<log_vector_t> responses_;
  std::mutex mu_;
  std::condition_variable cv_;
};

#endif
