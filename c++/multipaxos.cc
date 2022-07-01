#include "multipaxos.h"
#include "json.h"

#include <chrono>
#include <thread>

MultiPaxos::MultiPaxos(Log* log, json const& config)
    : running_(false),
      id_(config["id"]),
      port_(config["peers"][id_]),
      ballot_(kMaxNumPeers),
      heartbeat_pause_(config["heartbeat_pause"]),
      log_(log),
      tp_(config["threadpool_size"]) {
  for (std::string const peer : config["peers"])
    rpc_peers_.emplace_back(MultiPaxosRPC::NewStub(
        grpc::CreateChannel(peer, grpc::InsecureChannelCredentials())));

  ServerBuilder builder;
  builder.AddListeningPort(port_, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  rpc_server_ = builder.BuildAndStart();
}

void MultiPaxos::Start(void) {
  CHECK(!running_);
  running_ = true;
  heartbeat_thread_ = std::thread(&MultiPaxos::HeartbeatThread, this);
  CHECK(rpc_server_);
  DLOG(INFO) << id_ << " starting rpc server at " << port_;
  rpc_server_->Wait();
}

void MultiPaxos::Shutdown(void) {
  CHECK(running_);
  running_ = false;
  cv_leader_.notify_all();
  heartbeat_thread_.join();
  CHECK(rpc_server_);
  DLOG(INFO) << id_ << " stopping rpc server at " << port_;
  rpc_server_->Shutdown();
}

void MultiPaxos::HeartbeatThread() {
  DLOG(INFO) << id_ << " starting heartbeat thread";
  while (running_) {
    {
      std::unique_lock lock(mu_);
      while (running_ && !IsLeaderLockless())
        cv_leader_.wait(lock);
    }
    while (running_) {
      heartbeat_num_responses_ = 0;
      heartbeat_ok_responses_.clear();
      {
        std::scoped_lock lock(mu_);
        heartbeat_request_.set_ballot(ballot_);
        heartbeat_request_.set_last_executed(log_->LastExecuted());
        heartbeat_request_.set_global_last_executed(log_->GlobalLastExecuted());
      }
      for (auto& peer : rpc_peers_) {
        asio::post(tp_, [this, &peer] {
          ClientContext context;
          HeartbeatResponse response;
          Status status =
              peer->Heartbeat(&context, heartbeat_request_, &response);
          DLOG(INFO) << id_ << " sent heartbeat to " << context.peer();
          {
            std::scoped_lock lock(heartbeat_mu_);
            ++heartbeat_num_responses_;
            if (status.ok())
              heartbeat_ok_responses_.push_back(response.last_executed());
          }
          heartbeat_cv_.notify_one();
        });
      }
      {
        std::unique_lock lock(heartbeat_mu_);
        while (IsLeader() && heartbeat_num_responses_ != rpc_peers_.size())
          heartbeat_cv_.wait(lock);
      }
      {
        std::scoped_lock lock(heartbeat_mu_);
        if (heartbeat_ok_responses_.size() == rpc_peers_.size())
          log_->TrimUntil(*std::min_element(std::begin(heartbeat_ok_responses_),
                                            std::end(heartbeat_ok_responses_)));
      }
      std::this_thread::sleep_for(heartbeat_pause_);
      if (!IsLeader())
        break;
    }
  }
  DLOG(INFO) << id_ << " stopping heartbeat thread";
}

Status MultiPaxos::Heartbeat(ServerContext* context,
                             const HeartbeatRequest* request,
                             HeartbeatResponse* response) {
  DLOG(INFO) << id_ << " received heartbeat rpc from " << context->peer();
  std::scoped_lock lock(mu_);
  if (request->ballot() >= ballot_) {
    last_heartbeat_ = std::chrono::steady_clock::now();
    ballot_ = request->ballot();
    log_->CommitUntil(request->last_executed(), request->ballot());
    log_->TrimUntil(request->global_last_executed());
  }
  response->set_last_executed(log_->LastExecuted());
  return Status::OK;
}
