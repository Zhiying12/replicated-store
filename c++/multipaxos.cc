#include "multipaxos.h"
#include "json.h"

#include <chrono>
#include <thread>

using namespace std::chrono;

using nlohmann::json;

using grpc::ClientContext;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using multipaxos::Command;
using multipaxos::Instance;
using multipaxos::MultiPaxosRPC;

using multipaxos::InstanceState::INPROGRESS;

using multipaxos::ResponseType::OK;
using multipaxos::ResponseType::REJECT;

using multipaxos::CommitRequest;
using multipaxos::CommitResponse;

using multipaxos::PrepareRequest;
using multipaxos::PrepareResponse;

using multipaxos::AcceptRequest;
using multipaxos::AcceptResponse;

MultiPaxos::MultiPaxos(std::vector<Log*>& logs, json const& config)
    : ballot_(kMaxNumPeers),
      logs_(logs),
      id_(config["id"]),
      commit_received_(false),
      commit_interval_(config["commit_interval"]),
      engine_(id_),
      port_(config["peers"][id_]),
      num_peers_(config["peers"].size()),
      thread_pool_(config["threadpool_size"]),
      rpc_server_running_(false),
      prepare_thread_running_(false),
      commit_thread_running_(false),
      partition_size_(config["partition_size"]) {
  int64_t id = 0;
  for (std::string const peer : config["peers"])
    rpc_peers_.emplace_back(id++,
                            MultiPaxosRPC::NewStub(grpc::CreateChannel(
                                peer, grpc::InsecureChannelCredentials())));
}

void MultiPaxos::Start() {
  StartPrepareThread();
  StartCommitThread();
  StartRPCServer();
}

void MultiPaxos::Stop() {
  StopRPCServer();
  StopPrepareThread();
  StopCommitThread();
  thread_pool_.join();
}

void MultiPaxos::StartRPCServer() {
  DLOG(INFO) << id_ << " starting rpc server at " << port_;
  ServerBuilder builder;
  builder.AddListeningPort(port_, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  rpc_server_ = builder.BuildAndStart();
  {
    std::scoped_lock lock(mu_);
    rpc_server_running_ = true;
    rpc_server_running_cv_.notify_one();
  }
  rpc_server_thread_ = std::thread([this] { rpc_server_->Wait(); });
}

void MultiPaxos::StopRPCServer() {
  {
    std::unique_lock lock(mu_);
    while (!rpc_server_running_)
      rpc_server_running_cv_.wait(lock);
  }
  DLOG(INFO) << id_ << " stopping rpc server at " << port_;
  rpc_server_->Shutdown();
  rpc_server_thread_.join();
}

void MultiPaxos::StartPrepareThread() {
  DLOG(INFO) << id_ << " starting prepare thread";
  CHECK(!prepare_thread_running_);
  prepare_thread_running_ = true;
  prepare_thread_ = std::thread(&MultiPaxos::PrepareThread, this);
}

void MultiPaxos::StopPrepareThread() {
  DLOG(INFO) << id_ << " stopping prepare thread";
  CHECK(prepare_thread_running_);
  {
    std::unique_lock lock(mu_);
    prepare_thread_running_ = false;
  }
  cv_follower_.notify_one();
  prepare_thread_.join();
}

void MultiPaxos::StartCommitThread() {
  DLOG(INFO) << id_ << " starting commit thread";
  CHECK(!commit_thread_running_);
  commit_thread_running_ = true;
  commit_thread_ = std::thread(&MultiPaxos::CommitThread, this);
}

void MultiPaxos::StopCommitThread() {
  DLOG(INFO) << id_ << " stopping commit thread";
  CHECK(commit_thread_running_);
  {
    std::unique_lock lock(mu_);
    commit_thread_running_ = false;
  }
  cv_leader_.notify_one();
  commit_thread_.join();
}

Result MultiPaxos::Replicate(Command command, int64_t client_id) {
  auto ballot = Ballot();
  if (IsLeader(ballot, id_)) {
    int64_t p_index = hash_function(command.key()) % partition_size_;
    return RunAcceptPhase(ballot, logs_[p_index]->AdvanceLastIndex(), std::move(command),
                          client_id, p_index);
  }
  if (IsSomeoneElseLeader(ballot, id_))
    return Result{ResultType::kSomeoneElseLeader, ExtractLeaderId(ballot)};
  return Result{ResultType::kRetry, std::nullopt};
}

void MultiPaxos::PrepareThread() {
  while (prepare_thread_running_) {
    {
      std::unique_lock lock(mu_);
      while (prepare_thread_running_ && IsLeader(ballot_, id_))
        cv_follower_.wait(lock);
    }
    while (prepare_thread_running_) {
      SleepForRandomInterval();
      if (ReceivedCommit())
        continue;
      auto next_ballot = NextBallot();
      auto r = RunPreparePhase(next_ballot);
      if (r) {
        auto [max_last_index, log] = *r;
        BecomeLeader(next_ballot, max_last_index);
        Replay(next_ballot, log);
        break;
      }
    }
  }
}

void MultiPaxos::CommitThread() {
  std::vector<int64_t> gles(partition_size_, 0);
  while (commit_thread_running_) {
    {
      std::unique_lock lock(mu_);
      while (commit_thread_running_ && !IsLeader(ballot_, id_))
        cv_leader_.wait(lock);
    }
    for (auto i = 0; i < partition_size_; i++) {
      gles[i] = logs_[i]->GlobalLastExecuted();
    }
    while (commit_thread_running_) {
      auto ballot = Ballot();
      if (!IsLeader(ballot, id_))
        break;
      RunCommitPhase(ballot, gles);
      SleepForCommitInterval();
    }
  }
}

std::optional<
    std::pair<int64_t, std::unordered_map<int64_t, multipaxos::Instance>>>
MultiPaxos::RunPreparePhase(int64_t ballot) {
  auto state = std::make_shared<prepare_state_t>();

  PrepareRequest request;
  request.set_sender(id_);
  request.set_ballot(ballot);

  if (ballot > ballot_) {
    ++state->num_rpcs_;
    ++state->num_oks_;
    // TODO
    // state->log_ = log_->GetLog();
    // state->max_last_index_ = log_->LastIndex();
  } else {
    return std::nullopt;
  }

  for (auto& peer : rpc_peers_) {
    if (peer.id_ == id_) {
      continue;
    }
    asio::post(thread_pool_, [this, state, &peer, request] {
      ClientContext context;
      PrepareResponse response;
      Status s = peer.stub_->Prepare(&context, std::move(request), &response);
      DLOG(INFO) << id_ << " sent prepare request to " << peer.id_;
      {
        std::scoped_lock lock(state->mu_);
        ++state->num_rpcs_;
        if (s.ok()) {
          if (response.type() == OK) {
            ++state->num_oks_;
            for (int i = 0; i < response.instances_size(); ++i) {
              state->max_last_index_ = std::max(state->max_last_index_,
                                                response.instances(i).index());
              Insert(&state->log_, std::move(response.instances(i)));
            }
          } else {
            BecomeFollower(response.ballot());
          }
        }
      }
      state->cv_.notify_one();
    });
  }
  {
    std::unique_lock lock(state->mu_);
    while (state->num_oks_ <= num_peers_ / 2 && state->num_rpcs_ != num_peers_)
      state->cv_.wait(lock);
    if (state->num_oks_ > num_peers_ / 2)
      return std::make_pair(state->max_last_index_, std::move(state->log_));
  }
  return std::nullopt;
}

Result MultiPaxos::RunAcceptPhase(int64_t ballot,
                                  int64_t index,
                                  Command command,
                                  int64_t client_id,
                                  int64_t partition_index) {
  auto state = std::make_shared<accept_state_t>();

  Instance instance;
  instance.set_ballot(ballot);
  instance.set_index(index);
  instance.set_client_id(client_id);
  instance.set_state(INPROGRESS);
  *instance.mutable_command() = std::move(command);

  if (ballot == ballot_) {
    ++state->num_rpcs_;
    ++state->num_oks_;
    logs_[partition_index]->Append(instance);
  } else {
    auto leader = ExtractLeaderId(ballot_);
    return Result{ResultType::kSomeoneElseLeader, leader};
  }

  AcceptRequest request;
  request.set_sender(id_);
  *request.mutable_instance() = std::move(instance);
  request.set_partition_index(partition_index);

  for (auto& peer : rpc_peers_) {
    if (peer.id_ == id_) {
      continue;
    }
    asio::post(thread_pool_, [this, state, &peer, request] {
      ClientContext context;
      AcceptResponse response;
      Status s = peer.stub_->Accept(&context, std::move(request), &response);
      DLOG(INFO) << id_ << " sent accept request to " << peer.id_;
      {
        std::scoped_lock lock(state->mu_);
        ++state->num_rpcs_;
        if (s.ok()) {
          if (response.type() == OK)
            ++state->num_oks_;
          else
            BecomeFollower(response.ballot());
        }
      }
      state->cv_.notify_one();
    });
  }
  {
    std::unique_lock lock(state->mu_);
    while (IsLeader(ballot_, id_) && state->num_oks_ <= num_peers_ / 2 &&
           state->num_rpcs_ != num_peers_)
      state->cv_.wait(lock);
    if (state->num_oks_ > num_peers_ / 2) {
      logs_[partition_index]->Commit(index);
      return Result{ResultType::kOk, std::nullopt};
    }
    if (!IsLeader(ballot_, id_))
      return Result{ResultType::kSomeoneElseLeader, ExtractLeaderId(ballot_)};
  }
  return Result{ResultType::kRetry, std::nullopt};
}

void MultiPaxos::RunCommitPhase(int64_t ballot,
                                std::vector<int64_t>& gles) {
  auto state = std::make_shared<commit_state_t>();

  CommitRequest request;
  request.set_ballot(ballot);
  request.set_sender(id_);

  ++state->num_rpcs_;
  ++state->num_oks_;
  for (auto i = 0; i < partition_size_; i ++) {
    auto last_executed = logs_[i]->LastExecuted();
    state->min_last_executed_.emplace_back(last_executed);
    request.add_last_executed(last_executed);
    request.add_global_last_executed(gles[i]);
    logs_[i]->TrimUntil(gles[i]);
  }

  for (auto& peer : rpc_peers_) {
    if (peer.id_ == id_) {
      continue;
    }
    asio::post(thread_pool_, [this, state, &peer, request] {
      ClientContext context;
      CommitResponse response;
      Status s = peer.stub_->Commit(&context, std::move(request), &response);
      DLOG(INFO) << id_ << " sent commit to " << peer.id_;
      {
        std::scoped_lock lock(state->mu_);
        ++state->num_rpcs_;
        if (s.ok()) {
          if (response.type() == OK) {
            ++state->num_oks_;
            for (auto i = 0; i < partition_size_; i ++) {
              if (response.last_executed(i) < state->min_last_executed_[i])
                state->min_last_executed_[i] = response.last_executed(i);
            }
          } else {
            BecomeFollower(response.ballot());
          }
        }
      }
      state->cv_.notify_one();
    });
  }
  {
    std::unique_lock lock(state->mu_);
    while (IsLeader(ballot_, id_) && state->num_rpcs_ != num_peers_)
      state->cv_.wait(lock);
    if (state->num_oks_ == num_peers_) {
      for (auto i = 0; i < partition_size_; i ++) {
        gles[i] = state->min_last_executed_[i];
      }
    }
  }
}

void MultiPaxos::Replay(
    int64_t ballot,
    std::unordered_map<int64_t, multipaxos::Instance> const& log) {
  for (auto const& [index, instance] : log) {
    int partition_index = hash_function(instance.command().key());
    Result r = RunAcceptPhase(ballot, instance.index(), instance.command(),
                              instance.client_id(), partition_index);
    while (r.type_ == ResultType::kRetry)
      r = RunAcceptPhase(ballot, instance.index(), instance.command(),
                         instance.client_id(), partition_index);
    if (r.type_ == ResultType::kSomeoneElseLeader)
      return;
  }
}

Status MultiPaxos::Prepare(ServerContext*,
                           const PrepareRequest* request,
                           PrepareResponse* response) {
  DLOG(INFO) << id_ << " <--prepare-- " << request->sender();
  if (request->ballot() > ballot_) {
    BecomeFollower(request->ballot());
    //TODO
    // for (auto& i : log_->Instances())
    //   *response->add_instances() = std::move(i);
    response->set_type(OK);
  } else {
    response->set_ballot(ballot_);
    response->set_type(REJECT);
  }
  return Status::OK;
}

Status MultiPaxos::Accept(ServerContext*,
                          const AcceptRequest* request,
                          AcceptResponse* response) {
  DLOG(INFO) << id_ << " <--accept--- " << request->sender();
  if (request->instance().ballot() >= ballot_) {
    logs_[request->partition_index()]->Append(request->instance());
    response->set_type(OK);
    if (request->instance().ballot() > ballot_)
      BecomeFollower(request->instance().ballot());
  }
  if (request->instance().ballot() < ballot_) {
    response->set_ballot(ballot_);
    response->set_type(REJECT);
  }
  return Status::OK;
}

Status MultiPaxos::Commit(ServerContext*,
                          const CommitRequest* request,
                          CommitResponse* response) {
  DLOG(INFO) << id_ << " <--commit--- " << request->sender();
  if (request->ballot() >= ballot_) {
    commit_received_ = true;
    for (auto i = 0; i < partition_size_; i++) {
      logs_[i]->CommitUntil(request->last_executed(i), request->ballot());
      logs_[i]->TrimUntil(request->global_last_executed(i));
      response->add_last_executed(logs_[i]->LastExecuted());
    }
    response->set_type(OK);
    if (request->ballot() > ballot_)
      BecomeFollower(request->ballot());
  } else {
    response->set_ballot(ballot_);
    response->set_type(REJECT);
  }
  return Status::OK;
}
