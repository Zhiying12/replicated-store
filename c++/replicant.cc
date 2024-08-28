#include <glog/logging.h>
#include <asio.hpp>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>

#include "json.h"
#include "replicant.h"

using asio::ip::tcp;
using nlohmann::json;

Replicant::Replicant(asio::io_context* io_context, json const& config)
    : id_(config["id"]),
      multi_paxos_(logs_, config),
      ip_port_(config["peers"][id_]),
      io_context_(io_context),
      acceptor_(asio::make_strand(*io_context_)),
      client_manager_(id_, config["peers"].size(), &multi_paxos_),
      partition_size_(config["partition_size"]) {
  for (auto i = 0; i < partition_size_; i++) {
    Log log(kvstore::CreateStore(config));
    logs_.emplace_back(&log);
  }
}

void Replicant::Start() {
  multi_paxos_.Start();
  StartExecutorThread();
  StartServer();
}

void Replicant::Stop() {
  StopServer();
  StopExecutorThread();
  multi_paxos_.Stop();
}

void Replicant::StartServer() {
  auto pos = ip_port_.find(":") + 1;
  CHECK_NE(pos, std::string::npos);
  int port = std::stoi(ip_port_.substr(pos)) + 1;

  tcp::endpoint endpoint(tcp::v4(), port);
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(tcp::acceptor::reuse_address(true));
  acceptor_.set_option(tcp::no_delay(true));
  acceptor_.bind(endpoint);
  acceptor_.listen(5);
  DLOG(INFO) << id_ << " starting server at port " << port;

  auto self(shared_from_this());
  asio::dispatch(acceptor_.get_executor(), [this, self] { AcceptClient(); });
}

void Replicant::StopServer() {
  auto self(shared_from_this());
  asio::post(acceptor_.get_executor(), [this, self] { acceptor_.close(); });
  client_manager_.StopAll();
}

void Replicant::StartExecutorThread() {
  DLOG(INFO) << id_ << " starting executor thread";
  for (auto i = 0; i < partition_size_; i++) {
    executor_threads_.emplace_back(&Replicant::ExecutorThread, this, i);
  }
}

void Replicant::StopExecutorThread() {
  DLOG(INFO) << id_ << " stopping executor thread";
  for (auto& log : logs_) {
    log->Stop();
  }
  for (auto& t : executor_threads_) {
    t.join();
  }
}

void Replicant::ExecutorThread(int index) {
  for (;;) {
    auto r = logs_[index]->Execute();
    if (!r)
      break;
    auto [id, result] = std::move(*r);
    auto client = client_manager_.Get(id);
    if (client)
      client->Write(result.value_);
  }
}

void Replicant::AcceptClient() {
  auto self(shared_from_this());
  acceptor_.async_accept(asio::make_strand(*io_context_),
                         [this, self](std::error_code ec, tcp::socket socket) {
                           if (!acceptor_.is_open())
                             return;
                           if (!ec) {
                             client_manager_.Start(std::move(socket));
                             AcceptClient();
                           }
                         });
}
