#include <gtest/gtest.h>
#include <atomic>
#include <thread>

#include "log.h"
#include "memkvstore.h"

TEST(LogTest, Constructor) {
  Log log;

  EXPECT_EQ(log.LastExecuted(), 0);
  EXPECT_EQ(log.GlobalLastExecuted(), 0);
  EXPECT_FALSE(log.IsExecutable());
  EXPECT_EQ(nullptr, log[0]);
  EXPECT_EQ(nullptr, log[-1]);
  EXPECT_EQ(nullptr, log[3]);
}

TEST(LogTest, Append) {
  // append two instances at indexes 1 and 2 to the log and ensure they are in
  // the log.
  {
    Log log;
    Command cmd;

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i1));
    EXPECT_EQ(index, log[index]->index_);

    index = log.AdvanceLastIndex();
    Instance i2{0, index, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i2));
    EXPECT_EQ(index, log[index]->index_);
  }
  // when appending an instance in executed state, its state should be updated
  // to committed.
  {
    Log log;
    Command cmd;

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kExecuted, cmd};
    log.Append(std::move(i1));
    EXPECT_EQ(InstanceState::kCommitted, log[index]->state_);
  }
  // when append an instance at an index higher than last_index_, last_index_
  // should be updated.
  {
    Log log;
    Command cmd;

    int64_t index = 42;
    Instance i1{0, index, 0, InstanceState::kExecuted, cmd};
    log.Append(std::move(i1));
    EXPECT_EQ(index, log[index]->index_);
    EXPECT_EQ(index + 1, log.AdvanceLastIndex());
  }
  // appending an instance at an index containing an instance with a lower
  // ballot number should replace the instance in the log.
  {
    Log log;
    Command cmd1{CommandType::kPut, "", ""};

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd1};
    log.Append(std::move(i1));
    EXPECT_EQ(index, log[index]->index_);
    EXPECT_EQ(CommandType::kPut, log[index]->command_.type_);

    Command cmd2{CommandType::kDel, "", ""};
    Instance i2{1, index, 0, InstanceState::kInProgress, cmd2};
    log.Append(std::move(i2));
    EXPECT_EQ(CommandType::kDel, log[index]->command_.type_);
  }
  // appending an instance at an index containing an instance with a lower
  // ballot number should have no effect.
  {
    Log log;
    Command cmd1{CommandType::kPut, "", ""};

    int64_t index = log.AdvanceLastIndex();
    Instance i1{2, index, 0, InstanceState::kInProgress, cmd1};
    log.Append(std::move(i1));
    EXPECT_EQ(index, log[index]->index_);
    EXPECT_EQ(CommandType::kPut, log[index]->command_.type_);

    Command cmd2{CommandType::kDel, "", ""};
    Instance i2{1, index, 0, InstanceState::kInProgress, cmd2};
    log.Append(std::move(i2));
    EXPECT_EQ(CommandType::kPut, log[index]->command_.type_);
  }
  // filling the gaps in the log (i.e. inserting entries at indexes lower than
  // the current value of last_index_) should not affect last_index_
  {
    Log log;
    Command cmd;
    int64_t index = 42;
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i1));

    Instance i2{0, index - 10, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i2));

    EXPECT_EQ(index + 1, log.AdvanceLastIndex());
  }
}

TEST(LogDeathTest, Append) {
  // ensure that the assertion fires for case (3) from the design doc.
  {
    Log log;
    Command cmd1;

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kCommitted, cmd1};
    log.Append(std::move(i1));

    Command cmd2{CommandType::kPut, "", ""};
    Instance i2{0, index, 0, InstanceState::kInProgress, cmd2};
    EXPECT_DEATH(log.Append(std::move(i2)), "case 3");
  }
  // same as above, except when the instance already in the log is in executed
  // state.
  {
    Log log;
    Command cmd1;

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kExecuted, cmd1};
    log.Append(std::move(i1));

    Command cmd2{CommandType::kPut, "", ""};
    Instance i2{0, index, 0, InstanceState::kInProgress, cmd2};
    EXPECT_DEATH(log.Append(std::move(i2)), "case 3");
  }
  // ensure that the assertion fires for case (4) from the design doc.
  {
    Log log;
    Command cmd1{CommandType::kPut, "", ""};

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd1};
    log.Append(std::move(i1));

    Command cmd2{CommandType::kDel, "", ""};
    Instance i2{0, index, 0, InstanceState::kInProgress, cmd2};
    EXPECT_DEATH(log.Append(std::move(i2)), "case 4");
  }
}

TEST(LogTest, Commit) {
  // common case: an instance gets appended and committing it changes its state
  // to committed.
  {
    Log log;
    Command cmd;

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i1));
    EXPECT_EQ(InstanceState::kInProgress, log[index]->state_);
    log.Commit(index);
    EXPECT_EQ(InstanceState::kCommitted, log[index]->state_);
  }
}

TEST(LogTest, AppendCommit) {
  // if we append and instance and commit it, the log should become executable
  {
    Log log;
    Command cmd;

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i1));
    EXPECT_FALSE(log.IsExecutable());

    log.Commit(index);
    EXPECT_TRUE(log.IsExecutable());
  }
  // if we append two instances in sequence and commit the second one, the log
  // should not become executable; committing the first one should make the log
  // executable again.
  {
    Log log;
    Command cmd;

    int64_t index1 = log.AdvanceLastIndex();
    Instance i1{0, index1, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i1));

    int64_t index2 = log.AdvanceLastIndex();
    Instance i2{0, index2, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i2));

    log.Commit(index2);
    EXPECT_FALSE(log.IsExecutable());
    log.Commit(index1);
    EXPECT_TRUE(log.IsExecutable());
  }
  // if commit is called first on an index where there is no instance yet, it
  // should still eventually succeed when append is called and the instance is
  // put at index.
  {
    Log log;
    Command cmd;
    int64_t index = log.AdvanceLastIndex();
    std::thread commit([&log, index] { log.Commit(index); });
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd};
    std::this_thread::yield();
    log.Append(std::move(i1));
    commit.join();
    EXPECT_EQ(InstanceState::kCommitted, log[index]->state_);
  }
}

TEST(LogTest, AppendCommitExecute) {
  // simplest case: append an entry, commit it, and execute it.
  {
    Log log;
    MemKVStore store;
    Command cmd;
    std::atomic<bool> done = false;

    std::thread execute([&done, &log, &store] {
      while (!done)
        log.Execute(&store);
    });

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i1));

    done = true;
    log.Commit(index);
    execute.join();

    EXPECT_EQ(InstanceState::kExecuted, log[index]->state_);
    EXPECT_EQ(index, log.LastExecuted());
  }
}

TEST(LogDeathTest, Commit) {
  Log log;

  EXPECT_DEATH(log.Commit(0), "invalid index");
  EXPECT_DEATH(log.Commit(-1), "invalid index");
}
