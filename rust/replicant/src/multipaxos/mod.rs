use super::log::Log;
use crate::kvstore::memkvstore::MemKVStore;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Condvar, Mutex};
use log::debug;

tonic::include_proto!("multipaxos");

struct MultiPaxos {
    ready: AtomicBool,
    commit_received: AtomicBool,
    prepare_thread_running: AtomicBool,
    commit_thread_running: AtomicBool,
    id: i64,
    log: Arc<Log>,
    state: Mutex<State>,
    cv_leader: Condvar,
    cv_follower: Condvar,
}

#[derive(Default)]
struct State {
    ballot: i64,
    rpc_server_running: bool,
}

const ID_BITS: i64 = 0xff;
const ROUND_INCREMENT: i64 = ID_BITS + 1;
const MAX_NUM_PEERS: i64 = 0xf;

impl MultiPaxos {
    fn new(id: i64, log: Arc<Log>) -> Self {
        Self {
            ready: AtomicBool::new(false),
            commit_received: AtomicBool::new(false),
            prepare_thread_running: AtomicBool::new(false),
            commit_thread_running: AtomicBool::new(false),
            id: id,
            log: log,
            state: Mutex::new(State::default()),
            cv_leader: Condvar::new(),
            cv_follower: Condvar::new(),
        }
    }

    fn next_ballot(&self) -> i64 {
        let state = self.state.lock().unwrap();
        let mut next_ballot = state.ballot;
        next_ballot += ROUND_INCREMENT;
        next_ballot = (next_ballot & !ID_BITS) | self.id;
        next_ballot
    }

    fn become_leader(&mut self, next_ballot: i64) {
        let mut state = self.state.lock().unwrap();
        debug!("{} became a leader: ballot: {} -> {}",
               self.id, state.ballot, next_ballot);
        state.ballot = next_ballot;
        *self.ready.get_mut() = false;
        self.cv_leader.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const NUM_PEERS: i64 = 3;

    fn make_peers(log: Arc<Log>) -> Vec<MultiPaxos> {
        let mut peers = vec![];
        for id in 0..NUM_PEERS {
            peers.push(MultiPaxos::new(id, log.clone()));
        }
        peers
    }

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn next_ballot() {
        init();
        let store = Box::new(MemKVStore::new());
        let peers = make_peers(Arc::new(Log::new(store)));

        for id in 0..NUM_PEERS {
            let ballot = id + ROUND_INCREMENT;
            assert_eq!(ballot, peers[id as usize].next_ballot())
        }
    }
}
