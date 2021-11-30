use crate::kvstore::traits::KVStore;
use crate::kvstore::memstore::MemStore;
use crate::kvstore::command::Command;
use crate::consensus::traits::Consensus;

pub struct DummyPaxos<T: KVStore> {
    store: T
}

impl<T: KVStore> Consensus<T> for DummyPaxos<T> {
    fn new(store: T) -> Self {
        DummyPaxos { store }
    }

    fn agree_and_execute(&mut self, cmd: Command) -> Result<&str, &str> {
        match cmd {
            Command::Get(key) => self.store.get(key),
            Command::Put(key, value) => self.store.put(key, value),
            Command::Del(key) => self.store.del(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_put_del() {
        let mut paxos = DummyPaxos::new(MemStore::new());

        // get and del non-existent
        let cmd = Command::Get("foo");
        assert_eq!(paxos.agree_and_execute(cmd).err().unwrap(), "not found");
        let cmd = Command::Del("foo");
        assert_eq!(paxos.agree_and_execute(cmd).err().unwrap(), "not found");

        // put followed by get
        let cmd = Command::Put("foo".to_string(), "bar".to_string());
        assert_eq!(paxos.agree_and_execute(cmd).unwrap(), "ok");
        let cmd = Command::Get("foo");
        assert_eq!(paxos.agree_and_execute(cmd).unwrap(), "bar");

        // update followed by get
        let cmd = Command::Put("foo".to_string(), "baz".to_string());
        assert_eq!(paxos.agree_and_execute(cmd).unwrap(), "ok");
        let cmd = Command::Get("foo");
        assert_eq!(paxos.agree_and_execute(cmd).unwrap(), "baz");

        // del followed by get
        let cmd = Command::Del("foo");
        assert_eq!(paxos.agree_and_execute(cmd).unwrap(), "ok");
        let cmd = Command::Get("foo");
        assert_eq!(paxos.agree_and_execute(cmd).err().unwrap(), "not found");
    }
}
