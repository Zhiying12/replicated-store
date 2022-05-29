package store

import "github.com/psu-csl/replicated-store/go/command"

const (
	KeyNotFound string = "key not found"
	Empty              = ""
)

type MemKVStore struct {
	store map[string]string
}

func NewMemKVStore() *MemKVStore {
	s := MemKVStore{
		store: make(map[string]string),
	}
	return &s
}

func (s *MemKVStore) Get(key string) *string {
	if value, ok := s.store[key]; ok {
		return &value
	} else {
		return nil
	}
}

func (s *MemKVStore) Put(key string, value string) bool {
	s.store[key] = value
	return true
}

func (s *MemKVStore) Del(key string) bool {
	if _, ok := s.store[key]; ok {
		delete(s.store, key)
		return true
	} else {
		return false
	}
}

func (s *MemKVStore) Execute(cmd command.Command) command.Result {
	if cmd.Type == command.Get {
		value := s.Get(cmd.Key)
		if value != nil {
			return command.Result{Ok: true, Value: *value}
		} else {
			return command.Result{Ok: false, Value: KeyNotFound}
		}
	}

	if cmd.Type == command.Put {
		s.Put(cmd.Key, cmd.Value)
		return command.Result{Ok: true, Value: Empty}
	}

	if cmd.Type != command.Del {
		panic("Command type not Del")
	}

	if s.Del(cmd.Key) {
		return command.Result{Ok: true, Value: Empty}
	}
	return command.Result{Ok: false, Value: KeyNotFound}
}
