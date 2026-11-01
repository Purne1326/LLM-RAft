package kvstore

import (
	"encoding/json"
	"sync"
)

type OpType string

const (
	OpPut    OpType = "Put"
	OpGet    OpType = "Get"
	OpDelete OpType = "Delete"
)

// Op represents a client command logged into Raft.
type Op struct {
	Type     OpType `json:"type"`
	Key      string `json:"key"`
	Value    string `json:"value"`
	ClientID string `json:"client_id"`
	SeqNum   int64  `json:"seq_num"`
}

// Result represents the result returned after applying an Op.
type Result struct {
	Value  string
	Err    string
	SeqNum int64
}

// MemoryKVStateMachine is the core KV engine with client deduplication.
type MemoryKVStateMachine struct {
	mu           sync.Mutex
	data         map[string]string
	lastSequence map[string]int64  // clientID -> last executed seq_num
	lastResults  map[string]Result // clientID -> result of last executed seq_num
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		data:         make(map[string]string),
		lastSequence: make(map[string]int64),
		lastResults:  make(map[string]Result),
	}
}

func (sm *MemoryKVStateMachine) Apply(op Op) Result {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Duplicate detection: if already executed for this client, return memoized result
	if lastSeq, ok := sm.lastSequence[op.ClientID]; ok && op.SeqNum <= lastSeq {
		return sm.lastResults[op.ClientID]
	}

	var res Result
	res.SeqNum = op.SeqNum

	switch op.Type {
	case OpPut:
		sm.data[op.Key] = op.Value
		res.Value = op.Value
	case OpGet:
		val, exists := sm.data[op.Key]
		if exists {
			res.Value = val
		} else {
			res.Err = "KeyNotFound"
		}
	case OpDelete:
		delete(sm.data, op.Key)
		res.Value = ""
	}

	// Update deduplication table
	if op.ClientID != "" {
		sm.lastSequence[op.ClientID] = op.SeqNum
		sm.lastResults[op.ClientID] = res
	}

	return res
}

func (sm *MemoryKVStateMachine) Snapshot() []byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	state := struct {
		Data         map[string]string
		LastSequence map[string]int64
		LastResults  map[string]Result
	}{
		Data:         sm.data,
		LastSequence: sm.lastSequence,
		LastResults:  sm.lastResults,
	}

	b, _ := json.Marshal(state)
	return b
}

func (sm *MemoryKVStateMachine) RestoreSnapshot(data []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var state struct {
		Data         map[string]string
		LastSequence map[string]int64
		LastResults  map[string]Result
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	sm.data = state.Data
	sm.lastSequence = state.LastSequence
	sm.lastResults = state.LastResults
	return nil
}

