package kvstore

import (
	"encoding/json"
	"raft-kv/pkg/raft"
	"sync"
	"time"
)

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         bool
	maxRaftState int

	sm      *MemoryKVStateMachine
	waitMap map[int]chan Result // logIndex -> channel notifying waiting client handler
}

func StartKVServer(
	peers []raft.ClientTransport,
	me int,
	persister raft.Persister,
	maxRaftState int,
	cfg raft.Config,
) *KVServer {
	applyCh := make(chan raft.ApplyMsg, 100)
	sm := NewMemoryKVStateMachine()

	kv := &KVServer{
		me:           me,
		applyCh:      applyCh,
		maxRaftState: maxRaftState,
		sm:           sm,
		waitMap:      make(map[int]chan Result),
	}

	kv.rf = raft.Make(peers, me, persister, applyCh, cfg)

	// Restore snapshot if exists
	snapshotData := persister.ReadSnapshot()
	if len(snapshotData) > 0 {
		_ = sm.RestoreSnapshot(snapshotData)
	}

	go kv.applyLoop(persister)

	return kv
}

func (kv *KVServer) ExecuteOp(op Op, timeout time.Duration) (Result, bool, int) {
	// Propose command to Raft
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Result{Err: "WrongLeader"}, false, kv.rf.GetLeaderID()
	}

	kv.mu.Lock()
	ch := make(chan Result, 1)
	kv.waitMap[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitMap, index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		// Check current term to ensure leadership hasn't changed during execution
		currentTerm, stillLeader := kv.rf.GetState()
		if !stillLeader || currentTerm != term {
			return Result{Err: "LeaderLost"}, false, kv.rf.GetLeaderID()
		}
		return res, true, kv.me

	case <-time.After(timeout):
		return Result{Err: "Timeout"}, false, kv.rf.GetLeaderID()
	}
}

func (kv *KVServer) applyLoop(persister raft.Persister) {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			// Convert command to Op
			var op Op
			switch c := msg.Command.(type) {
			case Op:
				op = c
			case map[string]interface{}:
				b, _ := json.Marshal(c)
				_ = json.Unmarshal(b, &op)
			default:
				b, _ := json.Marshal(c)
				_ = json.Unmarshal(b, &op)
			}

			res := kv.sm.Apply(op)

			kv.mu.Lock()
			if ch, ok := kv.waitMap[msg.CommandIndex]; ok {
				ch <- res
			}

			// Check snapshot trigger
			if kv.maxRaftState > 0 && persister.RaftStateSize() >= kv.maxRaftState {
				snapshotBytes := kv.sm.Snapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshotBytes)
			}
			kv.mu.Unlock()

		} else if msg.SnapshotValid {
			kv.mu.Lock()
			_ = kv.sm.RestoreSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Raft() *raft.Raft {
	return kv.rf
}

func (kv *KVServer) Kill() {
	kv.mu.Lock()
	kv.dead = true
	kv.mu.Unlock()
	kv.rf.Kill()
}

