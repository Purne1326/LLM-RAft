package client

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"raft-kv/pkg/kvstore"
	"sync"
	"time"
)

type Clerk struct {
	servers  []*kvstore.KVServer
	clientID string
	seqNum   int64
	leaderID int
	mu       sync.Mutex
}

func MakeClerk(servers []*kvstore.KVServer) *Clerk {
	n, _ := rand.Int(rand.Reader, big.NewInt(1000000000))
	return &Clerk{
		servers:  servers,
		clientID: fmt.Sprintf("client-%d", n.Int64()),
		seqNum:   0,
		leaderID: 0,
	}
}

func (ck *Clerk) Put(key string, value string) error {
	return ck.execute(kvstore.OpPut, key, value)
}

func (ck *Clerk) Get(key string) (string, error) {
	ck.mu.Lock()
	ck.seqNum++
	seq := ck.seqNum
	clientID := ck.clientID
	ck.mu.Unlock()

	op := kvstore.Op{
		Type:     kvstore.OpGet,
		Key:      key,
		ClientID: clientID,
		SeqNum:   seq,
	}

	for {
		leader := ck.leaderID
		server := ck.servers[leader]

		res, ok, newLeader := server.ExecuteOp(op, 800*time.Millisecond)
		if ok && res.Err == "" {
			return res.Value, nil
		}
		if ok && res.Err == "KeyNotFound" {
			return "", fmt.Errorf("KeyNotFound")
		}

		if newLeader >= 0 && newLeader < len(ck.servers) {
			ck.leaderID = newLeader
		} else {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) Delete(key string) error {
	return ck.execute(kvstore.OpDelete, key, "")
}

func (ck *Clerk) execute(opType kvstore.OpType, key string, value string) error {
	ck.mu.Lock()
	ck.seqNum++
	seq := ck.seqNum
	clientID := ck.clientID
	ck.mu.Unlock()

	op := kvstore.Op{
		Type:     opType,
		Key:      key,
		Value:    value,
		ClientID: clientID,
		SeqNum:   seq,
	}

	for {
		leader := ck.leaderID
		server := ck.servers[leader]

		res, ok, newLeader := server.ExecuteOp(op, 800*time.Millisecond)
		if ok && res.Err == "" {
			return nil
		}

		if newLeader >= 0 && newLeader < len(ck.servers) {
			ck.leaderID = newLeader
		} else {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

