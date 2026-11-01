package tests

import (
	"raft-kv/pkg/client"
	"raft-kv/pkg/kvstore"
	"raft-kv/pkg/raft"
	"raft-kv/pkg/transport"
	"testing"
	"time"
)

func createKVCluster(n int, maxRaftState int) (*transport.SimulatedNetwork, []*kvstore.KVServer, *client.Clerk) {
	net := transport.NewSimulatedNetwork(100)
	servers := make([]*kvstore.KVServer, n)

	peerTransports := make([][]raft.ClientTransport, n)
	for i := 0; i < n; i++ {
		peerTransports[i] = make([]raft.ClientTransport, n)
		for j := 0; j < n; j++ {
			peerTransports[i][j] = transport.NewSimulatedPeerClient(net, i, j)
		}
	}

	cfg := raft.DefaultConfig()
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.MinElectionTimeout = 100 * time.Millisecond
	cfg.MaxElectionTimeout = 200 * time.Millisecond

	for i := 0; i < n; i++ {
		persister := raft.NewInMemPersister()
		servers[i] = kvstore.StartKVServer(peerTransports[i], i, persister, maxRaftState, cfg)
		net.RegisterServer(i, servers[i].Raft())
	}

	clerk := client.MakeClerk(servers)
	return net, servers, clerk
}

func TestKVReplication(t *testing.T) {
	_, servers, clerk := createKVCluster(3, 0)
	defer func() {
		for _, s := range servers {
			s.Kill()
		}
	}()

	// Wait for election
	time.Sleep(200 * time.Millisecond)

	// Test Basic Put and Get
	err := clerk.Put("user:101", "Alice")
	if err != nil {
		t.Fatalf("Failed to Put: %v", err)
	}

	val, err := clerk.Get("user:101")
	if err != nil {
		t.Fatalf("Failed to Get: %v", err)
	}
	if val != "Alice" {
		t.Fatalf("Expected 'Alice', got '%s'", val)
	}

	// Overwrite value
	_ = clerk.Put("user:101", "Bob")
	val2, _ := clerk.Get("user:101")
	if val2 != "Bob" {
		t.Fatalf("Expected 'Bob', got '%s'", val2)
	}

	t.Log("Basic Put/Get replication across cluster verified successfully.")
}

