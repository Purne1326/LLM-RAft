package tests

import (
	"raft-kv/pkg/client"
	"raft-kv/pkg/kvstore"
	"raft-kv/pkg/raft"
	"raft-kv/pkg/transport"
	"testing"
	"time"
)

func TestCrashAndPersistenceRecovery(t *testing.T) {
	net := transport.NewSimulatedNetwork(123)
	n := 3
	servers := make([]*kvstore.KVServer, n)
	persisters := make([]*raft.InMemPersister, n)

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
		persisters[i] = raft.NewInMemPersister()
		servers[i] = kvstore.StartKVServer(peerTransports[i], i, persisters[i], 0, cfg)
		net.RegisterServer(i, servers[i].Raft())
	}
	defer func() {
		for _, s := range servers {
			s.Kill()
		}
	}()

	time.Sleep(200 * time.Millisecond)

	clerk := client.MakeClerk(servers)
	_ = clerk.Put("persistent_key", "original_value")

	// Crash server 0
	t.Log("Crashing server 0...")
	servers[0].Kill()
	net.Disconnect(0)

	// Write more data to remaining majority (servers 1 and 2)
	_ = clerk.Put("persistent_key", "updated_value")

	// Restart server 0 from its saved persister
	t.Log("Restarting server 0 from persister...")
	servers[0] = kvstore.StartKVServer(peerTransports[0], 0, persisters[0], 0, cfg)
	net.RegisterServer(0, servers[0].Raft())
	net.Connect(0)

	time.Sleep(300 * time.Millisecond)

	// Verify that restarted node synchronizes with cluster and serves consistent data
	clerkRecovered := client.MakeClerk(servers)
	val, err := clerkRecovered.Get("persistent_key")
	if err != nil || val != "updated_value" {
		t.Fatalf("Recovered node failed to read state: val=%s, err=%v", val, err)
	}

	t.Log("Crash and persistence recovery verified successfully.")
}

