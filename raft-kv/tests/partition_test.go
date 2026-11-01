package tests

import (
	"raft-kv/pkg/client"
	"raft-kv/pkg/kvstore"
	"raft-kv/pkg/raft"
	"raft-kv/pkg/transport"
	"testing"
	"time"
)

func TestNetworkPartitionAndHealing(t *testing.T) {
	net := transport.NewSimulatedNetwork(777)
	n := 5
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
		servers[i] = kvstore.StartKVServer(peerTransports[i], i, persister, 0, cfg)
		net.RegisterServer(i, servers[i].Raft())
	}
	defer func() {
		for _, s := range servers {
			s.Kill()
		}
	}()

	time.Sleep(300 * time.Millisecond)

	// Clerk connected to all servers
	clerk := client.MakeClerk(servers)

	err := clerk.Put("k1", "v1")
	if err != nil {
		t.Fatalf("Pre-partition put failed: %v", err)
	}

	t.Log("Partitioning cluster into Minority [0, 1] and Majority [2, 3, 4]")
	net.Partition([]int{0, 1}, []int{2, 3, 4})

	// Wait for majority [2, 3, 4] to elect a leader if needed
	time.Sleep(350 * time.Millisecond)

	// Clerk accessing majority partition should succeed
	majorityClerk := client.MakeClerk(servers[2:])
	err = majorityClerk.Put("k2", "v2_majority")
	if err != nil {
		t.Fatalf("Majority partition was unable to commit write: %v", err)
	}

	val, err := majorityClerk.Get("k2")
	if err != nil || val != "v2_majority" {
		t.Fatalf("Failed to read from majority: val=%s, err=%v", val, err)
	}

	t.Log("Healing network partition...")
	net.Heal()
	time.Sleep(400 * time.Millisecond)

	// Now whole cluster is healed; all nodes should see the latest writes
	clerkAll := client.MakeClerk(servers)
	valFinal, err := clerkAll.Get("k2")
	if err != nil || valFinal != "v2_majority" {
		t.Fatalf("Post-heal read failed: val=%s, err=%v", valFinal, err)
	}

	t.Log("Network partition and healing test passed successfully!")
}

