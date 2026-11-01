package tests

import (
	"raft-kv/pkg/raft"
	"raft-kv/pkg/transport"
	"testing"
	"time"
)

func createTestCluster(n int) (*transport.SimulatedNetwork, []*raft.Raft, []*raft.InMemPersister) {
	net := transport.NewSimulatedNetwork(42)
	rafts := make([]*raft.Raft, n)
	persisters := make([]*raft.InMemPersister, n)

	// Create transports
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
		applyCh := make(chan raft.ApplyMsg, 100)
		rafts[i] = raft.Make(peerTransports[i], i, persisters[i], applyCh, cfg)
		net.RegisterServer(i, rafts[i])
	}

	return net, rafts, persisters
}

func checkSingleLeader(t *testing.T, rafts []*raft.Raft) int {
	for it := 0; it < 20; it++ {
		time.Sleep(50 * time.Millisecond)
		leaders := []int{}
		for i, rf := range rafts {
			_, isLeader := rf.GetState()
			if isLeader {
				leaders = append(leaders, i)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
	}
	t.Fatalf("Expected exactly 1 leader, got unexpected election state")
	return -1
}

func TestInitialElection(t *testing.T) {
	_, rafts, _ := createTestCluster(3)
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	leader := checkSingleLeader(t, rafts)
	t.Logf("Initial election succeeded. Leader elected: node %d", leader)
}

func TestLeaderFailover(t *testing.T) {
	_, rafts, _ := createTestCluster(3)
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	leader1 := checkSingleLeader(t, rafts)
	t.Logf("Killing original leader: node %d", leader1)

	// Kill leader
	rafts[leader1].Kill()

	// Wait and verify a new leader is elected among remaining 2 nodes within 1 second
	start := time.Now()
	var newLeader int = -1
	for time.Since(start) < 1200*time.Millisecond {
		time.Sleep(50 * time.Millisecond)
		for i, rf := range rafts {
			if i == leader1 {
				continue
			}
			_, isLeader := rf.GetState()
			if isLeader {
				newLeader = i
				break
			}
		}
		if newLeader != -1 {
			break
		}
	}

	if newLeader == -1 {
		t.Fatalf("Failed to elect new leader after killing leader %d", leader1)
	}
	t.Logf("Leader failover successful! New leader is node %d", newLeader)
}

