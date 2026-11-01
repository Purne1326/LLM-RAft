package transport

import (
	"math/rand"
	"raft-kv/pkg/raft"
	"sync"
)

type RaftNodeServer interface {
	RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply)
	AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply)
	InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply)
}

type SimulatedNetwork struct {
	mu           sync.Mutex
	servers      map[int]RaftNodeServer
	partitions   map[int]int // nodeID -> partitionGroupID
	disconnected map[int]bool
	dropRate     float64
	rnd          *rand.Rand
}

func NewSimulatedNetwork(seed int64) *SimulatedNetwork {
	return &SimulatedNetwork{
		servers:      make(map[int]RaftNodeServer),
		partitions:   make(map[int]int),
		disconnected: make(map[int]bool),
		rnd:          rand.New(rand.NewSource(seed)),
	}
}

func (net *SimulatedNetwork) RegisterServer(id int, server RaftNodeServer) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.servers[id] = server
	net.partitions[id] = 0
}

func (net *SimulatedNetwork) Disconnect(id int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.disconnected[id] = true
}

func (net *SimulatedNetwork) Connect(id int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delete(net.disconnected, id)
}

func (net *SimulatedNetwork) Partition(group1 []int, group2 []int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	for _, n := range group1 {
		net.partitions[n] = 1
	}
	for _, n := range group2 {
		net.partitions[n] = 2
	}
}

func (net *SimulatedNetwork) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	for k := range net.partitions {
		net.partitions[k] = 0
	}
}

func (net *SimulatedNetwork) SetDropRate(rate float64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.dropRate = rate
}

func (net *SimulatedNetwork) canDeliver(from, to int) bool {
	net.mu.Lock()
	defer net.mu.Unlock()

	if net.disconnected[from] || net.disconnected[to] {
		return false
	}
	if net.partitions[from] != net.partitions[to] {
		return false
	}
	if net.dropRate > 0 && net.rnd.Float64() < net.dropRate {
		return false
	}
	return true
}

// ClientTransport implementation for simulated network
type SimulatedPeerClient struct {
	net  *SimulatedNetwork
	from int
	to   int
}

func NewSimulatedPeerClient(net *SimulatedNetwork, from, to int) *SimulatedPeerClient {
	return &SimulatedPeerClient{net: net, from: from, to: to}
}

func (sp *SimulatedPeerClient) CallRequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
	if !sp.net.canDeliver(sp.from, sp.to) {
		return false
	}
	sp.net.mu.Lock()
	srv, ok := sp.net.servers[sp.to]
	sp.net.mu.Unlock()
	if !ok {
		return false
	}

	srv.RequestVote(args, reply)
	return sp.net.canDeliver(sp.to, sp.from)
}

func (sp *SimulatedPeerClient) CallAppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
	if !sp.net.canDeliver(sp.from, sp.to) {
		return false
	}
	sp.net.mu.Lock()
	srv, ok := sp.net.servers[sp.to]
	sp.net.mu.Unlock()
	if !ok {
		return false
	}

	srv.AppendEntries(args, reply)
	return sp.net.canDeliver(sp.to, sp.from)
}

func (sp *SimulatedPeerClient) CallInstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) bool {
	if !sp.net.canDeliver(sp.from, sp.to) {
		return false
	}
	sp.net.mu.Lock()
	srv, ok := sp.net.servers[sp.to]
	sp.net.mu.Unlock()
	if !ok {
		return false
	}

	srv.InstallSnapshot(args, reply)
	return sp.net.canDeliver(sp.to, sp.from)
}

