package raft

import (
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Raft implements an individual Raft consensus node.
type Raft struct {
	mu        sync.Mutex
	peers     []ClientTransport
	persister Persister
	me        int // this node's index into peers[]
	dead      int32

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	role        Role

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// Snapshot metadata:
	lastIncludedIndex int
	lastIncludedTerm  int

	// Timers and communication:
	applyCh        chan ApplyMsg
	lastHeard      time.Time
	electionTime   time.Duration
	cfg            Config
}

type PersistentState struct {
	CurrentTerm       int        `json:"current_term"`
	VotedFor          int        `json:"voted_for"`
	LastIncludedIndex int        `json:"last_included_index"`
	LastIncludedTerm  int        `json:"last_included_term"`
	Log               []LogEntry `json:"log"`
}

func Make(
	peers []ClientTransport,
	me int,
	persister Persister,
	applyCh chan ApplyMsg,
	cfg Config,
) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		currentTerm:       0,
		votedFor:          -1,
		log:               make([]LogEntry, 0),
		commitIndex:       0,
		lastApplied:       0,
		role:              Follower,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		applyCh:           applyCh,
		cfg:               cfg,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
	}

	// Base 0-index dummy entry for 1-based log indexing
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0, Command: nil})

	// Restore persistent state from persister
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTimer()

	// Start background ticker
	go rf.ticker()

	return rf
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) GetLeaderID() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		return rf.me
	}
	return rf.votedFor
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
	delta := int64(rf.cfg.MaxElectionTimeout - rf.cfg.MinElectionTimeout)
	if delta <= 0 {
		delta = 150 * int64(time.Millisecond)
	}
	randomTimeout := rf.cfg.MinElectionTimeout + time.Duration(rand.Int63n(delta))
	rf.electionTime = randomTimeout
}

// Log index translation helpers (accounting for compacted log indices)
func (rf *Raft) getLogEntry(index int) *LogEntry {
	idx := index - rf.lastIncludedIndex
	if idx < 0 || idx >= len(rf.log) {
		return nil
	}
	return &rf.log[idx]
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return rf.lastIncludedTerm
}

// Persist writes term, vote, and log to disk / persister
func (rf *Raft) persist() {
	state := PersistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Log:               rf.log,
	}
	data, err := json.Marshal(state)
	if err == nil {
		rf.persister.Save(data, nil)
	}
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err == nil {
		rf.currentTerm = state.CurrentTerm
		rf.votedFor = state.VotedFor
		rf.lastIncludedIndex = state.LastIncludedIndex
		rf.lastIncludedTerm = state.LastIncludedTerm
		rf.log = state.Log
		rf.commitIndex = state.LastIncludedIndex
		rf.lastApplied = state.LastIncludedIndex
	}
}

// Start appends a new command to the leader's log.
// Returns (index, term, isLeader).
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()

	// Broadcast AppendEntries to replicate immediately
	go rf.broadcastAppendEntries(false)

	return index, term, true
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()

		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}

		// Check if election timeout elapsed
		if time.Since(rf.lastHeard) > rf.electionTime {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

