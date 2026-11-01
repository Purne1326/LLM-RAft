package raft

import (
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents an individual log entry in Raft.
type LogEntry struct {
	Index   int         `json:"index"`
	Term    int         `json:"term"`
	Command interface{} `json:"command"`
}

// ApplyMsg is sent on the applyChannel to the state machine whenever an entry is committed.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For snapshots
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Config represents tuning options for Raft timers and paths.
type Config struct {
	HeartbeatInterval    time.Duration
	MinElectionTimeout   time.Duration
	MaxElectionTimeout   time.Duration
	StorageDir           string
}

func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:  50 * time.Millisecond,
		MinElectionTimeout: 150 * time.Millisecond,
		MaxElectionTimeout: 300 * time.Millisecond,
		StorageDir:         "",
	}
}

