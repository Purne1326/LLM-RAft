package raft

// RequestVoteArgs holds parameters for RequestVote RPC.
type RequestVoteArgs struct {
	Term         int `json:"term"`
	CandidateID  int `json:"candidate_id"`
	LastLogIndex int `json:"last_log_index"`
	LastLogTerm  int `json:"last_log_term"`
}

// RequestVoteReply holds response parameters for RequestVote RPC.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs holds parameters for AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     int        `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

// AppendEntriesReply holds response parameters for AppendEntries RPC.
type AppendEntriesReply struct {
	Term          int  `json:"term"`
	Success       bool `json:"success"`
	ConflictIndex int  `json:"conflict_index"`
	ConflictTerm  int  `json:"conflict_term"`
}

// InstallSnapshotArgs holds parameters for InstallSnapshot RPC.
type InstallSnapshotArgs struct {
	Term              int    `json:"term"`
	LeaderID          int    `json:"leader_id"`
	LastIncludedIndex int    `json:"last_included_index"`
	LastIncludedTerm  int    `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// InstallSnapshotReply holds response parameters for InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term int `json:"term"`
}

