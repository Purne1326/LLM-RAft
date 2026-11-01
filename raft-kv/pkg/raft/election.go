package raft

import (
	"sync"
	"time"
)

// RequestVote handles incoming vote requests from candidates.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule 1: If args.Term < currentTerm, reject vote
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If term is higher, step down to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Rule 2: Check election safety (up-to-date log check)
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	logUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logUpToDate = true
	}

	canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateID)
	if canVote && logUpToDate {
		rf.votedFor = args.CandidateID
		rf.role = Follower
		reply.VoteGranted = true
		rf.resetElectionTimer()
		rf.persist()
	}
}

func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	term := rf.currentTerm
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	votes := 1
	var voteMu sync.Mutex

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply

			ok := rf.peers[peer].CallRequestVote(&args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				rf.persist()
				return
			}

			if rf.role != Candidate || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				currentVotes := votes
				voteMu.Unlock()

				// If majority reached, transition to Leader
				if currentVotes > len(rf.peers)/2 && rf.role == Candidate {
					rf.becomeLeader()
				}
			}
		}(peerIdx)
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	lastIndex := rf.getLastLogIndex()

	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastIndex

	// Start leader heartbeat ticker
	go rf.leaderHeartbeatLoop()

	// Broadcast initial empty AppendEntries (heartbeat)
	go rf.broadcastAppendEntries(true)
}

func (rf *Raft) leaderHeartbeatLoop() {
	for !rf.killed() {
		time.Sleep(rf.cfg.HeartbeatInterval)
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.broadcastAppendEntries(true)
	}
}

