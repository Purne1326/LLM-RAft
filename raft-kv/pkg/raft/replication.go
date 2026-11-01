package raft

// AppendEntries RPC handler on followers
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule 1: Reject if sender's term is lower
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Update term and reset role if sender's term is newer or equal
	if args.Term > rf.currentTerm || rf.role == Candidate {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	reply.Success = false

	// Rule 2: Log matching property check at prevLogIndex
	lastLogIndex := rf.getLastLogIndex()
	if args.PrevLogIndex > lastLogIndex {
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex > rf.lastIncludedIndex {
		prevEntry := rf.getLogEntry(args.PrevLogIndex)
		if prevEntry == nil || prevEntry.Term != args.PrevLogTerm {
			reply.ConflictTerm = -1
			if prevEntry != nil {
				reply.ConflictTerm = prevEntry.Term
				// Find first index with conflictTerm
				for idx := args.PrevLogIndex; idx > rf.lastIncludedIndex; idx-- {
					e := rf.getLogEntry(idx)
					if e != nil && e.Term == reply.ConflictTerm {
						reply.ConflictIndex = idx
					} else {
						break
					}
				}
			} else {
				reply.ConflictIndex = rf.lastIncludedIndex + 1
			}
			return
		}
	}

	// Rule 3: Insert / replace entries
	for i, entry := range args.Entries {
		existing := rf.getLogEntry(entry.Index)
		if existing != nil {
			if existing.Term != entry.Term {
				// Conflict: truncate log from this index onwards
				sliceIdx := entry.Index - rf.lastIncludedIndex
				rf.log = rf.log[:sliceIdx]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			// Append new entries
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// Rule 4: Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
		rf.applyEntries()
	}

	reply.Success = true
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	leaderID := rf.me
	commitIndex := rf.commitIndex
	rf.mu.Unlock()

	for peerIdx := range rf.peers {
		if peerIdx == leaderID {
			continue
		}

		go rf.sendAppendEntriesToPeer(peerIdx, term, leaderID, commitIndex)
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peer int, term int, leaderID int, commitIndex int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.lastIncludedIndex {
		// Follower is behind our compacted log: send InstallSnapshot instead!
		go rf.sendInstallSnapshotToPeer(peer, term, leaderID)
		rf.mu.Unlock()
		return
	}

	prevLogTerm := 0
	if prevLogIndex == rf.lastIncludedIndex {
		prevLogTerm = rf.lastIncludedTerm
	} else {
		entry := rf.getLogEntry(prevLogIndex)
		if entry != nil {
			prevLogTerm = entry.Term
		}
	}

	var entries []LogEntry
	lastIndex := rf.getLastLogIndex()
	if lastIndex >= rf.nextIndex[peer] {
		startSlice := rf.nextIndex[peer] - rf.lastIncludedIndex
		entries = make([]LogEntry, len(rf.log[startSlice:]))
		copy(entries, rf.log[startSlice:])
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	ok := rf.peers[peer].CallAppendEntries(&args, &reply)
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

	if rf.role != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newNext > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newNext
		}
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}

		// Check if commitIndex can be advanced (Raft rule: leader only commits from current term)
		for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
			entry := rf.getLogEntry(N)
			if entry == nil || entry.Term != rf.currentTerm {
				continue
			}

			count := 1
			for p := range rf.peers {
				if p != rf.me && rf.matchIndex[p] >= N {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				rf.applyEntries()
				break
			}
		}
	} else {
		// Decrement nextIndex on failure using conflict optimization
		if reply.ConflictIndex > 0 {
			rf.nextIndex[peer] = reply.ConflictIndex
		} else if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
		}
	}
}

func (rf *Raft) applyEntries() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		entry := rf.getLogEntry(rf.lastApplied)
		if entry != nil {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			rf.applyCh <- msg
		}
	}
}

