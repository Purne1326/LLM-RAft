package raft

// Snapshot discards log entries up to index and saves state machine snapshot.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}

	entry := rf.getLogEntry(index)
	if entry == nil {
		return
	}

	rf.lastIncludedTerm = entry.Term
	sliceIdx := index - rf.lastIncludedIndex

	// Retain remaining uncompacted log entries
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: index, Term: entry.Term, Command: nil})
	if sliceIdx+1 < len(rf.log) {
		newLog = append(newLog, rf.log[sliceIdx+1:]...)
	}

	rf.log = newLog
	rf.lastIncludedIndex = index

	// Persist metadata and snapshot
	state := PersistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Log:               rf.log,
	}
	stateBytes, _ := jsonMarshal(state)
	rf.persister.Save(stateBytes, snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// Compact log
	if args.LastIncludedIndex < rf.getLastLogIndex() {
		entry := rf.getLogEntry(args.LastIncludedIndex)
		if entry != nil && entry.Term == args.LastIncludedTerm {
			sliceIdx := args.LastIncludedIndex - rf.lastIncludedIndex
			newLog := make([]LogEntry, 0)
			newLog = append(newLog, LogEntry{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Command: nil})
			if sliceIdx+1 < len(rf.log) {
				newLog = append(newLog, rf.log[sliceIdx+1:]...)
			}
			rf.log = newLog
		} else {
			rf.log = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Command: nil}}
		}
	} else {
		rf.log = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Command: nil}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	state := PersistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Log:               rf.log,
	}
	stateBytes, _ := jsonMarshal(state)
	rf.persister.Save(stateBytes, args.Data)

	// Send snapshot on applyCh to update state machine
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() {
		rf.applyCh <- msg
	}()
}

func (rf *Raft) sendInstallSnapshotToPeer(peer int, term int, leaderID int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	ok := rf.peers[peer].CallInstallSnapshot(&args, &reply)
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

	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

