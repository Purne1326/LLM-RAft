package raft

import "encoding/json"

// ClientTransport abstracts the communication between Raft peers.
type ClientTransport interface {
	CallRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) bool
	CallAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) bool
	CallInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool
}

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

