package transport

import (
	"net"
	"net/rpc"
	"raft-kv/pkg/raft"
	"sync"
	"time"
)

type TCPPeerClient struct {
	addr    string
	timeout time.Duration
	client  *rpc.Client
	mu      sync.Mutex
}

func NewTCPPeerClient(addr string, timeout time.Duration) *TCPPeerClient {
	return &TCPPeerClient{addr: addr, timeout: timeout}
}

func (c *TCPPeerClient) ensureConnected() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return err
	}
	c.client = rpc.NewClient(conn)
	return nil
}

func (c *TCPPeerClient) CallRequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
	if err := c.ensureConnected(); err != nil {
		return false
	}
	call := c.client.Go("RaftRPC.RequestVote", args, reply, nil)
	select {
	case <-call.Done:
		return call.Error == nil
	case <-time.After(c.timeout):
		return false
	}
}

func (c *TCPPeerClient) CallAppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
	if err := c.ensureConnected(); err != nil {
		return false
	}
	call := c.client.Go("RaftRPC.AppendEntries", args, reply, nil)
	select {
	case <-call.Done:
		return call.Error == nil
	case <-time.After(c.timeout):
		return false
	}
}

func (c *TCPPeerClient) CallInstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) bool {
	if err := c.ensureConnected(); err != nil {
		return false
	}
	call := c.client.Go("RaftRPC.InstallSnapshot", args, reply, nil)
	select {
	case <-call.Done:
		return call.Error == nil
	case <-time.After(c.timeout):
		return false
	}
}

