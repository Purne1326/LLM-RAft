package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"raft-kv/pkg/kvstore"
	"raft-kv/pkg/raft"
	"raft-kv/pkg/transport"
	"strings"
	"syscall"
	"time"
)

type RaftRPC struct {
	rf *raft.Raft
}

func (r *RaftRPC) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	r.rf.RequestVote(args, reply)
	return nil
}

func (r *RaftRPC) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	r.rf.AppendEntries(args, reply)
	return nil
}

func (r *RaftRPC) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	r.rf.InstallSnapshot(args, reply)
	return nil
}

func main() {
	id := flag.Int("id", 0, "Node ID (0-indexed)")
	listenAddr := flag.String("listen", ":8001", "TCP listen address for RPCs")
	cluster := flag.String("cluster", ":8001,:8002,:8003", "Comma-separated peer addresses")
	dataDir := flag.String("data", "./data/node0", "Directory for persistent WAL and state")
	flag.Parse()

	peerAddrs := strings.Split(*cluster, ",")
	n := len(peerAddrs)
	if *id < 0 || *id >= n {
		log.Fatalf("Invalid node ID %d for cluster size %d", *id, n)
	}

	peerClients := make([]raft.ClientTransport, n)
	for i, addr := range peerAddrs {
		if i == *id {
			peerClients[i] = nil
		} else {
			peerClients[i] = transport.NewTCPPeerClient(addr, 500*time.Millisecond)
		}
	}

	persister, err := raft.NewFilePersister(*dataDir)
	if err != nil {
		log.Fatalf("Failed to initialize file persister: %v", err)
	}

	cfg := raft.DefaultConfig()
	server := kvstore.StartKVServer(peerClients, *id, persister, 1024*1024, cfg)

	// Register RPC server
	rpcServer := rpc.NewServer()
	rpcWrapper := &RaftRPC{rf: server.Raft()}
	_ = rpcServer.Register(rpcWrapper)

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *listenAddr, err)
	}
	defer listener.Close()

	fmt.Printf("[Raft Node %d] Started. Listening on %s. Persisting to %s\n", *id, *listenAddr, *dataDir)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go rpcServer.ServeConn(conn)
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down Raft node...")
	server.Kill()
}

