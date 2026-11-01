package raft

import (
	"os"
	"path/filepath"
	"sync"
)

// Persister abstracts persistent storage of Raft metadata, log, and snapshots.
type Persister interface {
	Save(raftState []byte, snapshot []byte) error
	ReadRaftState() []byte
	ReadSnapshot() []byte
	RaftStateSize() int
}

// InMemPersister is a fast thread-safe in-memory persister for tests.
type InMemPersister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

func NewInMemPersister() *InMemPersister {
	return &InMemPersister{}
}

func (p *InMemPersister) Save(raftState []byte, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = make([]byte, len(raftState))
	copy(p.raftState, raftState)
	if snapshot != nil {
		p.snapshot = make([]byte, len(snapshot))
		copy(p.snapshot, snapshot)
	}
	return nil
}

func (p *InMemPersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := make([]byte, len(p.raftState))
	copy(state, p.raftState)
	return state
}

func (p *InMemPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := make([]byte, len(p.snapshot))
	copy(snap, p.snapshot)
	return snap
}

func (p *InMemPersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

// FilePersister writes write-ahead state to disk and calls fsync for crash durability.
type FilePersister struct {
	mu        sync.Mutex
	dir       string
	stateFile string
	snapFile  string
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FilePersister{
		dir:       dir,
		stateFile: filepath.Join(dir, "raft_state.bin"),
		snapFile:  filepath.Join(dir, "snapshot.bin"),
	}, nil
}

func (fp *FilePersister) Save(raftState []byte, snapshot []byte) error {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Write raft state with explicit fsync
	tmpState := fp.stateFile + ".tmp"
	f, err := os.OpenFile(tmpState, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(raftState); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // fsync
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpState, fp.stateFile); err != nil {
		return err
	}

	if snapshot != nil {
		tmpSnap := fp.snapFile + ".tmp"
		sf, err := os.OpenFile(tmpSnap, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		if _, err := sf.Write(snapshot); err != nil {
			sf.Close()
			return err
		}
		if err := sf.Sync(); err != nil {
			sf.Close()
			return err
		}
		if err := sf.Close(); err != nil {
			return err
		}
		if err := os.Rename(tmpSnap, fp.snapFile); err != nil {
			return err
		}
	}
	return nil
}

func (fp *FilePersister) ReadRaftState() []byte {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	data, _ := os.ReadFile(fp.stateFile)
	return data
}

func (fp *FilePersister) ReadSnapshot() []byte {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	data, _ := os.ReadFile(fp.snapFile)
	return data
}

func (fp *FilePersister) RaftStateSize() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	info, err := os.Stat(fp.stateFile)
	if err != nil {
		return 0
	}
	return int(info.Size())
}

