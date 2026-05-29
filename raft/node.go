package raft

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/agberohq/dpx"
	"github.com/agberohq/dpx/engine"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// Node implements dpx.Proposer using Hashicorp's Raft library.
type Node struct {
	raft          *raft.Raft
	fsm           *fsm
	transport     raft.Transport
	logDB         *raftboltdb.BoltStore
	stableDB      *raftboltdb.BoltStore
	snapshotStore raft.SnapshotStore
	dir           string
}

// Open creates a new Raft node and returns it as a dpx.Proposer.
func Open(cfg dpx.Config, eng engine.StorageEngine, w dpx.WatchNotifier) (dpx.Proposer, error) {
	if cfg.NodeID == "" {
		cfg.NodeID = "1"
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0"
	}

	raftDir := cfg.RaftDir
	if raftDir == "" {
		raftDir = filepath.Join(os.TempDir(), "dpx-"+cfg.NodeID)
	}

	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("raft: mkdir %s: %w", raftDir, err)
	}

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(cfg.NodeID)
	if cfg.Logger != nil {
		config.LogOutput = cfg.Logger.Writer()
	} else {
		config.LogOutput = io.Discard
	}

	// Create the FSM.
	f := newFSM(eng, w)

	// Create log and stable stores.
	logDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("raft: new log store: %w", err)
	}

	stableDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.db"))
	if err != nil {
		logDB.Close()
		return nil, fmt.Errorf("raft: new stable store: %w", err)
	}

	// Create snapshot store.
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		stableDB.Close()
		logDB.Close()
		return nil, fmt.Errorf("raft: snapshot store: %w", err)
	}

	// Setup transport.
	addr, err := net.ResolveTCPAddr("tcp", cfg.ListenAddr)
	if err != nil {
		snapshotStore.Close()
		stableDB.Close()
		logDB.Close()
		return nil, fmt.Errorf("raft: resolve addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.ListenAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		snapshotStore.Close()
		stableDB.Close()
		logDB.Close()
		return nil, fmt.Errorf("raft: transport: %w", err)
	}

	// Create Raft node.
	r, err := raft.NewRaft(config, f, logDB, stableDB, snapshotStore, transport)
	if err != nil {
		transport.Close()
		snapshotStore.Close()
		stableDB.Close()
		logDB.Close()
		return nil, fmt.Errorf("raft: new raft: %w", err)
	}

	// Bootstrap if peers are configured.
	if len(cfg.Peers) > 0 {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for id, addr := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: raft.ServerAddress(addr),
			})
		}
		configuration := raft.Configuration{Servers: servers}
		r.BootstrapCluster(configuration)
	}

	return &Node{
		raft:          r,
		fsm:           f,
		transport:     transport,
		logDB:         logDB,
		stableDB:      stableDB,
		snapshotStore: snapshotStore,
		dir:           raftDir,
	}, nil
}

// Propose submits a command to the Raft cluster and waits for it to be applied.
func (n *Node) Propose(data []byte) (dpx.ApplyResult, error) {
	if n.raft.State() != raft.Leader {
		return dpx.ApplyResult{}, dpx.ErrNotLeader
	}

	future := n.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return dpx.ApplyResult{}, fmt.Errorf("raft: apply: %w", err)
	}

	resp := future.Response()
	result, ok := resp.(dpx.ApplyResult)
	if !ok {
		return dpx.ApplyResult{}, fmt.Errorf("raft: unexpected response type: %T", resp)
	}
	return result, nil
}

// Shutdown gracefully stops the Raft node and cleans up resources.
func (n *Node) Shutdown() error {
	// Shutdown Raft.
	future := n.raft.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft: shutdown: %w", err)
	}

	// Close transport using io.Closer interface if available.
	if closer, ok := n.transport.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("raft: transport close: %w", err)
		}
	}

	// Close snapshot store.
	if err := n.snapshotStore.Close(); err != nil {
		return fmt.Errorf("raft: snapshot store close: %w", err)
	}

	// Close databases.
	if err := n.stableDB.Close(); err != nil {
		return fmt.Errorf("raft: stable store close: %w", err)
	}
	if err := n.logDB.Close(); err != nil {
		return fmt.Errorf("raft: log store close: %w", err)
	}

	return nil
}

// Leader returns the current leader's address.
func (n *Node) Leader() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// State returns the current Raft state.
func (n *Node) State() string {
	return n.raft.State().String()
}
