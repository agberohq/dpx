package raft

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/olekukonko/hlc"
)

// Node implements shared.Proposer using HashiCorp Raft.
type Node struct {
	raft      *hraft.Raft
	fsm       *dpxFSM
	transport hraft.Transport
	logDB     io.Closer // *raftboltdb.BoltStore or nil (inmem)
	stableDB  io.Closer // *raftboltdb.BoltStore or nil (inmem)
	dir       string
}

// Open creates a Raft node. Single-node embedded mode (empty Peers) uses
// in-memory transport and log/stable stores — no TCP, no BoltDB, no disk I/O
// on the Raft path. Multi-node sets ListenAddr + Peers and uses TCP + BoltDB.
func Open(cfg shared.Config, eng engine.StorageEngine, w shared.WatchNotifier) (shared.Proposer, error) {
	if cfg.NodeID == "" {
		cfg.NodeID = "1"
	}

	rCfg := hraft.DefaultConfig()
	rCfg.LocalID = hraft.ServerID(cfg.NodeID)
	if cfg.Logger != nil {
		rCfg.LogOutput = cfg.Logger
	} else {
		rCfg.LogOutput = io.Discard
	}

	// Tuning for embedded single-process use.
	// BatchApplyCh: raft collects all pending Apply() calls and delivers them
	// to ApplyBatch() in one shot — our FSM handles this natively.
	// CommitTimeout: how long the leader waits before flushing a partial batch.
	// 1ms instead of 50ms default = 50x less idle latency under low concurrency.
	rCfg.BatchApplyCh = true
	rCfg.CommitTimeout = 1 * time.Millisecond
	rCfg.HeartbeatTimeout = 50 * time.Millisecond
	rCfg.ElectionTimeout = 50 * time.Millisecond
	rCfg.LeaderLeaseTimeout = 40 * time.Millisecond // must be <= HeartbeatTimeout
	rCfg.MaxAppendEntries = 256

	// FSM owns its own HLC clock — no shared.Clock interface needed.
	clock := hlc.NewClock()
	f := newFSM(eng, cfg.SyncPolicy, w, cfg.Metrics, clock)
	if _, err := f.open(nil); err != nil {
		return nil, fmt.Errorf("raft: fsm open: %w", err)
	}

	snapDir, err := snapshotDir(cfg)
	if err != nil {
		return nil, fmt.Errorf("raft: snapshot dir: %w", err)
	}
	snap, err := hraft.NewFileSnapshotStore(snapDir, 3, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("raft: snapshot store: %w", err)
	}

	// Embedded single-node: bypass Raft entirely.
	// directProposer gives the same OCC semantics with zero channel overhead.
	if len(cfg.Peers) == 0 {
		return newDirectProposer(eng, cfg.SyncPolicy, w, cfg.Metrics, cfg.Telemetry)
	}

	var (
		transport    hraft.Transport
		logStore     hraft.LogStore
		stableStore  hraft.StableStore
		logCloser    io.Closer
		stableCloser io.Closer
	)

	{
		// ── Multi-node: TCP transport + BoltDB persistent stores ──
		listenAddr := cfg.ListenAddr
		if listenAddr == "" {
			listenAddr = "127.0.0.1:0"
		}
		addr, err := net.ResolveTCPAddr("tcp", listenAddr)
		if err != nil {
			return nil, fmt.Errorf("raft: resolve addr: %w", err)
		}
		t, tErr := hraft.NewTCPTransport(listenAddr, addr, 3, 10*time.Second, io.Discard)
		if tErr != nil {
			return nil, fmt.Errorf("raft: transport: %w", tErr)
		}
		transport = t

		raftDir := cfg.RaftDir
		if raftDir == "" {
			raftDir = filepath.Join(os.TempDir(), "dpx-"+cfg.NodeID)
		}
		if err := os.MkdirAll(raftDir, 0755); err != nil {
			if c, ok := transport.(io.Closer); ok {
				c.Close()
			}
			return nil, fmt.Errorf("raft: mkdir: %w", err)
		}

		lb, lErr := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.db"))
		if lErr != nil {
			if c, ok := transport.(io.Closer); ok {
				c.Close()
			}
			return nil, fmt.Errorf("raft: log store: %w", lErr)
		}
		sb, sErr := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.db"))
		if sErr != nil {
			lb.Close()
			if c, ok := transport.(io.Closer); ok {
				c.Close()
			}
			return nil, fmt.Errorf("raft: stable store: %w", sErr)
		}
		logStore = lb
		stableStore = sb
		logCloser = lb
		stableCloser = sb
	}

	r, err := hraft.NewRaft(rCfg, f, logStore, stableStore, snap, transport)
	if err != nil {
		if c, ok := transport.(io.Closer); ok {
			c.Close()
		}
		if logCloser != nil {
			logCloser.Close()
		}
		if stableCloser != nil {
			stableCloser.Close()
		}
		return nil, fmt.Errorf("raft: new raft: %w", err)
	}

	// Bootstrap — multi-node only (embedded returns early above).
	var servers []hraft.Server
	for id, addr := range cfg.Peers {
		servers = append(servers, hraft.Server{
			ID:      hraft.ServerID(id),
			Address: hraft.ServerAddress(addr),
		})
	}
	r.BootstrapCluster(hraft.Configuration{Servers: servers})

	// Wait for leader election. Single-node: ~few ms. Multi-node: up to 5s timeout.
	select {
	case <-r.LeaderCh():
	case <-time.After(5 * time.Second):
	}

	return &Node{
		raft:      r,
		fsm:       f,
		transport: transport,
		logDB:     logCloser,
		stableDB:  stableCloser,
	}, nil
}

func snapshotDir(cfg shared.Config) (string, error) {
	if cfg.RaftDir != "" {
		if err := os.MkdirAll(cfg.RaftDir, 0755); err != nil {
			return "", err
		}
		return cfg.RaftDir, nil
	}
	// Embedded mode: unique temp dir per node instance.
	dir, err := os.MkdirTemp("", "dpx-snap-"+cfg.NodeID+"-*")
	return dir, err
}

func (n *Node) Propose(data []byte) (shared.ApplyResult, error) {
	if n.raft.State() != hraft.Leader {
		return shared.ApplyResult{}, fmt.Errorf("dpx/raft: not the leader")
	}
	future := n.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return shared.ApplyResult{}, fmt.Errorf("raft: apply: %w", err)
	}
	resp := future.Response()
	result, ok := resp.(shared.ApplyResult)
	if !ok {
		return shared.ApplyResult{}, fmt.Errorf("raft: unexpected response type: %T", resp)
	}
	return result, nil
}

func (n *Node) Shutdown() error {
	if err := n.raft.Shutdown().Error(); err != nil {
		return fmt.Errorf("raft: shutdown: %w", err)
	}
	if closer, ok := n.transport.(io.Closer); ok {
		closer.Close()
	}
	if n.stableDB != nil {
		n.stableDB.Close()
	}
	if n.logDB != nil {
		n.logDB.Close()
	}
	return nil
}

func (n *Node) Leader() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

func (n *Node) State() string {
	return n.raft.State().String()
}
