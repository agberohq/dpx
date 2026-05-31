package conductor

import (
	"context"
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
	"github.com/olekukonko/jack"
)

// raftInFlightLimit caps concurrent proposals in the distributed Raft pipeline.
// Sized to MaxAppendEntries (256): Raft pipelines at most that many log entries
// per AppendEntries RPC, so more in-flight proposals than this just piles up
// BoltDB writes without increasing commit throughput.
const raftInFlightLimit = 256

// raftAcquireTimeout is the maximum time a caller will wait for a semaphore
// slot before giving up. This prevents the infinite hang that occurred with
// context.Background() when Raft was slow or not-leader.
// 10s matches the raft.Apply timeout so callers never wait longer for admission
// than they would for the commit itself.
const raftAcquireTimeout = 10 * time.Second

// Node implements shared.Proposer using HashiCorp Raft.
type Node struct {
	raft      *hraft.Raft
	fsm       *dpxFSM
	transport hraft.Transport
	logDB     io.Closer
	stableDB  io.Closer
	dir       string

	// sem provides backpressure for distributed mode only.
	// Callers block here (CoDel-aware, priority-ordered) rather than
	// receiving errors when the BoltDB WAL pipeline is saturated.
	// Sized to raftInFlightLimit = MaxAppendEntries = 256.
	sem *jack.Semaphore
}

// Open creates a Raft node. Single-node embedded mode (empty Peers) uses
// directProposer / shardedDirectProposer. Multi-node uses full HashiCorp Raft.
func Open(cfg shared.Config, eng engine.StorageEngine, w shared.WatchNotifier) (shared.Proposer, error) {
	if cfg.NodeID == "" {
		cfg.NodeID = "1"
	}

	// Embedded single-node: bypass HashiCorp Raft entirely.
	// No semaphore — the directProposer channel + drain loop handles
	// backpressure and a semaphore adds measurable hot-path overhead.
	if len(cfg.Peers) == 0 {
		if isShardedEngine(eng) {
			return newShardedDirectProposer(eng, cfg.SyncPolicy, w, cfg.Metrics, cfg.Telemetry)
		}
		return newDirectProposer(eng, cfg.SyncPolicy, w, cfg.Metrics, cfg.Telemetry)
	}

	rCfg := hraft.DefaultConfig()
	rCfg.LocalID = hraft.ServerID(cfg.NodeID)
	if cfg.Logger != nil {
		rCfg.LogOutput = cfg.Logger
	} else {
		rCfg.LogOutput = io.Discard
	}

	rCfg.BatchApplyCh = true
	rCfg.CommitTimeout = 1 * time.Millisecond
	rCfg.HeartbeatTimeout = 50 * time.Millisecond
	rCfg.ElectionTimeout = 50 * time.Millisecond
	rCfg.LeaderLeaseTimeout = 40 * time.Millisecond
	rCfg.MaxAppendEntries = 256

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

	var (
		transport    hraft.Transport
		logStore     hraft.LogStore
		stableStore  hraft.StableStore
		logCloser    io.Closer
		stableCloser io.Closer
	)

	{
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

	var servers []hraft.Server
	for id, addr := range cfg.Peers {
		servers = append(servers, hraft.Server{
			ID:      hraft.ServerID(id),
			Address: hraft.ServerAddress(addr),
		})
	}
	r.BootstrapCluster(hraft.Configuration{Servers: servers})

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
		sem: jack.NewSemaphore(
			raftInFlightLimit,
			jack.SemaphoreWithTargetSojourn(5*time.Millisecond),
			jack.SemaphoreWithMaxSojourn(raftAcquireTimeout),
		),
	}, nil
}

// isShardedEngine reports whether eng is a sharded memory engine.
func isShardedEngine(eng engine.StorageEngine) bool {
	type shardedMarker interface {
		IsSharded() bool
	}
	if s, ok := eng.(shardedMarker); ok {
		return s.IsSharded()
	}
	return false
}

func snapshotDir(cfg shared.Config) (string, error) {
	if cfg.RaftDir != "" {
		if err := os.MkdirAll(cfg.RaftDir, 0755); err != nil {
			return "", err
		}
		return cfg.RaftDir, nil
	}
	dir, err := os.MkdirTemp("", "dpx-snap-"+cfg.NodeID+"-*")
	return dir, err
}

// Propose submits data to the Raft cluster with backpressure.
//
// Admission control: callers block in jack's CoDel-aware semaphore rather
// than hammering raft.Apply with more proposals than the WAL pipeline can
// absorb. A bounded timeout prevents infinite waits if Raft is unhealthy.
//
// Leader check is intentionally done after acquiring the semaphore slot:
// checking before acquiring creates a TOCTOU race where the node can lose
// leadership between the check and raft.Apply. Letting raft.Apply return
// ErrNotLeader is the correct single-check point.
func (n *Node) Propose(data []byte) (shared.ApplyResult, error) {
	// Acquire with a timeout so callers never block forever if Raft is
	// stuck (e.g. no quorum, leadership lost during warmup/seeding).
	ctx, cancel := context.WithTimeout(context.Background(), raftAcquireTimeout)
	defer cancel()

	if err := n.sem.Acquire(ctx, jack.PriorityHigh); err != nil {
		// sem.Close() on Shutdown, or timeout waiting for a slot.
		return shared.ApplyResult{}, fmt.Errorf("raft: admission: %w", err)
	}
	defer n.sem.Release()

	future := n.raft.Apply(data, raftAcquireTimeout)
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
	// Close the semaphore first to unblock goroutines waiting in Acquire.
	n.sem.Close()
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
