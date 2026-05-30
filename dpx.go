// Package dpx provides a strictly-serializable embedded key-value store
// backed by Raft consensus and Pebble storage.
package dpx

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
	"github.com/olekukonko/hlc"
	"github.com/olekukonko/jack"
	"github.com/vmihailenco/msgpack/v5"
)

// KVStore is the public interface exposed by a DPX Node.
type KVStore interface {
	RunInTx(ctx context.Context, fn func(tx KVTx) error) error
	GetRange(ctx context.Context, start, end []byte, limit int) ([]engine.KVPair, error)
	GetRangeReverse(ctx context.Context, start, end []byte, limit int) ([]engine.KVPair, error)
	WatchKey(ctx context.Context, prefix []byte) (<-chan struct{}, error)
	GetDatabaseTime(ctx context.Context) (time.Time, error)
	GetSafeReadPoint(ctx context.Context) (uint64, error)
	Close() error
}

// KVTx is the transaction handle passed to RunInTx callbacks.
type KVTx interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	AtomicAdd(ctx context.Context, key []byte, delta int64) (int64, error)
	GetRange(ctx context.Context, start, end []byte, limit int) ([]engine.KVPair, error)
	AllocateNextSequence(ctx context.Context) (uint64, error)
}

// Node is a DPX consensus node. Safe for concurrent use after Open().
type Node struct {
	proposer shared.Proposer
	engine   engine.StorageEngine
	batcher  *Batcher
	retry    *jack.Retry
	shutdown *jack.Shutdown
	watchers *watcherMap
	metrics  *Metrics
	clock    *hlc.Clock
	closed   atomic.Bool
}

// Open constructs and starts a DPX Node.
//
// newProposer is the Raft factory — pass dpxraft.Open:
//
//	import dpxraft "github.com/agberohq/dpx/raft"
//	node, err := dpx.Open(cfg, dpxraft.Open)
func Open(cfg Config, newProposer shared.ProposerFactory) (*Node, error) {
	if cfg.Engine == nil {
		return nil, fmt.Errorf("dpx: Config.Engine is required")
	}
	applyDefaults(&cfg)

	if err := cfg.Engine.Open(); err != nil {
		return nil, fmt.Errorf("dpx: engine open: %w", err)
	}

	watchers := newWatcherMap()

	sharedCfg := cfg.toShared()
	proposer, err := newProposer(sharedCfg, cfg.Engine, watchers)
	if err != nil {
		cfg.Engine.Close()
		return nil, fmt.Errorf("dpx: proposer: %w", err)
	}

	batcher := newBatcher(cfg.Batch)

	n := &Node{
		proposer: proposer,
		engine:   cfg.Engine,
		batcher:  batcher,
		watchers: watchers,
		metrics:  cfg.Metrics,
		clock:    hlc.NewClock(),
	}

	n.retry = jack.NewRetry(
		jack.RetryWithMaxAttempts(cfg.Retry.MaxAttempts),
		jack.RetryWithBaseDelay(cfg.Retry.BaseDelay),
		jack.RetryWithMaxDelay(cfg.Retry.MaxDelay),
		jack.RetryWithMultiplier(cfg.Retry.Multiplier),
		jack.RetryWithJitter(true),
		jack.RetryWithRetryIf(func(err error) bool {
			return errors.Is(err, ErrConflict)
		}),
		jack.RetryWithOnRetry(func(attempt int, err error) {
			if n.metrics != nil {
				n.metrics.ConflictTotal.Add(1)
			}
		}),
	)

	sd := jack.NewShutdown(jack.ShutdownWithTimeout(cfg.ShutdownTimeout))
	mustRegister := func(name string, pri int, fn func(context.Context) error) {
		if err := sd.RegisterWithPriority(name, pri, fn); err != nil {
			panic(fmt.Sprintf("dpx: register shutdown %q: %v", name, err))
		}
	}
	mustRegister("raft", 0, func(_ context.Context) error { return proposer.Shutdown() })
	mustRegister("watchers", 1, func(_ context.Context) error { watchers.closeAll(); return nil })
	mustRegister("engine", 2, func(_ context.Context) error { return cfg.Engine.Close() })
	n.shutdown = sd

	return n, nil
}

// RunInTx executes fn in a strictly-serializable transaction.
func (n *Node) RunInTx(ctx context.Context, fn func(tx KVTx) error) error {
	if n.closed.Load() {
		return ErrStoreClosed
	}
	err := n.retry.Do(ctx, func(ctx context.Context) error {
		return n.runOnce(ctx, fn)
	})
	if errors.Is(err, jack.ErrRetryExhausted) {
		if n.metrics != nil {
			n.metrics.ConflictExhausted.Add(1)
		}
		return ErrConflictExhausted
	}
	return err
}

func (n *Node) runOnce(ctx context.Context, fn func(KVTx) error) error {
	snap, err := n.engine.GetSnapshot()
	if err != nil {
		return fmt.Errorf("dpx: GetSnapshot: %w", err)
	}
	defer snap.Close()

	tx := &dpxTx{
		snap:    snap,
		readSet: make(map[string]shared.ReadEntry, 8),
		writes:  make([]shared.WriteEntry, 0, 8),
	}

	fnErr := fn(tx)

	if fnErr != nil {
		return fnErr
	}
	if tx.empty() {
		return nil
	}
	if err := tx.validate(); err != nil {
		return err
	}

	ts := n.clock.Tick()
	data, err := msgpack.Marshal(&shared.Proposal{
		ReadSet:          tx.readSetSlice(),
		Writes:           tx.writes,
		TimestampWall:    ts.Wall,
		TimestampCounter: ts.Counter,
	})
	if err != nil {
		return fmt.Errorf("dpx: marshal: %w", err)
	}

	if err := n.batcher.Wait(ctx, len(data)); err != nil {
		return err
	}

	start := time.Now()
	res, err := n.proposer.Propose(data)
	if err != nil {
		return err
	}
	if n.metrics != nil {
		n.metrics.RaftCommitDurationNs.Add(time.Since(start).Nanoseconds())
	}

	if res.Err != nil {
		return res.Err
	}
	if res.Conflict {
		return ErrConflict
	}
	return nil
}

// GetRange performs a forward range scan outside a transaction.
func (n *Node) GetRange(_ context.Context, start, end []byte, limit int) ([]engine.KVPair, error) {
	if n.closed.Load() {
		return nil, ErrStoreClosed
	}
	snap, err := n.engine.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Close()
	return collectIter(snap.NewIter(start, end), limit, false)
}

// GetRangeReverse performs a reverse range scan outside a transaction.
func (n *Node) GetRangeReverse(_ context.Context, start, end []byte, limit int) ([]engine.KVPair, error) {
	if n.closed.Load() {
		return nil, ErrStoreClosed
	}
	snap, err := n.engine.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Close()
	return collectIter(snap.NewIter(start, end), limit, true)
}

// WatchKey registers a watch on a key prefix. Best-effort delivery.
func (n *Node) WatchKey(ctx context.Context, prefix []byte) (<-chan struct{}, error) {
	if n.closed.Load() {
		return nil, ErrStoreClosed
	}
	return n.watchers.register(ctx, string(prefix)), nil
}

// GetDatabaseTime returns the current HLC timestamp as a physical time.Time.
func (n *Node) GetDatabaseTime(_ context.Context) (time.Time, error) {
	return n.clock.Now().Physical(), nil
}

// GetSafeReadPoint returns the last Raft log index applied to this node.
func (n *Node) GetSafeReadPoint(_ context.Context) (uint64, error) {
	return n.engine.CurrentSequence(), nil
}

// Backup writes a consistent engine checkpoint to destDir.
func (n *Node) Backup(_ context.Context, destDir string) error {
	if n.closed.Load() {
		return ErrStoreClosed
	}
	if err := n.engine.Sync(); err != nil {
		return fmt.Errorf("dpx: Backup Sync: %w", err)
	}
	if err := n.engine.CreateCheckpoint(destDir); err != nil {
		return fmt.Errorf("dpx: Backup checkpoint: %w", err)
	}
	if n.metrics != nil {
		n.metrics.BackupTotal.Add(1)
	}
	return nil
}

// SetBatchConfig replaces the BatchConfig atomically.
func (n *Node) SetBatchConfig(cfg BatchConfig) {
	n.batcher.SetConfig(cfg)
}

// Close gracefully shuts down the node (Raft -> watchers -> engine).
func (n *Node) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}
	stats := n.shutdown.TriggerShutdown()
	if len(stats.Errors) > 0 {
		return stats.Errors[0]
	}
	return nil
}
