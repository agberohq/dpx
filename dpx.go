// Package dpx provides a strictly-serializable embedded key-value store
// backed by Raft consensus and Pebble storage.
//
// The Raft implementation lives in the raft/ subdirectory. This package
// contains everything that is independent of the Raft library: the
// transaction model, batching, watchers, encoding, and the public API.
//
//	node, err := dpx.Open(cfg, dpxraft.Open)
//	if err != nil { log.Fatal(err) }
//	defer node.Close()
//
//	err = node.RunInTx(ctx, func(tx dpx.KVTx) error {
//	    val, err := tx.Get(ctx, []byte("key"))
//	    if err != nil { return err }
//	    return tx.Set(ctx, []byte("key"), newVal)
//	})
package dpx

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/olekukonko/jack"
	"github.com/vmihailenco/msgpack/v5"
)

// Proposer is the interface the raft/ package satisfies.
// Node calls Propose for every committed RunInTx attempt.
// The interface keeps the root package free of any Raft library import.
type Proposer interface {
	Propose(data []byte) (ApplyResult, error)
	Shutdown() error
}

// ApplyResult is the value the FSM returns to the proposing goroutine.
// Exported so raft/ can construct it without an import cycle.
type ApplyResult struct {
	Conflict bool  // OCC conflict detected; Node maps this to ErrConflict
	Err      error // fatal FSM error; surfaced directly to the caller
}

// WatchNotifier is the interface the watcherMap satisfies.
// Exported so raft/fsm.go can call NotifyBatch without importing watch.go
// internals.
type WatchNotifier interface {
	NotifyBatch(writes []WriteEntry, metrics *Metrics)
}

// ProposerFactory is the function signature callers pass to Open.
// Typically: dpxraft.Open
type ProposerFactory func(cfg Config, eng engine.StorageEngine, w WatchNotifier) (Proposer, error)

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
	proposer Proposer
	engine   engine.StorageEngine
	batcher  *Batcher
	retry    *jack.Retry
	shutdown *jack.Shutdown
	watchers *watcherMap
	metrics  *Metrics
	closed   atomic.Bool
}

// Open constructs and starts a DPX Node.
//
// newProposer is the Raft factory — pass dpxraft.Open:
//
//	import dpxraft "github.com/agberohq/dpx/raft"
//	node, err := dpx.Open(cfg, dpxraft.Open)
func Open(cfg Config, newProposer ProposerFactory) (*Node, error) {
	if cfg.Engine == nil {
		return nil, fmt.Errorf("dpx: Config.Engine is required")
	}
	applyDefaults(&cfg)

	if err := cfg.Engine.Open(); err != nil {
		return nil, fmt.Errorf("dpx: engine open: %w", err)
	}

	watchers := newWatcherMap()

	proposer, err := newProposer(cfg, cfg.Engine, watchers)
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

	// Shutdown priority:
	//   0: stop Raft — no new Propose calls after this
	//   1: close watchers — safe once Raft is stopped
	//   2: close engine — Pebble flush + close
	sd := jack.NewShutdown(jack.ShutdownWithTimeout(cfg.ShutdownTimeout))
	mustRegister := func(name string, pri int, fn func(context.Context) error) {
		if err := sd.RegisterWithPriority(name, pri, fn); err != nil {
			// RegisterWithPriority only fails if Shutdown already triggered;
			// that can't happen here since we just created sd.
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
// Retries automatically on OCC conflicts up to RetryConfig.MaxAttempts.
// fn must be idempotent.
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

// In runOnce method:
func (n *Node) runOnce(ctx context.Context, fn func(KVTx) error) error {
	snap, err := n.engine.GetSnapshot()
	if err != nil {
		return fmt.Errorf("dpx: GetSnapshot: %w", err)
	}
	defer snap.Close() // Moved defer before fn call

	tx := &dpxTx{
		snap:    snap,
		readSet: make(map[string]ReadEntry, 8),
		writes:  make([]WriteEntry, 0, 8),
	}

	fnErr := fn(tx)
	// snap.Close() removed from here

	if fnErr != nil {
		return fnErr
	}
	if tx.empty() {
		return nil
	}
	if err := tx.validate(); err != nil {
		return err
	}

	data, err := msgpack.Marshal(&Proposal{
		ReadSet: tx.readSetSlice(),
		Writes:  tx.writes,
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

// GetDatabaseTime returns the current wall-clock time. Advisory only.
func (n *Node) GetDatabaseTime(_ context.Context) (time.Time, error) {
	return time.Now(), nil
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

// Close gracefully shuts down the node (Raft → watchers → engine).
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
