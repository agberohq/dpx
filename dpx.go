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
	proposer  shared.Proposer
	engine    engine.StorageEngine
	batcher   *Batcher
	retry     *jack.Retry
	shutdown  *jack.Shutdown
	watchers  *watcherMap
	metrics   *Metrics
	telemetry *shared.Telemetry
	clock     *hlc.Clock
	closed    atomic.Bool
}

// Open constructs and starts a DPX Node.
func Open(cfg Config, newProposer shared.ProposerFactory) (*Node, error) {
	if cfg.Engine == nil {
		return nil, fmt.Errorf("dpx: Config.Engine is required")
	}
	applyDefaults(&cfg)

	// Timer: EngineOpen
	var engineOpenStart time.Time
	if cfg.Telemetry != nil {
		engineOpenStart = time.Now()
	}
	if err := cfg.Engine.Open(); err != nil {
		return nil, fmt.Errorf("dpx: engine open: %w", err)
	}
	if cfg.Telemetry != nil {
		cfg.Telemetry.EngineOpen.Record(time.Since(engineOpenStart))
	}

	// Wire telemetry into the engine for internal stage recording
	if cfg.Telemetry != nil {
		cfg.Engine.SetTelemetry(cfg.Telemetry)
	}

	watchers := newWatcherMap(cfg.Telemetry)
	sharedCfg := cfg.toShared()

	// Timer: RaftBootstrap
	var raftStart time.Time
	if cfg.Telemetry != nil {
		raftStart = time.Now()
	}
	proposer, err := newProposer(sharedCfg, cfg.Engine, watchers)
	if err != nil {
		cfg.Engine.Close()
		return nil, fmt.Errorf("dpx: proposer: %w", err)
	}
	if cfg.Telemetry != nil {
		cfg.Telemetry.RaftBootstrap.Record(time.Since(raftStart))
	}

	batcher := newBatcher(cfg.Batch, cfg.Telemetry)

	n := &Node{
		proposer:  proposer,
		engine:    cfg.Engine,
		batcher:   batcher,
		watchers:  watchers,
		metrics:   cfg.Metrics,
		telemetry: cfg.Telemetry,
		clock:     hlc.NewClock(),
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

	// Timer: ShutdownRaft
	mustRegister("raft", 0, func(_ context.Context) error {
		if n.telemetry != nil {
			t0 := time.Now()
			defer func() { n.telemetry.ShutdownRaft.Record(time.Since(t0)) }()
		}
		return proposer.Shutdown()
	})

	mustRegister("watchers", 1, func(_ context.Context) error { watchers.closeAll(); return nil })

	// Timer: ShutdownEngine
	mustRegister("engine", 2, func(_ context.Context) error {
		if n.telemetry != nil {
			t0 := time.Now()
			defer func() { n.telemetry.ShutdownEngine.Record(time.Since(t0)) }()
		}
		return cfg.Engine.Close()
	})

	n.shutdown = sd
	return n, nil
}

// RunInTx executes fn in a strictly-serializable transaction.
func (n *Node) RunInTx(ctx context.Context, fn func(tx KVTx) error) error {
	if n.closed.Load() {
		return ErrStoreClosed
	}

	var totalExec time.Duration
	var retryStart time.Time
	if n.telemetry != nil {
		retryStart = time.Now()
	}

	err := n.retry.Do(ctx, func(ctx context.Context) error {
		t0 := time.Now()
		defer func() { totalExec += time.Since(t0) }()
		return n.runOnce(ctx, fn)
	})

	// Record retry backoff = wall clock time minus actual execution time
	if n.telemetry != nil {
		if backoff := time.Since(retryStart) - totalExec; backoff > 0 {
			n.telemetry.RetryBackoff.Record(backoff)
		}
	}

	if errors.Is(err, jack.ErrRetryExhausted) {
		if n.metrics != nil {
			n.metrics.ConflictExhausted.Add(1)
		}
		return ErrConflictExhausted
	}
	return err
}

func (n *Node) runOnce(ctx context.Context, fn func(KVTx) error) error {
	t0 := time.Now()

	// Timer: SnapshotCreate (Engine-specific)
	var snapStart time.Time
	if n.telemetry != nil {
		snapStart = time.Now()
	}
	snap, err := n.engine.GetSnapshot()
	if err != nil {
		return fmt.Errorf("dpx: GetSnapshot: %w", err)
	}
	if n.telemetry != nil {
		n.telemetry.SnapshotCreate.Record(time.Since(snapStart))
	}
	// Existing timer: GetSnapshot (covers dpx overhead + engine call)
	if n.telemetry != nil {
		n.telemetry.GetSnapshot.Record(time.Since(t0))
	}

	defer snap.Close()

	tx := &dpxTx{
		snap:      snap,
		readSet:   make(map[string]shared.ReadEntry, 8),
		writes:    make([]shared.WriteEntry, 0, 8),
		telemetry: n.telemetry,
	}

	t1 := time.Now()
	fnErr := fn(tx)
	if n.telemetry != nil {
		n.telemetry.Speculate.Record(time.Since(t1))
	}
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
	proposal := &shared.Proposal{
		ReadSet:          tx.readSetSlice(),
		Writes:           tx.writes,
		TimestampWall:    ts.Wall,
		TimestampCounter: ts.Counter,
	}

	t3 := time.Now()
	var res shared.ApplyResult
	if dp, ok := n.proposer.(shared.DirectProposer); ok {
		res, err = dp.ProposeDirect(proposal)
	} else {
		t2 := time.Now()
		var data []byte
		data = proposal.Marshal()
		//if err != nil {
		//	return fmt.Errorf("dpx: marshal: %w", err)
		//}
		if n.telemetry != nil {
			n.telemetry.Marshal.Record(time.Since(t2))
		}

		if err := n.batcher.Wait(ctx, len(data)); err != nil {
			return err
		}

		res, err = n.proposer.Propose(data)
	}
	if err != nil {
		return err
	}
	if n.telemetry != nil {
		n.telemetry.Propose.Record(time.Since(t3))
	}
	if n.metrics != nil {
		n.metrics.RaftCommitDurationNs.Add(time.Since(t3).Nanoseconds())
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

	// Timer: EngineSync
	var syncStart time.Time
	if n.telemetry != nil {
		syncStart = time.Now()
	}
	if err := n.engine.Sync(); err != nil {
		return fmt.Errorf("dpx: Backup Sync: %w", err)
	}
	if n.telemetry != nil {
		n.telemetry.EngineSync.Record(time.Since(syncStart))
	}

	// Timer: SnapshotCreate (Backup path)
	var snapStart time.Time
	if n.telemetry != nil {
		snapStart = time.Now()
	}
	if err := n.engine.CreateCheckpoint(destDir); err != nil {
		return fmt.Errorf("dpx: Backup checkpoint: %w", err)
	}
	if n.telemetry != nil {
		n.telemetry.SnapshotCreate.Record(time.Since(snapStart))
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

// Close gracefully shuts down the node.
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
