package dpx

import (
	"sync/atomic"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/olekukonko/ll"
)

// SyncPolicy controls WAL durability on each committed Raft entry.
type SyncPolicy int

const (
	// SyncBatch fsyncs once per committed entry via engine.Sync().
	// Recommended default. Zero value so Config{} is safe without explicit set.
	SyncBatch SyncPolicy = iota

	// SyncFull fsyncs inside every engine.ApplyBatch call.
	// Maximum single-node durability; lowest throughput.
	SyncFull

	// SyncNone skips all fsyncs. Maximum throughput; local data loss on crash.
	// Raft quorum still provides replication safety across the cluster.
	SyncNone
)

// BatchConfig controls the adaptive flush delay.
type BatchConfig struct {
	MaxEntries int           // default: 256
	MaxBytes   int64         // default: 4 MiB
	MinAge     time.Duration // default: 50µs
	MaxAge     time.Duration // default: 2ms
	EMAAlpha   float64       // default: 0.1
	K          float64       // default: 8.0
}

// RetryConfig controls jack.Retry for OCC conflict retries.
type RetryConfig struct {
	MaxAttempts int           // default: 50
	BaseDelay   time.Duration // default: 1ms
	MaxDelay    time.Duration // default: 100ms
	Multiplier  float64       // default: 2.0
}

// Metrics holds atomic counters. Pass a non-nil *Metrics to Config to enable.
type Metrics struct {
	ConflictTotal        atomic.Uint64
	ConflictExhausted    atomic.Uint64
	WatchDropped         atomic.Uint64
	SnapshotSaveTotal    atomic.Uint64
	SnapshotRecoverTotal atomic.Uint64
	BackupTotal          atomic.Uint64

	ApplyDurationNs           atomic.Int64
	RaftCommitDurationNs      atomic.Int64
	KeyEpochRebuildDurationNs atomic.Int64
}

// Config is passed to Open(). Engine is the only required field.
type Config struct {
	// NodeID identifies this node in the Raft cluster. Default: "1".
	// Must be unique and stable across restarts.
	NodeID string

	// Engine is the storage backend. Required.
	Engine engine.StorageEngine

	// SyncPolicy controls WAL durability. Default: SyncBatch (zero value).
	SyncPolicy SyncPolicy

	// Network — leave empty for single-node embedded mode (no TCP socket opened).
	// ListenAddr is the Raft RPC address, e.g. "0.0.0.0:7001".
	// Peers maps NodeID → "host:port" for the initial cluster bootstrap.
	ListenAddr string
	Peers      map[string]string

	// RaftDir is where the Raft implementation stores its log and stable state.
	// Defaults to os.TempDir()/dpx-{NodeID}.
	RaftDir string

	// Tuning.
	Batch BatchConfig
	Retry RetryConfig

	// ShutdownTimeout is the max time jack.Shutdown waits for teardown.
	// Default: 30s.
	ShutdownTimeout time.Duration

	// Observability. nil = no collection.
	Metrics *Metrics
	Logger  *ll.Logger
}

func applyDefaults(cfg *Config) {
	if cfg.NodeID == "" {
		cfg.NodeID = "1"
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Batch.MaxEntries == 0 {
		cfg.Batch.MaxEntries = 256
	}
	if cfg.Batch.MaxBytes == 0 {
		cfg.Batch.MaxBytes = 4 << 20
	}
	if cfg.Batch.MinAge == 0 {
		cfg.Batch.MinAge = 50 * time.Microsecond
	}
	if cfg.Batch.MaxAge == 0 {
		cfg.Batch.MaxAge = 2 * time.Millisecond
	}
	if cfg.Batch.EMAAlpha == 0 {
		cfg.Batch.EMAAlpha = 0.1
	}
	if cfg.Batch.K == 0 {
		cfg.Batch.K = 8.0
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = 50
	}
	if cfg.Retry.BaseDelay == 0 {
		cfg.Retry.BaseDelay = time.Millisecond
	}
	if cfg.Retry.MaxDelay == 0 {
		cfg.Retry.MaxDelay = 100 * time.Millisecond
	}
	if cfg.Retry.Multiplier == 0 {
		cfg.Retry.Multiplier = 2.0
	}
}
