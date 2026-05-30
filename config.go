package dpx

import (
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
	"github.com/olekukonko/ll"
)

// SyncPolicy controls WAL durability. Alias of shared.SyncPolicy so callers
// use dpx.SyncBatch etc. without importing internal/shared.
type SyncPolicy = shared.SyncPolicy

const (
	SyncBatch = shared.SyncBatch
	SyncFull  = shared.SyncFull
	SyncNone  = shared.SyncNone
)

// Metrics is the shared atomic counter struct. Alias so callers use dpx.Metrics.
type Metrics = shared.Metrics

// BatchConfig controls the adaptive flush delay in the Batcher.
type BatchConfig struct {
	MaxEntries int
	MaxBytes   int64
	MinAge     time.Duration
	MaxAge     time.Duration
	EMAAlpha   float64
	K          float64
}

// RetryConfig controls jack.Retry for OCC conflict retries.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// Config is passed to dpx.Open. Engine is the only required field.
type Config struct {
	NodeID string
	Engine engine.StorageEngine

	SyncPolicy SyncPolicy

	// Network — leave empty for single-node embedded mode (uses in-memory transport).
	// Set ListenAddr + Peers for a multi-node cluster (uses TCP transport + BoltDB).
	ListenAddr string
	Peers      map[string]string
	RaftDir    string

	Batch           BatchConfig
	Retry           RetryConfig
	ShutdownTimeout time.Duration

	Metrics   *Metrics
	Telemetry *shared.Telemetry // nil = disabled; captures per-stage latency

	// Logger is the parent application's logger (e.g. Teller's ll.Logger).
	// Raft internals write to it. nil = discard.
	Logger *ll.Logger
}

// toShared builds the shared.Config for ProposerFactory.
// No type conversions needed — SyncPolicy is an alias, Metrics is an alias.
func (c Config) toShared() shared.Config {
	return shared.Config{
		NodeID:          c.NodeID,
		ListenAddr:      c.ListenAddr,
		Peers:           c.Peers,
		RaftDir:         c.RaftDir,
		Engine:          c.Engine,
		SyncPolicy:      c.SyncPolicy,
		ShutdownTimeout: c.ShutdownTimeout,
		Metrics:         c.Metrics,
		Telemetry:       c.Telemetry,
		Logger:          llWriter(c.Logger),
	}
}

func llWriter(logger *ll.Logger) interface{ Write([]byte) (int, error) } {
	if logger == nil {
		return noopWriter{}
	}
	return &llWriterAdapter{l: logger}
}

type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }

type llWriterAdapter struct{ l *ll.Logger }

func (w *llWriterAdapter) Write(p []byte) (n int, err error) {
	w.l.Info(string(p))
	return len(p), nil
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
