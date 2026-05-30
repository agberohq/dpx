// Package shared holds types used by both dpx and raft.
// It breaks the import cycle: raft implements Proposer without importing dpx.
//
// Rule: shared imports only stdlib and engine. Never dpx, never hlc.
// Types that belong to a single package stay there.
package shared

import (
	"io"
	"sync/atomic"
	"time"

	"github.com/agberohq/dpx/engine"
)

// ── Proposer boundary ────────────────────────────────────────────────────────

// Proposer is the interface internal/raft satisfies.
type Proposer interface {
	Propose(data []byte) (ApplyResult, error)
	Shutdown() error
}

// ApplyResult is returned from the FSM to the proposing goroutine.
type ApplyResult struct {
	Conflict bool
	Err      error
}

// WatchNotifier is the interface watcherMap satisfies.
type WatchNotifier interface {
	NotifyBatch(writes []WriteEntry, metrics *Metrics)
}

// ProposerFactory is the function signature callers pass to dpx.Open.
type ProposerFactory func(cfg Config, eng engine.StorageEngine, w WatchNotifier) (Proposer, error)

// ── Wire format ──────────────────────────────────────────────────────────────

// Proposal is the unit proposed to Raft per RunInTx call.
// Serialised with msgpack. Timestamp carries the HLC wall+counter
// as plain uint64/uint16 so shared does not need to import hlc.
type Proposal struct {
	ReadSet          []ReadEntry
	Writes           []WriteEntry
	TimestampWall    uint64
	TimestampCounter uint16
}

func (p *Proposal) TimestampIsZero() bool {
	return p.TimestampWall == 0 && p.TimestampCounter == 0
}

// ReadEntry records a key read during speculative execution.
type ReadEntry struct {
	Key     []byte
	Epoch   uint64
	IsDebit bool
}

// WriteEntry is one mutation in the write set.
type WriteEntry struct {
	Op    WriteOp
	Key   []byte
	Value []byte
}

// WriteOp distinguishes the mutation type.
type WriteOp uint8

const (
	OpSet    WriteOp = 1
	OpDelete WriteOp = 2
	OpCredit WriteOp = 3
	OpDebit  WriteOp = 4
)

const (
	ResultOK       uint64 = 0
	ResultConflict uint64 = 1
)

// ── Metrics ──────────────────────────────────────────────────────────────────

// Metrics is the single shared struct written by both dpx.Node and the FSM.
// Pass the same *Metrics pointer to dpx.Open and it flows to the FSM via Config.
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

// ── Config ───────────────────────────────────────────────────────────────────

// SyncPolicy controls WAL durability. Canonical definition lives here;
// dpx.SyncPolicy is a type alias so callers use the same constants.
type SyncPolicy int

const (
	SyncBatch SyncPolicy = iota // fsync once per Raft round trip (default)
	SyncFull                    // fsync every ApplyBatch
	SyncNone                    // no fsync
)

// Config is the complete config passed from dpx.Open to ProposerFactory.
// It carries everything internal/raft needs without requiring internal/raft
// to import the parent dpx package.
type Config struct {
	NodeID     string
	ListenAddr string            // multi-node TCP address; empty = embedded inmem
	Peers      map[string]string // empty = single-node embedded mode
	RaftDir    string

	Engine     engine.StorageEngine
	SyncPolicy SyncPolicy

	ShutdownTimeout time.Duration

	Metrics *Metrics
	Logger  io.Writer // nil = discard
}
