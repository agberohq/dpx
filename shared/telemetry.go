package shared

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// StageTimer measures latency for one pipeline stage using lock-free atomics.
// Captures sum + count (for mean) and min/max via CAS loops.
type StageTimer struct {
	sumNs atomic.Int64
	count atomic.Int64
	minNs atomic.Int64 // 0 = uninitialised
	maxNs atomic.Int64
}

// Record adds one observation.
func (s *StageTimer) Record(d time.Duration) {
	ns := d.Nanoseconds()
	s.sumNs.Add(ns)
	s.count.Add(1)

	// Min (CAS loop — uncontended in practice since min rarely changes)
	for {
		cur := s.minNs.Load()
		if cur != 0 && cur <= ns {
			break
		}
		if s.minNs.CompareAndSwap(cur, ns) {
			break
		}
	}
	// Max
	for {
		cur := s.maxNs.Load()
		if cur >= ns {
			break
		}
		if s.maxNs.CompareAndSwap(cur, ns) {
			break
		}
	}
}

// Count returns the number of observations.
func (s *StageTimer) Count() int64 { return s.count.Load() }

// Mean returns the arithmetic mean, or 0 if no observations.
func (s *StageTimer) Mean() time.Duration {
	c := s.count.Load()
	if c == 0 {
		return 0
	}
	return time.Duration(s.sumNs.Load() / c)
}

// Min returns the minimum observed duration, or 0 if no observations.
func (s *StageTimer) Min() time.Duration {
	return time.Duration(s.minNs.Load())
}

// Max returns the maximum observed duration.
func (s *StageTimer) Max() time.Duration {
	return time.Duration(s.maxNs.Load())
}

// Telemetry captures per-stage latency across the DPX request pipeline.
// Pass the same *Telemetry to dpx.Config.Telemetry so it flows to every layer.
//
// Pipeline stages (in order for a single RunInTx):
//
//	GetSnapshot → fn(tx) → Marshal → Propose → [detectConflict → applyProposal → EngineApply]
type Telemetry struct {
	// Existing (dpx.go runOnce)
	GetSnapshot StageTimer
	Speculate   StageTimer
	Marshal     StageTimer
	Propose     StageTimer

	// Existing (raft/fsm.go applyBatch)
	ConflictDetect StageTimer
	ApplyProposal  StageTimer
	EngineApply    StageTimer

	// Batcher
	BatcherWait      StageTimer // Wait() duration
	BatcherFlushSize StageTimer // batch size distribution

	// Direct Proposer
	DirectSubmit     StageTimer // submitCh send
	DirectAccumulate StageTimer // batch accumulation time
	DirectRoundTrip  StageTimer // ProposeDirect → result

	// Engine internals
	EngineSync      StageTimer // Sync() call duration
	SnapshotCreate  StageTimer // GetSnapshot engine call
	IterMaterialise StageTimer // badger eager copy / pebble NewIter

	// Transaction breakdown
	TxValidate    StageTimer // validate() call
	TxReadSetCopy StageTimer // readSetSlice allocation
	RetryBackoff  StageTimer // total time spent in retry delays

	// Watch
	WatchNotify      StageTimer // NotifyBatch broadcast
	WatchChannelSend StageTimer // per-channel send

	// Startup
	EngineOpen      StageTimer
	KeyEpochRebuild StageTimer // already have as int64 metric, but timer is useful
	RaftBootstrap   StageTimer

	// Shutdown
	ShutdownRaft   StageTimer
	ShutdownEngine StageTimer

	Clone StageTimer // copy-on-write clone cost
}

// RecordClone records a clone/copy-on-write duration observation.
// Satisfies engine.StageRecorder.
func (t *Telemetry) RecordClone(d time.Duration) {
	t.Clone.Record(d)
}

// RecordIterMaterialise records an iterator materialisation duration observation.
// Satisfies engine.StageRecorder.
func (t *Telemetry) RecordIterMaterialise(d time.Duration) {
	t.IterMaterialise.Record(d)
}

// Print writes a human-readable breakdown to w.
// Only stages with at least one observation are printed.
func (t *Telemetry) Print(w io.Writer) {
	type row struct {
		name  string
		timer *StageTimer
	}
	rows := []row{
		{"get_snapshot", &t.GetSnapshot},
		{"speculate_fn", &t.Speculate},
		{"marshal", &t.Marshal},
		{"propose_total", &t.Propose},
		{"  conflict_detect", &t.ConflictDetect},
		{"  apply_proposal", &t.ApplyProposal},
		{"  engine_apply", &t.EngineApply},
		{"    clone_cow", &t.Clone},
		{"iter_materialise", &t.IterMaterialise},
		{"retry_backoff", &t.RetryBackoff},
		{"tx_validate", &t.TxValidate},
		{"tx_readset_copy", &t.TxReadSetCopy},
		{"batcher_wait", &t.BatcherWait},
		{"batcher_flush_size", &t.BatcherFlushSize},
		{"direct_submit", &t.DirectSubmit},
		{"direct_accumulate", &t.DirectAccumulate},
		{"direct_roundtrip", &t.DirectRoundTrip},
		{"engine_sync", &t.EngineSync},
		{"snapshot_create", &t.SnapshotCreate},
		{"watch_notify", &t.WatchNotify},
		{"watch_channel_send", &t.WatchChannelSend},
		{"engine_open", &t.EngineOpen},
		{"key_epoch_rebuild", &t.KeyEpochRebuild},
		{"raft_bootstrap", &t.RaftBootstrap},
		{"shutdown_raft", &t.ShutdownRaft},
		{"shutdown_engine", &t.ShutdownEngine},
	}
	fmt.Fprintf(w, "%-22s %10s %10s %10s %10s\n", "stage", "count", "mean", "min", "max")
	fmt.Fprintf(w, "%-22s %10s %10s %10s %10s\n",
		"----------------------", "----------", "----------", "----------", "----------")
	for _, r := range rows {
		if r.timer.Count() == 0 {
			continue
		}
		fmt.Fprintf(w, "%-22s %10d %10s %10s %10s\n",
			r.name,
			r.timer.Count(),
			r.timer.Mean().Round(time.Microsecond),
			r.timer.Min().Round(time.Microsecond),
			r.timer.Max().Round(time.Microsecond),
		)
	}
}
