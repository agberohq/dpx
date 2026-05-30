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
	// dpx.go / runOnce stages
	GetSnapshot StageTimer // engine.GetSnapshot()
	Speculate   StageTimer // fn(tx) speculative execution
	Marshal     StageTimer // msgpack.Marshal(proposal)
	Propose     StageTimer // full proposer.Propose() round-trip

	// raft/fsm.go / applyBatch stages (inside Propose)
	ConflictDetect StageTimer // detectConflict per proposal
	ApplyProposal  StageTimer // applyProposal per proposal (excludes engine)
	EngineApply    StageTimer // engine.ApplyBatch per proposal

	// engine/memory stages (inside EngineApply)
	Clone StageTimer // copy-on-write clone cost
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
