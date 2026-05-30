package dpx

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/dpx/shared"
)

// Batcher implements the adaptive flush delay that allows Dragonboat to
// batch concurrent SyncPropose calls into one Raft round trip.
//
// Each RunInTx call invokes Wait before calling SyncPropose. The Wait
// introduces a brief delay computed from the exponential moving average
// of inter-arrival times. Concurrent goroutines accumulate during the
// delay; Dragonboat pipelines their proposals internally.
//
// The batcher does NOT concatenate proposals into a single Raft entry.
// Each RunInTx remains one Raft log entry.
type Batcher struct {
	cfg       atomic.Pointer[BatchConfig]
	mu        sync.Mutex
	ema       float64           // rolling inter-arrival time (seconds)
	lastAt    time.Time         // time of last Wait call
	inFlight  atomic.Int64      // number of goroutines currently inside Wait
	inBytes   atomic.Int64      // total proposal bytes of in-flight goroutines
	telemetry *shared.Telemetry // <-- ADDED
}

// newBatcher creates a Batcher with the given config.
func newBatcher(cfg BatchConfig, telemetry *shared.Telemetry) *Batcher { // <-- UPDATED SIGNATURE
	b := &Batcher{
		telemetry: telemetry, // <-- STORED
	}
	b.cfg.Store(&cfg)
	return b
}

// SetConfig replaces the BatchConfig atomically.
// Takes effect on the next Wait call. No restart required.
func (b *Batcher) SetConfig(cfg BatchConfig) {
	b.cfg.Store(&cfg)
}

// Wait delays the calling goroutine for the EMA-computed flush age,
// or returns immediately if the entry/byte threshold is exceeded.
//
// proposalBytes is the marshalled size of the proposal; it must be known
// before calling Wait (marshal before calling Wait, not after).
//
// Returns ctx.Err() if the context is cancelled before the delay expires.
func (b *Batcher) Wait(ctx context.Context, proposalBytes int) error {
	cfg := b.cfg.Load()
	b.inFlight.Add(1)
	b.inBytes.Add(int64(proposalBytes))
	defer func() {
		b.inFlight.Add(-1)
		b.inBytes.Add(-int64(proposalBytes))
	}()

	// Flush immediately if thresholds exceeded.
	if b.inFlight.Load() >= int64(cfg.MaxEntries) ||
		b.inBytes.Load() >= cfg.MaxBytes {
		// Return nil (proceed immediately). ctx.Err() is returned if cancelled.
		return ctx.Err()
	}

	// Compute flush age from EMA inter-arrival time.
	b.mu.Lock()
	now := time.Now()
	if !b.lastAt.IsZero() {
		iat := now.Sub(b.lastAt).Seconds()
		b.ema = cfg.EMAAlpha*iat + (1-cfg.EMAAlpha)*b.ema
	}
	b.lastAt = now
	age := clampDuration(
		time.Duration(float64(time.Second)*cfg.K*b.ema),
		cfg.MinAge,
		cfg.MaxAge,
	)
	b.mu.Unlock()

	// TELEMETRY HOOKS
	var waitStart time.Time
	if b.telemetry != nil {
		waitStart = time.Now()
		// Record batch size distribution at flush-decision time.
		// We store inFlight count as "duration" nanoseconds so it fits the
		// existing histogram-like StageTimer API without structural changes.
		b.telemetry.BatcherFlushSize.Record(time.Duration(b.inFlight.Load()))
	}

	// Per-call timer: safe for concurrent callers.
	// time.NewTimer + defer Stop avoids goroutine leak.
	// The allocation (~100ns) is acceptable for v0.1.
	t := time.NewTimer(age)
	defer t.Stop()

	select {
	case <-ctx.Done():
		if b.telemetry != nil {
			b.telemetry.BatcherWait.Record(time.Since(waitStart))
		}
		return ctx.Err()
	case <-t.C:
		if b.telemetry != nil {
			b.telemetry.BatcherWait.Record(time.Since(waitStart))
		}
		return nil
	}
}
