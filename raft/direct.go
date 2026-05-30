package raft

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
	"github.com/olekukonko/hlc"
	"github.com/vmihailenco/msgpack/v5"
)

var errDirectClosed = errors.New("dpx/raft: direct proposer closed")

// proposalEntry is one goroutine's submission to the pipeline.
type proposalEntry struct {
	p        *shared.Proposal
	resultCh chan shared.ApplyResult
}

// directProposer bypasses HashiCorp Raft for single-node embedded mode.
//
// Pipeline: goroutines submit to submitCh; a single applierLoop goroutine owns
// the FSM and drains the channel in batches. This eliminates per-proposal mutex
// contention — callers only pay a channel send (~50ns) + wait for their result.
// Multiple concurrent proposals are applied in one engine.ApplyBatch call,
// amortizing the sharded-clone cost across the batch.
type directProposer struct {
	fsm    *dpxFSM
	closed atomic.Bool
	index  uint64 // monotone log index — only touched by applierLoop goroutine

	submitCh chan *proposalEntry
	done     chan struct{}
	wg       sync.WaitGroup
}

const (
	submitBufSize = 4096
	maxBatchSize  = 512
	flushInterval = 50 * time.Microsecond // max latency added by batcher
)

func newDirectProposer(
	eng engine.StorageEngine,
	syncPolicy shared.SyncPolicy,
	w shared.WatchNotifier,
	metrics *shared.Metrics,
	telemetry *shared.Telemetry,
) (*directProposer, error) {
	clock := hlc.NewClock()
	f := newFSM(eng, syncPolicy, w, metrics, clock)
	f.setTelemetry(telemetry)
	if _, err := f.open(nil); err != nil {
		return nil, err
	}
	d := &directProposer{
		fsm:      f,
		submitCh: make(chan *proposalEntry, submitBufSize),
		done:     make(chan struct{}),
	}
	d.wg.Add(1)
	go d.applierLoop()
	return d, nil
}

// ProposeDirect implements shared.DirectProposer — skips msgpack.
func (d *directProposer) ProposeDirect(p *shared.Proposal) (shared.ApplyResult, error) {
	if d.closed.Load() {
		return shared.ApplyResult{}, errDirectClosed
	}
	entry := &proposalEntry{
		p:        p,
		resultCh: make(chan shared.ApplyResult, 1),
	}
	select {
	case d.submitCh <- entry:
	case <-d.done:
		return shared.ApplyResult{}, errDirectClosed
	}
	select {
	case res := <-entry.resultCh:
		return res, nil
	case <-d.done:
		return shared.ApplyResult{}, errDirectClosed
	}
}

// Propose satisfies shared.Proposer for the msgpack (Raft) path.
func (d *directProposer) Propose(data []byte) (shared.ApplyResult, error) {
	if d.closed.Load() {
		return shared.ApplyResult{}, errDirectClosed
	}
	var p shared.Proposal
	if err := msgpack.Unmarshal(data, &p); err != nil {
		return shared.ApplyResult{}, err
	}
	return d.ProposeDirect(&p)
}

// Shutdown drains in-flight proposals and stops the applierLoop goroutine.
// Safe to call multiple times.
func (d *directProposer) Shutdown() error {
	if !d.closed.CompareAndSwap(false, true) {
		return nil // already shut down
	}
	close(d.done)
	d.wg.Wait()
	return nil
}

// applierLoop is the single goroutine that owns FSM state.
// It drains submitCh in batches, applying all queued proposals in one
// engine.ApplyBatch call.
func (d *directProposer) applierLoop() {
	defer d.wg.Done()

	batch := make([]*proposalEntry, 0, maxBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		d.applyBatch(batch)
		for i := range batch {
			batch[i] = nil // release reference for GC
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-d.done:
			flush()
			return

		case <-ticker.C:
			flush()

		case entry := <-d.submitCh:
			batch = append(batch, entry)
			// Non-blocking drain: collect everything already queued.
		drain:
			for len(batch) < maxBatchSize {
				select {
				case e := <-d.submitCh:
					batch = append(batch, e)
				default:
					break drain
				}
			}
			if len(batch) >= maxBatchSize {
				flush()
			}
		}
	}
}

// applyBatch applies a batch of proposals in one engine.ApplyBatch call.
// Only called from applierLoop — no locking needed.
func (d *directProposer) applyBatch(entries []*proposalEntry) {
	shadow := d.fsm.pool.Get().(map[string]engine.EpochRecord)
	defer func() {
		clear(shadow)
		d.fsm.pool.Put(shadow)
	}()

	batch := d.fsm.engine.NewBatch()
	results := make([]shared.ApplyResult, len(entries))
	hasWrites := false

	for i, entry := range entries {
		d.index++
		idx := d.index

		if !entry.p.TimestampIsZero() {
			d.fsm.clock.Observe(hlc.Timestamp{
				Wall:    entry.p.TimestampWall,
				Counter: entry.p.TimestampCounter,
			})
		}

		if d.fsm.detectConflict(entry.p, shadow) {
			if d.fsm.metrics != nil {
				d.fsm.metrics.ConflictTotal.Add(1)
			}
			results[i] = shared.ApplyResult{Conflict: true}
			continue
		}

		if err := d.fsm.applyProposalToBatch(idx, entry.p, shadow, batch); err != nil {
			results[i] = shared.ApplyResult{Err: err}
			continue
		}

		hasWrites = true
		results[i] = shared.ApplyResult{}
	}

	if hasWrites {
		wo := engine.WriteOptions{Sync: d.fsm.syncPolicy == shared.SyncFull}
		t0 := time.Now()
		if err := d.fsm.engine.ApplyBatch(batch, wo); err != nil {
			for i := range results {
				if results[i].Err == nil && !results[i].Conflict {
					results[i] = shared.ApplyResult{Err: err}
				}
			}
		} else {
			if d.fsm.telemetry != nil {
				d.fsm.telemetry.EngineApply.Record(time.Since(t0))
			}
			d.fsm.state.applied = d.index
			if d.fsm.syncPolicy == shared.SyncBatch {
				d.fsm.engine.Sync()
			}
		}
	}

	// Notify watchers and return results.
	for i, entry := range entries {
		if results[i].Err == nil && !results[i].Conflict && d.fsm.watchers != nil {
			d.fsm.watchers.NotifyBatch(entry.p.Writes, d.fsm.metrics)
		}
		entry.resultCh <- results[i]
	}
}
