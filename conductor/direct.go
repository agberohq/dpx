package conductor

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
	"github.com/olekukonko/hlc"
)

var errDirectClosed = errors.New("dpx/raft: direct proposer closed")

// proposalEntry is one goroutine's submission to the pipeline.
type proposalEntry struct {
	p        *shared.Proposal
	resultCh chan shared.ApplyResult
	accStart time.Time
}

// directProposer bypasses HashiCorp Raft for single-node embedded mode.
//
// Pipeline design:
//
//	applierLoop (goroutine 1)      writeLoop (goroutine 2)
//	─────────────────────────      ──────────────────────
//	Phase A: drain submitCh        Phase B: engine.ApplyBatch
//	         conflict detect                notify callers
//	         build engine batch             return pooled slices
//	         send to writeCh  →    receive from writeCh
//
// Phase A owns FSM state (keyEpoch, index) — single-threaded, no locks.
// Phase B owns no shared state — runs concurrently with the next Phase A.
// The writeCh depth of 1 means Phase A blocks only when Phase B is still
// committing, which is the natural backpressure point.
type directProposer struct {
	fsm           *dpxFSM
	closed        atomic.Bool
	index         uint64
	submitCh      chan *proposalEntry
	done          chan struct{}
	wg            sync.WaitGroup
	flushInterval time.Duration // configurable per proposer type
}

const (
	// submitBufSize is the channel buffer. Sized generously so producers
	// never block waiting for the applierLoop to drain.
	submitBufSize = 16384

	// maxBatchSize caps how many entries one phase-A pass may contain.
	maxBatchSize = 2048

	// defaultFlushInterval is used by the non-sharded single proposer.
	// At 512 goroutines / 1 shard, the drain loop fills batches naturally
	// and the ticker fires as a backstop for quiet periods.
	defaultFlushInterval = 1 * time.Millisecond

	// shardedFlushInterval is used by each shard in the sharded proposer.
	// With 8192 goroutines / 64 shards = 128 goroutines/shard and a ~9ms
	// round-trip, Little's Law gives ~1152 ops/shard/flush window. Raising
	// the interval lets the drain loop accumulate larger batches before the
	// ticker fires, recovering the batch-size collapse seen at 1ms.
	shardedFlushInterval = 2 * time.Millisecond
)

const maxStackShadow = 64

type shadowEntry struct {
	key   string
	epoch engine.EpochRecord
}

func newDirectProposer(
	eng engine.StorageEngine,
	syncPolicy shared.SyncPolicy,
	w shared.WatchNotifier,
	metrics *shared.Metrics,
	telemetry *shared.Telemetry,
) (*directProposer, error) {
	return newDirectProposerWithInterval(eng, syncPolicy, w, metrics, telemetry, defaultFlushInterval)
}

func newDirectProposerWithInterval(
	eng engine.StorageEngine,
	syncPolicy shared.SyncPolicy,
	w shared.WatchNotifier,
	metrics *shared.Metrics,
	telemetry *shared.Telemetry,
	interval time.Duration,
) (*directProposer, error) {
	clock := hlc.NewClock()
	f := newFSM(eng, syncPolicy, w, metrics, clock)
	f.setTelemetry(telemetry)
	f.skipHLC = true
	if _, err := f.open(nil); err != nil {
		return nil, err
	}
	d := &directProposer{
		fsm:           f,
		submitCh:      make(chan *proposalEntry, submitBufSize),
		done:          make(chan struct{}),
		flushInterval: interval,
	}
	// Always use the inline applyBatchDirect path (d.sharded = false).
	// The shardedDirectProposer already provides parallelism by routing each
	// proposal to one of 64 independent directProposers. Adding a phase-A/B
	// pipeline inside each shard's proposer added goroutine coordination cost
	// (128 extra goroutines) without useful overlap — batch size stayed ~43
	// because the writeCh backpressure ate the accumulation window.
	d.wg.Add(1)
	go d.applierLoop()
	return d, nil
}

// ProposeDirect implements shared.DirectProposer.
func (d *directProposer) ProposeDirect(p *shared.Proposal) (shared.ApplyResult, error) {
	if d.closed.Load() {
		return shared.ApplyResult{}, errDirectClosed
	}

	var t0 time.Time
	entry := &proposalEntry{
		p:        p,
		resultCh: make(chan shared.ApplyResult, 1),
	}
	if d.fsm.telemetry != nil {
		t0 = time.Now()
		entry.accStart = t0
	}

	select {
	case d.submitCh <- entry:
		if d.fsm.telemetry != nil {
			d.fsm.telemetry.DirectSubmit.Record(time.Since(t0))
		}
	case <-d.done:
		return shared.ApplyResult{}, errDirectClosed
	}

	select {
	case res := <-entry.resultCh:
		if d.fsm.telemetry != nil {
			d.fsm.telemetry.DirectRoundTrip.Record(time.Since(t0))
		}
		return res, nil
	case <-d.done:
		return shared.ApplyResult{}, errDirectClosed
	}
}

// Propose satisfies shared.Proposer — native codec, no reflection.
func (d *directProposer) Propose(data []byte) (shared.ApplyResult, error) {
	if d.closed.Load() {
		return shared.ApplyResult{}, errDirectClosed
	}
	var p shared.Proposal
	if err := p.Unmarshal(data); err != nil {
		return shared.ApplyResult{}, err
	}
	return d.ProposeDirect(&p)
}

func (d *directProposer) Shutdown() error {
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(d.done)
	d.wg.Wait()
	return nil
}

// applierLoop — Phase A.
// Owns FSM state exclusively. Drains submitCh, runs conflict detection,
// builds the engine batch, then hands off to writeLoop via writeCh.
func (d *directProposer) applierLoop() {
	defer d.wg.Done()

	batch := make([]*proposalEntry, 0, maxBatchSize)
	ticker := time.NewTicker(d.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if d.fsm.telemetry != nil {
			d.fsm.telemetry.DirectAccumulate.Record(time.Since(batch[0].accStart))
		}
		// Inline path: conflict detection + commit + notify in one call.
		// No preparedBatch, no channel, no second goroutine.
		d.applyBatchDirect(batch)
		for i := range batch {
			batch[i] = nil
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

// applyBatchDirect is the non-sharded (inline) path.
// It fuses conflict detection, engine batch building, engine commit, and
// result delivery into a single call inside applierLoop — no preparedBatch
// allocation, no sync.Pool get/put, no writeCh send/receive, no goroutine
// wake-up. This is the original v1 applyBatch behaviour, recovered for the
// non-sharded engine where the phase-A/phase-B pipeline buys nothing.
func (d *directProposer) applyBatchDirect(entries []*proposalEntry) {
	engBatch := d.fsm.engine.NewBatch()
	results := make([]shared.ApplyResult, len(entries))
	hasWrite := false

	if len(entries) <= maxStackShadow {
		var stackShadow [maxStackShadow]shadowEntry
		shadowLen := 0

		for i, entry := range entries {
			d.index++
			idx := d.index
			if !d.fsm.skipHLC && !entry.p.TimestampIsZero() {
				d.fsm.clock.Observe(hlc.Timestamp{
					Wall:    entry.p.TimestampWall,
					Counter: entry.p.TimestampCounter,
				})
			}

			conflict := false
			for _, re := range entry.p.ReadSet {
				k := unsafe.String(unsafe.SliceData(re.Key), len(re.Key))
				if c := d.fsm.state.keyEpoch[k]; c.Epoch > re.Epoch {
					if !(c.IsCredit && re.IsDebit) {
						conflict = true
						break
					}
				}
				for j := 0; j < shadowLen; j++ {
					if stackShadow[j].key == k {
						if !(stackShadow[j].epoch.IsCredit && re.IsDebit) {
							conflict = true
						}
						break
					}
				}
			}
			if conflict {
				if d.fsm.metrics != nil {
					d.fsm.metrics.ConflictTotal.Add(1)
				}
				results[i] = shared.ApplyResult{Conflict: true}
				continue
			}

			vkBuf := d.fsm.vkBufPool.Get().([]byte)
			var epochBuf [9]byte
			var appliedBuf [8]byte

			for _, w := range entry.p.Writes {
				isCredit := w.Op == shared.OpCredit
				switch w.Op {
				case shared.OpSet:
					engBatch.Set(w.Key, w.Value)
				case shared.OpDelete:
					engBatch.Delete(w.Key)
				case shared.OpCredit, shared.OpDebit:
					engBatch.Merge(w.Key, w.Value)
				}
				er := engine.EpochRecord{Epoch: idx, IsCredit: isCredit}
				engBatch.Set(versionKey(vkBuf, w.Key), encodeEpochRecord(er, epochBuf[:]))
				if shadowLen < maxStackShadow {
					stackShadow[shadowLen] = shadowEntry{
						key:   unsafe.String(unsafe.SliceData(w.Key), len(w.Key)),
						epoch: er,
					}
					shadowLen++
				}
				d.fsm.state.keyEpoch[string(w.Key)] = er
			}
			engBatch.Set(appliedKey, encodeUint64(idx, appliedBuf[:]))
			d.fsm.vkBufPool.Put(vkBuf)
			hasWrite = true
			results[i] = shared.ApplyResult{}
		}
	} else {
		shadow := d.fsm.pool.Get().(map[string]engine.EpochRecord)
		defer func() {
			clear(shadow)
			d.fsm.pool.Put(shadow)
		}()
		for i, entry := range entries {
			d.index++
			idx := d.index
			if !d.fsm.skipHLC && !entry.p.TimestampIsZero() {
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
			if err := d.fsm.applyProposalToBatch(idx, entry.p, shadow, engBatch); err != nil {
				results[i] = shared.ApplyResult{Err: err}
				continue
			}
			hasWrite = true
			results[i] = shared.ApplyResult{}
		}
	}

	if hasWrite {
		wo := engine.WriteOptions{Sync: d.fsm.syncPolicy == shared.SyncFull}
		t0 := time.Now()
		if err := d.fsm.engine.ApplyBatch(engBatch, wo); err != nil {
			for i := range results {
				if results[i].Err == nil && !results[i].Conflict {
					results[i] = shared.ApplyResult{Err: err}
				}
			}
		} else {
			if d.fsm.telemetry != nil {
				d.fsm.telemetry.EngineApply.Record(time.Since(t0))
			}
			d.fsm.applied.Store(d.index)
			if d.fsm.syncPolicy == shared.SyncBatch {
				d.fsm.engine.Sync()
			}
		}
	}

	for i, entry := range entries {
		if results[i].Err == nil && !results[i].Conflict && d.fsm.watchers != nil {
			d.fsm.watchers.NotifyBatch(entry.p.Writes, d.fsm.metrics)
		}
		entry.resultCh <- results[i]
	}
}

// Called from writeLoop — runs concurrently with the next preparePhaseA.
//
// SAFETY: touches ONLY fields of preparedBatch — zero access to directProposer
// or dpxFSM fields (except the atomic applied.Store via pb.fsm).
// Returns pooled slices to the pool after all callers have been notified.
