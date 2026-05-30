// Package raft wires HashiCorp Raft into DPX.
package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
	hraft "github.com/hashicorp/raft"
	"github.com/olekukonko/hlc"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	rawIterStart = []byte("__dpx:ver:")
	rawIterEnd   = append([]byte("__dpx:ver:"), bytes.Repeat([]byte{0xFF}, 16)...)
	appliedKey   = []byte("__dpx:applied")
)

func versionKey(buf, key []byte) []byte {
	return append(append(buf[:0], "__dpx:ver:"...), key...)
}

func encodeEpochRecord(er engine.EpochRecord, buf []byte) []byte {
	b := buf[:9]
	binary.LittleEndian.PutUint64(b, er.Epoch)
	if er.IsCredit {
		b[8] = 1
	} else {
		b[8] = 0
	}
	return b
}

func decodeEpochRecord(b []byte) (engine.EpochRecord, bool) {
	if len(b) < 9 {
		return engine.EpochRecord{}, false
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b),
		IsCredit: b[8] == 1,
	}, true
}

func encodeUint64(v uint64, buf []byte) []byte {
	b := buf[:8]
	binary.LittleEndian.PutUint64(b, v)
	return b
}

type applyState struct {
	keyEpoch map[string]engine.EpochRecord
	applied  uint64
}

type dpxFSM struct {
	engine     engine.StorageEngine
	state      applyState
	syncPolicy shared.SyncPolicy
	clock      *hlc.Clock

	pool      sync.Pool
	vkBufPool sync.Pool

	watchers  shared.WatchNotifier
	metrics   *shared.Metrics
	telemetry *shared.Telemetry
}

func newFSM(
	eng engine.StorageEngine,
	syncPolicy shared.SyncPolicy,
	watchers shared.WatchNotifier,
	metrics *shared.Metrics,
	clock *hlc.Clock,
) *dpxFSM {
	return &dpxFSM{
		engine:     eng,
		syncPolicy: syncPolicy,
		watchers:   watchers,
		metrics:    metrics,
		clock:      clock,
		telemetry:  nil, // wired via setTelemetry after construction
		state: applyState{
			// Initialise to empty map. node.Open() calls f.open() immediately
			// after construction to rebuild from existing engine data on restart.
			// For a fresh engine this stays empty, which is correct.
			keyEpoch: make(map[string]engine.EpochRecord, 1<<10),
		},
		pool: sync.Pool{
			New: func() any { return make(map[string]engine.EpochRecord, 256) },
		},
		vkBufPool: sync.Pool{
			New: func() any { return make([]byte, 0, 256) },
		},
	}
}

func (f *dpxFSM) setTelemetry(t *shared.Telemetry) { f.telemetry = t }

func (f *dpxFSM) open(stopc <-chan struct{}) (uint64, error) {
	start := time.Now()
	f.state.keyEpoch = make(map[string]engine.EpochRecord, 1<<16)

	iter := f.engine.RawIter(rawIterStart, rawIterEnd)
	defer iter.Close()

	const prefixLen = len("__dpx:ver:")
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) <= prefixLen {
			continue
		}
		userKey := string(k[prefixLen:])
		er, ok := decodeEpochRecord(iter.Value())
		if !ok {
			return 0, fmt.Errorf("dpx/raft: corrupted version record for key %q (len=%d)",
				userKey, len(iter.Value()))
		}
		f.state.keyEpoch[userKey] = er

		if stopc != nil {
			select {
			case <-stopc:
				return 0, fmt.Errorf("dpx/raft: open aborted")
			default:
			}
		}
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("dpx/raft: open scan: %w", err)
	}

	f.state.applied = f.engine.CurrentSequence()

	if f.metrics != nil {
		f.metrics.KeyEpochRebuildDurationNs.Add(time.Since(start).Nanoseconds())
	}
	return f.state.applied, nil
}

func (f *dpxFSM) Apply(log *hraft.Log) interface{} {
	if log.Type != hraft.LogCommand {
		return shared.ApplyResult{}
	}
	return f.applyBatch([]*hraft.Log{log})[0]
}

func (f *dpxFSM) ApplyBatch(logs []*hraft.Log) []interface{} {
	return f.applyBatch(logs)
}

func (f *dpxFSM) applyBatch(logs []*hraft.Log) []interface{} {
	results := make([]interface{}, len(logs))

	shadow := f.pool.Get().(map[string]engine.EpochRecord)
	defer func() {
		clear(shadow)
		f.pool.Put(shadow)
	}()

	for i, log := range logs {
		if log.Type != hraft.LogCommand {
			results[i] = shared.ApplyResult{}
			continue
		}

		var proposal shared.Proposal
		if err := msgpack.Unmarshal(log.Data, &proposal); err != nil {
			results[i] = shared.ApplyResult{
				Err: fmt.Errorf("dpx/raft: corrupt entry at index %d: %w", log.Index, err),
			}
			continue
		}

		if !proposal.TimestampIsZero() {
			f.clock.Observe(hlc.Timestamp{Wall: proposal.TimestampWall, Counter: proposal.TimestampCounter})
		}

		tcd := time.Now()
		conflict := f.detectConflict(&proposal, shadow)
		if f.telemetry != nil {
			f.telemetry.ConflictDetect.Record(time.Since(tcd))
		}
		if conflict {
			results[i] = shared.ApplyResult{Conflict: true}
			continue
		}

		tap := time.Now()
		if err := f.applyProposal(log.Index, &proposal, shadow); err != nil {
			results[i] = shared.ApplyResult{Err: err}
			continue
		}

		if f.telemetry != nil {
			f.telemetry.ApplyProposal.Record(time.Since(tap))
		}
		results[i] = shared.ApplyResult{}

		if f.watchers != nil {
			f.watchers.NotifyBatch(proposal.Writes, f.metrics)
		}
	}
	return results
}

func (f *dpxFSM) detectConflict(proposal *shared.Proposal, shadow map[string]engine.EpochRecord) bool {
	for _, re := range proposal.ReadSet {
		shadowKey := unsafe.String(unsafe.SliceData(re.Key), len(re.Key))

		if c := f.state.keyEpoch[shadowKey]; c.Epoch > re.Epoch {
			if !(c.IsCredit && re.IsDebit) {
				return true
			}
		}
		if s, ok := shadow[shadowKey]; ok {
			if !(s.IsCredit && re.IsDebit) {
				return true
			}
		}
	}
	return false
}

func (f *dpxFSM) applyProposal(index uint64, proposal *shared.Proposal, shadow map[string]engine.EpochRecord) error {
	start := time.Now()

	batch := f.engine.NewBatch()
	vkBuf := f.vkBufPool.Get().([]byte)
	defer f.vkBufPool.Put(vkBuf)

	var epochBuf [9]byte
	var appliedBuf [8]byte

	for _, w := range proposal.Writes {
		isCredit := w.Op == shared.OpCredit
		switch w.Op {
		case shared.OpSet:
			batch.Set(w.Key, w.Value)
		case shared.OpDelete:
			batch.Delete(w.Key)
		case shared.OpCredit, shared.OpDebit:
			batch.Merge(w.Key, w.Value)
		default:
			return fmt.Errorf("dpx/raft: unknown WriteOp %d at index %d", w.Op, index)
		}

		er := engine.EpochRecord{Epoch: index, IsCredit: isCredit}
		batch.Set(versionKey(vkBuf, w.Key), encodeEpochRecord(er, epochBuf[:]))

		shadow[unsafe.String(unsafe.SliceData(w.Key), len(w.Key))] = er
		f.state.keyEpoch[string(w.Key)] = er
	}

	batch.Set(appliedKey, encodeUint64(index, appliedBuf[:]))

	wo := engine.WriteOptions{Sync: f.syncPolicy == shared.SyncFull}
	tea := time.Now()
	if err := f.engine.ApplyBatch(batch, wo); err != nil {
		return err
	}
	if f.telemetry != nil {
		f.telemetry.EngineApply.Record(time.Since(tea))
	}
	f.state.applied = index

	if f.syncPolicy == shared.SyncBatch {
		if err := f.engine.Sync(); err != nil {
			return fmt.Errorf("dpx/raft: WAL sync at index %d: %w", index, err)
		}
	}

	if f.metrics != nil {
		f.metrics.ApplyDurationNs.Add(time.Since(start).Nanoseconds())
	}
	return nil
}

// applyProposalToBatch adds one proposal's mutations to a caller-provided engine batch
// and updates shadow + keyEpoch, but does NOT call engine.ApplyBatch.
// Used by directProposer.applyBatch to accumulate multiple proposals before one commit.
// Must only be called from the applierLoop goroutine.
func (f *dpxFSM) applyProposalToBatch(index uint64, proposal *shared.Proposal, shadow map[string]engine.EpochRecord, batch engine.Batch) error {
	vkBuf := f.vkBufPool.Get().([]byte)
	defer f.vkBufPool.Put(vkBuf)

	var epochBuf [9]byte
	var appliedBuf [8]byte

	for _, w := range proposal.Writes {
		isCredit := w.Op == shared.OpCredit
		switch w.Op {
		case shared.OpSet:
			batch.Set(w.Key, w.Value)
		case shared.OpDelete:
			batch.Delete(w.Key)
		case shared.OpCredit, shared.OpDebit:
			batch.Merge(w.Key, w.Value)
		default:
			return fmt.Errorf("dpx/raft: unknown WriteOp %d at index %d", w.Op, index)
		}
		er := engine.EpochRecord{Epoch: index, IsCredit: isCredit}
		batch.Set(versionKey(vkBuf, w.Key), encodeEpochRecord(er, epochBuf[:]))
		shadow[unsafe.String(unsafe.SliceData(w.Key), len(w.Key))] = er
		f.state.keyEpoch[string(w.Key)] = er
	}
	batch.Set(appliedKey, encodeUint64(index, appliedBuf[:]))
	return nil
}

// applyBatchDirect applies multiple proposals in a single engine.ApplyBatch call.
// Called exclusively by directProposer's run() goroutine — no locking needed.
// Each proposal gets its own conflict check and shadow map slot; all non-conflicting
// writes are accumulated into one engine batch and committed atomically.
func (f *dpxFSM) applyBatchDirect(proposals []*shared.Proposal) []shared.ApplyResult {
	results := make([]shared.ApplyResult, len(proposals))

	shadow := f.pool.Get().(map[string]engine.EpochRecord)
	defer func() { clear(shadow); f.pool.Put(shadow) }()

	vkBuf := f.vkBufPool.Get().([]byte)
	defer f.vkBufPool.Put(vkBuf)

	var epochBuf [9]byte
	var appliedBuf [8]byte
	batch := f.engine.NewBatch()
	hasWrites := false
	start := time.Now()

	for i, p := range proposals {
		f.state.applied++
		idx := f.state.applied

		if !p.TimestampIsZero() {
			f.clock.Observe(hlc.Timestamp{Wall: p.TimestampWall, Counter: p.TimestampCounter})
		}

		if f.detectConflict(p, shadow) {
			if f.metrics != nil {
				f.metrics.ConflictTotal.Add(1)
			}
			results[i] = shared.ApplyResult{Conflict: true}
			continue
		}

		badOp := false
		for _, w := range p.Writes {
			isCredit := w.Op == shared.OpCredit
			switch w.Op {
			case shared.OpSet:
				batch.Set(w.Key, w.Value)
			case shared.OpDelete:
				batch.Delete(w.Key)
			case shared.OpCredit, shared.OpDebit:
				batch.Merge(w.Key, w.Value)
			default:
				results[i] = shared.ApplyResult{Err: fmt.Errorf("dpx/raft: unknown WriteOp %d", w.Op)}
				badOp = true
				break
			}
			if badOp {
				break
			}
			er := engine.EpochRecord{Epoch: idx, IsCredit: isCredit}
			batch.Set(versionKey(vkBuf, w.Key), encodeEpochRecord(er, epochBuf[:]))
			shadow[unsafe.String(unsafe.SliceData(w.Key), len(w.Key))] = er
			f.state.keyEpoch[string(w.Key)] = er
		}
		if badOp {
			continue
		}

		batch.Set(appliedKey, encodeUint64(idx, appliedBuf[:]))
		hasWrites = true
		results[i] = shared.ApplyResult{}
	}

	if hasWrites {
		wo := engine.WriteOptions{Sync: f.syncPolicy == shared.SyncFull}
		tea := time.Now()
		if err := f.engine.ApplyBatch(batch, wo); err != nil {
			for i := range results {
				if results[i].Err == nil && !results[i].Conflict {
					results[i] = shared.ApplyResult{Err: err}
				}
			}
			return results
		}
		if f.telemetry != nil {
			f.telemetry.EngineApply.Record(time.Since(tea))
		}
		if f.syncPolicy == shared.SyncBatch {
			f.engine.Sync()
		}
		if f.metrics != nil {
			f.metrics.ApplyDurationNs.Add(time.Since(start).Nanoseconds())
		}
	}

	// Notify watchers after batch committed.
	for i, p := range proposals {
		if results[i].Err == nil && !results[i].Conflict && f.watchers != nil {
			f.watchers.NotifyBatch(p.Writes, f.metrics)
		}
	}

	return results
}

func (f *dpxFSM) Snapshot() (hraft.FSMSnapshot, error) {
	dir := filepath.Join(
		f.engine.DataDir(),
		"snap-"+strconv.FormatInt(time.Now().UnixNano(), 10),
	)
	if err := f.engine.CreateCheckpoint(dir); err != nil {
		return nil, fmt.Errorf("dpx/raft: checkpoint: %w", err)
	}
	return &dpxFSMSnapshot{dir: dir, metrics: f.metrics}, nil
}

func (f *dpxFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if f.metrics != nil {
		f.metrics.SnapshotRecoverTotal.Add(1)
	}
	dir := f.engine.DataDir()
	if err := f.engine.Close(); err != nil {
		return fmt.Errorf("dpx/raft: Restore close engine: %w", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("dpx/raft: Restore remove dir: %w", err)
	}
	if err := untarDir(rc, dir); err != nil {
		return fmt.Errorf("dpx/raft: Restore untar: %w", err)
	}
	if err := f.engine.Open(); err != nil {
		return fmt.Errorf("dpx/raft: Restore open engine: %w", err)
	}
	_, err := f.open(nil)
	return err
}

type dpxFSMSnapshot struct {
	dir     string
	metrics *shared.Metrics
}

func (s *dpxFSMSnapshot) Persist(sink hraft.SnapshotSink) error {
	if s.metrics != nil {
		s.metrics.SnapshotSaveTotal.Add(1)
	}
	if err := tarDir(s.dir, sink); err != nil {
		sink.Cancel()
		return fmt.Errorf("dpx/raft: Persist: %w", err)
	}
	return sink.Close()
}

func (s *dpxFSMSnapshot) Release() {
	os.RemoveAll(s.dir)
}
