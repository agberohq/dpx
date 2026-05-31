package conductor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
	hraft "github.com/hashicorp/raft"
	"github.com/olekukonko/hlc"
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

// applyState is owned exclusively by phase A (applierLoop / applyBatch).
// It must never be accessed from any other goroutine.
type applyState struct {
	keyEpoch map[string]engine.EpochRecord
}

type dpxFSM struct {
	// applied is updated by phase B (writeLoop / applyProposalToBatch) after
	// a successful engine commit. Atomic so CurrentSequence reads are safe
	// from any goroutine without holding any lock.
	applied    atomic.Uint64
	engine     engine.StorageEngine
	state      applyState
	syncPolicy shared.SyncPolicy
	clock      *hlc.Clock
	pool       sync.Pool
	vkBufPool  sync.Pool
	watchers   shared.WatchNotifier
	metrics    *shared.Metrics
	telemetry  *shared.Telemetry
	skipHLC    bool // true for embedded mode: skip clock.Observe
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
		telemetry:  nil,
		state: applyState{
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
	f.applied.Store(f.engine.CurrentSequence())

	if f.telemetry != nil {
		f.telemetry.KeyEpochRebuild.Record(time.Since(start))
	}
	if f.metrics != nil {
		f.metrics.KeyEpochRebuildDurationNs.Add(time.Since(start).Nanoseconds())
	}
	return f.applied.Load(), nil
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
		if err := proposal.Unmarshal(log.Data); err != nil {
			results[i] = shared.ApplyResult{
				Err: fmt.Errorf("dpx/raft: corrupt entry at index %d: %w", log.Index, err),
			}
			continue
		}

		if !f.skipHLC && !proposal.TimestampIsZero() {
			f.clock.Observe(hlc.Timestamp{Wall: proposal.TimestampWall, Counter: proposal.TimestampCounter})
		}

		tcd := time.Now()
		if f.detectConflict(&proposal, shadow) {
			results[i] = shared.ApplyResult{Conflict: true}
			if f.telemetry != nil {
				f.telemetry.ConflictDetect.Record(time.Since(tcd))
			}
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

	f.applied.Store(index)
	if f.syncPolicy == shared.SyncBatch {
		var syncStart time.Time
		if f.telemetry != nil {
			syncStart = time.Now()
		}
		if err := f.engine.Sync(); err != nil {
			return fmt.Errorf("dpx/raft: WAL sync at index %d: %w", index, err)
		}
		if f.telemetry != nil {
			f.telemetry.EngineSync.Record(time.Since(syncStart))
		}
	}

	if f.metrics != nil {
		f.metrics.ApplyDurationNs.Add(time.Since(start).Nanoseconds())
	}
	return nil
}

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

func (f *dpxFSM) Snapshot() (hraft.FSMSnapshot, error) {
	var snapStart time.Time
	if f.telemetry != nil {
		snapStart = time.Now()
	}

	dir := filepath.Join(
		f.engine.DataDir(),
		"snap-"+strconv.FormatInt(time.Now().UnixNano(), 10),
	)
	if err := f.engine.CreateCheckpoint(dir); err != nil {
		return nil, fmt.Errorf("dpx/raft: checkpoint: %w", err)
	}
	if f.telemetry != nil {
		f.telemetry.SnapshotCreate.Record(time.Since(snapStart))
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
