// Package raft wires HashiCorp Raft into DPX.
// It contains only two things: the FSM (this file) and the cluster setup (node.go).
// Everything else — transactions, batching, watchers, encoding — stays in the
// parent dpx package.
//
// Import graph:
//
//	dpx        — public API, no Raft library import
//	dpx/raft   — imports dpx (for shared types) and hashicorp/raft
//	caller     — imports dpx and dpx/raft, passes dpxraft.Open to dpx.Open
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

	"github.com/agberohq/dpx"
	"github.com/agberohq/dpx/engine"
	hraft "github.com/hashicorp/raft"
	"github.com/vmihailenco/msgpack/v5"
)

// FSM-private encoding helpers
// These are implementation details of the FSM. They are not exported and do
// not belong in the parent dpx package.

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

// applyState

type applyState struct {
	keyEpoch map[string]engine.EpochRecord
	applied  uint64
}

// dpxFSM

// dpxFSM implements hraft.BatchingFSM.
//
// HashiCorp Raft guarantees Apply/ApplyBatch are never called concurrently
// with each other, so keyEpoch and the shadow pool need no locking.
// Snapshot() may run concurrently with Persist() but NOT with Apply.
type dpxFSM struct {
	engine     engine.StorageEngine
	state      applyState
	syncPolicy dpx.SyncPolicy

	pool      sync.Pool // reuses shadow maps; cleared per ApplyBatch call
	vkBufPool sync.Pool // reuses version-key construction buffers

	watchers dpx.WatchNotifier // shared with Node; called after each commit
	metrics  *dpx.Metrics
}

func newFSM(
	eng engine.StorageEngine,
	syncPolicy dpx.SyncPolicy,
	watchers dpx.WatchNotifier,
	metrics *dpx.Metrics,
) *dpxFSM {
	return &dpxFSM{
		engine:     eng,
		syncPolicy: syncPolicy,
		watchers:   watchers,
		metrics:    metrics,
		pool: sync.Pool{
			New: func() any { return make(map[string]engine.EpochRecord, 256) },
		},
		vkBufPool: sync.Pool{
			New: func() any { return make([]byte, 0, 256) },
		},
	}
}

// open scans __dpx:ver:* to rebuild keyEpoch and reads __dpx:applied.
// Called once after the Raft node starts, and again inside Restore.
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

// Apply implements hraft.FSM — single committed LogCommand entry.
func (f *dpxFSM) Apply(log *hraft.Log) interface{} {
	if log.Type != hraft.LogCommand {
		return dpx.ApplyResult{}
	}
	return f.applyBatch([]*hraft.Log{log})[0]
}

// ApplyBatch implements hraft.BatchingFSM — multiple entries committed together.
// This recovers the batching behaviour of Dragonboat's Update([]Entry).
// The returned slice must be the same length as the input.
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
			results[i] = dpx.ApplyResult{}
			continue
		}

		var proposal dpx.Proposal
		if err := msgpack.Unmarshal(log.Data, &proposal); err != nil {
			results[i] = dpx.ApplyResult{
				Err: fmt.Errorf("dpx/raft: corrupt entry at index %d: %w", log.Index, err),
			}
			continue
		}

		if f.detectConflict(&proposal, shadow) {
			results[i] = dpx.ApplyResult{Conflict: true}
			continue
		}

		if err := f.applyProposal(log.Index, &proposal, shadow); err != nil {
			results[i] = dpx.ApplyResult{Err: err}
			continue
		}

		results[i] = dpx.ApplyResult{}

		if f.watchers != nil {
			f.watchers.NotifyBatch(proposal.Writes, f.metrics)
		}
	}
	return results
}

func (f *dpxFSM) detectConflict(proposal *dpx.Proposal, shadow map[string]engine.EpochRecord) bool {
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

func (f *dpxFSM) applyProposal(index uint64, proposal *dpx.Proposal, shadow map[string]engine.EpochRecord) error {
	start := time.Now()

	batch := f.engine.NewBatch()
	vkBuf := f.vkBufPool.Get().([]byte)
	defer f.vkBufPool.Put(vkBuf)

	var epochBuf [9]byte
	var appliedBuf [8]byte

	for _, w := range proposal.Writes {
		isCredit := w.Op == dpx.OpCredit
		switch w.Op {
		case dpx.OpSet:
			batch.Set(w.Key, w.Value)
		case dpx.OpDelete:
			batch.Delete(w.Key)
		case dpx.OpCredit, dpx.OpDebit:
			batch.Merge(w.Key, w.Value)
		default:
			return fmt.Errorf("dpx/raft: unknown WriteOp %d at index %d", w.Op, index)
		}

		er := engine.EpochRecord{Epoch: index, IsCredit: isCredit}
		batch.Set(versionKey(vkBuf, w.Key), encodeEpochRecord(er, epochBuf[:]))

		// unsafe.String safe: w.Key is in log.Data which outlives applyBatch.
		shadow[unsafe.String(unsafe.SliceData(w.Key), len(w.Key))] = er

		// Must allocate: Raft may reuse log.Data after Apply returns.
		f.state.keyEpoch[string(w.Key)] = er
	}

	batch.Set(appliedKey, encodeUint64(index, appliedBuf[:]))

	if err := f.engine.ApplyBatch(batch, engine.WriteOptions{Sync: f.syncPolicy == dpx.SyncFull}); err != nil {
		return err
	}
	f.state.applied = index

	if f.syncPolicy == dpx.SyncBatch {
		if err := f.engine.Sync(); err != nil {
			return fmt.Errorf("dpx/raft: WAL sync at index %d: %w", index, err)
		}
	}

	if f.metrics != nil {
		f.metrics.ApplyDurationNs.Add(time.Since(start).Nanoseconds())
	}
	return nil
}

// Snapshot implements hraft.FSM. Must return quickly — Apply is blocked while
// it runs. The actual streaming happens in dpxFSMSnapshot.Persist which runs
// concurrently with Apply.
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

// Restore implements hraft.FSM. Not called concurrently with Apply.
func (f *dpxFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if f.metrics != nil {
		defer f.metrics.SnapshotRecoverTotal.Add(1)
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

// dpxFSMSnapshot

type dpxFSMSnapshot struct {
	dir     string
	metrics *dpx.Metrics
}

func (s *dpxFSMSnapshot) Persist(sink hraft.SnapshotSink) error {
	if s.metrics != nil {
		defer s.metrics.SnapshotSaveTotal.Add(1)
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
