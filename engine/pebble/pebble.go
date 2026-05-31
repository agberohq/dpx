// Package pebble provides a Pebble-backed StorageEngine for DPX.
// Use in production. Supports WAL sync, LSM checkpoints, and the
// int64 Merge operator required for AtomicAdd credit commutativity.
//
// Iterator Key() and Value() copies:
// Pebble's iterator returns slices that point into its internal buffers;
// those slices are invalidated on the next call to Next/Prev/First/Close.
// Both pebbleIter and pebbleConsumerIter copy eagerly in Key() and Value()
// so callers never observe buffer reuse.
package pebble

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/cockroachdb/pebble"
)

// Engine is the Pebble-backed StorageEngine.
// Create with New(dir), then pass to dpx.Open via Config.Engine.
type Engine struct {
	db        *pebble.DB
	dir       string
	telemetry engine.StageRecorder

	// sharedSnap is a pooled read snapshot refreshed every snapshotRefreshInterval.
	// db.NewSnapshot() pins memtable/SST references and was costing 10–22ms per
	// call at 100K keys. Sharing one snapshot across all concurrent readers
	// amortises that cost: GetSnapshot() returns the current shared snapshot
	// wrapped in a no-op closer, paying only an atomic load.
	//
	// Safety: the snapshot is swapped atomically; the old snapshot is closed
	// after snapshotRefreshInterval so any reader that grabbed it before the
	// swap has time to finish. seq is updated alongside the snapshot so
	// Sequence() remains accurate.
	sharedSnap   atomic.Pointer[sharedSnapshot]
	snapshotStop chan struct{}
	snapshotWg   sync.WaitGroup
}

// sharedSnapshot bundles a Pebble snapshot with its sequence number.
type sharedSnapshot struct {
	snap *pebble.Snapshot
	seq  uint64
}

// snapshotRefreshInterval controls how often the shared snapshot is replaced.
// 1ms matches the directProposer's defaultFlushInterval — a new snapshot is
// available within one commit cycle, keeping reads within ~1ms of freshness.
const snapshotRefreshInterval = 1 * time.Millisecond

// New creates a Pebble engine that stores data in dir.
// Open must be called before any reads or writes.
func New(dir string) *Engine {
	return &Engine{dir: dir}
}

// SetTelemetry optionally provides a StageRecorder for internal timing.
func (e *Engine) SetTelemetry(r engine.StageRecorder) {
	e.telemetry = r
}

// Open initialises the Pebble database with production-tuned options.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.dir, 0o750); err != nil {
		return err
	}
	opts := &pebble.Options{
		Merger: Int64Merger,

		// Larger memtable: fewer L0 flushes per second, reducing write stalls.
		// Default is 4MB; 64MB lets us absorb bursts without hitting L0.
		MemTableSize: 64 << 20,

		// Allow more L0 files before triggering compaction. Default is 4.
		// Higher threshold reduces compaction pressure mid-benchmark at the
		// cost of slightly slower reads when L0 is full — acceptable for
		// write-heavy workloads.
		L0CompactionThreshold: 8,
		L0StopWritesThreshold: 24,

		// Cap open file descriptors. Default is unlimited (up to ulimit).
		// Explicit cap prevents fd exhaustion when running many concurrent tests.
		MaxOpenFiles: 1000,

		// Disable the rate limiter for compaction I/O — we want compaction to
		// run as fast as possible so it doesn't block foreground writes.
		// Zero means unlimited; the default 0 already does this but being
		// explicit documents intent.
		DisableWAL: false,
	}
	db, err := pebble.Open(e.dir, opts)
	if err != nil {
		return err
	}
	e.db = db
	e.snapshotStop = make(chan struct{})
	e.refreshSharedSnapshot() // create initial snapshot synchronously
	e.snapshotWg.Add(1)
	go e.snapshotRefreshLoop() // start background refresh goroutine
	return nil
}

// refreshSharedSnapshot atomically replaces the shared snapshot.
// Called from Open() and from snapshotRefreshLoop().
func (e *Engine) refreshSharedSnapshot() {
	seq := e.CurrentSequence()
	snap := e.db.NewSnapshot()
	old := e.sharedSnap.Swap(&sharedSnapshot{snap: snap, seq: seq})
	if old != nil {
		// Close old snapshot after a grace period so in-flight readers finish.
		// A reader that grabbed the old snapshot has at most snapshotRefreshInterval
		// to complete its reads before the snapshot is closed.
		time.AfterFunc(snapshotRefreshInterval*2, func() {
			old.snap.Close()
		})
	}
}

// snapshotRefreshLoop periodically refreshes the shared read snapshot.
// Runs as a background goroutine for the lifetime of the engine.
func (e *Engine) snapshotRefreshLoop() {
	defer e.snapshotWg.Done()
	ticker := time.NewTicker(snapshotRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.refreshSharedSnapshot()
		case <-e.snapshotStop:
			return
		}
	}
}

// Close flushes the WAL and releases all Pebble resources.
func (e *Engine) Close() error {
	if e.db == nil {
		return nil
	}
	if e.snapshotStop != nil {
		close(e.snapshotStop)
		e.snapshotWg.Wait() // wait for refresh goroutine to exit before closing DB
	}
	// Close the current shared snapshot before closing the DB.
	if ss := e.sharedSnap.Load(); ss != nil {
		ss.snap.Close()
	}
	return e.db.Close()
}

// Get returns the current committed value for key.
// Returns engine.ErrKeyNotFound if the key does not exist.
func (e *Engine) Get(key []byte) ([]byte, error) {
	val, closer, err := e.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, engine.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

// GetSnapshot returns the current shared snapshot.
// Cost: one atomic pointer load — no new Pebble snapshot allocation.
// The snapshot is refreshed every snapshotRefreshInterval in the background.
// GetSnapshot returns a private point-in-time snapshot isolated from
// subsequent writes. Each caller gets their own snapshot and must Close() it.
// The background sharedSnap goroutine keeps Pebble's block cache primed so
// reads against this snapshot hit cache rather than disk.
func (e *Engine) GetSnapshot() (engine.Snapshot, error) {
	seq := e.CurrentSequence()
	snap := e.db.NewSnapshot()
	return &pebbleSnapshot{snap: snap, seq: seq, telemetry: e.telemetry}, nil
}

// NewBatch creates a new Pebble write batch.
func (e *Engine) NewBatch() engine.Batch {
	return &pebbleBatch{b: e.db.NewBatch()}
}

// ApplyBatch applies the batch to Pebble atomically.
func (e *Engine) ApplyBatch(batch engine.Batch, opts engine.WriteOptions) error {
	b := batch.(*pebbleBatch)
	wo := pebble.NoSync
	if opts.Sync {
		wo = pebble.Sync
	}
	return e.db.Apply(b.b, wo)
}

// Sync forces a WAL fsync without flushing memtables.
func (e *Engine) Sync() error {
	return e.db.LogData(nil, pebble.Sync)
}

// Compact forces a full LSM compaction. Used by the benchmark runner only.
func (e *Engine) Compact() error {
	if e.db == nil {
		return nil
	}
	if err := e.db.Flush(); err != nil {
		return err
	}
	return e.db.Compact(nil, []byte{0xff}, true)
}

// CurrentSequence reads __dpx:applied from Pebble.
func (e *Engine) CurrentSequence() uint64 {
	val, closer, err := e.db.Get([]byte("__dpx:applied"))
	if err != nil {
		return 0
	}
	defer closer.Close()
	if len(val) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(val)
}

// CreateCheckpoint writes a consistent snapshot of the database to dir.
func (e *Engine) CreateCheckpoint(dir string) error {
	if err := e.db.Flush(); err != nil {
		return err
	}
	return e.db.Checkpoint(dir)
}

// DataDir returns the Pebble data directory.
func (e *Engine) DataDir() string { return e.dir }

// RawIter returns a forward iterator bounded by [start, end).
func (e *Engine) RawIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if e.telemetry != nil {
		matStart = time.Now()
	}
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if e.telemetry != nil {
		e.telemetry.RecordIterMaterialise(time.Since(matStart))
	}
	if err != nil {
		return &errIter{err: err}
	}
	return &pebbleIter{iter: iter}
}

// ── Snapshots ────────────────────────────────────────────────────────────────

// pebbleSharedSnapshot wraps the pooled shared snapshot.
// Close() is a no-op — the snapshot lifecycle is managed by snapshotRefreshLoop.
type pebbleSharedSnapshot struct {
	ss        *sharedSnapshot
	telemetry engine.StageRecorder
}

func (s *pebbleSharedSnapshot) Get(key []byte) ([]byte, error) {
	val, closer, err := s.ss.snap.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, engine.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

func (s *pebbleSharedSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := append(append(make([]byte, 0, 10+len(key)), "__dpx:ver:"...), key...)
	val, closer, err := s.ss.snap.Get(vk)
	if errors.Is(err, pebble.ErrNotFound) {
		return engine.EpochRecord{}, nil
	}
	if err != nil {
		return engine.EpochRecord{}, err
	}
	defer closer.Close()
	return decodeEpochRecord(val), nil
}

func (s *pebbleSharedSnapshot) NewIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if s.telemetry != nil {
		matStart = time.Now()
	}
	iter, err := s.ss.snap.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if s.telemetry != nil {
		s.telemetry.RecordIterMaterialise(time.Since(matStart))
	}
	if err != nil {
		return &errIter{err: err}
	}
	return &pebbleConsumerIter{iter: iter}
}

func (s *pebbleSharedSnapshot) Sequence() uint64 { return s.ss.seq }
func (s *pebbleSharedSnapshot) Close() error     { return nil } // owned by refreshLoop

// pebbleSnapshot — used only for writes that explicitly need a fresh snapshot.
// pebbleSnapshot is returned by GetSnapshot() — private, caller-owned.
type pebbleSnapshot struct {
	snap      *pebble.Snapshot
	seq       uint64
	telemetry engine.StageRecorder
}

func (s *pebbleSnapshot) Get(key []byte) ([]byte, error) {
	val, closer, err := s.snap.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, engine.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

func (s *pebbleSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := append(append(make([]byte, 0, 10+len(key)), "__dpx:ver:"...), key...)
	val, closer, err := s.snap.Get(vk)
	if errors.Is(err, pebble.ErrNotFound) {
		return engine.EpochRecord{}, nil
	}
	if err != nil {
		return engine.EpochRecord{}, err
	}
	defer closer.Close()
	return decodeEpochRecord(val), nil
}

func (s *pebbleSnapshot) NewIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if s.telemetry != nil {
		matStart = time.Now()
	}
	iter, err := s.snap.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if s.telemetry != nil {
		s.telemetry.RecordIterMaterialise(time.Since(matStart))
	}
	if err != nil {
		return &errIter{err: err}
	}
	return &pebbleConsumerIter{iter: iter}
}

func (s *pebbleSnapshot) Sequence() uint64 { return s.seq }
func (s *pebbleSnapshot) Close() error     { return s.snap.Close() }

// ── Iterators ────────────────────────────────────────────────────────────────

type pebbleIter struct{ iter *pebble.Iterator }

func (i *pebbleIter) First() bool   { return i.iter.First() }
func (i *pebbleIter) Next() bool    { return i.iter.Next() }
func (i *pebbleIter) Prev() bool    { return i.iter.Prev() }
func (i *pebbleIter) Valid() bool   { return i.iter.Valid() }
func (i *pebbleIter) Key() []byte   { return append([]byte(nil), i.iter.Key()...) }
func (i *pebbleIter) Value() []byte { return append([]byte(nil), i.iter.Value()...) }
func (i *pebbleIter) Error() error  { return i.iter.Error() }
func (i *pebbleIter) Close() error  { return i.iter.Close() }

type pebbleConsumerIter struct{ iter *pebble.Iterator }

func (i *pebbleConsumerIter) skipReserved() {
	for i.iter.Valid() && isReserved(i.iter.Key()) {
		i.iter.Next()
	}
}

func (i *pebbleConsumerIter) skipReservedBackward() {
	for i.iter.Valid() && isReserved(i.iter.Key()) {
		i.iter.Prev()
	}
}

func (i *pebbleConsumerIter) First() bool {
	if !i.iter.First() {
		return false
	}
	i.skipReserved()
	return i.iter.Valid()
}

func (i *pebbleConsumerIter) Next() bool {
	if !i.iter.Next() {
		return false
	}
	i.skipReserved()
	return i.iter.Valid()
}

func (i *pebbleConsumerIter) Prev() bool {
	if !i.iter.Prev() {
		return false
	}
	i.skipReservedBackward()
	return i.iter.Valid()
}

func (i *pebbleConsumerIter) Valid() bool   { return i.iter.Valid() }
func (i *pebbleConsumerIter) Key() []byte   { return append([]byte(nil), i.iter.Key()...) }
func (i *pebbleConsumerIter) Value() []byte { return append([]byte(nil), i.iter.Value()...) }
func (i *pebbleConsumerIter) Error() error  { return i.iter.Error() }
func (i *pebbleConsumerIter) Close() error  { return i.iter.Close() }

// ── Batch ────────────────────────────────────────────────────────────────────

type pebbleBatch struct{ b *pebble.Batch }

func (b *pebbleBatch) Set(key, value []byte)   { b.b.Set(key, value, nil) }
func (b *pebbleBatch) Delete(key []byte)       { b.b.Delete(key, nil) }
func (b *pebbleBatch) Merge(key, value []byte) { b.b.Merge(key, value, nil) }
func (b *pebbleBatch) Reset()                  { b.b.Reset() }

// ── Int64Merger ──────────────────────────────────────────────────────────────

var Int64Merger = &pebble.Merger{
	Name: "dpx.int64add",
	Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		m := &int64Merger{}
		if len(value) >= 8 {
			m.sum = int64(binary.LittleEndian.Uint64(value))
		}
		return m, nil
	},
}

type int64Merger struct{ sum int64 }

func (m *int64Merger) MergeNewer(value []byte) error {
	if len(value) >= 8 {
		m.sum += int64(binary.LittleEndian.Uint64(value))
	}
	return nil
}

func (m *int64Merger) MergeOlder(value []byte) error { return m.MergeNewer(value) }

func (m *int64Merger) Finish(_ bool) ([]byte, io.Closer, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(m.sum))
	return b, nil, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func isReserved(key []byte) bool {
	const prefix = "__dpx:"
	return len(key) >= len(prefix) && string(key[:len(prefix)]) == prefix
}

func decodeEpochRecord(b []byte) engine.EpochRecord {
	if len(b) < 9 {
		return engine.EpochRecord{}
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b),
		IsCredit: b[8] == 1,
	}
}

type errIter struct{ err error }

func (i *errIter) First() bool   { return false }
func (i *errIter) Next() bool    { return false }
func (i *errIter) Prev() bool    { return false }
func (i *errIter) Valid() bool   { return false }
func (i *errIter) Key() []byte   { return nil }
func (i *errIter) Value() []byte { return nil }
func (i *errIter) Error() error  { return i.err }
func (i *errIter) Close() error  { return nil }
