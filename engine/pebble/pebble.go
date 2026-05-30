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

	"github.com/agberohq/dpx/engine"
	"github.com/cockroachdb/pebble"
)

// Engine is the Pebble-backed StorageEngine.
// Create with New(dir), then pass to dpx.Open via Config.Engine.
type Engine struct {
	db  *pebble.DB
	dir string
}

// New creates a Pebble engine that stores data in dir.
// Open must be called before any reads or writes.
func New(dir string) *Engine {
	return &Engine{dir: dir}
}

// Open initialises the Pebble database.
// Registers Int64Merger ("dpx.int64add") — name is locked in the Pebble
// MANIFEST at this point and cannot be changed without a full data migration.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.dir, 0o750); err != nil {
		return err
	}
	opts := &pebble.Options{Merger: Int64Merger}
	db, err := pebble.Open(e.dir, opts)
	if err != nil {
		return err
	}
	e.db = db
	return nil
}

// Close flushes the WAL and releases all Pebble resources.
func (e *Engine) Close() error {
	if e.db == nil {
		return nil
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

// GetSnapshot returns a consistent Pebble snapshot.
// Sequence() on the returned snapshot equals CurrentSequence() at call time.
func (e *Engine) GetSnapshot() (engine.Snapshot, error) {
	seq := e.CurrentSequence()
	snap := e.db.NewSnapshot()
	return &pebbleSnapshot{snap: snap, seq: seq}, nil
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
// LogData(nil, pebble.Sync) writes a zero-length WAL record with sync=true,
// which is the correct Pebble idiom for WAL-only durability.
func (e *Engine) Sync() error {
	return e.db.LogData(nil, pebble.Sync)
}

// CurrentSequence reads __dpx:applied from Pebble.
// Returns 0 on a fresh database or if the key has never been written.
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
//
// Pebble's Checkpoint captures only SSTable files (not the active WAL).
// Any data sitting in the memtable would be missing from the checkpoint.
// We therefore call Flush() first to push all memtable data into SSTables,
// then Checkpoint captures a complete, fully-readable copy.
//
// Flush() is a synchronous write barrier — it returns only after all
// current memtable contents are on disk as an SSTable. The checkpoint
// itself uses hard links (copy-on-write) and is near-instantaneous.
func (e *Engine) CreateCheckpoint(dir string) error {
	if err := e.db.Flush(); err != nil {
		return err
	}
	return e.db.Checkpoint(dir)
}

// DataDir returns the Pebble data directory.
func (e *Engine) DataDir() string { return e.dir }

// RawIter returns a forward iterator bounded by [start, end) that includes
// __dpx: prefix keys. Used only by StateMachine.Open() to rebuild keyEpoch.
//
// The end bound uses 16 × 0xFF bytes to guarantee coverage of any user key
// byte value, including bytes > 0x7E that would be excluded by "~" (0x7E).
func (e *Engine) RawIter(start, end []byte) engine.Iterator {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return &errIter{err: err}
	}
	return &pebbleIter{iter: iter}
}

// pebbleSnapshot

type pebbleSnapshot struct {
	snap *pebble.Snapshot
	seq  uint64
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
	iter, err := s.snap.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return &errIter{err: err}
	}
	return &pebbleConsumerIter{iter: iter}
}

func (s *pebbleSnapshot) Sequence() uint64 { return s.seq }
func (s *pebbleSnapshot) Close() error     { return s.snap.Close() }

// pebbleIter (raw; includes __dpx: keys)

// pebbleIter wraps a pebble.Iterator and copies Key/Value eagerly.
// Pebble's iterator invalidates Key() and Value() buffers on the next
// positioning call (Next, Prev, First, Close). Copying here means callers
// can retain Key()/Value() slices across calls without unsafe aliasing.
type pebbleIter struct{ iter *pebble.Iterator }

func (i *pebbleIter) First() bool   { return i.iter.First() }
func (i *pebbleIter) Next() bool    { return i.iter.Next() }
func (i *pebbleIter) Prev() bool    { return i.iter.Prev() }
func (i *pebbleIter) Valid() bool   { return i.iter.Valid() }
func (i *pebbleIter) Key() []byte   { return append([]byte(nil), i.iter.Key()...) }
func (i *pebbleIter) Value() []byte { return append([]byte(nil), i.iter.Value()...) }
func (i *pebbleIter) Error() error  { return i.iter.Error() }
func (i *pebbleIter) Close() error  { return i.iter.Close() }

// pebbleConsumerIter (skips __dpx: keys)

// pebbleConsumerIter wraps pebble.Iterator and skips reserved keys.
// Key() and Value() copy eagerly for the same reason as pebbleIter.
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

// pebbleBatch

type pebbleBatch struct{ b *pebble.Batch }

func (b *pebbleBatch) Set(key, value []byte)   { b.b.Set(key, value, nil) }
func (b *pebbleBatch) Delete(key []byte)       { b.b.Delete(key, nil) }
func (b *pebbleBatch) Merge(key, value []byte) { b.b.Merge(key, value, nil) }
func (b *pebbleBatch) Reset()                  { b.b.Reset() }

// Int64Merger

// Int64Merger is the Pebble Merger for AtomicAdd credit commutativity.
// Name "dpx.int64add" is locked in the Pebble MANIFEST at Open time.
// Changing it requires a full data migration.
//
// nil base value (non-existent or deleted key): Merge receives nil → sum = 0.
// This means Credit on a non-existent key creates it with the delta as value,
// and Delete followed by Credit resurrects the key with just the delta.
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

// MergeOlder delegates to MergeNewer because int64 addition is commutative.
func (m *int64Merger) MergeOlder(value []byte) error {
	return m.MergeNewer(value)
}

func (m *int64Merger) Finish(_ bool) ([]byte, io.Closer, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(m.sum))
	return b, nil, nil
}

// helpers

// isReserved reports whether key starts with the __dpx: prefix.
func isReserved(key []byte) bool {
	const prefix = "__dpx:"
	return len(key) >= len(prefix) && string(key[:len(prefix)]) == prefix
}

// decodeEpochRecord reads a 9-byte slice into an EpochRecord.
// Returns zero value for nil or short input.
func decodeEpochRecord(b []byte) engine.EpochRecord {
	if len(b) < 9 {
		return engine.EpochRecord{}
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b),
		IsCredit: b[8] == 1,
	}
}

// errIter

// errIter is returned when iterator construction fails.
// It immediately reports the error via Error() and is never Valid().
type errIter struct{ err error }

func (i *errIter) First() bool   { return false }
func (i *errIter) Next() bool    { return false }
func (i *errIter) Prev() bool    { return false }
func (i *errIter) Valid() bool   { return false }
func (i *errIter) Key() []byte   { return nil }
func (i *errIter) Value() []byte { return nil }
func (i *errIter) Error() error  { return i.err }
func (i *errIter) Close() error  { return nil }
