// Package badger provides a Badger-backed StorageEngine for DPX.
//
// Badger is a pure-Go embedded key-value store with no CGo dependency,
// making it suitable for environments where building Pebble's C dependencies
// is not possible. The trade-off is lower raw throughput compared to Pebble.
//
// Badger does not have a native Merge operator equivalent to Pebble's.
// AtomicAdd (credit/debit) is implemented via read-modify-write inside a
// Badger transaction, which is safe because all writes are serialised through
// the Raft log — ApplyBatch is called from a single goroutine (the Raft
// state machine's Update loop).
//
// Iterator Key() and Value():
// Badger's iterator returns item handles whose val/key bytes are only valid
// during the transaction. We copy eagerly in Key() and Value() so callers
// can retain slices across positioning calls.
package badger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// Engine is the Badger-backed StorageEngine.
// Create with New(dir), then pass to dpx.Open via Config.Engine.
type Engine struct {
	db        *badger.DB
	dir       string
	telemetry engine.StageRecorder
}

// New creates a Badger engine that stores data in dir.
// Open must be called before any reads or writes.
func New(dir string) *Engine {
	return &Engine{dir: dir}
}

// SetTelemetry optionally provides a StageRecorder for internal timing.
func (e *Engine) SetTelemetry(r engine.StageRecorder) {
	e.telemetry = r
}

// Open initialises the Badger database.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.dir, 0o750); err != nil {
		return err
	}
	opts := badger.DefaultOptions(e.dir).
		WithLogger(nil) // suppress Badger's default stderr logging
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	e.db = db
	return nil
}

// Close flushes pending writes and releases all Badger resources.
func (e *Engine) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

// Get returns the current committed value for key.
// Returns engine.ErrKeyNotFound if the key does not exist.
func (e *Engine) Get(key []byte) ([]byte, error) {
	var val []byte
	err := e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return engine.ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

// GetSnapshot returns a consistent Badger snapshot (a read-only transaction).
func (e *Engine) GetSnapshot() (engine.Snapshot, error) {
	seq := e.CurrentSequence()
	txn := e.db.NewTransaction(false) // read-only
	return &badgerSnapshot{txn: txn, seq: seq, telemetry: e.telemetry}, nil
}

// NewBatch creates a write batch for this engine.
func (e *Engine) NewBatch() engine.Batch {
	return &badgerBatch{}
}

// ApplyBatch applies all mutations in the batch atomically.
// Merge (AtomicAdd) is implemented as read-modify-write inside a single
// Badger transaction. This is safe because ApplyBatch is only ever called
// from the single-goroutine Raft state machine Update loop.
func (e *Engine) ApplyBatch(batch engine.Batch, _ engine.WriteOptions) error {
	b := batch.(*badgerBatch)
	txn := e.db.NewTransaction(true) // read-write
	defer txn.Discard()

	for _, op := range b.ops {
		switch op.typ {
		case opSet:
			if err := txn.Set([]byte(op.key), op.value); err != nil {
				return err
			}
		case opDelete:
			if err := txn.Delete([]byte(op.key)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		case opMerge:
			// Read current value, add delta, write back.
			var cur int64
			item, err := txn.Get([]byte(op.key))
			if err == nil {
				val, err2 := item.ValueCopy(nil)
				if err2 != nil {
					return err2
				}
				if len(val) >= 8 {
					cur = int64(binary.LittleEndian.Uint64(val))
				}
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			var delta int64
			if len(op.value) >= 8 {
				delta = int64(binary.LittleEndian.Uint64(op.value))
			}
			result := make([]byte, 8)
			binary.LittleEndian.PutUint64(result, uint64(cur+delta))
			if err := txn.Set([]byte(op.key), result); err != nil {
				return err
			}
		}
	}

	// Update applied sequence.
	for _, op := range b.ops {
		if op.typ == opSet && op.key == "__dpx:applied" && len(op.value) >= 8 {
			// Already written above via the Set loop.
			break
		}
	}

	return txn.Commit()
}

// Sync flushes Badger's value log to disk.
func (e *Engine) Sync() error {
	return e.db.Sync()
}

// CurrentSequence reads __dpx:applied from Badger.
// Returns 0 on a fresh database.
func (e *Engine) CurrentSequence() uint64 {
	val, err := e.Get([]byte("__dpx:applied"))
	if err != nil || len(val) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(val)
}

// CreateCheckpoint serialises the current database state to dir/dump as msgpack.
// Badger's built-in backup streams to an io.Writer; we capture it to a file.
//
// Unlike Pebble's hard-link checkpoint, this is a full data copy.
// For large databases this may be slow — use Pebble for production workloads
// where checkpoint speed matters.
func (e *Engine) CreateCheckpoint(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}

	// Collect all key-value pairs into a map and serialise as msgpack.
	// This matches the memory engine format and makes restore straightforward.
	data := make(map[string][]byte)
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.KeyCopy(nil))
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			data[k] = v
		}
		return nil
	})
	if err != nil {
		return err
	}

	b, err := msgpack.Marshal(data)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "dump"), b, 0o600)
}

// DataDir returns the Badger data directory.
func (e *Engine) DataDir() string { return e.dir }

// RawIter returns a forward iterator over [start, end) that includes
// __dpx: prefix keys. Used only by StateMachine.Open() to rebuild keyEpoch.
func (e *Engine) RawIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if e.telemetry != nil {
		matStart = time.Now()
	}

	// Collect matching pairs under a snapshot transaction.
	var pairs [][2][]byte
	e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			if len(end) > 0 && bytes.Compare(k, end) >= 0 {
				break
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			pairs = append(pairs, [2][]byte{k, v})
		}
		return nil
	})

	if e.telemetry != nil {
		e.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &badgerIter{pairs: pairs, idx: -1}
}

// badgerSnapshot

type badgerSnapshot struct {
	txn       *badger.Txn
	seq       uint64
	telemetry engine.StageRecorder
}

func (s *badgerSnapshot) Get(key []byte) ([]byte, error) {
	item, err := s.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, engine.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (s *badgerSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := append(append(make([]byte, 0, 10+len(key)), "__dpx:ver:"...), key...)
	item, err := s.txn.Get(vk)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return engine.EpochRecord{}, nil
	}
	if err != nil {
		return engine.EpochRecord{}, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return engine.EpochRecord{}, err
	}
	return decodeEpochRecord(val), nil
}

func (s *badgerSnapshot) NewIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if s.telemetry != nil {
		matStart = time.Now()
	}

	// Materialise the range under the snapshot transaction.
	var pairs [][2][]byte
	opts := badger.DefaultIteratorOptions
	it := s.txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(start); it.Valid(); it.Next() {
		item := it.Item()
		k := item.KeyCopy(nil)
		if len(end) > 0 && bytes.Compare(k, end) >= 0 {
			break
		}
		if isReserved(k) {
			continue
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			break
		}
		pairs = append(pairs, [2][]byte{k, v})
	}

	if s.telemetry != nil {
		s.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &badgerIter{pairs: pairs, idx: -1}
}

func (s *badgerSnapshot) Sequence() uint64 { return s.seq }
func (s *badgerSnapshot) Close() error     { s.txn.Discard(); return nil }

// badgerBatch

type opType byte

const (
	opSet    opType = 's'
	opDelete opType = 'd'
	opMerge  opType = 'm'
)

type batchOp struct {
	typ   opType
	key   string
	value []byte
}

type badgerBatch struct{ ops []batchOp }

func (b *badgerBatch) Set(key, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.ops = append(b.ops, batchOp{typ: opSet, key: string(key), value: cp})
}

func (b *badgerBatch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{typ: opDelete, key: string(key)})
}

func (b *badgerBatch) Merge(key, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.ops = append(b.ops, batchOp{typ: opMerge, key: string(key), value: cp})
}

func (b *badgerBatch) Reset() { b.ops = b.ops[:0] }

// badgerIter

// badgerIter is a materialised iterator backed by a pre-collected slice.
// We materialise eagerly because Badger's transaction-bound iterator cannot
// be held open after the transaction commits or discards.
type badgerIter struct {
	pairs [][2][]byte
	idx   int
}

func (i *badgerIter) First() bool  { i.idx = 0; return i.idx < len(i.pairs) }
func (i *badgerIter) Next() bool   { i.idx++; return i.idx < len(i.pairs) }
func (i *badgerIter) Prev() bool   { i.idx--; return i.idx >= 0 }
func (i *badgerIter) Valid() bool  { return i.idx >= 0 && i.idx < len(i.pairs) }
func (i *badgerIter) Error() error { return nil }
func (i *badgerIter) Close() error { return nil }

func (i *badgerIter) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.pairs[i.idx][0]
}

func (i *badgerIter) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.pairs[i.idx][1]
}

// helpers

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
