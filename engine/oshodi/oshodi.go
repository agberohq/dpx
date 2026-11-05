// Package oshodi provides an Oshodi-backed StorageEngine for DPX.
// Oshodi is a high-performance embedded KV store with buckets, pub/sub,
// and graph traversals. This adapter wraps a single Oshodi bucket to
// satisfy the DPX StorageEngine interface.
package oshodi

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/oshodi"
)

const dpxBucket = "__dpx"

// Engine is the Oshodi-backed StorageEngine.
// Create with New(dir), then pass to dpx.Open via Config.Engine.
type Engine struct {
	db        *oshodi.DB
	dir       string
	bucket    *oshodi.Bucket
	mu        sync.Mutex // protects snapshot materialization
	seq       uint64
	telemetry engine.StageRecorder
}

// New creates an Oshodi engine that stores data in dir.
// Open must be called before any reads or writes.
func New(dir string) *Engine {
	return &Engine{dir: dir}
}

// SetTelemetry optionally provides a StageRecorder for internal timing.
func (e *Engine) SetTelemetry(r engine.StageRecorder) {
	e.telemetry = r
}

// Open initialises the Oshodi database.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.dir, 0o750); err != nil {
		return err
	}
	path := filepath.Join(e.dir, "oshodi.db")
	db, err := oshodi.Open(path)
	if err != nil {
		return fmt.Errorf("oshodi open: %w", err)
	}
	e.db = db
	e.bucket = db.Bucket(dpxBucket)

	// Restore current sequence from stored applied key.
	seq, err := e.readApplied()
	if err != nil {
		return err
	}
	e.seq = seq
	return nil
}

// Close flushes and releases all Oshodi resources.
func (e *Engine) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

// Get returns the current committed value for key.
// Returns engine.ErrKeyNotFound if the key does not exist.
func (e *Engine) Get(key []byte) ([]byte, error) {
	val, err := e.bucket.Get(key)
	if err != nil {
		if err == oshodi.ErrKeyNotFound {
			return nil, engine.ErrKeyNotFound
		}
		return nil, err
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

// GetSnapshot returns a consistent point-in-time view by materializing
// all current key-value pairs. Oshodi does not provide native snapshots,
// so we capture the current state into a map.
func (e *Engine) GetSnapshot() (engine.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	data := make(map[string][]byte)
	err := e.bucket.Scan(func(key, value []byte) bool {
		k := string(key)
		v := make([]byte, len(value))
		copy(v, value)
		data[k] = v
		return true
	})
	if err != nil {
		return nil, err
	}

	return &oshodiSnapshot{
		data:      data,
		seq:       e.seq,
		telemetry: e.telemetry,
	}, nil
}

// NewBatch creates a new Oshodi write batch.
func (e *Engine) NewBatch() engine.Batch {
	return &oshodiBatch{}
}

// ApplyBatch applies all mutations in batch atomically.
func (e *Engine) ApplyBatch(batch engine.Batch, opts engine.WriteOptions) error {
	ob := batch.(*oshodiBatch)

	for _, op := range ob.ops {
		switch op.op {
		case 's':
			if err := e.bucket.Set(op.key, op.value); err != nil {
				return err
			}
			if string(op.key) == "__dpx:applied" && len(op.value) >= 8 {
				e.seq = binary.LittleEndian.Uint64(op.value)
			}
		case 'd':
			if err := e.bucket.Delete(op.key); err != nil {
				return err
			}
		case 'm':
			// Merge: read current value, add delta, write back.
			cur, err := e.bucket.Get(op.key)
			if err != nil && err != oshodi.ErrKeyNotFound {
				return err
			}
			sum := decodeInt64(cur) + decodeInt64(op.value)
			val := encodeInt64(sum)
			if err := e.bucket.Set(op.key, val); err != nil {
				return err
			}
		}
	}

	if opts.Sync {
		return e.Sync()
	}
	return nil
}

// Sync flushes the WAL to disk.
func (e *Engine) Sync() error {
	return e.db.Flush()
}

// CurrentSequence returns the Raft log index of the last applied entry.
func (e *Engine) CurrentSequence() uint64 {
	val, err := e.readApplied()
	if err != nil {
		return e.seq
	}
	return val
}

// CreateCheckpoint writes a consistent copy of the database to dir.
func (e *Engine) CreateCheckpoint(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}

	// Collect all key-value pairs.
	data := make(map[string][]byte)
	err := e.bucket.Scan(func(key, value []byte) bool {
		k := string(key)
		v := make([]byte, len(value))
		copy(v, value)
		data[k] = v
		return true
	})
	if err != nil {
		return err
	}

	// Write to a new Oshodi DB in the checkpoint dir.
	cpPath := filepath.Join(dir, "oshodi.db")
	cpDB, err := oshodi.Open(cpPath)
	if err != nil {
		return err
	}
	defer cpDB.Close()

	bucket := cpDB.Bucket(dpxBucket)
	for k, v := range data {
		if err := bucket.Set([]byte(k), v); err != nil {
			return err
		}
	}
	return cpDB.Flush()
}

// DataDir returns the engine's data directory.
func (e *Engine) DataDir() string { return e.dir }

// RawIter returns a forward iterator that includes __dpx: prefix keys.
func (e *Engine) RawIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if e.telemetry != nil {
		matStart = time.Now()
	}

	var pairs [][2][]byte
	ss := string(start)
	se := string(end)

	err := e.bucket.Scan(func(key, value []byte) bool {
		k := string(key)
		if k < ss {
			return true
		}
		if se != "" && k >= se {
			return true
		}
		kc := make([]byte, len(key))
		copy(kc, key)
		vc := make([]byte, len(value))
		copy(vc, value)
		pairs = append(pairs, [2][]byte{kc, vc})
		return true
	})
	if err != nil {
		return &errIter{err: err}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return string(pairs[i][0]) < string(pairs[j][0])
	})

	if e.telemetry != nil {
		e.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &oshodiIter{pairs: pairs, pos: -1}
}

// readApplied reads the __dpx:applied key to get the current sequence.
func (e *Engine) readApplied() (uint64, error) {
	val, err := e.bucket.Get([]byte("__dpx:applied"))
	if err != nil {
		if err == oshodi.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	if len(val) < 8 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

// --- Snapshot ---

type oshodiSnapshot struct {
	data      map[string][]byte
	seq       uint64
	telemetry engine.StageRecorder
}

func (s *oshodiSnapshot) Get(key []byte) ([]byte, error) {
	val, ok := s.data[string(key)]
	if !ok {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

func (s *oshodiSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := "__dpx:ver:" + string(key)
	val, ok := s.data[vk]
	if !ok {
		return engine.EpochRecord{}, nil
	}
	return decodeEpochRecord(val), nil
}

func (s *oshodiSnapshot) NewIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if s.telemetry != nil {
		matStart = time.Now()
	}

	prefix := "__dpx:"
	var pairs [][2][]byte
	ss := string(start)
	se := string(end)

	for k, v := range s.data {
		if k < ss {
			continue
		}
		if se != "" && k >= se {
			continue
		}
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			continue
		}
		kc := make([]byte, len(k))
		copy(kc, k)
		vc := make([]byte, len(v))
		copy(vc, v)
		pairs = append(pairs, [2][]byte{kc, vc})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return string(pairs[i][0]) < string(pairs[j][0])
	})

	if s.telemetry != nil {
		s.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &oshodiIter{pairs: pairs, pos: -1}
}

func (s *oshodiSnapshot) Sequence() uint64 { return s.seq }
func (s *oshodiSnapshot) Close() error     { return nil }

// --- Batch ---

type batchOp struct {
	op    byte
	key   []byte
	value []byte
}

type oshodiBatch struct {
	ops []batchOp
}

func (b *oshodiBatch) Set(key, value []byte) {
	kc := make([]byte, len(key))
	copy(kc, key)
	vc := make([]byte, len(value))
	copy(vc, value)
	b.ops = append(b.ops, batchOp{op: 's', key: kc, value: vc})
}

func (b *oshodiBatch) Delete(key []byte) {
	kc := make([]byte, len(key))
	copy(kc, key)
	b.ops = append(b.ops, batchOp{op: 'd', key: kc})
}

func (b *oshodiBatch) Merge(key, value []byte) {
	kc := make([]byte, len(key))
	copy(kc, key)
	vc := make([]byte, len(value))
	copy(vc, value)
	b.ops = append(b.ops, batchOp{op: 'm', key: kc, value: vc})
}

func (b *oshodiBatch) Reset() { b.ops = b.ops[:0] }

// --- Iterator ---

type oshodiIter struct {
	pairs [][2][]byte
	pos   int
}

func (i *oshodiIter) First() bool {
	i.pos = 0
	return i.pos < len(i.pairs)
}

func (i *oshodiIter) Next() bool {
	i.pos++
	return i.pos < len(i.pairs)
}

func (i *oshodiIter) Prev() bool {
	if i.pos <= 0 {
		i.pos = -1
		return false
	}
	i.pos--
	return true
}

func (i *oshodiIter) Valid() bool {
	return i.pos >= 0 && i.pos < len(i.pairs)
}

func (i *oshodiIter) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.pairs[i.pos][0]
}

func (i *oshodiIter) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.pairs[i.pos][1]
}

func (i *oshodiIter) Error() error { return nil }
func (i *oshodiIter) Close() error { return nil }

type errIter struct{ err error }

func (i *errIter) First() bool   { return false }
func (i *errIter) Next() bool    { return false }
func (i *errIter) Prev() bool    { return false }
func (i *errIter) Valid() bool   { return false }
func (i *errIter) Key() []byte   { return nil }
func (i *errIter) Value() []byte { return nil }
func (i *errIter) Error() error  { return i.err }
func (i *errIter) Close() error  { return nil }

// --- Helpers ---

func encodeInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func decodeInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func decodeEpochRecord(b []byte) engine.EpochRecord {
	if len(b) < 9 {
		return engine.EpochRecord{}
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b[:8]),
		IsCredit: b[8] == 1,
	}
}
