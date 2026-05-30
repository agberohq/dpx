// Package memory provides an in-memory StorageEngine for DPX.
// Intended for unit tests and CI. Not for production use.
//
// Uses OCC with no global mutex — exercises the same retry paths as the
// Pebble engine so tests catch real concurrency bugs.
//
// GetSnapshot is O(N) (full map clone). Acceptable for test scale (<100k keys).
package memory

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/agberohq/dpx/engine"
	"github.com/vmihailenco/msgpack/v5"
)

// Engine is the in-memory StorageEngine.
// Create with New(), then pass to dpx.Open via Config.Engine.
type Engine struct {
	mu       sync.RWMutex
	data     map[string][]byte
	versions map[string]engine.EpochRecord
	applied  uint64
	keys     []string // sorted; maintained by applyBatch
	dir      string
}

// New creates an in-memory engine.
func New() *Engine {
	// Use a unique subdirectory so that Restore()'s os.RemoveAll(DataDir())
	// doesn't try to delete the system temp directory.
	dir, _ := os.MkdirTemp("", "dpx-memory-*")
	return &Engine{
		data:     make(map[string][]byte),
		versions: make(map[string]engine.EpochRecord),
		keys:     make([]string, 0, 1024),
		dir:      dir,
	}
}

func (e *Engine) Open() error {
	// Load state from a previous CreateCheckpoint if one exists in DataDir.
	path := filepath.Join(e.dir, "dump")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh engine
		}
		return err
	}
	type dump struct {
		Data     map[string][]byte
		Versions map[string]engine.EpochRecord
		Applied  uint64
	}
	var d dump
	if err := msgpack.Unmarshal(b, &d); err != nil {
		return fmt.Errorf("memory engine: load dump: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data = d.Data
	if e.data == nil {
		e.data = make(map[string][]byte)
	}
	e.versions = d.Versions
	if e.versions == nil {
		e.versions = make(map[string]engine.EpochRecord)
	}
	e.applied = d.Applied
	// Rebuild sorted keys index.
	e.keys = make([]string, 0, len(e.data))
	for k := range e.data {
		e.keys = append(e.keys, k)
	}
	sort.Strings(e.keys)
	return nil
}
func (e *Engine) DataDir() string { return e.dir }
func (e *Engine) Close() error    { return nil }
func (e *Engine) Sync() error     { return nil }

func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	v := e.data[string(key)]
	e.mu.RUnlock()
	if v == nil {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (e *Engine) GetSnapshot() (engine.Snapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	data := make(map[string][]byte, len(e.data))
	for k, v := range e.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		data[k] = cp
	}
	vers := make(map[string]engine.EpochRecord, len(e.versions))
	for k, v := range e.versions {
		vers[k] = v
	}
	keys := make([]string, len(e.keys))
	copy(keys, e.keys)

	return &memSnapshot{
		data:     data,
		versions: vers,
		keys:     keys,
		seq:      e.applied,
	}, nil
}

func (e *Engine) NewBatch() engine.Batch { return &memBatch{} }

type memOp struct {
	op    byte // 's'=set, 'd'=delete, 'm'=merge
	key   string
	value []byte
}

type memBatch struct{ ops []memOp }

func (b *memBatch) Set(key, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.ops = append(b.ops, memOp{op: 's', key: string(key), value: cp})
}

func (b *memBatch) Delete(key []byte) {
	b.ops = append(b.ops, memOp{op: 'd', key: string(key)})
}

func (b *memBatch) Merge(key, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.ops = append(b.ops, memOp{op: 'm', key: string(key), value: cp})
}

func (b *memBatch) Reset() { b.ops = b.ops[:0] }

func (e *Engine) ApplyBatch(batch engine.Batch, _ engine.WriteOptions) error {
	b := batch.(*memBatch)
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, op := range b.ops {
		switch op.op {
		case 's':
			e.data[op.key] = op.value
			e.insertKey(op.key)
		case 'd':
			delete(e.data, op.key)
			e.deleteKey(op.key)
		case 'm':
			e.data[op.key] = mergeInt64(e.data[op.key], op.value)
			e.insertKey(op.key)
		}
	}

	// Route __dpx: keys to in-memory metadata fields.
	for _, op := range b.ops {
		if op.key == "__dpx:applied" && op.op == 's' && len(op.value) >= 8 {
			e.applied = binary.LittleEndian.Uint64(op.value)
		}
		if len(op.key) > 10 && op.key[:10] == "__dpx:ver:" && op.op == 's' {
			userKey := op.key[10:]
			if len(op.value) >= 9 {
				e.versions[userKey] = engine.EpochRecord{
					Epoch:    binary.LittleEndian.Uint64(op.value),
					IsCredit: op.value[8] == 1,
				}
			}
		}
	}
	return nil
}

func mergeInt64(existing, delta []byte) []byte {
	var cur int64
	if len(existing) >= 8 {
		cur = int64(binary.LittleEndian.Uint64(existing))
	}
	var d int64
	if len(delta) >= 8 {
		d = int64(binary.LittleEndian.Uint64(delta))
	}
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(cur+d))
	return b
}

func (e *Engine) CurrentSequence() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.applied
}

func (e *Engine) CreateCheckpoint(dir string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	type dump struct {
		Data     map[string][]byte
		Versions map[string]engine.EpochRecord
		Applied  uint64
	}
	b, err := msgpack.Marshal(dump{Data: e.data, Versions: e.versions, Applied: e.applied})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "dump"), b, 0o600)
}

func (e *Engine) RawIter(start, end []byte) engine.Iterator {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, en := string(start), string(end)

	startIdx := sort.SearchStrings(e.keys, s)

	count := 0
	for i := startIdx; i < len(e.keys); i++ {
		k := e.keys[i]
		if en != "" && k >= en {
			break
		}
		count++
	}

	pairs := make([][2][]byte, 0, count)
	for i := startIdx; i < len(e.keys); i++ {
		k := e.keys[i]
		if en != "" && k >= en {
			break
		}
		v := e.data[k]
		kcp := []byte(k)
		vcp := make([]byte, len(v))
		copy(vcp, v)
		pairs = append(pairs, [2][]byte{kcp, vcp})
	}
	return &memIter{pairs: pairs, idx: -1}
}

// sorted key index

func (e *Engine) insertKey(k string) {
	i := sort.SearchStrings(e.keys, k)
	if i < len(e.keys) && e.keys[i] == k {
		return
	}
	e.keys = append(e.keys, "")
	copy(e.keys[i+1:], e.keys[i:])
	e.keys[i] = k
}

func (e *Engine) deleteKey(k string) {
	i := sort.SearchStrings(e.keys, k)
	if i >= len(e.keys) || e.keys[i] != k {
		return
	}
	e.keys = append(e.keys[:i], e.keys[i+1:]...)
}

// memSnapshot

type memSnapshot struct {
	data     map[string][]byte
	versions map[string]engine.EpochRecord
	keys     []string
	seq      uint64
}

func (s *memSnapshot) Get(key []byte) ([]byte, error) {
	v := s.data[string(key)]
	if v == nil {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (s *memSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	return s.versions[string(key)], nil
}

func (s *memSnapshot) NewIter(start, end []byte) engine.Iterator {
	st, en := string(start), string(end)

	// Binary search to the first key >= start, skipping O(N) linear prefix scan.
	startIdx := sort.SearchStrings(s.keys, st)

	// Count matching keys to pre-allocate the pairs slice (eliminates realloc).
	count := 0
	for i := startIdx; i < len(s.keys); i++ {
		k := s.keys[i]
		if en != "" && k >= en {
			break
		}
		if !isReserved(k) {
			count++
		}
	}

	pairs := make([][2][]byte, 0, count)
	for i := startIdx; i < len(s.keys); i++ {
		k := s.keys[i]
		if en != "" && k >= en {
			break
		}
		if isReserved(k) {
			continue
		}
		v := s.data[k]
		kcp := []byte(k)
		vcp := make([]byte, len(v))
		copy(vcp, v)
		pairs = append(pairs, [2][]byte{kcp, vcp})
	}
	return &memIter{pairs: pairs, idx: -1}
}

func (s *memSnapshot) Sequence() uint64 { return s.seq }
func (s *memSnapshot) Close() error     { return nil }

func isReserved(key string) bool {
	return len(key) >= 6 && key[:6] == "__dpx:"
}

// memIter

type memIter struct {
	pairs [][2][]byte
	idx   int
}

func (i *memIter) First() bool  { i.idx = 0; return i.idx < len(i.pairs) }
func (i *memIter) Next() bool   { i.idx++; return i.idx < len(i.pairs) }
func (i *memIter) Prev() bool   { i.idx--; return i.idx >= 0 }
func (i *memIter) Valid() bool  { return i.idx >= 0 && i.idx < len(i.pairs) }
func (i *memIter) Error() error { return nil }
func (i *memIter) Close() error { return nil }

func (i *memIter) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.pairs[i.idx][0]
}

func (i *memIter) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.pairs[i.idx][1]
}

// decodeEpochRecord reads a 9-byte slice into an EpochRecord.
// Returns zero value for nil or short input.
// Format: [epoch: 8 bytes LE][isCredit: 1 byte].
func decodeEpochRecord(b []byte) engine.EpochRecord {
	if len(b) < 9 {
		return engine.EpochRecord{}
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b),
		IsCredit: b[8] == 1,
	}
}
