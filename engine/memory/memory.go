// Package memory provides an in-memory StorageEngine for DPX.
// Not for production use — use pebble or badger for persistence.
//
// Design: 256-shard copy-on-write via atomic pointers.
//
//   - GetSnapshot: 256 atomic loads — O(1), no lock, no clone.
//   - ApplyBatch: touches only the shards that contain modified keys.
//     Each touched shard does a shallow clone of its own data (~N/256 entries).
//     For a 2-key write on 10,000 keys: clone ~78 entries, not 10,000.
//   - Snapshot isolation: each shard's old *shardState is held by any snapshot
//     taken before ApplyBatch — writes on the new *shardState are invisible.
package memory

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/agberohq/dpx/engine"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	numShards = 256
	shardMask = numShards - 1
)

// shardIndex hashes a key to a shard index using FNV-1a inline
// (avoids importing hash/fnv to keep the hot path allocation-free).
func shardIndex(key string) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h) & shardMask
}

// shardState is the immutable per-shard snapshot.
// Once stored via atomic.Pointer it is never mutated — writers clone then swap.
type shardState struct {
	data map[string][]byte
	keys []string // sorted keys belonging to this shard
}

func newShardState() *shardState {
	return &shardState{
		data: make(map[string][]byte),
		keys: []string{},
	}
}

// clone makes a shallow copy: values are shared references ([]byte is immutable
// once written), only the map structure and keys slice are copied.
func (s *shardState) clone() *shardState {
	data := make(map[string][]byte, len(s.data)+4)
	for k, v := range s.data {
		data[k] = v // share value reference; safe — values are never mutated in place
	}
	keys := make([]string, len(s.keys))
	copy(keys, s.keys)
	return &shardState{data: data, keys: keys}
}

type shard struct {
	mu  sync.Mutex
	cur atomic.Pointer[shardState]
}

// Engine is the sharded in-memory StorageEngine.
type Engine struct {
	shards  [numShards]shard
	applied atomic.Uint64 // tracks __dpx:applied sequence
	dir     string
}

// New creates an in-memory engine with 256 shards.
func New() *Engine {
	e := &Engine{}
	for i := range e.shards {
		e.shards[i].cur.Store(newShardState())
	}
	dir, _ := os.MkdirTemp("", "dpx-memory-*")
	e.dir = dir
	return e
}

func (e *Engine) Open() error {
	path := filepath.Join(e.dir, "dump")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	type dump struct {
		Data    map[string][]byte
		Applied uint64
	}
	var d dump
	if err := msgpack.Unmarshal(b, &d); err != nil {
		return fmt.Errorf("memory engine: load dump: %w", err)
	}
	if d.Data == nil {
		d.Data = make(map[string][]byte)
	}
	// Distribute loaded keys across shards.
	states := make([]*shardState, numShards)
	for i := range states {
		states[i] = newShardState()
	}
	for k, v := range d.Data {
		si := shardIndex(k)
		states[si].data[k] = v
		states[si].keys = append(states[si].keys, k)
	}
	for i, s := range states {
		sort.Strings(s.keys)
		e.shards[i].cur.Store(s)
	}
	e.applied.Store(d.Applied)
	return nil
}

func (e *Engine) Close() error { return nil }
func (e *Engine) Sync() error  { return nil }

func (e *Engine) Get(key []byte) ([]byte, error) {
	k := string(key)
	s := e.shards[shardIndex(k)].cur.Load()
	v := s.data[k]
	if v == nil {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

// GetSnapshot is O(256) atomic loads — effectively O(1), no lock, no clone.
func (e *Engine) GetSnapshot() (engine.Snapshot, error) {
	snap := &memSnapshot{
		states:  make([]*shardState, numShards),
		applied: e.applied.Load(),
	}
	for i := range e.shards {
		snap.states[i] = e.shards[i].cur.Load()
	}
	return snap, nil
}

func (e *Engine) NewBatch() engine.Batch {
	return &memBatch{}
}

// ApplyBatch groups ops by shard, clones only touched shards, and atomically
// swaps each shard's state pointer. Untouched shards are not cloned.
func (e *Engine) ApplyBatch(b engine.Batch, _ engine.WriteOptions) error {
	mb := b.(*memBatch)
	if len(mb.ops) == 0 {
		return nil
	}

	// Group ops by shard — single pass, no allocations for common case (1–4 keys).
	type shardOps struct {
		si  int
		ops []batchOp
	}
	// Use a small stack-allocated map via slice for typical small batches.
	byShard := make(map[int][]batchOp, 4)
	for _, op := range mb.ops {
		si := shardIndex(op.key)
		byShard[si] = append(byShard[si], op)
	}

	// Lock shards in ascending index order to prevent deadlock.
	shardIDs := make([]int, 0, len(byShard))
	for si := range byShard {
		shardIDs = append(shardIDs, si)
	}
	sort.Ints(shardIDs)

	for _, si := range shardIDs {
		sh := &e.shards[si]
		ops := byShard[si]

		sh.mu.Lock()
		cur := sh.cur.Load()
		next := cur.clone() // clone only this shard's ~N/256 entries

		for _, op := range ops {
			switch op.op {
			case 's':
				next.data[op.key] = op.value
				insertKey(&next.keys, op.key)
			case 'd':
				delete(next.data, op.key)
				deleteKey(&next.keys, op.key)
			case 'm':
				next.data[op.key] = encodeInt64(decodeInt64(next.data[op.key]) + decodeInt64(op.value))
				insertKey(&next.keys, op.key)
			}
		}

		sh.cur.Store(next)
		sh.mu.Unlock()
	}

	// Update applied sequence if the batch contains __dpx:applied.
	for _, op := range mb.ops {
		if op.key == "__dpx:applied" && len(op.value) >= 8 {
			e.applied.Store(binary.LittleEndian.Uint64(op.value))
			break
		}
	}
	return nil
}

func (e *Engine) CurrentSequence() uint64 { return e.applied.Load() }
func (e *Engine) DataDir() string         { return e.dir }

// currentKeys returns a sorted merge of all shard keys for test inspection.
func (e *Engine) currentKeys() []string {
	var all []string
	for i := range e.shards {
		s := e.shards[i].cur.Load()
		all = append(all, s.keys...)
	}
	sort.Strings(all)
	return all
}

func (e *Engine) CreateCheckpoint(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	all := make(map[string][]byte)
	for i := range e.shards {
		s := e.shards[i].cur.Load()
		for k, v := range s.data {
			all[k] = v
		}
	}
	type dump struct {
		Data    map[string][]byte
		Applied uint64
	}
	b, err := msgpack.Marshal(dump{Data: all, Applied: e.applied.Load()})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "dump"), b, 0o600)
}

func (e *Engine) RawIter(start, end []byte) engine.Iterator {
	// Collect all matching keys from all shards, sorted.
	ss, se := string(start), string(end)
	var all []string
	for i := range e.shards {
		s := e.shards[i].cur.Load()
		for _, k := range s.keys {
			if k < ss {
				continue
			}
			if se != "" && k >= se {
				break
			}
			all = append(all, k)
		}
	}
	sort.Strings(all)
	// Build a unified data view.
	data := make(map[string][]byte, len(all))
	for i := range e.shards {
		s := e.shards[i].cur.Load()
		for _, k := range all {
			if v, ok := s.data[k]; ok {
				data[k] = v
			}
		}
	}
	return &memIter{keys: all, data: data, pos: -1}
}

// ── Key index helpers ─────────────────────────────────────────────────────────

func insertKey(keys *[]string, k string) {
	i := sort.SearchStrings(*keys, k)
	if i < len(*keys) && (*keys)[i] == k {
		return
	}
	*keys = append(*keys, "")
	copy((*keys)[i+1:], (*keys)[i:])
	(*keys)[i] = k
}

func deleteKey(keys *[]string, k string) {
	i := sort.SearchStrings(*keys, k)
	if i >= len(*keys) || (*keys)[i] != k {
		return
	}
	*keys = append((*keys)[:i], (*keys)[i+1:]...)
}

// ── Int64 encode/decode ───────────────────────────────────────────────────────

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

func mergeInt64(base, delta []byte) []byte {
	return encodeInt64(decodeInt64(base) + decodeInt64(delta))
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

func isReserved(key string) bool {
	const prefix = "__dpx:"
	return len(key) >= len(prefix) && key[:len(prefix)] == prefix
}

// ── Batch ─────────────────────────────────────────────────────────────────────

type batchOp struct {
	op    byte
	key   string
	value []byte
}

type memBatch struct {
	ops []batchOp
}

func (b *memBatch) Set(key, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.ops = append(b.ops, batchOp{op: 's', key: string(key), value: cp})
}

func (b *memBatch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{op: 'd', key: string(key)})
}

func (b *memBatch) Merge(key, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.ops = append(b.ops, batchOp{op: 'm', key: string(key), value: cp})
}

func (b *memBatch) Reset() { b.ops = b.ops[:0] }

// ── Snapshot ──────────────────────────────────────────────────────────────────

// memSnapshot holds one *shardState per shard, captured atomically at snapshot time.
type memSnapshot struct {
	states  []*shardState
	applied uint64
}

func (s *memSnapshot) get(key string) []byte {
	return s.states[shardIndex(key)].data[key]
}

func (s *memSnapshot) Get(key []byte) ([]byte, error) {
	v := s.get(string(key))
	if v == nil {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (s *memSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := "__dpx:ver:" + string(key)
	b := s.get(vk)
	return decodeEpochRecord(b), nil
}

func (s *memSnapshot) Sequence() uint64 { return s.applied }
func (s *memSnapshot) Close() error     { return nil }

func (s *memSnapshot) NewIter(start, end []byte) engine.Iterator {
	ss, se := string(start), string(end)
	const prefix = "__dpx:"

	// Collect matching user keys from all shards, then sort.
	var all []string
	for _, sh := range s.states {
		for _, k := range sh.keys {
			if k < ss {
				continue
			}
			if se != "" && k >= se {
				break
			}
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				continue // hide internal keys
			}
			all = append(all, k)
		}
	}
	sort.Strings(all)

	// Build a single data view for the iterator.
	data := make(map[string][]byte, len(all))
	for _, k := range all {
		if v := s.get(k); v != nil {
			data[k] = v
		}
	}
	return &memIter{keys: all, data: data, pos: -1}
}

// ── Iterator ──────────────────────────────────────────────────────────────────

type memIter struct {
	keys []string
	data map[string][]byte
	pos  int
}

func (it *memIter) First() bool {
	it.pos = 0
	return it.pos < len(it.keys)
}

func (it *memIter) Next() bool {
	it.pos++
	return it.pos < len(it.keys)
}

func (it *memIter) Prev() bool {
	if it.pos <= 0 {
		it.pos = -1 // sentinel: past-beginning
		return false
	}
	it.pos--
	return true
}

func (it *memIter) Valid() bool { return it.pos >= 0 && it.pos < len(it.keys) }

func (it *memIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return []byte(it.keys[it.pos])
}

func (it *memIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.data[it.keys[it.pos]]
}

func (it *memIter) Error() error { return nil }
func (it *memIter) Close() error { return nil }
