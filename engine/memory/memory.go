package memory

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/tidwall/btree"
)

const numShards = 64
const shardMask = numShards - 1

func shardFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) & shardMask
}

// item is a single key-value pair in the btree.
type item struct {
	key   string
	value []byte
}

func itemLess(a, b item) bool {
	return a.key < b.key
}

// state is an immutable snapshot of shard data.
// Copy() is O(1) because tidwall/btree uses copy-on-write.
type state struct {
	tree    *btree.BTreeG[item]
	applied uint64
}

func (s *state) clone() *state {
	return &state{
		tree:    s.tree.Copy(),
		applied: s.applied,
	}
}

// shardState wraps a state with its own mutex for batch serialization.
type shardState struct {
	mu  sync.Mutex
	cur atomic.Pointer[state]
}

// Engine is the in-memory StorageEngine.
// Sharded mode partitions keys across 64 shards for parallel writes.
type Engine struct {
	sharded   bool
	shards    [numShards]shardState
	cur       atomic.Pointer[state] // legacy non-sharded path
	mu        sync.Mutex            // legacy non-sharded path
	dir       string
	telemetry engine.StageRecorder
}

// New creates a non-sharded in-memory engine.
func New() *Engine {
	e := &Engine{sharded: false}
	e.cur.Store(&state{tree: btree.NewBTreeG(itemLess)})
	dir, _ := os.MkdirTemp("", "dpx-memory-*")
	e.dir = dir
	return e
}

// NewSharded creates a sharded in-memory engine.
// Each shard has its own mutex and COW btree, enabling parallel writes.
func NewSharded() *Engine {
	e := &Engine{sharded: true}
	for i := range e.shards {
		e.shards[i].cur.Store(&state{tree: btree.NewBTreeG(itemLess)})
	}
	dir, _ := os.MkdirTemp("", "dpx-sharded-*")
	e.dir = dir
	return e
}

func (e *Engine) SetTelemetry(r engine.StageRecorder) {
	e.telemetry = r
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
	data, applied, err := decodeDump(b)
	if err != nil {
		return fmt.Errorf("memory engine: load dump: %w", err)
	}
	if e.sharded {
		for k, v := range data {
			s := shardFor(k)
			st := e.shards[s].cur.Load()
			next := st.clone()
			next.tree.Set(item{key: k, value: v})
			e.shards[s].cur.Store(next)
		}
		for i := range e.shards {
			st := e.shards[i].cur.Load()
			e.shards[i].cur.Store(&state{tree: st.tree, applied: applied})
		}
	} else {
		next := &state{tree: btree.NewBTreeG(itemLess), applied: applied}
		for k, v := range data {
			next.tree.Set(item{key: k, value: v})
		}
		e.cur.Store(next)
	}
	return nil
}

func (e *Engine) Close() error { return nil }
func (e *Engine) Sync() error  { return nil }

func (e *Engine) Get(key []byte) ([]byte, error) {
	if e.sharded {
		s := &e.shards[shardFor(string(key))]
		st := s.cur.Load()
		it, ok := st.tree.Get(item{key: string(key)})
		if !ok {
			return nil, engine.ErrKeyNotFound
		}
		cp := make([]byte, len(it.value))
		copy(cp, it.value)
		return cp, nil
	}
	s := e.cur.Load()
	it, ok := s.tree.Get(item{key: string(key)})
	if !ok {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(it.value))
	copy(cp, it.value)
	return cp, nil
}

func (e *Engine) GetSnapshot() (engine.Snapshot, error) {
	if e.sharded {
		snap := &shardedSnapshot{
			states:    make([]*state, numShards),
			telemetry: e.telemetry,
		}
		for i := range e.shards {
			snap.states[i] = e.shards[i].cur.Load()
		}
		return snap, nil
	}
	return &memSnapshot{s: e.cur.Load(), telemetry: e.telemetry}, nil
}

func (e *Engine) NewBatch() engine.Batch {
	if e.sharded {
		return &shardedBatch{byShard: make(map[int]*memBatch)}
	}
	return &memBatch{}
}

func (e *Engine) ApplyBatch(b engine.Batch, _ engine.WriteOptions) error {
	if e.sharded {
		return e.applyBatchSharded(b)
	}
	return e.applyBatchLegacy(b)
}

func (e *Engine) applyBatchLegacy(b engine.Batch) error {
	mb := b.(*memBatch)
	e.mu.Lock()
	defer e.mu.Unlock()
	cur := e.cur.Load()

	var cloneStart time.Time
	if e.telemetry != nil {
		cloneStart = time.Now()
	}
	next := cur.clone()
	if e.telemetry != nil {
		e.telemetry.RecordClone(time.Since(cloneStart))
	}

	for _, op := range mb.ops {
		switch op.op {
		case 's':
			next.tree.Set(item{key: op.key, value: op.value})
			if op.key == "__dpx:applied" && len(op.value) >= 8 {
				next.applied = binary.LittleEndian.Uint64(op.value)
			}
		case 'd':
			next.tree.Delete(item{key: op.key})
		case 'm':
			old, _ := next.tree.Get(item{key: op.key})
			next.tree.Set(item{key: op.key, value: encodeInt64(decodeInt64(old.value) + decodeInt64(op.value))})
		}
	}

	e.cur.Store(next)
	return nil
}

func (e *Engine) applyBatchSharded(b engine.Batch) error {
	sb := b.(*shardedBatch)
	if len(sb.byShard) == 0 {
		return nil
	}
	shardIds := make([]int, 0, len(sb.byShard))
	for s := range sb.byShard {
		shardIds = append(shardIds, s)
	}
	sort.Ints(shardIds)

	for _, s := range shardIds {
		sh := &e.shards[s]
		batch := sb.byShard[s]
		sh.mu.Lock()
		cur := sh.cur.Load()

		var cloneStart time.Time
		if e.telemetry != nil {
			cloneStart = time.Now()
		}
		next := cur.clone()
		if e.telemetry != nil {
			e.telemetry.RecordClone(time.Since(cloneStart))
		}

		for _, op := range batch.ops {
			switch op.op {
			case 's':
				next.tree.Set(item{key: op.key, value: op.value})
				if op.key == "__dpx:applied" && len(op.value) >= 8 {
					next.applied = binary.LittleEndian.Uint64(op.value)
				}
			case 'd':
				next.tree.Delete(item{key: op.key})
			case 'm':
				old, _ := next.tree.Get(item{key: op.key})
				next.tree.Set(item{key: op.key, value: encodeInt64(decodeInt64(old.value) + decodeInt64(op.value))})
			}
		}

		sh.cur.Store(next)
		sh.mu.Unlock()
	}
	return nil
}

func (e *Engine) CurrentSequence() uint64 {
	if e.sharded {
		var maxSeq uint64
		for i := range e.shards {
			st := e.shards[i].cur.Load()
			if st.applied > maxSeq {
				maxSeq = st.applied
			}
		}
		return maxSeq
	}
	return e.cur.Load().applied
}

func (e *Engine) DataDir() string { return e.dir }

func (e *Engine) currentKeys() []string {
	if e.sharded {
		var all []string
		for i := range e.shards {
			st := e.shards[i].cur.Load()
			st.tree.Scan(func(it item) bool {
				all = append(all, it.key)
				return true
			})
		}
		sort.Strings(all)
		return all
	}
	s := e.cur.Load()
	var keys []string
	s.tree.Scan(func(it item) bool {
		keys = append(keys, it.key)
		return true
	})
	return keys
}

func (e *Engine) CreateCheckpoint(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	allData := make(map[string][]byte)
	var applied uint64
	if e.sharded {
		for i := range e.shards {
			st := e.shards[i].cur.Load()
			st.tree.Scan(func(it item) bool {
				allData[it.key] = it.value
				return true
			})
			if st.applied > applied {
				applied = st.applied
			}
		}
	} else {
		s := e.cur.Load()
		s.tree.Scan(func(it item) bool {
			allData[it.key] = it.value
			return true
		})
		applied = s.applied
	}
	b := encodeDump(allData, applied)
	return os.WriteFile(filepath.Join(dir, "dump"), b, 0o600)
}

func (e *Engine) RawIter(start, end []byte) engine.Iterator {
	if e.sharded {
		return e.rawIterSharded(start, end)
	}
	return e.rawIterLegacy(start, end)
}

func (e *Engine) rawIterLegacy(start, end []byte) engine.Iterator {
	var matStart time.Time
	if e.telemetry != nil {
		matStart = time.Now()
	}

	s := e.cur.Load()
	ss, se := string(start), string(end)
	var pairs [][2][]byte

	if se == "" {
		s.tree.Scan(func(it item) bool {
			if it.key < ss {
				return true
			}
			cp := make([]byte, len(it.value))
			copy(cp, it.value)
			pairs = append(pairs, [2][]byte{[]byte(it.key), cp})
			return true
		})
	} else {
		s.tree.Ascend(item{key: ss}, func(it item) bool {
			if it.key >= se {
				return false
			}
			cp := make([]byte, len(it.value))
			copy(cp, it.value)
			pairs = append(pairs, [2][]byte{[]byte(it.key), cp})
			return true
		})
	}

	if e.telemetry != nil {
		e.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &memIter{pairs: pairs, pos: -1}
}

func (e *Engine) rawIterSharded(start, end []byte) engine.Iterator {
	var matStart time.Time
	if e.telemetry != nil {
		matStart = time.Now()
	}

	var pairs [][2][]byte
	ss, se := string(start), string(end)
	for i := range e.shards {
		st := e.shards[i].cur.Load()
		if se == "" {
			st.tree.Scan(func(it item) bool {
				if it.key < ss {
					return true
				}
				cp := make([]byte, len(it.value))
				copy(cp, it.value)
				pairs = append(pairs, [2][]byte{[]byte(it.key), cp})
				return true
			})
		} else {
			st.tree.Ascend(item{key: ss}, func(it item) bool {
				if it.key >= se {
					return false
				}
				cp := make([]byte, len(it.value))
				copy(cp, it.value)
				pairs = append(pairs, [2][]byte{[]byte(it.key), cp})
				return true
			})
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return string(pairs[i][0]) < string(pairs[j][0]) })

	if e.telemetry != nil {
		e.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &memIter{pairs: pairs, pos: -1}
}

// Sharded Batch

type shardedBatch struct {
	byShard map[int]*memBatch
}

func (b *shardedBatch) Set(key, value []byte) {
	s := shardFor(string(key))
	if b.byShard[s] == nil {
		b.byShard[s] = &memBatch{}
	}
	b.byShard[s].Set(key, value)
}

func (b *shardedBatch) Delete(key []byte) {
	s := shardFor(string(key))
	if b.byShard[s] == nil {
		b.byShard[s] = &memBatch{}
	}
	b.byShard[s].Delete(key)
}

func (b *shardedBatch) Merge(key, value []byte) {
	s := shardFor(string(key))
	if b.byShard[s] == nil {
		b.byShard[s] = &memBatch{}
	}
	b.byShard[s].Merge(key, value)
}

func (b *shardedBatch) Reset() {
	b.byShard = make(map[int]*memBatch)
}

// Legacy Batch

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

// Legacy Snapshot

type memSnapshot struct {
	s         *state
	telemetry engine.StageRecorder
}

func (s *memSnapshot) Get(key []byte) ([]byte, error) {
	it, ok := s.s.tree.Get(item{key: string(key)})
	if !ok {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(it.value))
	copy(cp, it.value)
	return cp, nil
}

func (s *memSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := "__dpx:ver:" + string(key)
	it, ok := s.s.tree.Get(item{key: vk})
	if !ok {
		return engine.EpochRecord{}, nil
	}
	return decodeEpochRecord(it.value), nil
}

func (s *memSnapshot) NewIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if s.telemetry != nil {
		matStart = time.Now()
	}

	ss, se := string(start), string(end)
	prefix := "__dpx:"
	var pairs [][2][]byte

	if se == "" {
		s.s.tree.Scan(func(it item) bool {
			if it.key < ss {
				return true
			}
			if len(it.key) >= len(prefix) && it.key[:len(prefix)] == prefix {
				return true
			}
			cp := make([]byte, len(it.value))
			copy(cp, it.value)
			pairs = append(pairs, [2][]byte{[]byte(it.key), cp})
			return true
		})
	} else {
		s.s.tree.Ascend(item{key: ss}, func(it item) bool {
			if it.key >= se {
				return false
			}
			if len(it.key) >= len(prefix) && it.key[:len(prefix)] == prefix {
				return true
			}
			cp := make([]byte, len(it.value))
			copy(cp, it.value)
			pairs = append(pairs, [2][]byte{[]byte(it.key), cp})
			return true
		})
	}

	if s.telemetry != nil {
		s.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &memIter{pairs: pairs, pos: -1}
}

func (s *memSnapshot) Sequence() uint64 { return s.s.applied }
func (s *memSnapshot) Close() error     { return nil }

// Sharded Snapshot

type shardedSnapshot struct {
	states    []*state
	telemetry engine.StageRecorder
}

func (s *shardedSnapshot) Get(key []byte) ([]byte, error) {
	sh := shardFor(string(key))
	st := s.states[sh]
	it, ok := st.tree.Get(item{key: string(key)})
	if !ok {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(it.value))
	copy(cp, it.value)
	return cp, nil
}

func (s *shardedSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := "__dpx:ver:" + string(key)
	sh := shardFor(vk)
	st := s.states[sh]
	it, ok := st.tree.Get(item{key: vk})
	if !ok {
		return engine.EpochRecord{}, nil
	}
	return decodeEpochRecord(it.value), nil
}

func (s *shardedSnapshot) NewIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if s.telemetry != nil {
		matStart = time.Now()
	}

	var all [][2][]byte
	ss, se := string(start), string(end)
	prefix := "__dpx:"
	for i := range s.states {
		if se == "" {
			s.states[i].tree.Scan(func(it item) bool {
				if it.key < ss {
					return true
				}
				if len(it.key) >= len(prefix) && it.key[:len(prefix)] == prefix {
					return true
				}
				cp := make([]byte, len(it.value))
				copy(cp, it.value)
				all = append(all, [2][]byte{[]byte(it.key), cp})
				return true
			})
		} else {
			s.states[i].tree.Ascend(item{key: ss}, func(it item) bool {
				if it.key >= se {
					return false
				}
				if len(it.key) >= len(prefix) && it.key[:len(prefix)] == prefix {
					return true
				}
				cp := make([]byte, len(it.value))
				copy(cp, it.value)
				all = append(all, [2][]byte{[]byte(it.key), cp})
				return true
			})
		}
	}

	if s.telemetry != nil {
		s.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	// Sort by key so forward and reverse iteration are globally ordered.
	// Each shard's btree is internally sorted but concatenation across
	// 64 shards loses global order — a single sort here restores it.
	sort.Slice(all, func(i, j int) bool {
		return string(all[i][0]) < string(all[j][0])
	})

	return &memIter{pairs: all, pos: -1}
}

// Sequence returns the maximum applied index across all shards.
// This matches CurrentSequence() semantics and ensures AllocateNextSequence
// returns a strictly increasing value as writes commit.
func (s *shardedSnapshot) Sequence() uint64 {
	var max uint64
	for _, st := range s.states {
		if st.applied > max {
			max = st.applied
		}
	}
	return max
}
func (s *shardedSnapshot) Close() error { return nil }

// Iterator

type memIter struct {
	pairs [][2][]byte
	pos   int
}

func (it *memIter) First() bool {
	it.pos = 0
	return it.pos < len(it.pairs)
}

func (it *memIter) Next() bool {
	it.pos++
	return it.pos < len(it.pairs)
}

func (it *memIter) Prev() bool {
	if it.pos <= 0 {
		it.pos = -1
		return false
	}
	it.pos--
	return true
}

func (it *memIter) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.pairs)
}

func (it *memIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.pairs[it.pos][0]
}

func (it *memIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.pairs[it.pos][1]
}

func (it *memIter) Error() error { return nil }
func (it *memIter) Close() error { return nil }

// Helpers

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

// IsSharded reports whether this engine uses sharded storage.
func (e *Engine) IsSharded() bool { return e.sharded }
