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
	"github.com/vmihailenco/msgpack/v5"
)

const numShards = 32
const shardMask = numShards - 1

func shardFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) & shardMask
}

// state is the immutable snapshot of engine data at a point in time.
type state struct {
	data    map[string][]byte
	keys    []string
	applied uint64
}

func (s *state) clone() *state {
	data := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		data[k] = v
	}
	keys := make([]string, len(s.keys))
	copy(keys, s.keys)
	return &state{data: data, keys: keys, applied: s.applied}
}

// shardState wraps a state with its own mutex for independent locking.
type shardState struct {
	mu  sync.Mutex
	cur atomic.Pointer[state]
}

// Engine is the in-memory StorageEngine.
// When sharded=true, keys are partitioned across 32 shards for parallel writes.
// When sharded=false, all keys live in shard 0 (backward compatible).
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
	e.cur.Store(&state{
		data: make(map[string][]byte),
		keys: []string{},
	})
	dir, _ := os.MkdirTemp("", "dpx-memory-*")
	e.dir = dir
	return e
}

// NewSharded creates a sharded in-memory engine.
// Each shard has its own mutex and state, enabling parallel writes.
func NewSharded() *Engine {
	e := &Engine{sharded: true}
	for i := range e.shards {
		e.shards[i].cur.Store(&state{
			data: make(map[string][]byte),
			keys: []string{},
		})
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
	if e.sharded {
		for k, v := range d.Data {
			s := shardFor(k)
			st := e.shards[s].cur.Load()
			next := st.clone()
			next.data[k] = v
			insertKey(&next.keys, k)
			e.shards[s].cur.Store(next)
		}
	} else {
		keys := make([]string, 0, len(d.Data))
		for k := range d.Data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		e.cur.Store(&state{data: d.Data, keys: keys, applied: d.Applied})
	}
	return nil
}

func (e *Engine) Close() error { return nil }
func (e *Engine) Sync() error  { return nil }

func (e *Engine) Get(key []byte) ([]byte, error) {
	if e.sharded {
		s := &e.shards[shardFor(string(key))]
		st := s.cur.Load()
		v, ok := st.data[string(key)]
		if !ok {
			return nil, engine.ErrKeyNotFound
		}
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp, nil
	}
	s := e.cur.Load()
	v := s.data[string(key)]
	if v == nil {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
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

	keysDirty := false
	for _, op := range mb.ops {
		switch op.op {
		case 's':
			if _, exists := next.data[op.key]; !exists {
				next.keys = append(next.keys, op.key)
				keysDirty = true
			}
			next.data[op.key] = op.value
			if op.key == "__dpx:applied" && len(op.value) >= 8 {
				next.applied = binary.LittleEndian.Uint64(op.value)
			}
		case 'd':
			if _, exists := next.data[op.key]; exists {
				delete(next.data, op.key)
				keysDirty = true
			}
		case 'm':
			if _, exists := next.data[op.key]; !exists {
				next.keys = append(next.keys, op.key)
				keysDirty = true
			}
			next.data[op.key] = encodeInt64(decodeInt64(next.data[op.key]) + decodeInt64(op.value))
		}
	}

	// O(N log N) sort once per batch instead of O(N^2) shuffling!
	if keysDirty {
		sort.Strings(next.keys)
		writeIdx := 0
		for i := 0; i < len(next.keys); i++ {
			k := next.keys[i]
			if i > 0 && next.keys[i-1] == k {
				continue // skip duplicates
			}
			if _, exists := next.data[k]; exists {
				next.keys[writeIdx] = k
				writeIdx++
			}
		}
		next.keys = next.keys[:writeIdx]
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

		keysDirty := false
		for _, op := range batch.ops {
			switch op.op {
			case 's':
				if _, exists := next.data[op.key]; !exists {
					next.keys = append(next.keys, op.key)
					keysDirty = true
				}
				next.data[op.key] = op.value
			case 'd':
				if _, exists := next.data[op.key]; exists {
					delete(next.data, op.key)
					keysDirty = true
				}
			case 'm':
				if _, exists := next.data[op.key]; !exists {
					next.keys = append(next.keys, op.key)
					keysDirty = true
				}
				next.data[op.key] = encodeInt64(decodeInt64(next.data[op.key]) + decodeInt64(op.value))
			}
		}

		if keysDirty {
			sort.Strings(next.keys)
			writeIdx := 0
			for i := 0; i < len(next.keys); i++ {
				k := next.keys[i]
				if i > 0 && next.keys[i-1] == k {
					continue
				}
				if _, exists := next.data[k]; exists {
					next.keys[writeIdx] = k
					writeIdx++
				}
			}
			next.keys = next.keys[:writeIdx]
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
			all = append(all, st.keys...)
		}
		sort.Strings(all)
		return all
	}
	s := e.cur.Load()
	keys := make([]string, len(s.keys))
	copy(keys, s.keys)
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
			for k, v := range st.data {
				allData[k] = v
			}
			if st.applied > applied {
				applied = st.applied
			}
		}
	} else {
		s := e.cur.Load()
		allData = s.data
		applied = s.applied
	}
	type dump struct {
		Data    map[string][]byte
		Applied uint64
	}
	b, err := msgpack.Marshal(dump{Data: allData, Applied: applied})
	if err != nil {
		return err
	}
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
	startIdx := sort.SearchStrings(s.keys, ss)
	var filtered []string
	for i := startIdx; i < len(s.keys); i++ {
		k := s.keys[i]
		if se != "" && k >= se {
			break
		}
		filtered = append(filtered, k)
	}

	if e.telemetry != nil {
		e.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &memIter{keys: filtered, data: s.data, pos: -1}
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
		for _, k := range st.keys {
			if k >= ss && (se == "" || k < se) {
				v := st.data[k]
				cp := make([]byte, len(v))
				copy(cp, v)
				pairs = append(pairs, [2][]byte{[]byte(k), cp})
			}
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
	v := s.s.data[string(key)]
	if v == nil {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (s *memSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := "__dpx:ver:" + string(key)
	b := s.s.data[vk]
	if len(b) < 9 {
		return engine.EpochRecord{}, nil
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b[:8]),
		IsCredit: b[8] == 1,
	}, nil
}

func (s *memSnapshot) NewIter(start, end []byte) engine.Iterator {
	var matStart time.Time
	if s.telemetry != nil {
		matStart = time.Now()
	}

	ss, se := string(start), string(end)
	prefix := "__dpx:"
	startIdx := sort.SearchStrings(s.s.keys, ss)
	var filtered []string
	for i := startIdx; i < len(s.s.keys); i++ {
		k := s.s.keys[i]
		if se != "" && k >= se {
			break
		}
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			continue
		}
		filtered = append(filtered, k)
	}

	if s.telemetry != nil {
		s.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &memIter{keys: filtered, data: s.s.data, pos: -1}
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
	v, ok := st.data[string(key)]
	if !ok {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (s *shardedSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := "__dpx:ver:" + string(key)
	sh := shardFor(vk)
	st := s.states[sh]
	b := st.data[vk]
	if len(b) < 9 {
		return engine.EpochRecord{}, nil
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b[:8]),
		IsCredit: b[8] == 1,
	}, nil
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
		for _, k := range s.states[i].keys {
			if k >= ss && (se == "" || k < se) {
				if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
					continue
				}
				v := s.states[i].data[k]
				cp := make([]byte, len(v))
				copy(cp, v)
				all = append(all, [2][]byte{[]byte(k), cp})
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return string(all[i][0]) < string(all[j][0]) })

	if s.telemetry != nil {
		s.telemetry.RecordIterMaterialise(time.Since(matStart))
	}

	return &memIter{pairs: all, pos: -1}
}

func (s *shardedSnapshot) Sequence() uint64 { return 0 } // aggregated across shards
func (s *shardedSnapshot) Close() error     { return nil }

// Iterator

type memIter struct {
	keys  []string
	data  map[string][]byte
	pairs [][2][]byte
	pos   int
}

func (it *memIter) First() bool {
	it.pos = 0
	if it.pairs != nil {
		return it.pos < len(it.pairs)
	}
	return it.pos < len(it.keys)
}

func (it *memIter) Next() bool {
	it.pos++
	if it.pairs != nil {
		return it.pos < len(it.pairs)
	}
	return it.pos < len(it.keys)
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
	if it.pos < 0 {
		return false
	}
	if it.pairs != nil {
		return it.pos < len(it.pairs)
	}
	return it.pos < len(it.keys)
}

func (it *memIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	if it.pairs != nil {
		return it.pairs[it.pos][0]
	}
	return []byte(it.keys[it.pos])
}

func (it *memIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	if it.pairs != nil {
		return it.pairs[it.pos][1]
	}
	return it.data[it.keys[it.pos]]
}

func (it *memIter) Error() error { return nil }
func (it *memIter) Close() error { return nil }

// Helpers

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
// Satisfies the shardedMarker interface used by raft/node.go.
func (e *Engine) IsSharded() bool { return e.sharded }
