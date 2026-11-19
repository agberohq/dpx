package memory

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/agberohq/dpx/engine"
)

// MVCCEngine is a Multi-Version Concurrency Control in-memory storage engine.
//
// Instead of cloning the entire B-tree on every transaction (COW), MVCC stores
// versioned entries in a single hash map. Each entry has a sequence number, and
// snapshots are just a sequence number — no cloning needed.
//
// Commit protocol:
//   - Each ApplyBatch call assigns a single commit sequence for all keys in the batch.
//   - All entries in a batch share the same sequence, so a snapshot cannot see only
//     part of a transaction — atomic visibility is guaranteed.
//
// Key properties:
// - Snapshot creation: O(1) — just read the current sequence counter
// - Point read: O(1) — hash map lookup, then find the right version
// - Point write: O(1) — append new version to hash map
// - Range scan: O(n log k) — collect keys in range, sort, then read versions
//
// Compaction:
//   - Automatic background compaction runs when version count exceeds threshold.
//   - Compaction respects the oldest active snapshot: it never removes versions
//     that a live snapshot might still read.
//   - Deleted keys and tombstones are cleaned up when no snapshot references them.
//
// Trade-offs:
// - No native range scan efficiency — must collect and sort keys
// - For DPX's workload (point lookups dominate), this is ideal
type MVCCEngine struct {
	sharded   bool
	shards    [numShards]*mvccShard
	dir       string
	telemetry engine.StageRecorder

	// Global sequence counter for versioning
	seqCounter atomic.Uint64

	// commits publishes multi-shard commits in sequence order. Snapshots read
	// from it instead of seqCounter so they never observe a commit whose shards
	// are only partially written. Concurrent appliers make a plain "highest
	// completed sequence" watermark incorrect: two overlapping commits could
	// publish a sequence whose writes are not all visible yet.
	commits *commitTracker

	// Compaction threshold: if a key has more versions than this, compact
	compactThreshold int

	// Active snapshot tracking for safe compaction. The oldest live snapshot
	// is tracked exactly and raised again when snapshots close.
	snaps snapshotTracker

	// Background compaction control
	compactStop chan struct{}
	compactDone chan struct{}
}

// mvccShard is a single shard's MVCC data.
// For sharded mode, the data map uses atomic.Value per key so readers
// (snapshots) can load entry slices without holding any lock for the
// duration of the read, while writers (ApplyShardDirect) create new
// slices and store them atomically.
// The map itself still requires a brief RLock per lookup: writers insert
// keys under a write lock, and an unlocked map read racing a map write is
// undefined behaviour in Go (race detector violation, possible crash).
// The RLock is held only for the pointer resolution; entry-slice reads
// that follow are lock-free (see atomicValue).
type mvccShard struct {
	mu   sync.RWMutex            // guards the data map structure
	data map[string]*atomicValue // key -> atomic pointer to []*mvccEntry
	// dirty records keys written since the last compaction pass, so compaction
	// cost follows recent writes instead of the whole keyspace.
	dirty map[string]struct{}
}

// lookup resolves the atomic value for key under a brief read lock, or
// returns nil if the key is absent. The returned pointer stays valid for
// the lifetime of the engine: compaction swaps slice contents atomically
// and never frees the atomicValue while readers may hold it.
func (s *mvccShard) lookup(key string) *atomicValue {
	s.mu.RLock()
	av := s.data[key]
	s.mu.RUnlock()
	return av
}

// atomicValue wraps []*mvccEntry with atomic load/store for lock-free reads.
type atomicValue struct {
	v atomic.Pointer[mvccEntrySlice]
	// sorted reports whether the entry slice is in ascending commit-sequence
	// order, which lets version lookups binary search instead of scanning.
	// Appends through the proposer are ordered per key (the key's conflict
	// stripe is held for check-and-apply), so this stays true on the hot path;
	// a direct concurrent ApplyBatch that appends out of order clears it.
	sorted atomic.Bool
}

type mvccEntrySlice []*mvccEntry

func newAtomicValue(entries []*mvccEntry) *atomicValue {
	av := &atomicValue{}
	av.sorted.Store(true)
	s := mvccEntrySlice(entries)
	av.v.Store(&s)
	return av
}

func (av *atomicValue) Load() []*mvccEntry {
	p := av.v.Load()
	if p == nil {
		return nil
	}
	return []*mvccEntry(*p)
}

func (av *atomicValue) Store(entries []*mvccEntry) {
	s := mvccEntrySlice(entries)
	av.v.Store(&s)
}

// mvccEntry is a single versioned value.
type mvccEntry struct {
	seq     uint64
	value   []byte
	deleted bool
}

// NewMVCC creates a non-sharded MVCC in-memory engine.
func NewMVCC() *MVCCEngine {
	return newMVCCEngine(false)
}

// NewMVCCSharded creates a sharded MVCC in-memory engine.
func NewMVCCSharded() *MVCCEngine {
	return newMVCCEngine(true)
}

func newMVCCEngine(sharded bool) *MVCCEngine {
	e := &MVCCEngine{
		sharded:          sharded,
		compactThreshold: 10,
		compactStop:      make(chan struct{}),
		compactDone:      make(chan struct{}),
	}
	e.commits = newCommitTracker()
	for i := range e.shards {
		e.shards[i] = &mvccShard{
			data:  make(map[string]*atomicValue, 1024),
			dirty: make(map[string]struct{}, 256),
		}
	}
	prefix := "dpx-mvcc"
	if sharded {
		prefix = "dpx-mvcc-sharded"
	}
	dir, _ := os.MkdirTemp("", prefix+"-*")
	e.dir = dir
	go e.compactLoop()
	return e
}

func (e *MVCCEngine) Open() error {
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
		return fmt.Errorf("mvcc engine: load dump: %w", err)
	}
	e.seqCounter.Store(applied)
	e.commits.reset(applied)
	for k, v := range data {
		sh := shardFor(k)
		s := e.shards[sh]
		s.mu.Lock()
		av, ok := s.data[k]
		if !ok {
			av = newAtomicValue(nil)
			s.data[k] = av
		}
		entries := av.Load()
		entries = append(entries, &mvccEntry{
			seq:   applied,
			value: v,
		})
		av.Store(entries)
		s.dirty[k] = struct{}{}
		s.mu.Unlock()
	}
	return nil
}

func (e *MVCCEngine) Close() error {
	close(e.compactStop)
	<-e.compactDone
	return nil
}
func (e *MVCCEngine) Sync() error { return nil }

func (e *MVCCEngine) SetTelemetry(r engine.StageRecorder) {
	e.telemetry = r
}

func (e *MVCCEngine) Get(key []byte) ([]byte, error) {
	s := e.shards[shardFor(string(key))]

	av := s.lookup(string(key))
	if av == nil {
		return nil, engine.ErrKeyNotFound
	}
	entries := av.Load()
	if len(entries) == 0 {
		return nil, engine.ErrKeyNotFound
	}
	// Check the latest entry: if it's a delete, the key is gone
	latest := entries[len(entries)-1]
	if latest.deleted {
		return nil, engine.ErrKeyNotFound
	}
	// Otherwise return the latest value
	cp := make([]byte, len(latest.value))
	copy(cp, latest.value)
	return cp, nil
}

func (e *MVCCEngine) GetSnapshot() (engine.Snapshot, error) {
	// Read the last fully published sequence: every commit up to and including
	// it has all of its shards written.
	seq := e.commits.published.Load()
	snap := &mvccSnapshot{
		seq:    seq,
		engine: e,
	}
	e.snaps.add(seq)
	return snap, nil
}

func (e *MVCCEngine) NewBatch() engine.Batch {
	return &mvccBatch{
		ops: make([]mvccBatchOp, 0, 8),
	}
}

func (e *MVCCEngine) ApplyBatch(b engine.Batch, _ engine.WriteOptions) error {
	mb := b.(*mvccBatch)
	if len(mb.ops) == 0 {
		return nil
	}

	// Atomic commit protocol: reserve one sequence range for the entire batch.
	// All keys in this batch become visible at the same sequence, so a snapshot
	// cannot see only part of the transaction. The range is only published once
	// every shard has been written (see commitTracker).
	startSeq, commitSeq := e.commits.reserve(&e.seqCounter, uint64(len(mb.ops)))

	// Group by shard
	byShard := make(map[int][]mvccBatchOp)
	for _, op := range mb.ops {
		sh := shardFor(op.key)
		byShard[sh] = append(byShard[sh], op)
	}

	// Apply to each shard, using the same commit sequence for all entries
	for sh, ops := range byShard {
		s := e.shards[sh]
		s.mu.Lock()
		for _, op := range ops {
			entry := &mvccEntry{
				seq:     commitSeq,
				value:   op.value,
				deleted: op.op == 'd',
			}
			av, ok := s.data[op.key]
			if !ok {
				av = newAtomicValue(nil)
				s.data[op.key] = av
			}
			s.dirty[op.key] = struct{}{}
			entries := av.Load()
			switch op.op {
			case 's', 'd':
				entries = appendVersion(av, entries, entry)
			case 'm':
				// For merge, find the current value and add delta
				current := e.getCurrentValue(av, entries)
				delta := decodeInt64MVCC(op.value)
				newVal := encodeInt64MVCC(decodeInt64MVCC(current) + delta)
				entry.value = newVal
				entries = appendVersion(av, entries, entry)
			}
			av.Store(entries)
		}
		s.mu.Unlock()
	}

	// Publish: advances only across a contiguous prefix of completed commits.
	e.commits.complete(startSeq, commitSeq)
	return nil
}

// MVCCWriteOp is a single write operation for ApplyShardDirect.
type MVCCWriteOp struct {
	Key   string
	Value []byte
	Op    byte // 's' = set, 'd' = delete, 'm' = merge
	// Shard is the engine shard the key hashes to. Only read by
	// ApplySortedDirect; the other apply paths compute it themselves.
	Shard int
}

// EngineShardFor reports the engine shard a key hashes to. Exported so the
// conductor can route version keys to the shard GetVersion will read from.
func EngineShardFor(key string) int {
	return shardFor(key)
}

// ApplyShardsDirect applies per-shard write groups under one commit sequence.
// Each group is applied to its own engine shard; all entries across groups
// share the same commitSeq so a snapshot sees all or none of them.
// Caller must hold the proposer-level serialization (CAS spinlock).
func (e *MVCCEngine) ApplyShardsDirect(groups map[int][]MVCCWriteOp) uint64 {
	total := 0
	for _, ws := range groups {
		total += len(ws)
	}
	if total == 0 {
		return e.seqCounter.Load()
	}
	startSeq, commitSeq := e.commits.reserve(&e.seqCounter, uint64(total))

	for shardIdx, ws := range groups {
		if len(ws) == 0 {
			continue
		}
		e.applyOneShard(shardIdx, ws, commitSeq)
	}

	e.commits.complete(startSeq, commitSeq)
	return commitSeq
}

// applyOneShard applies one shard group's ops at the given commit sequence.
// keyedOp is one write ready to apply: the entry is built before the shard
// lock is taken so the critical section stays short.
type keyedOp struct {
	key   string
	entry *mvccEntry
	op    byte
	value []byte
}

// newKeyedOps materialises entries for ws at commitSeq. Callers with several
// shard groups allocate one slice for the whole transaction and pass sub-slices
// to applyShardEntries, instead of allocating per shard.
func newKeyedOps(ws []MVCCWriteOp, commitSeq uint64) []keyedOp {
	ops := make([]keyedOp, len(ws))
	for i, w := range ws {
		ops[i] = keyedOp{
			key:   w.Key,
			entry: &mvccEntry{seq: commitSeq, value: w.Value, deleted: w.Op == 'd'},
			op:    w.Op,
			value: w.Value,
		}
	}
	return ops
}

func (e *MVCCEngine) applyOneShard(shardIdx int, ws []MVCCWriteOp, commitSeq uint64) {
	e.applyShardEntries(shardIdx, newKeyedOps(ws, commitSeq))
}

// applyShardEntries applies pre-built entries to one shard under its write lock.
func (e *MVCCEngine) applyShardEntries(shardIdx int, ops []keyedOp) {
	if len(ops) == 0 {
		return
	}
	s := e.shards[shardIdx]

	s.mu.Lock()
	for _, ko := range ops {
		av, ok := s.data[ko.key]
		if !ok {
			av = newAtomicValue(nil)
			s.data[ko.key] = av
		}
		s.dirty[ko.key] = struct{}{}
		entries := av.Load()
		switch ko.op {
		case 's', 'd':
			entries = appendVersion(av, entries, ko.entry)
		case 'm':
			current := e.getCurrentValue(av, entries)
			delta := decodeInt64MVCC(ko.value)
			newVal := encodeInt64MVCC(decodeInt64MVCC(current) + delta)
			ko.entry.value = newVal
			entries = appendVersion(av, entries, ko.entry)
		}
		av.Store(entries)
	}
	s.mu.Unlock()
}

// ApplySortedDirect applies ops that the caller has already sorted by Shard,
// taking each shard's write lock once for its contiguous run. A caller-sorted
// flat slice avoids per-shard grouping allocations.
// Caller must hold the proposer's conflict stripes for these keys.
func (e *MVCCEngine) ApplySortedDirect(ops []MVCCWriteOp) uint64 {
	if len(ops) == 0 {
		return e.seqCounter.Load()
	}
	startSeq, commitSeq := e.commits.reserve(&e.seqCounter, uint64(len(ops)))

	// One allocation for the whole transaction, not one per shard run.
	entries := newKeyedOps(ops, commitSeq)
	for i := 0; i < len(ops); {
		shardIdx := ops[i].Shard
		j := i + 1
		for j < len(ops) && ops[j].Shard == shardIdx {
			j++
		}
		e.applyShardEntries(shardIdx, entries[i:j])
		i = j
	}

	e.commits.complete(startSeq, commitSeq)
	return commitSeq
}

// ApplyShardDirect applies writes directly to a specific shard.
// The caller MUST already hold exclusive writer access to this shard
// (e.g., via the sharded proposer's CAS spinlock).
//
// SHORT-LOCK READERS: point lookups resolve the shard's map entry under a
// brief RLock (mvccShard.lookup); the entry-slice reads that follow use
// atomic pointer swaps and never block. The data map itself requires a
// write lock for add/delete operations (new keys, compaction cleanup),
// because Go doesn't allow concurrent map reads racing a map write. Writers
// hold it only briefly — just long enough to process all writes and store
// new slice pointers. Since the CAS spinlock serializes writers, the mutex
// only contends with point-read lockups and Compact(), which runs in the
// background every 100ms.
// Since the CAS spinlock serializes writers, the mutex only contends with
// Compact() which runs in the background every 100ms.
//
// Returns the commit sequence assigned to this batch.
func (e *MVCCEngine) ApplyShardDirect(shardIdx int, writes []MVCCWriteOp) uint64 { // Atomic commit protocol: reserve a sequence range for this shard's writes.
	startSeq, commitSeq := e.commits.reserve(&e.seqCounter, uint64(len(writes)))

	s := e.shards[shardIdx]

	// Build entries outside the lock; merge deltas are computed inside
	type keyedOp struct {
		key   string
		entry *mvccEntry
		op    byte
		value []byte // original value for merge
	}
	ops := make([]keyedOp, 0, len(writes))
	for _, w := range writes {
		entry := &mvccEntry{
			seq:     commitSeq,
			value:   w.Value,
			deleted: w.Op == 'd',
		}
		ops = append(ops, keyedOp{key: w.Key, entry: entry, op: w.Op, value: w.Value})
	}

	// Batch all map operations under a single lock acquisition
	s.mu.Lock()
	for _, ko := range ops {
		av, ok := s.data[ko.key]
		if !ok {
			av = newAtomicValue(nil)
			s.data[ko.key] = av
		}
		s.dirty[ko.key] = struct{}{}
		entries := av.Load()
		switch ko.op {
		case 's', 'd':
			entries = appendVersion(av, entries, ko.entry)
		case 'm':
			current := e.getCurrentValue(av, entries)
			delta := decodeInt64MVCC(ko.value)
			newVal := encodeInt64MVCC(decodeInt64MVCC(current) + delta)
			ko.entry.value = newVal
			entries = appendVersion(av, entries, ko.entry)
		}
		av.Store(entries)
	}
	s.mu.Unlock()

	e.commits.complete(startSeq, commitSeq)

	return commitSeq
}

// appendVersion appends entry to av, clearing the sorted hint when the new
// sequence is lower than the last appended one.
func appendVersion(av *atomicValue, entries []*mvccEntry, entry *mvccEntry) []*mvccEntry {
	if n := len(entries); n > 0 && entries[n-1].seq > entry.seq {
		av.sorted.Store(false)
	}
	return append(entries, entry)
}

// getCurrentValue returns the value of the newest version. Entries are
// appended in write order, which concurrent appliers make non-monotonic, so we
// scan for the highest sequence rather than assuming the last entry is newest.
func (e *MVCCEngine) getCurrentValue(av *atomicValue, entries []*mvccEntry) []byte {
	newest := -1
	if av.sorted.Load() {
		newest = len(entries) - 1
	} else {
		var newestSeq uint64
		for i, en := range entries {
			if newest < 0 || en.seq >= newestSeq {
				newest, newestSeq = i, en.seq
			}
		}
	}
	if newest < 0 || entries[newest].deleted {
		return nil
	}
	return entries[newest].value
}

func (e *MVCCEngine) CurrentSequence() uint64 {
	return e.seqCounter.Load()
}

func (e *MVCCEngine) DataDir() string { return e.dir }

func (e *MVCCEngine) CreateCheckpoint(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	allData := make(map[string][]byte)
	var applied uint64
	for i := range e.shards {
		s := e.shards[i]
		s.mu.RLock()
		for k, av := range s.data {
			entries := av.Load()
			for j := len(entries) - 1; j >= 0; j-- {
				if !entries[j].deleted {
					allData[k] = entries[j].value
					if entries[j].seq > applied {
						applied = entries[j].seq
					}
					break
				}
			}
		}
		s.mu.RUnlock()
	}
	b := encodeDump(allData, applied)
	return os.WriteFile(filepath.Join(dir, "dump"), b, 0o600)
}

func (e *MVCCEngine) RawIter(start, end []byte) engine.Iterator {
	return e.newIterator(start, end, e.commits.published.Load())
}

// IsSharded reports whether this engine uses sharded storage.
func (e *MVCCEngine) IsSharded() bool { return e.sharded }

// compactLoop runs background compaction periodically.
// It respects the oldest active snapshot: never removes versions that
// a live snapshot might still read.
func (e *MVCCEngine) compactLoop() {
	defer close(e.compactDone)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-e.compactStop:
			return
		case <-ticker.C:
			e.Compact()
		}
	}
}

// Compact removes old versions to reclaim memory.
// It respects the oldest active snapshot and in-flight transactions:
// - Never removes the latest version of any key
// - Never removes versions at or after the oldest active snapshot
// - Cleans up deleted keys when no snapshot references their tombstone
// - Keeps at least one version after the safety boundary
//
// Uses atomic pointer swaps so readers never block. Compaction creates
// new slices and stores them atomically — readers holding references to
// old slices continue safely.
func (e *MVCCEngine) Compact() int {
	// If any snapshots are active, be conservative: only remove versions
	// that are strictly before the oldest snapshot's sequence.
	safeSeq := e.snaps.oldest()

	var removed int
	for i := range e.shards {
		s := e.shards[i]
		s.mu.Lock()
		if len(s.dirty) == 0 {
			s.mu.Unlock()
			continue
		}
		dirty := s.dirty
		s.dirty = make(map[string]struct{}, len(dirty)/2+1)

		for k := range dirty {
			av, ok := s.data[k]
			if !ok {
				continue
			}
			entries := av.Load()
			if len(entries) == 0 {
				continue
			}

			// Find the two newest versions by sequence. Append order is write
			// order, not sequence order, so index position cannot be used.
			newest1, newest2 := -1, -1
			for i := range entries {
				if newest1 < 0 || entries[i].seq > entries[newest1].seq {
					newest2 = newest1
					newest1 = i
					continue
				}
				if newest2 < 0 || entries[i].seq > entries[newest2].seq {
					newest2 = i
				}
			}
			if newest1 < 0 {
				continue
			}

			// Tombstone cleanup: nothing can still read this key, so drop it.
			if safeSeq == 0 && entries[newest1].deleted {
				removed += len(entries)
				delete(s.data, k)
				continue
			}

			if len(entries) <= 2 {
				continue
			}

			kept := make([]*mvccEntry, 0, len(entries))
			for i, en := range entries {
				if i == newest1 || i == newest2 || (safeSeq > 0 && en.seq >= safeSeq) {
					kept = append(kept, en)
				}
			}
			if len(kept) == len(entries) {
				continue
			}
			// Restore ascending order so later lookups can binary search.
			sort.Slice(kept, func(i, j int) bool { return kept[i].seq < kept[j].seq })
			av.sorted.Store(true)
			removed += len(entries) - len(kept)
			av.Store(kept)
		}
		s.mu.Unlock()
	}
	return removed
}

// mvccBatch is a batch of MVCC operations.
type mvccBatchOp struct {
	op    byte
	key   string
	value []byte
}

type mvccBatch struct {
	ops []mvccBatchOp
}

func (b *mvccBatch) Set(key, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.ops = append(b.ops, mvccBatchOp{op: 's', key: string(key), value: cp})
}

func (b *mvccBatch) Delete(key []byte) {
	b.ops = append(b.ops, mvccBatchOp{op: 'd', key: string(key)})
}

func (b *mvccBatch) Merge(key, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.ops = append(b.ops, mvccBatchOp{op: 'm', key: string(key), value: cp})
}

func (b *mvccBatch) Reset() { b.ops = b.ops[:0] }

// mvccSnapshot is a point-in-time view of the MVCC engine.
// Creating a snapshot is O(1) — just capture the current sequence number.
type mvccSnapshot struct {
	seq    uint64
	engine *MVCCEngine
}

func (s *mvccSnapshot) Get(key []byte) ([]byte, error) {
	// The lookup key is only used for the duration of the map read, so it can
	// alias the caller's buffer instead of allocating a string per read.
	k := unsafe.String(unsafe.SliceData(key), len(key))
	sh := shardFor(k)
	st := s.engine.shards[sh]

	av := st.lookup(k)
	if av == nil {
		return nil, engine.ErrKeyNotFound
	}
	// Lock-free read: load the current entry slice atomically.
	// The writer may have just swapped to a newer slice, but our
	// snapshot sequence will filter out any entries we shouldn't see.
	entries := av.Load()
	if len(entries) == 0 {
		return nil, engine.ErrKeyNotFound
	}
	// Newest version visible at this snapshot's sequence.
	idx := latestVersionAt(av, entries, s.seq)
	if idx < 0 {
		return nil, engine.ErrKeyNotFound
	}
	entry := entries[idx]
	if entry.deleted {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(entry.value))
	copy(cp, entry.value)
	return cp, nil
}

// ViewValue returns the stored value without copying. Values are immutable:
// a write appends a new version rather than mutating an existing one, and
// compaction only swaps slice headers. See engine.ValueViewer.
func (s *mvccSnapshot) ViewValue(key []byte) ([]byte, error) {
	k := unsafe.String(unsafe.SliceData(key), len(key))
	sh := shardFor(k)
	av := s.engine.shards[sh].lookup(k)
	if av == nil {
		return nil, engine.ErrKeyNotFound
	}
	entries := av.Load()
	idx := latestVersionAt(av, entries, s.seq)
	if idx < 0 || entries[idx].deleted {
		return nil, engine.ErrKeyNotFound
	}
	return entries[idx].value, nil
}

func (s *mvccSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := "__dpx:ver:" + string(key)
	sh := shardFor(vk)
	st := s.engine.shards[sh]

	av := st.lookup(vk)
	if av == nil {
		return engine.EpochRecord{}, nil
	}
	entries := av.Load()
	if len(entries) == 0 {
		return engine.EpochRecord{}, nil
	}
	idx := latestVersionAt(av, entries, s.seq)
	if idx < 0 {
		return engine.EpochRecord{}, nil
	}
	entry := entries[idx]
	return decodeEpochRecordMVCC(entry.value), nil
}

func (s *mvccSnapshot) NewIter(start, end []byte) engine.Iterator {
	return s.engine.newIterator(start, end, s.seq)
}

func (s *mvccSnapshot) Sequence() uint64 { return s.seq }
func (s *mvccSnapshot) Close() error {
	// Release this snapshot from tracking.
	s.engine.snaps.remove(s.seq)
	return nil
}

// latestVersionAt returns the index of the newest version visible at target:
// the entry with the greatest seq <= target, ties resolving to the later append.
//
// Append order is write order, not sequence order: two concurrent appliers can
// write a higher sequence before a lower one. A binary search over an unsorted
// slice returns the wrong version, so this scans (version chains are short —
// compaction keeps at most a couple of versions past the oldest snapshot).
func latestVersionAt(av *atomicValue, entries []*mvccEntry, target uint64) int {
	if av.sorted.Load() {
		i := sort.Search(len(entries), func(i int) bool { return entries[i].seq > target })
		if i == 0 {
			return -1
		}
		return i - 1
	}
	best := -1
	var bestSeq uint64
	for i, en := range entries {
		if en.seq > target {
			continue
		}
		if best < 0 || en.seq >= bestSeq {
			best, bestSeq = i, en.seq
		}
	}
	return best
}

// newIterator creates an iterator over the key range at the given sequence.
func (e *MVCCEngine) newIterator(start, end []byte, seq uint64) engine.Iterator {
	var pairs [][2][]byte
	ss, se := string(start), string(end)
	prefix := "__dpx:"

	for i := range e.shards {
		st := e.shards[i]
		st.mu.RLock()
		for k, av := range st.data {
			// Filter by range
			if k < ss {
				continue
			}
			if se != "" && k >= se {
				continue
			}
			// Skip internal keys
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				continue
			}
			// Lock-free read of entry slice
			entries := av.Load()
			if len(entries) == 0 {
				continue
			}
			// Find the right version
			idx := latestVersionAt(av, entries, seq)
			if idx >= 0 && !entries[idx].deleted {
				cp := make([]byte, len(entries[idx].value))
				copy(cp, entries[idx].value)
				pairs = append(pairs, [2][]byte{[]byte(k), cp})
			}
		}
		st.mu.RUnlock()
	}

	sort.Slice(pairs, func(i, j int) bool {
		return string(pairs[i][0]) < string(pairs[j][0])
	})

	return &mvccIter{pairs: pairs, pos: -1}
}

// mvccIter is an iterator over MVCC data.
type mvccIter struct {
	pairs [][2][]byte
	pos   int
}

func (it *mvccIter) First() bool {
	it.pos = 0
	return it.pos < len(it.pairs)
}

func (it *mvccIter) Next() bool {
	it.pos++
	return it.pos < len(it.pairs)
}

func (it *mvccIter) Prev() bool {
	if it.pos <= 0 {
		it.pos = -1
		return false
	}
	it.pos--
	return true
}

func (it *mvccIter) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.pairs)
}

func (it *mvccIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.pairs[it.pos][0]
}

func (it *mvccIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.pairs[it.pos][1]
}

func (it *mvccIter) Error() error { return nil }
func (it *mvccIter) Close() error { return nil }

// Helpers

func encodeInt64MVCC(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func decodeInt64MVCC(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func decodeEpochRecordMVCC(b []byte) engine.EpochRecord {
	if len(b) < 9 {
		return engine.EpochRecord{}
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b[:8]),
		IsCredit: b[8] == 1,
	}
}

// snapshotTracker tracks the exact set of live snapshot sequences so
// compaction can prune versions that no snapshot can still read.
type snapshotTracker struct {
	mu   sync.Mutex
	live map[uint64]int
	min  atomic.Uint64 // cached oldest live sequence, 0 = none
}

func (t *snapshotTracker) add(seq uint64) {
	t.mu.Lock()
	if t.live == nil {
		t.live = make(map[uint64]int, 64)
	}
	t.live[seq]++
	if cur := t.min.Load(); cur == 0 || seq < cur {
		t.min.Store(seq)
	}
	t.mu.Unlock()
}

func (t *snapshotTracker) remove(seq uint64) {
	t.mu.Lock()
	if n, ok := t.live[seq]; ok {
		if n <= 1 {
			delete(t.live, seq)
		} else {
			t.live[seq] = n - 1
		}
	}
	if t.min.Load() == seq {
		var min uint64
		first := true
		for s := range t.live {
			if first || s < min {
				min, first = s, false
			}
		}
		t.min.Store(min)
	}
	t.mu.Unlock()
}

// oldest returns the lowest live snapshot sequence, or 0 when none are open.
func (t *snapshotTracker) oldest() uint64 {
	return t.min.Load()
}

// commitTracker publishes commits in sequence order.
//
// A commit reserves a sequence range under the tracker mutex before writing any
// shard, then marks the range complete after the last shard write. published
// advances only across a contiguous prefix of completed ranges, so a snapshot
// never sees a commit whose shards are partially written.
type commitTracker struct {
	mu        sync.Mutex
	done      map[uint64]uint64 // range start -> end, completed but not publishable yet
	published atomic.Uint64
}

func newCommitTracker() *commitTracker {
	return &commitTracker{done: make(map[uint64]uint64, 64)}
}

// reset sets the published point and drops any tracked ranges. Used when
// restoring from a checkpoint, where everything up to seq is already durable.
func (t *commitTracker) reset(seq uint64) {
	t.mu.Lock()
	t.done = make(map[uint64]uint64, 64)
	t.published.Store(seq)
	t.mu.Unlock()
}

// reserve allocates n contiguous sequence numbers and returns the inclusive
// range [start, end]. Allocation happens under the tracker mutex so ranges are
// always contiguous and in ascending order.
func (t *commitTracker) reserve(counter *atomic.Uint64, n uint64) (start, end uint64) {
	t.mu.Lock()
	end = counter.Add(n)
	start = end - n + 1
	t.mu.Unlock()
	return start, end
}

// complete marks a reserved range as fully applied and advances published over
// any contiguous prefix that is now complete.
func (t *commitTracker) complete(start, end uint64) {
	t.mu.Lock()
	t.done[start] = end
	for {
		cur := t.published.Load()
		next, ok := t.done[cur+1]
		if !ok {
			break
		}
		t.published.Store(next)
		delete(t.done, cur+1)
	}
	t.mu.Unlock()
}
