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

	// lastCommitted tracks the highest sequence for which all entries have been
	// written to shard slices. Updated at the end of ApplyBatch after all shards
	// are written. Snapshots use this instead of seqCounter to avoid seeing
	// entries from a batch that hasn't finished writing yet.
	lastCommitted atomic.Uint64

	// Compaction threshold: if a key has more versions than this, compact
	compactThreshold int

	// Active snapshot tracking for safe compaction
	oldestSnapshot atomic.Uint64 // lowest sequence of any open snapshot
	snapshotCount  atomic.Int64  // number of open snapshots

	// Background compaction control
	compactStop chan struct{}
	compactDone chan struct{}
}

// mvccShard is a single shard's MVCC data.
type mvccShard struct {
	mu   sync.RWMutex
	data map[string][]*mvccEntry // key -> sorted list of versions
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
	e.oldestSnapshot.Store(0)
	for i := range e.shards {
		e.shards[i] = &mvccShard{
			data: make(map[string][]*mvccEntry, 1024),
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
	for k, v := range data {
		sh := shardFor(k)
		e.shards[sh].mu.Lock()
		e.shards[sh].data[k] = append(e.shards[sh].data[k], &mvccEntry{
			seq:   applied,
			value: v,
		})
		e.shards[sh].mu.Unlock()
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
	sh := shardFor(string(key))
	s := e.shards[sh]
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, ok := s.data[string(key)]
	if !ok {
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
	// Read the last committed sequence. This is the highest sequence for which
	// all entries have been fully written to the shard slices. We use a separate
	// atomic to avoid the race where seqCounter has been incremented by a batch
	// that hasn't finished writing its entries yet.
	seq := e.lastCommitted.Load()
	snap := &mvccSnapshot{
		seq:    seq,
		engine: e,
	}
	// Track this snapshot for safe compaction.
	// Only update oldestSnapshot if this snapshot is older than the current tracked one.
	e.snapshotCount.Add(1)
	for {
		old := e.oldestSnapshot.Load()
		if old == 0 || seq < old {
			if e.oldestSnapshot.CompareAndSwap(old, seq) {
				break
			}
			// CAS failed, another goroutine updated it; retry
			continue
		}
		// This snapshot is newer than the oldest; no need to update
		break
	}
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

	// Atomic commit protocol: assign a single commit sequence for the entire batch.
	// All keys in this batch become visible at the same sequence, so a snapshot
	// cannot see only part of the transaction.
	commitSeq := e.seqCounter.Add(uint64(len(mb.ops)))

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
			switch op.op {
			case 's':
				s.data[op.key] = append(s.data[op.key], entry)
			case 'd':
				s.data[op.key] = append(s.data[op.key], entry)
			case 'm':
				// For merge, find the current value and add delta
				current := e.getCurrentValue(s.data[op.key])
				delta := decodeInt64MVCC(op.value)
				newVal := encodeInt64MVCC(decodeInt64MVCC(current) + delta)
				entry.value = newVal
				s.data[op.key] = append(s.data[op.key], entry)
			}
		}
		s.mu.Unlock()
	}

	// Mark this commit as fully visible. Snapshots taken after this point
	// will see all entries in this batch.
	e.lastCommitted.Store(commitSeq)

	return nil
}

// MVCCWriteOp is a single write operation for ApplyShardDirect.
type MVCCWriteOp struct {
	Key   string
	Value []byte
	Op    byte // 's' = set, 'd' = delete, 'm' = merge
}

// ApplyShardDirect applies writes directly to a specific shard.
// The caller MUST already hold exclusive writer access to this shard
// (e.g., via the sharded proposer's CAS spinlock). The RWMutex is still
// acquired to exclude concurrent readers (snapshots, Compact), but since
// the CAS spinlock already serializes writers, the lock only contends with
// readers, not other writers — this is much faster than the full ApplyBatch
// path where the RWMutex serializes both readers and writers.
//
// Returns the commit sequence assigned to this batch.
func (e *MVCCEngine) ApplyShardDirect(shardIdx int, writes []MVCCWriteOp) uint64 {
	// Atomic commit protocol: assign a single commit sequence for this shard's writes.
	commitSeq := e.seqCounter.Add(uint64(len(writes)))

	s := e.shards[shardIdx]
	s.mu.Lock()
	// Writer-writer exclusion is already handled by CAS spinlock in the caller.
	// This lock only excludes readers (snapshots, Compact) during the write.

	for _, w := range writes {
		entry := &mvccEntry{
			seq:     commitSeq,
			value:   w.Value,
			deleted: w.Op == 'd',
		}
		switch w.Op {
		case 's':
			s.data[w.Key] = append(s.data[w.Key], entry)
		case 'd':
			s.data[w.Key] = append(s.data[w.Key], entry)
		case 'm':
			// For merge, find the current value and add delta
			entries := s.data[w.Key]
			current := e.getCurrentValue(entries)
			delta := decodeInt64MVCC(w.Value)
			newVal := encodeInt64MVCC(decodeInt64MVCC(current) + delta)
			entry.value = newVal
			s.data[w.Key] = append(entries, entry)
		}
	}

	s.mu.Unlock()

	// Mark this commit as fully visible.
	e.lastCommitted.Store(commitSeq)

	return commitSeq
}

func (e *MVCCEngine) getCurrentValue(entries []*mvccEntry) []byte {
	for i := len(entries) - 1; i >= 0; i-- {
		if !entries[i].deleted {
			return entries[i].value
		}
	}
	return nil
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
		for k, entries := range s.data {
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
	return e.newIterator(start, end, e.seqCounter.Load())
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
// IMPORTANT: Compaction must not modify slices that active snapshots may be
// reading. Since snapshots hold no reference to the slice itself (they just
// hold the sequence number and look up the shard's current slice under RLock),
// we must ensure the slice replacement is safe. The shard's mutex protects
// against concurrent modification, but a snapshot holding RLock could be
// iterating the old slice while we replace it. To be safe, we only compact
// when no snapshots are active, or we keep enough versions for the oldest snapshot.
func (e *MVCCEngine) Compact() int {
	// If any snapshots are active, be conservative: only remove versions
	// that are strictly before the oldest snapshot's sequence.
	safeSeq := uint64(0)
	if e.snapshotCount.Load() > 0 {
		safeSeq = e.oldestSnapshot.Load()
	}

	var removed int
	for i := range e.shards {
		s := e.shards[i]
		s.mu.Lock()
		for k, entries := range s.data {
			// Clean up tombstones first: if the latest entry is a delete and
			// there are no active snapshots that could see it, remove the key entirely
			if safeSeq == 0 && len(entries) >= 1 && entries[len(entries)-1].deleted {
				removed += len(entries)
				delete(s.data, k)
				continue
			}

			if len(entries) <= 2 {
				continue
			}

			// Find how many entries are strictly before the safety boundary.
			// These are the only ones we can safely remove.
			removeCount := 0
			if safeSeq > 0 {
				// Count entries with seq < safeSeq
				for j, entry := range entries {
					if entry.seq < safeSeq {
						removeCount = j + 1
					} else {
						break
					}
				}
				// Always keep at least the last 2 entries
				keep := len(entries) - removeCount
				if keep < 2 {
					removeCount = len(entries) - 2
				}
			} else {
				// No active snapshots: can remove all but the last 2
				removeCount = len(entries) - 2
			}

			if removeCount > 0 {
				removed += removeCount
				// Create a new slice with only the kept entries.
				// This is safe because we hold the write lock.
				kept := make([]*mvccEntry, len(entries)-removeCount)
				copy(kept, entries[removeCount:])
				s.data[k] = kept
			}
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
	sh := shardFor(string(key))
	st := s.engine.shards[sh]
	st.mu.RLock()
	defer st.mu.RUnlock()

	entries, ok := st.data[string(key)]
	if !ok {
		return nil, engine.ErrKeyNotFound
	}
	// Binary search for the latest entry <= snapshot seq
	idx := s.findVersion(entries, s.seq)
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

func (s *mvccSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	vk := "__dpx:ver:" + string(key)
	sh := shardFor(vk)
	st := s.engine.shards[sh]
	st.mu.RLock()
	defer st.mu.RUnlock()

	entries, ok := st.data[vk]
	if !ok {
		return engine.EpochRecord{}, nil
	}
	idx := s.findVersion(entries, s.seq)
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
	// Release this snapshot from tracking
	s.engine.snapshotCount.Add(-1)
	return nil
}

// findVersion performs binary search for the latest entry with seq <= targetSeq.
func (s *mvccSnapshot) findVersion(entries []*mvccEntry, targetSeq uint64) int {
	// Entries are appended in order, so seq is monotonically increasing.
	// We need the latest entry with seq <= targetSeq.
	low := 0
	hi := len(entries) - 1
	result := -1
	for low <= hi {
		mid := low + (hi-low)/2
		if entries[mid].seq <= targetSeq {
			result = mid
			low = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

// newIterator creates an iterator over the key range at the given sequence.
func (e *MVCCEngine) newIterator(start, end []byte, seq uint64) engine.Iterator {
	var pairs [][2][]byte
	ss, se := string(start), string(end)
	prefix := "__dpx:"

	for i := range e.shards {
		st := e.shards[i]
		st.mu.RLock()
		for k, entries := range st.data {
			// Filter by range
			if k < ss {
				continue
			}
			if se != "" && k >= se {
				continue
			}
			// Skip internal keys
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				st.mu.RUnlock()
				continue
			}
			// Find the right version
			idx := findVersionLinear(entries, seq)
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

func findVersionLinear(entries []*mvccEntry, seq uint64) int {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].seq <= seq {
			return i
		}
	}
	return -1
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
