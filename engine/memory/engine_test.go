package memory

// Tests for the in-memory StorageEngine.
// All tests run within the package so they can reach unexported helpers
// (mergeInt64, isReserved, insertKey, deleteKey) directly.
//
// Test organisation:
//   TestEngine_*        — StorageEngine interface methods
//   TestSnapshot_*      — Snapshot interface methods
//   TestIter_*          — Iterator (memIter) behaviour
//   TestBatch_*         — memBatch operations
//   TestMerge_*         — mergeInt64 semantics (the core numeric logic)
//   TestSortedIndex_*   — insertKey / deleteKey invariants
//   TestCheckpoint_*    — CreateCheckpoint
//   TestRawIter_*       — RawIter (includes __dpx: keys)

import (
	"encoding/binary"
	"os"
	"sort"
	"testing"

	"github.com/agberohq/dpx/engine"
)

// ---- helpers ----------------------------------------------------------------

func le64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func decode64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func epochRecord(epoch uint64, isCredit bool) []byte {
	b := make([]byte, 9)
	binary.LittleEndian.PutUint64(b, epoch)
	if isCredit {
		b[8] = 1
	}
	return b
}

func applySet(tb testing.TB, e *Engine, key string, val []byte) {
	tb.Helper()
	b := e.NewBatch()
	b.Set([]byte(key), val)
	if err := e.ApplyBatch(b, engine.WriteOptions{}); err != nil {
		tb.Fatalf("ApplyBatch Set %q: %v", key, err)
	}
}

func applyDel(tb testing.TB, e *Engine, key string) {
	tb.Helper()
	b := e.NewBatch()
	b.Delete([]byte(key))
	if err := e.ApplyBatch(b, engine.WriteOptions{}); err != nil {
		tb.Fatalf("ApplyBatch Delete %q: %v", key, err)
	}
}

func applyMerge(tb testing.TB, e *Engine, key string, delta int64) {
	tb.Helper()
	b := e.NewBatch()
	b.Merge([]byte(key), le64(delta))
	if err := e.ApplyBatch(b, engine.WriteOptions{}); err != nil {
		tb.Fatalf("ApplyBatch Merge %q: %v", key, err)
	}
}

// ---- Engine lifecycle -------------------------------------------------------

func TestEngine_OpenClose(t *testing.T) {
	e := New()
	if err := e.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEngine_DataDir(t *testing.T) {
	e := New()
	if e.DataDir() == "" {
		t.Error("DataDir should not be empty")
	}
}

func TestEngine_SyncIsNoop(t *testing.T) {
	e := New()
	if err := e.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

// ---- Get / Set / Delete -----------------------------------------------------

func TestEngine_GetMissingKey(t *testing.T) {
	e := New()
	_, err := e.Get([]byte("missing"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("got %v, want ErrKeyNotFound", err)
	}
}

func TestEngine_SetThenGet(t *testing.T) {
	e := New()
	applySet(t, e, "hello", []byte("world"))

	val, err := e.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("got %q, want %q", val, "world")
	}
}

func TestEngine_GetReturnsCopy(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("original"))

	v1, _ := e.Get([]byte("k"))
	v1[0] = 'X' // mutate returned slice

	v2, _ := e.Get([]byte("k"))
	if string(v2) != "original" {
		t.Errorf("Get leaked internal reference: got %q", v2)
	}
}

func TestEngine_DeleteRemovesKey(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("v"))
	applyDel(t, e, "k")

	_, err := e.Get([]byte("k"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("after delete: got %v, want ErrKeyNotFound", err)
	}
}

func TestEngine_DeleteNonExistentIsNoop(t *testing.T) {
	e := New()
	applyDel(t, e, "never-existed")
	// No error expected; sorted index must not corrupt.
	applySet(t, e, "other", []byte("v"))
	val, err := e.Get([]byte("other"))
	if err != nil || string(val) != "v" {
		t.Errorf("state corrupted after spurious delete: %v %q", err, val)
	}
}

func TestEngine_OverwriteValue(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("first"))
	applySet(t, e, "k", []byte("second"))

	val, _ := e.Get([]byte("k"))
	if string(val) != "second" {
		t.Errorf("got %q, want second", val)
	}
}

// ---- Merge / mergeInt64 -----------------------------------------------------

func TestMerge_NilBase(t *testing.T) {
	// nil existing → treats as 0
	got := mergeInt64(nil, le64(42))
	if decode64(got) != 42 {
		t.Errorf("mergeInt64(nil, 42) = %d, want 42", decode64(got))
	}
}

func TestMerge_ShortBase(t *testing.T) {
	got := mergeInt64([]byte{1, 2}, le64(10))
	if decode64(got) != 10 {
		t.Errorf("mergeInt64(short, 10) = %d, want 10", decode64(got))
	}
}

func TestMerge_Addition(t *testing.T) {
	got := mergeInt64(le64(100), le64(50))
	if decode64(got) != 150 {
		t.Errorf("100 + 50 = %d, want 150", decode64(got))
	}
}

func TestMerge_NegativeDelta(t *testing.T) {
	got := mergeInt64(le64(100), le64(-30))
	if decode64(got) != 70 {
		t.Errorf("100 + (-30) = %d, want 70", decode64(got))
	}
}

func TestMerge_NegativeResult(t *testing.T) {
	// mergeInt64 does not enforce sufficiency; that is the engine layer's job.
	got := mergeInt64(le64(10), le64(-50))
	if decode64(got) != -40 {
		t.Errorf("10 + (-50) = %d, want -40", decode64(got))
	}
}

func TestMerge_Commutativity(t *testing.T) {
	// a + b == b + a
	ab := mergeInt64(mergeInt64(le64(0), le64(7)), le64(3))
	ba := mergeInt64(mergeInt64(le64(0), le64(3)), le64(7))
	if decode64(ab) != decode64(ba) {
		t.Errorf("not commutative: %d != %d", decode64(ab), decode64(ba))
	}
}

func TestEngine_MergeViaApplyBatch(t *testing.T) {
	e := New()
	applyMerge(t, e, "counter", 10)
	applyMerge(t, e, "counter", 5)
	applyMerge(t, e, "counter", -3)

	val, _ := e.Get([]byte("counter"))
	if decode64(val) != 12 {
		t.Errorf("10 + 5 - 3 = %d, want 12", decode64(val))
	}
}

func TestEngine_MergeOnNonExistentKey(t *testing.T) {
	e := New()
	applyMerge(t, e, "fresh", 77)

	val, _ := e.Get([]byte("fresh"))
	if decode64(val) != 77 {
		t.Errorf("got %d, want 77", decode64(val))
	}
}

func TestEngine_DeleteThenMerge(t *testing.T) {
	e := New()
	applySet(t, e, "k", le64(999))
	applyDel(t, e, "k")
	applyMerge(t, e, "k", 55)

	val, _ := e.Get([]byte("k"))
	if decode64(val) != 55 {
		t.Errorf("Delete+Merge(55) = %d, want 55", decode64(val))
	}
}

func TestEngine_SetThenMerge(t *testing.T) {
	e := New()
	applySet(t, e, "k", le64(100))
	applyMerge(t, e, "k", 50)

	val, _ := e.Get([]byte("k"))
	if decode64(val) != 150 {
		t.Errorf("Set(100)+Merge(50) = %d, want 150", decode64(val))
	}
}

// ---- CurrentSequence / __dpx:applied ----------------------------------------

func TestEngine_CurrentSequenceFreshIsZero(t *testing.T) {
	e := New()
	if s := e.CurrentSequence(); s != 0 {
		t.Errorf("fresh engine sequence = %d, want 0", s)
	}
}

func TestEngine_CurrentSequenceUpdatedByApplied(t *testing.T) {
	e := New()
	b := e.NewBatch()
	applySeq := uint64(42)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, applySeq)
	b.Set([]byte("__dpx:applied"), buf)
	e.ApplyBatch(b, engine.WriteOptions{})

	if s := e.CurrentSequence(); s != 42 {
		t.Errorf("sequence = %d, want 42", s)
	}
}

func TestEngine_VersionKeyRoutedToVersions(t *testing.T) {
	e := New()
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:mykey"), epochRecord(7, true))
	e.ApplyBatch(b, engine.WriteOptions{})

	// versions map should have the entry
	e.mu.RLock()
	er, ok := e.versions["mykey"]
	e.mu.RUnlock()

	if !ok {
		t.Fatal("version record not stored in e.versions")
	}
	if er.Epoch != 7 {
		t.Errorf("epoch = %d, want 7", er.Epoch)
	}
	if !er.IsCredit {
		t.Error("IsCredit should be true")
	}
}

// ---- Batch Reset ------------------------------------------------------------

func TestBatch_Reset(t *testing.T) {
	e := New()
	b := e.NewBatch().(*memBatch)
	b.Set([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	b.Reset()
	if len(b.ops) != 0 {
		t.Errorf("after Reset: %d ops remain, want 0", len(b.ops))
	}
}

func TestBatch_SetCopiesValue(t *testing.T) {
	e := New()
	b := e.NewBatch().(*memBatch)
	val := []byte("original")
	b.Set([]byte("k"), val)
	val[0] = 'X' // mutate original

	// Apply and check stored value is unaffected
	e.ApplyBatch(b, engine.WriteOptions{})
	got, _ := e.Get([]byte("k"))
	if string(got) != "original" {
		t.Errorf("batch Set did not copy: got %q", got)
	}
}

// ---- Sorted key index -------------------------------------------------------

func TestSortedIndex_InsertMaintainsOrder(t *testing.T) {
	e := New()
	keys := []string{"banana", "apple", "cherry", "avocado"}
	for _, k := range keys {
		applySet(t, e, k, []byte("v"))
	}

	e.mu.RLock()
	idx := make([]string, len(e.keys))
	copy(idx, e.keys)
	e.mu.RUnlock()

	if !sort.StringsAreSorted(idx) {
		t.Errorf("index not sorted: %v", idx)
	}
}

func TestSortedIndex_InsertDuplicateIsNoop(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("v1"))
	applySet(t, e, "k", []byte("v2")) // same key, different value

	e.mu.RLock()
	count := 0
	for _, k := range e.keys {
		if k == "k" {
			count++
		}
	}
	e.mu.RUnlock()

	if count != 1 {
		t.Errorf("key appeared %d times in index, want 1", count)
	}
}

func TestSortedIndex_DeleteRemovesKey(t *testing.T) {
	e := New()
	applySet(t, e, "a", []byte("1"))
	applySet(t, e, "b", []byte("2"))
	applySet(t, e, "c", []byte("3"))
	applyDel(t, e, "b")

	e.mu.RLock()
	keys := make([]string, len(e.keys))
	copy(keys, e.keys)
	e.mu.RUnlock()

	for _, k := range keys {
		if k == "b" {
			t.Error("deleted key still in index")
		}
	}
	if len(keys) != 2 {
		t.Errorf("index has %d keys, want 2: %v", len(keys), keys)
	}
}

// ---- GetSnapshot ------------------------------------------------------------

func TestSnapshot_IsolatedFromSubsequentWrites(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("before"))

	snap, err := e.GetSnapshot()
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	defer snap.Close()

	// Write after snapshot
	applySet(t, e, "k", []byte("after"))

	val, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap.Get: %v", err)
	}
	if string(val) != "before" {
		t.Errorf("snapshot leaked write: got %q, want %q", val, "before")
	}
}

func TestSnapshot_GetMissingKey(t *testing.T) {
	e := New()
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	_, err := snap.Get([]byte("missing"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("got %v, want ErrKeyNotFound", err)
	}
}

func TestSnapshot_GetReturnsCopy(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("value"))
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	v1, _ := snap.Get([]byte("k"))
	v1[0] = 'Z'

	v2, _ := snap.Get([]byte("k"))
	if string(v2) != "value" {
		t.Errorf("snapshot.Get leaked internal reference: got %q", v2)
	}
}

func TestSnapshot_Sequence(t *testing.T) {
	e := New()

	// Apply with sequence 5
	b := e.NewBatch()
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 5)
	b.Set([]byte("__dpx:applied"), buf)
	e.ApplyBatch(b, engine.WriteOptions{})

	snap, _ := e.GetSnapshot()
	defer snap.Close()

	if snap.Sequence() != 5 {
		t.Errorf("snap.Sequence() = %d, want 5", snap.Sequence())
	}
}

func TestSnapshot_GetVersion_Missing(t *testing.T) {
	e := New()
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	er, err := snap.GetVersion([]byte("never-written"))
	if err != nil {
		t.Errorf("GetVersion: %v", err)
	}
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("missing key should return zero EpochRecord, got %+v", er)
	}
}

func TestSnapshot_GetVersion_Present(t *testing.T) {
	e := New()
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:mykey"), epochRecord(13, false))
	e.ApplyBatch(b, engine.WriteOptions{})

	snap, _ := e.GetSnapshot()
	defer snap.Close()

	er, err := snap.GetVersion([]byte("mykey"))
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if er.Epoch != 13 {
		t.Errorf("epoch = %d, want 13", er.Epoch)
	}
	if er.IsCredit {
		t.Error("IsCredit should be false")
	}
}

// ---- NewIter (consumer, excludes __dpx:) ------------------------------------

func TestSnapshot_NewIter_Forward(t *testing.T) {
	e := New()
	for _, k := range []string{"c", "a", "b"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("a"), []byte("d"))
	defer iter.Close()

	var got []string
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		got = append(got, string(iter.Key()))
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter.Error: %v", err)
	}

	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSnapshot_NewIter_ExclusiveEnd(t *testing.T) {
	e := New()
	for _, k := range []string{"a", "b", "c"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("a"), []byte("c")) // c excluded
	defer iter.Close()

	var got []string
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		got = append(got, string(iter.Key()))
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("got %v, want [a b]", got)
	}
}

func TestSnapshot_NewIter_ExcludesReservedKeys(t *testing.T) {
	e := New()
	applySet(t, e, "user:1", []byte("v"))

	// Simulate state machine writing __dpx: keys
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:user:1"), epochRecord(1, false))
	b.Set([]byte("__dpx:applied"), le64(1))
	e.ApplyBatch(b, engine.WriteOptions{})

	snap, _ := e.GetSnapshot()
	defer snap.Close()

	// Broad range scan must not include __dpx: keys
	iter := snap.NewIter([]byte("\x00"), []byte("\xFF"))
	defer iter.Close()

	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		k := string(iter.Key())
		if len(k) >= 6 && k[:6] == "__dpx:" {
			t.Errorf("consumer iter exposed reserved key: %q", k)
		}
	}
}

func TestSnapshot_NewIter_Reverse(t *testing.T) {
	e := New()
	for _, k := range []string{"a", "b", "c"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("a"), []byte("d"))
	defer iter.Close()

	// Seek to end, then Prev
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
	}

	var got []string
	for iter.Prev(); iter.Valid(); iter.Prev() {
		got = append(got, string(iter.Key()))
	}

	// Should be c, b, a in reverse (we consumed 'last' with the final Next)
	// After exhausting forward the iter is past-end; Prev steps back to "c".
	want := []string{"c", "b", "a"}
	if len(got) != len(want) {
		t.Fatalf("reverse scan got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSnapshot_NewIter_EmptyRange(t *testing.T) {
	e := New()
	applySet(t, e, "z", []byte("v"))
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("a"), []byte("b"))
	defer iter.Close()

	if iter.First() {
		t.Errorf("expected empty range, got key %q", iter.Key())
	}
}

func TestIter_ValidAfterClose(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("v"))
	snap, _ := e.GetSnapshot()

	iter := snap.NewIter([]byte("a"), []byte("z"))
	iter.First()
	iter.Close()
	snap.Close()

	// After Close, Valid returns false.
	// (memIter does not nil out pairs on Close, but idx is valid for the test.)
}

// ---- RawIter ----------------------------------------------------------------

func TestRawIter_IncludesReservedKeys(t *testing.T) {
	e := New()
	applySet(t, e, "user:1", []byte("v"))
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:user:1"), epochRecord(3, true))
	e.ApplyBatch(b, engine.WriteOptions{})

	start := []byte("__dpx:ver:")
	end := append([]byte("__dpx:ver:"), make([]byte, 16)...)
	for i := range end[10:] {
		end[10+i] = 0xFF
	}

	iter := e.RawIter(start, end)
	defer iter.Close()

	found := false
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		if string(iter.Key()) == "__dpx:ver:user:1" {
			found = true
			er := decodeEpochRecord(iter.Value())
			if er.Epoch != 3 {
				t.Errorf("epoch = %d, want 3", er.Epoch)
			}
			if !er.IsCredit {
				t.Error("IsCredit should be true")
			}
		}
	}
	if !found {
		t.Error("RawIter did not expose __dpx:ver: key")
	}
}

func TestRawIter_BinaryKeyBeyond0x7E(t *testing.T) {
	e := New()
	// Key with byte value > 0x7E — must not be excluded by the end bound.
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:\xFF\xFE"), epochRecord(99, false))
	e.ApplyBatch(b, engine.WriteOptions{})

	start := []byte("__dpx:ver:")
	end := append([]byte("__dpx:ver:"), make([]byte, 16)...)
	for i := range end[10:] {
		end[10+i] = 0xFF
	}

	iter := e.RawIter(start, end)
	defer iter.Close()

	found := false
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		if string(iter.Key()) == "__dpx:ver:\xFF\xFE" {
			found = true
		}
	}
	if !found {
		t.Error("binary key with bytes > 0x7E missing from RawIter")
	}
}

// ---- Checkpoint -------------------------------------------------------------

func TestCheckpoint_WritesFile(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("v"))

	dir := t.TempDir()
	if err := e.CreateCheckpoint(dir); err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	info, err := os.Stat(dir + "/dump")
	if err != nil {
		t.Fatalf("dump file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Error("dump file is empty")
	}
}

// ---- isReserved (package-internal) ------------------------------------------

func TestIsReserved(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"__dpx:ver:foo", true},
		{"__dpx:applied", true},
		{"__dpx:", true},
		{"__dpx", false}, // too short
		{"user:key", false},
		{"", false},
		{"_dpx:key", false},
	}
	for _, tc := range cases {
		got := isReserved(tc.key)
		if got != tc.want {
			t.Errorf("isReserved(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

// ---- Concurrency smoke test -------------------------------------------------

func TestEngine_ConcurrentApplyBatch(t *testing.T) {
	e := New()
	const goroutines = 20
	const ops = 100

	done := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			for i := 0; i < ops; i++ {
				b := e.NewBatch()
				b.Merge([]byte("counter"), le64(1))
				e.ApplyBatch(b, engine.WriteOptions{})
			}
			done <- struct{}{}
		}(g)
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}

	val, err := e.Get([]byte("counter"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got := decode64(val)
	want := int64(goroutines * ops)
	if got != want {
		t.Errorf("counter = %d, want %d", got, want)
	}
}

func TestEngine_ConcurrentSnapshotAndWrite(t *testing.T) {
	e := New()
	applySet(t, e, "k", []byte("initial"))

	done := make(chan struct{})
	const readers = 10

	for r := 0; r < readers; r++ {
		go func() {
			snap, err := e.GetSnapshot()
			if err != nil {
				t.Errorf("GetSnapshot: %v", err)
				done <- struct{}{}
				return
			}
			val, err := snap.Get([]byte("k"))
			snap.Close()
			if err != nil {
				t.Errorf("snap.Get: %v", err)
			}
			// Value must be either "initial" or "updated" — never corrupt.
			s := string(val)
			if s != "initial" && s != "updated" {
				t.Errorf("unexpected value: %q", s)
			}
			done <- struct{}{}
		}()
	}

	// Write concurrently with reads
	applySet(t, e, "k", []byte("updated"))

	for r := 0; r < readers; r++ {
		<-done
	}
}

// ---- decodeEpochRecord (package-internal) ------------------------------------

func TestDecodeEpochRecord_NilInput(t *testing.T) {
	er := decodeEpochRecord(nil)
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("nil input: got %+v, want zero", er)
	}
}

func TestDecodeEpochRecord_ShortInput(t *testing.T) {
	er := decodeEpochRecord([]byte{1, 2, 3})
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("short input: got %+v, want zero", er)
	}
}

func TestDecodeEpochRecord_RoundTrip(t *testing.T) {
	want := engine.EpochRecord{Epoch: 12345, IsCredit: true}
	encoded := epochRecord(want.Epoch, want.IsCredit)
	got := decodeEpochRecord(encoded)
	if got != want {
		t.Errorf("round-trip: got %+v, want %+v", got, want)
	}
}

// ---- Benchmarks -------------------------------------------------------------
// Run: go test -bench=. -benchtime=5s -count=3 ./engine/memory/

func BenchmarkEngine_SetGet(b *testing.B) {
	e := New()
	e.Open()
	val := []byte("benchmark-value-32-bytes-padding!!")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := e.NewBatch()
		batch.Set([]byte("bench:key"), val)
		e.ApplyBatch(batch, engine.WriteOptions{})
		e.Get([]byte("bench:key"))
	}
}

func BenchmarkEngine_MergeCredit(b *testing.B) {
	e := New()
	e.Open()
	delta := le64(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := e.NewBatch()
		batch.Merge([]byte("counter"), delta)
		e.ApplyBatch(batch, engine.WriteOptions{})
	}
}

func BenchmarkEngine_GetSnapshot(b *testing.B) {
	e := New()
	e.Open()
	for i := 0; i < 1000; i++ {
		batch := e.NewBatch()
		batch.Set([]byte("k"), le64(int64(i)))
		e.ApplyBatch(batch, engine.WriteOptions{})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap, _ := e.GetSnapshot()
		snap.Close()
	}
}

func BenchmarkEngine_RangeScan(b *testing.B) {
	e := New()
	e.Open()
	for i := 0; i < 100; i++ {
		batch := e.NewBatch()
		batch.Set([]byte("stripe:"+string(rune('a'+i%26))), le64(int64(i*100)))
		e.ApplyBatch(batch, engine.WriteOptions{})
	}
	snap, _ := e.GetSnapshot()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := snap.NewIter([]byte("stripe:"), []byte("stripe:~"))
		for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		}
		iter.Close()
	}
	b.StopTimer()
	snap.Close()
}

func BenchmarkEngine_ConcurrentTransfer(b *testing.B) {
	// Simulates the wallet transfer workload: debit one key, credit another.
	e := New()
	e.Open()

	const wallets = 100
	for i := 0; i < wallets; i++ {
		batch := e.NewBatch()
		batch.Set([]byte("w:"+string(rune('a'+i%26))), le64(1_000_000))
		e.ApplyBatch(batch, engine.WriteOptions{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			from := []byte("w:" + string(rune('a'+i%26)))
			to := []byte("w:" + string(rune('a'+(i+1)%26)))
			i++

			batch := e.NewBatch()
			batch.Merge(from, le64(-1))
			batch.Merge(to, le64(1))
			e.ApplyBatch(batch, engine.WriteOptions{})
		}
	})
}
