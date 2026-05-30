package pebble

// Tests for the Pebble-backed StorageEngine.
// Run within the package to access unexported helpers:
//   isReserved, decodeEpochRecord, Int64Merger, int64Merger
//
// Each test that opens Pebble uses t.TempDir() and defers engine.Close()
// so the directory is cleaned up even on test failure.
//
// Test organisation:
//   TestEngine_*         — StorageEngine interface methods
//   TestSnapshot_*       — Snapshot interface methods
//   TestIter_*           — Iterator behaviour (raw and consumer)
//   TestBatch_*          — pebbleBatch operations
//   TestMerger_*         — Int64Merger / int64Merger value semantics
//   TestRawIter_*        — RawIter (includes __dpx: keys)
//   TestCheckpoint_*     — CreateCheckpoint
//   TestIsReserved_*     — isReserved helper
//   TestDecodeEpochRecord_* — decodeEpochRecord helper

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/agberohq/dpx/engine"
)

// helpers

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

func epochBytes(epoch uint64, isCredit bool) []byte {
	b := make([]byte, 9)
	binary.LittleEndian.PutUint64(b, epoch)
	if isCredit {
		b[8] = 1
	}
	return b
}

// openEngine opens a fresh Pebble engine in a temp dir and registers cleanup.
func openEngine(tb testing.TB) *Engine {
	tb.Helper()
	dir := tb.TempDir()
	e := New(dir)
	if err := e.Open(); err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() {
		if err := e.Close(); err != nil {
			tb.Logf("Close: %v", err)
		}
	})
	return e
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

// Engine lifecycle

func TestEngine_OpenClose(t *testing.T) {
	dir := t.TempDir()
	e := New(dir)
	if err := e.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEngine_CloseNilDBIsNoop(t *testing.T) {
	e := New(t.TempDir())
	// Close without Open should not panic.
	if err := e.Close(); err != nil {
		t.Errorf("Close on un-opened engine: %v", err)
	}
}

func TestEngine_DataDir(t *testing.T) {
	dir := t.TempDir()
	e := New(dir)
	if e.DataDir() != dir {
		t.Errorf("DataDir() = %q, want %q", e.DataDir(), dir)
	}
}

func TestEngine_OpenCreatesDir(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "sub", "deep")
	e := New(dir)
	if err := e.Open(); err != nil {
		t.Fatalf("Open with nested dir: %v", err)
	}
	defer e.Close()

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("directory not created: %v", err)
	}
}

// Get / Set / Delete

func TestEngine_GetMissingKey(t *testing.T) {
	e := openEngine(t)
	_, err := e.Get([]byte("missing"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("got %v, want ErrKeyNotFound", err)
	}
}

func TestEngine_SetThenGet(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "hello", []byte("world"))

	val, err := e.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("got %q, want world", val)
	}
}

func TestEngine_GetReturnsCopy(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("original"))

	v1, _ := e.Get([]byte("k"))
	v1[0] = 'X'

	v2, _ := e.Get([]byte("k"))
	if string(v2) != "original" {
		t.Errorf("Get leaked internal reference: got %q", v2)
	}
}

func TestEngine_DeleteRemovesKey(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("v"))
	applyDel(t, e, "k")

	_, err := e.Get([]byte("k"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("after delete: got %v, want ErrKeyNotFound", err)
	}
}

func TestEngine_OverwriteValue(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("first"))
	applySet(t, e, "k", []byte("second"))

	val, _ := e.Get([]byte("k"))
	if string(val) != "second" {
		t.Errorf("got %q, want second", val)
	}
}

// CurrentSequence

func TestEngine_CurrentSequenceFreshIsZero(t *testing.T) {
	e := openEngine(t)
	if s := e.CurrentSequence(); s != 0 {
		t.Errorf("fresh sequence = %d, want 0", s)
	}
}

func TestEngine_CurrentSequenceUpdatedByApplied(t *testing.T) {
	e := openEngine(t)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 99)
	b := e.NewBatch()
	b.Set([]byte("__dpx:applied"), buf)
	e.ApplyBatch(b, engine.WriteOptions{})

	if s := e.CurrentSequence(); s != 99 {
		t.Errorf("sequence = %d, want 99", s)
	}
}

func TestEngine_CurrentSequencePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	e1 := New(dir)
	e1.Open()
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 77)
	b := e1.NewBatch()
	b.Set([]byte("__dpx:applied"), buf)
	e1.ApplyBatch(b, engine.WriteOptions{})
	e1.Close()

	e2 := New(dir)
	if err := e2.Open(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	if s := e2.CurrentSequence(); s != 77 {
		t.Errorf("after reopen sequence = %d, want 77", s)
	}
}

// Merge / Int64Merger

func TestEngine_MergeOnNewKey(t *testing.T) {
	e := openEngine(t)
	applyMerge(t, e, "counter", 42)

	val, _ := e.Get([]byte("counter"))
	if decode64(val) != 42 {
		t.Errorf("got %d, want 42", decode64(val))
	}
}

func TestEngine_MergeAccumulates(t *testing.T) {
	e := openEngine(t)
	applyMerge(t, e, "counter", 10)
	applyMerge(t, e, "counter", 5)
	applyMerge(t, e, "counter", -3)

	val, _ := e.Get([]byte("counter"))
	if decode64(val) != 12 {
		t.Errorf("10+5-3 = %d, want 12", decode64(val))
	}
}

func TestEngine_MergeAfterDelete(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", le64(999))
	applyDel(t, e, "k")
	applyMerge(t, e, "k", 55)

	val, _ := e.Get([]byte("k"))
	if decode64(val) != 55 {
		t.Errorf("Delete+Merge(55) = %d, want 55", decode64(val))
	}
}

func TestEngine_MergeAfterSet(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", le64(100))
	applyMerge(t, e, "k", 50)

	val, _ := e.Get([]byte("k"))
	if decode64(val) != 150 {
		t.Errorf("Set(100)+Merge(50) = %d, want 150", decode64(val))
	}
}

// Int64Merger internals

func TestMerger_NilBase(t *testing.T) {
	// Simulates a Merge call on a non-existent key (nil base value).
	m, err := Int64Merger.Merge([]byte("k"), nil)
	if err != nil {
		t.Fatalf("Merge(nil): %v", err)
	}
	// Apply delta of 10
	if err := m.MergeNewer(le64(10)); err != nil {
		t.Fatalf("MergeNewer: %v", err)
	}
	result, _, err := m.Finish(false)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if decode64(result) != 10 {
		t.Errorf("nil base + 10 = %d, want 10", decode64(result))
	}
}

func TestMerger_PositiveDelta(t *testing.T) {
	m, _ := Int64Merger.Merge([]byte("k"), le64(100))
	m.MergeNewer(le64(50))
	result, _, _ := m.Finish(true)
	if decode64(result) != 150 {
		t.Errorf("100 + 50 = %d, want 150", decode64(result))
	}
}

func TestMerger_NegativeDelta(t *testing.T) {
	m, _ := Int64Merger.Merge([]byte("k"), le64(100))
	m.MergeNewer(le64(-30))
	result, _, _ := m.Finish(true)
	if decode64(result) != 70 {
		t.Errorf("100 + (-30) = %d, want 70", decode64(result))
	}
}

func TestMerger_MergeOlderIsCommutative(t *testing.T) {
	// MergeOlder and MergeNewer should produce same result (int64 add commutes).
	m1, _ := Int64Merger.Merge([]byte("k"), le64(0))
	m1.MergeNewer(le64(7))
	m1.MergeOlder(le64(3))
	r1, _, _ := m1.Finish(true)

	m2, _ := Int64Merger.Merge([]byte("k"), le64(0))
	m2.MergeOlder(le64(7))
	m2.MergeNewer(le64(3))
	r2, _, _ := m2.Finish(true)

	if decode64(r1) != decode64(r2) {
		t.Errorf("not commutative: %d != %d", decode64(r1), decode64(r2))
	}
}

func TestMerger_ShortBaseValue(t *testing.T) {
	// Base shorter than 8 bytes is treated as 0.
	m, _ := Int64Merger.Merge([]byte("k"), []byte{1, 2, 3})
	m.MergeNewer(le64(5))
	result, _, _ := m.Finish(false)
	if decode64(result) != 5 {
		t.Errorf("short base + 5 = %d, want 5", decode64(result))
	}
}

// Sync

func TestEngine_SyncDoesNotError(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("v"))
	if err := e.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

// Snapshot

func TestSnapshot_IsolatedFromSubsequentWrites(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("before"))

	snap, err := e.GetSnapshot()
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	defer snap.Close()

	applySet(t, e, "k", []byte("after"))

	val, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap.Get: %v", err)
	}
	if string(val) != "before" {
		t.Errorf("snapshot leaked write: got %q, want before", val)
	}
}

func TestSnapshot_GetMissingKey(t *testing.T) {
	e := openEngine(t)
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	_, err := snap.Get([]byte("missing"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("got %v, want ErrKeyNotFound", err)
	}
}

func TestSnapshot_GetReturnsCopy(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("value"))
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	v1, _ := snap.Get([]byte("k"))
	v1[0] = 'Z'

	v2, _ := snap.Get([]byte("k"))
	if string(v2) != "value" {
		t.Errorf("snap.Get leaked reference: got %q", v2)
	}
}

func TestSnapshot_Sequence(t *testing.T) {
	e := openEngine(t)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 17)
	b := e.NewBatch()
	b.Set([]byte("__dpx:applied"), buf)
	e.ApplyBatch(b, engine.WriteOptions{})

	snap, _ := e.GetSnapshot()
	defer snap.Close()

	if snap.Sequence() != 17 {
		t.Errorf("snap.Sequence() = %d, want 17", snap.Sequence())
	}
}

func TestSnapshot_GetVersion_Missing(t *testing.T) {
	e := openEngine(t)
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
	e := openEngine(t)
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:mykey"), epochBytes(21, true))
	e.ApplyBatch(b, engine.WriteOptions{})

	snap, _ := e.GetSnapshot()
	defer snap.Close()

	er, err := snap.GetVersion([]byte("mykey"))
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if er.Epoch != 21 {
		t.Errorf("epoch = %d, want 21", er.Epoch)
	}
	if !er.IsCredit {
		t.Error("IsCredit should be true")
	}
}

// Consumer iterator (excludes __dpx:)

func TestSnapshot_NewIter_Forward(t *testing.T) {
	e := openEngine(t)
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
	e := openEngine(t)
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
	e := openEngine(t)
	applySet(t, e, "user:1", []byte("v"))

	// Write __dpx: internal keys directly via ApplyBatch
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:user:1"), epochBytes(1, false))
	b.Set([]byte("__dpx:applied"), le64(1))
	e.ApplyBatch(b, engine.WriteOptions{})

	snap, _ := e.GetSnapshot()
	defer snap.Close()

	// Full range scan must not include __dpx: keys
	iter := snap.NewIter([]byte("\x00"), []byte("\xFF"))
	defer iter.Close()

	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		k := string(iter.Key())
		if len(k) >= 6 && k[:6] == "__dpx:" {
			t.Errorf("consumer iter exposed reserved key: %q", k)
		}
	}
}

func TestSnapshot_NewIter_KeyValueCopied(t *testing.T) {
	// Key() and Value() must return copies, not slices into Pebble's buffer.
	e := openEngine(t)
	applySet(t, e, "k", []byte("val"))
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("k"), []byte("l"))
	defer iter.Close()

	iter.First()
	k1 := iter.Key()
	v1 := iter.Value()

	iter.Next() // advance past the end

	// k1 and v1 must still hold their values
	if string(k1) != "k" {
		t.Errorf("Key() not copied: %q", k1)
	}
	if string(v1) != "val" {
		t.Errorf("Value() not copied: %q", v1)
	}
}

func TestSnapshot_NewIter_EmptyRange(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "z", []byte("v"))
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("a"), []byte("b"))
	defer iter.Close()

	if iter.First() {
		t.Errorf("expected empty range, got key %q", iter.Key())
	}
}

func TestSnapshot_NewIter_Reverse(t *testing.T) {
	e := openEngine(t)
	for _, k := range []string{"a", "b", "c"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("a"), []byte("d"))
	defer iter.Close()

	// Exhaust forward, then walk back.
	for iter.First(); iter.Valid(); iter.Next() {
	}

	var got []string
	for iter.Prev(); iter.Valid(); iter.Prev() {
		got = append(got, string(iter.Key()))
	}
	// Pebble's semantics after exhaustion: Prev lands on last valid key.
	// Sequence should be c, b, a.
	want := []string{"c", "b", "a"}
	if len(got) != len(want) {
		t.Fatalf("reverse got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q, want %q", i, got[i], want[i])
		}
	}
}

// RawIter

func TestRawIter_IncludesReservedKeys(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "user:1", []byte("v"))

	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:user:1"), epochBytes(5, true))
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
			if er.Epoch != 5 {
				t.Errorf("epoch = %d, want 5", er.Epoch)
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
	e := openEngine(t)
	b := e.NewBatch()
	// Key with bytes > 0x7E — must not be cut off by the end bound.
	b.Set([]byte("__dpx:ver:\xFF\xFE"), epochBytes(99, false))
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

func TestRawIter_DoesNotIncludeUserKeys(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "userkey", []byte("v"))
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:x"), epochBytes(1, false))
	e.ApplyBatch(b, engine.WriteOptions{})

	start := []byte("__dpx:ver:")
	end := append([]byte("__dpx:ver:"), make([]byte, 16)...)
	for i := range end[10:] {
		end[10+i] = 0xFF
	}

	iter := e.RawIter(start, end)
	defer iter.Close()

	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		if string(iter.Key()) == "userkey" {
			t.Error("RawIter returned user key outside requested range")
		}
	}
}

// Checkpoint

func TestCheckpoint_CreatesDir(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("v"))

	dest := filepath.Join(t.TempDir(), "checkpoint")
	if err := e.CreateCheckpoint(dest); err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	info, err := os.Stat(dest)
	if err != nil {
		t.Fatalf("checkpoint dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("checkpoint path is not a directory")
	}
}

func TestCheckpoint_CanBeOpenedAsDatabase(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "persisted", []byte("yes"))

	dest := filepath.Join(t.TempDir(), "snap")
	if err := e.CreateCheckpoint(dest); err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	// Open the checkpoint as a new engine and read back the key.
	e2 := New(dest)
	if err := e2.Open(); err != nil {
		t.Fatalf("Open checkpoint: %v", err)
	}
	defer e2.Close()

	val, err := e2.Get([]byte("persisted"))
	if err != nil {
		t.Fatalf("Get from checkpoint: %v", err)
	}
	if string(val) != "yes" {
		t.Errorf("got %q, want yes", val)
	}
}

func TestCheckpoint_IncludesVersionKeys(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:k"), epochBytes(3, false))
	b.Set([]byte("__dpx:applied"), le64(3))
	e.ApplyBatch(b, engine.WriteOptions{})

	dest := filepath.Join(t.TempDir(), "snap")
	e.CreateCheckpoint(dest)

	e2 := New(dest)
	e2.Open()
	defer e2.Close()

	// CurrentSequence reads __dpx:applied from restored checkpoint.
	if seq := e2.CurrentSequence(); seq != 3 {
		t.Errorf("checkpoint sequence = %d, want 3", seq)
	}
}

func TestCheckpoint_CompletesConcurrentlyWithWrites(t *testing.T) {
	// CreateCheckpoint calls Flush() then Checkpoint().
	// Flush() is synchronous (waits for memtable to drain to SSTable).
	// Checkpoint() hard-links SSTable files and does not block writes.
	// This test verifies neither call deadlocks when writes race concurrently.
	e := openEngine(t)
	applySet(t, e, "before", []byte("1"))

	dest := t.TempDir() + "/snap"
	checkpointDone := make(chan error, 1)
	go func() {
		checkpointDone <- e.CreateCheckpoint(dest)
	}()

	// Concurrent write must not deadlock or corrupt state.
	applySet(t, e, "concurrent", []byte("2"))

	if err := <-checkpointDone; err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	// The "before" key written before Flush must appear in the checkpoint.
	e2 := New(dest)
	if err := e2.Open(); err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer e2.Close()
	val, err := e2.Get([]byte("before"))
	if err != nil {
		t.Errorf("checkpoint missing pre-flush key: %v", err)
	} else if string(val) != "1" {
		t.Errorf("checkpoint key = %q, want 1", val)
	}
}

// Batch Reset

func TestBatch_Reset(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch().(*pebbleBatch)
	b.Set([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	b.Reset()
	// After Reset the underlying pebble.Batch is cleared.
	// Apply the reset batch — should be a no-op, no error.
	if err := e.ApplyBatch(b, engine.WriteOptions{}); err != nil {
		t.Errorf("ApplyBatch after Reset: %v", err)
	}
	// "a" must not have been written.
	_, err := e.Get([]byte("a"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("key written after Reset: %v", err)
	}
}

// isReserved helper

func TestIsReserved(t *testing.T) {
	cases := []struct {
		key  []byte
		want bool
	}{
		{[]byte("__dpx:ver:foo"), true},
		{[]byte("__dpx:applied"), true},
		{[]byte("__dpx:"), true},
		{[]byte("__dpx"), false},
		{[]byte("user:key"), false},
		{[]byte(""), false},
		{[]byte("_dpx:key"), false},
	}
	for _, tc := range cases {
		got := isReserved(tc.key)
		if got != tc.want {
			t.Errorf("isReserved(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

// decodeEpochRecord helper

func TestDecodeEpochRecord_NilInput(t *testing.T) {
	er := decodeEpochRecord(nil)
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("nil input: got %+v, want zero", er)
	}
}

func TestDecodeEpochRecord_ShortInput(t *testing.T) {
	er := decodeEpochRecord([]byte{0, 1, 2})
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("short input: got %+v, want zero", er)
	}
}

func TestDecodeEpochRecord_RoundTrip(t *testing.T) {
	cases := []engine.EpochRecord{
		{Epoch: 1, IsCredit: true},
		{Epoch: 999999, IsCredit: false},
		{Epoch: 0, IsCredit: false},
	}
	for _, want := range cases {
		enc := epochBytes(want.Epoch, want.IsCredit)
		got := decodeEpochRecord(enc)
		if got != want {
			t.Errorf("round-trip: got %+v, want %+v", got, want)
		}
	}
}

// Persistence across reopen

func TestEngine_DataPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	e1 := New(dir)
	e1.Open()
	applySet(t, e1, "persistent", []byte("value"))
	e1.Close()

	e2 := New(dir)
	if err := e2.Open(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	val, err := e2.Get([]byte("persistent"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("got %q, want value", val)
	}
}

func TestEngine_MergePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	e1 := New(dir)
	e1.Open()
	applyMerge(t, e1, "counter", 10)
	applyMerge(t, e1, "counter", 5)
	e1.Close()

	e2 := New(dir)
	e2.Open()
	defer e2.Close()

	val, _ := e2.Get([]byte("counter"))
	if decode64(val) != 15 {
		t.Errorf("persisted counter = %d, want 15", decode64(val))
	}
}

// Benchmarks
// Run: go test -bench=. -benchtime=5s -count=3 ./engine/pebble/
//
// These benchmarks measure the storage engine in isolation — before any Raft
// overhead. They establish the ceiling performance that the distributed layer
// works within.

func BenchmarkEngine_Set(b *testing.B) {
	e := openEngine(b)
	val := []byte("benchmark-value-32-bytes-padding!!")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := e.NewBatch()
		batch.Set([]byte("bench:key"), val)
		e.ApplyBatch(batch, engine.WriteOptions{})
	}
}

func BenchmarkEngine_SetSync(b *testing.B) {
	e := openEngine(b)
	val := []byte("benchmark-value-32-bytes-padding!!")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := e.NewBatch()
		batch.Set([]byte("bench:key"), val)
		e.ApplyBatch(batch, engine.WriteOptions{Sync: true})
	}
}

func BenchmarkEngine_Get(b *testing.B) {
	e := openEngine(b)
	applySet(b, e, "bench:key", []byte("value"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get([]byte("bench:key"))
	}
}

func BenchmarkEngine_MergeCredit(b *testing.B) {
	e := openEngine(b)
	delta := le64(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := e.NewBatch()
		batch.Merge([]byte("counter"), delta)
		e.ApplyBatch(batch, engine.WriteOptions{})
	}
}

func BenchmarkEngine_GetSnapshot(b *testing.B) {
	e := openEngine(b)
	applySet(b, e, "k", []byte("v"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap, _ := e.GetSnapshot()
		snap.Close()
	}
}

func BenchmarkEngine_SnapshotGet(b *testing.B) {
	e := openEngine(b)
	applySet(b, e, "bench:key", []byte("value"))
	snap, _ := e.GetSnapshot()
	b.Cleanup(func() { snap.Close() })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.Get([]byte("bench:key"))
	}
}

func BenchmarkEngine_RangeScan_100Keys(b *testing.B) {
	e := openEngine(b)
	for i := 0; i < 100; i++ {
		batch := e.NewBatch()
		batch.Set([]byte("stripe:"+string(rune('a'+i%26))), le64(int64(i)))
		e.ApplyBatch(batch, engine.WriteOptions{})
	}
	snap, _ := e.GetSnapshot()
	b.Cleanup(func() { snap.Close() })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := snap.NewIter([]byte("stripe:"), []byte("stripe:~"))
		for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		}
		iter.Close()
	}
}

func BenchmarkEngine_TransferBatch(b *testing.B) {
	// One batch = one debit Merge + one credit Merge (atomic transfer).
	// This is the hot path for every wallet transfer.
	e := openEngine(b)

	from := []byte("wallet:alice")
	to := []byte("wallet:bob")
	applySet(b, e, "wallet:alice", le64(1_000_000))
	applySet(b, e, "wallet:bob", le64(1_000_000))

	debit := le64(-1)
	credit := le64(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := e.NewBatch()
		batch.Merge(from, debit)
		batch.Merge(to, credit)
		e.ApplyBatch(batch, engine.WriteOptions{})
	}
}

func BenchmarkEngine_ConcurrentMerge(b *testing.B) {
	e := openEngine(b)
	delta := le64(1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			batch := e.NewBatch()
			batch.Merge([]byte("counter"), delta)
			e.ApplyBatch(batch, engine.WriteOptions{})
		}
	})
}
