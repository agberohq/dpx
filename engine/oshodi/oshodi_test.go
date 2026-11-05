package oshodi

// Tests for the Oshodi-backed StorageEngine.
// Run within the package to access unexported helpers:
//   decodeInt64, decodeEpochRecord, oshodiBatch, oshodiIter
//
// Each test that opens Oshodi uses t.TempDir() and registers engine.Close()
// via cleanup so the directory is cleaned up even on test failure.
//
// Test organisation:
//   TestEngine_*    — StorageEngine interface methods
//   TestSnapshot_*  — Snapshot interface methods
//   TestIter_*      — Iterator behaviour (raw and consumer)
//   TestBatch_*     — oshodiBatch operations
//   TestCheckpoint_ — CreateCheckpoint
//   TestHelpers_*   — encodeInt64 / decodeInt64 / decodeEpochRecord

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/agberohq/dpx/engine"
)

// shared helpers

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

// openEngine opens a fresh Oshodi engine in a temp dir and registers cleanup.
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

func TestEngine_CloseWithoutOpenIsNoop(t *testing.T) {
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

func TestEngine_DBFileCreatedInDir(t *testing.T) {
	dir := t.TempDir()
	e := New(dir)
	if err := e.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	if _, err := os.Stat(filepath.Join(dir, "oshodi.db")); err != nil {
		t.Errorf("oshodi.db not created in data dir: %v", err)
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

func TestEngine_DeleteMissingKeyDoesNotError(t *testing.T) {
	e := openEngine(t)
	// Deleting a key that never existed is a no-op for Oshodi.
	b := e.NewBatch()
	b.Delete([]byte("never-existed"))
	if err := e.ApplyBatch(b, engine.WriteOptions{}); err != nil {
		t.Errorf("ApplyBatch Delete missing key: %v", err)
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

func TestEngine_ShortAppliedValueIsZero(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch()
	b.Set([]byte("__dpx:applied"), []byte{1, 2, 3}) // shorter than 8 bytes
	e.ApplyBatch(b, engine.WriteOptions{})

	if s := e.CurrentSequence(); s != 0 {
		t.Errorf("short applied value: sequence = %d, want 0", s)
	}
}

// Merge (read-modify-write semantics)

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

func TestEngine_MergeOnShortExistingValue(t *testing.T) {
	// Values shorter than 8 bytes are treated as a zero base.
	// DPX only merges int64-encoded epoch values into keys holding
	// either le64 payloads or nothing.
	e := openEngine(t)
	applySet(t, e, "k", []byte("xy"))
	applyMerge(t, e, "k", 7)

	val, _ := e.Get([]byte("k"))
	if decode64(val) != 7 {
		t.Errorf("Merge after Set(short) = %d, want 7", decode64(val))
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

func TestEngine_WriteOptionsSyncFlushes(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	if err := e.ApplyBatch(b, engine.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("ApplyBatch Sync: %v", err)
	}
}

func TestEngine_SetTelemetryIsSafe(t *testing.T) {
	e := openEngine(t)
	e.SetTelemetry(nil) // must not panic or break subsequent use
	applySet(t, e, "k", []byte("v"))
	if _, err := e.Get([]byte("k")); err != nil {
		t.Errorf("Get after SetTelemetry(nil): %v", err)
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

func TestSnapshot_IsolatedFromDeletes(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("v"))

	snap, _ := e.GetSnapshot()
	defer snap.Close()

	applyDel(t, e, "k")

	// Snapshot still sees the deleted key.
	val, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap.Get after delete: %v", err)
	}
	if string(val) != "v" {
		t.Errorf("snapshot lost key on delete: got %v", err)
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

func TestSnapshot_SequenceFrozenAtCreation(t *testing.T) {
	e := openEngine(t)
	// Snapshot before any applied writes.
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	// Advance the sequence on the engine afterwards.
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 42)
	b := e.NewBatch()
	b.Set([]byte("__dpx:applied"), buf)
	e.ApplyBatch(b, engine.WriteOptions{})

	// Snapshots materialize state, so the captured sequence must not advance.
	if snap.Sequence() != 0 {
		t.Errorf("snap.Sequence() = %d, want 0 (frozen at creation)", snap.Sequence())
	}
}

func TestSnapshot_CloseIsNoop(t *testing.T) {
	e := openEngine(t)
	snap, _ := e.GetSnapshot()
	if err := snap.Close(); err != nil {
		t.Errorf("snap.Close: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Errorf("double snap.Close: %v", err)
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

func TestSnapshot_GetVersion_ExcludesNonVersionKeys(t *testing.T) {
	// A user key under __dpx: that is NOT the ver: prefix must not be
	// misread as a version record.
	e := openEngine(t)
	b := e.NewBatch()
	b.Set([]byte("__dpx:other:mykey"), epochBytes(5, false))
	e.ApplyBatch(b, engine.WriteOptions{})

	snap, _ := e.GetSnapshot()
	defer snap.Close()

	er, err := snap.GetVersion([]byte("mykey"))
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("got %+v, want zero EpochRecord", er)
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

func TestSnapshot_NewIter_InclusiveStart(t *testing.T) {
	e := openEngine(t)
	for _, k := range []string{"a", "b"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("a"), []byte("z"))
	defer iter.Close()

	var got []string
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		got = append(got, string(iter.Key()))
	}
	want := []string{"a", "b"}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("got %v, want %v", got, want)
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
	iter := snap.NewIter([]byte(""), []byte(""))
	defer iter.Close()

	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		k := string(iter.Key())
		if len(k) >= 6 && k[:6] == "__dpx:" {
			t.Errorf("consumer iter exposed reserved key: %q", k)
		}
	}
}

func TestSnapshot_NewIter_SortedAcrossShardsLikeMap(t *testing.T) {
	// Oshodi snapshots materialize to a map, so iteration order must be
	// re-sorted. Verify a larger key set comes back fully ordered.
	e := openEngine(t)
	for _, k := range []string{"k10", "k01", "k2", "k1", "k3"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	var got []string
	iter := snap.NewIter([]byte("k"), []byte("k~"))
	defer iter.Close()
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		got = append(got, string(iter.Key()))
	}

	want := []string{"k01", "k1", "k10", "k2", "k3"} // lexicographic
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatalf("position %d: got %v, want %v", i, got, want)
		}
	}
}

func TestSnapshot_NewIter_KeyValueCopied(t *testing.T) {
	// Key() and Value() must survive further iteration (materialized pairs).
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

func TestSnapshot_NewIter_EmptyKeyRangeSweep(t *testing.T) {
	// Empty start/end bounds sweep the whole materialized state.
	e := openEngine(t)
	for _, k := range []string{"c", "a", "b"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	var got []string
	iter := snap.NewIter(nil, nil)
	defer iter.Close()
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		got = append(got, string(iter.Key()))
	}
	if len(got) != 3 {
		t.Errorf("got %v, want 3 unordered keys sorted", got)
	}
}

func TestIter_Navigation(t *testing.T) {
	e := openEngine(t)
	for _, k := range []string{"a", "b", "c"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()

	iter := snap.NewIter([]byte("a"), []byte("d"))
	defer iter.Close()

	// Exhaust forward.
	for iter.First(); iter.Valid(); iter.Next() {
	}

	// Prev from past-the-end lands on the last key.
	if !iter.Prev() || string(iter.Key()) != "c" {
		t.Fatalf("Prev from exhaustion = %v/%q, want true/c", iter.Valid(), iter.Key())
	}
	if !iter.Prev() || string(iter.Key()) != "b" {
		t.Fatalf("second Prev got %q, want b", iter.Key())
	}
	if !iter.Prev() || string(iter.Key()) != "a" {
		t.Fatalf("third Prev got %q, want a", iter.Key())
	}

	// Prev before the start invalidates.
	if iter.Prev() {
		t.Error("Prev before first key should return false")
	}
	if iter.Valid() {
		t.Error("iter should be invalid before start")
	}
	if iter.Key() != nil || iter.Value() != nil {
		t.Errorf("Key/Value on invalid iter: %q/%q, want nil/nil", iter.Key(), iter.Value())
	}

	// Forward again from invalid state repositions to the first key.
	if !iter.First() || string(iter.Key()) != "a" {
		t.Fatalf("First after invalidation got %q, want a", iter.Key())
	}

	// Over-advancing past end invalidates.
	iter.Next() // b
	iter.Next() // c
	if iter.Next() {
		t.Error("Next past last key should return false")
	}
	if iter.Valid() {
		t.Error("iter should be invalid past end")
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
	// After exhaustion, Prev lands on last valid key: sequence c, b, a.
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

func TestEngine_RawIter_IncludesReservedKeys(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "user:1", []byte("v"))

	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:user:1"), epochBytes(5, true))
	e.ApplyBatch(b, engine.WriteOptions{})

	open := []byte("__dpx:ver:")
	end := append(open[:len(open):len(open)], make([]byte, 16)...)
	for i := range end[len(open):] {
		end[len(open)+i] = 0xFF
	}

	iter := e.RawIter(open, end)
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

func TestEngine_RawIter_IncludesAppliedKey(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch()
	b.Set([]byte("__dpx:applied"), le64(9))
	e.ApplyBatch(b, engine.WriteOptions{})

	iter := e.RawIter([]byte("__dpx:applied"), []byte("__dpx:applied\xFF"))
	defer iter.Close()

	if !iter.First() {
		t.Fatalf("RawIter empty; want __dpx:applied")
	}
	if string(iter.Key()) != "__dpx:applied" {
		t.Errorf("got key %q, want __dpx:applied", iter.Key())
	}
	if decode64(iter.Value()) != 9 {
		t.Errorf("applied value = %d, want 9", decode64(iter.Value()))
	}
}

func TestEngine_RawIter_ExcludesUserKeys(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "userkey", []byte("v"))
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:x"), epochBytes(1, false))
	e.ApplyBatch(b, engine.WriteOptions{})

	open := []byte("__dpx:ver:")
	end := append(open[:len(open):len(open)], make([]byte, 16)...)
	for i := range end[len(open):] {
		end[len(open)+i] = 0xFF
	}

	iter := e.RawIter(open, end)
	defer iter.Close()

	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		if string(iter.Key()) == "userkey" {
			t.Error("RawIter returned user key outside requested range")
		}
	}
}

func TestEngine_RawIter_EmptyRange(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("v"))

	iter := e.RawIter([]byte("k"), []byte("k")) // empty range
	defer iter.Close()

	if iter.First() {
		t.Errorf("empty range swept key %q", iter.Key())
	}
	if err := iter.Error(); err != nil {
		t.Errorf("iter error: %v", err)
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

func TestCheckpoint_IncludesVersionAndAppliedKeys(t *testing.T) {
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

	// Version records survive; snapshot restore reads them.
	snap, _ := e2.GetSnapshot()
	defer snap.Close()
	er, err := snap.GetVersion([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if er.Epoch != 3 || er.IsCredit {
		t.Errorf("checkpoint EpochRecord = %+v, want {3 false}", er)
	}
}

func TestCheckpoint_ExcludesUnflushedNothing_RoundTripsMerges(t *testing.T) {
	// Merges are applied eagerly via read-modify-write, so checkpoints
	// must contain their summed values.
	e := openEngine(t)
	applyMerge(t, e, "counter", 10)
	applyMerge(t, e, "counter", 5)

	dest := filepath.Join(t.TempDir(), "snap")
	if err := e.CreateCheckpoint(dest); err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	e2 := New(dest)
	e2.Open()
	defer e2.Close()

	val, err := e2.Get([]byte("counter"))
	if err != nil {
		t.Fatalf("Get from checkpoint: %v", err)
	}
	if decode64(val) != 15 {
		t.Errorf("checkpointed counter = %d, want 15", decode64(val))
	}
}

// Batch Reset

func TestBatch_Reset(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch().(*oshodiBatch)
	b.Set([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	b.Reset()
	// Reset should clear all pending ops.
	if len(b.ops) != 0 {
		t.Fatalf("Reset left %d ops, want 0", len(b.ops))
	}
	// Apply the reset batch — should be a no-op, no error.
	if err := e.ApplyBatch(b, engine.WriteOptions{}); err != nil {
		t.Errorf("ApplyBatch after Reset: %v", err)
	}
	_, err := e.Get([]byte("a"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("key written after Reset: %v", err)
	}
}

func TestBatch_CopiesInputs(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch().(*oshodiBatch)

	key := []byte("k")
	val := []byte("original")
	b.Set(key, val)
	key[0] = 'X'
	val[0] = 'X'

	batch := e.NewBatch().(*oshodiBatch)
	batch.ops = b.ops

	if err := e.ApplyBatch(batch, engine.WriteOptions{}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	got, _ := e.Get([]byte("k"))
	if string(got) != "original" {
		t.Errorf("batch mutated caller buffers: got %q", got)
	}
}

func TestBatch_MixedOpsAppliedInOrder(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch().(*oshodiBatch)
	b.Set([]byte("k"), le64(100))
	b.Merge([]byte("k"), le64(5))
	b.Delete([]byte("k"))
	if err := e.ApplyBatch(b, engine.WriteOptions{}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	// Final op was Delete, so the key must not exist.
	_, err := e.Get([]byte("k"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("got %v, want ErrKeyNotFound", err)
	}
}

// Helper internals

func TestHelpers_EncodeDecodeInt64(t *testing.T) {
	cases := []int64{0, 1, -1, 42, -42, 1 << 62, -(1 << 62)}
	for _, want := range cases {
		got := decodeInt64(encodeInt64(want))
		if got != want {
			t.Errorf("round-trip %d: got %d", want, got)
		}
	}
}

func TestHelpers_DecodeInt64_ShortInput(t *testing.T) {
	if got := decodeInt64([]byte{1, 2, 3}); got != 0 {
		t.Errorf("short input: got %d, want 0", got)
	}
	if got := decodeInt64(nil); got != 0 {
		t.Errorf("nil input: got %d, want 0", got)
	}
}

func TestDecodeEpochRecord_ShortInput(t *testing.T) {
	er := decodeEpochRecord([]byte{0, 1, 2})
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("short input: got %+v, want zero", er)
	}
}

func TestDecodeEpochRecord_NilInput(t *testing.T) {
	er := decodeEpochRecord(nil)
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("nil input: got %+v, want zero", er)
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

func TestEngine_DataPersistsAcrossSyncAndReopen(t *testing.T) {
	dir := t.TempDir()

	e1 := New(dir)
	e1.Open()
	applySet(t, e1, "durable", []byte("data"))
	if err := e1.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	e1.Close()

	e2 := New(dir)
	e2.Open()
	defer e2.Close()

	val, err := e2.Get([]byte("durable"))
	if err != nil {
		t.Fatalf("Get after sync+reopen: %v", err)
	}
	if string(val) != "data" {
		t.Errorf("got %q, want data", val)
	}
}
