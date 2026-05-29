package badger

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/agberohq/dpx/engine"
	"github.com/vmihailenco/msgpack/v5"
)

// ---- shared helpers ---------------------------------------------------------

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

// openEngine works for both *testing.T and *testing.B via testing.TB.
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

// ---- Engine lifecycle -------------------------------------------------------

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

// ---- Get / Set / Delete -----------------------------------------------------

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

// ---- Merge ------------------------------------------------------------------

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

func TestEngine_MergeNegativeResult(t *testing.T) {
	e := openEngine(t)
	applyMerge(t, e, "k", 10)
	applyMerge(t, e, "k", -50)
	val, _ := e.Get([]byte("k"))
	if decode64(val) != -40 {
		t.Errorf("10-50 = %d, want -40", decode64(val))
	}
}

// ---- CurrentSequence --------------------------------------------------------

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

// ---- Sync -------------------------------------------------------------------

func TestEngine_SyncDoesNotError(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("v"))
	if err := e.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

// ---- Snapshot ---------------------------------------------------------------

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
		t.Errorf("missing key: got %+v, want zero EpochRecord", er)
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

// ---- Consumer iterator ------------------------------------------------------

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
	iter := snap.NewIter([]byte("a"), []byte("c"))
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
	b := e.NewBatch()
	b.Set([]byte("__dpx:ver:user:1"), epochBytes(1, false))
	b.Set([]byte("__dpx:applied"), le64(1))
	e.ApplyBatch(b, engine.WriteOptions{})
	snap, _ := e.GetSnapshot()
	defer snap.Close()
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
	e := openEngine(t)
	for _, k := range []string{"a", "b", "c"} {
		applySet(t, e, k, []byte(k))
	}
	snap, _ := e.GetSnapshot()
	defer snap.Close()
	iter := snap.NewIter([]byte("a"), []byte("d"))
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
	}
	var got []string
	for iter.Prev(); iter.Valid(); iter.Prev() {
		got = append(got, string(iter.Key()))
	}
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

// ---- RawIter ----------------------------------------------------------------

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

// ---- Checkpoint -------------------------------------------------------------

func TestCheckpoint_WritesFile(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "k", []byte("v"))
	dir := t.TempDir()
	if err := e.CreateCheckpoint(dir); err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "dump"))
	if err != nil {
		t.Fatalf("dump file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Error("dump file is empty")
	}
}

func TestCheckpoint_ContainsAllKeys(t *testing.T) {
	e := openEngine(t)
	applySet(t, e, "key1", []byte("val1"))
	applySet(t, e, "key2", []byte("val2"))
	b := e.NewBatch()
	b.Set([]byte("__dpx:applied"), le64(2))
	e.ApplyBatch(b, engine.WriteOptions{})

	dest := t.TempDir()
	if err := e.CreateCheckpoint(dest); err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dest, "dump"))
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	var data map[string][]byte
	if err := msgpack.Unmarshal(raw, &data); err != nil {
		t.Fatalf("unmarshal dump: %v", err)
	}
	if string(data["key1"]) != "val1" {
		t.Errorf("key1 = %q, want val1", data["key1"])
	}
	if string(data["key2"]) != "val2" {
		t.Errorf("key2 = %q, want val2", data["key2"])
	}
	if len(data["__dpx:applied"]) < 8 {
		t.Error("__dpx:applied missing from checkpoint dump")
	}
}

// ---- Batch Reset ------------------------------------------------------------

func TestBatch_Reset(t *testing.T) {
	b := (&Engine{}).NewBatch().(*badgerBatch)
	b.Set([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	b.Reset()
	if len(b.ops) != 0 {
		t.Errorf("after Reset: %d ops remain, want 0", len(b.ops))
	}
}

func TestBatch_SetCopiesValue(t *testing.T) {
	e := openEngine(t)
	b := e.NewBatch().(*badgerBatch)
	val := []byte("original")
	b.Set([]byte("k"), val)
	val[0] = 'X'
	e.ApplyBatch(b, engine.WriteOptions{})
	got, _ := e.Get([]byte("k"))
	if string(got) != "original" {
		t.Errorf("batch Set did not copy: got %q", got)
	}
}

// ---- Persistence across reopen ----------------------------------------------

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

// ---- isReserved / decodeEpochRecord -----------------------------------------

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
	}
	for _, tc := range cases {
		got := isReserved(tc.key)
		if got != tc.want {
			t.Errorf("isReserved(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestDecodeEpochRecord_NilInput(t *testing.T) {
	er := decodeEpochRecord(nil)
	if er.Epoch != 0 || er.IsCredit {
		t.Errorf("nil input: got %+v, want zero", er)
	}
}

func TestDecodeEpochRecord_RoundTrip(t *testing.T) {
	want := engine.EpochRecord{Epoch: 42, IsCredit: true}
	enc := epochBytes(want.Epoch, want.IsCredit)
	got := decodeEpochRecord(enc)
	if got != want {
		t.Errorf("round-trip: got %+v, want %+v", got, want)
	}
}

// ---- Benchmarks -------------------------------------------------------------
//
// Badger uses read-modify-write for Merge (no native merge operator).
// Run: go test -benchmem -bench=. -run='^$' -count=2 -cpu=8 ./engine/badger/

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

func BenchmarkEngine_Get(b *testing.B) {
	e := openEngine(b)
	applySet(b, e, "bench:key", []byte("value"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get([]byte("bench:key"))
	}
}

func BenchmarkEngine_MergeCredit(b *testing.B) {
	// Each Merge = read current value + add delta + write back (no native merger).
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

func BenchmarkEngine_RangeScan_100Keys(b *testing.B) {
	// Badger NewIter materialises all matching pairs eagerly.
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
	// One transfer = debit Merge + credit Merge = 2 read-modify-writes.
	e := openEngine(b)
	applySet(b, e, "wallet:alice", le64(1_000_000))
	applySet(b, e, "wallet:bob", le64(1_000_000))

	debit := le64(-1)
	credit := le64(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := e.NewBatch()
		batch.Merge([]byte("wallet:alice"), debit)
		batch.Merge([]byte("wallet:bob"), credit)
		e.ApplyBatch(batch, engine.WriteOptions{})
	}
}
