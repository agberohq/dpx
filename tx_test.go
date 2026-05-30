package dpx

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
)

type fakeSnapshot struct {
	data     map[string][]byte
	versions map[string]engine.EpochRecord
	seq      uint64
}

func newFakeSnapshot(seq uint64) *fakeSnapshot {
	return &fakeSnapshot{
		data:     make(map[string][]byte),
		versions: make(map[string]engine.EpochRecord),
		seq:      seq,
	}
}

func (s *fakeSnapshot) set(key string, val []byte, epoch uint64, isCredit bool) {
	s.data[key] = val
	s.versions[key] = engine.EpochRecord{Epoch: epoch, IsCredit: isCredit}
}

func (s *fakeSnapshot) Get(key []byte) ([]byte, error) {
	v, ok := s.data[string(key)]
	if !ok {
		return nil, engine.ErrKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (s *fakeSnapshot) GetVersion(key []byte) (engine.EpochRecord, error) {
	return s.versions[string(key)], nil
}

func (s *fakeSnapshot) NewIter(start, end []byte) engine.Iterator {
	st, en := string(start), string(end)
	var pairs [][2][]byte
	var keys []string
	for k := range s.data {
		if k >= st && (en == "" || k < en) {
			keys = append(keys, k)
		}
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, k := range keys {
		v := s.data[k]
		cp := make([]byte, len(v))
		copy(cp, v)
		pairs = append(pairs, [2][]byte{[]byte(k), cp})
	}
	return &fakeIter{pairs: pairs, idx: -1}
}

func (s *fakeSnapshot) Sequence() uint64 { return s.seq }
func (s *fakeSnapshot) Close() error     { return nil }

type fakeIter struct {
	pairs [][2][]byte
	idx   int
}

func (i *fakeIter) First() bool  { i.idx = 0; return i.idx < len(i.pairs) }
func (i *fakeIter) Next() bool   { i.idx++; return i.idx < len(i.pairs) }
func (i *fakeIter) Prev() bool   { i.idx--; return i.idx >= 0 }
func (i *fakeIter) Valid() bool  { return i.idx >= 0 && i.idx < len(i.pairs) }
func (i *fakeIter) Error() error { return nil }
func (i *fakeIter) Close() error { return nil }
func (i *fakeIter) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.pairs[i.idx][0]
}
func (i *fakeIter) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.pairs[i.idx][1]
}

func newTx(snap *fakeSnapshot) *dpxTx {
	return &dpxTx{
		snap:    snap,
		readSet: make(map[string]shared.ReadEntry),
		writes:  make([]shared.WriteEntry, 0, 4),
	}
}

func le64tx(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func decode64tx(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func TestTx_Get_PopulatesReadSet(t *testing.T) {
	snap := newFakeSnapshot(10)
	snap.set("alice", le64tx(100), 5, false)

	tx := newTx(snap)
	ctx := context.Background()

	val, err := tx.Get(ctx, []byte("alice"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if decode64tx(val) != 100 {
		t.Errorf("value = %d, want 100", decode64tx(val))
	}

	re, ok := tx.readSet["alice"]
	if !ok {
		t.Fatal("key not in readSet after Get")
	}
	if re.Epoch != 5 {
		t.Errorf("readSet epoch = %d, want 5", re.Epoch)
	}
	if re.IsDebit {
		t.Error("Get should not set IsDebit=true")
	}
}

func TestTx_Get_MissingKeyNotInReadSet(t *testing.T) {
	snap := newFakeSnapshot(1)
	tx := newTx(snap)
	ctx := context.Background()

	_, err := tx.Get(ctx, []byte("missing"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("got %v, want ErrKeyNotFound", err)
	}
	if _, ok := tx.readSet["missing"]; ok {
		t.Error("missing key should not be in readSet")
	}
}

func TestTx_Get_ReservedKeyRejected(t *testing.T) {
	snap := newFakeSnapshot(1)
	tx := newTx(snap)
	ctx := context.Background()

	_, err := tx.Get(ctx, []byte("__dpx:anything"))
	if err != ErrReservedKey {
		t.Errorf("got %v, want ErrReservedKey", err)
	}
}

func TestTx_Set_AppendsWrite(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	ctx := context.Background()

	if err := tx.Set(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if len(tx.writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(tx.writes))
	}
	if tx.writes[0].Op != shared.OpSet {
		t.Errorf("op = %v, want OpSet", tx.writes[0].Op)
	}
	if string(tx.writes[0].Value) != "v" {
		t.Errorf("value = %q, want v", tx.writes[0].Value)
	}
}

func TestTx_Set_CopiesValue(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	ctx := context.Background()

	val := []byte("original")
	_ = tx.Set(ctx, []byte("k"), val)
	val[0] = 'X'

	if string(tx.writes[0].Value) != "original" {
		t.Error("Set did not copy value")
	}
}

func TestTx_Set_DoesNotPopulateReadSet(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	_ = tx.Set(context.Background(), []byte("k"), []byte("v"))
	if _, ok := tx.readSet["k"]; ok {
		t.Error("Set should not add key to readSet")
	}
}

func TestTx_Set_ReservedKeyRejected(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	err := tx.Set(context.Background(), []byte("__dpx:ver:x"), []byte("v"))
	if err != ErrReservedKey {
		t.Errorf("got %v, want ErrReservedKey", err)
	}
}

func TestTx_Delete_AppendsWrite(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	if err := tx.Delete(context.Background(), []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(tx.writes) != 1 || tx.writes[0].Op != shared.OpDelete {
		t.Errorf("expected OpDelete write, got %+v", tx.writes)
	}
}

func TestTx_Delete_DoesNotPopulateReadSet(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	_ = tx.Delete(context.Background(), []byte("k"))
	if _, ok := tx.readSet["k"]; ok {
		t.Error("Delete should not add key to readSet")
	}
}

func TestTx_AtomicAdd_Credit_NoReadSet(t *testing.T) {
	snap := newFakeSnapshot(1)
	snap.set("bal", le64tx(50), 1, false)
	tx := newTx(snap)
	ctx := context.Background()

	ret, err := tx.AtomicAdd(ctx, []byte("bal"), 10)
	if err != nil {
		t.Fatalf("AtomicAdd credit: %v", err)
	}
	if ret != 50 {
		t.Errorf("credit return = %d, want 50 (snapshot-time)", ret)
	}
	if _, ok := tx.readSet["bal"]; ok {
		t.Error("credit should not add key to readSet")
	}
	if len(tx.writes) != 1 || tx.writes[0].Op != shared.OpCredit {
		t.Errorf("expected OpCredit, got %+v", tx.writes)
	}
}

func TestTx_AtomicAdd_Debit_InReadSet(t *testing.T) {
	snap := newFakeSnapshot(5)
	snap.set("bal", le64tx(100), 3, false)
	tx := newTx(snap)
	ctx := context.Background()

	ret, err := tx.AtomicAdd(ctx, []byte("bal"), -30)
	if err != nil {
		t.Fatalf("AtomicAdd debit: %v", err)
	}
	if ret != 70 {
		t.Errorf("debit return = %d, want 70", ret)
	}
	re, ok := tx.readSet["bal"]
	if !ok {
		t.Fatal("debit must add key to readSet")
	}
	if !re.IsDebit {
		t.Error("readSet entry should have IsDebit=true for debit")
	}
	if re.Epoch != 3 {
		t.Errorf("epoch = %d, want 3", re.Epoch)
	}
	if len(tx.writes) != 1 || tx.writes[0].Op != shared.OpDebit {
		t.Errorf("expected OpDebit write, got %+v", tx.writes)
	}
}

func TestTx_AtomicAdd_Probe_InReadSet_NoWrite(t *testing.T) {
	snap := newFakeSnapshot(1)
	snap.set("bal", le64tx(200), 2, false)
	tx := newTx(snap)
	ctx := context.Background()

	ret, err := tx.AtomicAdd(ctx, []byte("bal"), 0)
	if err != nil {
		t.Fatalf("AtomicAdd probe: %v", err)
	}
	if ret != 200 {
		t.Errorf("probe return = %d, want 200", ret)
	}
	if _, ok := tx.readSet["bal"]; !ok {
		t.Error("probe must add key to readSet")
	}
	if len(tx.writes) != 0 {
		t.Errorf("probe should not buffer a write, got %+v", tx.writes)
	}
}

func TestTx_AtomicAdd_CreditOnMissingKey_ReturnsZero(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	ctx := context.Background()

	ret, err := tx.AtomicAdd(ctx, []byte("new"), 50)
	if err != nil {
		t.Fatalf("AtomicAdd credit missing: %v", err)
	}
	if ret != 0 {
		t.Errorf("credit on missing key return = %d, want 0", ret)
	}
}

func TestTx_AtomicAdd_DebitOnMissingKey_ReturnsNegativeDelta(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	ctx := context.Background()

	ret, err := tx.AtomicAdd(ctx, []byte("new"), -10)
	if err != nil {
		t.Fatalf("AtomicAdd debit missing: %v", err)
	}
	if ret != -10 {
		t.Errorf("debit on missing key return = %d, want -10", ret)
	}
}

func TestTx_AtomicAdd_ReservedKeyRejected(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	_, err := tx.AtomicAdd(context.Background(), []byte("__dpx:ver:x"), 1)
	if err != ErrReservedKey {
		t.Errorf("got %v, want ErrReservedKey", err)
	}
}

func TestTx_GetRange_AdvisoryNotInReadSet(t *testing.T) {
	snap := newFakeSnapshot(1)
	snap.set("s:a", le64tx(10), 1, false)
	snap.set("s:b", le64tx(20), 2, false)
	tx := newTx(snap)
	ctx := context.Background()

	pairs, err := tx.GetRange(ctx, []byte("s:"), []byte("s:~"), 0)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if len(pairs) != 2 {
		t.Errorf("got %d pairs, want 2", len(pairs))
	}
	if len(tx.readSet) != 0 {
		t.Errorf("GetRange must not populate readSet, got %v", tx.readSet)
	}
}

func TestTx_GetRange_LimitApplied(t *testing.T) {
	snap := newFakeSnapshot(1)
	for _, k := range []string{"s:a", "s:b", "s:c", "s:d"} {
		snap.set(k, []byte("v"), 1, false)
	}
	tx := newTx(snap)

	pairs, _ := tx.GetRange(context.Background(), []byte("s:"), []byte("s:~"), 2)
	if len(pairs) != 2 {
		t.Errorf("limit not applied: got %d pairs, want 2", len(pairs))
	}
}

func TestTx_Empty_NoWrites(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	if !tx.empty() {
		t.Error("fresh tx should be empty")
	}
}

func TestTx_Empty_AfterProbeOnlyIsFalse(t *testing.T) {
	snap := newFakeSnapshot(1)
	snap.set("k", le64tx(5), 1, false)
	tx := newTx(snap)
	_, _ = tx.AtomicAdd(context.Background(), []byte("k"), 0)
	if !tx.empty() {
		t.Error("probe-only tx should still be empty (no writes)")
	}
}

func TestTx_Empty_AfterSetIsFalse(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	_ = tx.Set(context.Background(), []byte("k"), []byte("v"))
	if tx.empty() {
		t.Error("tx with Set should not be empty")
	}
}

func TestTx_Validate_CreditAndDelete_SameKey(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	ctx := context.Background()
	_, _ = tx.AtomicAdd(ctx, []byte("k"), 10)
	_ = tx.Delete(ctx, []byte("k"))

	if err := tx.validate(); err != ErrInvalidProposal {
		t.Errorf("got %v, want ErrInvalidProposal", err)
	}
}

func TestTx_Validate_CreditAndDelete_DifferentKeys(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	ctx := context.Background()
	_, _ = tx.AtomicAdd(ctx, []byte("a"), 10)
	_ = tx.Delete(ctx, []byte("b"))

	if err := tx.validate(); err != nil {
		t.Errorf("different keys: got %v, want nil", err)
	}
}

func TestTx_Validate_EmptyIsNil(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	if err := tx.validate(); err != nil {
		t.Errorf("empty tx validate: %v", err)
	}
}

func TestTx_ReadSetSlice_Empty(t *testing.T) {
	tx := newTx(newFakeSnapshot(1))
	if rs := tx.readSetSlice(); rs != nil {
		t.Errorf("empty readSet should return nil slice, got %v", rs)
	}
}

func TestTx_ReadSetSlice_ContainsAllEntries(t *testing.T) {
	snap := newFakeSnapshot(1)
	snap.set("a", le64tx(1), 1, false)
	snap.set("b", le64tx(2), 2, false)
	tx := newTx(snap)
	ctx := context.Background()

	_, _ = tx.Get(ctx, []byte("a"))
	_, _ = tx.AtomicAdd(ctx, []byte("b"), -5)

	rs := tx.readSetSlice()
	if len(rs) != 2 {
		t.Fatalf("readSetSlice len = %d, want 2", len(rs))
	}
}

func TestTx_AllocateNextSequence_IsSnapPlusOne(t *testing.T) {
	snap := newFakeSnapshot(42)
	tx := newTx(snap)

	seq, err := tx.AllocateNextSequence(context.Background())
	if err != nil {
		t.Fatalf("AllocateNextSequence: %v", err)
	}
	if seq != 43 {
		t.Errorf("seq = %d, want 43 (snap.Sequence()+1)", seq)
	}
}
