package conductor

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/shared"
	hraft "github.com/hashicorp/raft"
	"github.com/olekukonko/hlc"
	"github.com/vmihailenco/msgpack/v5"
)

func setupFSM(t *testing.T) (*dpxFSM, *memory.Engine) {
	t.Helper()
	eng := memory.New()
	if err := eng.Open(); err != nil {
		t.Fatalf("engine Open: %v", err)
	}
	f := newFSM(eng, shared.SyncBatch, nil, nil, hlc.NewClock())
	return f, eng
}

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

func proposalBytes(t *testing.T, p *shared.Proposal) []byte {
	t.Helper()
	data, err := msgpack.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func TestFSM_Apply_SetAndGet(t *testing.T) {
	f, eng := setupFSM(t)

	p := &shared.Proposal{
		Writes: []shared.WriteEntry{
			{Op: shared.OpSet, Key: []byte("k"), Value: []byte("v")},
		},
	}
	log := &hraft.Log{Index: 1, Type: hraft.LogCommand, Data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (shared.ApplyResult{}) {
		t.Errorf("Set Apply: got %+v, want zero ApplyResult", result)
	}

	val, err := eng.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after Apply: %v", err)
	}
	if string(val) != "v" {
		t.Errorf("Get = %q, want %q", val, "v")
	}

	ver, err := eng.Get([]byte("__dpx:ver:k"))
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if decode64(ver) != 1 {
		t.Errorf("epoch = %d, want 1", decode64(ver))
	}
}

func TestFSM_Apply_Delete(t *testing.T) {
	f, eng := setupFSM(t)

	b := eng.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	eng.ApplyBatch(b, engine.WriteOptions{})

	p := &shared.Proposal{
		ReadSet: []shared.ReadEntry{
			{Key: []byte("k"), Epoch: 1},
		},
		Writes: []shared.WriteEntry{
			{Op: shared.OpDelete, Key: []byte("k")},
		},
	}
	log := &hraft.Log{Index: 2, Type: hraft.LogCommand, Data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (shared.ApplyResult{}) {
		t.Errorf("Delete Apply: got %+v, want zero ApplyResult", result)
	}

	_, err := eng.Get([]byte("k"))
	if err == nil {
		t.Error("key should be deleted")
	}
}

func TestFSM_Apply_ConflictOnEpochMismatch(t *testing.T) {
	f, eng := setupFSM(t)

	// Write both the data key and its version record so that f.open()
	// rebuilds keyEpoch["k"].Epoch = 1, enabling conflict detection.
	var epochBuf [9]byte
	binary.LittleEndian.PutUint64(epochBuf[:8], 1) // epoch=1, isCredit=0
	b := eng.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	b.Set([]byte("__dpx:ver:k"), epochBuf[:])
	eng.ApplyBatch(b, engine.WriteOptions{})

	// Rebuild keyEpoch from engine so the FSM knows k was written at epoch 1.
	if _, err := f.open(nil); err != nil {
		t.Fatalf("open: %v", err)
	}

	p := &shared.Proposal{
		ReadSet: []shared.ReadEntry{
			{Key: []byte("k"), Epoch: 0},
		},
		Writes: []shared.WriteEntry{
			{Op: shared.OpSet, Key: []byte("k"), Value: []byte("new")},
		},
	}
	log := &hraft.Log{Index: 2, Type: hraft.LogCommand, Data: proposalBytes(t, p)}

	result := f.Apply(log)
	ar, ok := result.(shared.ApplyResult)
	if !ok || !ar.Conflict {
		t.Error("expected Conflict=true on epoch mismatch")
	}

	val, _ := eng.Get([]byte("k"))
	if string(val) != "v" {
		t.Errorf("value changed despite conflict: %q", val)
	}
}

func TestFSM_Apply_CreditNoConflict(t *testing.T) {
	f, eng := setupFSM(t)

	p := &shared.Proposal{
		Writes: []shared.WriteEntry{
			{Op: shared.OpCredit, Key: []byte("bal"), Value: le64(50)},
		},
	}
	log := &hraft.Log{Index: 1, Type: hraft.LogCommand, Data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (shared.ApplyResult{}) {
		t.Errorf("Credit Apply: got %+v, want zero ApplyResult", result)
	}

	val, err := eng.Get([]byte("bal"))
	if err != nil {
		t.Fatalf("Get after credit: %v", err)
	}
	if decode64(val) != 50 {
		t.Errorf("balance = %d, want 50", decode64(val))
	}
}

func TestFSM_Apply_ReadOnlyProposalNoOp(t *testing.T) {
	f, eng := setupFSM(t)

	p := &shared.Proposal{
		ReadSet: []shared.ReadEntry{
			{Key: []byte("k"), Epoch: 0},
		},
	}
	log := &hraft.Log{Index: 1, Type: hraft.LogCommand, Data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (shared.ApplyResult{}) {
		t.Errorf("read-only Apply: got %+v, want zero ApplyResult", result)
	}

	_, err := eng.Get([]byte("k"))
	if err == nil {
		t.Error("read-only proposal should not write to engine")
	}
}

func TestFSM_Apply_AppliedIndexUpdated(t *testing.T) {
	f, eng := setupFSM(t)

	p := &shared.Proposal{
		Writes: []shared.WriteEntry{
			{Op: shared.OpSet, Key: []byte("k"), Value: []byte("v")},
		},
	}
	log := &hraft.Log{Index: 42, Type: hraft.LogCommand, Data: proposalBytes(t, p)}

	f.Apply(log)

	appliedBytes, err := eng.Get([]byte("__dpx:applied"))
	if err != nil {
		t.Fatalf("applied key not found: %v", err)
	}
	applied := binary.LittleEndian.Uint64(appliedBytes)
	if applied != 42 {
		t.Errorf("applied index = %d, want 42", applied)
	}
}

func TestFSM_Apply_MultipleWrites(t *testing.T) {
	f, eng := setupFSM(t)

	p := &shared.Proposal{
		Writes: []shared.WriteEntry{
			{Op: shared.OpSet, Key: []byte("a"), Value: []byte("1")},
			{Op: shared.OpSet, Key: []byte("b"), Value: []byte("2")},
			{Op: shared.OpCredit, Key: []byte("c"), Value: le64(100)},
		},
	}
	log := &hraft.Log{Index: 1, Type: hraft.LogCommand, Data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (shared.ApplyResult{}) {
		t.Errorf("multi-write Apply: got %+v, want zero ApplyResult", result)
	}

	va, _ := eng.Get([]byte("a"))
	vb, _ := eng.Get([]byte("b"))
	vc, _ := eng.Get([]byte("c"))

	if string(va) != "1" || string(vb) != "2" || decode64(vc) != 100 {
		t.Errorf("multi-write: a=%q b=%q c=%d", va, vb, decode64(vc))
	}
}

func TestFSM_Apply_DebitOnMissingKeyIsOK(t *testing.T) {
	f, eng := setupFSM(t)

	p := &shared.Proposal{
		ReadSet: []shared.ReadEntry{
			{Key: []byte("new"), Epoch: 0, IsDebit: true},
		},
		Writes: []shared.WriteEntry{
			{Op: shared.OpDebit, Key: []byte("new"), Value: le64(-50)},
		},
	}
	log := &hraft.Log{Index: 1, Type: hraft.LogCommand, Data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (shared.ApplyResult{}) {
		t.Errorf("debit on missing key: got %+v, want zero ApplyResult", result)
	}

	val, _ := eng.Get([]byte("new"))
	if decode64(val) != -50 {
		t.Errorf("balance = %d, want -50", decode64(val))
	}
}

func TestFSM_Snapshot_Restore(t *testing.T) {
	f, eng := setupFSM(t)

	b := eng.NewBatch()
	b.Set([]byte("a"), []byte("1"))
	b.Set([]byte("b"), []byte("2"))
	eng.ApplyBatch(b, engine.WriteOptions{})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	newEng := memory.New()
	newEng.Open()
	defer newEng.Close()
	newFSM := newFSM(newEng, shared.SyncBatch, nil, nil, hlc.NewClock())

	sink := &mockSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if err := newFSM.Restore(sink); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	va, _ := newEng.Get([]byte("a"))
	vb, _ := newEng.Get([]byte("b"))
	if string(va) != "1" || string(vb) != "2" {
		t.Errorf("restored: a=%q b=%q", va, vb)
	}
}

func TestFSM_Open_RecoversKeyEpochs(t *testing.T) {
	eng := memory.New()
	eng.Open()
	defer eng.Close()

	// Version records are 9 bytes: 8-byte LE epoch + 1-byte isCredit flag.
	var vk1, vk2 [9]byte
	binary.LittleEndian.PutUint64(vk1[:8], 10)
	binary.LittleEndian.PutUint64(vk2[:8], 20)
	b := eng.NewBatch()
	b.Set([]byte("__dpx:ver:k1"), vk1[:])
	b.Set([]byte("__dpx:ver:k2"), vk2[:])
	eng.ApplyBatch(b, engine.WriteOptions{})

	f := newFSM(eng, shared.SyncBatch, nil, nil, hlc.NewClock())
	if _, err := f.open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	if f.state.keyEpoch["k1"].Epoch != 10 {
		t.Errorf("keyEpoch(k1) = %d, want 10", f.state.keyEpoch["k1"].Epoch)
	}
	if f.state.keyEpoch["k2"].Epoch != 20 {
		t.Errorf("keyEpoch(k2) = %d, want 20", f.state.keyEpoch["k2"].Epoch)
	}
}

func TestFSM_Open_NoVersionKeysIsOK(t *testing.T) {
	eng := memory.New()
	eng.Open()
	defer eng.Close()

	f := newFSM(eng, shared.SyncBatch, nil, nil, hlc.NewClock())
	if _, err := f.open(nil); err != nil {
		t.Fatalf("Open with no version keys: %v", err)
	}
	if f.state.keyEpoch["nonexistent"].Epoch != 0 {
		t.Error("keyEpoch for missing key should be 0")
	}
}

type mockSnapshotSink struct {
	data []byte
}

func (m *mockSnapshotSink) Read(p []byte) (n int, err error) {
	if len(m.data) == 0 {
		return 0, io.EOF
	}
	n = copy(p, m.data)
	m.data = m.data[n:]
	return n, nil
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	m.data = append(m.data, p...)
	return len(p), nil
}
func (m *mockSnapshotSink) Close() error  { return nil }
func (m *mockSnapshotSink) ID() string    { return "mock" }
func (m *mockSnapshotSink) Cancel() error { return nil }
