package raft

import (
	"encoding/binary"
	"testing"

	"github.com/agberohq/dpx"
	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/hashicorp/raft"
	"github.com/vmihailenco/msgpack/v5"
)

// helpers

func setupFSM(t *testing.T) (*fsm, *memory.Engine) {
	t.Helper()
	eng := memory.New()
	if err := eng.Open(); err != nil {
		t.Fatalf("engine Open: %v", err)
	}
	f := newFSM(eng, nil) // nil watchers — tests don't exercise notifications
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

func proposalBytes(t *testing.T, p *dpx.Proposal) []byte {
	t.Helper()
	data, err := msgpack.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func TestFSM_Apply_SetAndGet(t *testing.T) {
	f, eng := setupFSM(t)

	// Propose a Set.
	p := &dpx.Proposal{
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpSet, Key: []byte("k"), Value: []byte("v")},
		},
	}
	log := &mockRaftLog{index: 1, data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (dpx.ApplyResult{}) {
		t.Errorf("Set Apply: got %+v, want zero ApplyResult", result)
	}

	// Verify the engine has the key.
	val, err := eng.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after Apply: %v", err)
	}
	if string(val) != "v" {
		t.Errorf("Get = %q, want %q", val, "v")
	}

	// Verify the version key.
	ver, err := eng.Get(dpx.VersionKey(nil, []byte("k")))
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if decode64(ver) != 1 {
		t.Errorf("epoch = %d, want 1", decode64(ver))
	}
}

func TestFSM_Apply_Delete(t *testing.T) {
	f, eng := setupFSM(t)

	// Set a key first.
	eng.ApplyBatch([]engine.WriteOp{
		{Type: engine.OpSet, Key: []byte("k"), Value: []byte("v")},
	})
	if err := eng.SetVersion([]byte("k"), engine.EpochRecord{Epoch: 1}); err != nil {
		t.Fatalf("SetVersion: %v", err)
	}

	// Now delete it via FSM.
	p := &dpx.Proposal{
		ReadSet: []dpx.ReadEntry{
			{Key: []byte("k"), Epoch: 1},
		},
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpDelete, Key: []byte("k")},
		},
	}
	log := &mockRaftLog{index: 2, data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (dpx.ApplyResult{}) {
		t.Errorf("Delete Apply: got %+v, want zero ApplyResult", result)
	}

	// Key should be gone.
	_, err := eng.Get([]byte("k"))
	if err == nil {
		t.Error("key should be deleted")
	}
}

func TestFSM_Apply_ConflictOnEpochMismatch(t *testing.T) {
	f, eng := setupFSM(t)

	// Set key with epoch 5.
	eng.ApplyBatch([]engine.WriteOp{
		{Type: engine.OpSet, Key: []byte("k"), Value: []byte("v")},
	})
	if err := eng.SetVersion([]byte("k"), engine.EpochRecord{Epoch: 5}); err != nil {
		t.Fatalf("SetVersion: %v", err)
	}

	// Propose with stale epoch 3 (conflict).
	p := &dpx.Proposal{
		ReadSet: []dpx.ReadEntry{
			{Key: []byte("k"), Epoch: 3},
		},
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpSet, Key: []byte("k"), Value: []byte("new")},
		},
	}
	log := &mockRaftLog{index: 6, data: proposalBytes(t, p)}

	result := f.Apply(log)
	if !result.Conflict {
		t.Error("expected Conflict=true on epoch mismatch")
	}

	// Value must not have changed.
	val, _ := eng.Get([]byte("k"))
	if string(val) != "v" {
		t.Errorf("value changed despite conflict: %q", val)
	}
}

func TestFSM_Apply_ConflictOnDebitEpochMismatch(t *testing.T) {
	f, eng := setupFSM(t)

	// Set up a balance key with epoch 2.
	eng.ApplyBatch([]engine.WriteOp{
		{Type: engine.OpSet, Key: []byte("bal"), Value: le64(100)},
	})
	if err := eng.SetVersion([]byte("bal"), engine.EpochRecord{Epoch: 2}); err != nil {
		t.Fatalf("SetVersion: %v", err)
	}

	// Propose a debit with stale epoch 1.
	p := &dpx.Proposal{
		ReadSet: []dpx.ReadEntry{
			{Key: []byte("bal"), Epoch: 1, IsDebit: true},
		},
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpDebit, Key: []byte("bal"), Value: le64(-30)},
		},
	}
	log := &mockRaftLog{index: 3, data: proposalBytes(t, p)}

	result := f.Apply(log)
	if !result.Conflict {
		t.Error("expected Conflict=true on debit epoch mismatch")
	}

	// Balance must remain 100.
	val, _ := eng.Get([]byte("bal"))
	if decode64(val) != 100 {
		t.Errorf("balance changed despite conflict: %d", decode64(val))
	}
}

func TestFSM_Apply_CreditNoConflict(t *testing.T) {
	f, eng := setupFSM(t)

	// Credits don't have read-set entries, so they never conflict.
	p := &dpx.Proposal{
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpCredit, Key: []byte("bal"), Value: le64(50)},
		},
	}
	log := &mockRaftLog{index: 1, data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (dpx.ApplyResult{}) {
		t.Errorf("Credit Apply: got %+v, want zero ApplyResult", result)
	}

	// The merge should have created the key.
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

	// Proposal with only read-set (no writes).
	p := &dpx.Proposal{
		ReadSet: []dpx.ReadEntry{
			{Key: []byte("k"), Epoch: 0},
		},
	}
	log := &mockRaftLog{index: 1, data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (dpx.ApplyResult{}) {
		t.Errorf("read-only Apply: got %+v, want zero ApplyResult", result)
	}

	// Engine must be untouched.
	_, err := eng.Get([]byte("k"))
	if err == nil {
		t.Error("read-only proposal should not write to engine")
	}
}

func TestFSM_Apply_AppliedIndexUpdated(t *testing.T) {
	f, eng := setupFSM(t)

	p := &dpx.Proposal{
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpSet, Key: []byte("k"), Value: []byte("v")},
		},
	}
	log := &mockRaftLog{index: 42, data: proposalBytes(t, p)}

	f.Apply(log)

	appliedBytes, err := eng.Get(dpx.AppliedKey)
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

	p := &dpx.Proposal{
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpSet, Key: []byte("a"), Value: []byte("1")},
			{Op: dpx.OpSet, Key: []byte("b"), Value: []byte("2")},
			{Op: dpx.OpCredit, Key: []byte("c"), Value: le64(100)},
		},
	}
	log := &mockRaftLog{index: 1, data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (dpx.ApplyResult{}) {
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

	// Debit on non-existent key: the version check should not conflict
	// (epoch 0 vs epoch 0 = match), but the merge will produce a negative value.
	p := &dpx.Proposal{
		ReadSet: []dpx.ReadEntry{
			{Key: []byte("new"), Epoch: 0, IsDebit: true},
		},
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpDebit, Key: []byte("new"), Value: le64(-50)},
		},
	}
	log := &mockRaftLog{index: 1, data: proposalBytes(t, p)}

	result := f.Apply(log)
	if result != (dpx.ApplyResult{}) {
		t.Errorf("debit on missing key: got %+v, want zero ApplyResult", result)
	}

	val, _ := eng.Get([]byte("new"))
	if decode64(val) != -50 {
		t.Errorf("balance = %d, want -50", decode64(val))
	}
}

// FSM.Snapshot / Restore

func TestFSM_Snapshot_Restore(t *testing.T) {
	f, eng := setupFSM(t)

	// Write some data.
	eng.ApplyBatch([]engine.WriteOp{
		{Type: engine.OpSet, Key: []byte("a"), Value: []byte("1")},
		{Type: engine.OpSet, Key: []byte("b"), Value: []byte("2")},
	})
	eng.SetVersion([]byte("a"), engine.EpochRecord{Epoch: 1})
	eng.SetVersion([]byte("b"), engine.EpochRecord{Epoch: 1})
	eng.Set(dpx.AppliedKey, le64(5))

	// Snapshot.
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Create a new engine and FSM, restore into it.
	newEng := memory.New()
	newEng.Open()
	defer newEng.Close()
	newFSM := newFSM(newEng, nil)

	// Persist the snapshot to a sink and restore.
	sink := &mockSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if err := newFSM.Restore(sink); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify data.
	va, _ := newEng.Get([]byte("a"))
	vb, _ := newEng.Get([]byte("b"))
	if string(va) != "1" || string(vb) != "2" {
		t.Errorf("restored: a=%q b=%q", va, vb)
	}

	appliedBytes, _ := newEng.Get(dpx.AppliedKey)
	applied := binary.LittleEndian.Uint64(appliedBytes)
	if applied != 5 {
		t.Errorf("restored applied index = %d, want 5", applied)
	}
}

func TestFSM_Snapshot_RestoreWithVersions(t *testing.T) {
	f, eng := setupFSM(t)

	// Write data with versions.
	eng.ApplyBatch([]engine.WriteOp{
		{Type: engine.OpSet, Key: []byte("x"), Value: le64(42)},
	})
	eng.SetVersion([]byte("x"), engine.EpochRecord{Epoch: 7, IsCredit: false})

	// Snapshot and restore.
	snap, _ := f.Snapshot()
	sink := &mockSnapshotSink{}
	snap.Persist(sink)

	newEng := memory.New()
	newEng.Open()
	defer newEng.Close()
	newFSM := newFSM(newEng, nil)
	newFSM.Restore(sink)

	// Check version.
	ver, err := newEng.GetVersion([]byte("x"))
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if ver.Epoch != 7 {
		t.Errorf("restored epoch = %d, want 7", ver.Epoch)
	}
	if ver.IsCredit {
		t.Error("restored IsCredit should be false")
	}
}

// FSM.Open (key epoch recovery)

func TestFSM_Open_RecoversKeyEpochs(t *testing.T) {
	eng := memory.New()
	eng.Open()
	defer eng.Close()

	// Manually set up version keys.
	eng.ApplyBatch([]engine.WriteOp{
		{Type: engine.OpSet, Key: dpx.VersionKey(nil, []byte("k1")), Value: le64(10)},
		{Type: engine.OpSet, Key: dpx.VersionKey(nil, []byte("k2")), Value: le64(20)},
	})
	// Set applied index.
	eng.Set(dpx.AppliedKey, le64(20))

	// Open FSM — should recover key epochs.
	f := newFSM(eng, nil)
	if err := f.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}

	if f.keyEpoch("k1") != 10 {
		t.Errorf("keyEpoch(k1) = %d, want 10", f.keyEpoch("k1"))
	}
	if f.keyEpoch("k2") != 20 {
		t.Errorf("keyEpoch(k2) = %d, want 20", f.keyEpoch("k2"))
	}
}

func TestFSM_Open_NoVersionKeysIsOK(t *testing.T) {
	eng := memory.New()
	eng.Open()
	defer eng.Close()

	f := newFSM(eng, nil)
	if err := f.Open(); err != nil {
		t.Fatalf("Open with no version keys: %v", err)
	}
	// keyEpoch map should be empty but usable.
	if f.keyEpoch("nonexistent") != 0 {
		t.Error("keyEpoch for missing key should be 0")
	}
}

// mock types

type mockRaftLog struct {
	index uint64
	data  []byte
}

func (m *mockRaftLog) Index() uint64      { return m.index }
func (m *mockRaftLog) Term() uint64       { return 1 }
func (m *mockRaftLog) Type() raft.LogType { return raft.LogCommand }
func (m *mockRaftLog) Data() []byte       { return m.data }

type mockSnapshotSink struct {
	data []byte
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *mockSnapshotSink) Close() error { return nil }

func (m *mockSnapshotSink) ID() string { return "mock" }

func (m *mockSnapshotSink) Cancel() error { return nil }
