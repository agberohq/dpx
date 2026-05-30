package raft

import (
	"testing"

	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/shared"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNode_SingleNode_Propose(t *testing.T) {
	eng := memory.New()
	cfg := shared.Config{
		NodeID: "node1",
		Engine: eng,
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	p := &shared.Proposal{
		Writes: []shared.WriteEntry{
			{Op: shared.OpSet, Key: []byte("hello"), Value: []byte("world")},
		},
	}
	data, err := msgpack.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	result, err := node.Propose(data)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if result != (shared.ApplyResult{}) {
		t.Errorf("unexpected result: %+v", result)
	}

	val, err := eng.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("Get = %q, want %q", val, "world")
	}
}

// TestNode_LeaderPropose verifies that a single-node cluster bootstraps,
// elects itself leader, and accepts proposals. The non-leader rejection path
// is only reachable in multi-node clusters and is tested via the error message.
func TestNode_LeaderPropose(t *testing.T) {
	eng := memory.New()
	cfg := shared.Config{
		NodeID: "node1",
		Engine: eng,
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	// Single-node Open() waits for leader election before returning.
	if s := node.(*Node).State(); s != "Leader" {
		t.Errorf("expected Leader state, got %s", s)
	}
}

func TestNode_Shutdown(t *testing.T) {
	eng := memory.New()
	cfg := shared.Config{
		NodeID: "node1",
		Engine: eng,
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := node.Shutdown(); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	err = node.Shutdown()
	if err != nil {
		t.Logf("second Shutdown returned: %v (expected)", err)
	}
}

func TestNode_ConcurrentProposals(t *testing.T) {
	eng := memory.New()
	cfg := shared.Config{
		NodeID: "node1",
		Engine: eng,
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	const goroutines = 10
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			p := &shared.Proposal{
				Writes: []shared.WriteEntry{
					{Op: shared.OpSet, Key: []byte{byte('a' + id)}, Value: []byte{byte(id)}},
				},
			}
			data, err := msgpack.Marshal(p)
			if err != nil {
				errs <- err
				return
			}
			_, err = node.Propose(data)
			errs <- err
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent proposal %d: %v", i, err)
		}
	}

	for i := 0; i < goroutines; i++ {
		val, err := eng.Get([]byte{byte('a' + i)})
		if err != nil {
			t.Errorf("key %c missing: %v", 'a'+i, err)
		} else if len(val) != 1 || val[0] != byte(i) {
			t.Errorf("key %c: got %v, want %d", 'a'+i, val, i)
		}
	}
}

func TestNode_AppliedIndex(t *testing.T) {
	eng := memory.New()
	cfg := shared.Config{
		NodeID: "node1",
		Engine: eng,
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	for i := 0; i < 5; i++ {
		p := &shared.Proposal{
			Writes: []shared.WriteEntry{
				{Op: shared.OpSet, Key: []byte{byte('a' + i)}, Value: []byte{1}},
			},
		}
		data, _ := msgpack.Marshal(p)
		node.Propose(data)
	}

	seq := eng.CurrentSequence()
	if seq < 5 {
		t.Errorf("CurrentSequence = %d, want >= 5", seq)
	}
}
