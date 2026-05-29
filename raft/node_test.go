package raft

import (
	"testing"
	"time"

	"github.com/agberohq/dpx"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/vmihailenco/msgpack/v5"
)

// Test single-node cluster: bootstrap and propose.
func TestNode_SingleNode_Propose(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		NodeID:     "node1",
		Engine:     eng,
		ListenAddr: "127.0.0.1:0",
		Peers: map[string]string{
			"node1": "127.0.0.1:0", // single-node cluster
		},
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	// Wait for leadership election (single node should become leader quickly).
	time.Sleep(500 * time.Millisecond)

	// Propose a simple Set.
	p := &dpx.Proposal{
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpSet, Key: []byte("hello"), Value: []byte("world")},
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
	if result != (dpx.ApplyResult{}) {
		t.Errorf("unexpected result: %+v", result)
	}

	// Verify the data was committed.
	val, err := eng.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("Get = %q, want %q", val, "world")
	}
}

// Test proposing to a non-leader returns ErrNotLeader.
func TestNode_NonLeader_RejectsPropose(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		NodeID:     "node1",
		Engine:     eng,
		ListenAddr: "127.0.0.1:0",
		// No peers — won't bootstrap, stays follower.
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	time.Sleep(200 * time.Millisecond)

	_, err = node.Propose([]byte("data"))
	if err != dpx.ErrNotLeader {
		t.Errorf("got %v, want ErrNotLeader", err)
	}
}

// Test shutdown cleans up resources.
func TestNode_Shutdown(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		NodeID:     "node1",
		Engine:     eng,
		ListenAddr: "127.0.0.1:0",
		Peers: map[string]string{
			"node1": "127.0.0.1:0",
		},
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Shutdown should succeed.
	if err := node.Shutdown(); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	// Second shutdown should be a no-op or error gracefully.
	err = node.Shutdown()
	if err != nil {
		t.Logf("second Shutdown returned: %v (expected)", err)
	}
}

// Test conflict detection through the full stack.
func TestNode_ConflictDetection(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		NodeID:     "node1",
		Engine:     eng,
		ListenAddr: "127.0.0.1:0",
		Peers: map[string]string{
			"node1": "127.0.0.1:0",
		},
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	time.Sleep(500 * time.Millisecond)

	// Propose an initial write.
	init := &dpx.Proposal{
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpSet, Key: []byte("k"), Value: []byte("v1")},
		},
	}
	data, _ := msgpack.Marshal(init)
	node.Propose(data)

	// Propose a conflicting write (stale epoch in read-set).
	conflict := &dpx.Proposal{
		ReadSet: []dpx.ReadEntry{
			{Key: []byte("k"), Epoch: 0}, // epoch 0 — key didn't exist yet
		},
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpSet, Key: []byte("k"), Value: []byte("v2")},
		},
	}
	data, _ = msgpack.Marshal(conflict)
	result, err := node.Propose(data)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !result.Conflict {
		t.Error("expected Conflict=true")
	}

	// Value must still be v1.
	val, _ := eng.Get([]byte("k"))
	if string(val) != "v1" {
		t.Errorf("value changed despite conflict: %q", val)
	}
}

// Test concurrent proposals from multiple goroutines.
func TestNode_ConcurrentProposals(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		NodeID:     "node1",
		Engine:     eng,
		ListenAddr: "127.0.0.1:0",
		Peers: map[string]string{
			"node1": "127.0.0.1:0",
		},
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	time.Sleep(500 * time.Millisecond)

	const goroutines = 10
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			p := &dpx.Proposal{
				Writes: []dpx.WriteEntry{
					{Op: dpx.OpSet, Key: []byte{byte('a' + id)}, Value: []byte{byte(id)}},
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

	// Verify all keys exist.
	for i := 0; i < goroutines; i++ {
		val, err := eng.Get([]byte{byte('a' + i)})
		if err != nil {
			t.Errorf("key %c missing: %v", 'a'+i, err)
		} else if len(val) != 1 || val[0] != byte(i) {
			t.Errorf("key %c: got %v, want %d", 'a'+i, val, i)
		}
	}
}

// Test that the FSM correctly updates the HLC.
func TestNode_HLCAdvances(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		NodeID:     "node1",
		Engine:     eng,
		ListenAddr: "127.0.0.1:0",
		Peers: map[string]string{
			"node1": "127.0.0.1:0",
		},
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	time.Sleep(500 * time.Millisecond)

	// Capture the initial HLC time by checking the FSM.
	raftNode := node.(*Node)
	initial := raftNode.fsm.hlc.Now()

	// Propose something.
	p := &dpx.Proposal{
		Writes: []dpx.WriteEntry{
			{Op: dpx.OpSet, Key: []byte("tick"), Value: []byte("1")},
		},
	}
	data, _ := msgpack.Marshal(p)
	node.Propose(data)

	// HLC should have advanced.
	after := raftNode.fsm.hlc.Now()
	if !after.After(initial) {
		t.Errorf("HLC did not advance: initial=%+v, after=%+v", initial, after)
	}
}

// Test that the FSM correctly updates applied index.
func TestNode_AppliedIndex(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		NodeID:     "node1",
		Engine:     eng,
		ListenAddr: "127.0.0.1:0",
		Peers: map[string]string{
			"node1": "127.0.0.1:0",
		},
	}

	node, err := Open(cfg, eng, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Shutdown()

	time.Sleep(500 * time.Millisecond)

	// Propose multiple entries.
	for i := 0; i < 5; i++ {
		p := &dpx.Proposal{
			Writes: []dpx.WriteEntry{
				{Op: dpx.OpSet, Key: []byte{byte('a' + i)}, Value: []byte{1}},
			},
		}
		data, _ := msgpack.Marshal(p)
		node.Propose(data)
	}

	// Check applied index.
	seq := eng.CurrentSequence()
	if seq < 5 {
		t.Errorf("CurrentSequence = %d, want >= 5", seq)
	}
}
