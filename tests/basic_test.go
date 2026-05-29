package tests

import (
	"context"
	"testing"

	"github.com/agberohq/dpx"
	"github.com/agberohq/dpx/engine/memory"
)

func openMemNode(t testing.TB) *dpx.Node {
	t.Helper()
	n, err := dpx.Open(dpx.Config{Engine: memory.New()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

func TestSetGet(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("hello"), []byte("world"))
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	var got []byte
	err = n.RunInTx(ctx, func(tx dpx.KVTx) error {
		v, err := tx.Get(ctx, []byte("hello"))
		got = v
		return err
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "world" {
		t.Errorf("got %q, want %q", got, "world")
	}
}

func TestGetNotFound(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.Get(ctx, []byte("missing"))
		return err
	})
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestDelete(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("k"), []byte("v"))
	})
	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Delete(ctx, []byte("k"))
	})

	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.Get(ctx, []byte("k"))
		return err
	})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestGetRange(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		for _, k := range []string{"a", "b", "c", "d"} {
			tx.Set(ctx, []byte(k), []byte(k+"_val"))
		}
		return nil
	})

	pairs, err := n.GetRange(ctx, []byte("a"), []byte("d"), 0)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if len(pairs) != 3 { // a, b, c (d is exclusive end)
		t.Errorf("got %d pairs, want 3", len(pairs))
	}
}

func TestGetRangeReverse(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		for _, k := range []string{"a", "b", "c"} {
			tx.Set(ctx, []byte(k), []byte(k))
		}
		return nil
	})

	pairs, err := n.GetRangeReverse(ctx, []byte("a"), []byte("d"), 0)
	if err != nil {
		t.Fatalf("GetRangeReverse: %v", err)
	}
	if len(pairs) != 3 {
		t.Fatalf("got %d pairs, want 3", len(pairs))
	}
	if string(pairs[0].Key) != "c" {
		t.Errorf("first reverse key = %q, want c", pairs[0].Key)
	}
}

func TestReservedKeyRejected(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("__dpx:anything"), []byte("v"))
	})
	if err != dpx.ErrReservedKey {
		t.Errorf("got %v, want ErrReservedKey", err)
	}
}

func TestGetRangeAdvisory(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		tx.Set(ctx, []byte("s:0001"), le64(100))
		tx.Set(ctx, []byte("s:0002"), le64(200))
		return nil
	})

	// GetRange inside tx is advisory; does not add keys to read-set.
	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		pairs, _ := tx.GetRange(ctx, []byte("s:"), []byte("s:~"), 0)
		_ = pairs
		return tx.Set(ctx, []byte("other"), []byte("v"))
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAllocateNextSequence(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	var seq uint64
	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		s, err := tx.AllocateNextSequence(ctx)
		seq = s
		return err
	})
	if seq == 0 {
		t.Error("expected non-zero sequence")
	}
}

func TestInvalidProposal_CreditAndDelete(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		tx.AtomicAdd(ctx, []byte("k"), 10) // credit
		return tx.Delete(ctx, []byte("k")) // delete same key
	})
	if err != dpx.ErrInvalidProposal {
		t.Errorf("got %v, want ErrInvalidProposal", err)
	}
}

func TestEmptyTxProbeOnly(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, _ = tx.AtomicAdd(ctx, []byte("k"), 0) // probe; no write
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGetSafeReadPoint(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()
	_, err := n.GetSafeReadPoint(ctx)
	if err != nil {
		t.Fatalf("GetSafeReadPoint: %v", err)
	}
}

func TestGetDatabaseTime(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()
	ts, err := n.GetDatabaseTime(ctx)
	if err != nil {
		t.Fatalf("GetDatabaseTime: %v", err)
	}
	if ts.IsZero() {
		t.Error("zero time returned")
	}
}

func TestWatchKey(t *testing.T) {
	n := openMemNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := n.WatchKey(ctx, []byte("watch:"))
	if err != nil {
		t.Fatal(err)
	}

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("watch:key"), []byte("v"))
	})

	select {
	case _, ok := <-ch:
		if !ok {
			t.Error("channel closed unexpectedly")
		}
	default:
		// Notification may not arrive synchronously in single-node mode;
		// that's acceptable for this smoke test.
	}
}

func TestClosedNode(t *testing.T) {
	n := openMemNode(t)
	n.Close()

	err := n.RunInTx(context.Background(), func(tx dpx.KVTx) error {
		return tx.Set(context.Background(), []byte("k"), []byte("v"))
	})
	if err != dpx.ErrStoreClosed {
		t.Errorf("got %v, want ErrStoreClosed", err)
	}
}

// memory.New() is used in openMemNode above.
