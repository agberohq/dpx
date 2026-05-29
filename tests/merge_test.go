package tests

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/agberohq/dpx"
)

func TestCreditOnNonExistentKey(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	if err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.AtomicAdd(ctx, []byte("fresh"), 50)
		return err
	}); err != nil {
		t.Fatalf("credit on non-existent key: %v", err)
	}

	pairs, _ := n.GetRange(ctx, []byte("fresh"), []byte("fresh\x00"), 1)
	if len(pairs) == 0 {
		t.Fatal("key not created")
	}
	got := int64(binary.LittleEndian.Uint64(pairs[0].Value))
	if got != 50 {
		t.Errorf("got %d, want 50", got)
	}
}

func TestSetThenCredit(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("k"), le64(100))
	})
	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.AtomicAdd(ctx, []byte("k"), 50)
		return err
	})

	pairs, _ := n.GetRange(ctx, []byte("k"), []byte("k\x00"), 1)
	if len(pairs) == 0 {
		t.Fatal("key missing")
	}
	got := int64(binary.LittleEndian.Uint64(pairs[0].Value))
	if got != 150 {
		t.Errorf("Set(100)+Credit(50) = %d, want 150", got)
	}
}

func TestDeleteThenCredit(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	n.RunInTx(ctx, func(tx dpx.KVTx) error { return tx.Set(ctx, []byte("k"), le64(99)) })
	n.RunInTx(ctx, func(tx dpx.KVTx) error { return tx.Delete(ctx, []byte("k")) })
	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.AtomicAdd(ctx, []byte("k"), 50)
		return err
	})

	pairs, _ := n.GetRange(ctx, []byte("k"), []byte("k\x00"), 1)
	if len(pairs) == 0 {
		t.Fatal("key not resurrected after Delete+Credit")
	}
	got := int64(binary.LittleEndian.Uint64(pairs[0].Value))
	if got != 50 {
		t.Errorf("Delete+Credit(50) = %d, want 50", got)
	}
}

func TestCreditThenDeleteBlocked(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		tx.AtomicAdd(ctx, []byte("k"), 50)
		return tx.Delete(ctx, []byte("k"))
	})
	if err != dpx.ErrInvalidProposal {
		t.Errorf("got %v, want ErrInvalidProposal", err)
	}
}

func TestInternalKeysInvisible(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("user:1"), []byte("v"))
	})

	pairs, err := n.GetRange(ctx, []byte("\x00"), []byte("\xFF"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pairs {
		if len(p.Key) >= 6 && string(p.Key[:6]) == "__dpx:" {
			t.Errorf("internal key leaked to consumer: %s", p.Key)
		}
	}
}
