package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/agberohq/dpx"
)

func TestTwoConcurrentDebits(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	// Seed balance = 100.
	if err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("balance"), le64(100))
	}); err != nil {
		t.Fatal(err)
	}

	// Two goroutines each try to debit 60.
	// Only one can succeed (100 < 120).
	var successes atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				newBal, err := tx.AtomicAdd(ctx, []byte("balance"), -60)
				if err != nil {
					return err
				}
				if newBal < 0 {
					return fmt.Errorf("insufficient funds")
				}
				return nil
			})
			if err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()

	if successes.Load() != 1 {
		t.Errorf("got %d successes, want 1", successes.Load())
	}
}

func TestCreditCommutativity(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	const goroutines = 20
	const delta = int64(5)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				_, err := tx.AtomicAdd(ctx, []byte("counter"), delta)
				return err
			}); err != nil {
				t.Errorf("credit failed: %v", err)
			}
		}()
	}
	wg.Wait()

	pairs, err := n.GetRange(ctx, []byte("counter"), []byte("counter\x00"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) == 0 {
		t.Fatal("key not found")
	}
	got := int64(binary.LittleEndian.Uint64(pairs[0].Value))
	want := int64(goroutines) * delta
	if got != want {
		t.Errorf("counter = %d, want %d", got, want)
	}
}

func TestBalanceInvariant(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	const wallets = 10
	const initBal = int64(1_000_000)
	const transfers = 200

	for i := 0; i < wallets; i++ {
		key := fmt.Sprintf("w:%04d", i)
		n.RunInTx(ctx, func(tx dpx.KVTx) error {
			return tx.Set(ctx, []byte(key), le64(initBal))
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < transfers; i++ {
		wg.Add(1)
		from := i % wallets
		to := (i + 1) % wallets
		go func(from, to int) {
			defer wg.Done()
			fk := []byte(fmt.Sprintf("w:%04d", from))
			tk := []byte(fmt.Sprintf("w:%04d", to))
			n.RunInTx(ctx, func(tx dpx.KVTx) error {
				newBal, err := tx.AtomicAdd(ctx, fk, -1)
				if err != nil || newBal < 0 {
					return fmt.Errorf("skip")
				}
				_, err = tx.AtomicAdd(ctx, tk, 1)
				return err
			})
		}(from, to)
	}
	wg.Wait()

	var total int64
	for i := 0; i < wallets; i++ {
		key := []byte(fmt.Sprintf("w:%04d", i))
		end := append(append([]byte(nil), key...), 0)
		pairs, _ := n.GetRange(ctx, key, end, 1)
		if len(pairs) > 0 {
			total += int64(binary.LittleEndian.Uint64(pairs[0].Value))
		}
	}
	want := int64(wallets) * initBal
	if total != want {
		t.Errorf("balance invariant broken: total=%d want=%d", total, want)
	}
}

func TestCreditBeforeDebitNoFalseConflict(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("k"), le64(50))
	})
	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.AtomicAdd(ctx, []byte("k"), 20)
		return err
	})

	err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
		v, err := tx.AtomicAdd(ctx, []byte("k"), -30)
		if err != nil {
			return err
		}
		if v < 0 {
			return fmt.Errorf("insufficient")
		}
		return nil
	})
	if err != nil {
		t.Errorf("debit after credit failed: %v", err)
	}
}

func le64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}
