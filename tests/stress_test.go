package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/agberohq/dpx"
)

// TestStressTransfer validates correctness under high concurrency.
// 1000 wallets, 10000 concurrent transfers, each transferring 1 unit.
// Total balance must be preserved.
func TestStressTransfer(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	const wallets = 1000
	const initBal = int64(1_000_000)
	const transfers = 10000

	// Seed wallets
	for i := 0; i < wallets; i++ {
		key := fmt.Sprintf("w:%06d", i)
		n.RunInTx(ctx, func(tx dpx.KVTx) error {
			return tx.Set(ctx, []byte(key), le64(initBal))
		})
	}

	var wg sync.WaitGroup
	var successes atomic.Int64
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < transfers; i++ {
		wg.Add(1)
		from := rng.Intn(wallets)
		to := rng.Intn(wallets)
		if from == to {
			to = (to + 1) % wallets
		}
		go func(from, to int) {
			defer wg.Done()
			fk := []byte(fmt.Sprintf("w:%06d", from))
			tk := []byte(fmt.Sprintf("w:%06d", to))
			err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				newBal, err := tx.AtomicAdd(ctx, fk, -1)
				if err != nil || newBal < 0 {
					return fmt.Errorf("insufficient")
				}
				_, err = tx.AtomicAdd(ctx, tk, 1)
				return err
			})
			if err == nil {
				successes.Add(1)
			}
		}(from, to)
	}
	wg.Wait()

	// Verify total balance is preserved
	var total int64
	for i := 0; i < wallets; i++ {
		key := []byte(fmt.Sprintf("w:%06d", i))
		end := append(append([]byte(nil), key...), 0)
		pairs, _ := n.GetRange(ctx, key, end, 1)
		if len(pairs) > 0 {
			total += int64(binary.LittleEndian.Uint64(pairs[0].Value))
		}
	}
	want := int64(wallets) * initBal
	if total != want {
		t.Errorf("balance invariant broken: total=%d want=%d (successes=%d)", total, want, successes.Load())
	}
}

// TestStressShardedTransfer runs the same test on the sharded engine.
func TestStressShardedTransfer(t *testing.T) {
	n := openShardedNode(t)
	ctx := context.Background()

	const wallets = 1000
	const initBal = int64(1_000_000)
	const transfers = 10000

	for i := 0; i < wallets; i++ {
		key := fmt.Sprintf("w:%06d", i)
		n.RunInTx(ctx, func(tx dpx.KVTx) error {
			return tx.Set(ctx, []byte(key), le64(initBal))
		})
	}

	var wg sync.WaitGroup
	var successes atomic.Int64
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < transfers; i++ {
		wg.Add(1)
		from := rng.Intn(wallets)
		to := rng.Intn(wallets)
		if from == to {
			to = (to + 1) % wallets
		}
		go func(from, to int) {
			defer wg.Done()
			fk := []byte(fmt.Sprintf("w:%06d", from))
			tk := []byte(fmt.Sprintf("w:%06d", to))
			err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				newBal, err := tx.AtomicAdd(ctx, fk, -1)
				if err != nil || newBal < 0 {
					return fmt.Errorf("insufficient")
				}
				_, err = tx.AtomicAdd(ctx, tk, 1)
				return err
			})
			if err == nil {
				successes.Add(1)
			}
		}(from, to)
	}
	wg.Wait()

	var total int64
	for i := 0; i < wallets; i++ {
		key := []byte(fmt.Sprintf("w:%06d", i))
		end := append(append([]byte(nil), key...), 0)
		pairs, _ := n.GetRange(ctx, key, end, 1)
		if len(pairs) > 0 {
			total += int64(binary.LittleEndian.Uint64(pairs[0].Value))
		}
	}
	want := int64(wallets) * initBal
	if total != want {
		t.Errorf("balance invariant broken: total=%d want=%d (successes=%d)", total, want, successes.Load())
	}
}

// TestStressContendedKey validates behavior when many goroutines
// contend on the same key — high conflict rate scenario.
func TestStressContendedKey(t *testing.T) {
	n := openMemNode(t)
	ctx := context.Background()

	const initBal = int64(1_000_000)
	const writers = 100
	const attempts = 1000

	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("hotkey"), le64(initBal))
	})

	var wg sync.WaitGroup
	var successes atomic.Int64

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < attempts; j++ {
				err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
					newBal, err := tx.AtomicAdd(ctx, []byte("hotkey"), -1)
					if err != nil || newBal < 0 {
						return fmt.Errorf("skip")
					}
					return nil
				})
				if err == nil {
					successes.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	pairs, _ := n.GetRange(ctx, []byte("hotkey"), []byte("hotkey\x00"), 1)
	if len(pairs) == 0 {
		t.Fatal("key not found")
	}
	final := int64(binary.LittleEndian.Uint64(pairs[0].Value))
	expected := initBal - successes.Load()
	if final != expected {
		t.Errorf("hotkey = %d, want %d (successes=%d)", final, expected, successes.Load())
	}
}

// TestHealthCheck validates the Health() endpoint.
func TestHealthCheck(t *testing.T) {
	n := openMemNodeWithMetrics(t)
	ctx := context.Background()

	// Healthy node
	h := n.Health()
	if !h.Alive {
		t.Error("expected alive")
	}
	if h.Closed {
		t.Error("expected not closed")
	}

	// After a transaction
	n.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("k"), []byte("v"))
	})
	h = n.Health()
	if h.Transactions != 1 {
		t.Errorf("expected 1 transaction, got %d", h.Transactions)
	}

	// After close
	n.Close()
	h = n.Health()
	if h.Alive {
		t.Error("expected not alive after close")
	}
	if !h.Closed {
		t.Error("expected closed after close")
	}
}

// TestPebbleStressTransfer runs the stress test on Pebble for durability validation.
func TestPebbleStressTransfer(t *testing.T) {
	n := openPebbleNode(t)
	ctx := context.Background()

	const wallets = 100
	const initBal = int64(10_000)
	const transfers = 1000

	for i := 0; i < wallets; i++ {
		key := fmt.Sprintf("w:%04d", i)
		n.RunInTx(ctx, func(tx dpx.KVTx) error {
			return tx.Set(ctx, []byte(key), le64(initBal))
		})
	}

	var wg sync.WaitGroup
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < transfers; i++ {
		wg.Add(1)
		from := rng.Intn(wallets)
		to := rng.Intn(wallets)
		if from == to {
			to = (to + 1) % wallets
		}
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
