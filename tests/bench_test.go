package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/agberohq/dpx"
)

func BenchmarkTransfer_Memory(b *testing.B) {
	n := openMemNode(b)
	ctx := context.Background()

	const wallets = 100
	const initBal = int64(1_000_000)

	for i := 0; i < wallets; i++ {
		key := fmt.Sprintf("bench:w:%04d", i)
		v := make([]byte, 8)
		binary.LittleEndian.PutUint64(v, uint64(initBal))
		n.RunInTx(ctx, func(tx dpx.KVTx) error {
			return tx.Set(ctx, []byte(key), v)
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			from := rng.Intn(wallets)
			to := rng.Intn(wallets)
			if from == to {
				to = (to + 1) % wallets
			}
			fk := []byte(fmt.Sprintf("bench:w:%04d", from))
			tk := []byte(fmt.Sprintf("bench:w:%04d", to))

			n.RunInTx(ctx, func(tx dpx.KVTx) error {
				newBal, err := tx.AtomicAdd(ctx, fk, -1)
				if err != nil || newBal < 0 {
					return fmt.Errorf("insufficient")
				}
				_, err = tx.AtomicAdd(ctx, tk, 1)
				return err
			})
		}
	})
}
