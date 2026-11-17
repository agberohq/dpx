package dpx

import (
	"context"
	"fmt"
	"testing"
)

// TestRunInTx_LargeReadSetConflict: a transaction whose read set exceeds the
// conductor's stack-shadow size must still conflict when a key it read is
// overwritten before it commits.
func TestRunInTx_LargeReadSetConflict(t *testing.T) {
	for _, open := range []struct {
		name string
		fn   func(Config) (*Node, error)
	}{{"sharded", OpenSharded}, {"mvcc-sharded", OpenMVCCSharded}, {"embedded", OpenEmbedded}} {
		t.Run(open.name, func(t *testing.T) {
			ctx := context.Background()
			n, err := open.fn(Config{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = n.Close() })

			const reads = 100
			key := func(i int) []byte { return []byte(fmt.Sprintf("big:%03d", i)) }
			if err := n.RunInTx(ctx, func(tx KVTx) error {
				for i := 0; i < reads; i++ {
					if err := tx.Set(ctx, key(i), []byte("v0")); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			attempts := 0
			err = n.RunInTx(ctx, func(tx KVTx) error {
				attempts++
				for i := 0; i < reads; i++ {
					if _, err := tx.Get(ctx, key(i)); err != nil {
						return err
					}
				}
				if attempts == 1 {
					// A concurrent writer overwrites the last key read.
					if err := n.RunInTx(ctx, func(w KVTx) error {
						return w.Set(ctx, key(reads-1), []byte("v1"))
					}); err != nil {
						return err
					}
				}
				return tx.Set(ctx, []byte("big:result"), []byte("done"))
			})
			if err != nil {
				t.Fatal(err)
			}
			if attempts < 2 {
				t.Fatalf("stale read set of %d keys committed without conflict (attempts=%d)", reads, attempts)
			}
		})
	}
}
