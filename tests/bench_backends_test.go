package tests

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agberohq/dpx"
	dpxraft "github.com/agberohq/dpx/conductor"
	"github.com/agberohq/dpx/engine/badger"
	"github.com/agberohq/dpx/engine/memory"
	dpxoshodi "github.com/agberohq/dpx/engine/oshodi"
	"github.com/agberohq/dpx/engine/pebble"
	"github.com/agberohq/oshodi"
)

// This file is the cross-backend benchmark matrix.
//
// The proposer's conflict-detection and commit path is engine-agnostic, so any
// change to it must be measured on every backend, not just the in-memory
// sharded engine. In particular, disk engines trade CPU for fsync: a change
// that looks like a 4x win on memory can be invisible (or negative) once the
// commit path is dominated by file sync.
//
// Workloads:
//   transfer2kv  — 2 AtomicAdds, the shape dpx's own benchmarks use.
//   tellarshape  — 9 gets + 5 atomic adds + 12 unique-key sets, matching one
//                  Teller transfer (measured with an instrumented tx wrapper).

type backendSpec struct {
	name    string
	open    func(b *testing.B) *dpx.Node
	noshard bool
}

func backendSpecs() []backendSpec {
	return []backendSpec{
		{name: "mem-sharded", open: func(b *testing.B) *dpx.Node {
			b.Helper()
			n, err := dpx.OpenSharded(dpx.Config{SyncPolicy: dpx.SyncNone})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			return n
		}},
		{name: "mem-mvcc", open: func(b *testing.B) *dpx.Node {
			b.Helper()
			n, err := dpx.OpenMVCCSharded(dpx.Config{SyncPolicy: dpx.SyncNone})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			return n
		}},
		{name: "mem-embedded", open: func(b *testing.B) *dpx.Node {
			b.Helper()
			n, err := dpx.OpenEmbedded(dpx.Config{SyncPolicy: dpx.SyncNone})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			return n
		}},
		{name: "pebble-none", open: func(b *testing.B) *dpx.Node {
			b.Helper()
			n, err := dpx.OpenPebble(dpx.Config{DataDir: b.TempDir(), SyncPolicy: dpx.SyncNone})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			return n
		}},
		{name: "pebble-batch", open: func(b *testing.B) *dpx.Node {
			b.Helper()
			n, err := dpx.OpenPebble(dpx.Config{DataDir: b.TempDir(), SyncPolicy: dpx.SyncBatch})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			return n
		}},
		{name: "pebble-full", open: func(b *testing.B) *dpx.Node {
			b.Helper()
			n, err := dpx.OpenPebble(dpx.Config{DataDir: b.TempDir(), SyncPolicy: dpx.SyncFull})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			return n
		}},
		{name: "oshodi-default", open: oshodiOpener()},
		{name: "oshodi-coalesce", open: oshodiOpener(oshodi.WithWriteCoalescing(true))},
		{name: "oshodi-tuned", open: oshodiOpener(
			oshodi.WithWriteCoalescing(true),
			oshodi.WithShardCount(256),
			oshodi.WithCacheSize(100_000),
			oshodi.WithBatchSettings(1, 1000, time.Millisecond),
			oshodi.WithWAL(true),
			oshodi.WithWALSyncMode(false),
		)},
		{name: "oshodi-walsync", open: oshodiOpener(
			oshodi.WithWriteCoalescing(true),
			oshodi.WithWAL(true),
			oshodi.WithWALSyncMode(true),
		)},
		{name: "badger", open: func(b *testing.B) *dpx.Node {
			b.Helper()
			dir := b.TempDir()
			n, err := dpx.Open(dpx.Config{
				Engine:     badger.New(dir),
				SyncPolicy: dpx.SyncNone,
				RaftDir:    dir + "/raft",
			}, dpxraft.Open)
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			return n
		}},
	}
}

// oshodiOpener builds a node over the Oshodi engine with the given oshodi
// options, so benchmarks can cover tuned configurations.
func oshodiOpener(opts ...oshodi.Option) func(b *testing.B) *dpx.Node {
	return func(b *testing.B) *dpx.Node {
		b.Helper()
		n, err := dpx.Open(dpx.Config{
			Engine:     dpxoshodi.New(b.TempDir(), opts...),
			SyncPolicy: dpx.SyncNone,
		}, dpxraft.Open)
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		return n
	}
}

// ensure the engine package symbols stay referenced if specs change.
var (
	_ = memory.New
	_ = dpxoshodi.New
	_ = pebble.New
)

// seedKeys creates n balance keys with a large starting balance.
func seedKeys(b *testing.B, n *dpx.Node, keys [][]byte, tag string) {
	b.Helper()
	ctx := context.Background()
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench:%s:w%04d", tag, i))
		if err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
			_, err := tx.AtomicAdd(ctx, keys[i], int64(b.N)*1000+1_000_000)
			return err
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
}

func benchTransfer2KV(b *testing.B, n *dpx.Node) {
	b.Helper()
	ctx := context.Background()
	const numWallets = 64
	keys := make([][]byte, numWallets)
	seedKeys(b, n, keys, "t2")
	receiver := []byte("bench:t2:recv")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			from := keys[i%numWallets]
			err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				if _, err := tx.AtomicAdd(ctx, from, -1); err != nil {
					return err
				}
				_, err := tx.AtomicAdd(ctx, receiver, 1)
				return err
			})
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func benchTellerShape(b *testing.B, n *dpx.Node) {
	b.Helper()
	ctx := context.Background()
	const numWallets = 64
	keys := make([][]byte, numWallets)
	seedKeys(b, n, keys, "ts")
	payload := make([]byte, 400)
	var seq uint64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			seq++
			base := seq * 1000
			err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				for j := 0; j < 9; j++ {
					if _, err := tx.Get(ctx, keys[(i+j)%numWallets]); err != nil && !dpx.IsNotFound(err) {
						return err
					}
				}
				for j := 0; j < 5; j++ {
					if _, err := tx.AtomicAdd(ctx, keys[(i+j)%numWallets], -1); err != nil {
						return err
					}
				}
				for j := 0; j < 12; j++ {
					k := []byte(fmt.Sprintf("bench:ts:rec:%d:%d", base, j))
					if err := tx.Set(ctx, k, payload); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// benchDisjoint is the scaling benchmark: every goroutine writes only its own
// key pair, so there is no shared hot key and no conflict retry. Any remaining
// serialization is the store's, not the workload's.
func benchDisjoint(b *testing.B, n *dpx.Node) {
	b.Helper()
	ctx := context.Background()
	const perWorker = 8
	workers := runtime.GOMAXPROCS(0)

	// Seed outside the timed region.
	pairs := make([][2][]byte, workers*perWorker)
	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			p := &pairs[w*perWorker+i]
			p[0] = []byte(fmt.Sprintf("bench:dj:%d:%d:a", w, i))
			p[1] = []byte(fmt.Sprintf("bench:dj:%d:%d:b", w, i))
			if err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				if _, err := tx.AtomicAdd(ctx, p[0], 1_000_000); err != nil {
					return err
				}
				_, err := tx.AtomicAdd(ctx, p[1], 1_000_000)
				return err
			}); err != nil {
				b.Fatalf("seed: %v", err)
			}
		}
	}

	var next atomic.Int64
	var workerIDs [64]atomic.Int64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		slot := int(next.Add(1)) - 1
		if slot >= len(workerIDs) {
			slot = len(workerIDs) - 1
		}
		w := int(workerIDs[slot].Add(1)) - 1
		if w >= workers {
			w = workers - 1
		}
		i := 0
		for pb.Next() {
			pr := pairs[w*perWorker+(i%perWorker)]
			err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				if _, err := tx.AtomicAdd(ctx, pr[0], -1); err != nil {
					return err
				}
				_, err := tx.AtomicAdd(ctx, pr[1], 1)
				return err
			})
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// benchBounded is the same op mix as benchTellerShape but reuses a fixed key
// set, so the keyspace does not grow with b.N. It isolates the write path from
// per-snapshot cost: backends that materialize a snapshot per transaction pay
// O(keyspace) on every transaction, which makes an append-heavy benchmark
// measure the snapshot, not the store.
func benchBounded(b *testing.B, n *dpx.Node) {
	b.Helper()
	ctx := context.Background()
	const numKeys = 64
	keys := make([][]byte, numKeys)
	seedKeys(b, n, keys, "bd")
	payload := make([]byte, 400)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
				for j := 0; j < 9; j++ {
					if _, err := tx.Get(ctx, keys[(i+j)%numKeys]); err != nil && !dpx.IsNotFound(err) {
						return err
					}
				}
				for j := 0; j < 5; j++ {
					if _, err := tx.AtomicAdd(ctx, keys[(i+j)%numKeys], -1); err != nil {
						return err
					}
				}
				for j := 0; j < 12; j++ {
					if err := tx.Set(ctx, keys[(i*7+j)%numKeys], payload); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkBackends runs the matrix: backend x workload.
//
//	go test -run '^$' -bench 'BenchmarkBackends' -cpu 1,12 -benchtime 1s ./tests/
func BenchmarkBackends(b *testing.B) {
	for _, spec := range backendSpecs() {
		spec := spec
		b.Run(spec.name, func(b *testing.B) {
			node := spec.open(b)
			defer node.Close()
			b.Run("transfer2kv", func(b *testing.B) { benchTransfer2KV(b, node) })
			b.Run("bounded", func(b *testing.B) { benchBounded(b, node) })
			b.Run("disjoint", func(b *testing.B) { benchDisjoint(b, node) })
			b.Run("tellarshape", func(b *testing.B) { benchTellerShape(b, node) })
		})
	}
}

// BenchmarkPebbleSyncBatchCallers asks whether the fsync cost of SyncBatch is
// amortised by batching when more callers are in flight than there are cores.
// The matrix runs under RunParallel, which caps in-flight proposals at
// GOMAXPROCS; a real server has far more.
func BenchmarkPebbleSyncBatchCallers(b *testing.B) {
	for _, callers := range []int{12, 64, 256} {
		callers := callers
		b.Run(fmt.Sprintf("callers=%d", callers), func(b *testing.B) {
			dir := b.TempDir()
			n, err := dpx.OpenPebble(dpx.Config{DataDir: dir, SyncPolicy: dpx.SyncBatch})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer n.Close()

			ctx := context.Background()
			const numWallets = 64
			keys := make([][]byte, numWallets)
			seedKeys(b, n, keys, "pb")

			b.ResetTimer()
			b.ReportAllocs()
			var wg sync.WaitGroup
			var ops atomic.Int64
			per := b.N / callers
			for c := 0; c < callers; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					for i := 0; i < per; i++ {
						from := keys[(c+i)%numWallets]
						if err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
							if _, err := tx.AtomicAdd(ctx, from, -1); err != nil {
								return err
							}
							_, err := tx.AtomicAdd(ctx, keys[(c+i+1)%numWallets], 1)
							return err
						}); err != nil {
							b.Error(err)
							return
						}
						ops.Add(1)
					}
				}(c)
			}
			wg.Wait()
			b.ReportMetric(float64(ops.Load())/b.Elapsed().Seconds(), "tx/s")
		})
	}
}

// BenchmarkConflictRate measures how often a chained transfer pattern
// (op i: wallet i -> wallet i+1) has to retry. Adjacent pairs share a wallet,
// so the credit leg of one op conflicts with the debit leg of the next; this
// is a property of the workload, not of the store.
func BenchmarkConflictRate(b *testing.B) {
	for _, wallets := range []int{100, 1000} {
		wallets := wallets
		b.Run(fmt.Sprintf("wallets=%d", wallets), func(b *testing.B) {
			m := &dpx.Metrics{}
			n, err := dpx.Open(dpx.Config{
				Engine:     memory.NewMVCCSharded(),
				SyncPolicy: dpx.SyncNone,
				Metrics:    m,
			}, dpxraft.Open)
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer n.Close()

			ctx := context.Background()
			keys := make([][]byte, wallets)
			seedKeys(b, n, keys, "cr")

			var counter atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := int(counter.Add(1) - 1)
					from, to := keys[idx%wallets], keys[(idx+1)%wallets]
					if err := n.RunInTx(ctx, func(tx dpx.KVTx) error {
						if _, err := tx.AtomicAdd(ctx, from, -1); err != nil {
							return err
						}
						_, err := tx.AtomicAdd(ctx, to, 1)
						return err
					}); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(m.ConflictTotal.Load())/float64(b.N), "conflicts/tx")
		})
	}
}
