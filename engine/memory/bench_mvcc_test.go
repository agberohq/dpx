package memory

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tidwall/btree"
)

// BenchmarkMVCC_vs_BTree compares the MVCC engine against the B-tree engine
// using the same transaction patterns as DPX workspace runner.
func BenchmarkMVCC_vs_BTree(b *testing.B) {
	// Pre-populate with 10K keys (simulating 10K wallets)
	b.Run("Snapshot_creation", func(b *testing.B) {
		b.Run("BTree", func(b *testing.B) {
			tree := &state{tree: btree.NewBTreeG(itemLess)}
			for i := 0; i < 10_000; i++ {
				key := fmt.Sprintf("wallet:%06d", i)
				tree.tree.Set(item{key: key, value: encodeInt64(100_000)})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tree.clone()
			}
		})

		b.Run("MVCC", func(b *testing.B) {
			e := NewMVCC()
			for i := 0; i < 10_000; i++ {
				key := []byte(fmt.Sprintf("wallet:%06d", i))
				batch := e.NewBatch()
				batch.Set(key, encodeInt64MVCC(100_000))
				e.ApplyBatch(batch, struct{ Sync bool }{Sync: false})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snap, _ := e.GetSnapshot()
				snap.Close()
			}
		})
	})

	b.Run("Point_read", func(b *testing.B) {
		b.Run("BTree", func(b *testing.B) {
			tree := &state{tree: btree.NewBTreeG(itemLess)}
			for i := 0; i < 10_000; i++ {
				key := fmt.Sprintf("wallet:%06d", i)
				tree.tree.Set(item{key: key, value: encodeInt64(100_000)})
			}
			snap := tree.clone()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("wallet:%06d", i%10_000)
				snap.tree.Get(item{key: key})
			}
		})

		b.Run("MVCC", func(b *testing.B) {
			e := NewMVCC()
			for i := 0; i < 10_000; i++ {
				key := []byte(fmt.Sprintf("wallet:%06d", i))
				batch := e.NewBatch()
				batch.Set(key, encodeInt64MVCC(100_000))
				e.ApplyBatch(batch, struct{ Sync bool }{Sync: false})
			}
			snap, _ := e.GetSnapshot()
			defer snap.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("wallet:%06d", i%10_000))
				snap.Get(key)
			}
		})
	})

	b.Run("Point_write", func(b *testing.B) {
		b.Run("BTree_COW", func(b *testing.B) {
			tree := &state{tree: btree.NewBTreeG(itemLess)}
			for i := 0; i < 10_000; i++ {
				key := fmt.Sprintf("wallet:%06d", i)
				tree.tree.Set(item{key: key, value: encodeInt64(100_000)})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snap := tree.clone()
				key := fmt.Sprintf("wallet:%06d", i%10_000)
				snap.tree.Set(item{key: key, value: encodeInt64(99_900)})
			}
		})

		b.Run("MVCC_append", func(b *testing.B) {
			e := NewMVCC()
			for i := 0; i < 10_000; i++ {
				key := []byte(fmt.Sprintf("wallet:%06d", i))
				batch := e.NewBatch()
				batch.Set(key, encodeInt64MVCC(100_000))
				e.ApplyBatch(batch, struct{ Sync bool }{Sync: false})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("wallet:%06d", i%10_000))
				batch := e.NewBatch()
				batch.Set(key, encodeInt64MVCC(99_900))
				e.ApplyBatch(batch, struct{ Sync bool }{Sync: false})
			}
		})
	})

	b.Run("Full_transaction_2reads_2writes", func(b *testing.B) {
		b.Run("BTree", func(b *testing.B) {
			tree := &state{tree: btree.NewBTreeG(itemLess)}
			for i := 0; i < 10_000; i++ {
				key := fmt.Sprintf("wallet:%06d", i)
				tree.tree.Set(item{key: key, value: encodeInt64(100_000)})
				verKey := "__dpx:ver:" + key
				tree.tree.Set(item{key: verKey, value: make([]byte, 9)})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Snapshot (COW clone)
				snap := tree.clone()

				// Read 2 keys
				src := fmt.Sprintf("wallet:%06d", i%10_000)
				dst := fmt.Sprintf("wallet:%06d", (i+1)%10_000)
				snap.tree.Get(item{key: src})
				snap.tree.Get(item{key: dst})

				// Read 2 version keys
				snap.tree.Get(item{key: "__dpx:ver:" + src})
				snap.tree.Get(item{key: "__dpx:ver:" + dst})

				// Write 2 balance keys
				snap.tree.Set(item{key: src, value: encodeInt64(99_900)})
				snap.tree.Set(item{key: dst, value: encodeInt64(100_100)})

				// Write 2 version keys
				snap.tree.Set(item{key: "__dpx:ver:" + src, value: make([]byte, 9)})
				snap.tree.Set(item{key: "__dpx:ver:" + dst, value: make([]byte, 9)})
			}
		})

		b.Run("MVCC", func(b *testing.B) {
			e := NewMVCC()
			for i := 0; i < 10_000; i++ {
				key := []byte(fmt.Sprintf("wallet:%06d", i))
				batch := e.NewBatch()
				batch.Set(key, encodeInt64MVCC(100_000))
				e.ApplyBatch(batch, struct{ Sync bool }{Sync: false})

				verKey := []byte("__dpx:ver:" + string(key))
				verBatch := e.NewBatch()
				verBatch.Set(verKey, make([]byte, 9))
				e.ApplyBatch(verBatch, struct{ Sync bool }{Sync: false})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Snapshot (O(1) sequence read)
				snap, _ := e.GetSnapshot()

				// Read 2 keys
				src := []byte(fmt.Sprintf("wallet:%06d", i%10_000))
				dst := []byte(fmt.Sprintf("wallet:%06d", (i+1)%10_000))
				snap.Get(src)
				snap.Get(dst)

				// Read 2 version keys
				snap.Get([]byte("__dpx:ver:" + string(src)))
				snap.Get([]byte("__dpx:ver:" + string(dst)))

				// Write batch (2 balance + 2 version = 4 writes)
				batch := e.NewBatch()
				batch.Set(src, encodeInt64MVCC(99_900))
				batch.Set(dst, encodeInt64MVCC(100_100))
				batch.Set([]byte("__dpx:ver:"+string(src)), make([]byte, 9))
				batch.Set([]byte("__dpx:ver:"+string(dst)), make([]byte, 9))
				e.ApplyBatch(batch, struct{ Sync bool }{Sync: false})

				snap.Close()
			}
		})
	})
}

// BenchmarkMVCC_scaling measures how MVCC performance scales with key count.
func BenchmarkMVCC_scaling(b *testing.B) {
	for _, keyCount := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("%d_keys", keyCount), func(b *testing.B) {
			e := NewMVCC()
			for i := 0; i < keyCount; i++ {
				key := []byte(fmt.Sprintf("wallet:%08d", i))
				batch := e.NewBatch()
				batch.Set(key, encodeInt64MVCC(100_000))
				e.ApplyBatch(batch, struct{ Sync bool }{Sync: false})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snap, _ := e.GetSnapshot()
				key := []byte(fmt.Sprintf("wallet:%08d", i%keyCount))
				snap.Get(key)
				batch := e.NewBatch()
				batch.Set(key, encodeInt64MVCC(99_900))
				e.ApplyBatch(batch, struct{ Sync bool }{Sync: false})
				snap.Close()
			}
		})
	}
}

// BenchmarkBTree_scaling measures how B-tree performance scales with key count.
func BenchmarkBTree_scaling(b *testing.B) {
	for _, keyCount := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("%d_keys", keyCount), func(b *testing.B) {
			tree := &state{tree: btree.NewBTreeG(itemLess)}
			for i := 0; i < keyCount; i++ {
				key := fmt.Sprintf("wallet:%08d", i)
				tree.tree.Set(item{key: key, value: encodeInt64(100_000)})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snap := tree.clone()
				key := fmt.Sprintf("wallet:%08d", i%keyCount)
				snap.tree.Get(item{key: key})
				snap.tree.Set(item{key: key, value: encodeInt64(99_900)})
			}
		})
	}
}

func encodeInt64Bench(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}
