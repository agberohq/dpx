package memory

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tidwall/btree"
)

// BenchmarkBTree_Ops measures individual tidwall/btree operations in isolation.
func BenchmarkBTree_Ops(b *testing.B) {
	// Pre-populate with 100K keys
	tree := &state{tree: btree.NewBTreeG(itemLess)}
	for i := 0; i < 100_000; i++ {
		key := makeKey(i)
		tree.tree.Set(item{key: key, value: []byte{1, 2, 3, 4, 5, 6, 7, 8}})
	}

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := makeKey(i % 100_000)
			tree.tree.Get(item{key: key})
		}
	})

	b.Run("Set", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := makeKey(i % 100_000)
			tree.tree.Set(item{key: key, value: []byte{1, 2, 3, 4, 5, 6, 7, 8}})
		}
	})

	b.Run("Copy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tree.tree.Copy()
		}
	})

	b.Run("Copy+Set", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			copied := tree.tree.Copy()
			key := makeKey(i % 100_000)
			copied.Set(item{key: key, value: []byte{1, 2, 3, 4, 5, 6, 7, 8}})
		}
	})

	b.Run("Copy+Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			copied := tree.tree.Copy()
			key := makeKey(i % 100_000)
			copied.Get(item{key: key})
		}
	})
}

// BenchmarkHashMap_Ops measures sync.Map operations for comparison.
func BenchmarkHashMap_Ops(b *testing.B) {
	// Pre-populate with 100K keys
	m := &sync.Map{}
	for i := 0; i < 100_000; i++ {
		key := makeKey(i)
		m.Store(key, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	}

	b.Run("Load", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := makeKey(i % 100_000)
			m.Load(key)
		}
	})

	b.Run("Store", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := makeKey(i % 100_000)
			m.Store(key, []byte{1, 2, 3, 4, 5, 6, 7, 8})
		}
	})
}

// BenchmarkAtomicMap_Ops measures atomic.Pointer-based map swap for comparison.
func BenchmarkAtomicMap_Ops(b *testing.B) {
	// Pre-populate with 100K keys using atomic map swap pattern
	type entry struct {
		value []byte
	}
	type version struct {
		data map[string]*entry
	}

	cur := &version{data: make(map[string]*entry, 100_000)}
	for i := 0; i < 100_000; i++ {
		key := makeKey(i)
		cur.data[key] = &entry{value: []byte{1, 2, 3, 4, 5, 6, 7, 8}}
	}

	var ptr atomic.Pointer[version]
	ptr.Store(cur)

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v := ptr.Load()
			key := makeKey(i % 100_000)
			e, _ := v.data[key]
			_ = e
		}
	})

	b.Run("Clone+Set", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v := ptr.Load()
			newData := make(map[string]*entry, len(v.data))
			for k, e := range v.data {
				newData[k] = e
			}
			key := makeKey(i % 100_000)
			newData[key] = &entry{value: []byte{1, 2, 3, 4, 5, 6, 7, 8}}
			ptr.Store(&version{data: newData})
		}
	})
}

// BenchmarkTransactionPath simulates the full transaction path: snapshot + read + write + commit.
func BenchmarkTransactionPath(b *testing.B) {
	// Pre-populate with 10K keys (simulating 10K wallets)
	tree := &state{tree: btree.NewBTreeG(itemLess)}
	for i := 0; i < 10_000; i++ {
		key := makeKey(i)
		tree.tree.Set(item{key: key, value: encodeInt64(100_000)})
		verKey := "__dpx:ver:" + key
		tree.tree.Set(item{key: verKey, value: encodeEpoch(0, false)})
	}

	b.Run("BTree_snapshot+2reads+2writes", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Snapshot (COW clone)
			snap := tree.clone()

			// Read 2 keys (debit source + credit dest)
			src := makeKey(i % 10_000)
			dst := makeKey((i + 1) % 10_000)
			snap.tree.Get(item{key: src})
			snap.tree.Get(item{key: dst})

			// Read 2 version keys
			snap.tree.Get(item{key: "__dpx:ver:" + src})
			snap.tree.Get(item{key: "__dpx:ver:" + dst})

			// Write 2 keys (balance updates)
			snap.tree.Set(item{key: src, value: encodeInt64(99_900)})
			snap.tree.Set(item{key: dst, value: encodeInt64(100_100)})

			// Write 2 version keys
			snap.tree.Set(item{key: "__dpx:ver:" + src, value: encodeEpoch(uint64(i), false)})
			snap.tree.Set(item{key: "__dpx:ver:" + dst, value: encodeEpoch(uint64(i), true)})
		}
	})
}

// BenchmarkHashIndex compares hash map with ordered index for range scans.
func BenchmarkHashIndex(b *testing.B) {
	// Pre-populate with 100K keys
	data := make(map[string][]byte, 100_000)
	for i := 0; i < 100_000; i++ {
		key := makeKey(i)
		data[key] = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	}

	b.Run("PointLookup", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := makeKey(i % 100_000)
			_ = data[key]
		}
	})

	b.Run("PointLookup+Update", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := makeKey(i % 100_000)
			data[key] = []byte{1, 2, 3, 4, 5, 6, 7, 8}
		}
	})
}

func makeKey(i int) string {
	// Simulate wallet keys like "wallet:12345"
	return fmt.Sprintf("wallet:%06d", i)
}

func encodeEpoch(epoch uint64, isCredit bool) []byte {
	b := make([]byte, 9)
	// Store epoch as little-endian uint64
	for j := 0; j < 8; j++ {
		b[j] = byte(epoch >> (j * 8))
	}
	if isCredit {
		b[8] = 1
	}
	return b
}
