package memory

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/agberohq/dpx/engine"
)

// TestSnapshot_ShardedConsistentCut: a batch spanning shards must be all or
// nothing to every snapshot. The writer keeps two keys in different shards
// equal, so a snapshot that loads one shard before a commit and the other
// after sees them differ.
func TestSnapshot_ShardedConsistentCut(t *testing.T) {
	e := NewSharded()
	t.Cleanup(func() { _ = e.Close() })

	a := []byte("cut:a")
	var b []byte
	for i := 0; ; i++ {
		b = []byte(fmt.Sprintf("cut:b%d", i))
		if shardFor(string(b)) != shardFor(string(a)) {
			break
		}
	}
	enc := func(v uint64) []byte {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, v)
		return buf
	}
	read := func(s engine.Snapshot, k []byte) uint64 {
		v, err := s.Get(k)
		if err != nil {
			return 0
		}
		return binary.LittleEndian.Uint64(v)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := uint64(1); !stop.Load(); v++ {
			batch := e.NewBatch()
			batch.Set(a, enc(v))
			batch.Set(b, enc(v))
			if err := e.ApplyBatch(batch, engine.WriteOptions{}); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	var torn int
	for i := 0; i < 20_000; i++ {
		s, err := e.GetSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		if va, vb := read(s, a), read(s, b); va != vb {
			torn++
		}
		_ = s.Close()
	}
	stop.Store(true)
	wg.Wait()
	if torn != 0 {
		t.Fatalf("%d of 20000 snapshots saw a half-applied batch", torn)
	}
}
