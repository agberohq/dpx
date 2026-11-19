package memory

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMVCCConcurrentCommitsAreAtomicallyVisible checks that a commit spanning
// two shards is never observed half-applied. Both keys are written with the
// same value, so any snapshot seeing different values saw a torn commit.
func TestMVCCConcurrentCommitsAreAtomicallyVisible(t *testing.T) {
	k1, k2 := []byte("mvcc:a"), []byte("mvcc:b")
	if EngineShardFor(string(k1)) == EngineShardFor(string(k2)) {
		t.Skip("keys hash to the same shard; test needs two shards")
	}

	e := NewMVCCSharded()

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Writers: each commit writes both keys with the same value.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var n uint64
			for !stop.Load() {
				n++
				v := encodeInt64MVCC(int64(n)*1000 + int64(w))
				e.ApplyShardsDirect(map[int][]MVCCWriteOp{
					EngineShardFor(string(k1)): {{Key: string(k1), Value: v, Op: 's'}},
					EngineShardFor(string(k2)): {{Key: string(k2), Value: v, Op: 's'}},
				})
			}
		}(w)
	}

	// Readers: both keys must always come from the same commit.
	var torn atomic.Int64
	var reads atomic.Int64
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap, err := e.GetSnapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		v1, err1 := snap.Get(k1)
		v2, err2 := snap.Get(k2)
		snap.Close()
		if err1 != nil || err2 != nil {
			// Nothing committed yet, or this snapshot predates the first write.
			continue
		}
		reads.Add(1)
		if len(v1) != 8 || len(v2) != 8 {
			t.Fatalf("unexpected value lengths: %d %d", len(v1), len(v2))
		}
		if string(v1) != string(v2) {
			torn.Add(1)
		}
	}

	stop.Store(true)
	wg.Wait()

	if reads.Load() == 0 {
		t.Fatal("no snapshots observed a committed value")
	}
	if torn.Load() != 0 {
		t.Fatalf("observed %d torn snapshots out of %d reads", torn.Load(), reads.Load())
	}
}

// TestCommitTrackerAdvancesOnlyContiguously checks the tracker itself: a range
// that completes after a later one must not let the later one publish first.
func TestCommitTrackerAdvancesOnlyContiguously(t *testing.T) {
	var counter atomic.Uint64
	tr := newCommitTracker()

	s1, e1 := tr.reserve(&counter, 2) // [1,2]
	s2, e2 := tr.reserve(&counter, 3) // [3,5]
	if s1 != 1 || e1 != 2 || s2 != 3 || e2 != 5 {
		t.Fatalf("unexpected ranges: [%d,%d] [%d,%d]", s1, e1, s2, e2)
	}

	// The later range completes first: nothing may be published past 0.
	tr.complete(s2, e2)
	if got := tr.published.Load(); got != 0 {
		t.Fatalf("published = %d after out-of-order completion, want 0", got)
	}

	// The first range completes: both ranges publish together.
	tr.complete(s1, e1)
	if got := tr.published.Load(); got != 5 {
		t.Fatalf("published = %d, want 5", got)
	}

	// And a fresh range publishes immediately.
	tr.reset(5)
	s3, e3 := tr.reserve(&counter, 1)
	tr.complete(s3, e3)
	if got := tr.published.Load(); got != e3 {
		t.Fatalf("published = %d, want %d", got, e3)
	}
}
