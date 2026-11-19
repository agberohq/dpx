package conductor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/shared"
)

func proposalFor(reads, writes []string) *shared.Proposal {
	p := &shared.Proposal{}
	for _, k := range reads {
		p.ReadSet = append(p.ReadSet, shared.ReadEntry{Key: []byte(k)})
	}
	for _, k := range writes {
		p.Writes = append(p.Writes, shared.WriteEntry{Key: []byte(k)})
	}
	return p
}

func TestConflictStripeForIsStableAndInRange(t *testing.T) {
	keys := []string{"wallet:1", "wallet:2", "", "bench:ts:rec:1000:7"}
	for _, k := range keys {
		a := conflictStripeFor([]byte(k))
		b := conflictStripeForString(k)
		if a != b {
			t.Fatalf("byte and string hashes disagree for %q: %d vs %d", k, a, b)
		}
		if int(a) >= numConflictStripes {
			t.Fatalf("stripe %d out of range for %q", a, k)
		}
		if again := conflictStripeFor([]byte(k)); again != a {
			t.Fatalf("hash is not deterministic for %q", k)
		}
	}
}

func TestLockForDedupesAndSorts(t *testing.T) {
	tbl := newConflictTable()
	// Repeating a key must not produce a duplicate stripe, otherwise a
	// proposal would deadlock trying to lock the same stripe twice.
	p := proposalFor([]string{"a", "a", "b"}, []string{"a", "c", "c"})

	var buf [32]uint32
	ids := tbl.lockFor(p, buf[:0])
	if len(ids) == 0 {
		t.Fatal("no stripes")
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			t.Fatalf("stripes not strictly ascending: %v", ids)
		}
	}
	// A buffered call must return the same set.
	ids2 := tbl.lockFor(p, make([]uint32, 0, 8))
	if len(ids) != len(ids2) {
		t.Fatalf("unstable stripe count: %d vs %d", len(ids), len(ids2))
	}
	for i := range ids {
		if ids[i] != ids2[i] {
			t.Fatalf("unstable stripes: %v vs %v", ids, ids2)
		}
	}
}

func TestConflictTableEpochRoundTrip(t *testing.T) {
	tbl := newConflictTable()
	key := "wallet:abc"
	id := conflictStripeForString(key)

	if got := tbl.get(id, key); got != (engine.EpochRecord{}) {
		t.Fatalf("empty table returned %+v", got)
	}

	tbl.set(id, key, engine.EpochRecord{Epoch: 7, IsCredit: true})
	got := tbl.get(id, key)
	if got.Epoch != 7 || !got.IsCredit {
		t.Fatalf("get = %+v, want {7 true}", got)
	}
}

// TestConflictTableDisjointKeysDoNotBlock checks that holding one key's stripe
// does not block a proposal touching only a different key.
func TestConflictTableDisjointKeysDoNotBlock(t *testing.T) {
	tbl := newConflictTable()

	// Find two keys in different stripes.
	var ka, kb string
	var ida uint32
	for i := 0; i < 4096; i++ {
		ka = fmt.Sprintf("wallet:%d", i)
		ida = conflictStripeForString(ka)
		kb = fmt.Sprintf("wallet:%d", i+1)
		if conflictStripeForString(kb) != ida {
			break
		}
	}
	idb := conflictStripeForString(kb)
	if ida == idb {
		t.Fatal("could not find keys in different stripes")
	}

	tbl.lock([]uint32{ida})
	defer tbl.unlock([]uint32{ida})

	acquired := make(chan struct{})
	go func() {
		tbl.lock([]uint32{idb})
		close(acquired)
		tbl.unlock([]uint32{idb})
	}()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("proposal touching a disjoint key blocked on another stripe")
	}
}

// TestConflictTableSharedKeySerializes is the correctness counterpart: two
// proposals that touch the same key must not run concurrently.
func TestConflictTableSharedKeySerializes(t *testing.T) {
	tbl := newConflictTable()
	key := "wallet:contended"
	id := conflictStripeForString(key)

	tbl.lock([]uint32{id})
	acquired := make(chan struct{})
	go func() {
		tbl.lock([]uint32{id})
		close(acquired)
		tbl.unlock([]uint32{id})
	}()

	select {
	case <-acquired:
		t.Fatal("acquired a stripe that was already held")
	case <-time.After(50 * time.Millisecond):
	}
	tbl.unlock([]uint32{id})

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("stripe was not released")
	}
}

// TestConflictTableConcurrentKeys checks under the race detector that disjoint
// keys can be written in parallel without a data race on a shared map.
func TestConflictTableConcurrentKeys(t *testing.T) {
	tbl := newConflictTable()
	const workers = 8
	const perWorker = 500

	var wg sync.WaitGroup
	var conflicts atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var buf [16]uint32
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("w%d:key%d", w, i)
				p := proposalFor(nil, []string{key})
				ids := tbl.lockFor(p, buf[:0])
				tbl.lock(ids)
				if c := tbl.get(conflictStripeForString(key), key); c.Epoch > 0 {
					conflicts.Add(1)
				}
				tbl.set(conflictStripeForString(key), key, engine.EpochRecord{Epoch: uint64(i + 1)})
				tbl.unlock(ids)
			}
		}(w)
	}
	wg.Wait()

	if got := tbl.size(); got != workers*perWorker {
		t.Fatalf("tracked keys = %d, want %d", got, workers*perWorker)
	}
	if conflicts.Load() != 0 {
		t.Fatalf("unexpected conflicts between disjoint keys: %d", conflicts.Load())
	}
}

func TestConflictTableHydrate(t *testing.T) {
	tbl := newConflictTable()
	tbl.hydrate(map[string]engine.EpochRecord{
		"a": {Epoch: 1},
		"b": {Epoch: 2, IsCredit: true},
	})
	if got := tbl.get(conflictStripeForString("a"), "a"); got.Epoch != 1 {
		t.Fatalf("a = %+v", got)
	}
	if got := tbl.get(conflictStripeForString("b"), "b"); got.Epoch != 2 || !got.IsCredit {
		t.Fatalf("b = %+v", got)
	}
	if tbl.size() != 2 {
		t.Fatalf("size = %d", tbl.size())
	}
}

// TestApplySortedDirectKeepsVersionRecordsDistinct checks that each version
// record keeps its own value; the engine stores values by reference.
func TestApplySortedDirectKeepsVersionRecordsDistinct(t *testing.T) {
	eng := memory.NewMVCCSharded()
	if err := eng.Open(); err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	const epoch = 7
	k1, k2 := []byte("alpha"), []byte("beta")
	vk1, vk2 := string(versionKey(nil, k1)), string(versionKey(nil, k2))

	// One version record per key, with distinct flags (alpha credit, beta set).
	verBuf := make([]byte, 18)
	ops := []memory.MVCCWriteOp{
		{Key: vk1, Value: encodeEpochRecord(engine.EpochRecord{Epoch: epoch, IsCredit: true}, verBuf[0:9]),
			Op: 's', Shard: memory.EngineShardFor(vk1)},
		{Key: vk2, Value: encodeEpochRecord(engine.EpochRecord{Epoch: epoch, IsCredit: false}, verBuf[9:18]),
			Op: 's', Shard: memory.EngineShardFor(vk2)},
	}
	sortOpsByShard(ops)
	eng.ApplySortedDirect(ops)

	snap, err := eng.GetSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	got1, err := snap.GetVersion(k1)
	if err != nil {
		t.Fatal(err)
	}
	got2, err := snap.GetVersion(k2)
	if err != nil {
		t.Fatal(err)
	}
	if got1.Epoch != epoch || !got1.IsCredit {
		t.Fatalf("alpha version = %+v, want {%d true}", got1, epoch)
	}
	if got2.Epoch != epoch || got2.IsCredit {
		t.Fatalf("beta version = %+v, want {%d false}", got2, epoch)
	}
}
