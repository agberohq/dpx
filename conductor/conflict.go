package conductor

import (
	"sync"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
)

// numConflictStripes is the number of independent conflict maps. More stripes
// mean fewer unrelated proposals serialize on a hash collision.
const numConflictStripes = 16384

// conflictStripeFor maps a key to its stripe. FNV-1a, same hash the memory
// engine uses for its data shards, masked to the stripe count.
func conflictStripeFor(key []byte) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	h := uint32(offset32)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return h & (numConflictStripes - 1)
}

func conflictStripeForString(key string) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	h := uint32(offset32)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return h & (numConflictStripes - 1)
}

// conflictStripe owns the epoch entries for the keys that hash to it.
// The map is guarded by mu; one map per stripe (not one shared map) is what
// makes concurrent writes to unrelated keys legal in Go.
type conflictStripe struct {
	mu     sync.Mutex
	epochs map[string]engine.EpochRecord
}

// conflictTable holds the epoch record for every key, partitioned into stripes
// so concurrent writes to unrelated keys do not serialize.
//
// Locking rule: a proposal locks the stripes for the union of its read and
// write keys, in ascending stripe order. Any writer of a key locks that key's
// stripe, so holding the stripe for the whole check-then-apply window is
// sufficient: no other proposal can change a key this proposal observed.
// Ascending order makes the multi-stripe acquisition deadlock-free.
//
// Epoch values still come from one global counter, so epoch records remain
// comparable across stripes (a key never moves between stripes).
type conflictTable struct {
	stripes [numConflictStripes]conflictStripe
}

func newConflictTable() *conflictTable {
	// Stripe maps are allocated lazily; most stripes never see a key.
	return &conflictTable{}
}

// lockFor computes the sorted, de-duplicated stripe set for a proposal.
// The returned slice is owned by the caller (pass a reusable buffer to avoid
// allocating on the hot path).
func (t *conflictTable) lockFor(p *shared.Proposal, buf []uint32) []uint32 {
	buf = buf[:0]
	for _, re := range p.ReadSet {
		buf = append(buf, conflictStripeFor(re.Key))
	}
	for _, w := range p.Writes {
		buf = append(buf, conflictStripeFor(w.Key))
	}
	return t.dedupSort(buf)
}

func (t *conflictTable) dedupSort(ids []uint32) []uint32 {
	if len(ids) < 2 {
		return ids
	}
	// Insertion sort: proposals touch a handful of keys, and this avoids the
	// closure and interface overhead of sort.Slice in the commit path.
	for i := 1; i < len(ids); i++ {
		v := ids[i]
		j := i - 1
		for j >= 0 && ids[j] > v {
			ids[j+1] = ids[j]
			j--
		}
		ids[j+1] = v
	}
	out := ids[:1]
	for _, v := range ids[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func (t *conflictTable) lock(ids []uint32) {
	for _, id := range ids {
		t.stripes[id].mu.Lock()
	}
}

func (t *conflictTable) unlock(ids []uint32) {
	for i := len(ids) - 1; i >= 0; i-- {
		t.stripes[ids[i]].mu.Unlock()
	}
}

// get returns the recorded epoch for key within the given stripe.
// The caller must hold the stripe lock.
func (t *conflictTable) get(stripe uint32, key string) engine.EpochRecord {
	return t.stripes[stripe].epochs[key]
}

// set records the epoch for key. The caller must hold the stripe lock.
func (t *conflictTable) set(stripe uint32, key string, er engine.EpochRecord) {
	s := &t.stripes[stripe]
	if s.epochs == nil {
		s.epochs = make(map[string]engine.EpochRecord, 64)
	}
	s.epochs[key] = er
}

// hydrate fills the table from a key→epoch snapshot (the engine's version
// records at startup). Must be called before the table is shared.
func (t *conflictTable) hydrate(from map[string]engine.EpochRecord) {
	for k, v := range from {
		id := conflictStripeForString(k)
		s := &t.stripes[id]
		if s.epochs == nil {
			s.epochs = make(map[string]engine.EpochRecord, 64)
		}
		s.epochs[k] = v
	}
}

// size reports the number of tracked keys. Test/diagnostic helper.
func (t *conflictTable) size() int {
	n := 0
	for i := range t.stripes {
		t.stripes[i].mu.Lock()
		n += len(t.stripes[i].epochs)
		t.stripes[i].mu.Unlock()
	}
	return n
}
