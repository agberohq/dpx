package conductor

import (
	"errors"
	"sync/atomic"
	"unsafe"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/shared"
	"github.com/olekukonko/hlc"
)

var errShardedClosed = errors.New("dpx/raft: sharded proposer closed")

// shardedDirectProposer applies proposals inline — no submit channel, no
// applierLoop goroutine, no flush ticker, no result channel allocation.
//
// Concurrency comes from the striped conflict table: a proposal holds the
// stripes for the keys it reads and writes, so proposals over disjoint keys
// commit in parallel.
type shardedDirectProposer struct {
	shards    [numShards]*shardProposer
	closed    atomic.Bool
	conflicts *conflictTable // striped key→epoch table shared by all shards
}

// numShards must match engine/memory.numShards.
// 128 shards balances per-shard contention (16 goroutines/shard)
// against overhead of more FSM instances.
const numShards = 128

// shardProposer is a single shard's inline proposer.
type shardProposer struct {
	fsm       *dpxFSM
	index     *atomic.Uint64 // SHARED across all shards: epochs must be globally monotonic
	shardIdx  int            // shard index for routing
	conflicts *conflictTable
}

func newShardedDirectProposer(
	eng engine.StorageEngine,
	syncPolicy shared.SyncPolicy,
	w shared.WatchNotifier,
	metrics *shared.Metrics,
	telemetry *shared.Telemetry,
) (*shardedDirectProposer, error) {
	s := &shardedDirectProposer{conflicts: newConflictTable()}
	// One epoch counter for every shard. Epoch records stay comparable across
	// stripes only if a single counter issues them; a per-shard counter would
	// let a "low-count" shard stamp a key with a smaller epoch than the stale
	// record, producing permanent false conflicts on retry.
	globalIndex := &atomic.Uint64{}
	for i := range s.shards {
		clock := hlc.NewClock()
		f := newFSM(eng, syncPolicy, w, metrics, clock)
		f.skipHLC = true
		if i == 0 {
			// Hydrate the shared conflict table once from the engine's
			// version records, not once per shard.
			if _, err := f.open(nil); err != nil {
				return nil, err
			}
			s.conflicts.hydrate(f.state.keyEpoch)
		}
		f.state.keyEpoch = nil
		s.shards[i] = &shardProposer{
			fsm:       f,
			index:     globalIndex,
			shardIdx:  i,
			conflicts: s.conflicts,
		}
	}
	return s, nil
}

// shardFor mirrors engine/memory.shardFor for routing consistency.
// Uses FNV-1a hash of the key, masked to numShards-1.
func shardFor(key string) int {
	const offset32 = 2166136261
	const prime32 = 16777619
	h := uint32(offset32)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return int(h) & (numShards - 1)
}

func (s *shardedDirectProposer) shardFor(p *shared.Proposal) int {
	if len(p.Writes) > 0 {
		return shardFor(string(p.Writes[0].Key))
	}
	if len(p.ReadSet) > 0 {
		return shardFor(string(p.ReadSet[0].Key))
	}
	return 0
}

// ProposeDirect routes the proposal to the correct shard and applies inline.
func (s *shardedDirectProposer) ProposeDirect(p *shared.Proposal) (shared.ApplyResult, error) {
	if s.closed.Load() {
		return shared.ApplyResult{}, errShardedClosed
	}
	sh := s.shardFor(p)
	return s.shards[sh].applyInline(p), nil
}

// Propose unmarshals then routes — used by Raft path (should not happen in embedded).
func (s *shardedDirectProposer) Propose(data []byte) (shared.ApplyResult, error) {
	if s.closed.Load() {
		return shared.ApplyResult{}, errShardedClosed
	}
	var p shared.Proposal
	if err := p.Unmarshal(data); err != nil {
		return shared.ApplyResult{}, err
	}
	return s.ProposeDirect(&p)
}

func (s *shardedDirectProposer) Shutdown() error {
	s.closed.CompareAndSwap(false, true)
	return nil
}

// sortOpsByShard orders ops by engine shard so ApplySortedDirect can take one
// lock per contiguous run. Insertion sort: a proposal carries tens of ops, well
// below the size where sort.Slice's closure and interface overhead pays off.
func sortOpsByShard(ops []memory.MVCCWriteOp) {
	for i := 1; i < len(ops); i++ {
		v := ops[i]
		j := i - 1
		for j >= 0 && ops[j].Shard > v.Shard {
			ops[j+1] = ops[j]
			j--
		}
		ops[j+1] = v
	}
}

// applyInline applies a single proposal synchronously on this shard.
//
// The proposal holds the conflict stripes for every key it reads or writes for
// the whole check-then-apply window. Proposals with disjoint keys run in
// parallel; proposals sharing a key serialize on that key's stripe.
func (sp *shardProposer) applyInline(p *shared.Proposal) shared.ApplyResult {
	var lbuf [32]uint32
	lockIDs := sp.conflicts.lockFor(p, lbuf[:0])
	sp.conflicts.lock(lockIDs)
	defer sp.conflicts.unlock(lockIDs)

	idx := sp.index.Add(1)

	// Conflict detection: every read must still be at the epoch it saw.
	// No stack shadow is needed: any proposal that could change a key in this
	// read set would have to hold that key's stripe, which we hold.
	conflict := false
	for _, re := range p.ReadSet {
		k := unsafe.String(unsafe.SliceData(re.Key), len(re.Key))
		if c := sp.conflicts.get(conflictStripeFor(re.Key), k); c.Epoch > re.Epoch && (!c.IsCredit || !re.IsDebit) {
			conflict = true
			break
		}
	}

	if conflict {
		if sp.fsm.metrics != nil {
			sp.fsm.metrics.ConflictTotal.Add(1)
		}
		return shared.ApplyResult{Conflict: true}
	}

	// Apply to engine — use lock-free path for MVCC engine when possible
	if mvccEng, ok := sp.fsm.engine.(*memory.MVCCEngine); ok && mvccEng.IsSharded() {
		// MVCC path: the engine's per-shard locks plus the striped conflict
		// locks above are the only serialization; there is no engine-wide
		// commit mutex, so disjoint proposals apply in parallel.
		vkBuf := sp.fsm.vkBufPool.Get().([]byte)
		defer sp.fsm.vkBufPool.Put(vkBuf)

		// Version records are stored by reference by the engine, so every op
		// needs its own bytes. Reusing one scratch array made every version
		// record in a transaction carry the last key's IsCredit flag.
		verBuf := make([]byte, 9*len(p.Writes))

		opsPtr := sp.fsm.wopPool.Get().(*[]memory.MVCCWriteOp)
		ops := (*opsPtr)[:0]
		defer func() {
			*opsPtr = ops[:0]
			sp.fsm.wopPool.Put(opsPtr)
		}()

		for i, w := range p.Writes {
			isCredit := w.Op == shared.OpCredit
			keyStr := string(w.Key)
			code := byte('s')
			switch w.Op {
			case shared.OpSet, shared.OpSetAppend:
				code = 's'
			case shared.OpDelete:
				code = 'd'
			case shared.OpCredit, shared.OpDebit:
				code = 'm'
			}
			ops = append(ops, memory.MVCCWriteOp{
				Key: keyStr, Value: w.Value, Op: code,
				Shard: memory.EngineShardFor(keyStr),
			})

			er := engine.EpochRecord{Epoch: idx, IsCredit: isCredit}
			// Append-only keys skip the version record (one engine write), but
			// still enter the epoch map: a future Get of the key must conflict
			// against this write rather than miss it.
			if w.Op != shared.OpSetAppend {
				vk := string(versionKey(vkBuf, w.Key))
				seg := verBuf[i*9 : (i+1)*9]
				ops = append(ops, memory.MVCCWriteOp{
					Key: vk, Value: encodeEpochRecord(er, seg), Op: 's',
					Shard: memory.EngineShardFor(vk),
				})
			}
			sp.conflicts.set(conflictStripeFor(w.Key), keyStr, er)
		}
		// No __dpx:applied write on this path. The MVCC engine tracks its commit
		// sequence in seqCounter (CurrentSequence and GetSafeReadPoint both use
		// it), so the marker has no reader here — and writing one shared key from
		// every transaction serialises all appliers on that key's engine shard.

		sortOpsByShard(ops)
		commitSeq := mvccEng.ApplySortedDirect(ops)
		sp.fsm.applied.Store(commitSeq)
	} else {
		// Fallback: standard ApplyBatch path for non-MVCC engines
		engBatch := sp.fsm.engine.NewBatch()
		vkBuf := sp.fsm.vkBufPool.Get().([]byte)
		defer sp.fsm.vkBufPool.Put(vkBuf)

		var epochBuf [9]byte
		var appliedBuf [8]byte

		for _, w := range p.Writes {
			isCredit := w.Op == shared.OpCredit
			switch w.Op {
			case shared.OpSet, shared.OpSetAppend:
				engBatch.Set(w.Key, w.Value)
			case shared.OpDelete:
				engBatch.Delete(w.Key)
			case shared.OpCredit, shared.OpDebit:
				engBatch.Merge(w.Key, w.Value)
			}
			er := engine.EpochRecord{Epoch: idx, IsCredit: isCredit}
			if w.Op != shared.OpSetAppend {
				engBatch.Set(versionKey(vkBuf, w.Key), encodeEpochRecord(er, epochBuf[:]))
			}
			sp.conflicts.set(conflictStripeFor(w.Key), string(w.Key), er)
		}
		engBatch.Set(appliedKey, encodeUint64(idx, appliedBuf[:]))

		wo := engine.WriteOptions{Sync: sp.fsm.syncPolicy == shared.SyncFull}
		if err := sp.fsm.engine.ApplyBatch(engBatch, wo); err != nil {
			return shared.ApplyResult{Err: err}
		}

		sp.fsm.applied.Store(idx)
		if sp.fsm.syncPolicy == shared.SyncBatch {
			sp.fsm.engine.Sync()
		}
	}

	// Notify watchers
	if sp.fsm.watchers != nil {
		sp.fsm.watchers.NotifyBatch(p.Writes, sp.fsm.metrics)
	}

	return shared.ApplyResult{}
}
