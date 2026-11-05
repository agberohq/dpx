package conductor

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/shared"
	"github.com/olekukonko/hlc"
)

var errShardedClosed = errors.New("dpx/raft: sharded proposer closed")

// shardedDirectProposer routes proposals to 64 independent shards.
// Each shard applies proposals inline via applyInline — no submit channel,
// no applierLoop goroutine, no flush ticker, no result channel allocation.
//
// With 2048 goroutines / 64 shards = 32 goroutines/shard, each shard's
// CAS spinlock serializes access. The inline path eliminates all channel
// overhead, reducing per-proposal latency from ~9ms to ~150µs.
type shardedDirectProposer struct {
	shards   [numShards]*shardProposer
	closed   atomic.Bool
	epochMu  sync.Mutex                    // serializes conflict-check + apply across shards
	keyEpoch map[string]engine.EpochRecord // SHARED conflict map (global, not per-proposer)
}

// numShards must match engine/memory.numShards.
// 128 shards balances per-shard contention (16 goroutines/shard)
// against overhead of more FSM instances.
const numShards = 128

// shardProposer is a single shard's inline proposer.
type shardProposer struct {
	fsm      *dpxFSM
	index    *atomic.Uint64 // SHARED across all shards: epochs must be globally monotonic
	mu       atomic.Uint32  // used as a spinlock for serial access
	shardIdx int            // shard index for routing
	owner    *shardedDirectProposer
}

func newShardedDirectProposer(
	eng engine.StorageEngine,
	syncPolicy shared.SyncPolicy,
	w shared.WatchNotifier,
	metrics *shared.Metrics,
	telemetry *shared.Telemetry,
) (*shardedDirectProposer, error) {
	s := &shardedDirectProposer{}
	// One epoch counter for every shard. Version records store this value and
	// detectConflict compares it against keyEpoch; if each shard kept its own
	// counter, a key written by a "low-count" shard after being written by a
	// "high-count" shard would get a SMALLER epoch than the stale keyEpoch,
	// producing permanent false conflicts on retry.
	globalIndex := &atomic.Uint64{}
	s.keyEpoch = make(map[string]engine.EpochRecord, 1<<16)
	for i := range s.shards {
		clock := hlc.NewClock()
		f := newFSM(eng, syncPolicy, w, metrics, clock)
		f.skipHLC = true
		if _, err := f.open(nil); err != nil {
			return nil, err
		}
		// Every shard shares ONE conflict map. A per-shard map makes writes
		// applied by another shard invisible, so cross-shard conflicts are
		// never detected (lost updates, forked chains).
		f.state.keyEpoch = s.keyEpoch
		s.shards[i] = &shardProposer{
			fsm:      f,
			index:    globalIndex,
			shardIdx: i,
			owner:    s,
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

// applyInline applies a single proposal synchronously on this shard.
// Uses CAS-based locking to serialize access without a mutex.
func (sp *shardProposer) applyInline(p *shared.Proposal) shared.ApplyResult {
	// CAS-based spinlock with exponential backoff.
	// Tight spin for first few attempts (lock holder likely finishing soon),
	// then yield to avoid wasting CPU when contention is high.
	retries := 0
	for !sp.mu.CompareAndSwap(0, 1) {
		if retries < 4 {
			retries++
		} else {
			runtime.Gosched()
			retries = 0
		}
	}
	defer sp.mu.Store(0)

	// Conflict detection and application must be atomic with respect to ALL
	// shards, because the conflict map is global: another shard may write a
	// key this proposal read. Without this lock two proposals can both observe
	// an absent key and both create it (check-then-create race).
	owner := sp.owner
	if owner != nil {
		owner.epochMu.Lock()
		defer owner.epochMu.Unlock()
	}

	idx := sp.index.Add(1)

	// Conflict detection
	conflict := false
	if len(p.ReadSet) <= maxStackShadow {
		var stackShadow [maxStackShadow]shadowEntry
		shadowLen := 0

		for _, re := range p.ReadSet {
			k := unsafe.String(unsafe.SliceData(re.Key), len(re.Key))
			if c := sp.fsm.state.keyEpoch[k]; c.Epoch > re.Epoch {
				if !(c.IsCredit && re.IsDebit) {
					conflict = true
					break
				}
			}
			for j := 0; j < shadowLen; j++ {
				if stackShadow[j].key == k {
					if !(stackShadow[j].epoch.IsCredit && re.IsDebit) {
						conflict = true
					}
					break
				}
			}
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
		// MVCC lock-free path: skip RWMutex since CAS spinlock already serializes
		vkBuf := sp.fsm.vkBufPool.Get().([]byte)
		defer sp.fsm.vkBufPool.Put(vkBuf)

		var epochBuf [9]byte
		// Route every write to the ENGINE shard that readers will use for
		// that key. GetVersion looks up "__dpx:ver:<userKey>" in
		// engine/memory.shardFor(versionKey) — the version key MUST land in
		// that same shard, otherwise debit read-sets hydrate epoch 0 while
		// this shard's keyEpoch holds the real epoch, producing permanent
		// ErrConflictExhausted on second debits of the same key. User keys
		// are routed by their own hash; point/range reads use shardFor(key)
		// directly, so user data and version data are both consistent per
		// key. Each key group is applied to its own engine shard under the
		// proposal's single CAS-serialized inline path.
		type shardGroup struct {
			user []memory.MVCCWriteOp
			ver  []memory.MVCCWriteOp
		}
		groups := make(map[int][]memory.MVCCWriteOp, len(p.Writes)+1)
		groupFor := func(sh int) []memory.MVCCWriteOp {
			g, ok := groups[sh]
			if !ok {
				g = make([]memory.MVCCWriteOp, 0, len(p.Writes)+2)
				groups[sh] = g
			}
			return g
		}
		for _, w := range p.Writes {
			isCredit := w.Op == shared.OpCredit
			sh := memory.EngineShardFor(string(w.Key))
			switch w.Op {
			case shared.OpSet:
				groups[sh] = append(groupFor(sh),
					memory.MVCCWriteOp{Key: string(w.Key), Value: w.Value, Op: 's'})
			case shared.OpDelete:
				groups[sh] = append(groupFor(sh),
					memory.MVCCWriteOp{Key: string(w.Key), Op: 'd'})
			case shared.OpCredit, shared.OpDebit:
				groups[sh] = append(groupFor(sh),
					memory.MVCCWriteOp{Key: string(w.Key), Value: w.Value, Op: 'm'})
			}
			er := engine.EpochRecord{Epoch: idx, IsCredit: isCredit}
			vk := string(versionKey(vkBuf, w.Key))
			vsh := memory.EngineShardFor(vk)
			groups[vsh] = append(groupFor(vsh),
				memory.MVCCWriteOp{Key: vk, Value: encodeEpochRecord(er, epochBuf[:]), Op: 's'})
			sp.fsm.state.keyEpoch[string(w.Key)] = er
		}
		{
			vk := string(appliedKey)
			sh := memory.EngineShardFor(vk)
			groups[sh] = append(groupFor(sh), memory.MVCCWriteOp{
				Key:   vk,
				Value: encodeUint64(idx, epochBuf[:8]),
				Op:    's',
			})
		}

		commitSeq := mvccEng.ApplyShardsDirect(groups)
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
			case shared.OpSet:
				engBatch.Set(w.Key, w.Value)
			case shared.OpDelete:
				engBatch.Delete(w.Key)
			case shared.OpCredit, shared.OpDebit:
				engBatch.Merge(w.Key, w.Value)
			}
			er := engine.EpochRecord{Epoch: idx, IsCredit: isCredit}
			engBatch.Set(versionKey(vkBuf, w.Key), encodeEpochRecord(er, epochBuf[:]))
			sp.fsm.state.keyEpoch[string(w.Key)] = er
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
