package conductor

import (
	"errors"
	"runtime"
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
	shards [numShards]*shardProposer
	closed atomic.Bool
}

// numShards must match engine/memory.numShards.
const numShards = 64

// shardProposer is a single shard's inline proposer.
type shardProposer struct {
	fsm      *dpxFSM
	index    uint64
	mu       atomic.Uint32 // used as a spinlock for serial access
	shardIdx int           // shard index for routing
}

func newShardedDirectProposer(
	eng engine.StorageEngine,
	syncPolicy shared.SyncPolicy,
	w shared.WatchNotifier,
	metrics *shared.Metrics,
	telemetry *shared.Telemetry,
) (*shardedDirectProposer, error) {
	s := &shardedDirectProposer{}
	for i := range s.shards {
		clock := hlc.NewClock()
		f := newFSM(eng, syncPolicy, w, metrics, clock)
		f.skipHLC = true
		if _, err := f.open(nil); err != nil {
			return nil, err
		}
		s.shards[i] = &shardProposer{
			fsm:      f,
			shardIdx: i,
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
	// CAS-based spinlock: 0 = unlocked, 1 = locked
	for !sp.mu.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
	defer sp.mu.Store(0)

	sp.index++
	idx := sp.index

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
		writes := make([]memory.MVCCWriteOp, 0, len(p.Writes)+2) // +2 for version keys and applied

		for _, w := range p.Writes {
			isCredit := w.Op == shared.OpCredit
			switch w.Op {
			case shared.OpSet:
				writes = append(writes, memory.MVCCWriteOp{Key: string(w.Key), Value: w.Value, Op: 's'})
			case shared.OpDelete:
				writes = append(writes, memory.MVCCWriteOp{Key: string(w.Key), Op: 'd'})
			case shared.OpCredit, shared.OpDebit:
				writes = append(writes, memory.MVCCWriteOp{Key: string(w.Key), Value: w.Value, Op: 'm'})
			}
			er := engine.EpochRecord{Epoch: idx, IsCredit: isCredit}
			writes = append(writes, memory.MVCCWriteOp{
				Key:   string(versionKey(vkBuf, w.Key)),
				Value: encodeEpochRecord(er, epochBuf[:]),
				Op:    's',
			})
			sp.fsm.state.keyEpoch[string(w.Key)] = er
		}
		writes = append(writes, memory.MVCCWriteOp{
			Key:   string(appliedKey),
			Value: encodeUint64(idx, epochBuf[:8]),
			Op:    's',
		})

		commitSeq := mvccEng.ApplyShardDirect(sp.shardIdx, writes)
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

		sp.fsm.applied.Store(sp.index)
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
