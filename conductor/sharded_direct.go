package conductor

import (
	"errors"
	"sync/atomic"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
)

var errShardedClosed = errors.New("dpx/raft: sharded proposer closed")

// shardedDirectProposer creates N independent directProposers, one per shard.
// Proposals are routed to the correct shard based on the first write key.
// All keys in a proposal must belong to the same shard — caller's responsibility.
//
// This enables 32× parallelism for single-node embedded mode: each shard has
// its own FSM, applierLoop goroutine, and engine shard. Concurrent proposals
// to different shards execute in parallel with zero cross-shard contention.
type shardedDirectProposer struct {
	shards [numShards]*directProposer
	closed atomic.Bool
}

// numShards must match engine/memory.numShards.
const numShards = 64

func newShardedDirectProposer(
	eng engine.StorageEngine,
	syncPolicy shared.SyncPolicy,
	w shared.WatchNotifier,
	metrics *shared.Metrics,
	telemetry *shared.Telemetry,
) (*shardedDirectProposer, error) {
	s := &shardedDirectProposer{}
	for i := range s.shards {
		dp, err := newDirectProposerWithInterval(eng, syncPolicy, w, metrics, telemetry, shardedFlushInterval)
		if err != nil {
			for j := 0; j < i; j++ {
				s.shards[j].Shutdown()
			}
			return nil, err
		}
		s.shards[i] = dp
	}
	return s, nil
}

// shardFor returns the shard index for a proposal.
// Uses the first write key; falls back to first read key for read-only proposals.
func (s *shardedDirectProposer) shardFor(p *shared.Proposal) int {
	if len(p.Writes) > 0 {
		return shardFor(string(p.Writes[0].Key))
	}
	if len(p.ReadSet) > 0 {
		return shardFor(string(p.ReadSet[0].Key))
	}
	return 0
}

// ProposeDirect routes the proposal to the correct shard and skips msgpack.
func (s *shardedDirectProposer) ProposeDirect(p *shared.Proposal) (shared.ApplyResult, error) {
	if s.closed.Load() {
		return shared.ApplyResult{}, errShardedClosed
	}
	sh := s.shardFor(p)
	return s.shards[sh].ProposeDirect(p)
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

// Shutdown stops all shard proposers.
func (s *shardedDirectProposer) Shutdown() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	var firstErr error
	for _, sh := range s.shards {
		if err := sh.Shutdown(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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
