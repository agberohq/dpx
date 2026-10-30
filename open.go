package dpx

//
// These are the only entry points Teller (and any other consumer) needs.
// No import of conductor, engine/memory, engine/pebble, or shared required.

import (
	"github.com/agberohq/dpx/conductor"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/engine/pebble"
)

// OpenEmbedded opens a single-shard in-memory DPX node.
// Intended for development, testing, and single-process deployments where
// durability is not required. Data is lost on Close().
//
// Example:
//
//	node, err := dpx.OpenEmbedded(dpx.Config{})
func OpenEmbedded(cfg Config) (*Node, error) {
	cfg.Engine = memory.New()
	return Open(cfg, conductor.Open)
}

// OpenSharded opens a 64-shard in-memory DPX node.
// Provides higher write throughput than OpenEmbedded by parallelising
// conflict detection across 64 independent shards. Data is lost on Close().
//
// Use for high-concurrency embedded workloads (e.g. Teller production with
// the memory engine).
//
// Example:
//
//	node, err := dpx.OpenSharded(dpx.Config{})
func OpenSharded(cfg Config) (*Node, error) {
	cfg.Engine = memory.NewSharded()
	return Open(cfg, conductor.Open)
}

// OpenMVCCSharded opens a 64-shard MVCC in-memory DPX node.
// MVCC (Multi-Version Concurrency Control) eliminates COW tree cloning by
// storing versioned entries in hash maps. Snapshot creation is O(1) — just
// reading a sequence counter — with no allocations.
//
// Commit protocol:
// Each ApplyBatch assigns a single commit sequence for all keys in the batch.
// All entries share the same sequence, so a snapshot can never see only part
// of a transaction — atomic visibility is guaranteed. A lastCommitted counter
// ensures snapshots only see fully-written batches, preventing snapshot isolation
// violations under concurrent writes.
//
// Compaction:
// A background goroutine runs every 100ms to reclaim old versions. Compaction
// respects active snapshots: it never removes versions that an open snapshot
// might still read. Deleted keys (tombstones) are cleaned up when no snapshot
// references them.
//
// Performance (vs B-tree sharded):
//   - Throughput: ~670k TPS vs ~170k TPS (4x faster)
//   - Snapshot creation: O(1) sequence read vs COW btree clone
//   - Write allocation: 8x less memory per transaction
//   - Scaling: flat performance regardless of key count
//
// Use for maximum throughput embedded workloads. Data is lost on Close().
//
// Example:
//
//	node, err := dpx.OpenMVCCSharded(dpx.Config{})
func OpenMVCCSharded(cfg Config) (*Node, error) {
	cfg.Engine = memory.NewMVCCSharded()
	return Open(cfg, conductor.Open)
}

// OpenPebble opens a Pebble-backed DPX node.
// Data is durable across restarts. cfg.DataDir must be set to the directory
// where Pebble will store its SST files and WAL.
//
// Example:
//
//	node, err := dpx.OpenPebble(dpx.Config{DataDir: "/var/lib/teller/dpx"})
func OpenPebble(cfg Config) (*Node, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "/tmp/dpx-pebble"
	}
	cfg.Engine = pebble.New(cfg.DataDir)
	return Open(cfg, conductor.Open)
}
