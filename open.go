package dpx

//
// These are the only entry points Teller (and any other consumer) needs.
// No import of conductor, engine/memory, engine/pebble, or shared required.

import (
	"github.com/agberohq/dpx/conductor"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/engine/oshodi"
	"github.com/agberohq/dpx/engine/pebble"
	oshodidb "github.com/agberohq/oshodi"
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

// OpenSharded opens a 128-shard copy-on-write btree in-memory DPX node.
// Data is lost on Close().
//
// Compatibility engine. Every apply in this engine takes a single commit mutex
// because snapshots are published with a seqlock over one commit counter, so
// throughput does not improve with concurrency: the striped proposer parallelises
// the proposal path, then all writers queue on the engine's commit lock. Prefer
// OpenConcurrent (MVCC) for write-concurrent workloads.
//
// Example:
//
//	node, err := dpx.OpenSharded(dpx.Config{})
func OpenSharded(cfg Config) (*Node, error) {
	cfg.Engine = memory.NewSharded()
	return Open(cfg, conductor.Open)
}

// OpenConcurrent opens the recommended in-memory engine for write-concurrent
// workloads: sharded MVCC.
//
// Transactions that touch disjoint keys commit in parallel — the proposer holds
// only per-key conflict stripes, and the MVCC engine has no engine-wide commit
// mutex: commits reserve a sequence range and publish it atomically once every
// shard is written, so readers never observe a half-applied transaction.
//
// Data is lost on Close().
//
// Example:
//
//	node, err := dpx.OpenConcurrent(dpx.Config{})
func OpenConcurrent(cfg Config) (*Node, error) {
	return OpenMVCCSharded(cfg)
}

// OpenMVCCSharded opens a 128-shard MVCC in-memory DPX node.
// MVCC (Multi-Version Concurrency Control) eliminates COW tree cloning by
// storing versioned entries in hash maps. Snapshot creation is O(1) — just
// reading a sequence counter — with no allocations.
//
// Commit protocol:
// Each batch reserves a contiguous sequence range. All entries share the same
// sequence, so a snapshot can never see only part of a transaction. The range
// is published only once every shard has been written, so concurrent appliers
// cannot expose a half-written batch.
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

// OpenOshodi opens an Oshodi-backed DPX node.
// Oshodi is a high-performance embedded KV store with buckets, pub/sub,
// and graph traversals. Data is durable across restarts. cfg.DataDir must
// be set to the directory where Oshodi will store its data files.
//
// Example:
//
//	node, err := dpx.OpenOshodi(dpx.Config{DataDir: "/var/lib/teller/dpx"})
func OpenOshodi(cfg Config, opts ...oshodidb.Option) (*Node, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "/tmp/dpx-oshodi"
	}
	cfg.Engine = oshodi.New(cfg.DataDir, opts...)
	return Open(cfg, conductor.Open)
}
