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
