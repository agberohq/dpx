// Package lab measures DPX raw KV throughput and latency.
// DPX is a KV store — Teller owns all transaction/balance logic.
// This runner measures: how fast can DPX accept KV writes, reads, and atomic ops.
//
// Usage:
//
//	go run ./lab/runner.go
//	go run ./lab/runner.go -engine pebble -dir /tmp/dpx-bench -duration 30s -sync none
//	go run ./lab/runner.go -workload read -concurrency 128
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/agberohq/dpx"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/engine/pebble"
	dpxraft "github.com/agberohq/dpx/raft"
	"github.com/agberohq/dpx/shared"
)

var (
	flagEngine      = flag.String("engine", "memory", "storage engine: memory | pebble")
	flagDir         = flag.String("dir", "/tmp/dpx-bench", "data directory (pebble only)")
	flagDuration    = flag.Duration("duration", 15*time.Second, "benchmark duration")
	flagWarmup      = flag.Duration("warmup", 2*time.Second, "warmup period (excluded from results)")
	flagConcurrency = flag.Int("concurrency", runtime.GOMAXPROCS(0)*4, "concurrent workers")
	flagKeys        = flag.Int("keys", 10000, "number of distinct keys in the keyspace")
	flagWorkload    = flag.String("workload", "write", "workload: write | read | mixed | atomic")
	flagMode        = flag.String("mode", "embedded", "raft mode: embedded | distributed")
	flagSync        = flag.String("sync", "batch", "sync policy: batch | full | none")
	flagRaftDir     = flag.String("raftdir", "", "raft WAL dir (default: tempdir)")
	flagValueSize   = flag.Int("value", 64, "value size in bytes")
)

// Latency histogram

type histogram struct {
	mu      sync.Mutex
	samples []int64
}

func (h *histogram) record(d time.Duration) {
	h.mu.Lock()
	h.samples = append(h.samples, int64(d))
	h.mu.Unlock()
}

func (h *histogram) drain() []int64 {
	h.mu.Lock()
	s := make([]int64, len(h.samples))
	copy(s, h.samples)
	h.samples = h.samples[:0]
	h.mu.Unlock()
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s
}

func pct(s []int64, p float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	i := int(math.Ceil(p/100*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(s) {
		i = len(s) - 1
	}
	return time.Duration(s[i])
}

func avg(s []int64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	var sum int64
	for _, v := range s {
		sum += v
	}
	return time.Duration(sum / int64(len(s)))
}

// Workloads
// DPX is a KV store. Workloads measure raw KV throughput only.
// No balance checks, no business logic — that belongs in Teller.

func key(i int) []byte {
	return []byte(fmt.Sprintf("k:%08d", i))
}

func makeValue(size int, rng *rand.Rand) []byte {
	v := make([]byte, size)
	binary.LittleEndian.PutUint64(v, rng.Uint64())
	return v
}

// write: Set a random key — measures write throughput and commit latency.
func workloadWrite(ctx context.Context, node *dpx.Node, keys int, valueSize int, rng *rand.Rand) error {
	k := key(rng.Intn(keys))
	v := makeValue(valueSize, rng)
	return node.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, k, v)
	})
}

// read: Get a random key — measures read throughput (snapshot cost + Raft round-trip).
func workloadRead(ctx context.Context, node *dpx.Node, keys int, valueSize int, rng *rand.Rand) error {
	k := key(rng.Intn(keys))
	return node.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.Get(ctx, k)
		if err != nil {
			return nil // key may not exist; not an error for throughput measurement
		}
		return nil
	})
}

// atomic: AtomicAdd on a random key — measures CRDT/counter throughput.
// This is Teller's primary operation for balance stripes: credit = AtomicAdd(+delta).
// Credits never conflict (OpCredit has no ReadSet entry), so this is lock-free.
func workloadAtomic(ctx context.Context, node *dpx.Node, keys int, valueSize int, rng *rand.Rand) error {
	k := key(rng.Intn(keys))
	delta := int64(rng.Intn(100) + 1)
	return node.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.AtomicAdd(ctx, k, delta)
		return err
	})
}

// mixed: 70% atomic credits, 20% reads, 10% plain writes.
// Approximates Teller's balance-stripe workload pattern.
func workloadMixed(ctx context.Context, node *dpx.Node, keys int, valueSize int, rng *rand.Rand) error {
	r := rng.Intn(100)
	switch {
	case r < 70:
		return workloadAtomic(ctx, node, keys, valueSize, rng)
	case r < 90:
		return workloadRead(ctx, node, keys, valueSize, rng)
	default:
		return workloadWrite(ctx, node, keys, valueSize, rng)
	}
}

// Runner

type result struct {
	engine      string
	mode        string
	sync        string
	workload    string
	concurrency int
	keys        int
	valueSize   int
	duration    time.Duration
	ops         uint64
	errors      uint64
	conflicts   uint64
	latencies   []int64
}

func (r *result) tps() float64 { return float64(r.ops) / r.duration.Seconds() }

func (r *result) print(w *tabwriter.Writer) {
	fmt.Fprintf(w, "engine\t%s\n", r.engine)
	fmt.Fprintf(w, "mode\t%s\n", r.mode)
	fmt.Fprintf(w, "sync\t%s\n", r.sync)
	fmt.Fprintf(w, "workload\t%s\n", r.workload)
	fmt.Fprintf(w, "concurrency\t%d\n", r.concurrency)
	fmt.Fprintf(w, "keys\t%d\n", r.keys)
	fmt.Fprintf(w, "value_size\t%d bytes\n", r.valueSize)
	fmt.Fprintf(w, "duration\t%s\n", r.duration.Round(time.Millisecond))
	fmt.Fprintf(w, "ops\t%d\n", r.ops)
	fmt.Fprintf(w, "tps\t%.0f\n", r.tps())
	fmt.Fprintf(w, "errors\t%d\n", r.errors)
	fmt.Fprintf(w, "conflicts\t%d (%.2f%%)\n", r.conflicts,
		100*float64(r.conflicts)/math.Max(1, float64(r.ops+r.conflicts)))
	if len(r.latencies) > 0 {
		fmt.Fprintf(w, "lat_mean\t%s\n", avg(r.latencies).Round(time.Microsecond))
		fmt.Fprintf(w, "lat_p50\t%s\n", pct(r.latencies, 50).Round(time.Microsecond))
		fmt.Fprintf(w, "lat_p95\t%s\n", pct(r.latencies, 95).Round(time.Microsecond))
		fmt.Fprintf(w, "lat_p99\t%s\n", pct(r.latencies, 99).Round(time.Microsecond))
		fmt.Fprintf(w, "lat_p999\t%s\n", pct(r.latencies, 99.9).Round(time.Microsecond))
		fmt.Fprintf(w, "lat_max\t%s\n", pct(r.latencies, 100).Round(time.Microsecond))
	}
}

type runConfig struct {
	engine      string
	mode        string
	sync        string
	workload    string
	telemetry   *shared.Telemetry
	concurrency int
	keys        int
	valueSize   int
	warmup      time.Duration
	duration    time.Duration
	raftDir     string
}

func run(node *dpx.Node, cfg runConfig) result {
	ctx := context.Background()

	workFn := map[string]func(context.Context, *dpx.Node, int, int, *rand.Rand) error{
		"write":  workloadWrite,
		"read":   workloadRead,
		"atomic": workloadAtomic,
		"mixed":  workloadMixed,
	}[cfg.workload]

	// Seed keys so reads have data to find.
	fmt.Printf("  seeding %d keys (%d bytes each)...\n", cfg.keys, cfg.valueSize)
	seedCtx := context.Background()
	rngSeed := rand.New(rand.NewSource(42))
	for i := 0; i < cfg.keys; i++ {
		k := key(i)
		v := makeValue(cfg.valueSize, rngSeed)
		node.RunInTx(seedCtx, func(tx dpx.KVTx) error {
			return tx.Set(seedCtx, k, v)
		})
	}

	// Warmup.
	fmt.Printf("  warming up for %s...\n", cfg.warmup)
	var wg sync.WaitGroup
	warmupEnd := time.Now().Add(cfg.warmup)
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for time.Now().Before(warmupEnd) {
				workFn(ctx, node, cfg.keys, cfg.valueSize, rng)
			}
		}(int64(i))
	}
	wg.Wait()

	// Benchmark.
	fmt.Printf("  running %s at concurrency=%d workload=%s...\n", cfg.duration, cfg.concurrency, cfg.workload)
	var ops, errs, conflicts atomic.Uint64
	hist := &histogram{samples: make([]int64, 0, 200_000)}

	start := time.Now()
	end := start.Add(cfg.duration)
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + 9999))
			for time.Now().Before(end) {
				t0 := time.Now()
				err := workFn(ctx, node, cfg.keys, cfg.valueSize, rng)
				lat := time.Since(t0)
				switch {
				case err == nil:
					ops.Add(1)
					hist.record(lat)
				case isConflictExhausted(err):
					conflicts.Add(1)
				default:
					errs.Add(1)
				}
			}
		}(int64(i))
	}
	wg.Wait()
	elapsed := time.Since(start)

	samples := hist.drain()
	return result{
		engine:      cfg.engine,
		mode:        cfg.mode,
		sync:        cfg.sync,
		workload:    cfg.workload,
		concurrency: cfg.concurrency,
		keys:        cfg.keys,
		valueSize:   cfg.valueSize,
		duration:    elapsed,
		ops:         ops.Load(),
		errors:      errs.Load(),
		conflicts:   conflicts.Load(),
		latencies:   samples,
	}
}

func isConflictExhausted(err error) bool {
	return err != nil && err == dpx.ErrConflictExhausted
}

// Main

func main() {
	flag.Parse()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	raftDir := *flagRaftDir
	if raftDir == "" {
		var err error
		raftDir, err = os.MkdirTemp("", "dpx-lab-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "mkdirtemp: %v\n", err)
			os.Exit(1)
		}
		defer os.RemoveAll(raftDir)
	}

	syncPolicy := dpx.SyncBatch
	switch *flagSync {
	case "full":
		syncPolicy = dpx.SyncFull
	case "none":
		syncPolicy = dpx.SyncNone
	case "batch", "":
		syncPolicy = dpx.SyncBatch
	default:
		fmt.Fprintf(os.Stderr, "unknown sync policy %q (batch|full|none)\n", *flagSync)
		os.Exit(1)
	}

	cfg := runConfig{
		engine:      *flagEngine,
		mode:        *flagMode,
		sync:        *flagSync,
		workload:    *flagWorkload,
		concurrency: *flagConcurrency,
		keys:        *flagKeys,
		valueSize:   *flagValueSize,
		warmup:      *flagWarmup,
		duration:    *flagDuration,
		raftDir:     raftDir,
		telemetry:   &shared.Telemetry{},
	}

	fmt.Printf("\n╔═══════════════════════════════════════╗\n")
	fmt.Printf("║         DPX Lab — KV Throughput       ║\n")
	fmt.Printf("╚═══════════════════════════════════════╝\n\n")
	fmt.Printf("engine       : %s\n", cfg.engine)
	fmt.Printf("mode         : %s\n", cfg.mode)
	fmt.Printf("sync         : %s\n", cfg.sync)
	fmt.Printf("workload     : %s\n", cfg.workload)
	fmt.Printf("concurrency  : %d\n", cfg.concurrency)
	fmt.Printf("keys         : %d\n", cfg.keys)
	fmt.Printf("value_size   : %d bytes\n", cfg.valueSize)
	fmt.Printf("duration     : %s\n\n", cfg.duration)

	var eng interface {
		Open() error
		Close() error
	}
	switch cfg.engine {
	case "memory":
		eng = memory.New()
	case "pebble":
		eng = pebble.New(*flagDir)
	default:
		fmt.Fprintf(os.Stderr, "unknown engine %q\n", cfg.engine)
		os.Exit(1)
	}

	dpxCfg := dpx.Config{
		SyncPolicy: syncPolicy,
		RaftDir:    raftDir,
		Telemetry:  cfg.telemetry,
	}
	switch cfg.engine {
	case "memory":
		dpxCfg.Engine = memory.New()
	case "pebble":
		dpxCfg.Engine = pebble.New(*flagDir)
	}
	_ = eng

	if cfg.mode == "distributed" {
		dpxCfg.ListenAddr = "127.0.0.1:0"
	}

	node, err := dpx.Open(dpxCfg, dpxraft.Open)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-sig:
			fmt.Println("\ninterrupted")
			node.Close()
			os.Exit(1)
		case <-done:
		}
	}()

	r := run(node, cfg)
	node.Close()
	close(done)

	fmt.Printf("\n┌─ Results ──────────────────────────────\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	r.print(w)
	w.Flush()
	fmt.Printf("└────────────────────────────────────────\n\n")

	// Context: what Teller needs from DPX.
	// A real Teller transfer touches ~32 stripe keys (AtomicAdd each).
	// At 48k transfers/s that is 48000 × 32 = 1.536M KV ops/s needed from DPX.
	// Each RunInTx in DPX maps to one Raft round-trip regardless of key count.
	// So Teller batches all 32 stripe ops into one RunInTx = 48k Raft commits/s needed.
	if cfg.telemetry != nil {
		fmt.Printf("\n┌─ Stage Breakdown ──────────────────────────────────────────\n")
		cfg.telemetry.Print(os.Stdout)
		fmt.Printf("└────────────────────────────────────────────────────────────\n")
	}

	fmt.Printf("Context — what Teller needs from DPX:\n")
	fmt.Printf("  Target             : 48,000 RunInTx/s (one Raft commit per Teller transfer)\n")
	fmt.Printf("  This run           : %.0f RunInTx/s\n", r.tps())
	fmt.Printf("  Gap                : %.1fx slower than target\n\n", 48000/math.Max(1, r.tps()))
}
