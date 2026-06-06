// Package lab measures DPX raw KV throughput and latency.
// DPX is a KV store — Teller owns all transaction/balance logic.
// This runner measures: how fast can DPX accept KV writes, reads, and atomic ops.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/agberohq/dpx"
	dpxraft "github.com/agberohq/dpx/conductor"
	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/engine/pebble"
	"github.com/agberohq/dpx/shared"
)

var (
	flagEngine      = flag.String("engine", "memory", "storage engine: memory | memory-sharded | pebble")
	flagDir         = flag.String("dir", "/tmp/dpx-bench", "data directory (pebble only)")
	flagDuration    = flag.Duration("duration", 15*time.Second, "benchmark duration per run")
	flagWarmup      = flag.Duration("warmup", 3*time.Second, "warmup period (excluded from results)")
	flagConcurrency = flag.Int("concurrency", 2048, "concurrent workers")
	flagKeys        = flag.Int("keys", 100000, "number of distinct keys in the keyspace")
	flagWorkload    = flag.String("workload", "write", "workload: write | read | mixed | atomic")
	flagMode        = flag.String("mode", "embedded", "raft mode: embedded | distributed")
	flagSync        = flag.String("sync", "batch", "sync policy: batch | full | none")
	flagRaftDir     = flag.String("raftdir", "", "raft WAL dir (default: tempdir)")
	flagValueSize   = flag.Int("value", 64, "value size in bytes")
	flagTelemetry   = flag.String("telemetry", "summary", "telemetry level: summary | full | none")
	flagRuns        = flag.Int("runs", 1, "number of timed runs to average (warmup only on first)")
	flagKeyDist     = flag.String("keydist", "random", "key distribution: random | striped (striped pins each goroutine to its own key range)")
	flagCPUProfile  = flag.String("cpuprofile", "", "write cpu profile to file")
	flagMemProfile  = flag.String("memprofile", "", "write memory profile to file")
)

// Compacter is an optional interface engines may implement to force LSM
// compaction before a timed run. Pebble implements this; memory does not.
type Compacter interface {
	Compact() error
}

// RNG

type fastRNG struct{ x uint64 }

func (r *fastRNG) next() uint64 {
	r.x ^= r.x << 13
	r.x ^= r.x >> 7
	r.x ^= r.x << 17
	return r.x
}
func (r *fastRNG) intn(n int) int { return int(r.next() % uint64(n)) }

// Latency (per-goroutine, lock-free)

type localSamples struct{ data []int64 }

func newLocalSamples(cap int) *localSamples {
	return &localSamples{data: make([]int64, 0, cap)}
}
func (l *localSamples) record(d time.Duration) { l.data = append(l.data, int64(d)) }

func mergeAndSort(all []*localSamples) []int64 {
	total := 0
	for _, s := range all {
		total += len(s.data)
	}
	merged := make([]int64, 0, total)
	for _, s := range all {
		merged = append(merged, s.data...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })
	return merged
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

func workloadWrite(ctx context.Context, node *dpx.Node, keys [][]byte, valBuf []byte, rng *fastRNG) error {
	k := keys[rng.intn(len(keys))]
	binary.LittleEndian.PutUint64(valBuf, rng.next())
	return node.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, k, valBuf)
	})
}

func workloadRead(ctx context.Context, node *dpx.Node, keys [][]byte, valBuf []byte, rng *fastRNG) error {
	k := keys[rng.intn(len(keys))]
	return node.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.Get(ctx, k)
		if err != nil && err != dpx.ErrKeyNotFound {
			return err
		}
		return nil
	})
}

func workloadAtomic(ctx context.Context, node *dpx.Node, keys [][]byte, valBuf []byte, rng *fastRNG) error {
	k := keys[rng.intn(len(keys))]
	delta := int64(rng.intn(100) + 1)
	return node.RunInTx(ctx, func(tx dpx.KVTx) error {
		_, err := tx.AtomicAdd(ctx, k, delta)
		return err
	})
}

func workloadMixed(ctx context.Context, node *dpx.Node, keys [][]byte, valBuf []byte, rng *fastRNG) error {
	r := rng.intn(100)
	switch {
	case r < 70:
		return workloadRead(ctx, node, keys, valBuf, rng)
	case r < 90:
		return workloadAtomic(ctx, node, keys, valBuf, rng)
	default:
		return workloadWrite(ctx, node, keys, valBuf, rng)
	}
}

// Single timed run

type runResult struct {
	duration  time.Duration
	ops       uint64
	errors    uint64
	conflicts uint64
	latencies []int64
	gcPauses  time.Duration
	gcCount   uint32
}

func (r runResult) tps() float64 { return float64(r.ops) / r.duration.Seconds() }

// timedRun runs a single timed window using allKeys for every worker.
func timedRun(
	ctx context.Context,
	node *dpx.Node,
	workFn func(context.Context, *dpx.Node, [][]byte, []byte, *fastRNG) error,
	allKeys [][]byte,
	concurrency int,
	valueSize int,
	duration time.Duration,
) runResult {
	wk := make([][][]byte, concurrency)
	for i := range wk {
		wk[i] = allKeys
	}
	return timedRunWithWorkerKeys(ctx, node, workFn, wk, concurrency, valueSize, duration)
}

// timedRunWithWorkerKeys runs a single timed window.
// workerKeys[i] is the key slice for goroutine i, enabling striped distribution.
func timedRunWithWorkerKeys(
	ctx context.Context,
	node *dpx.Node,
	workFn func(context.Context, *dpx.Node, [][]byte, []byte, *fastRNG) error,
	workerKeys [][][]byte,
	concurrency int,
	valueSize int,
	duration time.Duration,
) runResult {
	var ops, errs, conflicts atomic.Uint64

	estPerWorker := int(float64(duration)/float64(time.Millisecond)*2) + 128
	if estPerWorker > 1_000_000 {
		estPerWorker = 1_000_000
	}
	locals := make([]*localSamples, concurrency)
	for i := range locals {
		locals[i] = newLocalSamples(estPerWorker)
	}

	var mStart runtime.MemStats
	runtime.ReadMemStats(&mStart)

	var wg sync.WaitGroup
	start := time.Now()
	end := start.Add(duration)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int, seed uint64) {
			defer wg.Done()
			rng := &fastRNG{x: seed + 9999}
			vb := make([]byte, valueSize)
			local := locals[idx]
			wk := workerKeys[idx]
			for time.Now().Before(end) {
				t0 := time.Now()
				err := workFn(ctx, node, wk, vb, rng)
				lat := time.Since(t0)
				switch {
				case err == nil:
					ops.Add(1)
					local.record(lat)
				case isConflictExhausted(err):
					conflicts.Add(1)
				default:
					errs.Add(1)
				}
			}
		}(i, uint64(i+1))
	}
	wg.Wait()
	elapsed := time.Since(start)

	var mEnd runtime.MemStats
	runtime.ReadMemStats(&mEnd)

	return runResult{
		duration:  elapsed,
		ops:       ops.Load(),
		errors:    errs.Load(),
		conflicts: conflicts.Load(),
		latencies: mergeAndSort(locals),
		gcPauses:  time.Duration(mEnd.PauseTotalNs - mStart.PauseTotalNs),
		gcCount:   mEnd.NumGC - mStart.NumGC,
	}
}

// Multi-run aggregation

// runStats holds the aggregate across N timed runs.
type runStats struct {
	engine      string
	mode        string
	sync        string
	workload    string
	concurrency int
	keys        int
	valueSize   int
	runs        int

	// Per-run TPS samples for mean/stddev.
	tpsSamples []float64

	// Aggregate latency across all runs merged.
	latencies []int64

	// Totals across all runs.
	totalOps       uint64
	totalErrors    uint64
	totalConflicts uint64
	totalDuration  time.Duration
	totalGCPauses  time.Duration
	totalGCCount   uint32
}

func newRunStats(cfg runConfig, numRuns int) *runStats {
	return &runStats{
		engine:      cfg.engine,
		mode:        cfg.mode,
		sync:        cfg.sync,
		workload:    cfg.workload,
		concurrency: cfg.concurrency,
		keys:        cfg.keys,
		valueSize:   cfg.valueSize,
		runs:        numRuns,
	}
}

func (s *runStats) add(r runResult, isWarmupRun bool) {
	s.tpsSamples = append(s.tpsSamples, r.tps())
	// When runs > 1, the first timed run is treated as extended warmup:
	// the Go runtime GC tuner and OS scheduler haven't stabilised yet.
	// We still record it so callers can see it in the per-run breakdown,
	// but we exclude it from the mean/stddev calculation.
	if !isWarmupRun {
		s.latencies = append(s.latencies, r.latencies...)
		s.totalOps += r.ops
		s.totalErrors += r.errors
		s.totalConflicts += r.conflicts
		s.totalDuration += r.duration
		s.totalGCPauses += r.gcPauses
		s.totalGCCount += r.gcCount
	}
}

// measuredSamples returns the TPS samples used for mean/stddev.
// When runs > 1, sample[0] is the runtime-warmup run and is excluded.
func (s *runStats) measuredSamples() []float64 {
	if len(s.tpsSamples) > 1 {
		return s.tpsSamples[1:]
	}
	return s.tpsSamples
}

func (s *runStats) meanTPS() float64 {
	samples := s.measuredSamples()
	if len(samples) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range samples {
		sum += v
	}
	return sum / float64(len(samples))
}

func (s *runStats) stddevTPS() float64 {
	samples := s.measuredSamples()
	if len(samples) < 2 {
		return 0
	}
	mean := s.meanTPS()
	variance := 0.0
	for _, v := range samples {
		d := v - mean
		variance += d * d
	}
	return math.Sqrt(variance / float64(len(samples)-1))
}

func (s *runStats) cvPct() float64 {
	mean := s.meanTPS()
	if mean == 0 {
		return 0
	}
	return s.stddevTPS() / mean * 100
}

func (s *runStats) sortedLatencies() []int64 {
	cp := make([]int64, len(s.latencies))
	copy(cp, s.latencies)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp
}

// Output

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

func printBox(rows [][2]string) {
	w0, w1 := 0, 0
	for _, r := range rows {
		if len(r[0]) > w0 {
			w0 = len(r[0])
		}
		if len(r[1]) > w1 {
			w1 = len(r[1])
		}
	}
	border := w0 + w1 + 7
	fmt.Println("┌" + repeat("─", border) + "┐")
	for i, r := range rows {
		fmt.Printf("│  %-*s  │  %-*s  │\n", w0, r[0], w1, r[1])
		if i == 0 {
			fmt.Println("├" + repeat("─", w0+4) + "┼" + repeat("─", w1+4) + "┤")
		}
	}
	fmt.Println("└" + repeat("─", border) + "┘")
}

func (s *runStats) print() {
	lats := s.sortedLatencies()
	meanTPS := s.meanTPS()
	stddev := s.stddevTPS()
	cv := s.cvPct()

	runLabel := fmt.Sprintf("%d", s.runs)
	if s.runs > 1 {
		runLabel = fmt.Sprintf("%d (mean ± %.0f, CV %.1f%%)", s.runs, stddev, cv)
	}

	rows := [][2]string{
		{"Metric", "Value"},
		{"engine", s.engine},
		{"mode", s.mode},
		{"sync", s.sync},
		{"workload", s.workload},
		{"concurrency", fmt.Sprintf("%d", s.concurrency)},
		{"keys", fmt.Sprintf("%d", s.keys)},
		{"value_size", fmt.Sprintf("%d bytes", s.valueSize)},
		{"runs", runLabel},
		{"total_ops", fmt.Sprintf("%d", s.totalOps)},
		{"total_errors", fmt.Sprintf("%d", s.totalErrors)},
		{"conflicts", fmt.Sprintf("%d (%.2f%%)", s.totalConflicts,
			100*float64(s.totalConflicts)/math.Max(1, float64(s.totalOps+s.totalConflicts)))},
		{"tps_mean", fmt.Sprintf("%.0f", meanTPS)},
	}
	if s.runs > 1 {
		rows = append(rows,
			[2]string{"tps_stddev", fmt.Sprintf("%.0f", stddev)},
			[2]string{"tps_cv", fmt.Sprintf("%.1f%%", cv)},
		)
		for i, t := range s.tpsSamples {
			label := fmt.Sprintf("  run_%d_tps", i+1)
			val := fmt.Sprintf("%.0f", t)
			if i == 0 {
				val += " (runtime warmup — excluded from mean)"
			}
			rows = append(rows, [2]string{label, val})
		}
	}
	if len(lats) > 0 {
		rows = append(rows,
			[2]string{"lat_mean", avg(lats).Round(time.Microsecond).String()},
			[2]string{"lat_p50", pct(lats, 50).Round(time.Microsecond).String()},
			[2]string{"lat_p95", pct(lats, 95).Round(time.Microsecond).String()},
			[2]string{"lat_p99", pct(lats, 99).Round(time.Microsecond).String()},
			[2]string{"lat_max", pct(lats, 100).Round(time.Microsecond).String()},
		)
	}
	printBox(rows)
}

func (s *runStats) printGoal() {
	const target = 1_536_000.0
	mean := s.meanTPS()
	gap := target / math.Max(1, mean)

	suffix := ""
	if s.runs > 1 {
		cv := s.cvPct()
		if cv > 5 {
			suffix = fmt.Sprintf(" ⚠ CV=%.1f%% — results noisy, add more runs", cv)
		}
	}

	printBox([][2]string{
		{"Goal", "Value"},
		{"Target (Teller)", "1,536,000 RunInTx/s"},
		{"Mean TPS", fmt.Sprintf("%.0f RunInTx/s%s", mean, suffix)},
		{"Gap", fmt.Sprintf("%.1fx slower than target", gap)},
	})
}

// Telemetry

func printTelemetrySummary(t *shared.Telemetry, s *runStats) {
	if t == nil {
		return
	}

	stages := []struct {
		name  string
		timer *shared.StageTimer
	}{
		{"Snapshot (engine)", &t.GetSnapshot},
		{"Speculate (fn)", &t.Speculate},
		{"Propose (total)", &t.Propose},
		{"Engine Apply", &t.EngineApply},
		{"  ├─ Map Clone (cow)", &t.Clone},
		{"Snapshot Create", &t.SnapshotCreate},
	}

	type row struct{ name, count, mean, p50, p99 string }
	var rows []row
	for _, st := range stages {
		if st.timer.Count() == 0 {
			continue
		}
		rows = append(rows, row{
			st.name,
			fmt.Sprintf("%d", st.timer.Count()),
			st.timer.Mean().Round(time.Microsecond).String(),
			"–", "–",
		})
	}
	if t.DirectRoundTrip.Count() > 0 {
		rows = append(rows,
			row{"Direct RT (total)", fmt.Sprintf("%d", t.DirectRoundTrip.Count()),
				t.DirectRoundTrip.Mean().Round(time.Microsecond).String(), "–", "–"},
			row{"  ├─ Submit", fmt.Sprintf("%d", t.DirectSubmit.Count()),
				t.DirectSubmit.Mean().Round(time.Nanosecond).String(), "–", "–"},
			row{"  └─ Accumulate", fmt.Sprintf("%d", t.DirectAccumulate.Count()),
				t.DirectAccumulate.Mean().Round(time.Microsecond).String(), "–", "–"},
		)
	}

	w := [5]int{5, 5, 4, 3, 3}
	hdr := [5]string{"Stage", "Count", "Mean", "p50", "p99"}
	for i, h := range hdr {
		if len(h) > w[i] {
			w[i] = len(h)
		}
	}
	for _, r := range rows {
		cols := [5]string{r.name, r.count, r.mean, r.p50, r.p99}
		for i, c := range cols {
			if len(c) > w[i] {
				w[i] = len(c)
			}
		}
	}
	total := w[0] + w[1] + w[2] + w[3] + w[4] + 4*3 + 2
	fmt.Println()
	fmt.Println("┌─ Pipeline Stage Summary " + repeat("─", total-25) + "┐")
	fmt.Printf("│  %-*s  │  %-*s  │  %-*s  │  %-*s  │  %-*s  │\n",
		w[0], hdr[0], w[1], hdr[1], w[2], hdr[2], w[3], hdr[3], w[4], hdr[4])
	fmt.Println("├" + repeat("─", w[0]+4) + "┼" + repeat("─", w[1]+4) + "┼" +
		repeat("─", w[2]+4) + "┼" + repeat("─", w[3]+4) + "┼" + repeat("─", w[4]+4) + "┤")
	for _, r := range rows {
		fmt.Printf("│  %-*s  │  %-*s  │  %-*s  │  %-*s  │  %-*s  │\n",
			w[0], r.name, w[1], r.count, w[2], r.mean, w[3], r.p50, w[4], r.p99)
	}
	fmt.Println("└" + repeat("─", w[0]+4) + "┴" + repeat("─", w[1]+4) + "┴" +
		repeat("─", w[2]+4) + "┴" + repeat("─", w[3]+4) + "┴" + repeat("─", w[4]+4) + "┘")

	fmt.Println()
	fmt.Println("┌─ Bottleneck Analysis ─────────────────────────────────────────┐")
	if t.DirectSubmit.Count() > 0 && t.EngineApply.Count() > 0 {
		avgBatch := float64(t.DirectSubmit.Count()) / float64(t.EngineApply.Count())
		fmt.Printf("│  Avg Batch Size   : %-43s│\n",
			fmt.Sprintf("%.1f ops/batch (cap: %d)", avgBatch, 2048))
	}
	if t.DirectRoundTrip.Count() > 0 {
		meanLat := t.DirectRoundTrip.Mean().Seconds()
		if meanLat > 0 {
			littlesMax := float64(s.concurrency) / meanLat
			fmt.Printf("│  Little's Law     : %-43s│\n",
				fmt.Sprintf("%.0f TPS (c=%d, lat=%s)", littlesMax, s.concurrency,
					t.DirectRoundTrip.Mean().Round(time.Microsecond)))
		}
	}
	if t.Clone.Count() > 0 && t.EngineApply.Count() > 0 {
		clonePct := float64(t.Clone.Mean()) / float64(t.EngineApply.Mean()) * 100
		fmt.Printf("│  Map Clone Cost   : %-43s│\n",
			fmt.Sprintf("%.1f%% of engine apply time", clonePct))
	}
	avgGCPauses := s.totalGCPauses / time.Duration(s.runs)
	avgGCCount := s.totalGCCount / uint32(s.runs)
	avgDuration := s.totalDuration / time.Duration(s.runs)
	fmt.Printf("│  GC (per run avg) : %-43s│\n",
		fmt.Sprintf("%d GCs, %s paused (%.2f%%)", avgGCCount,
			avgGCPauses.Round(time.Millisecond),
			float64(avgGCPauses)/float64(avgDuration)*100))
	fmt.Println("└───────────────────────────────────────────────────────────────┘")
}

func printMetrics(m *shared.Metrics) {
	if m == nil {
		return
	}
	fmt.Println()
	printBox([][2]string{
		{"Metric", "Value"},
		{"Conflict Total", fmt.Sprintf("%d", m.ConflictTotal.Load())},
		{"Conflict Exhausted", fmt.Sprintf("%d", m.ConflictExhausted.Load())},
	})
}

// Core run orchestration

type runConfig struct {
	engine      string
	mode        string
	sync        string
	workload    string
	keyDist     string // "random" | "striped"
	telemetry   *shared.Telemetry
	rawEng      engine.StorageEngine // non-nil: seed directly (distributed mode)
	concurrency int
	keys        int
	valueSize   int
	warmup      time.Duration
	duration    time.Duration
	raftDir     string
	numRuns     int
}

func runBenchmark(node *dpx.Node, cfg runConfig) *runStats {
	ctx := context.Background()

	workFn := map[string]func(context.Context, *dpx.Node, [][]byte, []byte, *fastRNG) error{
		"write":  workloadWrite,
		"read":   workloadRead,
		"atomic": workloadAtomic,
		"mixed":  workloadMixed,
	}[cfg.workload]

	// Generate keys. Striped distribution pins goroutine i to
	// keys [i*stride..(i+1)*stride), eliminating cross-shard key collision
	// that was limiting sharded write TPS. Random uses the full keyspace.
	fmt.Printf("  pre-generating %d keys (%s distribution)...\n", cfg.keys, cfg.keyDist)
	allKeys := make([][]byte, cfg.keys)
	for i := 0; i < cfg.keys; i++ {
		allKeys[i] = []byte(fmt.Sprintf("k:%08d", i))
	}

	// Build per-worker key slices for striped mode.
	// In random mode workerKeys[i] == allKeys (same slice, no copy).
	workerKeys := make([][][]byte, cfg.concurrency) // per-worker key view
	if cfg.keyDist == "striped" && cfg.concurrency > 0 {
		stride := cfg.keys / cfg.concurrency
		if stride < 1 {
			stride = 1
		}
		for i := 0; i < cfg.concurrency; i++ {
			lo := i * stride
			hi := lo + stride
			if hi > cfg.keys || i == cfg.concurrency-1 {
				hi = cfg.keys
			}
			if lo >= cfg.keys {
				lo = cfg.keys - 1
				hi = cfg.keys
			}
			workerKeys[i] = allKeys[lo:hi]
		}
	} else {
		for i := range workerKeys {
			workerKeys[i] = allKeys
		}
	}

	// Seed the database.
	// Distributed mode bypasses Raft and writes directly to the engine to
	// avoid spending the 256-slot semaphore budget on setup traffic.
	fmt.Printf("  seeding database (%d bytes each)...\n", cfg.valueSize)
	rngSeed := &fastRNG{x: 42}
	valBuf := make([]byte, cfg.valueSize)
	const seedBatchSize = 1000
	if cfg.rawEng != nil {
		// Direct engine path for distributed mode: no Raft, no semaphore.
		for i := 0; i < cfg.keys; i += seedBatchSize {
			batch := cfg.rawEng.NewBatch()
			for j := 0; j < seedBatchSize && i+j < cfg.keys; j++ {
				binary.LittleEndian.PutUint64(valBuf, rngSeed.next())
				v := make([]byte, cfg.valueSize)
				copy(v, valBuf)
				batch.Set(allKeys[i+j], v)
			}
			cfg.rawEng.ApplyBatch(batch, engine.WriteOptions{})
		}
	} else {
		seedCtx := context.Background()
		for i := 0; i < cfg.keys; i += seedBatchSize {
			node.RunInTx(seedCtx, func(tx dpx.KVTx) error {
				for j := 0; j < seedBatchSize && i+j < cfg.keys; j++ {
					binary.LittleEndian.PutUint64(valBuf, rngSeed.next())
					tx.Set(seedCtx, allKeys[i+j], valBuf)
				}
				return nil
			})
		}
	}

	// Compact LSM before write/mixed benchmarks if the engine supports it.
	// Pebble builds L0 files during seeding; compacting to L1+ eliminates
	// background compaction jitter during timed runs (was causing CV 7%).
	//
	// For read workloads we skip compaction AND add a cache warmup pass:
	// compacting pushes all data into deeper SST levels where reads must
	// traverse the full level hierarchy on cache misses. Without warmup the
	// first reads pay full disk I/O cost (~7ms) instead of cache hits.
	if c, ok := cfg.rawEng.(Compacter); ok {
		if cfg.workload != "read" {
			fmt.Printf("  compacting LSM...\n")
			if err := c.Compact(); err != nil {
				fmt.Printf("  compact warning: %v\n", err)
			}
		}
		// Cache warmup: read every key once so Pebble's block cache is hot
		// before timing starts. This matters for both read and mixed workloads.
		if cfg.workload == "read" || cfg.workload == "mixed" {
			fmt.Printf("  warming block cache (%d keys)...\n", cfg.keys)
			warmSnap, _ := cfg.rawEng.GetSnapshot()
			if warmSnap != nil {
				for _, k := range allKeys {
					warmSnap.Get(k)
				}
				warmSnap.Close()
			}
		}
	}

	// Warmup — only once, before all runs.
	fmt.Printf("  warming up for %s...\n", cfg.warmup)
	var wg sync.WaitGroup
	warmupEnd := time.Now().Add(cfg.warmup)
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func(idx int, seed uint64) {
			defer wg.Done()
			rng := &fastRNG{x: seed}
			vb := make([]byte, cfg.valueSize)
			wk := workerKeys[idx]
			for time.Now().Before(warmupEnd) {
				workFn(ctx, node, wk, vb, rng)
			}
		}(i, uint64(i+1))
	}
	wg.Wait()

	stats := newRunStats(cfg, cfg.numRuns)

	for run := 1; run <= cfg.numRuns; run++ {
		if cfg.numRuns > 1 {
			fmt.Printf("  run %d/%d (%s, concurrency=%d, workload=%s)...\n",
				run, cfg.numRuns, cfg.duration, cfg.concurrency, cfg.workload)
		} else {
			fmt.Printf("  running %s at concurrency=%d workload=%s...\n",
				cfg.duration, cfg.concurrency, cfg.workload)
		}

		// Reset telemetry before each timed run so we measure clean windows.
		if cfg.telemetry != nil {
			*cfg.telemetry = shared.Telemetry{}
		}

		r := timedRunWithWorkerKeys(ctx, node, workFn, workerKeys, cfg.concurrency, cfg.valueSize, cfg.duration)
		// First run is the Go runtime warmup run — excluded from mean/stddev
		// but still recorded in tpsSamples for the per-run breakdown.
		isWarmupRun := (run == 1 && cfg.numRuns > 1)
		stats.add(r, isWarmupRun)

		if cfg.numRuns > 1 {
			fmt.Printf("    → %.0f TPS\n", r.tps())
		}
	}

	return stats
}

func isConflictExhausted(err error) bool {
	return err != nil && err == dpx.ErrConflictExhausted
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// main

func main() {
	flag.Parse()

	if *flagCPUProfile != "" {
		f, err := os.Create(*flagCPUProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cpu profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "cpu profile start: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

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
		fmt.Fprintf(os.Stderr, "unknown sync policy %q\n", *flagSync)
		os.Exit(1)
	}

	numRuns := *flagRuns
	if numRuns < 1 {
		numRuns = 1
	}

	keyDist := *flagKeyDist
	if keyDist != "random" && keyDist != "striped" {
		fmt.Fprintf(os.Stderr, "unknown keydist %q (random|striped)\n", keyDist)
		os.Exit(1)
	}

	cfg := runConfig{
		engine:      *flagEngine,
		mode:        *flagMode,
		sync:        *flagSync,
		workload:    *flagWorkload,
		keyDist:     keyDist,
		concurrency: *flagConcurrency,
		keys:        *flagKeys,
		valueSize:   *flagValueSize,
		warmup:      *flagWarmup,
		duration:    *flagDuration,
		raftDir:     raftDir,
		telemetry:   &shared.Telemetry{},
		numRuns:     numRuns,
	}

	fmt.Printf("\n╔═══════════════════════════════════════╗\n")
	fmt.Printf("║         DPX Lab — KV Throughput       ║\n")
	fmt.Printf("╚═══════════════════════════════════════╝\n\n")

	cfgRows := [][2]string{
		{"Parameter", "Value"},
		{"engine", cfg.engine},
		{"mode", cfg.mode},
		{"sync", cfg.sync},
		{"workload", cfg.workload},
		{"concurrency", fmt.Sprintf("%d", cfg.concurrency)},
		{"keys", fmt.Sprintf("%d", cfg.keys)},
		{"value_size", fmt.Sprintf("%d bytes", cfg.valueSize)},
		{"duration", cfg.duration.String()},
		{"runs", fmt.Sprintf("%d", numRuns)},
		{"telemetry", *flagTelemetry},
	}
	printBox(cfgRows)
	fmt.Println()

	var eng engine.StorageEngine
	switch cfg.engine {
	case "memory":
		eng = memory.New()
	case "memory-sharded":
		eng = memory.NewSharded()
	case "pebble":
		eng = pebble.New(*flagDir)
	default:
		fmt.Fprintf(os.Stderr, "unknown engine %q\n", cfg.engine)
		os.Exit(1)
	}

	dpxCfg := dpx.Config{
		Engine:     eng,
		SyncPolicy: syncPolicy,
		RaftDir:    raftDir,
		Telemetry:  cfg.telemetry,
	}

	if cfg.mode == "distributed" {
		port, err := getFreePort()
		if err != nil {
			fmt.Fprintf(os.Stderr, "free port: %v\n", err)
			os.Exit(1)
		}
		listenAddr := fmt.Sprintf("127.0.0.1:%d", port)
		dpxCfg.ListenAddr = listenAddr
		dpxCfg.Peers = map[string]string{"1": listenAddr}
		// Distributed mode seeds via the raw engine to avoid spending semaphore
		// budget on setup traffic. The engine is opened via dpx.Open below.
		cfg.rawEng = eng
	} else {
		// For embedded mode, pass rawEng so pebble compaction is available.
		cfg.rawEng = eng
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

	stats := runBenchmark(node, cfg)
	node.Close()
	close(done)

	if *flagMemProfile != "" {
		f, err := os.Create(*flagMemProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mem profile: %v\n", err)
		} else {
			defer f.Close()
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Fprintf(os.Stderr, "mem profile write: %v\n", err)
			}
		}
	}

	fmt.Println()
	stats.print()

	switch *flagTelemetry {
	case "summary":
		printTelemetrySummary(cfg.telemetry, stats)
		printMetrics(dpxCfg.Metrics)
	case "full":
		if cfg.telemetry != nil {
			fmt.Printf("\n┌─ Full Pipeline Stage Breakdown ───────────────────────────\n")
			cfg.telemetry.Print(os.Stdout)
			fmt.Printf("└────────────────────────────────────────────────────────────\n")
		}
		printMetrics(dpxCfg.Metrics)
	}

	fmt.Println()
	stats.printGoal()
	fmt.Println()
}
