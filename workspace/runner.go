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
	"text/tabwriter"
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
	flagDuration    = flag.Duration("duration", 15*time.Second, "benchmark duration")
	flagWarmup      = flag.Duration("warmup", 3*time.Second, "warmup period (excluded from results)")
	flagConcurrency = flag.Int("concurrency", 2048, "concurrent workers (sharded needs 8192+ to saturate)")
	flagKeys        = flag.Int("keys", 100000, "number of distinct keys in the keyspace")
	flagWorkload    = flag.String("workload", "write", "workload: write | read | mixed | atomic")
	flagMode        = flag.String("mode", "embedded", "raft mode: embedded | distributed")
	flagSync        = flag.String("sync", "batch", "sync policy: batch | full | none")
	flagRaftDir     = flag.String("raftdir", "", "raft WAL dir (default: tempdir)")
	flagValueSize   = flag.Int("value", 64, "value size in bytes")
	flagTelemetry   = flag.String("telemetry", "summary", "telemetry level: summary | full | none")
	flagCPUProfile  = flag.String("cpuprofile", "", "write cpu profile to file (e.g. cpu.prof)")
	flagMemProfile  = flag.String("memprofile", "", "write memory profile to file (e.g. mem.prof)")
)

// fastRNG is a zero-allocation, lock-free XorShift PRNG.
type fastRNG struct{ x uint64 }

func (r *fastRNG) next() uint64 {
	r.x ^= r.x << 13
	r.x ^= r.x >> 7
	r.x ^= r.x << 17
	return r.x
}
func (r *fastRNG) intn(n int) int {
	return int(r.next() % uint64(n))
}

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

// Workloads (Zero-Allocation Hot Path)

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
		return workloadAtomic(ctx, node, keys, valBuf, rng)
	case r < 90:
		return workloadRead(ctx, node, keys, valBuf, rng)
	default:
		return workloadWrite(ctx, node, keys, valBuf, rng)
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
	gcPauses    time.Duration
	gcCount     uint32
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

	workFn := map[string]func(context.Context, *dpx.Node, [][]byte, []byte, *fastRNG) error{
		"write":  workloadWrite,
		"read":   workloadRead,
		"atomic": workloadAtomic,
		"mixed":  workloadMixed,
	}[cfg.workload]

	fmt.Printf("  pre-generating %d keys...\n", cfg.keys)
	allKeys := make([][]byte, cfg.keys)
	for i := 0; i < cfg.keys; i++ {
		allKeys[i] = []byte(fmt.Sprintf("k:%08d", i))
	}

	fmt.Printf("  seeding database (%d bytes each)...\n", cfg.valueSize)
	seedCtx := context.Background()
	rngSeed := &fastRNG{x: 42}
	valBuf := make([]byte, cfg.valueSize)

	// FAST SEEDING: Batch 1000 keys per transaction so we don't hit 100,000 Raft batcher delays!
	const seedBatchSize = 1000
	for i := 0; i < cfg.keys; i += seedBatchSize {
		node.RunInTx(seedCtx, func(tx dpx.KVTx) error {
			for j := 0; j < seedBatchSize && i+j < cfg.keys; j++ {
				k := allKeys[i+j]
				binary.LittleEndian.PutUint64(valBuf, rngSeed.next())
				tx.Set(seedCtx, k, valBuf)
			}
			return nil
		})
	}

	var mStart runtime.MemStats
	runtime.ReadMemStats(&mStart)

	fmt.Printf("  warming up for %s...\n", cfg.warmup)
	var wg sync.WaitGroup
	warmupEnd := time.Now().Add(cfg.warmup)
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := &fastRNG{x: seed}
			vb := make([]byte, cfg.valueSize)
			for time.Now().Before(warmupEnd) {
				workFn(ctx, node, allKeys, vb, rng)
			}
		}(uint64(i + 1))
	}
	wg.Wait()

	if cfg.telemetry != nil {
		*cfg.telemetry = shared.Telemetry{}
	}

	fmt.Printf("  running %s at concurrency=%d workload=%s...\n", cfg.duration, cfg.concurrency, cfg.workload)
	var ops, errs, conflicts atomic.Uint64
	hist := &histogram{samples: make([]int64, 0, 200_000)}

	runtime.ReadMemStats(&mStart)

	start := time.Now()
	end := start.Add(cfg.duration)
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := &fastRNG{x: seed + 9999}
			vb := make([]byte, cfg.valueSize)
			for time.Now().Before(end) {
				t0 := time.Now()
				err := workFn(ctx, node, allKeys, vb, rng)
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
		}(uint64(i + 1))
	}
	wg.Wait()
	elapsed := time.Since(start)

	var mEnd runtime.MemStats
	runtime.ReadMemStats(&mEnd)

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
		gcPauses:    time.Duration(mEnd.PauseTotalNs - mStart.PauseTotalNs),
		gcCount:     mEnd.NumGC - mStart.NumGC,
	}
}

func isConflictExhausted(err error) bool {
	return err != nil && err == dpx.ErrConflictExhausted
}

func printTelemetrySummary(t *shared.Telemetry, r result) {
	if t == nil {
		return
	}

	fmt.Printf("\n┌─ Pipeline Stage Summary ──────────────────────────────────\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	stages := []struct {
		name string
		t    *shared.StageTimer
	}{
		{"Snapshot (engine)", &t.GetSnapshot},
		{"Speculate (fn)", &t.Speculate},
		{"Propose (total)", &t.Propose},
		{"Engine Apply", &t.EngineApply},
		{"  ├─ Map Clone (cow)", &t.Clone},
		{"Snapshot Create", &t.SnapshotCreate},
	}

	fmt.Fprintf(w, "  %-25s %8s %12s %12s %12s\n", "Stage", "Count", "Mean", "p50", "p99")
	for _, s := range stages {
		if s.t.Count() == 0 {
			continue
		}
		fmt.Fprintf(w, "  %-25s %8d %12s %12s %12s\n",
			s.name,
			s.t.Count(),
			s.t.Mean().Round(time.Microsecond),
			"-", "-",
		)
	}

	if t.DirectRoundTrip.Count() > 0 {
		fmt.Fprintf(w, "  %-25s %8d %12s %12s %12s\n",
			"Direct RT (total)",
			t.DirectRoundTrip.Count(),
			t.DirectRoundTrip.Mean().Round(time.Microsecond),
			"-", "-",
		)
		fmt.Fprintf(w, "  %-25s %8d %12s %12s %12s\n",
			"  ├─ Submit",
			t.DirectSubmit.Count(),
			t.DirectSubmit.Mean().Round(time.Nanosecond),
			"-", "-",
		)
		fmt.Fprintf(w, "  %-25s %8d %12s %12s %12s\n",
			"  └─ Accumulate",
			t.DirectAccumulate.Count(),
			t.DirectAccumulate.Mean().Round(time.Microsecond),
			"-", "-",
		)
	}
	w.Flush()
	fmt.Printf("└────────────────────────────────────────────────────────────\n")

	fmt.Printf("\n┌─ Bottleneck Analysis ─────────────────────────────────────\n")

	if t.DirectSubmit.Count() > 0 && t.EngineApply.Count() > 0 {
		avgBatch := float64(t.DirectSubmit.Count()) / float64(t.EngineApply.Count())
		fmt.Printf("  Average Batch Size : %.1f ops/batch (Max: 512)\n", avgBatch)
	}

	if t.DirectRoundTrip.Count() > 0 {
		meanLat := t.DirectRoundTrip.Mean().Seconds()
		if meanLat > 0 {
			littlesMax := float64(r.concurrency) / meanLat
			fmt.Printf("  Little's Law Limit : %.0f TPS (Concurrency: %d, Latency: %s)\n",
				littlesMax, r.concurrency, t.DirectRoundTrip.Mean().Round(time.Microsecond))
		}
	}

	if t.Clone.Count() > 0 && t.EngineApply.Count() > 0 {
		cloneCost := t.Clone.Mean()
		applyCost := t.EngineApply.Mean()
		if applyCost > 0 {
			pct := float64(cloneCost) / float64(applyCost) * 100
			fmt.Printf("  Map Clone Cost     : %.1f%% of Engine Apply time is spent copying memory\n", pct)
		}
	}

	fmt.Printf("  Garbage Collection : %d GCs, %s total pause time (%.2f%% of duration)\n",
		r.gcCount, r.gcPauses.Round(time.Millisecond), float64(r.gcPauses)/float64(r.duration)*100)

	fmt.Printf("└────────────────────────────────────────────────────────────\n")
}

func printTelemetryFull(t *shared.Telemetry) {
	if t == nil {
		return
	}
	fmt.Printf("\n┌─ Full Pipeline Stage Breakdown ───────────────────────────\n")
	t.Print(os.Stdout)
	fmt.Printf("└────────────────────────────────────────────────────────────\n")
}

func printMetrics(m *shared.Metrics) {
	if m == nil {
		return
	}

	fmt.Printf("\n┌─ Metrics ─────────────────────────────────────────────────\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintf(w, "  %-35s %15d\n", "Conflict Total", m.ConflictTotal.Load())
	fmt.Fprintf(w, "  %-35s %15d\n", "Conflict Exhausted", m.ConflictExhausted.Load())

	w.Flush()
	fmt.Printf("└────────────────────────────────────────────────────────────\n")
}

// getFreePort asks the kernel for a free open port that is ready to use.
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

func main() {
	flag.Parse()

	if *flagCPUProfile != "" {
		f, err := os.Create(*flagCPUProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "could not start CPU profile: %v\n", err)
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
	fmt.Printf("duration     : %s\n", cfg.duration)
	fmt.Printf("telemetry    : %s\n\n", *flagTelemetry)

	var eng engine.StorageEngine
	switch cfg.engine {
	case "memory":
		eng = memory.New()
	case "memory-sharded":
		eng = memory.NewSharded()
	case "pebble":
		eng = pebble.New(*flagDir)
	default:
		fmt.Fprintf(os.Stderr, "unknown engine %q (use memory, memory-sharded, or pebble)\n", cfg.engine)
		os.Exit(1)
	}

	dpxCfg := dpx.Config{
		Engine:     eng,
		SyncPolicy: syncPolicy,
		RaftDir:    raftDir,
		Telemetry:  cfg.telemetry,
	}

	if cfg.mode == "distributed" {
		// Safely claim a free port from the OS so HashiCorp Raft knows EXACTLY
		// where it is listening, otherwise it will fail to elect a leader.
		port, err := getFreePort()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get free port: %v\n", err)
			os.Exit(1)
		}
		listenAddr := fmt.Sprintf("127.0.0.1:%d", port)
		dpxCfg.ListenAddr = listenAddr
		dpxCfg.Peers = map[string]string{"1": listenAddr}
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

	if *flagMemProfile != "" {
		f, err := os.Create(*flagMemProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create memory profile: %v\n", err)
		} else {
			defer f.Close()
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Fprintf(os.Stderr, "could not write memory profile: %v\n", err)
			}
		}
	}

	fmt.Printf("\n┌─ Results ──────────────────────────────\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	r.print(w)
	w.Flush()
	fmt.Printf("└────────────────────────────────────────\n")

	switch *flagTelemetry {
	case "summary":
		printTelemetrySummary(cfg.telemetry, r)
		printMetrics(dpxCfg.Metrics)
	case "full":
		printTelemetryFull(cfg.telemetry)
		printMetrics(dpxCfg.Metrics)
	}

	fmt.Printf("\nContext — what Teller needs from DPX:\n")
	fmt.Printf("  Target             : 1,536,000 RunInTx/s (one Raft commit per Teller transfer)\n")
	fmt.Printf("  This run           : %.0f RunInTx/s\n", r.tps())
	fmt.Printf("  Gap                : %.1fx slower than target\n\n", 1536000/math.Max(1, r.tps()))
}
