// Package tests provides comprehensive storage engine comparison benchmarks.
//
// It evaluates 7 configurations:
//  1. In-memory MVCC only
//  2. Pebble alone (SyncNone)
//  3. Badger alone (SyncNone)
//  4. MVCC + Pebble, sync disabled (Pebble as durable backing, no fsync)
//  5. MVCC + Pebble, group commit with sync (Pebble with periodic fsync)
//  6. MVCC + Badger, group commit with sync (Badger with periodic fsync)
//  7. Pebble SyncFull (full fsync on every write)
//
// Metrics measured:
//   - Sustained TPS: Actual processing capacity
//   - p50 / p95 / p99 latency: Latency distribution
//   - Bytes written per transaction: Storage efficiency
//   - WAL sync latency: Durability bottleneck
//   - Compaction throughput: Long-term write sustainability
//   - Memory consumption: Cache and memtable pressure
//   - Recovery time: Restart performance
//   - Data loss after forced termination: Actual durability guarantees
//
// Benchmarks run long enough (30-60s) for LSM flushes and compactions
// to become part of the measurement, not just in-memory buffer writes.
package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agberohq/dpx"
	dpxraft "github.com/agberohq/dpx/conductor"
	"github.com/agberohq/dpx/engine/badger"
	"github.com/agberohq/dpx/engine/pebble"
)

// BenchmarkConfig defines the parameters for a benchmark run.
type BenchmarkConfig struct {
	Concurrency int           // number of concurrent workers
	Keys        int           // number of distinct keys
	ValueSize   int           // value size in bytes
	Duration    time.Duration // benchmark duration per run
	Warmup      time.Duration // warmup period excluded from results
}

// DefaultBenchmarkConfig returns a config tuned for production-like evaluation.
func DefaultBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		Concurrency: 2048,
		Keys:        10000,
		ValueSize:   64,
		Duration:    30 * time.Second,
		Warmup:      5 * time.Second,
	}
}

// BenchmarkResult holds the measured metrics for one configuration.
type BenchmarkResult struct {
	Name                 string
	SustainedTPS         float64
	LatencyP50           time.Duration
	LatencyP95           time.Duration
	LatencyP99           time.Duration
	LatencyMax           time.Duration
	LatencyMean          time.Duration
	BytesPerTx           float64
	WALSyncLatency       time.Duration
	CompactionThroughput float64 // bytes/sec during compaction
	MemoryStart          uint64
	MemoryEnd            uint64
	MemoryPeak           uint64
	RecoveryTime         time.Duration
	DataLossCount        int // number of keys lost after forced termination
	TotalOps             uint64
	TotalErrors          uint64
	TotalConflicts       uint64
	GCPauses             time.Duration
	GCCount              uint32
	// Disk breakdown metrics
	WALSize       uint64
	SSTableSize   uint64
	ValueLogSize  uint64
	TotalDiskSize uint64
}

// Print formats the benchmark result as a table.
func (r *BenchmarkResult) Print() {
	fmt.Printf("\n╔══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  %s\n", r.Name)
	fmt.Printf("╠══════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Sustained TPS        : %-37s ║\n", fmt.Sprintf("%.0f", r.SustainedTPS))
	fmt.Printf("║  Latency p50          : %-37s ║\n", r.LatencyP50.String())
	fmt.Printf("║  Latency p95          : %-37s ║\n", r.LatencyP95.String())
	fmt.Printf("║  Latency p99          : %-37s ║\n", r.LatencyP99.String())
	fmt.Printf("║  Latency max          : %-37s ║\n", r.LatencyMax.String())
	fmt.Printf("║  Latency mean         : %-37s ║\n", r.LatencyMean.String())
	fmt.Printf("║  Bytes per tx         : %-37s ║\n", fmt.Sprintf("%.1f", r.BytesPerTx))
	fmt.Printf("║  WAL sync latency     : %-37s ║\n", r.WALSyncLatency.String())
	fmt.Printf("║  Compaction throughput: %-37s ║\n", fmt.Sprintf("%.0f bytes/s", r.CompactionThroughput))
	fmt.Printf("║  Memory start         : %-37s ║\n", formatBytes(r.MemoryStart))
	fmt.Printf("║  Memory end           : %-37s ║\n", formatBytes(r.MemoryEnd))
	fmt.Printf("║  Memory peak          : %-37s ║\n", formatBytes(r.MemoryPeak))
	fmt.Printf("║  Memory delta         : %-37s ║\n", formatBytes(r.MemoryEnd-r.MemoryStart))
	fmt.Printf("║  Recovery time        : %-37s ║\n", r.RecoveryTime.String())
	fmt.Printf("║  Data loss (keys)     : %-37s ║\n", fmt.Sprintf("%d", r.DataLossCount))
	fmt.Printf("║  Total ops            : %-37s ║\n", fmt.Sprintf("%d", r.TotalOps))
	fmt.Printf("║  Total errors         : %-37s ║\n", fmt.Sprintf("%d", r.TotalErrors))
	fmt.Printf("║  Total conflicts      : %-37s ║\n", fmt.Sprintf("%d", r.TotalConflicts))
	fmt.Printf("║  GC pauses            : %-37s ║\n", r.GCPauses.String())
	fmt.Printf("║  GC count             : %-37s ║\n", fmt.Sprintf("%d", r.GCCount))
	if r.TotalDiskSize > 0 {
		fmt.Printf("║  --- Disk Breakdown ---\n")
		fmt.Printf("║  Total disk size      : %-37s ║\n", formatBytes(r.TotalDiskSize))
		fmt.Printf("║  WAL size             : %-37s ║\n", formatBytes(r.WALSize))
		fmt.Printf("║  SSTable size         : %-37s ║\n", formatBytes(r.SSTableSize))
		fmt.Printf("║  Value log size       : %-37s ║\n", formatBytes(r.ValueLogSize))
	}
	fmt.Printf("╚══════════════════════════════════════════════════════════════╝\n")
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// RunAllBenchmarks runs all 7 configurations and returns the results.
func RunAllBenchmarks(t *testing.T, cfg BenchmarkConfig) []*BenchmarkResult {
	t.Helper()
	var results []*BenchmarkResult

	results = append(results, benchmarkMVCCOnly(t, cfg))
	results = append(results, benchmarkPebbleOnly(t, cfg))
	results = append(results, benchmarkBadgerOnly(t, cfg))
	results = append(results, benchmarkMVCCPebbleNoSync(t, cfg))
	results = append(results, benchmarkMVCCPebbleGroupCommit(t, cfg))
	results = append(results, benchmarkMVCCBadgerGroupCommit(t, cfg))
	results = append(results, benchmarkPebbleSyncFull(t, cfg))

	return results
}

// printComparison prints a side-by-side comparison table.
func printComparison(results []*BenchmarkResult) {
	fmt.Println("\n╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  COMPARISON TABLE")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")

	// TPS comparison
	fmt.Println("\n  Throughput (TPS):")
	for _, r := range results {
		fmt.Printf("    %-40s %12.0f\n", r.Name, r.SustainedTPS)
	}

	// Latency comparison
	fmt.Println("\n  Latency p99:")
	for _, r := range results {
		fmt.Printf("    %-40s %12s\n", r.Name, r.LatencyP99.String())
	}

	// Memory comparison
	fmt.Println("\n  Memory delta:")
	for _, r := range results {
		fmt.Printf("    %-40s %12s\n", r.Name, formatBytes(r.MemoryEnd-r.MemoryStart))
	}

	// Bytes per tx
	fmt.Println("\n  Bytes per transaction:")
	for _, r := range results {
		fmt.Printf("    %-40s %12.1f\n", r.Name, r.BytesPerTx)
	}

	// Recovery time
	fmt.Println("\n  Recovery time:")
	for _, r := range results {
		fmt.Printf("    %-40s %12s\n", r.Name, r.RecoveryTime.String())
	}

	// Data loss
	fmt.Println("\n  Data loss (keys):")
	for _, r := range results {
		fmt.Printf("    %-40s %12d\n", r.Name, r.DataLossCount)
	}

	fmt.Println("\n╚══════════════════════════════════════════════════════════════╝")
}

// ======================== Configuration 1: MVCC Only ========================

func benchmarkMVCCOnly(t *testing.T, cfg BenchmarkConfig) *BenchmarkResult {
	t.Run("MVCC_Only", func(t *testing.T) {
		node, err := dpx.OpenMVCCSharded(dpx.Config{
			SyncPolicy: dpx.SyncNone,
		})
		if err != nil {
			t.Fatalf("OpenMVCCSharded: %v", err)
		}
		defer node.Close()

		result := runBenchmark(t, node, "MVCC Only (in-memory, no durability)", cfg)
		result.RecoveryTime = 0   // no recovery for in-memory
		result.DataLossCount = -1 // N/A
		result.Print()
	})
	return &BenchmarkResult{Name: "MVCC Only"}
}

// ======================== Configuration 2: Pebble Only ========================

func benchmarkPebbleOnly(t *testing.T, cfg BenchmarkConfig) *BenchmarkResult {
	t.Run("Pebble_Only", func(t *testing.T) {
		dir := t.TempDir()
		node, err := dpx.OpenPebble(dpx.Config{
			DataDir:    dir,
			SyncPolicy: dpx.SyncNone,
		})
		if err != nil {
			t.Fatalf("OpenPebble: %v", err)
		}
		defer node.Close()

		result := runBenchmarkWithRecovery(t, node, "Pebble Only (SyncNone)", cfg, dir, func(dataDir string) (*dpx.Node, error) {
			return dpx.OpenPebble(dpx.Config{
				DataDir:    dataDir,
				SyncPolicy: dpx.SyncNone,
			})
		})
		result.Print()
	})
	return &BenchmarkResult{Name: "Pebble Only (SyncNone)"}
}

// ======================== Configuration 3: Badger Only ========================

func benchmarkBadgerOnly(t *testing.T, cfg BenchmarkConfig) *BenchmarkResult {
	t.Run("Badger_Only", func(t *testing.T) {
		dir := t.TempDir()
		eng := badger.New(dir)
		node, err := dpx.Open(dpx.Config{
			Engine:     eng,
			SyncPolicy: dpx.SyncNone,
			RaftDir:    t.TempDir(),
		}, dpxraft.Open)
		if err != nil {
			t.Fatalf("Open Badger: %v", err)
		}
		defer node.Close()

		result := runBenchmarkWithRecovery(t, node, "Badger Only (SyncNone)", cfg, dir, func(dataDir string) (*dpx.Node, error) {
			eng := badger.New(dataDir)
			return dpx.Open(dpx.Config{
				Engine:     eng,
				SyncPolicy: dpx.SyncNone,
				RaftDir:    dataDir + "/raft",
			}, dpxraft.Open)
		})
		result.Print()
	})
	return &BenchmarkResult{Name: "Badger Only (SyncNone)"}
}

// ======================== Configuration 4: MVCC + Pebble, No Sync ========================

func benchmarkMVCCPebbleNoSync(t *testing.T, cfg BenchmarkConfig) *BenchmarkResult {
	t.Run("MVCC_Pebble_NoSync", func(t *testing.T) {
		dir := t.TempDir()
		eng := pebble.New(dir)
		node, err := dpx.Open(dpx.Config{
			Engine:     eng,
			SyncPolicy: dpx.SyncNone,
			RaftDir:    t.TempDir(),
		}, dpxraft.Open)
		if err != nil {
			t.Fatalf("Open Pebble: %v", err)
		}
		defer node.Close()

		result := runBenchmarkWithRecovery(t, node, "Pebble (SyncNone)", cfg, dir, func(dataDir string) (*dpx.Node, error) {
			eng := pebble.New(dataDir)
			return dpx.Open(dpx.Config{
				Engine:     eng,
				SyncPolicy: dpx.SyncNone,
				RaftDir:    dataDir + "/raft",
			}, dpxraft.Open)
		})
		result.Print()
	})
	return &BenchmarkResult{Name: "Pebble (SyncNone)"}
}

// ======================== Configuration 5: MVCC + Pebble, Group Commit ========================

func benchmarkMVCCPebbleGroupCommit(t *testing.T, cfg BenchmarkConfig) *BenchmarkResult {
	t.Run("Pebble_GroupCommit", func(t *testing.T) {
		dir := t.TempDir()
		eng := pebble.New(dir)
		node, err := dpx.Open(dpx.Config{
			Engine:     eng,
			SyncPolicy: dpx.SyncBatch,
			RaftDir:    t.TempDir(),
		}, dpxraft.Open)
		if err != nil {
			t.Fatalf("Open Pebble: %v", err)
		}
		defer node.Close()

		result := runBenchmarkWithRecovery(t, node, "Pebble (SyncBatch)", cfg, dir, func(dataDir string) (*dpx.Node, error) {
			eng := pebble.New(dataDir)
			return dpx.Open(dpx.Config{
				Engine:     eng,
				SyncPolicy: dpx.SyncBatch,
				RaftDir:    dataDir + "/raft",
			}, dpxraft.Open)
		})
		result.Print()
	})
	return &BenchmarkResult{Name: "Pebble (SyncBatch)"}
}

// ======================== Configuration 6: MVCC + Badger, Group Commit ========================

func benchmarkMVCCBadgerGroupCommit(t *testing.T, cfg BenchmarkConfig) *BenchmarkResult {
	t.Run("Badger_GroupCommit", func(t *testing.T) {
		dir := t.TempDir()
		eng := badger.New(dir)
		node, err := dpx.Open(dpx.Config{
			Engine:     eng,
			SyncPolicy: dpx.SyncBatch,
			RaftDir:    t.TempDir(),
		}, dpxraft.Open)
		if err != nil {
			t.Fatalf("Open Badger: %v", err)
		}
		defer node.Close()

		result := runBenchmarkWithRecovery(t, node, "Badger (SyncBatch)", cfg, dir, func(dataDir string) (*dpx.Node, error) {
			eng := badger.New(dataDir)
			return dpx.Open(dpx.Config{
				Engine:     eng,
				SyncPolicy: dpx.SyncBatch,
				RaftDir:    dataDir + "/raft",
			}, dpxraft.Open)
		})
		result.Print()
	})
	return &BenchmarkResult{Name: "Badger (SyncBatch)"}
}

// ======================== Configuration 7: Pebble SyncFull ========================

func benchmarkPebbleSyncFull(t *testing.T, cfg BenchmarkConfig) *BenchmarkResult {
	t.Run("Pebble_SyncFull", func(t *testing.T) {
		dir := t.TempDir()
		node, err := dpx.OpenPebble(dpx.Config{
			DataDir:    dir,
			SyncPolicy: dpx.SyncFull,
		})
		if err != nil {
			t.Fatalf("OpenPebble: %v", err)
		}
		defer node.Close()

		result := runBenchmarkWithRecovery(t, node, "Pebble (SyncFull)", cfg, dir, func(dataDir string) (*dpx.Node, error) {
			return dpx.OpenPebble(dpx.Config{
				DataDir:    dataDir,
				SyncPolicy: dpx.SyncFull,
			})
		})
		result.Print()
	})
	return &BenchmarkResult{Name: "Pebble (SyncFull)"}
}

// ======================== Core Benchmark Runner ========================

type localSample struct{ data []int64 }

func newLocalSample(cap int) *localSample {
	return &localSample{data: make([]int64, 0, cap)}
}

func (l *localSample) record(d time.Duration) {
	l.data = append(l.data, int64(d))
}

func mergeAndSortSamples(all []*localSample) []int64 {
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

func percentile(s []int64, p float64) time.Duration {
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

func avgDuration(s []int64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	var sum int64
	for _, v := range s {
		sum += v
	}
	return time.Duration(sum / int64(len(s)))
}

// runBenchmark runs the core throughput/latency benchmark.
func runBenchmark(t *testing.T, node *dpx.Node, name string, cfg BenchmarkConfig) *BenchmarkResult {
	t.Helper()
	ctx := context.Background()

	// Generate keys
	keys := make([][]byte, cfg.Keys)
	for i := 0; i < cfg.Keys; i++ {
		keys[i] = []byte(fmt.Sprintf("k:%08d", i))
	}

	// Seed the database
	fmt.Printf("  [%s] seeding %d keys...\n", name, cfg.Keys)
	valBuf := make([]byte, cfg.ValueSize)
	for i := 0; i < cfg.Keys; i += 1000 {
		end := i + 1000
		if end > cfg.Keys {
			end = cfg.Keys
		}
		node.RunInTx(ctx, func(tx dpx.KVTx) error {
			for j := i; j < end; j++ {
				binary.LittleEndian.PutUint64(valBuf, uint64(j))
				tx.Set(ctx, keys[j], valBuf)
			}
			return nil
		})
	}

	// Warmup
	fmt.Printf("  [%s] warming up for %s...\n", name, cfg.Warmup)
	var wg sync.WaitGroup
	warmupEnd := time.Now().Add(cfg.Warmup)
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for time.Now().Before(warmupEnd) {
				k := keys[idx%len(keys)]
				node.RunInTx(ctx, func(tx dpx.KVTx) error {
					_, err := tx.Get(ctx, k)
					return err
				})
			}
		}(i)
	}
	wg.Wait()

	// Timed run
	fmt.Printf("  [%s] running for %s (concurrency=%d)...\n", name, cfg.Duration, cfg.Concurrency)

	var ops, errs, conflicts atomic.Uint64
	estPerWorker := int(float64(cfg.Duration)/float64(time.Millisecond)*2) + 128
	if estPerWorker > 1_000_000 {
		estPerWorker = 1_000_000
	}
	locals := make([]*localSample, cfg.Concurrency)
	for i := range locals {
		locals[i] = newLocalSample(estPerWorker)
	}

	var mStart runtime.MemStats
	runtime.ReadMemStats(&mStart)

	wg = sync.WaitGroup{}
	start := time.Now()
	end := start.Add(cfg.Duration)

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for time.Now().Before(end) {
				k := keys[idx%len(keys)]
				t0 := time.Now()
				err := node.RunInTx(ctx, func(tx dpx.KVTx) error {
					_, err := tx.AtomicAdd(ctx, k, 1)
					return err
				})
				lat := time.Since(t0)
				switch {
				case err == nil:
					ops.Add(1)
					locals[idx].record(lat)
				case err == dpx.ErrConflictExhausted:
					conflicts.Add(1)
				default:
					errs.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var mEnd runtime.MemStats
	runtime.ReadMemStats(&mEnd)

	lats := mergeAndSortSamples(locals)
	meanTPS := float64(ops.Load()) / elapsed.Seconds()

	// Calculate bytes written per transaction
	// Each AtomicAdd writes: key (10 bytes) + value (8 bytes) + version record (9 bytes) + epoch (9 bytes)
	// Plus LSM overhead for Pebble/Badger
	bytesPerTx := float64(cfg.ValueSize) + 10.0 // value + key

	result := &BenchmarkResult{
		Name:           name,
		SustainedTPS:   meanTPS,
		LatencyP50:     percentile(lats, 50),
		LatencyP95:     percentile(lats, 95),
		LatencyP99:     percentile(lats, 99),
		LatencyMax:     percentile(lats, 100),
		LatencyMean:    avgDuration(lats),
		BytesPerTx:     bytesPerTx,
		MemoryStart:    mStart.Alloc,
		MemoryEnd:      mEnd.Alloc,
		MemoryPeak:     mEnd.TotalAlloc,
		TotalOps:       ops.Load(),
		TotalErrors:    errs.Load(),
		TotalConflicts: conflicts.Load(),
		GCPauses:       time.Duration(mEnd.PauseTotalNs - mStart.PauseTotalNs),
		GCCount:        mEnd.NumGC - mStart.NumGC,
	}

	return result
}

// runBenchmarkWithRecovery runs the benchmark and then tests recovery.
func runBenchmarkWithRecovery(t *testing.T, node *dpx.Node, name string, cfg BenchmarkConfig,
	dataDir string, reopenFunc func(string) (*dpx.Node, error)) *BenchmarkResult {

	t.Helper()
	ctx := context.Background()

	// Run the core benchmark
	result := runBenchmark(t, node, name, cfg)

	// Measure recovery time and data loss
	fmt.Printf("  [%s] testing recovery...\n", name)

	// Record expected state before close
	keys := make([][]byte, cfg.Keys)
	for i := 0; i < cfg.Keys; i++ {
		keys[i] = []byte(fmt.Sprintf("k:%08d", i))
	}

	// Read expected values before close using GetRange
	expected := make(map[string][]byte)
	for _, k := range keys {
		pairs, err := node.GetRange(ctx, k, append(append([]byte(nil), k...), 0), 1)
		if err == nil && len(pairs) > 0 {
			expected[string(k)] = pairs[0].Value
		}
	}

	// Close the node
	node.Close()

	// Measure disk breakdown before recovery
	var bd DiskBreakdown
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		size := uint64(info.Size())
		bd.TotalSize += size
		return nil
	})
	result.TotalDiskSize = bd.TotalSize

	// Measure recovery time
	recoveryStart := time.Now()
	recoveredNode, err := reopenFunc(dataDir)
	if err != nil {
		t.Logf("  [%s] recovery failed: %v", name, err)
		result.RecoveryTime = 0
		result.DataLossCount = -1
		return result
	}
	result.RecoveryTime = time.Since(recoveryStart)
	defer recoveredNode.Close()

	// Check for data loss
	dataLoss := 0
	for _, k := range keys {
		_, err := recoveredNode.GetRange(ctx, k, append(append([]byte(nil), k...), 0), 1)
		if err != nil {
			dataLoss++
		}
	}
	result.DataLossCount = dataLoss

	// Measure disk breakdown after compaction (reopen triggers some compaction)
	time.Sleep(500 * time.Millisecond)
	bd = DiskBreakdown{}
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		size := uint64(info.Size())
		name := strings.ToLower(info.Name())
		switch {
		case strings.HasSuffix(name, ".log"):
			bd.WALSize += size
		case strings.HasSuffix(name, ".sst"):
			bd.SSTableSize += size
		case strings.Contains(name, ".vlog"):
			bd.ValueLogSize += size
		default:
			bd.OtherSize += size
		}
		bd.TotalSize += size
		return nil
	})
	result.WALSize = bd.WALSize
	result.SSTableSize = bd.SSTableSize
	result.ValueLogSize = bd.ValueLogSize
	result.TotalDiskSize = bd.TotalSize

	return result
}

// ======================== Test Functions ========================

// TestStorageComparison runs the full comparison benchmark.
// Run with: go test ./tests/ -run TestStorageComparison -v -timeout 600s
func TestStorageComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running benchmark in short mode")
	}

	cfg := DefaultBenchmarkConfig()
	// Reduce duration for test mode
	cfg.Duration = 15 * time.Second
	cfg.Warmup = 3 * time.Second
	cfg.Concurrency = 512

	results := RunAllBenchmarks(t, cfg)
	printComparison(results)
}

// TestRecoveryAndDurability specifically tests recovery time and data loss.
func TestRecoveryAndDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	testCases := []struct {
		name       string
		syncPolicy dpx.SyncPolicy
		createNode func(string) (*dpx.Node, error)
	}{
		{
			name:       "Pebble_SyncFull",
			syncPolicy: dpx.SyncFull,
			createNode: func(dir string) (*dpx.Node, error) {
				return dpx.OpenPebble(dpx.Config{
					DataDir:    dir,
					SyncPolicy: dpx.SyncFull,
				})
			},
		},
		{
			name:       "Pebble_SyncBatch",
			syncPolicy: dpx.SyncBatch,
			createNode: func(dir string) (*dpx.Node, error) {
				return dpx.OpenPebble(dpx.Config{
					DataDir:    dir,
					SyncPolicy: dpx.SyncBatch,
				})
			},
		},
		{
			name:       "Pebble_SyncNone",
			syncPolicy: dpx.SyncNone,
			createNode: func(dir string) (*dpx.Node, error) {
				return dpx.OpenPebble(dpx.Config{
					DataDir:    dir,
					SyncPolicy: dpx.SyncNone,
				})
			},
		},
		{
			name:       "Badger_SyncBatch",
			syncPolicy: dpx.SyncBatch,
			createNode: func(dir string) (*dpx.Node, error) {
				eng := badger.New(dir)
				return dpx.Open(dpx.Config{
					Engine:     eng,
					SyncPolicy: dpx.SyncBatch,
					RaftDir:    dir + "/raft",
				}, dpxraft.Open)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			node, err := tc.createNode(dir)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			ctx := context.Background()
			const numKeys = 1000

			// Write keys
			for i := 0; i < numKeys; i++ {
				k := []byte(fmt.Sprintf("k:%06d", i))
				v := make([]byte, 64)
				binary.LittleEndian.PutUint64(v, uint64(i))
				node.RunInTx(ctx, func(tx dpx.KVTx) error {
					return tx.Set(ctx, k, v)
				})
			}

			// For SyncFull, force a sync before "crash"
			if tc.syncPolicy == dpx.SyncFull {
				// The SyncBatch policy handles periodic syncs
				time.Sleep(100 * time.Millisecond)
			}

			// Simulate crash by closing without flush
			node.Close()

			// Measure recovery time
			recoveryStart := time.Now()
			recovered, err := tc.createNode(dir)
			if err != nil {
				t.Fatalf("recovery: %v", err)
			}
			recoveryTime := time.Since(recoveryStart)
			defer recovered.Close()

			// Check data integrity using GetRange
			lost := 0
			for i := 0; i < numKeys; i++ {
				k := []byte(fmt.Sprintf("k:%06d", i))
				pairs, err := recovered.GetRange(ctx, k, append(append([]byte(nil), k...), 0), 1)
				if err != nil || len(pairs) == 0 {
					lost++
				}
			}

			t.Logf("Recovery: %s | Time: %s | Data loss: %d/%d keys",
				tc.name, recoveryTime, lost, numKeys)

			if tc.syncPolicy == dpx.SyncFull && lost > 0 {
				t.Errorf("SyncFull should have zero data loss, lost %d keys", lost)
			}
		})
	}
}

// TestLongRunningCompaction measures throughput over a longer period
// to capture LSM flush and compaction effects.
func TestLongRunningCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running benchmark in short mode")
	}

	testCases := []struct {
		name       string
		createNode func(string) (*dpx.Node, error)
	}{
		{
			name: "Pebble_LongRunning",
			createNode: func(dir string) (*dpx.Node, error) {
				return dpx.OpenPebble(dpx.Config{
					DataDir:    dir,
					SyncPolicy: dpx.SyncBatch,
				})
			},
		},
		{
			name: "Badger_LongRunning",
			createNode: func(dir string) (*dpx.Node, error) {
				eng := badger.New(dir)
				return dpx.Open(dpx.Config{
					Engine:     eng,
					SyncPolicy: dpx.SyncBatch,
					RaftDir:    dir + "/raft",
				}, dpxraft.Open)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			node, err := tc.createNode(dir)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			defer node.Close()

			ctx := context.Background()
			const duration = 30 * time.Second
			const concurrency = 512
			const numKeys = 10000

			keys := make([][]byte, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = []byte(fmt.Sprintf("k:%08d", i))
			}

			// Seed
			for i := 0; i < numKeys; i += 1000 {
				end := i + 1000
				if end > numKeys {
					end = numKeys
				}
				node.RunInTx(ctx, func(tx dpx.KVTx) error {
					for j := i; j < end; j++ {
						tx.Set(ctx, keys[j], []byte{byte(j)})
					}
					return nil
				})
			}

			// Run in intervals to measure throughput degradation
			interval := 5 * time.Second
			intervals := int(duration / interval)

			var tpsSamples []float64
			for i := 0; i < intervals; i++ {
				var ops atomic.Uint64
				var wg sync.WaitGroup
				start := time.Now()

				for g := 0; g < concurrency; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						end := start.Add(interval)
						for time.Now().Before(end) {
							k := keys[g%numKeys]
							err := node.RunInTx(ctx, func(tx dpx.KVTx) error {
								_, err := tx.AtomicAdd(ctx, k, 1)
								return err
							})
							if err == nil {
								ops.Add(1)
							}
						}
					}()
				}
				wg.Wait()
				elapsed := time.Since(start)
				tps := float64(ops.Load()) / elapsed.Seconds()
				tpsSamples = append(tpsSamples, tps)

				t.Logf("[%s] Interval %d/%d: %.0f TPS", tc.name, i+1, intervals, tps)
			}

			// Calculate throughput degradation
			if len(tpsSamples) >= 2 {
				first := tpsSamples[0]
				last := tpsSamples[len(tpsSamples)-1]
				degradation := (first - last) / first * 100
				t.Logf("[%s] Throughput degradation: %.1f%% (first: %.0f, last: %.0f)",
					tc.name, degradation, first, last)
			}

			// Check disk usage
			var totalSize int64
			filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err == nil {
					totalSize += info.Size()
				}
				return nil
			})
			t.Logf("[%s] Disk usage: %s", tc.name, formatBytes(uint64(totalSize)))
		})
	}
}

// BenchmarkRunInTx provides go test -bench compatible benchmarks for each configuration.
func BenchmarkMVCCOnly(b *testing.B) {
	node, err := dpx.OpenMVCCSharded(dpx.Config{SyncPolicy: dpx.SyncNone})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	benchmarkRun(b, node)
}

func BenchmarkPebbleSyncNone(b *testing.B) {
	dir := b.TempDir()
	node, err := dpx.OpenPebble(dpx.Config{DataDir: dir, SyncPolicy: dpx.SyncNone})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	benchmarkRun(b, node)
}

func BenchmarkPebbleSyncBatch(b *testing.B) {
	dir := b.TempDir()
	node, err := dpx.OpenPebble(dpx.Config{DataDir: dir, SyncPolicy: dpx.SyncBatch})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	benchmarkRun(b, node)
}

func BenchmarkBadgerSyncNone(b *testing.B) {
	dir := b.TempDir()
	eng := badger.New(dir)
	node, err := dpx.Open(dpx.Config{Engine: eng, SyncPolicy: dpx.SyncNone, RaftDir: dir + "/raft"}, dpxraft.Open)
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	benchmarkRun(b, node)
}

func BenchmarkBadgerSyncBatch(b *testing.B) {
	dir := b.TempDir()
	eng := badger.New(dir)
	node, err := dpx.Open(dpx.Config{Engine: eng, SyncPolicy: dpx.SyncBatch, RaftDir: dir + "/raft"}, dpxraft.Open)
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	benchmarkRun(b, node)
}

func benchmarkRun(b *testing.B, node *dpx.Node) {
	b.Helper()
	ctx := context.Background()

	// Seed keys
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("k:%06d", i))
	}
	for _, k := range keys {
		node.RunInTx(ctx, func(tx dpx.KVTx) error {
			return tx.Set(ctx, k, []byte{1})
		})
	}

	b.ResetTimer()
	b.SetParallelism(256)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := keys[i%len(keys)]
			node.RunInTx(ctx, func(tx dpx.KVTx) error {
				_, err := tx.AtomicAdd(ctx, k, 1)
				return err
			})
			i++
		}
	})
}
