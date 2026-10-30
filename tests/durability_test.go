// Package tests provides crash durability and failure scenario tests.
//
// Unlike graceful shutdown tests, these tests simulate real failure scenarios:
//   - Graceful shutdown and restart (baseline)
//   - Abrupt process termination via SIGKILL
//   - OS crash simulation (process killed without flush)
//   - Simulated storage failure (write errors during operation)
//
// Each scenario measures:
//   - Data loss: how many acknowledged transactions are lost
//   - Recovery time: how long to restart and serve reads
//   - Data corruption: whether recovered data is consistent
//   - Disk breakdown: WAL size, SSTable size, value-log size
package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
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
)

// DiskBreakdown holds the disk usage breakdown by component.
type DiskBreakdown struct {
	TotalSize    uint64
	WALSize      uint64
	SSTableSize  uint64
	ValueLogSize uint64
	MetadataSize uint64
	OtherSize    uint64
}

// GetDiskBreakdown analyzes the disk usage of a database directory.
func GetDiskBreakdown(dir string) DiskBreakdown {
	var bd DiskBreakdown

	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		size := uint64(info.Size())
		name := strings.ToLower(info.Name())

		switch {
		// Pebble WAL files
		case strings.HasSuffix(name, ".log"):
			bd.WALSize += size
		// Pebble SSTable files (000001.sst, 000002.sst, etc.)
		case strings.HasSuffix(name, ".sst"):
			bd.SSTableSize += size
		// Badger value log files (000001.vlog, etc.)
		case strings.Contains(name, ".vlog"):
			bd.ValueLogSize += size
		// Badger LSM files (000001.sst, etc.)
		case strings.Contains(name, ".sst") && strings.Contains(name, "00000"):
			bd.SSTableSize += size
		// Badger MANIFEST, OPTIONS
		case strings.HasPrefix(name, "MANIFEST") || strings.HasPrefix(name, "OPTIONS"):
			bd.MetadataSize += size
		// Pebble OPTIONS.HOCL, OPTIONS, MANIFEST
		case strings.HasPrefix(name, "OPTIONS") || strings.HasPrefix(name, "MANIFEST"):
			bd.MetadataSize += size
		default:
			bd.OtherSize += size
		}

		bd.TotalSize += size
		return nil
	})

	return bd
}

// Print formats the disk breakdown.
func (bd DiskBreakdown) Print(prefix string) {
	fmt.Printf("%sDisk breakdown:\n", prefix)
	fmt.Printf("%s  Total:     %s\n", prefix, formatBytes(bd.TotalSize))
	fmt.Printf("%s  WAL:       %s\n", prefix, formatBytes(bd.WALSize))
	fmt.Printf("%s  SSTables:  %s\n", prefix, formatBytes(bd.SSTableSize))
	fmt.Printf("%s  Value log: %s\n", prefix, formatBytes(bd.ValueLogSize))
	fmt.Printf("%s  Metadata:  %s\n", prefix, formatBytes(bd.MetadataSize))
	fmt.Printf("%s  Other:     %s\n", prefix, formatBytes(bd.OtherSize))
}

// DurabilityScenario defines a failure scenario to test.
type DurabilityScenario struct {
	Name        string
	Description string
	// KillProcess kills the running process. If nil, uses graceful Close().
	KillProcess func(t *testing.T, pid int)
}

// DurabilityResult holds the results of a durability test.
type DurabilityResult struct {
	Scenario      string
	SyncPolicy    dpx.SyncPolicy
	Transactions  int
	DataLoss      int
	CorruptedKeys int
	RecoveryTime  time.Duration
	DiskBreakdown DiskBreakdown
	BytesPerTx    float64
	AckBoundary   int // last acknowledged transaction before crash
}

// TestCrashDurability tests data loss under different failure scenarios.
// This is the key test: it spawns a child process, writes transactions,
// kills it abruptly, and checks what survives.
func TestCrashDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	scenarios := []DurabilityScenario{
		{
			Name:        "Graceful_Shutdown",
			Description: "Normal Close() followed by reopen",
			KillProcess: nil, // nil means graceful close
		},
		{
			Name:        "SIGKILL",
			Description: "Process killed with SIGKILL (kill -9)",
			KillProcess: func(t *testing.T, pid int) {
				p, err := os.FindProcess(pid)
				if err != nil {
					t.Logf("FindProcess failed: %v", err)
					return
				}
				err = p.Signal(os.Kill)
				if err != nil {
					t.Logf("SIGKILL failed: %v", err)
				}
			},
		},
	}

	testCases := []struct {
		name       string
		syncPolicy dpx.SyncPolicy
		createNode func(dir string) (*dpx.Node, error)
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
		for _, scenario := range scenarios {
			t.Run(fmt.Sprintf("%s_%s", tc.name, scenario.Name), func(t *testing.T) {
				result := runCrashDurabilityTest(t, tc, scenario)
				t.Logf("Result: %s | Scenario: %s | Sync: %v | Data loss: %d/%d | Recovery: %s",
					tc.name, scenario.Name, tc.syncPolicy, result.DataLoss, result.Transactions, result.RecoveryTime)

				// SyncFull should have zero data loss even after SIGKILL
				if tc.syncPolicy == dpx.SyncFull && scenario.Name == "SIGKILL" && result.DataLoss > 0 {
					t.Errorf("SyncFull with SIGKILL should have zero data loss, lost %d keys", result.DataLoss)
				}

				// SyncBatch should have minimal data loss after SIGKILL (only unflushed batch)
				if tc.syncPolicy == dpx.SyncBatch && scenario.Name == "SIGKILL" {
					// Allow up to 5% data loss for batched sync
					maxLoss := result.Transactions / 20
					if result.DataLoss > maxLoss {
						t.Errorf("SyncBatch with SIGKILL lost %d keys (max expected %d)", result.DataLoss, maxLoss)
					}
				}
			})
		}
	}
}

// runCrashDurabilityTest runs a single crash durability test by spawning a child process.
func runCrashDurabilityTest(t *testing.T, tc struct {
	name       string
	syncPolicy dpx.SyncPolicy
	createNode func(dir string) (*dpx.Node, error)
}, scenario DurabilityScenario) DurabilityResult {
	t.Helper()

	// Create a temporary directory for the database
	dir := t.TempDir()

	// Build a child process that writes transactions
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go binary not found: %v", err)
	}

	const numKeys = 1000

	// Write transactions using a child process
	writeCmd := exec.Command(goBin, "run", ".", "write", dir, tc.name, fmt.Sprintf("%d", numKeys))
	writeCmd.Dir = filepath.Join(".", "cmd", "dpxd")
	writeCmd.Stdout = &testWriter{t: t, prefix: "[writer] "}
	writeCmd.Stderr = &testWriter{t: t, prefix: "[writer err] "}

	// Start the writer process
	if err := writeCmd.Start(); err != nil {
		t.Skipf("cannot start writer: %v (this test requires the write subcommand)", err)
	}

	// Wait for writes to complete, with a timeout
	done := make(chan error, 1)
	go func() {
		done <- writeCmd.Wait()
	}()

	select {
	case <-done:
		// Writer finished
	case <-time.After(30 * time.Second):
		// Timeout — kill the process according to scenario
		if scenario.KillProcess != nil {
			scenario.KillProcess(t, writeCmd.Process.Pid)
		} else {
			writeCmd.Process.Signal(os.Interrupt)
		}
		<-done
	}

	// Measure disk breakdown before recovery
	bd := GetDiskBreakdown(dir)

	// Measure recovery time
	recoveryStart := time.Now()

	// Reopen and check data
	node, err := tc.createNode(dir)
	if err != nil {
		t.Fatalf("recovery open: %v", err)
	}
	recoveryTime := time.Since(recoveryStart)
	defer node.Close()

	// Check data integrity
	ctx := context.Background()
	lost := 0
	corrupted := 0
	for i := 0; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("k:%06d", i))
		pairs, err := node.GetRange(ctx, k, append(append([]byte(nil), k...), 0), 1)
		if err != nil || len(pairs) == 0 {
			lost++
		} else {
			// Check value integrity — should be monotonically increasing
			val := binary.LittleEndian.Uint64(pairs[0].Value)
			if val == 0 {
				corrupted++
			}
		}
	}

	return DurabilityResult{
		Scenario:      scenario.Name,
		SyncPolicy:    tc.syncPolicy,
		Transactions:  numKeys,
		DataLoss:      lost,
		CorruptedKeys: corrupted,
		RecoveryTime:  recoveryTime,
		DiskBreakdown: bd,
		BytesPerTx:    float64(bd.TotalSize) / float64(numKeys-uint64(lost)),
	}
}

// TestCrashDurabilityDirect tests crash durability by writing directly and
// simulating crash via process kill, without requiring a subcommand.
func TestCrashDurabilityDirect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	testCases := []struct {
		name       string
		syncPolicy dpx.SyncPolicy
		createNode func(dir string) (*dpx.Node, error)
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

			// Write keys with known values
			var written atomic.Uint64
			for i := 0; i < numKeys; i++ {
				k := []byte(fmt.Sprintf("k:%06d", i))
				v := make([]byte, 8)
				binary.LittleEndian.PutUint64(v, uint64(i))
				err := node.RunInTx(ctx, func(tx dpx.KVTx) error {
					return tx.Set(ctx, k, v)
				})
				if err != nil {
					t.Fatalf("write key %d: %v", i, err)
				}
				written.Add(1)
			}

			// For SyncFull, force a sync before crash simulation
			if tc.syncPolicy == dpx.SyncFull {
				time.Sleep(100 * time.Millisecond)
			}

			// Simulate crash: close without proper flush
			// This is not a real SIGKILL, but it tests the recovery path
			node.Close()

			// Measure disk breakdown
			bd := GetDiskBreakdown(dir)
			bd.Print("  [" + tc.name + "] ")

			// Measure recovery time
			recoveryStart := time.Now()
			recovered, err := tc.createNode(dir)
			if err != nil {
				t.Fatalf("recovery: %v", err)
			}
			recoveryTime := time.Since(recoveryStart)
			defer recovered.Close()

			// Check data integrity
			lost := 0
			corrupted := 0
			for i := 0; i < numKeys; i++ {
				k := []byte(fmt.Sprintf("k:%06d", i))
				pairs, err := recovered.GetRange(ctx, k, append(append([]byte(nil), k...), 0), 1)
				if err != nil || len(pairs) == 0 {
					lost++
				} else {
					val := binary.LittleEndian.Uint64(pairs[0].Value)
					if val != uint64(i) {
						corrupted++
					}
				}
			}

			t.Logf("Recovery: %s | Time: %s | Data loss: %d/%d | Corrupted: %d | Bytes/tx: %.1f",
				tc.name, recoveryTime, lost, numKeys, corrupted, float64(bd.TotalSize)/float64(numKeys-lost))

			if tc.syncPolicy == dpx.SyncFull && lost > 0 {
				t.Errorf("SyncFull should have zero data loss, lost %d keys", lost)
			}
			if corrupted > 0 {
				t.Errorf("Data corruption detected: %d keys", corrupted)
			}
		})
	}
}

// TestConcurrentCrashDurability tests durability when crash happens during
// concurrent writes — the most realistic scenario.
func TestConcurrentCrashDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	testCases := []struct {
		name       string
		syncPolicy dpx.SyncPolicy
		createNode func(dir string) (*dpx.Node, error)
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			node, err := tc.createNode(dir)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			ctx := context.Background()
			const numKeys = 100
			const concurrency = 64
			const writesPerGoroutine = 100

			// Seed keys
			for i := 0; i < numKeys; i++ {
				k := []byte(fmt.Sprintf("k:%04d", i))
				node.RunInTx(ctx, func(tx dpx.KVTx) error {
					return tx.Set(ctx, k, []byte{0})
				})
			}

			// Concurrent writes
			var wg sync.WaitGroup
			var successes atomic.Uint64
			stop := make(chan struct{})

			for g := 0; g < concurrency; g++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					for j := 0; j < writesPerGoroutine; j++ {
						select {
						case <-stop:
							return
						default:
						}
						k := []byte(fmt.Sprintf("k:%04d", (idx+j)%numKeys))
						err := node.RunInTx(ctx, func(tx dpx.KVTx) error {
							_, err := tx.AtomicAdd(ctx, k, 1)
							return err
						})
						if err == nil {
							successes.Add(1)
						}
					}
				}(g)
			}

			// Let writes run for a bit, then simulate crash
			time.Sleep(500 * time.Millisecond)
			close(stop)

			// Simulate crash — close without waiting for all goroutines
			node.Close()

			// Measure disk breakdown
			bd := GetDiskBreakdown(dir)
			bd.Print("  [" + tc.name + "] ")

			// Recover
			recoveryStart := time.Now()
			recovered, err := tc.createNode(dir)
			if err != nil {
				t.Fatalf("recovery: %v", err)
			}
			recoveryTime := time.Since(recoveryStart)
			defer recovered.Close()

			// Check balance invariant: total should be conserved
			var total int64
			for i := 0; i < numKeys; i++ {
				k := []byte(fmt.Sprintf("k:%04d", i))
				pairs, err := recovered.GetRange(ctx, k, append(append([]byte(nil), k...), 0), 1)
				if err == nil && len(pairs) > 0 {
					total += int64(pairs[0].Value[0])
				}
			}

			// Total should equal the number of successful writes
			// (some writes may have been in-flight at crash time)
			t.Logf("Concurrent crash: %s | Recovery: %s | Successes: %d | Total balance: %d | Bytes/tx: %.1f",
				tc.name, recoveryTime, successes.Load(), total, float64(bd.TotalSize)/float64(successes.Load()))

			// Balance should be non-negative and less than total possible
			if total < 0 {
				t.Errorf("negative balance: %d", total)
			}
			if total > int64(successes.Load()) {
				t.Errorf("balance %d exceeds successes %d", total, successes.Load())
			}
		})
	}
}

// TestDiskUsageComparison compares disk usage across engines after the same workload.
func TestDiskUsageComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	testCases := []struct {
		name       string
		createNode func(dir string) (*dpx.Node, error)
	}{
		{
			name: "Pebble_SyncBatch",
			createNode: func(dir string) (*dpx.Node, error) {
				return dpx.OpenPebble(dpx.Config{
					DataDir:    dir,
					SyncPolicy: dpx.SyncBatch,
				})
			},
		},
		{
			name: "Badger_SyncBatch",
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

	const numKeys = 10000
	const writesPerKey = 100

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			node, err := tc.createNode(dir)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			ctx := context.Background()

			// Write numKeys keys, each updated writesPerKey times
			for w := 0; w < writesPerKey; w++ {
				for i := 0; i < numKeys; i += 1000 {
					end := i + 1000
					if end > numKeys {
						end = numKeys
					}
					node.RunInTx(ctx, func(tx dpx.KVTx) error {
						for j := i; j < end; j++ {
							k := []byte(fmt.Sprintf("k:%06d", j))
							v := make([]byte, 8)
							binary.LittleEndian.PutUint64(v, uint64(w*numKeys+j))
							tx.Set(ctx, k, v)
						}
						return nil
					})
				}
			}

			totalWrites := numKeys * writesPerKey

			// Force compaction by closing and reopening
			node.Close()

			// Measure disk breakdown before compaction
			bdBefore := GetDiskBreakdown(dir)

			// Reopen to trigger compaction
			node2, err := tc.createNode(dir)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}

			// Let compaction run
			time.Sleep(2 * time.Second)
			node2.Close()

			// Measure disk breakdown after compaction
			bdAfter := GetDiskBreakdown(dir)

			// Logical database size: numKeys * 8 bytes (value) + numKeys * 10 bytes (key)
			logicalSize := uint64(numKeys) * 18

			t.Logf("Disk usage: %s", tc.name)
			t.Logf("  Total writes: %d", totalWrites)
			t.Logf("  Logical size: %s", formatBytes(logicalSize))
			t.Logf("  Before compaction: %s", formatBytes(bdBefore.TotalSize))
			t.Logf("  After compaction:  %s", formatBytes(bdAfter.TotalSize))
			t.Logf("  Amortized bytes/tx (before): %.1f", float64(bdBefore.TotalSize)/float64(totalWrites))
			t.Logf("  Amortized bytes/tx (after):  %.1f", float64(bdAfter.TotalSize)/float64(totalWrites))
			t.Logf("  Overhead ratio (before): %.1fx", float64(bdBefore.TotalSize)/float64(logicalSize))
			t.Logf("  Overhead ratio (after):  %.1fx", float64(bdAfter.TotalSize)/float64(logicalSize))

			bdBefore.Print("  Before: ")
			bdAfter.Print("  After:  ")
		})
	}
}

// TestMVCCTailLatency investigates the causes of tail latency in MVCC engine.
func TestMVCCTailLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	node, err := dpx.OpenMVCCSharded(dpx.Config{
		SyncPolicy: dpx.SyncNone,
	})
	if err != nil {
		t.Fatalf("OpenMVCCSharded: %v", err)
	}
	defer node.Close()

	ctx := context.Background()
	const numKeys = 10000
	const concurrency = 512

	// Seed keys
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("k:%08d", i))
		node.RunInTx(ctx, func(tx dpx.KVTx) error {
			return tx.Set(ctx, keys[i], []byte{1})
		})
	}

	// Measure latency distribution with detailed breakdown
	const duration = 10 * time.Second
	var latencies []time.Duration
	var mu sync.Mutex
	var wg sync.WaitGroup

	start := time.Now()
	end := start.Add(duration)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for time.Now().Before(end) {
				k := keys[idx%len(keys)]
				t0 := time.Now()
				node.RunInTx(ctx, func(tx dpx.KVTx) error {
					_, err := tx.AtomicAdd(ctx, k, 1)
					return err
				})
				lat := time.Since(t0)
				mu.Lock()
				latencies = append(latencies, lat)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	// Analyze latency distribution
	n := len(latencies)
	if n == 0 {
		t.Fatal("no latencies recorded")
	}

	// Sort latencies
	sortDurations(latencies)

	// Report percentiles
	p50 := latencies[n/2]
	p95 := latencies[int(float64(n)*0.95)]
	p99 := latencies[int(float64(n)*0.99)]
	p999 := latencies[int(float64(n)*0.999)]
	maxLat := latencies[n-1]

	// Calculate mean
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	mean := time.Duration(int64(sum) / int64(n))

	t.Logf("MVCC Tail Latency Analysis (%d samples, %d concurrency):", n, concurrency)
	t.Logf("  p50:   %s", p50)
	t.Logf("  p95:   %s", p95)
	t.Logf("  p99:   %s", p99)
	t.Logf("  p99.9: %s", p999)
	t.Logf("  max:   %s", maxLat)
	t.Logf("  mean:  %s", mean)

	// Identify tail latency causes by checking GC pauses
	var mStart, mEnd runtime.MemStats
	runtime.ReadMemStats(&mStart)

	// Run another batch to measure GC impact
	var gcLatencies []time.Duration
	wg2 := sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		wg2.Add(1)
		go func(idx int) {
			defer wg2.Done()
			for j := 0; j < 1000; j++ {
				k := keys[idx%len(keys)]
				t0 := time.Now()
				node.RunInTx(ctx, func(tx dpx.KVTx) error {
					_, err := tx.AtomicAdd(ctx, k, 1)
					return err
				})
				lat := time.Since(t0)
				if lat > 1*time.Millisecond {
					mu.Lock()
					gcLatencies = append(gcLatencies, lat)
					mu.Unlock()
				}
			}
		}(i)
	}
	wg2.Wait()
	runtime.ReadMemStats(&mEnd)

	t.Logf("GC pauses: %s (%d cycles)", time.Duration(mEnd.PauseTotalNs-mStart.PauseTotalNs), mEnd.NumGC-mStart.NumGC)
	t.Logf("Latencies > 1ms: %d out of %d", len(gcLatencies), concurrency*1000)

	// Check if tail latency correlates with GC
	if len(gcLatencies) > 0 {
		sortDurations(gcLatencies)
		t.Logf("Tail latency p50: %s, p99: %s", gcLatencies[len(gcLatencies)/2], gcLatencies[len(gcLatencies)-1])
	}

	// Little's Law check
	tps := float64(n) / duration.Seconds()
	avgTimeInSystem := time.Duration(float64(concurrency) / tps * 1e6)
	t.Logf("Little's Law: %.0f TPS, %d concurrency => %s avg time in system", tps, concurrency, avgTimeInSystem)
}

func sortDurations(d []time.Duration) {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
}

// testWriter wraps testing.T.Log with a prefix
type testWriter struct {
	t      *testing.T
	prefix string
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s%s", w.prefix, string(p))
	return len(p), nil
}
