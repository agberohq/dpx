# DPX — Distributed Persistent Store

Strict-serializable KV store built for high-throughput micro-transaction banking workloads.

## Architecture

DPX provides a single abstraction: `RunInTx`, which executes a function in a strictly-serializable transaction with automatic OCC (Optimistic Concurrency Control) conflict detection and retry.

```
┌──────────────────────────────────────────────────────────────┐
│  Teller (banking application)                                │
│  Wallets, Transfers, Fees, Reversals, Ledger, Settlement     │
└──────────────────────────┬───────────────────────────────────┘
                           │ RunInTx(fn)
┌──────────────────────────▼───────────────────────────────────┐
│  DPX (distributed KV store)                                  │
│                                                              │
│  ┌────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │ Speculative │  │ OCC Conflict │  │ Engine (Pebble /   │  │
│  │ Execution   │→ │ Detection    │→ │ Memory / Badger)   │  │
│  └────────────┘  └──────────────┘  └────────────────────┘  │
│                                                              │
│  Credit/Debit semantics: credits commute, debits conflict    │
└──────────────────────────────────────────────────────────────┘
```

## Quick Start

```go
import "github.com/agberohq/dpx"

// In-memory, 64-shard (production recommended)
node, err := dpx.OpenSharded(dpx.Config{})

// In-memory, single-shard (testing)
node, err := dpx.OpenEmbedded(dpx.Config{})

// Pebble, durable (embedded disk)
node, err := dpx.OpenPebble(dpx.Config{DataDir: "/var/lib/dpx"})

// Transfer: debit sender, credit receiver
err = node.RunInTx(ctx, func(tx dpx.KVTx) error {
    newBal, err := tx.AtomicAdd(ctx, []byte("wallet:alice"), -100)
    if err != nil || newBal < 0 {
        return errors.New("insufficient funds")
    }
    _, err = tx.AtomicAdd(ctx, []byte("wallet:bob"), 100)
    return err
})
```

## Credit/Debit Semantics

DPX distinguishes between two types of writes:

| Operation | OCC Behavior | Use Case |
|-----------|-------------|----------|
| **Credit** (`AtomicAdd` with `delta > 0`) | Commutes — no read-set, no false conflicts | Deposits, refunds, interest |
| **Debit** (`AtomicAdd` with `delta <= 0`) | Full OCC — read-set entry, conflicts on concurrent debits | Withdrawals, transfers, fees |

This means concurrent credits to the same key never conflict, while concurrent debits correctly detect conflicts and retry. A credit followed by a debit on the same key never false-conflicts.

## Modes

### Embedded (Single Shard)

Single-process, single-shard. Best for testing and low-concurrency workloads.

```go
node, _ := dpx.OpenEmbedded(dpx.Config{})
```

### Sharded (64 Shards, B-tree)

Single-process, 64 independent shards using a copy-on-write B-tree. Each shard has its own conflict detection and apply path.

```go
node, _ := dpx.OpenSharded(dpx.Config{})
```

### MVCC Sharded (64 Shards, Hash Map) — Recommended

Single-process, 64 independent shards using MVCC (Multi-Version Concurrency Control) with hash map storage. **Recommended for Teller production.**

**Key advantages over B-tree sharded:**
- **4x faster**: ~670k TPS vs ~170k TPS
- **Atomic commit protocol**: All keys in a batch share one commit sequence, so snapshots never see partial transactions
- **O(1) snapshots**: No COW tree cloning — just read a sequence counter
- **Automatic compaction**: Background goroutine reclaims old versions while respecting active snapshots
- **Flat scaling**: Performance doesn't degrade with key count

```go
node, _ := dpx.OpenMVCCSharded(dpx.Config{})
```

**MVCC correctness guarantees:**
- **Atomic visibility**: A multi-key batch is either fully visible or fully invisible to any snapshot
- **Snapshot isolation**: Repeated reads within a snapshot always return the same value
- **Safe compaction**: Old versions are never removed while an active snapshot might read them
- **Tombstone cleanup**: Deleted keys are fully removed by compaction when no snapshot references them

### Pebble (Durable)

Pebble LSM-tree backend. Data persists across restarts. Use when durability is required.

```go
node, _ := dpx.OpenPebble(dpx.Config{DataDir: "/var/lib/dpx"})
```

### Distributed (Raft)

Multi-node cluster with HashiCorp Raft consensus. Use for cross-datacenter replication.

```go
node, _ := dpx.Open(dpx.Config{
    Engine:     pebble.New("/var/lib/dpx"),
    ListenAddr: "0.0.0.0:8200",
    Peers: map[string]string{
        "1": "10.0.0.1:8200",
        "2": "10.0.0.2:8200",
        "3": "10.0.0.3:8200",
    },
}, dpxraft.Open)
```

## API

### Node

| Method | Description |
|--------|-------------|
| `RunInTx(ctx, fn)` | Execute fn in a strictly-serializable transaction |
| `GetRange(ctx, start, end, limit)` | Forward range scan outside a transaction |
| `GetRangeReverse(ctx, start, end, limit)` | Reverse range scan outside a transaction |
| `WatchKey(ctx, prefix)` | Best-effort notification channel for key prefix changes |
| `GetDatabaseTime(ctx)` | Current HLC timestamp |
| `GetSafeReadPoint(ctx)` | Safe sequence for change feed consumption |
| `Backup(ctx, destDir)` | Write consistent engine checkpoint |
| `Health()` | Current health status (alive, transactions, conflicts) |
| `Close()` | Graceful shutdown |

### KVTx (Transaction)

| Method | Description |
|--------|-------------|
| `Get(ctx, key)` | Read a value (creates read-set entry) |
| `Set(ctx, key, value)` | Write a value |
| `Delete(ctx, key)` | Remove a key |
| `AtomicAdd(ctx, key, delta)` | Atomic int64 addition (credit/debit semantics) |
| `GetRange(ctx, start, end, limit)` | Advisory range scan (no read-set) |
| `AllocateNextSequence(ctx)` | Monotonic sequence number |

## Configuration

```go
dpx.Config{
    DataDir:         "/var/lib/dpx",
    SyncPolicy:      dpx.SyncBatch,  // fsync once per batch (default)
    ShutdownTimeout: 30 * time.Second,
    Batch: dpx.BatchConfig{
        MaxEntries: 256,
        MaxBytes:   4 << 20,  // 4MB
        MinAge:     10 * time.Microsecond,
        MaxAge:     500 * time.Microsecond,
    },
    Retry: dpx.RetryConfig{
        MaxAttempts: 50,
        BaseDelay:   time.Millisecond,
        MaxDelay:    100 * time.Millisecond,
        Multiplier:  2.0,
    },
    Metrics:   &dpx.Metrics{},
    Telemetry: &dpx.Telemetry{},
}
```

## Metrics

```go
metrics := &dpx.Metrics{}
node, _ := dpx.OpenSharded(dpx.Config{Metrics: metrics})

// Read metrics
fmt.Println("Conflicts:", metrics.ConflictTotal.Load())
fmt.Println("Transactions:", metrics.TransactionsTotal.Load())
```

## Health Checks

```go
h := node.Health()
if !h.Alive {
    log.Fatal("node not accepting transactions")
}
fmt.Printf("Transactions: %d, Conflicts: %d\n",
    h.Transactions, h.Conflicts)
```

## Performance

| Engine | TPS | Latency (mean) | Latency (p99) | vs Target |
|--------|-----|---------------|---------------|-----------|
| MVCC Sharded | ~960k | 2.1ms | 35ms | 1.6x slower |
| B-tree Sharded | ~170k | 12ms | 92ms | 9.2x slower |
| Target (Teller) | 1.5M | — | — | — |

For 200M transactions/day (~2,300 TPS sustained), both sharded modes exceed requirements by 70-400x. For burst workloads (500k-1.5M TPS), MVCC sharded is the recommended engine.

Key optimizations:

- **Inline apply**: Sharded mode applies proposals directly without channel overhead
- **CAS-based locking**: Lock-free shard serialization — writers serialized by CAS, RWMutex only excludes readers
- **ApplyShardDirect**: Lock-free path for MVCC engine that skips batch interface overhead
- **Object pooling**: Proposal entries and version key buffers are recycled
- **Credit commutativity**: Concurrent credits never conflict
- **250µs flush interval**: Minimal batch accumulation delay
- **MVCC atomic commits**: All keys in a batch share one sequence, eliminating partial visibility
- **MVCC auto-compaction**: Background goroutine reclaims old versions safely

### Performance bottleneck analysis

CPU profiling reveals the remaining bottleneck preventing 1.5M TPS:

| Component | CPU % | Description |
|-----------|-------|-------------|
| `pthread_cond_wait` | 28% | RWMutex slow path — reader-writer exclusion between snapshots and writes |
| `pthread_cond_signal` | 13% | RWMutex unlock signaling |
| `usleep` | 10% | CAS spinlock yield in `applyInline` |
| `madvise` | 8% | Memory allocation |
| `lockSlow` | 19% (cumulative) | RWMutex contention in MVCC shard |

The `keyEpoch` map is already per-shard (64 independent FSMs), so there is no shared epoch contention. The RWMutex per shard is needed for reader-writer exclusion: snapshots (readers) must not see partially-written data during `ApplyShardDirect` (writers). Since the CAS spinlock already serializes writers, the RWMutex only contends with readers, not other writers.

An atomic pointer swap (COW map) approach was tested but proved too expensive: copying a 100K-key map on every write reduced throughput from 1.19M to 6.9K TPS. The RWMutex remains the correct choice for this workload.

## Relationship with Teller

DPX is the **storage layer** for Teller. Teller owns all banking business logic:

- **Teller**: Wallets, transfers, fees, reversals, escrow, contracts, idempotency, signing, ledger, settlement
- **DPX**: Strict-serializable KV storage with OCC, credit/debit semantics, and durability

DPX does not duplicate any Teller functionality. It provides only the `RunInTx` transaction boundary and the underlying KV operations.

## License

MIT
