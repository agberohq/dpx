#!/usr/bin/env bash
set -e

echo "🔨 Building runner..."
go build -o dpx-bench ./runner.go

DURATION="10s"
KEYS=100000
RUNS=3   # each scenario averaged across N timed runs; warmup fires once

# ─── MEMORY (Global Lock) ────────────────────────────────────────────────────
echo ""
echo "================================================================="
echo " 1. EMBEDDED MEMORY — write  (Global Lock, baseline)"
echo "================================================================="
./dpx-bench -engine memory -mode embedded -concurrency 512 \
            -duration $DURATION -keys $KEYS -workload write -runs $RUNS

echo ""
echo "================================================================="
echo " 2. EMBEDDED MEMORY — read"
echo "================================================================="
./dpx-bench -engine memory -mode embedded -concurrency 512 \
            -duration $DURATION -keys $KEYS -workload read -runs $RUNS

echo ""
echo "================================================================="
echo " 3. EMBEDDED MEMORY — mixed  (70% read, 20% atomic, 10% write)"
echo "================================================================="
./dpx-bench -engine memory -mode embedded -concurrency 512 \
            -duration $DURATION -keys $KEYS -workload mixed -runs $RUNS -warmup 5s

# ─── MEMORY SHARDED (Zero Lock) ──────────────────────────────────────────────
echo ""
echo "================================================================="
echo " 4a. EMBEDDED MEMORY SHARDED — write, random keys (baseline)"
echo "================================================================="
./dpx-bench -engine memory-sharded -mode embedded -concurrency 8192 \
            -duration $DURATION -keys $KEYS -workload write -runs $RUNS -keydist random

echo ""
echo "================================================================="
echo " 4b. EMBEDDED MEMORY SHARDED — write, striped keys (zero collision)"
echo "================================================================="
./dpx-bench -engine memory-sharded -mode embedded -concurrency 8192 \
            -duration $DURATION -keys $KEYS -workload write -runs $RUNS -keydist striped

echo ""
echo "================================================================="
echo " 5. EMBEDDED MEMORY SHARDED — read"
echo "================================================================="
./dpx-bench -engine memory-sharded -mode embedded -concurrency 8192 \
            -duration $DURATION -keys $KEYS -workload read -runs $RUNS

echo ""
echo "================================================================="
echo " 6. EMBEDDED MEMORY SHARDED — mixed"
echo "================================================================="
./dpx-bench -engine memory-sharded -mode embedded -concurrency 8192 \
            -duration $DURATION -keys $KEYS -workload mixed -runs $RUNS -warmup 5s

# ─── PEBBLE (Disk/LSM) ───────────────────────────────────────────────────────
echo ""
echo "================================================================="
echo " 7. EMBEDDED PEBBLE — write  (Disk/LSM production baseline)"
echo "================================================================="
rm -rf /tmp/dpx-bench-pebble
./dpx-bench -engine pebble -mode embedded -dir /tmp/dpx-bench-pebble \
            -concurrency 2048 -duration $DURATION -keys $KEYS -workload write -runs $RUNS

echo ""
echo "================================================================="
echo " 8. EMBEDDED PEBBLE — read"
echo "================================================================="
./dpx-bench -engine pebble -mode embedded -dir /tmp/dpx-bench-pebble \
            -concurrency 2048 -duration $DURATION -keys $KEYS -workload read -runs $RUNS

echo ""
echo "================================================================="
echo " 9. EMBEDDED PEBBLE — mixed"
echo "================================================================="
./dpx-bench -engine pebble -mode embedded -dir /tmp/dpx-bench-pebble \
            -concurrency 2048 -duration $DURATION -keys $KEYS -workload mixed -runs $RUNS

# ─── DISTRIBUTED RAFT ────────────────────────────────────────────────────────
# Single run only: distributed is slow by design (BoltDB WAL, TCP loopback).
# The interesting number here is error rate, not TPS variance.
echo ""
echo "================================================================="
echo " 10. DISTRIBUTED RAFT — write  (HashiCorp Raft + BoltDB WAL)"
echo "================================================================="
rm -rf /tmp/dpx-bench-raft
./dpx-bench -engine memory -mode distributed -raftdir /tmp/dpx-bench-raft \
            -concurrency 512 -duration $DURATION -keys $KEYS -workload write -runs 1

echo ""
echo "✅ All benchmarks complete!"
