#!/usr/bin/env bash
set -e

# Build the runner to avoid compilation overhead during runs
echo "🔨 Building runner..."
go build -o dpx-bench ./runner.go

DURATION="10s"
KEYS=100000

echo ""
echo "================================================================="
echo " 1. EMBEDDED MEMORY (Global Lock)"
echo " Baseline DPX throughput. Limited by map cloning."
echo "================================================================="
./dpx-bench -engine memory -mode embedded -concurrency 512 -duration $DURATION -keys $KEYS

echo ""
echo "================================================================="
echo " 2. EMBEDDED MEMORY SHARDED (Zero Lock)"
echo " Maximum theoretical TPS of the DPX pipeline."
echo "================================================================="
./dpx-bench -engine memory-sharded -mode embedded -concurrency 8192 -duration $DURATION -keys $KEYS

echo ""
echo "================================================================="
echo " 3. EMBEDDED PEBBLE (Disk/LSM)"
echo " Production disk throughput (Pebble handles its own concurrency)."
echo "================================================================="
rm -rf /tmp/dpx-bench-pebble
./dpx-bench -engine pebble -mode embedded -dir /tmp/dpx-bench-pebble -concurrency 2048 -duration $DURATION -keys $KEYS

echo ""
echo "================================================================="
echo " 4. DISTRIBUTED RAFT (Memory + BoltDB WAL)"
echo " Real-world distributed overhead (HashiCorp Raft + Disk WAL)."
echo "================================================================="
rm -rf /tmp/dpx-bench-raft
./dpx-bench -engine memory -mode distributed -raftdir /tmp/dpx-bench-raft -concurrency 512 -duration $DURATION -keys $KEYS

echo ""
echo "✅ All benchmarks complete!"