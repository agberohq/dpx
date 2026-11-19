package dpx

import (
	"context"
	"errors"
	"time"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
)

// dpxTx is the KVTx implementation for one RunInTx attempt.
type dpxTx struct {
	snap      engine.Snapshot
	readSet   map[string]shared.ReadEntry
	writes    []shared.WriteEntry
	telemetry *shared.Telemetry
	node      *Node // for AllocateNextSequence counter; nil in tests using fakeSnapshot
}

// Get reads a key from the snapshot and records it in the read-set.
//
// GetVersion (the second Pebble/memory lookup for the epoch record) is
// deferred: we store epoch=0 now and fill in the real epoch lazily in
// readSetSlice(), which is called only when the transaction actually has
// writes to propose. Pure read-only transactions never call readSetSlice,
// so they pay zero version-lookup cost.
//
// This halves the number of storage reads per tx.Get call (from 2 to 1)
// for the common read path, which is the dominant operation for Pebble
// where each lookup is a full LSM traversal.
func (tx *dpxTx) Get(ctx context.Context, key []byte) ([]byte, error) {
	if isReserved(key) {
		return nil, ErrReservedKey
	}
	// ViewValue (when the engine supports it) skips a copy of the stored
	// value. The returned slice is immutable and owned by the engine; callers
	// of KVTx.Get must not modify it.
	val, err := snapshotValue(tx.snap, key)
	if err != nil {
		// Translate the engine-level sentinel to the dpx-level sentinel so
		// callers can use dpx.IsNotFound without importing engine.
		if err == engine.ErrKeyNotFound {
			// A read of a MISSING key must still join the read set. Without
			// this, every check-then-create pattern is racy: multiple
			// transactions can all observe "absent" and all create the key,
			// producing lost updates and forked chains. With the entry
			// recorded, a concurrent create raises the key epoch and this
			// transaction conflicts and retries.
			k := string(key)
			tx.readSet[k] = shared.ReadEntry{Key: []byte(k), Epoch: 0, IsDebit: false}
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	k := string(key)
	// Sentinel: epoch=0 signals "version not yet fetched".
	// readSetSlice() will hydrate the real epoch before building the proposal.
	// We intentionally do NOT call snap.GetVersion here.
	tx.readSet[k] = shared.ReadEntry{Key: []byte(k), Epoch: 0, IsDebit: false}
	return val, nil
}

// snapshotValue reads key from snap, preferring the zero-copy path.
func snapshotValue(snap engine.Snapshot, key []byte) ([]byte, error) {
	if vv, ok := snap.(engine.ValueViewer); ok {
		return vv.ViewValue(key)
	}
	return snap.Get(key)
}

// AppendOnlyWriter is an optional KVTx capability. SetAppendOnly writes a value
// without recording its conflict epoch: one engine write instead of two.
//
// CONTRACT: the key must never be read back with Get or AtomicAdd. Reads would
// baseline at epoch 0 and conflict against the key's live epoch indefinitely.
// Range scans are safe; they do not join the read set. See shared.OpSetAppend.
type AppendOnlyWriter interface {
	SetAppendOnly(ctx context.Context, key, value []byte) error
}

// OwnedWriter is an optional KVTx capability: SetOwned and SetAppendOnlyOwned
// take ownership of key and value instead of copying them.
//
// The caller must not modify or reuse either buffer after the call: the
// proposal holds them until the transaction commits. Use it when the buffers
// are freshly allocated (a marshalled record, a key built for this write);
// Set/SetAppendOnly copy and are safe for reused scratch buffers.
type OwnedWriter interface {
	SetOwned(ctx context.Context, key, value []byte) error
	SetAppendOnlyOwned(ctx context.Context, key, value []byte) error
}

// SetOwned buffers a plain write without copying key or value.
func (tx *dpxTx) SetOwned(ctx context.Context, key, value []byte) error {
	if isReserved(key) {
		return ErrReservedKey
	}
	tx.writes = append(tx.writes, shared.WriteEntry{Op: shared.OpSet, Key: key, Value: value})
	return nil
}

// SetAppendOnlyOwned buffers an append-only write without copying key or value.
func (tx *dpxTx) SetAppendOnlyOwned(ctx context.Context, key, value []byte) error {
	if isReserved(key) {
		return ErrReservedKey
	}
	tx.writes = append(tx.writes, shared.WriteEntry{Op: shared.OpSetAppend, Key: key, Value: value})
	return nil
}

// SetAppendOnly buffers a write that will not get a __dpx:ver: record.
func (tx *dpxTx) SetAppendOnly(ctx context.Context, key, value []byte) error {
	if isReserved(key) {
		return ErrReservedKey
	}
	cpKey := make([]byte, len(key))
	copy(cpKey, key)
	cpVal := make([]byte, len(value))
	copy(cpVal, value)
	tx.writes = append(tx.writes, shared.WriteEntry{Op: shared.OpSetAppend, Key: cpKey, Value: cpVal})
	return nil
}

// Set buffers a plain write.
func (tx *dpxTx) Set(ctx context.Context, key, value []byte) error {
	if isReserved(key) {
		return ErrReservedKey
	}
	cpKey := make([]byte, len(key))
	copy(cpKey, key)
	cpVal := make([]byte, len(value))
	copy(cpVal, value)
	tx.writes = append(tx.writes, shared.WriteEntry{Op: shared.OpSet, Key: cpKey, Value: cpVal})
	return nil
}

// Delete buffers a key deletion.
func (tx *dpxTx) Delete(ctx context.Context, key []byte) error {
	if isReserved(key) {
		return ErrReservedKey
	}
	cpKey := make([]byte, len(key))
	copy(cpKey, key)
	tx.writes = append(tx.writes, shared.WriteEntry{Op: shared.OpDelete, Key: cpKey})
	return nil
}

// AtomicAdd performs an atomic int64 addition.
func (tx *dpxTx) AtomicAdd(ctx context.Context, key []byte, delta int64) (int64, error) {
	if isReserved(key) {
		return 0, ErrReservedKey
	}
	if delta > 0 {
		val, err := snapshotValue(tx.snap, key)
		if err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
			return 0, err
		}
		cpKey := make([]byte, len(key))
		copy(cpKey, key)
		tx.writes = append(tx.writes, shared.WriteEntry{
			Op:    shared.OpCredit,
			Key:   cpKey,
			Value: encodeInt64(delta),
		})
		// Credit path is lock-free: the current value is read only to
		// establish the snapshot base, and the authoritative post-add value
		// is not known here. Return the snapshot-time value, matching the
		// documented contract for positive deltas (see store.KVTx.AtomicAdd:
		// only delta <= 0 promises the new value).
		return decodeInt64(val), nil
	}
	val, err := snapshotValue(tx.snap, key)
	if err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
		return 0, err
	}
	// Debit path: we MUST read the version here because this is a write that
	// participates in conflict detection (IsDebit=true guards the overdraft check).
	// This is not deferrable — the epoch is part of the semantic contract.
	ver, err := tx.snap.GetVersion(key)
	if err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
		return 0, err
	}
	k := string(key)
	tx.readSet[k] = shared.ReadEntry{Key: []byte(k), Epoch: ver.Epoch, IsDebit: true}
	if delta < 0 {
		cpKey := make([]byte, len(key))
		copy(cpKey, key)
		tx.writes = append(tx.writes, shared.WriteEntry{
			Op:    shared.OpDebit,
			Key:   cpKey,
			Value: encodeInt64(delta),
		})
	}
	return decodeInt64(val) + delta, nil
}

// GetRange scans keys in [start, end) within the snapshot.
func (tx *dpxTx) GetRange(ctx context.Context, start, end []byte, limit int) ([]KVPair, error) {
	iter := tx.snap.NewIter(start, end)
	defer iter.Close()
	var pairs []KVPair
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		if limit > 0 && len(pairs) >= limit {
			break
		}
		pairs = append(pairs, KVPair{
			Key:   append([]byte(nil), iter.Key()...),
			Value: append([]byte(nil), iter.Value()...),
		})
	}
	return pairs, iter.Error()
}

// AllocateNextSequence returns a strictly monotonic sequence number.
// It uses a node-level atomic counter so the sequence is globally monotonic
// across all shards — the sharded engine has per-shard applied indices that
// are not globally monotonic and cannot be used for this purpose.
func (tx *dpxTx) AllocateNextSequence(_ context.Context) (uint64, error) {
	if tx.node != nil {
		return tx.node.seqCounter.Add(1), nil
	}
	// Fallback for tests that construct dpxTx directly without a Node.
	return tx.snap.Sequence() + 1, nil
}

func (tx *dpxTx) empty() bool { return len(tx.writes) == 0 }

// readSetSlice builds the final ReadEntry slice for the Proposal.
//
// For any entry that had epoch=0 (recorded by Get without a version lookup),
// we now fetch the real epoch. This is only called when len(tx.writes) > 0,
// so pure read-only transactions pay zero extra GetVersion cost.
func (tx *dpxTx) readSetSlice() []shared.ReadEntry {
	if tx.telemetry != nil {
		defer func(start time.Time) {
			tx.telemetry.TxReadSetCopy.Record(time.Since(start))
		}(time.Now())
	}
	if len(tx.readSet) == 0 {
		return nil
	}
	rs := make([]shared.ReadEntry, 0, len(tx.readSet))
	for _, re := range tx.readSet {
		// Hydrate the epoch for entries that were recorded with the deferred
		// sentinel (epoch=0, IsDebit=false). Debit entries already have their
		// real epoch fetched eagerly in AtomicAdd, so we skip those.
		if re.Epoch == 0 && !re.IsDebit {
			ver, err := tx.snap.GetVersion(re.Key)
			if err == nil {
				re.Epoch = ver.Epoch
			}
			// On error (key not found, etc.) epoch stays 0 — safe because a
			// missing version means epoch 0 is the correct conflict baseline.
		}
		rs = append(rs, re)
	}
	return rs
}

// validate checks for incompatible operations in the same transaction.
func (tx *dpxTx) validate() error {
	if tx.telemetry != nil {
		defer func(start time.Time) {
			tx.telemetry.TxValidate.Record(time.Since(start))
		}(time.Now())
	}
	if len(tx.writes) == 0 {
		return nil
	}
	credits := make(map[string]struct{}, len(tx.writes))
	for _, w := range tx.writes {
		if w.Op == shared.OpCredit {
			credits[string(w.Key)] = struct{}{}
		}
	}
	for _, w := range tx.writes {
		if w.Op == shared.OpDelete {
			if _, ok := credits[string(w.Key)]; ok {
				return ErrInvalidProposal
			}
		}
	}
	return nil
}
