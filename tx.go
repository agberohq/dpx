package dpx

import (
	"context"
	"errors"

	"github.com/agberohq/dpx/engine"
)

// dpxTx is the KVTx implementation for one RunInTx attempt.
// It buffers writes and builds a read-set for OCC conflict detection.
// All reads go through the engine Snapshot taken at the start of the attempt.
type dpxTx struct {
	snap    engine.Snapshot
	readSet map[string]ReadEntry // key → ReadEntry (epoch + IsDebit)
	writes  []WriteEntry
}

// Get reads a key from the snapshot and records it in the read-set.
// Returns ErrKeyNotFound if the key does not exist.
func (tx *dpxTx) Get(ctx context.Context, key []byte) ([]byte, error) {
	if isReserved(key) {
		return nil, ErrReservedKey
	}
	val, err := tx.snap.Get(key)
	if err != nil {
		return nil, err
	}
	ver, err := tx.snap.GetVersion(key)
	if err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
		return nil, err
	}
	k := string(key) // allocates; key from caller may not persist
	tx.readSet[k] = ReadEntry{Key: []byte(k), Epoch: ver.Epoch, IsDebit: false}
	return val, nil
}

// Set buffers a plain write. Does not add the key to the read-set.
func (tx *dpxTx) Set(ctx context.Context, key, value []byte) error {
	if isReserved(key) {
		return ErrReservedKey
	}
	// Copy key and value: the caller's slices must not be aliased in the proposal.
	cpKey := make([]byte, len(key))
	copy(cpKey, key)
	cpVal := make([]byte, len(value))
	copy(cpVal, value)
	tx.writes = append(tx.writes, WriteEntry{Op: OpSet, Key: cpKey, Value: cpVal})
	return nil
}

// Delete buffers a key deletion. Does not add the key to the read-set.
// Note: if the same key has a pending OpCredit in this transaction,
// validate() will return ErrInvalidProposal.
func (tx *dpxTx) Delete(ctx context.Context, key []byte) error {
	if isReserved(key) {
		return ErrReservedKey
	}
	cpKey := make([]byte, len(key))
	copy(cpKey, key)
	tx.writes = append(tx.writes, WriteEntry{Op: OpDelete, Key: cpKey})
	return nil
}

// AtomicAdd performs an atomic int64 addition.
//
// delta > 0 (credit):
//
//	Applied via engine Merge at commit. Key NOT added to read-set.
//	Return value is the snapshot-time value — an approximation.
//	Callers must not rely on the post-credit total from this call.
//
// delta ≤ 0 (debit or probe):
//
//	Reads current value from snapshot. Key IS added to read-set with IsDebit=true.
//	Return value is snapshot-value + delta (speculative post-debit value).
func (tx *dpxTx) AtomicAdd(ctx context.Context, key []byte, delta int64) (int64, error) {
	if isReserved(key) {
		return 0, ErrReservedKey
	}

	if delta > 0 {
		// Credit path: read snapshot value for return value only; no read-set entry.
		val, err := tx.snap.Get(key)
		if err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
			return 0, err
		}
		cpKey := make([]byte, len(key))
		copy(cpKey, key)
		tx.writes = append(tx.writes, WriteEntry{
			Op:    OpCredit,
			Key:   cpKey,
			Value: encodeInt64(delta),
		})
		return decodeInt64(val), nil // snapshot-time approximation
	}

	// Debit / probe path: read current value, record in read-set.
	val, err := tx.snap.Get(key)
	if err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
		return 0, err
	}
	ver, err := tx.snap.GetVersion(key)
	if err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
		return 0, err
	}
	k := string(key)
	tx.readSet[k] = ReadEntry{Key: []byte(k), Epoch: ver.Epoch, IsDebit: true}

	if delta < 0 {
		cpKey := make([]byte, len(key))
		copy(cpKey, key)
		tx.writes = append(tx.writes, WriteEntry{
			Op:    OpDebit,
			Key:   cpKey,
			Value: encodeInt64(delta),
		})
	}
	// delta == 0: probe only; no write entry.

	return decodeInt64(val) + delta, nil
}

// GetRange scans keys in [start, end) within the snapshot.
// Results are NOT added to the read-set — GetRange is advisory inside a
// transaction. Use for aggregation (e.g. summing balance stripes) only.
// __dpx: prefix keys are excluded by the snapshot iterator.
func (tx *dpxTx) GetRange(ctx context.Context, start, end []byte, limit int) ([]engine.KVPair, error) {
	iter := tx.snap.NewIter(start, end)
	defer iter.Close()

	var pairs []engine.KVPair
	for ok := iter.First(); ok && iter.Valid(); ok = iter.Next() {
		if limit > 0 && len(pairs) >= limit {
			break
		}
		pairs = append(pairs, engine.KVPair{
			Key:   append([]byte(nil), iter.Key()...),
			Value: append([]byte(nil), iter.Value()...),
		})
	}
	return pairs, iter.Error()
}

// AllocateNextSequence returns snap.Sequence() + 1.
// This is advisory and non-unique: two concurrent transactions at the same
// snapshot return the same value. Not a global counter; use for local
// ordering within a single transaction only.
func (tx *dpxTx) AllocateNextSequence(_ context.Context) (uint64, error) {
	return tx.snap.Sequence() + 1, nil
}

// empty returns true if no writes were buffered (read-only or probe-only tx).
// An empty transaction is not proposed to Raft.
func (tx *dpxTx) empty() bool { return len(tx.writes) == 0 }

// readSetSlice converts the readSet map to a []ReadEntry for the Proposal.
func (tx *dpxTx) readSetSlice() []ReadEntry {
	if len(tx.readSet) == 0 {
		return nil
	}
	rs := make([]ReadEntry, 0, len(tx.readSet))
	for _, re := range tx.readSet {
		rs = append(rs, re)
	}
	return rs
}

// validate checks for incompatible operations in the same transaction.
// Returns ErrInvalidProposal if a Credit and Delete target the same key.
// Credit then Delete would lose the credit (tombstone wins in LSM ordering).
func (tx *dpxTx) validate() error {
	if len(tx.writes) == 0 {
		return nil
	}
	credits := make(map[string]struct{}, len(tx.writes))
	for _, w := range tx.writes {
		if w.Op == OpCredit {
			credits[string(w.Key)] = struct{}{}
		}
	}
	for _, w := range tx.writes {
		if w.Op == OpDelete {
			if _, ok := credits[string(w.Key)]; ok {
				return ErrInvalidProposal
			}
		}
	}
	return nil
}
