package dpx

import (
	"context"
	"errors"

	"github.com/agberohq/dpx/engine"
	"github.com/agberohq/dpx/shared"
)

// dpxTx is the KVTx implementation for one RunInTx attempt.
type dpxTx struct {
	snap    engine.Snapshot
	readSet map[string]shared.ReadEntry
	writes  []shared.WriteEntry
}

// Get reads a key from the snapshot and records it in the read-set.
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
	k := string(key)
	tx.readSet[k] = shared.ReadEntry{Key: []byte(k), Epoch: ver.Epoch, IsDebit: false}
	return val, nil
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
		val, err := tx.snap.Get(key)
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
		return decodeInt64(val), nil
	}

	val, err := tx.snap.Get(key)
	if err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
		return 0, err
	}
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
func (tx *dpxTx) AllocateNextSequence(_ context.Context) (uint64, error) {
	return tx.snap.Sequence() + 1, nil
}

func (tx *dpxTx) empty() bool { return len(tx.writes) == 0 }

func (tx *dpxTx) readSetSlice() []shared.ReadEntry {
	if len(tx.readSet) == 0 {
		return nil
	}
	rs := make([]shared.ReadEntry, 0, len(tx.readSet))
	for _, re := range tx.readSet {
		rs = append(rs, re)
	}
	return rs
}

// validate checks for incompatible operations in the same transaction.
func (tx *dpxTx) validate() error {
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
