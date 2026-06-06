package dpx

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/agberohq/dpx/engine"
)

// Reserved prefix

var dpxPrefixBytes = []byte("__dpx:")

func isReserved(key []byte) bool {
	return len(key) >= len(dpxPrefixBytes) &&
		bytes.Equal(key[:len(dpxPrefixBytes)], dpxPrefixBytes)
}

// Version key construction

// VersionKey appends "__dpx:ver:" + key into buf, reusing buf's backing array.
// Exported for raft/fsm.go which builds version keys in the hot Apply path.
func VersionKey(buf, key []byte) []byte {
	return append(append(buf[:0], "__dpx:ver:"...), key...)
}

// AppliedKey is the engine key that stores the last applied Raft log index.
// Exported for raft/fsm.go.
var AppliedKey = []byte("__dpx:applied")

// RawIterStart and RawIterEnd bound the __dpx:ver: prefix scan in FSM.Open.
// Exported for raft/fsm.go.
var (
	RawIterStart = []byte("__dpx:ver:")
	RawIterEnd   = append([]byte("__dpx:ver:"), bytes.Repeat([]byte{0xFF}, 16)...)
)

// Epoch record encoding

// EncodeEpochRecordInto writes into a caller-provided slice (≥9 bytes).
// Exported for raft/fsm.go; typically called with a [9]byte stack array.
func EncodeEpochRecordInto(er engine.EpochRecord, buf []byte) []byte {
	b := buf[:9]
	binary.LittleEndian.PutUint64(b, er.Epoch)
	if er.IsCredit {
		b[8] = 1
	} else {
		b[8] = 0
	}
	return b
}

// DecodeEpochRecord reads from a 9-byte slice.
// Returns (record, true) on success, (zero, false) on short/nil input.
// Exported for raft/fsm.go which must treat a short record as a fatal error.
func DecodeEpochRecord(b []byte) (engine.EpochRecord, bool) {
	if len(b) < 9 {
		return engine.EpochRecord{}, false
	}
	return engine.EpochRecord{
		Epoch:    binary.LittleEndian.Uint64(b),
		IsCredit: b[8] == 1,
	}, true
}

// uint64 / int64 encoding

// EncodeUint64Into writes into a caller-provided slice (≥8 bytes).
// Exported for raft/fsm.go.
func EncodeUint64Into(v uint64, buf []byte) []byte {
	b := buf[:8]
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func encodeInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func decodeInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

// Duration clamping

func clampDuration(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

// Iterator collection

// collectIter drains an Iterator into a []KVPair slice.
// Reverse uses collect-then-reverse: correct for all Iterator implementations
// regardless of Prev()-after-exhaustion behaviour.
func collectIter(iter engine.Iterator, limit int, reverse bool) ([]KVPair, error) {
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
	if err := iter.Error(); err != nil {
		return nil, err
	}
	if reverse {
		for i, j := 0, len(pairs)-1; i < j; i, j = i+1, j-1 {
			pairs[i], pairs[j] = pairs[j], pairs[i]
		}
	}
	return pairs, nil
}
