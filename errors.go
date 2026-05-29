package dpx

import "errors"

// Sentinel errors returned by DPX.

// ErrConflict is returned by RunInTx when an OCC conflict is detected.
// jack.Retry catches this and retries automatically.
var ErrConflict = errors.New("dpx: transaction conflict")

// ErrConflictExhausted is returned when all retry attempts are exhausted.
// Maps from jack.ErrRetryExhausted at the RunInTx boundary.
var ErrConflictExhausted = errors.New("dpx: transaction conflict: all retry attempts exhausted")

// ErrNotLeader is returned when a write is proposed to a follower node.
var ErrNotLeader = errors.New("dpx: not the Raft leader")

// ErrKeyNotFound is returned by KVTx.Get when the key does not exist.
var ErrKeyNotFound = errors.New("dpx: key not found")

// ErrInvalidProposal is returned when a transaction contains incompatible
// operations on the same key (e.g. OpCredit and OpDelete for the same key).
// This is a permanent error; jack.Retry does not retry it.
var ErrInvalidProposal = errors.New("dpx: invalid proposal: credit and delete on same key")

// ErrReservedKey is returned when a consumer operation targets a key with the
// __dpx: prefix, which is reserved for internal metadata.
var ErrReservedKey = errors.New("dpx: key prefix __dpx: is reserved")

// ErrStoreClosed is returned when operations are attempted on a closed Node.
var ErrStoreClosed = errors.New("dpx: node is closed")
