// Package engine defines the StorageEngine interface and shared types
// used by DPX. Consumers select a concrete engine (pebble or memory)
// at construction time; the rest of DPX is engine-agnostic.
package engine

// KVPair is a key-value result from a range scan.
type KVPair struct {
	Key   []byte
	Value []byte
}

// EpochRecord stores the Raft log index of the last write to a key,
// and whether that write was a credit (AtomicAdd with delta > 0).
// Stored as 9 bytes in __dpx:ver:{key}: 8-byte LE uint64 + 1-byte flag.
type EpochRecord struct {
	Epoch    uint64
	IsCredit bool
}

// WriteOptions controls durability for a single ApplyBatch call.
type WriteOptions struct {
	Sync bool // if true, fsync before returning
}

// StorageEngine is the minimal surface DPX needs from any KV backend.
// Implementations: engine/pebble (production), engine/memory (testing).
type StorageEngine interface {
	// Open initialises the engine. Must be called before any other method.
	Open() error
	// Close flushes and releases all resources.
	Close() error

	// Get reads the current committed value for key.
	// Returns ErrKeyNotFound if the key does not exist.
	Get(key []byte) ([]byte, error)

	// GetSnapshot returns a consistent point-in-time view.
	// Snapshot.Sequence() equals CurrentSequence() at the moment of the call.
	// The caller must call Close() on the returned Snapshot when done.
	GetSnapshot() (Snapshot, error)

	// NewBatch creates a write batch for this engine.
	NewBatch() Batch

	// ApplyBatch applies all mutations in batch atomically.
	// This is the only write path. Called by the Raft state machine in log order.
	// Errors are fatal — caller must shut down the node.
	ApplyBatch(batch Batch, opts WriteOptions) error

	// Sync flushes the WAL to disk.
	// For Pebble: db.LogData(nil, pebble.Sync) — WAL fsync without memtable flush.
	// For Memory: no-op.
	// Called by StateMachine.Sync() under SyncBatch policy.
	Sync() error

	// CurrentSequence returns the Raft log index of the last applied entry.
	// Reads __dpx:applied from the engine. Durable across restarts.
	// Updated atomically with every successful ApplyBatch.
	CurrentSequence() uint64

	// CreateCheckpoint writes a consistent copy of the database to dir.
	// Pebble: hard-link snapshot; does not block writes; LSM-consistent.
	// Memory: msgpack serialisation to dir/dump.
	// The copy includes all __dpx: metadata keys.
	CreateCheckpoint(dir string) error

	// DataDir returns the engine's data directory.
	// Used by the state machine to construct snapshot temp paths.
	DataDir() string

	// RawIter returns a forward iterator that includes __dpx: prefix keys.
	// Used only by StateMachine.Open() to rebuild the keyEpoch map.
	// Consumer-facing iterators (via Snapshot.NewIter) exclude __dpx: keys.
	RawIter(start, end []byte) Iterator
}

// Snapshot provides a consistent point-in-time view of the engine.
// All reads on a Snapshot see the same committed state.
type Snapshot interface {
	// Get returns the value at key as of snapshot time.
	// Returns ErrKeyNotFound if the key does not exist.
	Get(key []byte) ([]byte, error)

	// GetVersion returns the __dpx:ver:{key} EpochRecord from this snapshot.
	// Returns a zero EpochRecord if the key has never been written.
	GetVersion(key []byte) (EpochRecord, error)

	// NewIter creates a forward iterator over [start, end).
	// __dpx: prefix keys are excluded; the iterator is consumer-facing.
	NewIter(start, end []byte) Iterator

	// Sequence returns the Raft log index at the time this snapshot was taken.
	// Equals engine.CurrentSequence() at the moment GetSnapshot() was called.
	Sequence() uint64

	// Close releases the snapshot. Must be called when done.
	Close() error
}

// Iterator scans keys in sorted order.
// Callers must call Close() when done.
type Iterator interface {
	First() bool
	Next() bool
	Prev() bool
	Valid() bool
	Key() []byte   // valid only while Valid() is true; copy if storing
	Value() []byte // valid only while Valid() is true; copy if storing
	Error() error
	Close() error
}

// Batch collects mutations for atomic application via ApplyBatch.
// Batch contains no Data() method — wire serialisation is handled
// by the Proposal type, not by the engine batch.
type Batch interface {
	Set(key, value []byte)
	Delete(key []byte)
	// Merge performs an int64 addition via the engine's merger operator.
	// Used for both OpCredit and OpDebit; the delta is encoded as LE int64.
	// The engine does not need to know the sign of the delta.
	Merge(key, value []byte)
	Reset()
}
