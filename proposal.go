package dpx

// Proposal is the unit proposed to Raft per RunInTx call.
// Each Proposal becomes exactly one Raft log entry.
// Serialised with msgpack before being passed to Dragonboat.
type Proposal struct {
	ReadSet []ReadEntry
	Writes  []WriteEntry
}

// ReadEntry records a key that was read during speculative transaction
// execution, along with the epoch (Raft log index) of the last write to
// that key at the time of the read, and whether the read was for a debit
// sufficiency check.
//
// Only Get and AtomicAdd(delta ≤ 0) populate the ReadSet.
// GetRange and AtomicAdd(delta > 0) do NOT populate the ReadSet.
type ReadEntry struct {
	Key     []byte
	Epoch   uint64 // __dpx:ver:{key} at read time; 0 if key never written
	IsDebit bool   // true if this read was for a debit/probe sufficiency check
}

// WriteEntry is one mutation in the write set.
type WriteEntry struct {
	Op    WriteOp
	Key   []byte
	Value []byte // Set: raw value; Credit/Debit: int64 delta LE; Delete: nil
}

// WriteOp distinguishes the type of write in a WriteEntry.
type WriteOp uint8

const (
	// OpSet is a plain key-value write.
	// The corresponding read IS in the ReadSet with IsDebit=false.
	OpSet WriteOp = 1

	// OpDelete removes a key.
	// The corresponding read IS in the ReadSet with IsDebit=false.
	OpDelete WriteOp = 2

	// OpCredit is AtomicAdd with delta > 0.
	// Applied via engine Merge. NOT in the ReadSet. Commutes with other credits.
	OpCredit WriteOp = 3

	// OpDebit is AtomicAdd with delta ≤ 0 (including delta == 0 probe).
	// Applied via engine Merge. IS in the ReadSet with IsDebit=true.
	// Concurrent debits on the same key conflict and one must retry.
	OpDebit WriteOp = 4
)

// ResultOK and ResultConflict are the two outcomes carried in
// statemachine.Result.Value from Update() back to the proposing goroutine.
const (
	ResultOK       uint64 = 0
	ResultConflict uint64 = 1
)
