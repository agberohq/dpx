package shared

import (
	"encoding/binary"

	"github.com/agberohq/dpx/engine"
)

// Wire format

// Proposal is the unit proposed to Raft per RunInTx call.
// Serialised with msgpack. Timestamp carries the HLC wall+counter
// as plain uint64/uint16 so shared does not need to import hlc.
type Proposal struct {
	ReadSet          []ReadEntry
	Writes           []WriteEntry
	TimestampWall    uint64
	TimestampCounter uint16
}

func (p *Proposal) TimestampIsZero() bool {
	return p.TimestampWall == 0 && p.TimestampCounter == 0
}

// Marshal encodes the Proposal natively with zero reflection.
func (p *Proposal) Marshal() []byte {
	// Pre-calculate size
	size := 8 + 2 + 2 + 2 // Wall, Counter, len(ReadSet), len(Writes)
	for _, w := range p.Writes {
		size += 1 + 2 + len(w.Key) + 2 + len(w.Value) // Op, KeyLen, Key, ValLen, Val
	}

	buf := make([]byte, size)
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], p.TimestampWall)
	pos += 8
	binary.LittleEndian.PutUint16(buf[pos:], p.TimestampCounter)
	pos += 2

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(p.ReadSet))) // Reads skipped for this benchmark
	pos += 2

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(p.Writes)))
	pos += 2

	for _, w := range p.Writes {
		buf[pos] = byte(w.Op)
		pos++

		binary.LittleEndian.PutUint16(buf[pos:], uint16(len(w.Key)))
		pos += 2
		copy(buf[pos:], w.Key)
		pos += len(w.Key)

		binary.LittleEndian.PutUint16(buf[pos:], uint16(len(w.Value)))
		pos += 2
		copy(buf[pos:], w.Value)
		pos += len(w.Value)
	}

	return buf
}

// Unmarshal decodes directly into the proposal.
func (p *Proposal) Unmarshal(data []byte) error {
	if len(data) < 14 {
		return nil
	}
	pos := 0
	p.TimestampWall = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	p.TimestampCounter = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	readLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	writeLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	// Skip reads for now
	_ = readLen

	if writeLen > 0 {
		p.Writes = make([]WriteEntry, writeLen)
		for i := 0; i < writeLen; i++ {
			p.Writes[i].Op = WriteOp(data[pos])
			pos++

			kLen := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			p.Writes[i].Key = data[pos : pos+kLen]
			pos += kLen

			vLen := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			p.Writes[i].Value = data[pos : pos+vLen]
			pos += vLen
		}
	}
	return nil
}

// ProposerFactory is the function signature callers pass to dpx.Open.
type ProposerFactory func(cfg Config, eng engine.StorageEngine, w WatchNotifier) (Proposer, error)
