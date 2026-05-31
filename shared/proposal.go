package shared

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/agberohq/dpx/engine"
)

// Wire format — zero-reflection binary codec for Proposal.
//
// Layout (little-endian throughout):
//   [8] TimestampWall    uint64
//   [2] TimestampCounter uint16
//   [2] ReadSet length   uint16
//   for each ReadEntry:
//     [2] key length     uint16
//     [N] key bytes
//     [8] Epoch          uint64
//     [1] IsDebit        uint8 (0 or 1)
//   [2] Writes length    uint16
//   for each WriteEntry:
//     [1] Op             uint8
//     [2] key length     uint16
//     [N] key bytes
//     [2] value length   uint16
//     [N] value bytes
//
// All length fields are uint16, capping individual keys/values at 65535 bytes
// and read/write set sizes at 65535 entries — both well above any real limit.

// Proposal is the unit proposed to Raft per RunInTx call.
// The wire codec is zero-reflection; see Marshal/Unmarshal below.
// Timestamp carries the HLC wall+counter as plain uint64/uint16 so shared
// does not need to import hlc.
type Proposal struct {
	ReadSet          []ReadEntry
	Writes           []WriteEntry
	TimestampWall    uint64
	TimestampCounter uint16
}

func (p *Proposal) TimestampIsZero() bool {
	return p.TimestampWall == 0 && p.TimestampCounter == 0
}

// encodeBufPool reuses encoding buffers to reduce GC pressure.
// The pool holds []byte; callers take, resize if needed, return.
var encodeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// Marshal encodes the Proposal with zero reflection.
// Returns a freshly allocated slice — callers own it.
func (p *Proposal) Marshal() []byte {
	size := 8 + 2 // Wall, Counter
	size += 2     // ReadSet length
	for _, re := range p.ReadSet {
		size += 2 + len(re.Key) + 8 + 1 // KeyLen, Key, Epoch, IsDebit
	}
	size += 2 // Writes length
	for _, w := range p.Writes {
		size += 1 + 2 + len(w.Key) + 2 + len(w.Value) // Op, KeyLen, Key, ValLen, Val
	}

	buf := make([]byte, size)
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], p.TimestampWall)
	pos += 8
	binary.LittleEndian.PutUint16(buf[pos:], p.TimestampCounter)
	pos += 2

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(p.ReadSet)))
	pos += 2
	for _, re := range p.ReadSet {
		binary.LittleEndian.PutUint16(buf[pos:], uint16(len(re.Key)))
		pos += 2
		copy(buf[pos:], re.Key)
		pos += len(re.Key)
		binary.LittleEndian.PutUint64(buf[pos:], re.Epoch)
		pos += 8
		if re.IsDebit {
			buf[pos] = 1
		} else {
			buf[pos] = 0
		}
		pos++
	}

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

// minProposalBytes is the minimum valid encoded size:
// Wall(8) + Counter(2) + ReadLen(2) + WriteLen(2) = 14
const minProposalBytes = 14

// Unmarshal decodes a wire-encoded Proposal in-place.
// Key and Value slices in the result point into data — callers must not
// modify data while the Proposal is live.
func (p *Proposal) Unmarshal(data []byte) error {
	if len(data) < minProposalBytes {
		if len(data) == 0 {
			return nil // empty = no-op proposal
		}
		return fmt.Errorf("dpx/proposal: short buffer %d < %d", len(data), minProposalBytes)
	}
	pos := 0

	p.TimestampWall = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	p.TimestampCounter = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	readLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	if readLen > 0 {
		p.ReadSet = make([]ReadEntry, readLen)
		for i := 0; i < readLen; i++ {
			if pos+2 > len(data) {
				return fmt.Errorf("dpx/proposal: truncated ReadEntry[%d] key length at %d", i, pos)
			}
			kLen := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			if pos+kLen+8+1 > len(data) {
				return fmt.Errorf("dpx/proposal: truncated ReadEntry[%d] at %d", i, pos)
			}
			p.ReadSet[i].Key = data[pos : pos+kLen]
			pos += kLen
			p.ReadSet[i].Epoch = binary.LittleEndian.Uint64(data[pos:])
			pos += 8
			p.ReadSet[i].IsDebit = data[pos] != 0
			pos++
		}
	} else {
		p.ReadSet = nil
	}

	if pos+2 > len(data) {
		return fmt.Errorf("dpx/proposal: truncated Writes length at %d", pos)
	}
	writeLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	if writeLen > 0 {
		p.Writes = make([]WriteEntry, writeLen)
		for i := 0; i < writeLen; i++ {
			if pos+1+2 > len(data) {
				return fmt.Errorf("dpx/proposal: truncated WriteEntry[%d] op at %d", i, pos)
			}
			p.Writes[i].Op = WriteOp(data[pos])
			pos++
			kLen := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			if pos+kLen+2 > len(data) {
				return fmt.Errorf("dpx/proposal: truncated WriteEntry[%d] key at %d", i, pos)
			}
			p.Writes[i].Key = data[pos : pos+kLen]
			pos += kLen
			vLen := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			if pos+vLen > len(data) {
				return fmt.Errorf("dpx/proposal: truncated WriteEntry[%d] value at %d", i, pos)
			}
			p.Writes[i].Value = data[pos : pos+vLen]
			pos += vLen
		}
	} else {
		p.Writes = nil
	}

	return nil
}

// ProposerFactory is the function signature callers pass to dpx.Open.
type ProposerFactory func(cfg Config, eng engine.StorageEngine, w WatchNotifier) (Proposer, error)
