package badger

// Matches the memory engine's wire format so checkpoints are interchangeable.
//
// Wire format (little-endian):
// entry count    uint64
//   for each entry:
// key length   uint16
//     [N]  key bytes
// val length   uint32
//     [N]  val bytes
//
// Note: badger checkpoints don't carry an applied index (Pebble sequence
// serves that role for badger) so count is the first field, not applied.

import (
	"encoding/binary"
	"fmt"
)

// encodeDump serialises a key-value map to the wire format.
func encodeDump(data map[string][]byte) []byte {
	size := 8 // count
	for k, v := range data {
		size += 2 + len(k) + 4 + len(v)
	}
	buf := make([]byte, size)
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(len(data)))
	pos += 8

	for k, v := range data {
		binary.LittleEndian.PutUint16(buf[pos:], uint16(len(k)))
		pos += 2
		copy(buf[pos:], k)
		pos += len(k)
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(v)))
		pos += 4
		copy(buf[pos:], v)
		pos += len(v)
	}
	return buf
}

// decodeDump deserialises a buffer written by encodeDump.
func decodeDump(buf []byte) (map[string][]byte, error) {
	if len(buf) == 0 {
		return make(map[string][]byte), nil
	}
	if len(buf) < 8 {
		return nil, fmt.Errorf("badger dump: short header (%d bytes)", len(buf))
	}
	pos := 0
	count := binary.LittleEndian.Uint64(buf[pos:])
	pos += 8

	data := make(map[string][]byte, count)
	for i := uint64(0); i < count; i++ {
		if pos+2 > len(buf) {
			return nil, fmt.Errorf("badger dump: truncated key length at entry %d", i)
		}
		kLen := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+kLen+4 > len(buf) {
			return nil, fmt.Errorf("badger dump: truncated key at entry %d", i)
		}
		k := string(buf[pos : pos+kLen])
		pos += kLen
		vLen := int(binary.LittleEndian.Uint32(buf[pos:]))
		pos += 4
		if pos+vLen > len(buf) {
			return nil, fmt.Errorf("badger dump: truncated value at entry %d", i)
		}
		v := make([]byte, vLen)
		copy(v, buf[pos:pos+vLen])
		pos += vLen
		data[k] = v
	}
	return data, nil
}
