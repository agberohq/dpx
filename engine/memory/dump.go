package memory

// dump.go — zero-reflection binary codec for the memory engine's persistence
// format. Replaces msgpack for the Open/CreateCheckpoint cold paths.
//
// Wire format (little-endian):
//   [8]  applied index   uint64
//   [8]  entry count     uint64
//   for each entry:
//     [2]  key length    uint16
//     [N]  key bytes
//     [4]  val length    uint32
//     [N]  val bytes

import (
	"encoding/binary"
	"fmt"
)

// encodeDump serialises the key-value map and applied index.
func encodeDump(data map[string][]byte, applied uint64) []byte {
	// Pre-calculate size.
	size := 8 + 8 // applied + count
	for k, v := range data {
		size += 2 + len(k) + 4 + len(v)
	}
	buf := make([]byte, size)
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], applied)
	pos += 8
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
// Returns the key-value map, the applied index, and any error.
func decodeDump(buf []byte) (map[string][]byte, uint64, error) {
	if len(buf) == 0 {
		return make(map[string][]byte), 0, nil
	}
	if len(buf) < 16 {
		// Possibly an old msgpack dump — signal to caller via error.
		return nil, 0, fmt.Errorf("memory dump: short header (%d bytes), may be legacy format", len(buf))
	}
	pos := 0

	applied := binary.LittleEndian.Uint64(buf[pos:])
	pos += 8
	count := binary.LittleEndian.Uint64(buf[pos:])
	pos += 8

	data := make(map[string][]byte, count)
	for i := uint64(0); i < count; i++ {
		if pos+2 > len(buf) {
			return nil, 0, fmt.Errorf("memory dump: truncated key length at entry %d", i)
		}
		kLen := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+kLen+4 > len(buf) {
			return nil, 0, fmt.Errorf("memory dump: truncated key at entry %d", i)
		}
		k := string(buf[pos : pos+kLen])
		pos += kLen
		vLen := int(binary.LittleEndian.Uint32(buf[pos:]))
		pos += 4
		if pos+vLen > len(buf) {
			return nil, 0, fmt.Errorf("memory dump: truncated value at entry %d", i)
		}
		v := make([]byte, vLen)
		copy(v, buf[pos:pos+vLen])
		pos += vLen
		data[k] = v
	}
	return data, applied, nil
}
