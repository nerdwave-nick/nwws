package nwws

import (
	"encoding/binary"
	"math/bits"
)

// maskBytes XORs b with the masking key (RFC 6455 §5.3), starting at
// position pos of the key, and returns the key position after b. Masking and
// unmasking are the same operation; the returned position lets a payload be
// masked in pieces.
//
// It works a 64-bit word at a time: the key is rotated to start at pos and
// repeated across a word, and since a word spans whole key periods the
// position is unchanged after each word.
func maskBytes(key [4]byte, pos int, b []byte) int {
	pos &= 3
	if len(b) >= 8 {
		k32 := bits.RotateLeft32(binary.LittleEndian.Uint32(key[:]), -8*pos)
		k := uint64(k32)<<32 | uint64(k32)
		for len(b) >= 32 {
			binary.LittleEndian.PutUint64(b, binary.LittleEndian.Uint64(b)^k)
			binary.LittleEndian.PutUint64(b[8:], binary.LittleEndian.Uint64(b[8:])^k)
			binary.LittleEndian.PutUint64(b[16:], binary.LittleEndian.Uint64(b[16:])^k)
			binary.LittleEndian.PutUint64(b[24:], binary.LittleEndian.Uint64(b[24:])^k)
			b = b[32:]
		}
		for len(b) >= 8 {
			binary.LittleEndian.PutUint64(b, binary.LittleEndian.Uint64(b)^k)
			b = b[8:]
		}
	}
	for i := range b {
		b[i] ^= key[pos]
		pos = (pos + 1) & 3
	}
	return pos
}
