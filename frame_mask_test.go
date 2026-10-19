package nwws

import (
	"bytes"
	"fmt"
	"testing"
)

// maskBytewise is the reference algorithm of RFC 6455 §5.3: octet i of the
// payload is XORed with octet i MOD 4 of the masking key.
func maskBytewise(key [4]byte, pos int, b []byte) int {
	for i := range b {
		b[i] ^= key[pos&3]
		pos++
	}
	return pos & 3
}

func TestMaskBytes(t *testing.T) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	t.Run("§5.7 Hello", func(t *testing.T) {
		b := []byte("Hello")
		maskBytes(key, 0, b)
		if want := hello[4:]; !bytes.Equal(b, want) {
			t.Errorf("masked = % x, want % x", b, want)
		}
	})
	t.Run("§5.3 matches the bytewise algorithm", func(t *testing.T) {
		for _, n := range []int{0, 1, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 255, 1000} {
			for pos := range 4 {
				// An offset start checks unaligned slices too.
				for start := range 3 {
					src := make([]byte, start+n)
					for i := range src {
						src[i] = byte(i*7 + n)
					}
					got := bytes.Clone(src)
					want := bytes.Clone(src)
					gotPos := maskBytes(key, pos, got[start:])
					wantPos := maskBytewise(key, pos, want[start:])
					if !bytes.Equal(got, want) || gotPos != wantPos {
						t.Fatalf("n=%d pos=%d start=%d: got % x (pos %d), want % x (pos %d)", n, pos, start, got, gotPos, want, wantPos)
					}
				}
			}
		}
	})
	t.Run("§5.3 masking twice restores the payload", func(t *testing.T) {
		src := bytes.Repeat([]byte("nwws"), 100)
		b := bytes.Clone(src)
		maskBytes(key, 1, b)
		if bytes.Equal(b, src) {
			t.Fatal("masking left the payload unchanged")
		}
		maskBytes(key, 1, b)
		if !bytes.Equal(b, src) {
			t.Error("unmasking did not restore the payload")
		}
	})
	t.Run("§5.3 masking in pieces equals masking at once", func(t *testing.T) {
		src := bytes.Repeat([]byte{0x5a}, 1001)
		whole := bytes.Clone(src)
		maskBytes(key, 0, whole)
		pieces := bytes.Clone(src)
		pos := 0
		for _, cut := range [][2]int{{0, 3}, {3, 100}, {100, 101}, {101, 1001}} {
			pos = maskBytes(key, pos, pieces[cut[0]:cut[1]])
		}
		if !bytes.Equal(whole, pieces) {
			t.Error("piecewise masking differs")
		}
	})
}

var maskSizes = []int{16, 128, 1024, 4096, 65536}

func BenchmarkMaskBytes(b *testing.B) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	for _, n := range maskSizes {
		buf := make([]byte, n)
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				maskBytes(key, 1, buf)
			}
		})
	}
}

// BenchmarkMaskBytewise is the baseline the word-at-a-time maskBytes beats.
func BenchmarkMaskBytewise(b *testing.B) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	for _, n := range maskSizes {
		buf := make([]byte, n)
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				maskBytewise(key, 1, buf)
			}
		})
	}
}
