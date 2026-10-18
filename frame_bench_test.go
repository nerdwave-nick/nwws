package nwws

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

var frameSizes = []int{16, 1024, 65536}

func BenchmarkWriteFrame(b *testing.B) {
	for _, server := range []bool{true, false} {
		for _, n := range frameSizes {
			payload := make([]byte, n)
			b.Run(fmt.Sprintf("server=%v/%d", server, n), func(b *testing.B) {
				fw := &frameWriter{w: io.Discard, server: server}
				b.SetBytes(int64(n))
				b.ReportAllocs()
				for b.Loop() {
					if err := fw.writeFrame(true, 0, opBinary, payload); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkReadFrame(b *testing.B) {
	for _, server := range []bool{true, false} {
		for _, n := range frameSizes {
			var wire bytes.Buffer
			fw := &frameWriter{w: &wire, server: !server}
			if err := fw.writeFrame(true, 0, opBinary, make([]byte, n)); err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("server=%v/%d", server, n), func(b *testing.B) {
				r := bytes.NewReader(wire.Bytes())
				fr := &frameReader{r: r, server: server}
				b.SetBytes(int64(n))
				b.ReportAllocs()
				for b.Loop() {
					r.Reset(wire.Bytes())
					if _, _, err := fr.readFrame(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
