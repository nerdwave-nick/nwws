package nwws

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

// readBenchSizes are the message sizes of the read benchmarks: a typical
// small message, a mid-sized one and one at the default read limit.
var readBenchSizes = []struct {
	name string
	n    int
}{
	{"300B", 300},
	{"64KiB", 64 << 10},
	{"1MiB", 1 << 20},
}

// benchConn returns the server side of a loopback TCP connection as a
// Conn, with liveness off, and the raw client end.
func benchConn(b *testing.B) (*Conn, net.Conn) {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		s, _ := ln.Accept()
		accepted <- s
	}()
	peer, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	s := <-accepted
	if s == nil {
		b.Fatal("accept failed")
	}
	c := newConn(s, true, "", connConfig{readLimit: 2 << 20, closeTimeout: time.Second})
	b.Cleanup(func() {
		_ = peer.Close()
		c.closeNow(ErrClosed)
		c.wait()
	})
	return c, peer
}

// benchFrames returns count masked binary frames of n bytes each, as a
// client sends them, one after the other.
func benchFrames(b *testing.B, n, count int) []byte {
	b.Helper()
	var wire bytes.Buffer
	fw := frameWriter{w: &wire}
	payload := make([]byte, n)
	for range count {
		if err := fw.writeFrame(true, 0, opBinary, payload); err != nil {
			b.Fatal(err)
		}
	}
	return wire.Bytes()
}

// BenchmarkReadThroughput measures how fast Read takes messages that the
// peer sends as fast as it can, in batches of about 64 KiB.
func BenchmarkReadThroughput(b *testing.B) {
	for _, size := range readBenchSizes {
		b.Run(size.name, func(b *testing.B) {
			c, peer := benchConn(b)
			perBatch := max(1, (64<<10)/size.n)
			batch := benchFrames(b, size.n, perBatch)
			writeErr := make(chan error, 1)
			b.SetBytes(int64(size.n))
			b.ReportAllocs()
			b.ResetTimer()
			go func() {
				for sent := 0; sent < b.N; sent += perBatch {
					if _, err := peer.Write(batch); err != nil {
						writeErr <- err
						return
					}
				}
				writeErr <- nil
			}()
			ctx := context.Background()
			for range b.N {
				if _, _, err := c.Read(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := <-writeErr; err != nil {
				b.Fatal(err)
			}
		})
	}
}

// BenchmarkReadLatency measures the time from the peer starting to send
// a message to Read returning it, on an otherwise idle connection.
func BenchmarkReadLatency(b *testing.B) {
	for _, size := range readBenchSizes {
		b.Run(size.name, func(b *testing.B) {
			c, peer := benchConn(b)
			frame := benchFrames(b, size.n, 1)
			b.SetBytes(int64(size.n))
			b.ReportAllocs()
			ctx := context.Background()
			b.ResetTimer()
			for range b.N {
				if _, err := peer.Write(frame); err != nil {
					b.Fatal(err)
				}
				if _, _, err := c.Read(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
