package nwws

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// writeBenchSizes are the message sizes of the write benchmarks: a typical
// small message and one larger than the write buffer.
var writeBenchSizes = []struct {
	name string
	n    int
}{
	{"300B", 300},
	{"64KiB", 64 << 10},
}

// benchUnixConn returns the server side of a Unix domain socket as a
// Conn, with liveness off, and the raw client end. Unlike loopback TCP,
// it has no receive window that the kernel may shrink under memory
// pressure, which can stall a writer for seconds.
func benchUnixConn(b *testing.B) (*Conn, net.Conn) {
	b.Helper()
	ln, err := net.Listen("unix", filepath.Join(b.TempDir(), "s"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		s, _ := ln.Accept()
		accepted <- s
	}()
	peer, err := net.Dial("unix", ln.Addr().String())
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

// BenchmarkWriteThroughput measures how fast messages are written by one
// goroutine sending back to back, and by 16 goroutines sending at once,
// to a peer that reads as fast as it can over a Unix domain socket.
func BenchmarkWriteThroughput(b *testing.B) {
	for _, writers := range []struct {
		name string
		n    int
	}{{"1writer", 1}, {"16writers", 16}} {
		for _, size := range writeBenchSizes {
			b.Run(writers.name+"/"+size.name, func(b *testing.B) {
				c, peer := benchUnixConn(b)
				go func() { _, _ = io.Copy(io.Discard, peer) }()
				msg := make([]byte, size.n)
				ctx := context.Background()
				var left atomic.Int64
				left.Store(int64(b.N))
				b.SetBytes(int64(size.n))
				b.ReportAllocs()
				b.ResetTimer()
				var wg sync.WaitGroup
				for range writers.n {
					wg.Go(func() {
						for left.Add(-1) >= 0 {
							if err := c.Write(ctx, MessageBinary, msg); err != nil {
								b.Error(err)
								return
							}
						}
					})
				}
				wg.Wait()
			})
		}
	}
}
