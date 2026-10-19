package nwws

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

// BenchmarkLargeRead guards the large-message read path: Read takes 1 MiB
// binary messages that a raw peer sends as fast as it can over loopback
// TCP, in the server role (masked frames) and the client role (unmasked).
//
// Besides MB/s it reports speed-us, the time a fixed, cache-resident CPU
// workload takes on the benchmark goroutine, measured before and after the
// timed loop. It is a speed index of the machine during the run: on a
// hybrid or power-limited CPU, or a shared machine, the speed of the core
// a run lands on can change several times over between runs and even
// within one, and every read path's throughput follows it.
//
// To check the spread, run it at least 10 times:
//
//	go test -run '^$' -bench LargeRead -count 10
//
// and compare max/min of MB/s per role. Only compare runs whose speed-us
// agree, within about 20 %; if speed-us varies much more, the machine
// changed speed and the spread says nothing about nwws.
func BenchmarkLargeRead(b *testing.B) {
	const size = 1 << 20
	for _, role := range []struct {
		name   string
		server bool
	}{{"server", true}, {"client", false}} {
		b.Run(role.name, func(b *testing.B) {
			c, peer := loopbackConn(b, role.server)
			frame := peerFrame(b, role.server, size)
			speed := speedIndex()
			writeErr := make(chan error, 1)
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			go func() {
				for range b.N {
					if _, err := peer.Write(frame); err != nil {
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
			b.ReportMetric((speed+speedIndex())/2, "speed-us")
		})
	}
}

// loopbackConn returns one end of a loopback TCP connection as a Conn on
// the given side, with liveness off, and the raw other end.
func loopbackConn(b *testing.B, server bool) (*Conn, net.Conn) {
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
	c := newConn(s, server, "", connConfig{readLimit: 2 << 20, closeTimeout: time.Second})
	b.Cleanup(func() {
		_ = peer.Close()
		c.closeNow(ErrClosed)
		c.wait()
	})
	return c, peer
}

// peerFrame returns one binary frame of n bytes as the peer of a Conn on
// the given side sends it: masked to a server, unmasked to a client.
func peerFrame(b *testing.B, server bool, n int) []byte {
	b.Helper()
	var wire bytes.Buffer
	fw := frameWriter{w: &wire, server: !server}
	if err := fw.writeFrame(true, 0, opBinary, make([]byte, n)); err != nil {
		b.Fatal(err)
	}
	return wire.Bytes()
}

var speedSrc, speedDst [64 << 10]byte

// speedIndex returns the time, in microseconds, of a fixed CPU workload
// on data that stays in the cache.
func speedIndex() float64 {
	start := time.Now()
	for range 2000 {
		copy(speedDst[:], speedSrc[:])
		maskBytes([4]byte{1, 2, 3, 4}, 0, speedDst[:])
	}
	return float64(time.Since(start).Microseconds())
}
