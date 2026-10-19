package nwws

import (
	"bytes"
	"compress/flate"
	"context"
	"net"
	"testing"
	"time"
)

// BenchmarkReadInto is BenchmarkReadThroughput with ReadInto into a buffer
// that is reused for every message. In steady state it allocates nothing
// per message.
func BenchmarkReadInto(b *testing.B) {
	for _, size := range readBenchSizes {
		b.Run(size.name, func(b *testing.B) {
			c, peer := benchConn(b)
			perBatch := max(1, (64<<10)/size.n)
			batch := benchFrames(b, size.n, perBatch)
			benchRead(b, c, peer, batch, perBatch, size.n, true)
		})
	}
}

// BenchmarkLargeReadInto is BenchmarkLargeRead with ReadInto into a buffer
// that is reused for every message. Compare runs whose speed-us agree, as
// BenchmarkLargeRead explains.
func BenchmarkLargeReadInto(b *testing.B) {
	const size = 1 << 20
	for _, role := range []struct {
		name   string
		server bool
	}{{"server", true}, {"client", false}} {
		b.Run(role.name, func(b *testing.B) {
			c, peer := loopbackConn(b, role.server)
			speed := speedIndex()
			benchRead(b, c, peer, peerFrame(b, role.server, size), 1, size, true)
			b.ReportMetric((speed+speedIndex())/2, "speed-us")
		})
	}
}

// BenchmarkReadCompressed measures Read and ReadInto of compressed
// messages of compressible text, without context takeover, which a
// server peer sends to the client side as fast as it can.
func BenchmarkReadCompressed(b *testing.B) {
	for _, api := range []struct {
		name string
		into bool
	}{{"Read", false}, {"ReadInto", true}} {
		for _, size := range readBenchSizes {
			b.Run(api.name+"/"+size.name, func(b *testing.B) {
				c, peer := compressedBenchConn(b)
				var wire bytes.Buffer
				fw := frameWriter{w: &wire, server: true, rsv: rsv1Bit}
				payload := referenceDeflate(b, flate.BestSpeed, compressibleText(size.n))
				perBatch := max(1, (64<<10)/len(payload))
				for range perBatch {
					if err := fw.writeFrame(true, rsv1Bit, opBinary, payload); err != nil {
						b.Fatal(err)
					}
				}
				benchRead(b, c, peer, wire.Bytes(), perBatch, size.n, api.into)
			})
		}
	}
}

// compressedBenchConn returns the client side of a loopback TCP connection
// with permessage-deflate, without context takeover from the peer, as a
// Conn with liveness off, and the raw server end.
func compressedBenchConn(b *testing.B) (*Conn, net.Conn) {
	b.Helper()
	server, client := loopbackPair(b)
	cfg := connConfig{readLimit: 2 << 20, closeTimeout: time.Second}
	cfg.deflate = &deflateConfig{threshold: 64, level: flate.BestSpeed, writeWindowBits: maxWindowBits}
	c := newConn(client, false, "", cfg)
	b.Cleanup(func() {
		_ = server.Close()
		c.closeNow(ErrClosed)
		c.wait()
	})
	return c, server
}

// benchRead reads b.N messages of n bytes with Read, or with ReadInto into
// a reused buffer when into is set, while the peer writes batch, which
// holds perBatch messages, as often as needed.
func benchRead(b *testing.B, c *Conn, peer net.Conn, batch []byte, perBatch, n int, into bool) {
	writeErr := make(chan error, 1)
	b.SetBytes(int64(n))
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
	buf := make([]byte, 0, n)
	for range b.N {
		var err error
		if into {
			_, buf, err = c.ReadInto(ctx, buf)
		} else {
			_, _, err = c.Read(ctx)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := <-writeErr; err != nil {
		b.Fatal(err)
	}
}
