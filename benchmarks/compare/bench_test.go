package compare

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	smallSize = 300
	largeSize = 1 << 20
	writers   = 16
)

// sizeName returns the sub-benchmark name of a message size.
func sizeName(n int) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return "size=" + strconv.Itoa(n>>20) + "MiB"
	case n >= 1<<10 && n%(1<<10) == 0:
		return "size=" + strconv.Itoa(n>>10) + "KiB"
	}
	return "size=" + strconv.Itoa(n) + "B"
}

// forEachLib runs f as a sub-benchmark lib=<name> for every library.
func forEachLib(b *testing.B, f func(b *testing.B, lib library)) {
	for _, lib := range libraries {
		b.Run("lib="+lib.name, func(b *testing.B) { f(b, lib) })
	}
}

// BenchmarkRoundTrip measures one round trip of a 300 B message: the
// client writes it, the server echoes it, and the client reads the echo.
// allocs/op counts both sides, which share the process.
func BenchmarkRoundTrip(b *testing.B) {
	forEachLib(b, func(b *testing.B, lib library) {
		b.Run(sizeName(smallSize), func(b *testing.B) {
			client, server := pair(b, lib)
			echo(server)
			msg := make([]byte, smallSize)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := client.write(msg); err != nil {
					b.Fatal(err)
				}
				p, err := client.readReuse()
				if err != nil {
					b.Fatal(err)
				}
				if len(p) != smallSize {
					b.Fatalf("echo has %d bytes, want %d", len(p), smallSize)
				}
			}
		})
	})
}

// BenchmarkThroughput measures how fast one client goroutine writes
// messages to a server that reads and discards them. The time runs until
// the server has read the last message. allocs/op counts both sides.
func BenchmarkThroughput(b *testing.B) {
	forEachLib(b, func(b *testing.B, lib library) {
		for _, size := range []int{smallSize, 64 << 10} {
			b.Run(sizeName(size), func(b *testing.B) {
				client, server := pair(b, lib)
				n := b.N
				done := drain(server, n, size)
				msg := make([]byte, size)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for range n {
					if err := client.write(msg); err != nil {
						b.Fatal(err)
					}
				}
				if err := <-done; err != nil {
					b.Fatal(err)
				}
			})
		}
	})
}

// BenchmarkConcurrentWriters measures 16 client goroutines writing 300 B
// messages on one connection to a server that reads and discards them.
// gorilla's and gobwas's writers take turns on a mutex. The time runs until the server
// has read the last message. allocs/op counts both sides.
func BenchmarkConcurrentWriters(b *testing.B) {
	forEachLib(b, func(b *testing.B, lib library) {
		b.Run("writers="+strconv.Itoa(writers), func(b *testing.B) {
			b.Run(sizeName(smallSize), func(b *testing.B) {
				client, server := pair(b, lib)
				n := int64(b.N)
				done := drain(server, b.N, smallSize)
				msg := make([]byte, smallSize)
				var next atomic.Int64
				var wg sync.WaitGroup
				errs := make(chan error, writers)
				b.SetBytes(smallSize)
				b.ReportAllocs()
				b.ResetTimer()
				for range writers {
					wg.Go(func() {
						for next.Add(1) <= n {
							if err := client.write(msg); err != nil {
								errs <- err
								client.close() // the server fails at once
								return
							}
						}
					})
				}
				wg.Wait()
				close(errs)
				if err := <-errs; err != nil {
					b.Fatal(err)
				}
				if err := <-done; err != nil {
					b.Fatal(err)
				}
			})
		})
	})
}

// BenchmarkRead1MiB measures the server reading 1 MiB messages, which a
// raw client, not a WebSocket library, sends as fast as it can in single
// masked frames, so that only the read side differs between libraries.
// api=whole reads with the whole-message API, which returns a new slice
// for every message (nwws Read, coder Read, gorilla ReadMessage, gobwas
// wsutil.Reader and io.ReadAll as in wsutil.ReadData); api=reuse reads
// into a buffer the application reuses (nwws ReadInto, coder Reader,
// gorilla NextReader and gobwas wsutil.Reader into a reused
// bytes.Buffer).
func BenchmarkRead1MiB(b *testing.B) {
	frame := maskedFrame(largeSize)
	forEachLib(b, func(b *testing.B, lib library) {
		for _, api := range []string{"whole", "reuse"} {
			b.Run("api="+api, func(b *testing.B) {
				url, accepted := serve(b, lib, 1)
				nc := rawDial(b, url)
				server := accept(b, accepted)
				b.Cleanup(server.close)
				n := b.N
				writeErr := make(chan error, 1)
				b.SetBytes(largeSize)
				b.ReportAllocs()
				b.ResetTimer()
				check := func(i int, p []byte) error {
					if len(p) != largeSize {
						return fmt.Errorf("message %d has %d bytes, want %d", i, len(p), largeSize)
					}
					return nil
				}
				go func() {
					for range n {
						if _, err := nc.Write(frame); err != nil {
							writeErr <- err
							return
						}
					}
					writeErr <- nil
				}()
				pc := server.(puller)
				// The benchmark goroutine reads, as an application would.
				read := pc.readReuse
				if api == "whole" {
					read = pc.read
				}
				for i := range n {
					p, err := read()
					if err == nil {
						err = check(i, p)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if err := <-writeErr; err != nil {
					b.Fatal(err)
				}
			})
		}
	})
}
