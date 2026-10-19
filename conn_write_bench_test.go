package nwws

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
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

// BenchmarkWriteThroughput measures how fast messages are written by one
// goroutine sending back to back, and by 16 goroutines sending at once,
// to a peer that reads as fast as it can.
func BenchmarkWriteThroughput(b *testing.B) {
	for _, writers := range []struct {
		name string
		n    int
	}{{"1writer", 1}, {"16writers", 16}} {
		for _, size := range writeBenchSizes {
			b.Run(writers.name+"/"+size.name, func(b *testing.B) {
				c, peer := benchConn(b)
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
