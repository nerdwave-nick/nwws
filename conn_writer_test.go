package nwws

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// writeGoroutines counts the write goroutines running in the process.
func writeGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return bytes.Count(buf[:n], []byte("(*Conn).writeLoop("))
		}
		buf = make([]byte, 2*len(buf))
	}
}

func waitNoWriteGoroutine(t *testing.T) {
	t.Helper()
	waitFor(t, "no write goroutine", func() bool { return writeGoroutines() == 0 })
}

// The write goroutine runs only while frames wait to be written: a Write
// on an idle connection writes itself, frames queued behind a write start
// the goroutine, and it ends once nothing is left to write.
func TestWriteGoroutineOnlyWhileQueued(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	waitNoWriteGoroutine(t)

	mustWrite(t, c, MessageBinary, []byte("direct"))
	peer.expectFrame(opBinary, []byte("direct"))
	if n := writeGoroutines(); n != 0 {
		t.Fatalf("%d write goroutines after a direct write, want 0", n)
	}

	peer.send(true, opPing, []byte("ping")) // the pong is queued
	peer.expectFrame(opPong, []byte("ping"))
	waitNoWriteGoroutine(t)

	const writers, each = 8, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range each {
				mustWrite(t, c, MessageBinary, fmt.Appendf(nil, "%d:%d", w, i))
			}
		})
	}
	next := make([]int, writers)
	for range writers * each {
		h, b := peer.recv()
		var w, i int
		if _, err := fmt.Sscanf(string(b), "%d:%d", &w, &i); h.opcode != opBinary || err != nil || i != next[w] {
			t.Fatalf("frame %q out of order or corrupt", b)
		}
		next[w]++
	}
	wg.Wait()
	waitNoWriteGoroutine(t)

	mustWrite(t, c, MessageBinary, []byte("still open"))
	peer.expectFrame(opBinary, []byte("still open"))
}

// Liveness needs no write goroutine of its own: between pings, none runs,
// and pings keep coming.
func TestLivenessWithoutWriteGoroutine(t *testing.T) {
	const interval = 10 * time.Millisecond
	_, peer := startConn(t, true, liveConfig(interval, testTimeout))
	done := make(chan struct{})
	var pings atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			peer.send(true, opPong, peer.expectPing())
			pings.Add(1)
		}
	})
	waitNoWriteGoroutine(t)
	seen := pings.Load()
	waitFor(t, "two more pings", func() bool { return pings.Load() >= seen+2 })
	close(done)
	wg.Wait()
}
