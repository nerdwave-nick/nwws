package nwws

import (
	"bytes"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// On a liveness timeout, a peer that still reads frames but never answers
// pings receives a close frame 1001 with the reason "liveness timeout",
// and then TCP closes without waiting for its close frame. Read still
// reports 1006 with ErrLivenessTimeout locally.
func TestLivenessTimeoutSendsGoingAway(t *testing.T) {
	const interval, timeout = 20 * time.Millisecond, 100 * time.Millisecond
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, liveConfig(interval, timeout))
			read := readAsync(t, c)
			if reason := peer.expectClose(StatusGoingAway); reason != livenessCloseReason {
				t.Fatalf("close reason %q, want %q", reason, livenessCloseReason)
			}
			peer.expectEOF() // without the peer answering the close frame
			assertLivenessTimeout(t, waitErr(t, read))
			waitDone(t, c)
		})
	}
}

// frozenPair returns a loopback TCP pair with small socket buffers, so
// that writes to client block soon once nobody reads it.
func frozenPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	s, cl := tcpPair(t)
	if err := s.(*net.TCPConn).SetWriteBuffer(4 << 10); err != nil {
		t.Fatal(err)
	}
	if err := cl.(*net.TCPConn).SetReadBuffer(4 << 10); err != nil {
		t.Fatal(err)
	}
	return s, cl
}

// A frozen peer, which reads nothing, so that the connection's network
// writes block, is still detected: the close frame cannot be written, and
// the write deadline ends the wait for it. Read reports the liveness
// timeout within the pong timeout plus that deadline, and every goroutine
// of the connection ends.
func TestLivenessTimeoutFrozenPeer(t *testing.T) {
	const interval, timeout = 20 * time.Millisecond, 100 * time.Millisecond
	const slack = 500 * time.Millisecond
	before := runtime.NumGoroutine()

	s, cl := frozenPair(t)
	_ = cl // never read
	start := time.Now()
	c := newConn(s, true, "", liveConfig(interval, timeout))
	written := make(chan error, 1)
	go func() { written <- c.Write(t.Context(), MessageBinary, make([]byte, 16<<20)) }()

	_, _, err := readMsg(t, c)
	elapsed := time.Since(start)
	assertLivenessTimeout(t, err)
	if limit := interval + timeout + livenessCloseWait + slack; elapsed < timeout || elapsed > limit {
		t.Fatalf("detected after %v, want between %v and %v", elapsed, timeout, limit)
	}
	if err := waitErr(t, written); err == nil {
		t.Fatal("the blocked Write succeeded")
	}
	waitDone(t, c)
	if n := livenessGoroutines(); n != 0 {
		t.Fatalf("%d liveness goroutines still running", n)
	}
	waitFor(t, "the goroutine count to return to its baseline", func() bool {
		return runtime.NumGoroutine() <= before
	})
}

// waitClosing polls until the connection accepts no more frames: a close
// frame is queued.
func waitClosing(t *testing.T, c *Conn) {
	t.Helper()
	waitFor(t, "a queued close frame", func() bool {
		c.wmu.Lock()
		defer c.wmu.Unlock()
		return c.wclosed
	})
}

// The close frame of a liveness timeout goes through the single writer:
// while the writer is stuck in a network write, the close frame waits
// behind it, and the frames reach the peer intact and in order. A writer
// that stays stuck, even in a write that ignores deadlines, delays the
// close by at most the write deadline.
func TestLivenessCloseThroughSingleWriter(t *testing.T) {
	const interval, timeout = 5 * time.Millisecond, 50 * time.Millisecond

	start := func(t *testing.T) (*Conn, *rawPeer, *gateConn, <-chan error) {
		s, cl := tcpPair(t)
		g := newGateConn(s)
		c := newConn(g, true, "", liveConfig(interval, timeout))
		t.Cleanup(func() {
			c.closeNow(ErrClosed)
			c.wait()
		})
		written := make(chan error, 1)
		go func() { written <- c.Write(t.Context(), MessageBinary, []byte("first")) }()
		waitEntered(t, g)
		return c, newRawPeer(t, cl, false), g, written
	}

	t.Run("writer released", func(t *testing.T) {
		c, peer, g, written := start(t)
		read := readAsync(t, c)
		waitClosing(t, c) // the timeout expired while the write is held
		close(g.release)
		if err := waitErr(t, written); err != nil {
			t.Fatalf("Write: %v", err)
		}
		peer.expectFrame(opBinary, []byte("first"))
		for {
			h, b := peer.recv()
			if h.opcode == opPing {
				continue
			}
			want := closePayload(StatusGoingAway, livenessCloseReason)
			if h.opcode != opClose || !h.fin || !bytes.Equal(b, want) {
				t.Fatalf("peer got opcode %d fin %v payload %q, want the close frame %q", h.opcode, h.fin, b, want)
			}
			break
		}
		peer.expectEOF()
		assertLivenessTimeout(t, waitErr(t, read))
	})

	t.Run("writer stuck", func(t *testing.T) {
		c, peer, _, written := start(t)
		read := readAsync(t, c)
		waitClosing(t, c)
		queued := time.Now()
		err := waitErr(t, read)
		assertLivenessTimeout(t, err)
		if waited := time.Since(queued); waited > livenessCloseWait+500*time.Millisecond {
			t.Fatalf("closed %v after queueing the close frame, want at most about %v", waited, livenessCloseWait)
		}
		if err := waitErr(t, written); err == nil {
			t.Fatal("the stuck Write succeeded")
		}
		peer.expectEOF()
		waitDone(t, c)
	})
}

// A connection's liveness timer has finished once the connection has: when
// a blocked write and the timer's last tick both end a liveness timeout at
// the same moment, no timer callback of the connection still runs once
// its goroutines are done.
func TestLivenessTimerDoneWithConnection(t *testing.T) {
	const conns, rounds = 32, 4
	const interval, timeout = 5 * time.Millisecond, 20 * time.Millisecond
	cfg := liveConfig(interval, timeout)
	cfg.closeTimeout = timeout // the close frame's write deadline and the timer's last tick coincide
	for range rounds {
		var wg sync.WaitGroup
		for range conns {
			s, _ := frozenPair(t)
			c := newConn(s, true, "", cfg)
			go func() { _ = c.Write(t.Context(), MessageBinary, make([]byte, 4<<20)) }()
			wg.Go(func() {
				_, _, err := readMsg(t, c)
				assertLivenessTimeout(t, err)
				waitDone(t, c)
				if n := timerCallbacks(c); n != 0 {
					t.Errorf("%d liveness timer callbacks of the connection still running after it ended", n)
				}
			})
		}
		wg.Wait()
	}
}

// timerCallbacks counts the running liveness timer callbacks of c.
func timerCallbacks(c *Conn) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return bytes.Count(buf[:n], fmt.Appendf(nil, "(*liveness).fire(%p", c.live))
		}
		buf = make([]byte, 2*len(buf))
	}
}
