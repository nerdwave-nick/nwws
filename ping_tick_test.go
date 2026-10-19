package nwws

import (
	"bufio"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Liveness runs in the write goroutine. While that goroutine is blocked
// writing a batch to a peer that reads nothing, the liveness timeout still
// fires: Read reports it within the pong timeout plus the close frame's
// write deadline, the blocked writes fail, and every goroutine ends.
func TestLivenessTimeoutFrozenPeerQueuedWrite(t *testing.T) {
	const interval, timeout = 20 * time.Millisecond, 100 * time.Millisecond
	const slack = 500 * time.Millisecond
	before := runtime.NumGoroutine()

	s, cl := frozenPair(t)
	_ = cl // never read
	g := newGateConn(s)
	c := newConn(g, true, "", liveConfig(interval, timeout))
	first := make(chan error, 1)
	go func() { first <- c.Write(t.Context(), MessageBinary, []byte("first")) }()
	waitEntered(t, g) // the direct write is held
	queued := make(chan error, 1)
	go func() { queued <- c.Write(t.Context(), MessageBinary, make([]byte, 16<<20)) }()
	waitQueued(t, c, 1)
	start := time.Now()
	close(g.release) // the write goroutine takes the large message and blocks

	_, _, err := readMsg(t, c)
	elapsed := time.Since(start)
	assertLivenessTimeout(t, err)
	if limit := interval + timeout + livenessCloseWait + slack; elapsed > limit {
		t.Fatalf("detected after %v, want at most %v", elapsed, limit)
	}
	if err := waitErr(t, first); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := waitErr(t, queued); err == nil {
		t.Fatal("the blocked Write succeeded")
	}
	waitDone(t, c)
	waitFor(t, "the goroutine count to return to its baseline", func() bool {
		return runtime.NumGoroutine() <= before
	})
}

// smallBufferConn is startConn with small socket buffers between the
// Conn and its peer, so that the Conn's writes block as soon as the peer
// falls behind, and no frame waits behind much buffered data.
func smallBufferConn(t *testing.T, server bool, cfg connConfig) (*Conn, *rawPeer) {
	t.Helper()
	s, cl := tcpPair(t)
	mine, theirs := s, cl
	if !server {
		mine, theirs = cl, s
	}
	if err := mine.(*net.TCPConn).SetWriteBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	if err := theirs.(*net.TCPConn).SetReadBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	c := newConn(mine, server, "", cfg)
	t.Cleanup(func() {
		c.closeNow(ErrClosed)
		c.wait()
	})
	peer := newRawPeer(t, theirs, !server)
	peer.fr.r = bufio.NewReader(theirs) // keeps up with small frames
	return c, peer
}

// Direct writes leave the write goroutine idle, and pings still go out at
// the interval while a sender writes without pause, blocking in the
// network whenever the peer falls behind.
func TestPingsDuringDirectWrites(t *testing.T) {
	const interval, run = 20 * time.Millisecond, 300 * time.Millisecond
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := smallBufferConn(t, server, liveConfig(interval, testTimeout))
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				msg := make([]byte, 64)
				for {
					select {
					case <-stop:
						mustWrite(t, c, MessageText, []byte("end"))
						return
					default:
					}
					mustWrite(t, c, MessageBinary, msg)
				}
			})
			pings := 0
			deadline := time.Now().Add(run)
			for time.Now().Before(deadline) {
				h, b := peer.recv()
				if h.opcode == opPing {
					pings++
					peer.send(true, opPong, b)
				}
			}
			close(stop)
			for { // until the sender's last message
				if h, b := peer.recv(); h.opcode == opText && string(b) == "end" {
					break
				} else if h.opcode == opPing {
					peer.send(true, opPong, b)
				}
			}
			wg.Wait()
			t.Logf("%d pings in %v at an interval of %v", pings, run, interval)
			if want := int(run/interval) / 3; pings < want || pings > int(run/interval)+1 {
				t.Fatalf("%d pings in %v at an interval of %v, want between %d and %d", pings, run, interval, want, run/interval+1)
			}
		})
	}
}
