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
	waitEntered(t, g) // the first network write is held: this Write, or a ping
	const large = 16 << 20
	queued := make(chan error, 1)
	go func() { queued <- c.Write(t.Context(), MessageBinary, make([]byte, large)) }()
	waitFor(t, "the large message to be queued", func() bool {
		c.wmu.Lock()
		defer c.wmu.Unlock()
		for _, r := range c.queue {
			if len(r.payload) == large {
				return true
			}
		}
		return false
	})
	start := time.Now()
	close(g.release) // the write goroutine takes the large message and blocks

	_, _, err := readMsg(t, c)
	elapsed := time.Since(start)
	assertLivenessTimeout(t, err)
	if limit := interval + timeout + livenessCloseWait + slack; elapsed > limit {
		t.Fatalf("detected after %v, want at most %v", elapsed, limit)
	}
	_ = waitErr(t, first) // it may end with the connection
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
// falls behind, and no frame waits behind much buffered data. It uses
// smallSegmentPair, as the frames are small.
func smallBufferConn(t *testing.T, server bool, cfg connConfig) (*Conn, *rawPeer) {
	t.Helper()
	s, cl := smallSegmentPair(t)
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

// Pings keep their interval while earlier ones still await their pongs:
// a peer that answers each ping only after several intervals still gets
// a ping every interval, and the connection still measures the RTT.
func TestPingsKeepIntervalWhileUnanswered(t *testing.T) {
	const interval, delay, run = 20 * time.Millisecond, 60 * time.Millisecond, 300 * time.Millisecond
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, liveConfig(interval, testTimeout))
			type pong struct {
				payload []byte
				due     time.Time
			}
			pongs := make(chan pong, 64)
			var wg sync.WaitGroup
			wg.Go(func() { // answers each ping delay after it arrived
				for p := range pongs {
					time.Sleep(time.Until(p.due))
					peer.send(true, opPong, p.payload)
				}
			})
			defer wg.Wait()
			defer close(pongs)
			pings := 0
			deadline := time.Now().Add(run)
			for time.Now().Before(deadline) {
				p := peer.expectPing()
				pongs <- pong{payload: p, due: time.Now().Add(delay)}
				pings++
			}
			t.Logf("%d pings in %v at an interval of %v, each answered after %v", pings, run, interval, delay)
			if want := int(run/interval) * 3 / 5; pings < want {
				t.Fatalf("%d pings in %v at an interval of %v, want at least %d", pings, run, interval, want)
			}
			waitFor(t, "an RTT", func() bool { return c.RTT() != 0 })
			if rtt := c.RTT(); rtt < delay {
				t.Fatalf("RTT = %v, want at least the peer's delay of %v", rtt, delay)
			}
		})
	}
}

// A late tick keeps the schedule: the next ping falls due an interval
// after the last one was due, not an interval after the late tick, and a
// tick late by more than an interval sends one ping, not one per interval
// missed.
func TestTickKeepsSchedule(t *testing.T) {
	c, peer := startConn(t, true, liveConfig(time.Hour, time.Hour)) // ticks only when called
	l := c.live
	var due time.Time // when the first ping falls due
	waitFor(t, "the liveness timer", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		due = l.nextPing
		return !due.IsZero()
	})
	for _, tc := range []struct {
		late time.Duration // how late the tick runs
		next time.Duration // how long until the next tick is due
	}{
		{late: 15 * time.Minute, next: 45 * time.Minute},
		{late: 0, next: time.Hour},
		{late: 150 * time.Minute, next: 30 * time.Minute},
	} {
		now := due.Add(tc.late)
		next, ok := l.tick(now)
		if !ok || next != tc.next {
			t.Fatalf("tick %v late: next tick in %v, %v; want in %v", tc.late, next, ok, tc.next)
		}
		p := peer.expectPing()
		peer.send(true, opPong, p)
		waitFor(t, "the pong", func() bool {
			l.mu.Lock()
			defer l.mu.Unlock()
			return l.waiting.IsZero()
		})
		due = now.Add(next)
	}
}
