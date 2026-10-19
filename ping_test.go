package nwws

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// liveConfig is testConfig with liveness on.
func liveConfig(interval, timeout time.Duration) connConfig {
	cfg := testConfig()
	cfg.pingInterval, cfg.pongTimeout = interval, timeout
	return cfg
}

// expectPing reads one frame, checks that it is a ping and returns its
// payload.
func (p *rawPeer) expectPing() []byte {
	p.t.Helper()
	h, b := p.recv()
	if h.opcode != opPing || !h.fin {
		p.t.Fatalf("peer got opcode %d fin %v payload %q, want a ping", h.opcode, h.fin, b)
	}
	return b
}

// waitFor polls cond until it holds, failing the test after testTimeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// readAsync starts a Read in the background.
func readAsync(t *testing.T, c *Conn) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, _, err := readMsg(t, c)
		done <- err
	}()
	return done
}

func assertLivenessTimeout(t *testing.T, err error) {
	t.Helper()
	var ce *CloseError
	if !errors.Is(err, ErrLivenessTimeout) || !errors.As(err, &ce) || ce.Code != StatusAbnormalClosure {
		t.Fatalf("error %v, want CloseError 1006 wrapping ErrLivenessTimeout", err)
	}
}

func TestLivenessOptions(t *testing.T) {
	for _, tc := range []struct {
		opts              *ConnOptions
		interval, timeout time.Duration
	}{
		{nil, DefaultPingInterval, DefaultPongTimeout},
		{&ConnOptions{}, 10 * time.Second, 30 * time.Second},
		{&ConnOptions{PingInterval: -1, PongTimeout: -1}, DefaultPingInterval, DefaultPongTimeout},
		{&ConnOptions{PingInterval: time.Second, PongTimeout: 3 * time.Second}, time.Second, 3 * time.Second},
		{&ConnOptions{PingInterval: time.Second, DisableLiveness: true}, 0, 0},
	} {
		cfg := tc.opts.config()
		if cfg.pingInterval != tc.interval || cfg.pongTimeout != tc.timeout {
			t.Errorf("%+v: interval %v timeout %v, want %v %v", tc.opts, cfg.pingInterval, cfg.pongTimeout, tc.interval, tc.timeout)
		}
	}
}

// Each answered ping is followed by the next one an interval later, on
// both sides, each with a payload of its own.
func TestPingInterval(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			const interval = 20 * time.Millisecond
			c, peer := startConn(t, server, liveConfig(interval, testTimeout))
			start := time.Now()
			seen := map[string]bool{}
			for range 3 {
				p := peer.expectPing()
				if seen[string(p)] {
					t.Fatalf("ping payload %x repeated", p)
				}
				seen[string(p)] = true
				peer.send(true, opPong, p)
			}
			if elapsed := time.Since(start); elapsed < 3*interval*9/10 {
				t.Fatalf("3 pings in %v, want an interval of %v between them", elapsed, interval)
			}
			peer.send(true, opBinary, []byte("still open"))
			mustRead(t, c, MessageBinary, []byte("still open"))
		})
	}
}

// Pings go through the single writer: while the writer is stuck in a
// network write, no ping reaches the peer, and the frames stay intact.
func TestPingsThroughSingleWriter(t *testing.T) {
	t.Run("held writer", func(t *testing.T) {
		s, cl := tcpPair(t)
		g := newGateConn(s)
		const interval = 5 * time.Millisecond
		c := newConn(g, true, "", liveConfig(interval, testTimeout))
		t.Cleanup(func() {
			c.closeNow(ErrClosed)
			c.wait()
		})
		peer := newRawPeer(t, cl, false)

		written := make(chan error, 1)
		go func() { written <- c.Write(t.Context(), MessageBinary, []byte("first")) }()
		waitEntered(t, g)
		time.Sleep(10 * interval) // pings fall due while the write is held
		close(g.release)
		if err := waitErr(t, written); err != nil {
			t.Fatalf("Write: %v", err)
		}
		peer.expectFrame(opBinary, []byte("first"))
		peer.expectPing()
	})

	// A Write on an idle connection writes in the calling goroutine, so
	// with a single P the liveness goroutine may not run during the burst.
	// After their burst, the writers send empty messages and yield until the
	// peer has seen a ping between the frames.
	t.Run("concurrent writers", func(t *testing.T) {
		c, peer := startConn(t, true, liveConfig(time.Millisecond, testTimeout))
		const writers, each = 8, 50
		var wg sync.WaitGroup
		var sent atomic.Int64
		var pinged atomic.Bool
		stop := make(chan struct{})
		for w := range writers {
			wg.Go(func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					if i >= each && pinged.Load() {
						<-stop
						return
					}
					size := 0
					if i < each {
						size = 100 * i
					}
					mustWrite(t, c, MessageBinary, fmt.Appendf(nil, "%d:%d:%s", w, i, bytes.Repeat([]byte{'x'}, size)))
					sent.Add(1)
					if i >= each {
						runtime.Gosched()
					}
				}
			})
		}
		next := make([]int, writers)
		pings, data := 0, 0
		stopped := false
		for !stopped || int64(data) < sent.Load() {
			if !stopped && pings > 0 && data >= writers*each {
				close(stop)
				wg.Wait() // each writer has at most one message in flight, which the socket buffers
				stopped = true
				continue
			}
			h, b := peer.recv()
			switch h.opcode {
			case opPing:
				pings++
				pinged.Store(true)
				peer.send(true, opPong, b)
			case opBinary:
				var w, i int
				if _, err := fmt.Sscanf(string(b), "%d:%d:", &w, &i); err != nil || i != next[w] {
					t.Fatalf("frame %.20q out of order or corrupt (writer %d expects %d)", b, w, next[w])
				}
				next[w]++
				data++
			default:
				t.Fatalf("unexpected opcode %d", h.opcode)
			}
		}
	})
}

// The connection measures the time from a ping to its pong.
func TestRTT(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			const delay = 50 * time.Millisecond
			c, peer := startConn(t, server, liveConfig(20*time.Millisecond, testTimeout))
			if rtt := c.RTT(); rtt != 0 {
				t.Fatalf("RTT before any pong = %v, want 0", rtt)
			}
			p := peer.expectPing()
			time.Sleep(delay)
			peer.send(true, opPong, p)
			waitFor(t, "an RTT", func() bool { return c.RTT() != 0 })
			if rtt := c.RTT(); rtt < delay || rtt > delay+time.Second {
				t.Fatalf("RTT = %v, want about %v", rtt, delay)
			}
		})
	}
}

// RFC 6455 §5.5.3: unsolicited pongs are accepted and do not count as an
// answer to a ping when measuring the RTT.
func TestUnsolicitedPong(t *testing.T) {
	c, peer := startConn(t, true, liveConfig(20*time.Millisecond, testTimeout))
	peer.send(true, opPong, []byte("unsolicited"))
	p := peer.expectPing()
	peer.send(true, opPong, append([]byte("x"), p...)) // not the ping's payload
	peer.send(true, opBinary, []byte("after the pongs"))
	mustRead(t, c, MessageBinary, []byte("after the pongs"))
	if rtt := c.RTT(); rtt != 0 {
		t.Fatalf("RTT = %v after unsolicited pongs, want 0", rtt)
	}

	p = peer.expectPing()
	peer.send(true, opPong, p)
	waitFor(t, "an RTT", func() bool { return c.RTT() != 0 })
}

// blockedReadConn never returns from Read until it is closed: a peer that
// stopped reading.
type blockedReadConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (b *blockedReadConn) Read([]byte) (int, error) {
	<-b.closed
	return 0, net.ErrClosed
}

func (b *blockedReadConn) Close() error {
	b.once.Do(func() { close(b.closed) })
	return b.Conn.Close()
}

// A peer that stops answering is disconnected once the pong timeout has
// passed after a ping, and a pending Read reports the liveness timeout.
func TestSilentPeer(t *testing.T) {
	const interval, timeout = 20 * time.Millisecond, 100 * time.Millisecond
	const slack = 250 * time.Millisecond

	for _, server := range []bool{true, false} {
		t.Run("raw peer/"+map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, liveConfig(interval, timeout))
			read := readAsync(t, c)
			peer.expectPing() // and never answer
			pinged := time.Now()
			err := waitErr(t, read)
			elapsed := time.Since(pinged)
			assertLivenessTimeout(t, err)
			if elapsed < timeout*8/10 || elapsed > timeout+slack {
				t.Fatalf("closed %v after the ping, want about %v", elapsed, timeout)
			}
			peer.expectEOF()
			if err := c.Write(t.Context(), MessageBinary, []byte("late")); err == nil {
				t.Fatal("Write after the timeout succeeded")
			}
		})
	}

	t.Run("peer not reading", func(t *testing.T) {
		s, cl := tcpPair(t)
		stuck := &blockedReadConn{Conn: cl, closed: make(chan struct{})}
		peer := newConn(stuck, false, "", testConfig())
		t.Cleanup(func() {
			peer.closeNow(ErrClosed)
			peer.wait()
		})
		start := time.Now()
		c := newConn(s, true, "", liveConfig(interval, timeout))
		t.Cleanup(func() {
			c.closeNow(ErrClosed)
			c.wait()
		})
		_, _, err := readMsg(t, c)
		elapsed := time.Since(start)
		assertLivenessTimeout(t, err)
		if elapsed < timeout || elapsed > interval+timeout+slack {
			t.Fatalf("closed after %v, want about %v", elapsed, interval+timeout)
		}
	})
}

// While a message waits for the application's Read, frames are not read,
// so the peer's pongs wait too. That alone does not time the peer out.
func TestSlowReaderKeepsLivePeer(t *testing.T) {
	const interval, timeout = 10 * time.Millisecond, 50 * time.Millisecond
	s, cl := tcpPair(t)
	c := newConn(s, true, "", liveConfig(interval, timeout))
	peer := newConn(cl, false, "", testConfig()) // answers pings by itself
	t.Cleanup(func() {
		c.closeNow(ErrClosed)
		peer.closeNow(ErrClosed)
		c.wait()
		peer.wait()
	})

	mustWrite(t, peer, MessageBinary, []byte("m1"))
	time.Sleep(6 * timeout) // the application is slow to call Read
	mustRead(t, c, MessageBinary, []byte("m1"))

	mustWrite(t, peer, MessageBinary, []byte("m2"))
	mustRead(t, c, MessageBinary, []byte("m2"))
	mustWrite(t, c, MessageText, []byte("back"))
	mustRead(t, peer, MessageText, []byte("back"))

	// Liveness keeps working after the slow Read.
	waitFor(t, "an RTT", func() bool { return c.RTT() != 0 })
	time.Sleep(3 * timeout)
	mustWrite(t, peer, MessageBinary, []byte("m3"))
	mustRead(t, c, MessageBinary, []byte("m3"))
}

func TestLivenessDisabled(t *testing.T) {
	cfg := (&ConnOptions{PingInterval: 5 * time.Millisecond, PongTimeout: 20 * time.Millisecond, DisableLiveness: true}).config()
	c, peer := startConn(t, true, cfg)
	_ = peer.nc.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if h, b, err := peer.fr.readFrame(); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("peer read opcode %d payload %q err %v, want no frame", h.opcode, b, err)
	}
	peer.send(true, opBinary, []byte("still open"))
	mustRead(t, c, MessageBinary, []byte("still open"))
	if rtt := c.RTT(); rtt != 0 {
		t.Fatalf("RTT = %v with liveness off, want 0", rtt)
	}
}

// Accept and Dial take the liveness options: both sides ping and measure
// the RTT.
func TestLivenessOptionPlumbing(t *testing.T) {
	opts := ConnOptions{PingInterval: 10 * time.Millisecond}
	accepted := make(chan *Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r, &AcceptOptions{ConnOptions: opts})
		if err != nil {
			return
		}
		accepted <- c
		_, _, _ = c.Read(r.Context()) // until the client closes
	}))
	defer ts.Close()

	c, _, err := Dial(t.Context(), wsURL(ts.URL), &DialOptions{ConnOptions: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(StatusNormalClosure, "") }()
	var sc *Conn
	select {
	case sc = <-accepted:
	case <-time.After(testTimeout):
		t.Fatal("server never accepted")
	}
	waitFor(t, "the client's RTT", func() bool { return c.RTT() != 0 })
	waitFor(t, "the server's RTT", func() bool { return sc.RTT() != 0 })
}

// livenessGoroutines counts running liveness goroutines.
func livenessGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return bytes.Count(buf[:n], []byte("(*liveness).run"))
		}
		buf = make([]byte, 2*len(buf))
	}
}

// Liveness goroutines end with their connection, however it ends.
func TestLivenessNoGoroutineLeak(t *testing.T) {
	const interval, timeout = 5 * time.Millisecond, 50 * time.Millisecond
	before := runtime.NumGoroutine()

	for range 5 {
		// Closed by the application.
		s, cl := tcpPair(t)
		c := newConn(s, true, "", liveConfig(interval, timeout))
		peer := newConn(cl, false, "", liveConfig(interval, timeout))
		mustWrite(t, peer, MessageBinary, []byte("hi"))
		mustRead(t, c, MessageBinary, []byte("hi"))
		if err := c.Close(StatusNormalClosure, ""); err != nil {
			t.Fatalf("Close: %v", err)
		}
		waitDone(t, peer)

		// Closed by the liveness timeout.
		s2, cl2 := tcpPair(t)
		c2 := newConn(s2, true, "", liveConfig(interval, timeout))
		_, _, err := readMsg(t, c2)
		assertLivenessTimeout(t, err)
		waitDone(t, c2)
		_ = cl2.Close()

		// Lost: the peer closes TCP.
		s3, cl3 := tcpPair(t)
		c3 := newConn(s3, false, "", liveConfig(interval, timeout))
		_ = cl3.Close()
		waitDone(t, c3)
	}

	if n := livenessGoroutines(); n != 0 {
		t.Fatalf("%d liveness goroutines still running after their connections ended", n)
	}
	waitFor(t, "the goroutine count to return to its baseline", func() bool {
		return runtime.NumGoroutine() <= before
	})
}
