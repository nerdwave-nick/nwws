package nwws

import (
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// roles runs f as a subtest for the server and the client side.
func roles(t *testing.T, f func(t *testing.T, server bool)) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			f(t, server)
		})
	}
}

// closeTimeoutConfig is the configuration ConnOptions with CloseTimeout d
// gives, without liveness.
func closeTimeoutConfig(d time.Duration) connConfig {
	return (&ConnOptions{CloseTimeout: d, DisableLiveness: true}).config()
}

// assertTookAbout checks that d is at least want, less a little timer
// slack, and well below the default close timeout.
func assertTookAbout(t *testing.T, what string, d, want time.Duration) {
	t.Helper()
	if d < want*9/10 || d > want+time.Second {
		t.Fatalf("%s took %v, want about %v", what, d, want)
	}
}

func TestConnOptionsCloseTimeout(t *testing.T) {
	if DefaultCloseTimeout != 5*time.Second {
		t.Fatalf("DefaultCloseTimeout = %v, want 5s", DefaultCloseTimeout)
	}
	for _, tc := range []struct {
		opts *ConnOptions
		want time.Duration
	}{
		{nil, DefaultCloseTimeout},
		{&ConnOptions{}, DefaultCloseTimeout},
		{&ConnOptions{CloseTimeout: -time.Second}, DefaultCloseTimeout},
		{&ConnOptions{CloseTimeout: 250 * time.Millisecond}, 250 * time.Millisecond},
	} {
		if got := tc.opts.config().closeTimeout; got != tc.want {
			t.Errorf("%+v: close timeout %v, want %v", tc.opts, got, tc.want)
		}
	}
}

// Close waits for a silent peer only as long as ConnOptions.CloseTimeout.
func TestCloseHonoursCloseTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	roles(t, func(t *testing.T, server bool) {
		c, peer := startConn(t, server, closeTimeoutConfig(timeout))
		start := time.Now()
		done := closeAsync(c, StatusNormalClosure, "")
		peer.expectClose(StatusNormalClosure)
		err := waitErr(t, done)
		assertTookAbout(t, "Close", time.Since(start), timeout)
		if err == nil {
			t.Fatal("Close = nil, want a timeout error")
		}
	})
}

// The closing handshake the read side completes is bounded by
// ConnOptions.CloseTimeout too: on the client, the wait for the server to
// close TCP after the peer's close frame; on the server, the wait for the
// peer to close TCP after a protocol violation.
func TestReadSideHonoursCloseTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	t.Run("client after peer close", func(t *testing.T) {
		c, peer := startConn(t, false, closeTimeoutConfig(timeout))
		start := time.Now()
		peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
		peer.expectClose(StatusNormalClosure)
		waitDone(t, c) // the server never closes TCP
		assertTookAbout(t, "the client's wait for TCP close", time.Since(start), timeout)
	})
	t.Run("server after violation", func(t *testing.T) {
		c, peer := startConn(t, true, closeTimeoutConfig(timeout))
		peer.sendRaw(reservedOpcodeFrame(false))
		_, _, err := readMsg(t, c)
		start := time.Now()
		assertProtocolError(t, err, StatusProtocolError)
		peer.expectClose(StatusProtocolError)
		waitDone(t, c) // the peer never closes TCP
		assertTookAbout(t, "the server's drain", time.Since(start), timeout)
	})
}

// CloseNow closes the network connection at once, without a close frame,
// and a pending Read returns ErrClosed.
func TestCloseNow(t *testing.T) {
	roles(t, func(t *testing.T, server bool) {
		c, peer := startConn(t, server, testConfig())
		readErr := make(chan error, 1)
		go func() {
			_, _, err := c.Read(context.Background())
			readErr <- err
		}()
		start := time.Now()
		if err := c.CloseNow(); err != nil {
			t.Fatalf("CloseNow = %v", err)
		}
		if d := time.Since(start); d > time.Second {
			t.Fatalf("CloseNow took %v", d)
		}
		peer.expectEOF() // and no close frame before it
		if err := waitErr(t, readErr); !errors.Is(err, ErrClosed) {
			t.Fatalf("pending Read = %v, want ErrClosed", err)
		}
		if _, _, err := readMsg(t, c); !errors.Is(err, ErrClosed) {
			t.Fatalf("Read after CloseNow = %v, want ErrClosed", err)
		}
		if err := c.Write(t.Context(), MessageBinary, []byte("late")); !errors.Is(err, ErrClosed) {
			t.Fatalf("Write after CloseNow = %v, want ErrClosed", err)
		}
		if err := c.CloseNow(); !errors.Is(err, ErrClosed) {
			t.Fatalf("second CloseNow = %v, want ErrClosed", err)
		}
		if err := c.Close(StatusNormalClosure, ""); !errors.Is(err, ErrClosed) {
			t.Fatalf("Close after CloseNow = %v, want ErrClosed", err)
		}
	})
}

// writeSignalConn reports when a network write has started.
type writeSignalConn struct {
	net.Conn
	once    sync.Once
	writing chan struct{}
}

func (w *writeSignalConn) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.writing) })
	return w.Conn.Write(p)
}

// A Write blocked in a network write returns ErrClosed after CloseNow.
func TestCloseNowEndsBlockedWrite(t *testing.T) {
	s, cl := frozenPair(t)
	_ = cl // never read
	ws := &writeSignalConn{Conn: s, writing: make(chan struct{})}
	c := newConn(ws, true, "", testConfig())
	t.Cleanup(func() {
		c.closeNow(ErrClosed)
		c.wait()
	})
	written := make(chan error, 1)
	go func() { written <- c.Write(t.Context(), MessageBinary, make([]byte, 16<<20)) }()
	select {
	case <-ws.writing:
	case <-time.After(testTimeout):
		t.Fatal("the write did not start")
	}
	if err := c.CloseNow(); err != nil {
		t.Fatalf("CloseNow = %v", err)
	}
	if err := waitErr(t, written); !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked Write = %v, want ErrClosed", err)
	}
}

// Messages read ahead and not yet read are dropped by CloseNow.
func TestCloseNowDropsQueuedMessages(t *testing.T) {
	c, peer, cc := startCounted(t, testConfig())
	wire := clientFrames(t, []int{10, 20})
	peer.sendRaw(wire)
	waitRead(t, cc, len(wire))
	waitQueuedMessages(t, c, 2)
	if err := c.CloseNow(); err != nil {
		t.Fatalf("CloseNow = %v", err)
	}
	if _, p, err := readMsg(t, c); !errors.Is(err, ErrClosed) {
		t.Fatalf("Read after CloseNow = %d bytes, %v, want ErrClosed", len(p), err)
	}
}

// CloseNow during Close ends the wait for a silent peer at once.
func TestCloseNowDuringClose(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	start := time.Now()
	closed := closeAsync(c, StatusNormalClosure, "")
	peer.expectClose(StatusNormalClosure)
	if err := c.CloseNow(); err != nil {
		t.Fatalf("CloseNow = %v", err)
	}
	if err := waitErr(t, closed); err == nil {
		t.Fatal("Close = nil, want an error: the peer never answered")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close returned after %v", d)
	}
}

// CloseNow after a completed Close, and from many goroutines at once, is
// safe; exactly one call closes the connection.
func TestCloseNowRepeated(t *testing.T) {
	t.Run("after Close", func(t *testing.T) {
		c, peer := startConn(t, true, testConfig())
		closed := closeAsync(c, StatusNormalClosure, "")
		peer.expectClose(StatusNormalClosure)
		peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
		if err := waitErr(t, closed); err != nil {
			t.Fatalf("Close = %v", err)
		}
		if err := c.CloseNow(); !errors.Is(err, ErrClosed) {
			t.Fatalf("CloseNow after Close = %v, want ErrClosed", err)
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		const callers = 16
		c, _ := startConn(t, true, testConfig())
		var wg sync.WaitGroup
		var closers, others atomic.Int32
		for range callers {
			wg.Go(func() {
				switch err := c.CloseNow(); {
				case err == nil:
					closers.Add(1)
				case errors.Is(err, ErrClosed):
					others.Add(1)
				default:
					t.Errorf("CloseNow = %v", err)
				}
			})
		}
		wg.Wait()
		if closers.Load() != 1 || others.Load() != callers-1 {
			t.Fatalf("%d calls closed and %d returned ErrClosed, want 1 and %d", closers.Load(), others.Load(), callers-1)
		}
	})
}

// floodFrame encodes one 1 MiB binary message as the peer of a connection
// on the given side sends it, compressed if compress is set.
func floodFrame(t *testing.T, server, compress bool) []byte {
	t.Helper()
	payload := make([]byte, 1<<20)
	r := rand.NewChaCha8([32]byte{})
	_, _ = r.Read(payload)
	var rsv byte
	if compress {
		var z bytes.Buffer
		fw, err := flate.NewWriter(&z, flate.BestSpeed)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fw.Write(payload)
		if err := fw.Flush(); err != nil {
			t.Fatal(err)
		}
		payload, rsv = bytes.TrimSuffix(z.Bytes(), syncFlushTail), rsv1Bit
	}
	var wire bytes.Buffer
	if err := (&frameWriter{w: &wire, server: !server, rsv: rsv}).writeFrame(true, rsv, opBinary, payload); err != nil {
		t.Fatal(err)
	}
	return wire.Bytes()
}

// While Close waits for the peer's close frame, data the peer floods in
// is skipped without allocating: at most 1 MB is allocated in total while
// a peer floods 1 MiB messages throughout the close timeout.
func TestCloseFloodAllocations(t *testing.T) {
	const timeout = time.Second
	const maxAlloc = 1_000_000
	for _, tc := range []struct {
		name             string
		server, compress bool
	}{
		{"server", true, false},
		{"client", false, false},
		{"server compressed", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := readAheadConfig(DefaultReadBuffer)
			cfg.closeTimeout = timeout
			if tc.compress {
				cfg.deflate = &deflateConfig{threshold: 1, level: flate.BestSpeed, writeWindowBits: maxWindowBits}
			}
			c, peer := startConn(t, tc.server, cfg)
			frame := floodFrame(t, tc.server, tc.compress)

			var flooded atomic.Int64
			flood := make(chan struct{})
			go func() {
				defer close(flood)
				for {
					n, err := peer.nc.Write(frame)
					flooded.Add(int64(n))
					if err != nil {
						return
					}
				}
			}()
			// The read-ahead budget is full: the flood is under way, and
			// what was read ahead is allocated before the measurement.
			waitFor(t, "the connection to stop reading", func() bool {
				c.rmu.Lock()
				defer c.rmu.Unlock()
				return c.rwait == waitRoom
			})

			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			sent := flooded.Load()
			err := c.Close(StatusNormalClosure, "")
			runtime.ReadMemStats(&after)
			sent = flooded.Load() - sent
			<-flood

			if err == nil {
				t.Fatal("Close = nil, want a timeout error: the peer never answered")
			}
			alloc := after.TotalAlloc - before.TotalAlloc
			t.Logf("Close allocated %d bytes while the peer flooded %d bytes", alloc, sent)
			if sent < 4<<20 {
				t.Fatalf("the peer flooded only %d bytes during Close, want at least 4 MiB", sent)
			}
			if alloc > maxAlloc {
				t.Fatalf("Close allocated %d bytes during the flood, want at most %d", alloc, maxAlloc)
			}
		})
	}
}

// While Close waits, data frames of every kind are skipped, control
// frames between them are still read, and the peer's close frame
// completes the handshake.
func TestCloseSkipsDataUntilPeerClose(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	peer.send(false, opText, []byte("started before Close, "))
	done := closeAsync(c, StatusNormalClosure, "")
	peer.expectClose(StatusNormalClosure)
	peer.send(false, opContinuation, []byte("finished after it"))
	peer.send(true, opPing, []byte("ping"))
	peer.send(true, opContinuation, nil)
	peer.send(true, opBinary, make([]byte, 64<<10))
	peer.send(false, opText, []byte{0xff}) // not UTF-8, but skipped
	peer.send(true, opContinuation, []byte{0xfe})
	peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
	peer.expectEOF()
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Close = %v, want the handshake completed", err)
	}
}
