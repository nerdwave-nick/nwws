package nwws

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// A Write on an idle connection writes its frame itself, without the
// allocations of a queued write. It allocates nothing, also with a ctx
// that can be cancelled, as long as Writes pass the same one.
func TestWriteIdleAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops write buffers on purpose under the race detector")
	}
	cancellable, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, server := range []bool{true, false} {
		for name, ctx := range map[string]context.Context{"background": context.Background(), "cancellable": cancellable} {
			t.Run(map[bool]string{true: "server", false: "client"}[server]+"/"+name, func(t *testing.T) {
				c, peer := startConn(t, server, testConfig())
				go func() { _, _ = io.Copy(io.Discard, peer.nc) }()
				msg := make([]byte, 300)
				if n := testing.AllocsPerRun(200, func() {
					if err := c.Write(ctx, MessageBinary, msg); err != nil {
						t.Fatal(err)
					}
				}); n != 0 {
					t.Errorf("%v allocations per Write, want 0", n)
				}
			})
		}
	}
}

// startHeldAt returns a server Conn whose network write numbered hold is
// held until released.
func startHeldAt(t *testing.T, hold int) (*Conn, *rawPeer, *gateConn) {
	t.Helper()
	s, cl := tcpPair(t)
	g := newGateConn(s)
	g.hold = hold
	c := newConn(g, true, "", testConfig())
	t.Cleanup(func() {
		c.closeNow(ErrClosed)
		c.wait()
	})
	return c, newRawPeer(t, cl, false), g
}

func waitConnClosed(t *testing.T, g *gateConn) {
	t.Helper()
	select {
	case <-g.closed:
	case <-time.After(testTimeout):
		t.Fatal("connection not closed")
	}
}

// Cancelling the ctx of a direct write in flight closes the connection,
// also when earlier Writes passed the same ctx.
func TestWriteCancelledInFlightAfterWrites(t *testing.T) {
	c, peer, g := startHeldAt(t, 3)
	ctx, cancel := context.WithCancel(t.Context())
	for _, msg := range []string{"one", "two"} {
		if err := c.Write(ctx, MessageBinary, []byte(msg)); err != nil {
			t.Fatalf("Write: %v", err)
		}
		peer.expectFrame(opBinary, []byte(msg))
	}
	errc := make(chan error, 1)
	go func() { errc <- c.Write(ctx, MessageBinary, []byte("stuck")) }()
	waitEntered(t, g)
	cancel()
	if err := waitErr(t, errc); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write = %v, want Canceled", err)
	}
	waitConnClosed(t, g)
	if _, _, err := readMsg(t, c); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read on an aborted connection = %v, want the abandoned write's Canceled", err)
	}
}

// Cancelling the ctx of a Write that has returned leaves the connection
// alone, while it is idle and while a later Write with another ctx is in
// flight.
func TestWriteCancelledAfterReturn(t *testing.T) {
	c, peer, g := startHeldAt(t, 4) // "four"
	idle, cancelIdle := context.WithCancel(t.Context())
	busy, cancelBusy := context.WithCancel(t.Context())
	defer cancelBusy()

	if err := c.Write(idle, MessageBinary, []byte("one")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := c.Write(busy, MessageBinary, []byte("two")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	cancelBusy() // while idle
	if err := c.Write(idle, MessageBinary, []byte("three")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	other, cancelOther := context.WithCancel(t.Context())
	defer cancelOther()
	errc := make(chan error, 1)
	go func() { errc <- c.Write(other, MessageBinary, []byte("four")) }()
	waitEntered(t, g)
	cancelIdle() // while "four" is in flight
	close(g.release)
	if err := waitErr(t, errc); err != nil {
		t.Fatalf("Write = %v, want nil", err)
	}
	if err := c.Write(idle, MessageBinary, []byte("never")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write with a cancelled ctx = %v, want Canceled", err)
	}
	mustWrite(t, c, MessageBinary, []byte("five"))
	for _, msg := range []string{"one", "two", "three", "four", "five"} {
		peer.expectFrame(opBinary, []byte(msg))
	}
	select {
	case <-g.closed:
		t.Fatal("connection closed")
	default:
	}
}

// lateCtx is cancelled right after the first check of its Err, as a ctx
// cancelled while Write starts.
type lateCtx struct {
	context.Context
	cancel  context.CancelFunc
	checked atomic.Bool
}

func (c *lateCtx) Err() error {
	if !c.checked.Swap(true) {
		defer c.cancel()
		return nil
	}
	return c.Context.Err()
}

// A direct Write whose ctx is cancelled before its frame is written
// writes nothing, and the connection stays usable, also when earlier
// Writes passed the same ctx.
func TestWriteCancelledBeforeDirectWrite(t *testing.T) {
	for _, reused := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "reused"}[reused], func(t *testing.T) {
			c, peer := startConn(t, true, testConfig())
			inner, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := &lateCtx{Context: inner, cancel: cancel}
			if reused {
				ctx.checked.Store(true) // not cancelled by this Write
				if err := c.Write(ctx, MessageBinary, []byte("before")); err != nil {
					t.Fatalf("Write: %v", err)
				}
				peer.expectFrame(opBinary, []byte("before"))
				ctx.checked.Store(false)
			}
			if err := c.Write(ctx, MessageBinary, []byte("never")); !errors.Is(err, context.Canceled) {
				t.Fatalf("Write = %v, want Canceled", err)
			}
			mustWrite(t, c, MessageBinary, []byte("after"))
			peer.expectFrame(opBinary, []byte("after"))
		})
	}
}

// Once the connection has ended, a long-lived ctx that Writes passed no
// longer keeps it reachable.
func TestWriteCtxReleasesEndedConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collected := make(chan struct{})
	func() {
		s, cl := tcpPair(t)
		go func() { _, _ = io.Copy(io.Discard, cl) }()
		c := newConn(s, true, "", testConfig())
		runtime.AddCleanup(c, func(ch chan struct{}) { close(ch) }, collected)
		for range 3 {
			if err := c.Write(ctx, MessageBinary, []byte("msg")); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
		c.closeNow(ErrClosed)
		c.wait()
	}()
	deadline := time.Now().Add(testTimeout)
	for {
		runtime.GC()
		select {
		case <-collected:
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the ended connection is still reachable")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Pongs waiting for the writer go before data, also when the writer is
// idle and has not taken them yet.
func TestWriteAfterPendingPong(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	c.wmu.Lock()
	c.pongs = append(c.pongs, &writeReq{op: opPong, payload: []byte("pong")}) // not signalled
	c.wmu.Unlock()
	mustWrite(t, c, MessageBinary, []byte("data"))
	peer.expectFrame(opPong, []byte("pong"))
	peer.expectFrame(opBinary, []byte("data"))
}

// A data frame queued while a Write writes directly is sent once that
// write is done, and pongs queued meanwhile go before it.
func TestWriteQueuedBehindDirectWrite(t *testing.T) {
	c, peer, g := startGated(t)
	first := make(chan error, 1)
	go func() { first <- c.Write(t.Context(), MessageBinary, []byte("first")) }()
	waitEntered(t, g)

	peer.send(true, opPing, []byte("ping"))
	waitFor(t, "the pong to be queued", func() bool {
		c.wmu.Lock()
		defer c.wmu.Unlock()
		return len(c.pongs) == 1
	})
	second := make(chan error, 1)
	go func() { second <- c.Write(t.Context(), MessageBinary, []byte("second")) }()
	waitQueued(t, c, 1)
	close(g.release)
	for _, ch := range []<-chan error{first, second} {
		if err := waitErr(t, ch); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	peer.expectFrame(opBinary, []byte("first"))
	peer.expectFrame(opPong, []byte("ping"))
	peer.expectFrame(opBinary, []byte("second"))
}

// Concurrent writers, the peer's pings and Close: frames never interleave,
// the close frame goes last, and Writes after it fail with ErrClosed.
// The writers yield after each message and stop after maxPerWriter, so
// they cannot run megabytes ahead of the peer on a single P.
func TestWriteConcurrentWithClose(t *testing.T) {
	const maxPerWriter = 1000
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			const writers = 8
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := range writers {
				wg.Go(func() {
					<-start
					msg := make([]byte, 10+w*50)
					for range maxPerWriter {
						err := c.Write(t.Context(), MessageBinary, msg)
						if errors.Is(err, ErrClosed) {
							return
						}
						if err != nil {
							t.Errorf("Write: %v", err)
							return
						}
						runtime.Gosched()
					}
				})
			}
			pinged := make(chan struct{})
			go func() {
				defer close(pinged)
				<-start
				for range 20 {
					if err := peer.fw.writeFrame(true, 0, opPing, []byte("p")); err != nil {
						t.Errorf("peer ping: %v", err)
						return
					}
				}
			}()
			close(start)

			closed := make(chan error, 1)
			data, pongs := 0, 0
		frames:
			for {
				h, b := peer.recv()
				switch h.opcode {
				case opBinary:
					if !h.fin || (len(b)-10)%50 != 0 {
						t.Fatalf("data frame fin %v of %d bytes", h.fin, len(b))
					}
					data++
					if data == 2000 {
						go func() { closed <- c.Close(StatusNormalClosure, "") }()
					}
				case opPong:
					pongs++
				case opClose:
					break frames
				default:
					t.Fatalf("unexpected opcode %d", h.opcode)
				}
			}
			wg.Wait() // every writer has seen ErrClosed or written all its messages
			<-pinged
			peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
			if !server {
				_ = peer.nc.(*net.TCPConn).CloseWrite() // the server closes TCP first
			}
			_ = peer.nc.SetReadDeadline(time.Now().Add(testTimeout))
			if h, _, err := peer.fr.readFrame(); !errors.Is(err, io.EOF) {
				t.Fatalf("after the close frame: opcode %d, error %v; want EOF", h.opcode, err)
			}
			if err := waitErr(t, closed); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if pongs == 0 {
				t.Error("no pong before the close frame")
			}
		})
	}
}

// A direct write cut short by this side's CloseNow or Close returns
// ErrClosed; one that fails because the peer is gone returns
// StatusAbnormalClosure.
func TestDirectWriteFailures(t *testing.T) {
	// startHeld starts a direct write that stays in its network write.
	startHeld := func(t *testing.T, cfg connConfig) (*Conn, <-chan error) {
		s, _ := tcpPair(t)
		g := newGateConn(s)
		c := newConn(g, true, "", cfg)
		t.Cleanup(func() {
			c.closeNow(ErrClosed)
			c.wait()
		})
		written := make(chan error, 1)
		go func() { written <- c.Write(t.Context(), MessageBinary, []byte("held")) }()
		waitEntered(t, g)
		return c, written
	}

	t.Run("CloseNow", func(t *testing.T) {
		c, written := startHeld(t, testConfig())
		if err := c.CloseNow(); err != nil {
			t.Fatalf("CloseNow = %v", err)
		}
		if err := waitErr(t, written); !errors.Is(err, ErrClosed) {
			t.Fatalf("Write = %v, want ErrClosed", err)
		}
	})

	t.Run("Close", func(t *testing.T) {
		c, written := startHeld(t, closeTimeoutConfig(50*time.Millisecond))
		if err := c.Close(StatusNormalClosure, ""); err == nil {
			t.Fatal("Close = nil, want a timeout: the close frame waits behind the held write")
		}
		if err := waitErr(t, written); !errors.Is(err, ErrClosed) {
			t.Fatalf("Write = %v, want ErrClosed", err)
		}
	})

	t.Run("peer gone", func(t *testing.T) {
		s, _ := tcpPair(t)
		c := newConn(brokenConn{s}, true, "", testConfig())
		t.Cleanup(func() {
			c.closeNow(ErrClosed)
			c.wait()
		})
		err := c.Write(t.Context(), MessageBinary, []byte("lost"))
		var ce *CloseError
		if !errors.As(err, &ce) || ce.Code != StatusAbnormalClosure || !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("Write = %v, want CloseError 1006 wrapping EPIPE", err)
		}
	})
}
