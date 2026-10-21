package nwws

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// gateConn counts writes and holds the write numbered hold, the first
// unless set otherwise, until released.
type gateConn struct {
	net.Conn
	hold    int
	entered chan struct{} // receives when the held write is reached
	release chan struct{}
	closed  chan struct{}

	mu     sync.Mutex
	writes int
	once   sync.Once
}

func newGateConn(c net.Conn) *gateConn {
	return &gateConn{
		Conn:    c,
		hold:    1,
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (g *gateConn) Write(b []byte) (int, error) {
	g.mu.Lock()
	g.writes++
	held := g.writes == g.hold
	g.mu.Unlock()
	if held {
		g.entered <- struct{}{}
		select {
		case <-g.release:
		case <-g.closed:
			return 0, net.ErrClosed
		}
	}
	return g.Conn.Write(b)
}

func (g *gateConn) Close() error {
	g.once.Do(func() { close(g.closed) })
	return g.Conn.Close()
}

func (g *gateConn) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.writes
}

// startGated returns a server Conn whose first network write is held.
func startGated(t *testing.T) (*Conn, *rawPeer, *gateConn) {
	t.Helper()
	s, cl := tcpPair(t)
	g := newGateConn(s)
	c := newConn(g, true, "", testConfig())
	t.Cleanup(func() {
		c.closeNow(ErrClosed)
		c.wait()
	})
	return c, newRawPeer(t, cl, false), g
}

func waitEntered(t *testing.T, g *gateConn) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(testTimeout):
		t.Fatal("the writer never wrote")
	}
}

// waitQueued polls until n requests wait in the write queue.
func waitQueued(t *testing.T, c *Conn, n int) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		c.wmu.Lock()
		got := len(c.queue)
		c.wmu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d requests queued, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// Messages queued while a flush is in progress share the next flush.
func TestWriteCoalesces(t *testing.T) {
	c, peer, g := startGated(t)
	const n = 50
	var wg sync.WaitGroup
	wg.Go(func() { mustWrite(t, c, MessageBinary, []byte("first")) })
	waitEntered(t, g)
	for i := range n {
		wg.Go(func() { mustWrite(t, c, MessageBinary, fmt.Appendf(nil, "msg %02d", i)) })
	}
	waitQueued(t, c, n)
	close(g.release)
	wg.Wait()

	if got := g.count(); got != 2 {
		t.Fatalf("%d network writes for %d messages, want 2", got, n+1)
	}
	peer.expectFrame(opBinary, []byte("first"))
	for range n {
		if h, b := peer.recv(); h.opcode != opBinary || len(b) != 6 {
			t.Fatalf("frame opcode %d payload %q", h.opcode, b)
		}
	}
}

// Many goroutines may write at once; every message arrives whole and in
// each goroutine's order.
func TestWriteConcurrent(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			const writers, each = 32, 50
			var wg sync.WaitGroup
			for w := range writers {
				wg.Go(func() {
					for i := range each {
						msg := make([]byte, 8+w*37) // varied sizes, some above 125 bytes
						binary.BigEndian.PutUint32(msg, uint32(w))
						binary.BigEndian.PutUint32(msg[4:], uint32(i))
						if err := c.Write(context.Background(), MessageBinary, msg); err != nil {
							t.Errorf("Write: %v", err)
							return
						}
					}
				})
			}
			next := make([]uint32, writers)
			for range writers * each {
				h, b := peer.recv()
				if h.opcode != opBinary || !h.fin || len(b) < 8 {
					t.Fatalf("frame opcode %d fin %v len %d", h.opcode, h.fin, len(b))
				}
				w, i := binary.BigEndian.Uint32(b), binary.BigEndian.Uint32(b[4:])
				if int(w) >= writers || len(b) != 8+int(w)*37 || i != next[w] {
					t.Fatalf("writer %d message %d (len %d), want message %d", w, i, len(b), next[w])
				}
				next[w]++
			}
			wg.Wait()
		})
	}
}

// A Write cancelled while queued is withdrawn; the connection stays usable.
func TestWriteCancelledWhileQueued(t *testing.T) {
	c, peer, g := startGated(t)
	var wg sync.WaitGroup
	wg.Go(func() { mustWrite(t, c, MessageBinary, []byte("first")) })
	waitEntered(t, g)

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() { errc <- c.Write(ctx, MessageBinary, []byte("withdrawn")) }()
	waitQueued(t, c, 1)
	cancel()
	if err := waitErr(t, errc); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write = %v, want Canceled", err)
	}
	close(g.release)
	wg.Wait()
	mustWrite(t, c, MessageBinary, []byte("third"))
	peer.expectFrame(opBinary, []byte("first"))
	peer.expectFrame(opBinary, []byte("third"))
}

// A Write cancelled while its frame is on the wire cannot be withdrawn, so
// the connection is closed.
func TestWriteCancelledInFlight(t *testing.T) {
	c, _, g := startGated(t)
	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() { errc <- c.Write(ctx, MessageBinary, []byte("stuck")) }()
	waitEntered(t, g)
	cancel()
	if err := waitErr(t, errc); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write = %v, want Canceled", err)
	}
	select {
	case <-g.closed:
	case <-time.After(testTimeout):
		t.Fatal("connection not closed")
	}
	if _, _, err := readMsg(t, c); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read on an aborted connection = %v, want the abandoned write's Canceled", err)
	}
	if err := c.Write(t.Context(), MessageBinary, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write on an aborted connection = %v, want ErrClosed", err)
	}
}

func TestWriteValidates(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	if err := c.Write(t.Context(), MessageText, []byte("bad \xff")); err == nil {
		t.Error("Write of invalid UTF-8 text succeeded")
	}
	if err := c.Write(t.Context(), MessageType(9), nil); err == nil {
		t.Error("Write with an invalid message type succeeded")
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.Write(cancelled, MessageBinary, []byte("never")); !errors.Is(err, context.Canceled) {
		t.Errorf("Write with a cancelled context = %v", err)
	}
	mustWrite(t, c, MessageText, []byte("good"))
	peer.expectFrame(opText, []byte("good"))
}
