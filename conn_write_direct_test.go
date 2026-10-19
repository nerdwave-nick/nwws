package nwws

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// A Write on an idle connection writes its frame itself, without the
// allocations of a queued write. It allocates nothing in a normal build;
// the race detector adds one on the client side.
func TestWriteIdleAllocs(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			go func() { _, _ = io.Copy(io.Discard, peer.nc) }()
			msg := make([]byte, 300)
			ctx := context.Background()
			if n := testing.AllocsPerRun(200, func() {
				if err := c.Write(ctx, MessageBinary, msg); err != nil {
					t.Fatal(err)
				}
			}); n > 1 {
				t.Errorf("%v allocations per Write, want at most 1", n)
			}
		})
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
func TestWriteConcurrentWithClose(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			const writers = 8
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := range writers {
				wg.Go(func() {
					<-start
					msg := make([]byte, 10+w*500)
					for {
						err := c.Write(t.Context(), MessageBinary, msg)
						if errors.Is(err, ErrClosed) {
							return
						}
						if err != nil {
							t.Errorf("Write: %v", err)
							return
						}
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
					if !h.fin || (len(b)-10)%500 != 0 {
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
			wg.Wait() // every writer has seen ErrClosed
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
