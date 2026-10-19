package nwws

import (
	"context"
	"testing"
)

// holdsWriteBuffer reports whether c holds a pooled write buffer.
func holdsWriteBuffer(c *Conn) bool {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.wbuf != nil || c.bw != nil || c.fw.buf != nil
}

func waitNoWriteBuffer(t *testing.T, c *Conn) {
	t.Helper()
	waitFor(t, "the write buffer to go back to the pool", func() bool { return !holdsWriteBuffer(c) })
}

// An idle connection holds no write buffer: it takes one from the pool for
// writing and returns it once nothing is left to write, on every path.
func TestIdleHoldsNoWriteBuffer(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			if holdsWriteBuffer(c) {
				t.Fatal("a new connection holds a write buffer")
			}
			mustWrite(t, c, MessageBinary, []byte("direct"))
			if holdsWriteBuffer(c) {
				t.Fatal("a direct Write kept the write buffer")
			}
			peer.expectFrame(opBinary, []byte("direct"))

			peer.send(true, opPing, []byte("p"))
			peer.expectFrame(opPong, []byte("p")) // written by the write goroutine
			waitNoWriteBuffer(t, c)

			closed := make(chan error, 1)
			go func() { closed <- c.Close(StatusNormalClosure, "") }()
			peer.expectClose(StatusNormalClosure)
			peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
			_ = peer.nc.Close()
			if err := waitErr(t, closed); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if holdsWriteBuffer(c) {
				t.Fatal("the closed connection holds a write buffer")
			}
		})
	}

	t.Run("queued", func(t *testing.T) {
		c, peer, g := startGated(t)
		first := make(chan error, 1)
		go func() { first <- c.Write(t.Context(), MessageBinary, []byte("first")) }()
		waitEntered(t, g)
		second := make(chan error, 1)
		go func() { second <- c.Write(t.Context(), MessageBinary, []byte("second")) }()
		waitQueued(t, c, 1)
		close(g.release)
		for _, done := range []chan error{first, second} {
			if err := waitErr(t, done); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
		if holdsWriteBuffer(c) {
			t.Fatal("the write goroutine kept the write buffer")
		}
		peer.expectFrame(opBinary, []byte("first"))
		peer.expectFrame(opBinary, []byte("second"))
	})

	t.Run("withdrawn", func(t *testing.T) {
		c, peer, g := startGated(t)
		first := make(chan error, 1)
		go func() { first <- c.Write(t.Context(), MessageBinary, []byte("first")) }()
		waitEntered(t, g)
		ctx, cancel := context.WithCancel(t.Context())
		second := make(chan error, 1)
		go func() { second <- c.Write(ctx, MessageBinary, []byte("second")) }()
		waitQueued(t, c, 1)
		cancel()
		if err := waitErr(t, second); err == nil {
			t.Fatal("the withdrawn Write succeeded")
		}
		close(g.release)
		if err := waitErr(t, first); err != nil {
			t.Fatalf("Write: %v", err)
		}
		peer.expectFrame(opBinary, []byte("first"))
		waitNoWriteBuffer(t, c)
	})

	t.Run("failed write", func(t *testing.T) {
		c, peer := startConn(t, true, testConfig())
		_ = peer.nc.Close()
		for range 100 { // until the lost peer makes a write fail
			if c.Write(t.Context(), MessageBinary, make([]byte, 64<<10)) != nil {
				break
			}
		}
		waitDone(t, c)
		if holdsWriteBuffer(c) {
			t.Fatal("the failed connection holds a write buffer")
		}
	})
}

// A write buffer goes back to the pool reset, so the next connection's
// frames carry nothing of the previous one's.
func TestWriteBufferReuse(t *testing.T) {
	for range 3 {
		c, peer := startConn(t, false, testConfig())
		mustWrite(t, c, MessageText, []byte("hello"))
		peer.expectFrame(opText, []byte("hello"))
		mustWrite(t, c, MessageBinary, make([]byte, writeBufferSize+1))
		peer.expectFrame(opBinary, make([]byte, writeBufferSize+1))
		c.closeNow(ErrClosed)
		c.wait()
	}
}
