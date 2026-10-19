package nwws

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// closeAsync runs Close in the background.
func closeAsync(c *Conn, code StatusCode, reason string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.Close(code, reason) }()
	return done
}

func waitErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(testTimeout):
		t.Fatal("timed out")
		return nil
	}
}

func assertCloseError(t *testing.T, err error, code StatusCode, reason string) {
	t.Helper()
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != code || ce.Reason != reason {
		t.Fatalf("error %v, want CloseError %d %q", err, code, reason)
	}
}

// RFC 6455 §7.1.2 and §5.5.1: Close sends a close frame, waits for the
// peer's, and the server then closes TCP.
func TestCloseHandshakeServer(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	done := closeAsync(c, 4000, "done")
	if reason := peer.expectClose(4000); reason != "done" {
		t.Fatalf("close reason %q", reason)
	}
	peer.send(true, opClose, closePayload(4000, "done"))
	peer.expectEOF()
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := c.Write(t.Context(), MessageBinary, []byte("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write after Close = %v, want ErrClosed", err)
	}
	_, _, err := readMsg(t, c)
	assertCloseError(t, err, 4000, "done")
	if err := c.Close(StatusNormalClosure, ""); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close = %v, want ErrClosed", err)
	}
}

// RFC 6455 §7.1.1: the client waits for the server to close TCP.
func TestCloseHandshakeClient(t *testing.T) {
	c, peer := startConn(t, false, testConfig())
	done := closeAsync(c, StatusGoingAway, "")
	peer.expectClose(StatusGoingAway)
	peer.send(true, opClose, closePayload(StatusGoingAway, ""))
	select {
	case err := <-done:
		t.Fatalf("Close returned %v before the server closed TCP", err)
	case <-time.After(50 * time.Millisecond):
	}
	_ = peer.nc.Close()
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

// Messages queued before Close are sent before the close frame.
func TestCloseAfterQueuedMessages(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	mustWrite(t, c, MessageText, []byte("last words"))
	done := closeAsync(c, StatusNormalClosure, "")
	peer.expectFrame(opText, []byte("last words"))
	peer.expectClose(StatusNormalClosure)
	peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

// Close waits only a bounded time for the peer's close frame.
func TestCloseTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.closeTimeout = 100 * time.Millisecond
	c, peer := startConn(t, true, cfg)
	start := time.Now()
	done := closeAsync(c, StatusNormalClosure, "")
	peer.expectClose(StatusNormalClosure)
	peer.expectEOF() // never answered: the server gives up and closes TCP
	if err := waitErr(t, done); err == nil {
		t.Fatal("Close = nil, want a timeout error")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close took %v", d)
	}
}

// Data the peer sends after Close is discarded while waiting for its close
// frame, even when nobody reads.
func TestCloseDiscardsUnreadMessages(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	peer.send(true, opBinary, []byte("unread 1"))
	peer.send(true, opBinary, []byte("unread 2"))
	done := closeAsync(c, StatusNormalClosure, "")
	peer.expectClose(StatusNormalClosure)
	peer.send(true, opBinary, []byte("after close"))
	peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
	peer.expectEOF()
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

// RFC 6455 §5.5.1: a peer's close frame is answered and reported to Read.
func TestPeerClose(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			peer.send(true, opClose, closePayload(4001, "bye"))
			_, _, err := readMsg(t, c)
			assertCloseError(t, err, 4001, "bye")
			peer.expectClose(4001)
			if server {
				peer.expectEOF()
			} else {
				_ = peer.nc.Close() // the server closes TCP first
			}
			waitDone(t, c)
			if err := c.Write(t.Context(), MessageBinary, nil); !errors.Is(err, ErrClosed) {
				t.Fatalf("Write after peer close = %v, want ErrClosed", err)
			}
		})
	}
}

func waitDone(t *testing.T, c *Conn) {
	t.Helper()
	done := make(chan struct{})
	go func() { c.wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("connection goroutines still running")
	}
}

// RFC 6455 §7.4 and the IANA registry decide which codes are valid on the
// wire; the Autobahn suite (case 7.9) expects 1002 for the others.
func TestPeerCloseCodes(t *testing.T) {
	valid := []StatusCode{1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 3000, 3999, 4000, 4999}
	for _, code := range valid {
		c, peer := startConn(t, true, testConfig())
		peer.send(true, opClose, closePayload(code, "r"))
		_, _, err := readMsg(t, c)
		assertCloseError(t, err, code, "r")
		peer.expectClose(code)
	}
	invalid := []StatusCode{0, 999, 1004, 1005, 1006, 1015, 1016, 1100, 2000, 2999, 5000, 65535}
	for _, code := range invalid {
		c, peer := startConn(t, true, testConfig())
		peer.send(true, opClose, closePayload(code, ""))
		_, _, err := readMsg(t, c)
		assertProtocolError(t, err, StatusProtocolError)
		peer.expectClose(StatusProtocolError)
	}
}

func TestPeerClosePayloads(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		c, peer := startConn(t, true, testConfig())
		peer.send(true, opClose, nil)
		_, _, err := readMsg(t, c)
		assertCloseError(t, err, StatusNoStatusRcvd, "")
		peer.expectFrame(opClose, nil)
		peer.expectEOF()
	})
	t.Run("one byte", func(t *testing.T) {
		c, peer := startConn(t, true, testConfig())
		peer.send(true, opClose, []byte{0x03})
		_, _, err := readMsg(t, c)
		assertProtocolError(t, err, StatusProtocolError)
		peer.expectClose(StatusProtocolError)
	})
	t.Run("invalid UTF-8 reason", func(t *testing.T) {
		c, peer := startConn(t, true, testConfig())
		peer.send(true, opClose, append(closePayload(1000, ""), 0xff))
		_, _, err := readMsg(t, c)
		assertProtocolError(t, err, StatusInvalidFramePayloadData)
		peer.expectClose(StatusInvalidFramePayloadData)
	})
}

// Close refuses codes that must not be sent and reasons that do not fit.
func TestCloseValidatesArguments(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	for _, tc := range []struct {
		code   StatusCode
		reason string
	}{
		{999, ""}, {1004, ""}, {StatusNoStatusRcvd, ""}, {StatusAbnormalClosure, ""},
		{StatusTLSHandshake, ""}, {2000, ""}, {5000, ""},
		{StatusNormalClosure, strings.Repeat("x", 124)},
		{StatusNormalClosure, "\xff"},
	} {
		if err := c.Close(tc.code, tc.reason); err == nil || errors.Is(err, ErrClosed) {
			t.Errorf("Close(%d, %d-byte reason) = %v, want a validation error", tc.code, len(tc.reason), err)
		}
	}
	// The connection is still open, and the longest reason fits.
	mustWrite(t, c, MessageBinary, []byte("open"))
	peer.expectFrame(opBinary, []byte("open"))
	done := closeAsync(c, 4999, strings.Repeat("x", 123))
	peer.expectClose(4999)
	peer.send(true, opClose, closePayload(4999, ""))
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestStatusCodeString(t *testing.T) {
	for code, want := range map[StatusCode]string{
		StatusNormalClosure: "1000 (normal closure)",
		StatusMessageTooBig: "1009 (message too big)",
		4321:                "4321",
	} {
		if got := code.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int(code), got, want)
		}
	}
	err := &CloseError{Code: StatusGoingAway, Reason: "restart"}
	if got := err.Error(); !strings.Contains(got, "1001") || !strings.Contains(got, "restart") {
		t.Errorf("CloseError.Error() = %q", got)
	}
}

// A Read blocked when the connection closes returns.
func TestCloseUnblocksRead(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	readErr := make(chan error, 1)
	go func() {
		_, _, err := c.Read(context.Background())
		readErr <- err
	}()
	done := closeAsync(c, StatusNormalClosure, "")
	peer.expectClose(StatusNormalClosure)
	peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
	assertCloseError(t, waitErr(t, readErr), StatusNormalClosure, "")
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Close = %v", err)
	}
}
