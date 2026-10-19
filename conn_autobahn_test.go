package nwws

import (
	"context"
	"fmt"
	"testing"
)

// Regression tests for behaviour the Autobahn test suite checks.

// Autobahn 2.10 and 2.11: every ping of a burst is answered, in order,
// even when they all arrive while the writer is busy.
func TestPingBurstAnswered(t *testing.T) {
	c, peer, g := startGated(t)
	written := make(chan error, 1)
	go func() { written <- c.Write(context.Background(), MessageBinary, []byte("first")) }()
	waitEntered(t, g)

	const pings = 10
	for i := range pings {
		peer.send(true, opPing, fmt.Appendf(nil, "payload-%d", i))
	}
	// Frames are read in order, so every ping has been handled once the
	// message after them arrives.
	peer.send(true, opBinary, []byte("after"))
	mustRead(t, c, MessageBinary, []byte("after"))

	close(g.release)
	if err := <-written; err != nil {
		t.Fatalf("Write = %v", err)
	}
	peer.expectFrame(opBinary, []byte("first"))
	for i := range pings {
		peer.expectFrame(opPong, fmt.Appendf(nil, "payload-%d", i))
	}
}

// Pongs waiting for the writer are bounded: beyond maxPendingPongs, the
// oldest unanswered pings go unanswered (RFC 6455 §5.5.3 allows answering
// only the latest).
func TestPendingPongsBounded(t *testing.T) {
	c, peer, g := startGated(t)
	written := make(chan error, 1)
	go func() { written <- c.Write(context.Background(), MessageBinary, []byte("first")) }()
	waitEntered(t, g)

	pings := maxPendingPongs + 5
	for i := range pings {
		peer.send(true, opPing, fmt.Appendf(nil, "%d", i))
	}
	peer.send(true, opBinary, []byte("after"))
	mustRead(t, c, MessageBinary, []byte("after"))

	close(g.release)
	if err := <-written; err != nil {
		t.Fatalf("Write = %v", err)
	}
	peer.expectFrame(opBinary, []byte("first"))
	for i := pings - maxPendingPongs; i < pings; i++ {
		peer.expectFrame(opPong, fmt.Appendf(nil, "%d", i))
	}
}

// Autobahn 2.5 and 6.17.4 (client side): an application that calls Close
// as soon as Read reports a violation must not replace the failure's close
// code. The failure's close frame is queued before Read learns of it.
func TestViolationCloseCodeWinsOverClose(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		code StatusCode
	}{
		// A ping with a 126-byte payload (RFC 6455 §5.5).
		{"control too long", append([]byte{0x80 | byte(opPing), 126, 0, 126}, make([]byte, 126)...), StatusProtocolError},
		// A text frame that is not UTF-8 (RFC 6455 §8.1).
		{"invalid UTF-8", []byte{0x80 | byte(opText), 5, 0xf8, 0x80, 0x80, 0x80, 0xaf}, StatusInvalidFramePayloadData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for range 200 {
				c, peer := startConn(t, false, testConfig())
				peer.sendRaw(tc.raw)
				_, _, err := readMsg(t, c)
				assertProtocolError(t, err, tc.code)
				closed := make(chan error, 1)
				go func() { closed <- c.Close(StatusNormalClosure, "") }()
				peer.expectClose(tc.code)
				_ = peer.nc.Close()
				<-closed
			}
		})
	}
}
