package nwws

import (
	"testing"
	"time"
)

// reservedOpcodeFrame is an empty frame with the reserved opcode 0x3, a
// violation (RFC 6455 §5.2), as a peer sends it to the server side (masked)
// or to the client side (unmasked) of a connection.
func reservedOpcodeFrame(server bool) []byte {
	if server {
		return []byte{0x80 | 0x3, 0x80, 0, 0, 0, 0}
	}
	return []byte{0x80 | 0x3, 0}
}

// Autobahn 3.2–3.4, 4.1.3–4.2.5 and 5.15: a violation is reported by Read
// after the messages received before it, and its close frame goes out
// after what the application sends in reply to those messages.
func TestViolationAfterEarlierMessages(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			for range 20 {
				c, peer := startConn(t, server, testConfig())
				peer.send(true, opText, []byte("hello"))
				peer.sendRaw(reservedOpcodeFrame(server))

				mustRead(t, c, MessageText, []byte("hello"))
				mustWrite(t, c, MessageText, []byte("hello"))
				peer.expectFrame(opText, []byte("hello"))
				_, _, err := readMsg(t, c)
				assertProtocolError(t, err, StatusProtocolError)
				peer.expectClose(StatusProtocolError)
			}
		})
	}
}

// Autobahn 2.5 and 6.17.4 (client side), with messages before the
// violation: an application that calls Close as soon as Read reports the
// violation does not replace the violation's close code.
func TestCloseAfterViolationSendsItsCode(t *testing.T) {
	for range 200 {
		c, peer := startConn(t, false, testConfig())
		peer.send(true, opBinary, []byte("first"))
		peer.sendRaw([]byte{0x80 | byte(opText), 1, 0xff})

		mustRead(t, c, MessageBinary, []byte("first"))
		_, _, err := readMsg(t, c)
		assertProtocolError(t, err, StatusInvalidFramePayloadData)
		closed := make(chan error, 1)
		go func() { closed <- c.Close(StatusNormalClosure, "") }()
		peer.expectClose(StatusInvalidFramePayloadData)
		_ = peer.nc.Close()
		<-closed
	}
}

// A violation waits for Read at most the close timeout: when the
// application does not read, the connection is failed anyway.
func TestViolationUnreadFailsAfterCloseTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.closeTimeout = 300 * time.Millisecond
	c, peer := startConn(t, true, cfg)

	start := time.Now()
	peer.sendRaw(reservedOpcodeFrame(true))
	peer.expectClose(StatusProtocolError)
	if elapsed := time.Since(start); elapsed < cfg.closeTimeout {
		t.Errorf("close frame after %v, want it held for Read up to the close timeout %v", elapsed, cfg.closeTimeout)
	}
	peer.expectEOF()
	_, _, err := readMsg(t, c)
	assertProtocolError(t, err, StatusProtocolError)
}
