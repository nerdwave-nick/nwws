package nwws

import (
	"bytes"
	"errors"
	"runtime"
	"testing"
)

// frame is one frame a raw peer sends.
type frame struct {
	fin bool
	op  opcode
	p   string
}

func sendAll(p *rawPeer, frames []frame) {
	p.t.Helper()
	for _, f := range frames {
		p.send(f.fin, f.op, []byte(f.p))
	}
}

// RFC 6455 §5.4: fragments are reassembled, and control frames may be
// injected between them (§5.5).
func TestReadFragmented(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			sendAll(peer, []frame{
				{false, opText, "Hel"},
				{true, opPing, "p1"},
				{false, opContinuation, "lo, w\xc3"}, // "ö" split across fragments
				{true, opPong, "unsolicited"},
				{true, opContinuation, "\xb6rld"},
				{false, opBinary, "\x00\xff"},
				{true, opContinuation, ""},
				{true, opBinary, "single"},
			})
			mustRead(t, c, MessageText, []byte("Hello, wörld"))
			mustRead(t, c, MessageBinary, []byte{0, 0xff})
			mustRead(t, c, MessageBinary, []byte("single"))
			peer.expectFrame(opPong, []byte("p1"))
		})
	}
}

// RFC 6455 §5.5.2–§5.5.3: a ping is answered by a pong with its payload.
func TestPingAnsweredWithPong(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	peer.send(true, opPing, []byte("abc"))
	peer.expectFrame(opPong, []byte("abc"))
	peer.send(true, opPing, nil)
	peer.expectFrame(opPong, nil)
	long := bytes.Repeat([]byte{7}, maxControlPayload)
	peer.send(true, opPing, long)
	peer.expectFrame(opPong, long)
	// Data still flows normally afterwards.
	peer.send(true, opBinary, []byte("x"))
	mustRead(t, c, MessageBinary, []byte("x"))
}

// failsWith checks that Read fails and the peer receives a close frame with
// code, after which the server closes TCP once the peer answers.
func failsWith(t *testing.T, c *Conn, peer *rawPeer, code StatusCode) {
	t.Helper()
	_, _, err := readMsg(t, c)
	assertProtocolError(t, err, code)
	peer.expectClose(code)
	peer.send(true, opClose, closePayload(code, ""))
	peer.expectEOF()
}

// assertProtocolError checks that err reports a violation by the peer for
// which the connection closes with code.
func assertProtocolError(t *testing.T, err error, code StatusCode) {
	t.Helper()
	var pe *protocolError
	if !errors.As(err, &pe) || pe.code != code {
		t.Fatalf("error %v, want a protocol error closing with %d", err, code)
	}
}

// RFC 6455 §5.4: a continuation must follow an unfinished message, and a
// new data message must not start inside one.
func TestReadFragmentationErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		frames []frame
	}{
		{"continuation without a message", []frame{{true, opContinuation, "x"}}},
		{"binary inside text", []frame{{false, opText, "a"}, {true, opBinary, "b"}}},
		{"text inside text", []frame{{false, opText, "a"}, {true, opText, "b"}}},
		{"text inside binary after a ping", []frame{{false, opBinary, "a"}, {true, opPing, ""}, {false, opText, "b"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := startConn(t, true, testConfig())
			sendAll(peer, tc.frames)
			failsWith(t, c, peer, StatusProtocolError)
		})
	}
}

// Codec errors close the connection with 1002 (RFC 6455 §5.2, §5.5, §7.4.1).
func TestReadFrameErrorsClose1002(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"RSV1 without extension", []byte{0x80 | rsv1Bit | 0x2, 0x80, 0, 0, 0, 0}},
		{"reserved opcode", []byte{0x83, 0x80, 0, 0, 0, 0}},
		{"fragmented ping", []byte{0x09, 0x80, 0, 0, 0, 0}},
		{"long ping", append([]byte{0x89, 0x80 | 126, 0, 126, 0, 0, 0, 0}, make([]byte, 126)...)},
		{"unmasked frame from a client", []byte{0x82, 0x01, 'x'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := startConn(t, true, testConfig())
			peer.sendRaw(tc.raw)
			failsWith(t, c, peer, StatusProtocolError)
		})
	}
}

// Invalid UTF-8 in a text message closes the connection with 1007
// (RFC 6455 §8.1).
func TestReadInvalidUTF8(t *testing.T) {
	for _, tc := range []struct {
		name   string
		frames []frame
	}{
		{"single frame", []frame{{true, opText, "ok\xffno"}}},
		{"split rune made invalid", []frame{{false, opText, "a\xc3"}, {true, opContinuation, "("}}},
		{"truncated at the end", []frame{{false, opText, "a"}, {true, opContinuation, "\xe2\x82"}}},
		{"surrogate", []frame{{true, opText, "\xed\xa0\x80"}}},
		{"invalid prefix in an unfinished message", []frame{{false, opText, "\xf4\x90"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := startConn(t, true, testConfig())
			sendAll(peer, tc.frames)
			failsWith(t, c, peer, StatusInvalidFramePayloadData)
		})
	}

	t.Run("binary is not validated", func(t *testing.T) {
		c, peer := startConn(t, true, testConfig())
		peer.send(true, opBinary, []byte{0xff, 0xfe})
		mustRead(t, c, MessageBinary, []byte{0xff, 0xfe})
	})
}

// A message larger than the read limit closes the connection with 1009
// (RFC 6455 §7.4.1).
func TestReadLimit(t *testing.T) {
	cfg := testConfig()
	cfg.readLimit = 1024
	big := func(n int) string { return string(bytes.Repeat([]byte("a"), n)) }

	for _, tc := range []struct {
		name   string
		frames []frame
	}{
		{"one frame", []frame{{true, opBinary, big(1025)}}},
		{"fragments", []frame{{false, opBinary, big(600)}, {true, opContinuation, big(600)}}},
		{"small final fragment", []frame{{false, opText, big(1000)}, {true, opContinuation, big(25)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := startConn(t, true, cfg)
			sendAll(peer, tc.frames)
			failsWith(t, c, peer, StatusMessageTooBig)
		})
	}

	t.Run("at the limit with a ping inside", func(t *testing.T) {
		c, peer := startConn(t, true, cfg)
		sendAll(peer, []frame{
			{false, opBinary, big(1000)},
			{true, opPing, big(125)},
			{true, opContinuation, big(24)},
		})
		mustRead(t, c, MessageBinary, []byte(big(1024)))
		peer.expectFrame(opPong, []byte(big(125)))
	})
}

// The read limit bounds the memory of a whole message: a fragment that
// would exceed it is refused before its payload is allocated.
func TestReadLimitBoundsMessageMemory(t *testing.T) {
	cfg := testConfig()
	cfg.readLimit = 1 << 20
	c, peer := startConn(t, true, cfg)
	first := bytes.Repeat([]byte("a"), 900<<10)
	// The second fragment's header announces 900 KiB; its payload never
	// needs to arrive.
	header := []byte{0x00, 0x80 | 127, 0, 0, 0, 0, 0, 0x0e, 0x10, 0x00, 0, 0, 0, 0}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	peer.send(false, opBinary, first)
	peer.sendRaw(header)
	_, _, err := readMsg(t, c)
	runtime.ReadMemStats(&after)
	assertProtocolError(t, err, StatusMessageTooBig)
	peer.expectClose(StatusMessageTooBig)
	if got := after.TotalAlloc - before.TotalAlloc; got > 1500<<10 {
		t.Errorf("allocated %d bytes for a refused 1.8 MiB message with a 1 MiB limit", got)
	}
}

// A connection lost without a close frame reports 1006 (RFC 6455 §7.1.5).
func TestReadUnexpectedLoss(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"between frames", nil},
		{"inside a frame", []byte{0x82, 0x85, 0, 0, 0, 0, 'a'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := startConn(t, true, testConfig())
			if tc.raw != nil {
				peer.sendRaw(tc.raw)
			}
			_ = peer.nc.Close()
			_, _, err := readMsg(t, c)
			var ce *CloseError
			if !errors.As(err, &ce) || ce.Code != StatusAbnormalClosure {
				t.Fatalf("Read = %v, want CloseError 1006", err)
			}
			// The error stays.
			if _, _, err2 := readMsg(t, c); !errors.As(err2, &ce) || ce.Code != StatusAbnormalClosure {
				t.Fatalf("second Read = %v, want CloseError 1006", err2)
			}
		})
	}
}
