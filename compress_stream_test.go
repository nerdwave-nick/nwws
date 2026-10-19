package nwws

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"errors"
	"runtime"
	"strings"
	"testing"
)

// flushed compresses msg on its own and keeps the sync flush tail, so the
// output decompresses completely without the frames that follow it.
func flushed(t *testing.T, msg []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(msg); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// compressDict compresses msg as a peer with context takeover that starts
// a new DEFLATE stream per message does: preset with dict, ended by a final
// block when final, else by a sync flush without its tail.
func compressDict(t *testing.T, msg, dict []byte, final bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := flate.NewWriterDict(&buf, flate.DefaultCompression, dict)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(msg); err != nil {
		t.Fatal(err)
	}
	if final {
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSuffix(buf.Bytes(), syncTail)
}

// A compressed message is decompressed as its frames arrive: what the
// first frame shows fails the connection although the peer never sends
// the rest of the message.
func TestDeflateReadStreams(t *testing.T) {
	bomb := flushed(t, make([]byte, 4<<20)) // 4 MiB of zeros
	for _, tc := range []struct {
		name  string
		op    opcode
		first []byte
		code  StatusCode
	}{
		{"invalid UTF-8", opText, flushed(t, []byte("grüß \xff dich")), StatusInvalidFramePayloadData},
		{"invalid UTF-8 after valid text", opText, append(flushed(t, []byte(strings.Repeat("grüß ", 1000))), flushed(t, []byte("\xc3\x28"))...), StatusInvalidFramePayloadData},
		{"corrupt", opBinary, append(flushed(t, []byte("fine so far")), 0xff, 0xff, 0xff), StatusInvalidFramePayloadData},
		{"bomb", opBinary, bomb, StatusMessageTooBig},
	} {
		for _, server := range []bool{true, false} {
			t.Run(tc.name+map[bool]string{true: "/server", false: "/client"}[server], func(t *testing.T) {
				c, peer := startDeflateConnLimit(t, server, deflateOn(), 64<<10)
				peer.sendFrame(false, rsv1Bit, tc.op, tc.first)
				_, _, err := readMsg(t, c)
				assertProtocolError(t, err, tc.code)
				peer.expectClose(tc.code)
			})
		}
	}
}

// Pings between the fragments of a compressed message are answered while
// the message is still being read, and a close frame between them ends
// the connection with the peer's code.
func TestDeflateReadControlBetweenFragments(t *testing.T) {
	msg := []byte(strings.Repeat("Ein Gespenst geht um in Europa. ", 2000))
	z := compressDict(t, msg, nil, false)

	t.Run("pings", func(t *testing.T) {
		c, peer := startDeflateConn(t, true, deflateOn())
		// Many fragments, empty ones among them, each followed by a ping
		// that is answered before the next fragment is sent.
		rsv, op := rsv1Bit, opText
		for i, rest := 0, z; ; i++ {
			n := min(len(rest), []int{0, 1, 7, 300, 0, 2048}[i%6])
			fin := n == len(rest)
			peer.sendFrame(fin, rsv, op, rest[:n])
			rest = rest[n:]
			rsv, op = 0, opContinuation
			if fin {
				break
			}
			ping := []byte{byte(i)}
			peer.send(true, opPing, ping)
			peer.expectFrame(opPong, ping)
		}
		mustRead(t, c, MessageText, msg)
	})
	t.Run("close", func(t *testing.T) {
		c, peer := startDeflateConn(t, true, deflateOn())
		peer.sendFrame(false, rsv1Bit, opText, z[:len(z)/2])
		peer.send(true, opClose, closePayload(StatusGoingAway, "bye"))
		peer.expectClose(StatusGoingAway)
		_, _, err := readMsg(t, c)
		var ce *CloseError
		if !errors.As(err, &ce) || ce.Code != StatusGoingAway || ce.Reason != "bye" {
			t.Fatalf("Read error %v, want the peer's close 1001 bye", err)
		}
	})
}

// RFC 7692 §7.1.1 and §7.2.3.4: the decompression context carries over
// between fragmented messages, also when a message ends with a final block
// followed by trailing bytes in frames of their own.
func TestDeflateContextTakeoverFragmented(t *testing.T) {
	msgs := [][]byte{
		[]byte(strings.Repeat(`{"event":"tick","source":"vox-relay","seq":1} `, 40)),
		[]byte(strings.Repeat(`{"event":"tick","source":"vox-relay","seq":2} `, 40)),
		[]byte(strings.Repeat(`{"event":"tick","source":"vox-relay","seq":3} `, 40)),
		[]byte(strings.Repeat(`{"event":"tick","source":"vox-relay","seq":4} `, 40)),
	}
	for _, takeover := range []bool{true, false} {
		t.Run(map[bool]string{true: "takeover", false: "no takeover"}[takeover], func(t *testing.T) {
			cfg := deflateOn()
			cfg.readTakeover = takeover
			c, peer := startDeflateConn(t, false, cfg)
			var dict []byte
			for i, m := range msgs {
				final := i%2 == 1
				z := compressDict(t, m, dict, final)
				peer.sendFrame(false, rsv1Bit, opText, z[:len(z)/3])
				peer.send(true, opPing, []byte("p"))
				peer.sendFrame(false, 0, opContinuation, z[len(z)/3:])
				if final {
					// Bytes after the final block are ignored.
					peer.sendFrame(false, 0, opContinuation, []byte{0x00})
				}
				peer.sendFrame(true, 0, opContinuation, nil)
				mustRead(t, c, MessageText, m)
				peer.expectFrame(opPong, []byte("p"))
				if takeover {
					dict = append(dict, m...)
				}
			}
		})
	}
}

// The read limit applies to the decompressed size only: an incompressible
// message at the limit is read although its compressed form is larger.
func TestDeflateReadLimitIncompressible(t *testing.T) {
	const limit = 64 << 10
	random := make([]byte, limit+1)
	_, _ = rand.Read(random)
	pc := newPeerCompressor(t, false)
	for _, tc := range []struct {
		name      string
		size      int
		fragments int
	}{
		{"at the limit", limit, 1},
		{"at the limit in fragments", limit, 5},
		{"one byte over", limit + 1, 1},
		{"one byte over in fragments", limit + 1, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := startDeflateConnLimit(t, true, deflateOn(), limit)
			msg := random[:tc.size]
			z := pc.compress(t, msg)
			if len(z) <= limit {
				t.Fatalf("compressed to %d bytes, want more than the limit", len(z))
			}
			rsv, op := rsv1Bit, opBinary
			for i := range tc.fragments {
				part := z[i*len(z)/tc.fragments : (i+1)*len(z)/tc.fragments]
				peer.sendFrame(i == tc.fragments-1, rsv, op, part)
				rsv, op = 0, opContinuation
			}
			if tc.size <= limit {
				mustRead(t, c, MessageBinary, msg)
				return
			}
			_, _, err := readMsg(t, c)
			assertProtocolError(t, err, StatusMessageTooBig)
			peer.expectClose(StatusMessageTooBig)
		})
	}
}

// A compressed message is not buffered whole before it is decompressed:
// reading a large incompressible message allocates about its decompressed
// size, not that plus its compressed size.
func TestDeflateReadAllocation(t *testing.T) {
	const size = 1 << 20
	msg := make([]byte, size)
	_, _ = rand.Read(msg)
	pc := newPeerCompressor(t, false)
	z := pc.compress(t, msg)

	// The peer is the server side, which writes without masking copies.
	c, peer := startDeflateConnLimit(t, false, deflateOn(), 4<<20)
	peer.sendFrame(true, rsv1Bit, opBinary, []byte{0x02, 0x00}) // warm up the decompressor pool
	mustRead(t, c, MessageBinary, []byte{})

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		peer.sendFrame(true, rsv1Bit, opBinary, z)
	}()
	_, got, err := readMsg(t, c)
	runtime.ReadMemStats(&after)
	<-sent
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("Read = %d bytes, %v; want the message", len(got), err)
	}
	n := after.TotalAlloc - before.TotalAlloc
	t.Logf("reading a %d byte message allocated %d bytes", size, n)
	if n > size+size/2 {
		t.Errorf("reading a %d byte message allocated %d bytes", size, n)
	}
}

// Data that stops decompressing in a later fragment fails the connection
// with 1007, also for text that was valid up to there.
func TestDeflateCorruptLaterFragment(t *testing.T) {
	for _, op := range []opcode{opBinary, opText} {
		t.Run(map[opcode]string{opBinary: "binary", opText: "text"}[op], func(t *testing.T) {
			c, peer := startDeflateConn(t, true, deflateOn())
			peer.sendFrame(false, rsv1Bit, op, flushed(t, []byte(strings.Repeat("grüß ", 100))))
			peer.send(true, opPing, nil)
			peer.sendFrame(true, 0, opContinuation, []byte{0x07, 0xff, 0xff, 0xff})
			_, _, err := readMsg(t, c)
			assertProtocolError(t, err, StatusInvalidFramePayloadData)
			peer.expectFrame(opPong, nil)
			peer.expectClose(StatusInvalidFramePayloadData)
		})
	}
}
