package nwws

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// syncTail ends every DEFLATE sync flush; permessage-deflate removes it on
// send and appends it on receive (RFC 7692 §7.2.1, §7.2.2).
var syncTail = []byte{0x00, 0x00, 0xff, 0xff}

// deflateOn is the configuration of a connection that negotiated
// permessage-deflate with context takeover in both directions.
func deflateOn() deflateConfig {
	return deflateConfig{threshold: 64, level: flate.BestSpeed, writeTakeover: true, writeWindowBits: 15, readTakeover: true}
}

// startDeflateConn starts a connection with permessage-deflate as
// configured by d, and a raw peer that may set RSV1.
func startDeflateConn(t *testing.T, server bool, d deflateConfig) (*Conn, *rawPeer) {
	t.Helper()
	return startDeflateConnLimit(t, server, d, DefaultReadLimit)
}

func startDeflateConnLimit(t *testing.T, server bool, d deflateConfig, limit int64) (*Conn, *rawPeer) {
	t.Helper()
	cfg := testConfig()
	cfg.deflate = &d
	cfg.readLimit = limit
	c, peer := startConn(t, server, cfg)
	peer.fr.rsv, peer.fw.rsv = rsv1Bit, rsv1Bit
	return c, peer
}

func (p *rawPeer) sendFrame(fin bool, rsv byte, op opcode, payload []byte) {
	p.t.Helper()
	if err := p.fw.writeFrame(fin, rsv, op, payload); err != nil {
		p.t.Fatalf("peer write: %v", err)
	}
}

// peerCompressor compresses messages as an independent peer does, with
// compress/flate: a sync flush per message, its tail removed.
type peerCompressor struct {
	buf      bytes.Buffer
	w        *flate.Writer
	takeover bool
}

func newPeerCompressor(t *testing.T, takeover bool) *peerCompressor {
	pc := &peerCompressor{takeover: takeover}
	w, err := flate.NewWriter(&pc.buf, flate.DefaultCompression)
	if err != nil {
		t.Fatal(err)
	}
	pc.w = w
	return pc
}

func (pc *peerCompressor) compress(t *testing.T, msg []byte) []byte {
	t.Helper()
	pc.buf.Reset()
	if !pc.takeover {
		pc.w.Reset(&pc.buf)
	}
	if _, err := pc.w.Write(msg); err != nil {
		t.Fatal(err)
	}
	if err := pc.w.Flush(); err != nil {
		t.Fatal(err)
	}
	out, ok := bytes.CutSuffix(pc.buf.Bytes(), syncTail)
	if !ok {
		t.Fatalf("flush output %x lacks the sync tail", pc.buf.Bytes())
	}
	return bytes.Clone(out)
}

// peerInflate decompresses a message payload the way an independent peer
// does: tail appended, preset with dict, the previous messages' output.
func peerInflate(t *testing.T, payload, dict []byte) []byte {
	t.Helper()
	r := flate.NewReaderDict(io.MultiReader(bytes.NewReader(payload), bytes.NewReader(syncTail)), dict)
	out, err := io.ReadAll(r)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("inflate %x: %v", payload, err)
	}
	return out
}

// RFC 7692 §6 and §7.2.1: a data message of at least the threshold is sent
// compressed with RSV1 set and without the sync tail; shorter messages and
// control frames are sent as they are.
func TestDeflateWrite(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startDeflateConn(t, server, deflateOn())
			msg := []byte(strings.Repeat("all work and no play makes jack a dull boy ", 30))
			mustWrite(t, c, MessageText, msg)
			h, payload := peer.recv()
			if h.rsv != rsv1Bit || h.opcode != opText || !h.fin {
				t.Fatalf("frame rsv %#x opcode %d fin %v, want a final text frame with RSV1", h.rsv, h.opcode, h.fin)
			}
			if bytes.HasSuffix(payload, syncTail) || len(payload) >= len(msg)/4 {
				t.Fatalf("payload of %d bytes for %d: %x", len(payload), len(msg), payload)
			}
			if got := peerInflate(t, payload, nil); !bytes.Equal(got, msg) {
				t.Fatalf("inflated %q, want %q", got, msg)
			}

			short := bytes.Repeat([]byte("x"), 63) // below the threshold of 64
			mustWrite(t, c, MessageBinary, short)
			h, payload = peer.recv()
			if h.rsv != 0 || h.opcode != opBinary || !bytes.Equal(payload, short) {
				t.Fatalf("short message: rsv %#x opcode %d payload %q, want it uncompressed", h.rsv, h.opcode, payload)
			}

			ping := bytes.Repeat([]byte("p"), 100)
			peer.send(true, opPing, ping)
			h, payload = peer.recv()
			if h.rsv != 0 || h.opcode != opPong || !bytes.Equal(payload, ping) {
				t.Fatalf("pong: rsv %#x opcode %d payload %q, want it uncompressed", h.rsv, h.opcode, payload)
			}
		})
	}
}

// RFC 7692 §6.2 and §7.2.2: a message with RSV1 on its first frame is
// decompressed after appending the sync tail, also across fragments with
// control frames between them. Messages without RSV1 are read as they are.
func TestDeflateRead(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startDeflateConn(t, server, deflateConfig{threshold: 64, level: flate.BestSpeed, writeWindowBits: 15})
			pc := newPeerCompressor(t, false)

			msg := []byte(strings.Repeat("grüß dich, ", 50))
			peer.sendFrame(true, rsv1Bit, opText, pc.compress(t, msg))
			mustRead(t, c, MessageText, msg)

			peer.send(true, opBinary, []byte("plain"))
			mustRead(t, c, MessageBinary, []byte("plain"))

			big := make([]byte, 100_000)
			_, _ = rand.Read(big[:50_000]) // half incompressible
			z := pc.compress(t, big)
			third := len(z) / 3
			peer.sendFrame(false, rsv1Bit, opBinary, z[:third])
			peer.send(true, opPing, []byte("between"))
			peer.sendFrame(false, 0, opContinuation, z[third:2*third])
			peer.sendFrame(true, 0, opContinuation, z[2*third:])
			mustRead(t, c, MessageBinary, big)
			peer.expectFrame(opPong, []byte("between"))

			// An empty compressed message.
			peer.sendFrame(true, rsv1Bit, opBinary, pc.compress(t, nil))
			mustRead(t, c, MessageBinary, []byte{})

			// RFC 7692 §7.2.3.4: a peer may end a message with a final block.
			var buf bytes.Buffer
			w, _ := flate.NewWriter(&buf, flate.BestCompression)
			_, _ = w.Write(msg)
			_ = w.Close()
			peer.sendFrame(true, rsv1Bit, opText, buf.Bytes())
			mustRead(t, c, MessageText, msg)
		})
	}
}

// RFC 7692 §7.1.1: with context takeover, the compression context carries
// over between messages; without it, each message stands alone.
func TestDeflateContextTakeover(t *testing.T) {
	msgs := [][]byte{
		[]byte(strings.Repeat(`{"event":"tick","source":"vox-relay","payload":"abc"} `, 4)),
		[]byte(strings.Repeat(`{"event":"tick","source":"vox-relay","payload":"abd"} `, 4)),
		[]byte(strings.Repeat(`{"event":"tick","source":"vox-relay","payload":"abe"} `, 4)),
	}
	t.Run("write", func(t *testing.T) {
		for _, takeover := range []bool{true, false} {
			cfg := deflateOn()
			cfg.writeTakeover = takeover
			c, peer := startDeflateConn(t, true, cfg)
			var dict, first []byte
			for i, m := range msgs {
				mustWrite(t, c, MessageText, m)
				_, payload := peer.recv()
				if !takeover {
					dict = nil
				}
				got := peerInflate(t, payload, dict)
				if !bytes.Equal(got, m) {
					t.Fatalf("takeover %v: message %d inflated to %q", takeover, i, got)
				}
				dict = append(dict, got...)
				switch {
				case i == 0:
					first = payload
				case takeover && len(payload) >= len(first)/2:
					t.Errorf("takeover: message %d took %d bytes, the first %d; want the context reused", i, len(payload), len(first))
				case !takeover && len(payload) < len(first)-4:
					t.Errorf("no takeover: message %d took %d bytes, the first %d; want no context reused", i, len(payload), len(first))
				}
			}
		}
	})
	t.Run("read", func(t *testing.T) {
		for _, takeover := range []bool{true, false} {
			cfg := deflateOn()
			cfg.readTakeover = takeover
			c, peer := startDeflateConn(t, false, cfg)
			pc := newPeerCompressor(t, takeover)
			for range 3 {
				for _, m := range msgs {
					peer.sendFrame(true, rsv1Bit, opText, pc.compress(t, m))
					mustRead(t, c, MessageText, m)
				}
			}
		}
	})
	t.Run("read beyond the window", func(t *testing.T) {
		// Back-references reach up to 32 KiB into earlier messages.
		c, peer := startDeflateConn(t, true, deflateOn())
		pc := newPeerCompressor(t, true)
		block := make([]byte, 20_000)
		_, _ = rand.Read(block)
		for i := range 5 {
			m := append(fmt.Appendf(nil, "%d", i), block...)
			peer.sendFrame(true, rsv1Bit, opBinary, pc.compress(t, m))
			mustRead(t, c, MessageBinary, m)
		}
	})
}

// RFC 7692 §7.1.2: when the peer limits this side's window below the
// 32 KiB of compress/flate, messages are Huffman-coded without LZ77
// matches, which honours any window size.
func TestDeflateWindowBits(t *testing.T) {
	msg := bytes.Repeat([]byte("a"), 64<<10)
	for _, tc := range []struct {
		bits        int
		huffmanOnly bool
	}{{15, false}, {14, true}, {9, true}, {8, true}} {
		cfg := deflateOn()
		cfg.writeWindowBits = tc.bits
		c, peer := startDeflateConn(t, true, cfg)
		for range 2 {
			mustWrite(t, c, MessageBinary, msg)
			_, payload := peer.recv()
			if got := peerInflate(t, payload, nil); !bytes.Equal(got, msg) {
				t.Fatalf("%d bits: inflated %d bytes, want the message", tc.bits, len(got))
			}
			// Huffman coding takes at least one bit per byte; LZ77 needs
			// a few bytes for the whole run.
			if huffman := len(payload) >= len(msg)/8; huffman != tc.huffmanOnly {
				t.Fatalf("%d bits: %d bytes for %d, want Huffman only %v", tc.bits, len(payload), len(msg), tc.huffmanOnly)
			}
		}
	}
}

// RFC 7692 §6.1: RSV1 is allowed only on the first frame of a data
// message. Anything else fails the connection with 1002, as does RSV1
// without negotiated compression (RFC 6455 §5.2).
func TestDeflateCompressedBitErrors(t *testing.T) {
	ping := func(p *rawPeer) { p.sendFrame(true, rsv1Bit, opPing, []byte("x")) }
	for _, tc := range []struct {
		name string
		on   bool
		send func(*rawPeer)
	}{
		{"ping", true, ping},
		{"pong", true, func(p *rawPeer) { p.sendFrame(true, rsv1Bit, opPong, nil) }},
		{"close", true, func(p *rawPeer) { p.sendFrame(true, rsv1Bit, opClose, closePayload(StatusNormalClosure, "")) }},
		{"ping inside a message", true, func(p *rawPeer) {
			p.sendFrame(false, rsv1Bit, opText, []byte{0x02})
			ping(p)
		}},
		{"continuation", true, func(p *rawPeer) {
			p.sendFrame(false, 0, opBinary, []byte("a"))
			p.sendFrame(true, rsv1Bit, opContinuation, []byte("b"))
		}},
		{"continuation of compressed", true, func(p *rawPeer) {
			p.sendFrame(false, rsv1Bit, opBinary, []byte{0x02})
			p.sendFrame(true, rsv1Bit, opContinuation, []byte{0x00})
		}},
		{"not negotiated", false, func(p *rawPeer) { p.sendRaw([]byte{0x80 | rsv1Bit | byte(opText), 0x80, 0, 0, 0, 0}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c *Conn
			var peer *rawPeer
			if tc.on {
				c, peer = startDeflateConn(t, true, deflateOn())
			} else {
				c, peer = startConn(t, true, testConfig())
			}
			tc.send(peer)
			_, _, err := readMsg(t, c)
			assertProtocolError(t, err, StatusProtocolError)
			peer.expectClose(StatusProtocolError)
		})
	}
}

// The read limit applies to the decompressed size, and decompression stops
// once it is exceeded: a small compressed message that inflates to far more
// than the limit closes the connection with 1009 without inflating it all.
func TestDeflateReadLimit(t *testing.T) {
	const limit = 64 << 10
	pc := newPeerCompressor(t, false)
	start := func(t *testing.T) (*Conn, *rawPeer) {
		t.Helper()
		return startDeflateConnLimit(t, true, deflateOn(), limit)
	}

	t.Run("at the limit", func(t *testing.T) {
		c, peer := start(t)
		msg := bytes.Repeat([]byte("z"), limit)
		peer.sendFrame(true, rsv1Bit, opBinary, pc.compress(t, msg))
		mustRead(t, c, MessageBinary, msg)
	})
	t.Run("one byte over", func(t *testing.T) {
		c, peer := start(t)
		peer.sendFrame(true, rsv1Bit, opBinary, pc.compress(t, bytes.Repeat([]byte("z"), limit+1)))
		_, _, err := readMsg(t, c)
		assertProtocolError(t, err, StatusMessageTooBig)
		peer.expectClose(StatusMessageTooBig)
	})
	t.Run("bomb", func(t *testing.T) {
		c, peer := start(t)
		bomb := pc.compress(t, make([]byte, 16<<20)) // 16 MiB of zeros, about 32 KiB compressed
		if len(bomb) > limit {
			t.Fatalf("bomb of %d bytes does not fit the limit", len(bomb))
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		peer.sendFrame(true, rsv1Bit, opBinary, bomb)
		_, _, err := readMsg(t, c)
		runtime.ReadMemStats(&after)
		assertProtocolError(t, err, StatusMessageTooBig)
		peer.expectClose(StatusMessageTooBig)
		if n := after.TotalAlloc - before.TotalAlloc; n > 8<<20 {
			t.Errorf("reading the bomb allocated %d bytes; want decompression to stop at the limit", n)
		}
	})
	t.Run("compressed size over the limit", func(t *testing.T) {
		c, peer := start(t)
		peer.sendFrame(false, rsv1Bit, opBinary, make([]byte, limit/2))
		peer.sendFrame(true, 0, opContinuation, make([]byte, limit/2+1))
		_, _, err := readMsg(t, c)
		assertProtocolError(t, err, StatusMessageTooBig)
	})
}

// Data that does not decompress, or text that is not UTF-8 once
// decompressed, fails the connection with 1007.
func TestDeflateInvalidPayload(t *testing.T) {
	pc := newPeerCompressor(t, false)
	for _, tc := range []struct {
		name    string
		op      opcode
		payload []byte
	}{
		{"corrupt", opBinary, []byte{0xff, 0xff, 0xff, 0xff}},
		{"invalid UTF-8", opText, pc.compress(t, []byte("ok \xff\xfe not ok"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := startDeflateConn(t, true, deflateOn())
			peer.sendFrame(true, rsv1Bit, tc.op, tc.payload)
			_, _, err := readMsg(t, c)
			assertProtocolError(t, err, StatusInvalidFramePayloadData)
			peer.expectClose(StatusInvalidFramePayloadData)
		})
	}
}

// Two nwws peers with compression echo messages of every size and kind,
// with and without context takeover, also with concurrent writers.
func TestDeflateEcho(t *testing.T) {
	random := make([]byte, 300_000)
	_, _ = rand.Read(random)
	text := []byte(strings.Repeat("Ein Gespenst geht um in Europa. ", 10_000))
	for _, tc := range []struct {
		name           string
		server, client *CompressionOptions
	}{
		{"takeover", &CompressionOptions{Threshold: 1}, &CompressionOptions{Threshold: 1}},
		{"no takeover", &CompressionOptions{ServerNoContextTakeover: true}, &CompressionOptions{ClientNoContextTakeover: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ended := make(chan error, 1)
			ts := httptest.NewServer(echoConnHandler(&AcceptOptions{ConnOptions: ConnOptions{Compression: tc.server, ReadLimit: 4 << 20}}, ended))
			defer ts.Close()
			c, _, err := Dial(t.Context(), wsURL(ts.URL), &DialOptions{ConnOptions: ConnOptions{Compression: tc.client, ReadLimit: 4 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			if c.cfg.deflate == nil {
				t.Fatal("compression not negotiated")
			}
			for _, m := range []struct {
				typ MessageType
				p   []byte
			}{
				{MessageBinary, nil},
				{MessageText, []byte("a")},
				{MessageText, text[:200]},
				{MessageBinary, random},
				{MessageText, text},
				{MessageText, text[:200]},
				{MessageBinary, random[:40_000]},
			} {
				mustWrite(t, c, m.typ, m.p)
				mustRead(t, c, m.typ, m.p)
			}

			const writers, each = 8, 25
			var wg sync.WaitGroup
			for w := range writers {
				wg.Go(func() {
					for i := range each {
						msg := fmt.Appendf(nil, "writer %d message %d %s", w, i, text[:i*10])
						if err := c.Write(context.Background(), MessageText, msg); err != nil {
							t.Errorf("Write: %v", err)
							return
						}
					}
				})
			}
			seen := map[string]bool{}
			for range writers * each {
				_, b, err := readMsg(t, c)
				if err != nil {
					t.Fatalf("Read: %v", err)
				}
				seen[string(b)] = true
			}
			wg.Wait()
			if len(seen) != writers*each {
				t.Fatalf("%d distinct echoes, want %d", len(seen), writers*each)
			}
			if err := c.Close(StatusNormalClosure, "done"); err != nil {
				t.Fatalf("Close: %v", err)
			}
			var ce *CloseError
			if err := <-ended; !errors.As(err, &ce) || ce.Code != StatusNormalClosure {
				t.Fatalf("server ended with %v", err)
			}
		})
	}
}
