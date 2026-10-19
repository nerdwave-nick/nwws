package nwws

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// validLevels are the compression levels CompressionOptions.Level accepts.
var validLevels = []int{0, flate.HuffmanOnly, 1, 2, 3, 4, 5, 6, 7, 8, 9}

// compressibleText returns n bytes of text drawn from a small vocabulary,
// on which higher compression levels find more and longer matches.
func compressibleText(n int) []byte {
	words := strings.Fields(`the a of to and in that is was he for it with as his on
		be at by had not are but from or have an they which one you were her all
		she there would their we him been has when who will more no if out so said`)
	r := rand.New(rand.NewPCG(1, 2))
	var b bytes.Buffer
	for b.Len() < n {
		b.WriteString(words[r.IntN(len(words))])
		b.WriteByte(" \n"[r.IntN(2)])
	}
	return b.Bytes()[:n]
}

// referenceDeflate compresses msg as a fresh compress/flate writer at level
// does, ending with a sync flush whose tail is removed (RFC 7692 §7.2.1).
func referenceDeflate(t testing.TB, level int, msg []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, level)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(msg); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	out, ok := bytes.CutSuffix(buf.Bytes(), syncTail)
	if !ok {
		t.Fatalf("flush output lacks the sync tail")
	}
	return out
}

// effectiveLevel is the compress/flate level that Level selects.
func effectiveLevel(level int) int {
	if level == 0 {
		return flate.BestSpeed
	}
	return level
}

// serverFrameAtLevel accepts a connection with compression at level, has
// it send msg, and returns the payload of the frame on the wire, read by a
// raw client that offered permessage-deflate.
func serverFrameAtLevel(t *testing.T, level int, msg []byte) []byte {
	t.Helper()
	opts := &AcceptOptions{ConnOptions: ConnOptions{Compression: &CompressionOptions{Threshold: 1, Level: level}}}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r, opts)
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer c.closeNow(ErrClosed)
		if err := c.Write(r.Context(), MessageBinary, msg); err != nil {
			t.Errorf("Write: %v", err)
			return
		}
		_, _, _ = c.Read(r.Context()) // until the client goes away
	}))
	defer ts.Close()

	nc, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	_ = nc.SetDeadline(time.Now().Add(testTimeout))
	req := "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n"
	if _, err := io.WriteString(nc, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Extensions") == "" {
		t.Fatalf("status %d, extensions %q", resp.StatusCode, resp.Header.Get("Sec-WebSocket-Extensions"))
	}
	fr := frameReader{r: br, maxPayload: 1 << 30, rsv: rsv1Bit}
	h, payload, err := fr.readFrame()
	if err != nil {
		t.Fatal(err)
	}
	if h.rsv != rsv1Bit {
		t.Fatalf("frame rsv %#x, want RSV1", h.rsv)
	}
	return payload
}

// clientFrameAtLevel dials with compression at level, sends msg, and
// returns the payload of the frame on the wire, read by a raw server that
// accepts permessage-deflate.
func clientFrameAtLevel(t *testing.T, level int, msg []byte) []byte {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	got := make(chan []byte, 1)
	go func() {
		defer close(got)
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = nc.Close() }()
		_ = nc.SetDeadline(time.Now().Add(testTimeout))
		br := bufio.NewReader(nc)
		r, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		if _, err := io.WriteString(nc, response(r, "Sec-WebSocket-Extensions: permessage-deflate\r\n")); err != nil {
			return
		}
		fr := frameReader{r: br, server: true, maxPayload: 1 << 30, rsv: rsv1Bit}
		h, payload, err := fr.readFrame()
		if err != nil || h.rsv != rsv1Bit {
			return
		}
		got <- payload
	}()
	c, _, err := Dial(t.Context(), "ws://"+ln.Addr().String(), &DialOptions{ConnOptions: ConnOptions{
		Compression: &CompressionOptions{Threshold: 1, Level: level},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.closeNow(ErrClosed)
	mustWrite(t, c, MessageBinary, msg)
	payload, ok := <-got
	if !ok {
		t.Fatal("the raw server read no compressed frame")
	}
	return payload
}

// CompressionOptions.Level selects the level both sides compress with:
// the payload on the wire is what compress/flate makes at that level, and
// zero means flate.BestSpeed. A higher level compresses better.
func TestDeflateLevel(t *testing.T) {
	msg := compressibleText(64 << 10)
	for _, side := range []struct {
		name  string
		frame func(*testing.T, int, []byte) []byte
	}{{"server", serverFrameAtLevel}, {"client", clientFrameAtLevel}} {
		t.Run(side.name, func(t *testing.T) {
			sizes := map[int]int{}
			for _, level := range validLevels {
				payload := side.frame(t, level, msg)
				if want := referenceDeflate(t, effectiveLevel(level), msg); !bytes.Equal(payload, want) {
					t.Errorf("level %d: %d bytes on the wire, want the %d bytes of flate level %d",
						level, len(payload), len(want), effectiveLevel(level))
				}
				sizes[level] = len(payload)
			}
			t.Logf("payload sizes for %d bytes by level: %v", len(msg), sizes)
			if sizes[flate.BestCompression] >= sizes[flate.BestSpeed] {
				t.Errorf("BestCompression gave %d bytes, BestSpeed %d; want smaller", sizes[flate.BestCompression], sizes[flate.BestSpeed])
			}
			if sizes[flate.HuffmanOnly] <= sizes[flate.BestSpeed] {
				t.Errorf("HuffmanOnly gave %d bytes, BestSpeed %d; want larger", sizes[flate.HuffmanOnly], sizes[flate.BestSpeed])
			}
		})
	}
}

// A window limited by the peer to fewer than 15 bits forces Huffman-only
// coding, whatever the level (RFC 7692 §7.1.2).
func TestDeflateLevelLimitedWindow(t *testing.T) {
	msg := compressibleText(16 << 10)
	want := referenceDeflate(t, flate.HuffmanOnly, msg)
	for _, level := range []int{flate.BestSpeed, 6, flate.BestCompression} {
		for _, takeover := range []bool{true, false} {
			cfg := deflateOn()
			cfg.level, cfg.writeWindowBits, cfg.writeTakeover = level, 9, takeover
			c, peer := startDeflateConn(t, true, cfg)
			mustWrite(t, c, MessageBinary, msg)
			if _, payload := peer.recv(); !bytes.Equal(payload, want) {
				t.Errorf("level %d, takeover %v: %d bytes, want the %d bytes of Huffman-only coding", level, takeover, len(payload), len(want))
			}
		}
	}
}

// Compressors without context takeover are pooled; a connection never gets
// one of another level, also when connections at several levels compress
// at the same time.
func TestDeflateLevelPools(t *testing.T) {
	msg := compressibleText(8 << 10)
	levels := []int{flate.BestSpeed, flate.BestCompression, flate.HuffmanOnly, 5}
	want := map[int][]byte{}
	for _, l := range levels {
		want[l] = referenceDeflate(t, l, msg)
	}
	const conns, each = 8, 20
	var wg sync.WaitGroup
	for i := range conns {
		level := levels[i%len(levels)]
		cfg := deflateOn()
		cfg.level, cfg.writeTakeover = level, false
		c, peer := startDeflateConn(t, true, cfg)
		wg.Go(func() {
			for range each {
				if err := c.Write(context.Background(), MessageBinary, msg); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		})
		wg.Go(func() {
			for range each {
				_ = peer.nc.SetReadDeadline(time.Now().Add(testTimeout))
				_, payload, err := peer.fr.readFrame()
				if err != nil {
					t.Errorf("peer read: %v", err)
					return
				}
				if !bytes.Equal(payload, want[level]) {
					t.Errorf("level %d connection sent %d bytes, want the %d bytes of its level", level, len(payload), len(want[level]))
					return
				}
			}
		})
	}
	wg.Wait()
}

// Any level other than 0, flate.HuffmanOnly and 1–9 makes Accept answer
// with an error status without upgrading, and Dial fail without dialing.
func TestDeflateLevelInvalid(t *testing.T) {
	for _, level := range []int{flate.DefaultCompression, -3, 10, 100} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			t.Run("accept", func(t *testing.T) {
				acceptErr := make(chan error, 1)
				opts := &AcceptOptions{ConnOptions: ConnOptions{Compression: &CompressionOptions{Level: level}}}
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					c, err := Accept(w, r, opts)
					if c != nil {
						c.closeNow(ErrClosed)
					}
					acceptErr <- err
				}))
				defer ts.Close()
				c, resp, err := Dial(t.Context(), wsURL(ts.URL), nil)
				if c != nil || !errors.Is(err, ErrBadHandshake) || resp == nil || resp.StatusCode != http.StatusInternalServerError {
					t.Fatalf("Dial = %v, %v, %v; want a refused handshake with status 500", c, resp, err)
				}
				if err := <-acceptErr; err == nil || !strings.Contains(err.Error(), "level") {
					t.Fatalf("Accept error %v, want one about the level", err)
				}
			})
			t.Run("dial", func(t *testing.T) {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = ln.Close() }()
				// Nothing answers on ln: a Dial that connects waits for ctx.
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				c, resp, err := Dial(ctx, "ws://"+ln.Addr().String(), &DialOptions{ConnOptions: ConnOptions{
					Compression: &CompressionOptions{Level: level},
				}})
				if c != nil || resp != nil || err == nil || !strings.Contains(err.Error(), "level") {
					t.Fatalf("Dial = %v, %v, %v; want an error about the level", c, resp, err)
				}
				// Dial has returned, so a connection it made would be queued.
				_ = ln.(*net.TCPListener).SetDeadline(time.Now())
				if nc, err := ln.Accept(); err == nil {
					_ = nc.Close()
					t.Fatal("Dial connected before refusing the level")
				}
			})
		})
	}
}

// Two nwws peers echo messages at every valid level, with context takeover.
func TestDeflateLevelEcho(t *testing.T) {
	msg := compressibleText(40 << 10)
	for _, level := range validLevels {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			co := &CompressionOptions{Level: level}
			ended := make(chan error, 1)
			ts := httptest.NewServer(echoConnHandler(&AcceptOptions{ConnOptions: ConnOptions{Compression: co}}, ended))
			defer ts.Close()
			c, _, err := Dial(t.Context(), wsURL(ts.URL), &DialOptions{ConnOptions: ConnOptions{Compression: co}})
			if err != nil {
				t.Fatal(err)
			}
			if c.cfg.deflate == nil {
				t.Fatal("compression not negotiated")
			}
			for _, m := range [][]byte{msg, msg[:300], msg} {
				mustWrite(t, c, MessageText, m)
				mustRead(t, c, MessageText, m)
			}
			if err := c.Close(StatusNormalClosure, ""); err != nil {
				t.Fatalf("Close: %v", err)
			}
			<-ended
		})
	}
}

// BenchmarkDeflateRetainedMemory measures the heap a connection retains
// between messages: connection pairs over loopback TCP exchange one 64 KiB
// message each way, then the live heap is compared with the heap before
// they were opened. With context takeover each side keeps its compressor
// and its 32 KiB decompression window. The B/conn metric is per Conn,
// counting both ends of each pair.
//
//	go test -run '^$' -bench DeflateRetainedMemory -benchtime 3x
func BenchmarkDeflateRetainedMemory(b *testing.B) {
	msg := compressibleText(64 << 10)
	for _, tc := range []struct {
		name  string
		level int // -1: no compression
	}{{"off", -1}, {"level=1", 1}, {"level=6", 6}, {"level=9", 9}} {
		b.Run(tc.name, func(b *testing.B) {
			var total float64
			for range b.N {
				total += retainedPerConn(b, tc.level, msg)
			}
			b.ReportMetric(total/float64(b.N), "B/conn")
		})
	}
}

// retainedPerConn returns the heap bytes retained per Conn, as described
// on BenchmarkDeflateRetainedMemory.
func retainedPerConn(b *testing.B, level int, msg []byte) float64 {
	const pairs = 32
	cfg := connConfig{readLimit: DefaultReadLimit, closeTimeout: DefaultCloseTimeout}
	if level >= 0 {
		cfg.deflate = &deflateConfig{threshold: 1, level: level, writeTakeover: true, writeWindowBits: maxWindowBits, readTakeover: true}
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	conns := make([]*Conn, 0, 2*pairs)
	for range pairs {
		s, cl := loopbackPair(b)
		conns = append(conns, newConn(s, true, "", cfg), newConn(cl, false, "", cfg))
	}
	ctx := context.Background()
	for i := 0; i < len(conns); i += 2 {
		for _, pair := range [][2]*Conn{{conns[i], conns[i+1]}, {conns[i+1], conns[i]}} {
			if err := pair[0].Write(ctx, MessageText, msg); err != nil {
				b.Fatal(err)
			}
			if _, got, err := pair[1].Read(ctx); err != nil || !bytes.Equal(got, msg) {
				b.Fatalf("Read: %v", err)
			}
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	for _, c := range conns {
		c.closeNow(ErrClosed)
		c.wait()
	}
	return float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(len(conns))
}

// loopbackPair returns the two ends of a loopback TCP connection.
func loopbackPair(tb testing.TB) (server, client net.Conn) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		tb.Fatal(err)
	}
	if server = <-accepted; server == nil {
		tb.Fatal("accept failed")
	}
	return server, client
}
