package nwws

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"
)

// testTimeout bounds every wait in the connection tests, so a missing
// frame fails the test instead of hanging it.
const testTimeout = 5 * time.Second

// testConfig has a close timeout longer than testTimeout, so a test that
// only passes by waiting for it fails.
func testConfig() connConfig {
	return connConfig{readLimit: DefaultReadLimit, closeTimeout: 2 * testTimeout}
}

// tcpPair returns the two ends of a loopback TCP connection.
func tcpPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-accepted
	if server == nil {
		t.Fatal("accept failed")
	}
	closeQuietly(t, client)
	closeQuietly(t, server)
	return server, client
}

// rawPeer drives the far end of a connection under test with the frame
// codec, so tests control and observe every frame on the wire.
type rawPeer struct {
	t  *testing.T
	nc net.Conn
	fr frameReader
	fw frameWriter
}

func newRawPeer(t *testing.T, nc net.Conn, server bool) *rawPeer {
	return &rawPeer{
		t:  t,
		nc: nc,
		fr: frameReader{r: nc, server: server, maxPayload: 1 << 30},
		fw: frameWriter{w: nc, server: server},
	}
}

// startConn wraps one end of a TCP pair in a Conn (as the server when
// server is true) and returns it with a raw peer on the other end.
func startConn(t *testing.T, server bool, cfg connConfig) (*Conn, *rawPeer) {
	t.Helper()
	s, cl := tcpPair(t)
	mine, theirs := s, cl
	if !server {
		mine, theirs = cl, s
	}
	c := newConn(mine, server, "", cfg)
	t.Cleanup(func() {
		c.closeNow(ErrClosed)
		c.wait()
	})
	return c, newRawPeer(t, theirs, !server)
}

func (p *rawPeer) send(fin bool, op opcode, payload []byte) {
	p.t.Helper()
	_ = p.nc.SetWriteDeadline(time.Now().Add(testTimeout))
	if err := p.fw.writeFrame(fin, 0, op, payload); err != nil {
		p.t.Fatalf("peer write: %v", err)
	}
}

// sendRaw writes bytes as they are, for frames the codec refuses to write.
func (p *rawPeer) sendRaw(b []byte) {
	p.t.Helper()
	_ = p.nc.SetWriteDeadline(time.Now().Add(testTimeout))
	if _, err := p.nc.Write(b); err != nil {
		p.t.Fatalf("peer write: %v", err)
	}
}

func (p *rawPeer) recv() (frameHeader, []byte) {
	p.t.Helper()
	_ = p.nc.SetReadDeadline(time.Now().Add(testTimeout))
	h, b, err := p.fr.readFrame()
	if err != nil {
		p.t.Fatalf("peer read: %v", err)
	}
	return h, b
}

// expectFrame reads one frame and checks its opcode and payload.
func (p *rawPeer) expectFrame(op opcode, payload []byte) {
	p.t.Helper()
	h, b := p.recv()
	if h.opcode != op || !h.fin || !bytes.Equal(b, payload) {
		p.t.Fatalf("peer got opcode %d fin %v payload %q, want opcode %d payload %q", h.opcode, h.fin, b, op, payload)
	}
}

// expectClose skips frames up to a close frame, checks its status code and
// returns its reason.
func (p *rawPeer) expectClose(want StatusCode) string {
	p.t.Helper()
	for {
		h, b := p.recv()
		if h.opcode != opClose {
			continue
		}
		if len(b) < 2 {
			p.t.Fatalf("close payload %x, want code %d", b, want)
		}
		if got := StatusCode(binary.BigEndian.Uint16(b)); got != want {
			p.t.Fatalf("close code %d (reason %q), want %d", got, b[2:], want)
		}
		return string(b[2:])
	}
}

// expectEOF checks that the connection under test closes TCP.
func (p *rawPeer) expectEOF() {
	p.t.Helper()
	_ = p.nc.SetReadDeadline(time.Now().Add(testTimeout))
	if _, err := io.Copy(io.Discard, p.nc); err != nil {
		p.t.Fatalf("peer waited for TCP close: %v", err)
	}
}

func readMsg(t *testing.T, c *Conn) (MessageType, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	return c.Read(ctx)
}

func mustRead(t *testing.T, c *Conn, wantType MessageType, want []byte) {
	t.Helper()
	typ, b, err := readMsg(t, c)
	if err != nil || typ != wantType || !bytes.Equal(b, want) {
		t.Fatalf("Read = %v %q %v, want %v %q", typ, b, err, wantType, want)
	}
}

func mustWrite(t *testing.T, c *Conn, typ MessageType, p []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	if err := c.Write(ctx, typ, p); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

// echoConnHandler accepts connections and echoes every message until the
// connection ends, then reports the error that ended it.
func echoConnHandler(opts *AcceptOptions, ended chan<- error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r, opts)
		if err != nil {
			return
		}
		for {
			typ, b, err := c.Read(r.Context())
			if err == nil {
				err = c.Write(r.Context(), typ, b)
			}
			if err != nil {
				ended <- err
				return
			}
		}
	}
}

func TestAcceptDialEcho(t *testing.T) {
	big := bytes.Repeat([]byte("0123456789abcdef"), 20000) // 320 KB: 64-bit length, chunked masking
	for _, tlsOn := range []bool{false, true} {
		name := map[bool]string{false: "tcp", true: "tls"}[tlsOn]
		t.Run(name, func(t *testing.T) {
			ended := make(chan error, 1)
			h := echoConnHandler(&AcceptOptions{Subprotocols: []string{"echo"}}, ended)
			opts := &DialOptions{Subprotocols: []string{"echo"}}
			var ts *httptest.Server
			if tlsOn {
				ts = httptest.NewTLSServer(h)
				opts.TLSConfig = &tls.Config{RootCAs: ts.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs}
			} else {
				ts = httptest.NewServer(h)
			}
			defer ts.Close()

			c, resp, err := Dial(t.Context(), wsURL(ts.URL), opts)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols || c.Subprotocol() != "echo" {
				t.Fatalf("status %d, subprotocol %q", resp.StatusCode, c.Subprotocol())
			}
			for _, m := range []struct {
				typ MessageType
				p   []byte
			}{
				{MessageBinary, []byte{0, 1, 2, 0xff}},
				{MessageText, []byte("grüß dich")},
				{MessageBinary, nil},
				{MessageBinary, big},
			} {
				mustWrite(t, c, m.typ, m.p)
				mustRead(t, c, m.typ, m.p)
			}
			if err := c.Close(StatusNormalClosure, "bye"); err != nil {
				t.Fatalf("Close: %v", err)
			}
			var ce *CloseError
			if err := <-ended; !errors.As(err, &ce) || ce.Code != StatusNormalClosure || ce.Reason != "bye" {
				t.Fatalf("server's Read ended with %v, want CloseError 1000 bye", err)
			}
		})
	}
}

func TestReadContext(t *testing.T) {
	c, peer := startConn(t, true, testConfig())

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := c.Read(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read with deadline = %v, want DeadlineExceeded", err)
	}
	cancelled, cancel2 := context.WithCancel(t.Context())
	cancel2()
	if _, _, err := c.Read(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read with cancelled ctx = %v, want Canceled", err)
	}

	// The connection survives an abandoned Read.
	peer.send(true, opBinary, []byte("after"))
	mustRead(t, c, MessageBinary, []byte("after"))
}

// The read limit is set through the options of Accept and Dial.
func TestReadLimitOption(t *testing.T) {
	ended := make(chan error, 1)
	ts := httptest.NewServer(echoConnHandler(&AcceptOptions{ConnOptions: ConnOptions{ReadLimit: 8}}, ended))
	defer ts.Close()
	c, _, err := Dial(t.Context(), wsURL(ts.URL), &DialOptions{ConnOptions: ConnOptions{ReadLimit: 4}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(StatusNormalClosure, "") }()

	mustWrite(t, c, MessageBinary, []byte("1234"))
	mustRead(t, c, MessageBinary, []byte("1234"))
	// The server reads 6 bytes and echoes them; the client's limit is 4.
	mustWrite(t, c, MessageBinary, []byte("123456"))
	_, _, err = readMsg(t, c)
	assertProtocolError(t, err, StatusMessageTooBig)
	select {
	case err := <-ended:
		var ce *CloseError
		if !errors.As(err, &ce) || ce.Code != StatusMessageTooBig {
			t.Fatalf("server ended with %v, want CloseError 1009", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("server did not see the close")
	}
}

func TestConnOptionsDefaults(t *testing.T) {
	for _, tc := range []struct {
		opts *ConnOptions
		want int64
	}{
		{nil, DefaultReadLimit},
		{&ConnOptions{}, DefaultReadLimit},
		{&ConnOptions{ReadLimit: -1}, DefaultReadLimit},
		{&ConnOptions{ReadLimit: 10}, 10},
	} {
		if got := tc.opts.config().readLimit; got != tc.want {
			t.Errorf("%+v: read limit %d, want %d", tc.opts, got, tc.want)
		}
	}
}

// brokenConn fails every write, as a lost connection does.
type brokenConn struct{ net.Conn }

func (brokenConn) Write([]byte) (int, error) { return 0, syscall.EPIPE }

// A write that fails because the connection is lost reports 1006.
func TestWriteUnexpectedLoss(t *testing.T) {
	s, _ := tcpPair(t)
	c := newConn(brokenConn{s}, true, "", testConfig())
	t.Cleanup(func() {
		c.closeNow(ErrClosed)
		c.wait()
	})
	err := c.Write(t.Context(), MessageBinary, []byte("lost"))
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != StatusAbnormalClosure || !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("Write = %v, want CloseError 1006 wrapping EPIPE", err)
	}
	_, _, err = readMsg(t, c)
	if !errors.As(err, &ce) || ce.Code != StatusAbnormalClosure {
		t.Fatalf("Read = %v, want CloseError 1006", err)
	}
}
