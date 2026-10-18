package nwws

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func wsURL(httpURL string) string { return "ws" + strings.TrimPrefix(httpURL, "http") }

func closeQuietly(t *testing.T, c io.Closer) { t.Cleanup(func() { _ = c.Close() }) }

// echoServer accepts the handshake and echoes the upgraded byte stream.
func echoHandler(opts *AcceptOptions, seen chan<- *http.Request) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if seen != nil {
			seen <- r
		}
		hs, err := acceptHandshake(w, r, opts)
		if err != nil {
			return
		}
		defer func() { _ = hs.conn.Close() }()
		_, _ = io.Copy(hs.conn, hs.conn)
	}
}

func assertEcho(t *testing.T, c net.Conn) {
	t.Helper()
	if _, err := io.WriteString(c, "hello"); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 5)
	if _, err := io.ReadFull(c, b); err != nil || string(b) != "hello" {
		t.Fatalf("echo = %q, %v", b, err)
	}
}

func TestDialAgainstAccept(t *testing.T) {
	seen := make(chan *http.Request, 1)
	ts := httptest.NewServer(echoHandler(&AcceptOptions{Subprotocols: []string{"b", "a"}}, seen))
	defer ts.Close()

	hdr := http.Header{"X-Token": {"secret"}, "Sec-Websocket-Key": {"ignored"}}
	hs, resp, err := dialHandshake(t.Context(), wsURL(ts.URL)+"/path?q=1", &DialOptions{
		Header: hdr, Subprotocols: []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	closeQuietly(t, hs.conn)
	if resp.StatusCode != http.StatusSwitchingProtocols || hs.subprotocol != "b" || hs.extensions != "" {
		t.Fatalf("status %d, subprotocol %q, extensions %q", resp.StatusCode, hs.subprotocol, hs.extensions)
	}
	r := <-seen
	if r.URL.RequestURI() != "/path?q=1" || r.Header.Get("X-Token") != "secret" {
		t.Errorf("request URI %q, X-Token %q", r.URL.RequestURI(), r.Header.Get("X-Token"))
	}
	if got := r.Header.Get("Sec-WebSocket-Protocol"); got != "a, b" {
		t.Errorf("offered subprotocols %q", got)
	}
	if k, err := base64.StdEncoding.DecodeString(r.Header.Get("Sec-WebSocket-Key")); err != nil || len(k) != 16 {
		t.Errorf("key %q is not base64 of 16 bytes", r.Header.Get("Sec-WebSocket-Key"))
	}
	assertEcho(t, hs.conn)
}

func TestDialKeysAreRandom(t *testing.T) {
	seen := make(chan *http.Request, 2)
	ts := httptest.NewServer(echoHandler(nil, seen))
	defer ts.Close()
	for range 2 {
		hs, _, err := dialHandshake(t.Context(), wsURL(ts.URL), nil)
		if err != nil {
			t.Fatal(err)
		}
		closeQuietly(t, hs.conn)
	}
	if a, b := (<-seen).Header.Get("Sec-WebSocket-Key"), (<-seen).Header.Get("Sec-WebSocket-Key"); a == b {
		t.Fatalf("two dials used the same key %q", a)
	}
}

func TestDialServerRejection(t *testing.T) {
	ts := httptest.NewServer(echoHandler(nil, nil))
	defer ts.Close()
	hs, resp, err := dialHandshake(t.Context(), wsURL(ts.URL), &DialOptions{
		Header: http.Header{"Origin": {"https://evil.example"}},
	})
	if hs != nil || !errors.Is(err, ErrBadHandshake) {
		t.Fatalf("dial = %v, %v; want ErrBadHandshake", hs, err)
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("response %v, want 403", resp)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Error("rejection body was not kept")
	}
}

// rawServer answers one handshake with a response built by respond, which
// receives the request; whatever it returns is written in a single write.
func rawServer(t *testing.T, respond func(r *http.Request) string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closeQuietly(t, ln)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		r, err := http.ReadRequest(bufio.NewReader(c))
		if err != nil {
			return
		}
		_, _ = io.WriteString(c, respond(r))
		_, _ = io.Copy(io.Discard, c)
	}()
	return "ws://" + ln.Addr().String()
}

func response(r *http.Request, extra string) string {
	return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey(r.Header.Get("Sec-WebSocket-Key")) + "\r\n" + extra + "\r\n"
}

func TestDialValidatesResponse(t *testing.T) {
	tests := []struct {
		name    string
		offer   []string
		respond func(r *http.Request) string
	}{
		{"status not 101", nil, func(r *http.Request) string {
			return "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nno"
		}},
		{"missing Upgrade", nil, func(r *http.Request) string {
			return strings.Replace(response(r, ""), "Upgrade: websocket\r\n", "", 1)
		}},
		{"missing Connection", nil, func(r *http.Request) string {
			return strings.Replace(response(r, ""), "Connection: Upgrade\r\n", "", 1)
		}},
		{"wrong accept", nil, func(r *http.Request) string {
			return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
				"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
		}},
		{"subprotocol not offered", []string{"a"}, func(r *http.Request) string {
			return response(r, "Sec-WebSocket-Protocol: b\r\n")
		}},
		{"subprotocol when none offered", nil, func(r *http.Request) string {
			return response(r, "Sec-WebSocket-Protocol: a\r\n")
		}},
		{"two subprotocols", []string{"a", "b"}, func(r *http.Request) string {
			return response(r, "Sec-WebSocket-Protocol: a, b\r\n")
		}},
		{"extension never offered", nil, func(r *http.Request) string {
			return response(r, "Sec-WebSocket-Extensions: permessage-deflate\r\n")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := rawServer(t, tt.respond)
			hs, _, err := dialHandshake(t.Context(), url, &DialOptions{Subprotocols: tt.offer})
			if hs != nil || !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("dial = %v, %v; want ErrBadHandshake", hs, err)
			}
		})
	}
}

func TestDialAcceptsCaseInsensitiveTokens(t *testing.T) {
	url := rawServer(t, func(r *http.Request) string {
		return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: WebSocket\r\nConnection: keep-alive, UPGRADE\r\n" +
			"Sec-WebSocket-Accept: " + acceptKey(r.Header.Get("Sec-WebSocket-Key")) + "\r\n\r\n"
	})
	hs, _, err := dialHandshake(t.Context(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeQuietly(t, hs.conn)
}

func TestDialKeepsBufferedBytes(t *testing.T) {
	url := rawServer(t, func(r *http.Request) string { return response(r, "") + "early-frame" })
	hs, _, err := dialHandshake(t.Context(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeQuietly(t, hs.conn)
	b := make([]byte, len("early-frame"))
	if _, err := io.ReadFull(hs.conn, b); err != nil || string(b) != "early-frame" {
		t.Fatalf("read %q, %v; want early-frame", b, err)
	}
}

func TestDialRejectsBadURLs(t *testing.T) {
	for _, u := range []string{"http://example.com/", "ws:///nohost", "ws://example.com/#frag", "::"} {
		if _, _, err := dialHandshake(t.Context(), u, nil); err == nil {
			t.Errorf("dial %q succeeded", u)
		}
	}
}

// silentListener accepts connections and never answers; accepted reports
// each accepted connection.
func silentListener(t *testing.T) (addr string, accepted <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closeQuietly(t, ln)
	ch := make(chan struct{}, 1)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			closeQuietly(t, c)
			ch <- struct{}{}
		}
	}()
	return ln.Addr().String(), ch
}

func TestDialContextCancellation(t *testing.T) {
	for _, scheme := range []string{"ws", "wss"} {
		t.Run(scheme+" cancel", func(t *testing.T) {
			addr, accepted := silentListener(t)
			ctx, cancel := context.WithCancel(t.Context())
			errc := make(chan error, 1)
			go func() { _, _, err := dialHandshake(ctx, scheme+"://"+addr, nil); errc <- err }()
			<-accepted
			cancel()
			select {
			case err := <-errc:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("err = %v, want context.Canceled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation did not abort the handshake")
			}
		})
		t.Run(scheme+" deadline", func(t *testing.T) {
			addr, _ := silentListener(t)
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			_, _, err := dialHandshake(ctx, scheme+"://"+addr, nil)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %v, want context.DeadlineExceeded", err)
			}
		})
	}
	t.Run("already canceled", func(t *testing.T) {
		addr, _ := silentListener(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, _, err := dialHandshake(ctx, "ws://"+addr, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

// newClientCert issues a client certificate from a fresh CA.
func newClientCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "vox-caster"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

func TestDialMutualTLS(t *testing.T) {
	cert, clientCAs := newClientCert(t)
	peer := make(chan string, 1)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer <- r.TLS.PeerCertificates[0].Subject.CommonName
		echoHandler(nil, nil)(w, r)
	}))
	ts.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs}
	ts.StartTLS()
	defer ts.Close()
	roots := x509.NewCertPool()
	roots.AddCert(ts.Certificate())

	t.Run("with client certificate", func(t *testing.T) {
		cfg := &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}}
		hs, _, err := dialHandshake(t.Context(), wsURL(ts.URL), &DialOptions{TLSConfig: cfg})
		if err != nil {
			t.Fatal(err)
		}
		closeQuietly(t, hs.conn)
		if cn := <-peer; cn != "vox-caster" {
			t.Errorf("server saw client %q", cn)
		}
		if cfg.ServerName != "" {
			t.Error("dial modified the caller's tls.Config")
		}
		assertEcho(t, hs.conn)
	})
	t.Run("without client certificate", func(t *testing.T) {
		hs, _, err := dialHandshake(t.Context(), wsURL(ts.URL), &DialOptions{TLSConfig: &tls.Config{RootCAs: roots}})
		if err == nil {
			_ = hs.conn.Close()
			t.Fatal("dial succeeded without a client certificate")
		}
	})
}
