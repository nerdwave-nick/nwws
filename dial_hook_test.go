package nwws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// unixServer starts an httptest server, plain or TLS, that listens on a Unix
// socket. It returns the server and the socket path.
func unixServer(t *testing.T, h http.Handler, useTLS bool) (*httptest.Server, string) {
	t.Helper()
	// A short directory keeps the socket path within the 108-byte limit.
	dir, err := os.MkdirTemp("", "nwws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "s")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(h)
	_ = ts.Listener.Close()
	ts.Listener = ln
	if useTLS {
		ts.StartTLS()
	} else {
		ts.Start()
	}
	t.Cleanup(ts.Close)
	return ts, path
}

type dialCall struct{ network, addr string }

// unixDialer returns a NetDialContext hook that records each call and dials
// the Unix socket at path instead of the requested address.
func unixDialer(path string, calls chan<- dialCall) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		calls <- dialCall{network, addr}
		var d net.Dialer
		return d.DialContext(ctx, "unix", path)
	}
}

func TestDialNetDialContextUnixSocket(t *testing.T) {
	for _, scheme := range []string{"ws", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			seen := make(chan *http.Request, 1)
			ts, path := unixServer(t, echoHandler(nil, seen), scheme == "wss")
			calls := make(chan dialCall, 1)
			opts := &DialOptions{NetDialContext: unixDialer(path, calls)}
			if scheme == "wss" {
				roots := x509.NewCertPool()
				roots.AddCert(ts.Certificate())
				// httptest's certificate is valid for example.com.
				opts.TLSConfig = &tls.Config{RootCAs: roots, ServerName: "example.com"}
			}
			hs, _, err := dialHandshake(t.Context(), scheme+"://nwws.invalid/sock", opts)
			if err != nil {
				t.Fatal(err)
			}
			closeQuietly(t, hs.conn)
			port := map[string]string{"ws": "80", "wss": "443"}[scheme]
			if len(calls) == 0 {
				t.Fatal("the hook was not called")
			}
			if c := <-calls; c.network != "tcp" || c.addr != "nwws.invalid:"+port {
				t.Errorf("hook called with %q %q, want tcp nwws.invalid:%s", c.network, c.addr, port)
			}
			r := <-seen
			if r.Host != "nwws.invalid" || r.URL.Path != "/sock" {
				t.Errorf("server saw Host %q, path %q", r.Host, r.URL.Path)
			}
			if (r.TLS != nil) != (scheme == "wss") {
				t.Errorf("server saw TLS %v", r.TLS != nil)
			}
			assertEcho(t, hs.conn)
		})
	}
}

type ctxKey struct{}

func TestDialNetDialContextReceivesContext(t *testing.T) {
	t.Run("value and cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.WithValue(t.Context(), ctxKey{}, "marker"))
		entered := make(chan any, 1)
		opts := &DialOptions{NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			entered <- ctx.Value(ctxKey{})
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		errc := make(chan error, 1)
		go func() { _, _, err := dialHandshake(ctx, "ws://example.invalid", opts); errc <- err }()
		select {
		case v := <-entered:
			if v != "marker" {
				t.Errorf("hook context value %v, want the caller's", v)
			}
		case err := <-errc:
			t.Fatalf("dial returned %v without calling the hook", err)
		}
		cancel()
		select {
		case err := <-errc:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancellation did not reach the hook")
		}
	})
	t.Run("hook error", func(t *testing.T) {
		boom := errors.New("boom")
		opts := &DialOptions{NetDialContext: func(context.Context, string, string) (net.Conn, error) { return nil, boom }}
		if _, _, err := dialHandshake(t.Context(), "ws://example.invalid", opts); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the hook's error", err)
		}
	})
	t.Run("hook returns no connection", func(t *testing.T) {
		opts := &DialOptions{NetDialContext: func(context.Context, string, string) (net.Conn, error) { return nil, nil }}
		if _, _, err := dialHandshake(t.Context(), "ws://example.invalid", opts); err == nil {
			t.Fatal("dial succeeded without a connection")
		}
	})
	t.Run("handshake still bounded by ctx", func(t *testing.T) {
		addr, _ := silentListener(t)
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		opts := &DialOptions{NetDialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		}}
		if _, _, err := dialHandshake(ctx, "ws://example.invalid", opts); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
	})
}

// TestDialALPNHTTP11 dials a TLS server that prefers h2 over http/1.1. The
// client must offer only http/1.1, whatever the caller's NextProtos, or the
// server would select h2 and the upgrade could not work.
func TestDialALPNHTTP11(t *testing.T) {
	seen := make(chan *http.Request, 1)
	ts := httptest.NewUnstartedServer(echoHandler(nil, seen))
	ts.EnableHTTP2 = true
	ts.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
	ts.StartTLS()
	defer ts.Close()
	roots := x509.NewCertPool()
	roots.AddCert(ts.Certificate())

	for _, protos := range [][]string{nil, {"h2"}, {"h2", "http/1.1"}, {"spdy/3", "acme-tls/1"}} {
		t.Run(strings.Join(protos, ","), func(t *testing.T) {
			cfg := &tls.Config{RootCAs: roots, NextProtos: slices.Clone(protos)}
			hs, _, err := dialHandshake(t.Context(), wsURL(ts.URL), &DialOptions{TLSConfig: cfg})
			if err != nil {
				t.Fatal(err)
			}
			closeQuietly(t, hs.conn)
			r := <-seen
			if r.TLS.NegotiatedProtocol != "http/1.1" || r.ProtoMajor != 1 {
				t.Errorf("server negotiated %q over %s", r.TLS.NegotiatedProtocol, r.Proto)
			}
			if !slices.Equal(cfg.NextProtos, protos) || cfg.ServerName != "" {
				t.Errorf("dial modified the caller's tls.Config: NextProtos %q, ServerName %q", cfg.NextProtos, cfg.ServerName)
			}
			assertEcho(t, hs.conn)
		})
	}
}

func TestDialTLSConfigNextProtos(t *testing.T) {
	caller := &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
	for _, opts := range []*DialOptions{nil, {}, {TLSConfig: caller}} {
		cfg := opts.tlsConfig("example.com")
		if !slices.Equal(cfg.NextProtos, []string{"http/1.1"}) {
			t.Errorf("NextProtos = %q, want [http/1.1]", cfg.NextProtos)
		}
	}
	if !slices.Equal(caller.NextProtos, []string{"h2", "http/1.1"}) {
		t.Errorf("caller's NextProtos became %q", caller.NextProtos)
	}
}
