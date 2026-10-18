package nwws

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newUpgradeRequest returns a valid RFC 6455 §4.1 opening handshake request
// as a server handler would see it.
func newUpgradeRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://example.com/chat", nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	return r
}

func TestAcceptKeyRFCExample(t *testing.T) {
	// RFC 6455 §1.3 worked example.
	got := acceptKey("dGhlIHNhbXBsZSBub25jZQ==")
	if want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="; got != want {
		t.Fatalf("acceptKey = %q, want %q", got, want)
	}
}

// TestAcceptRejections drives acceptHandshake with a ResponseRecorder, which
// cannot be hijacked: a correct status code therefore also shows that the
// rejection happened before any hijack attempt.
func TestAcceptRejections(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(r *http.Request)
		opts    *AcceptOptions
		status  int
		headers map[string]string
	}{
		{"RFC 6455 §4.1 method must be GET", func(r *http.Request) { r.Method = http.MethodPost },
			nil, http.StatusMethodNotAllowed, map[string]string{"Allow": "GET"}},
		{"RFC 6455 §4.1 HTTP/1.1 required", func(r *http.Request) { r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/1.0", 1, 0 },
			nil, http.StatusHTTPVersionNotSupported, nil},
		{"missing Connection", func(r *http.Request) { r.Header.Del("Connection") },
			nil, http.StatusUpgradeRequired, map[string]string{"Upgrade": "websocket", "Connection": "Upgrade"}},
		{"Connection without upgrade token", func(r *http.Request) { r.Header.Set("Connection", "keep-alive, upgraded") },
			nil, http.StatusUpgradeRequired, nil},
		{"missing Upgrade", func(r *http.Request) { r.Header.Del("Upgrade") },
			nil, http.StatusUpgradeRequired, map[string]string{"Upgrade": "websocket"}},
		{"Upgrade not websocket", func(r *http.Request) { r.Header.Set("Upgrade", "h2c") },
			nil, http.StatusUpgradeRequired, nil},
		{"RFC 6455 §4.4 unsupported version", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", "8") },
			nil, http.StatusUpgradeRequired, map[string]string{"Sec-WebSocket-Version": "13"}},
		{"missing version", func(r *http.Request) { r.Header.Del("Sec-WebSocket-Version") },
			nil, http.StatusUpgradeRequired, map[string]string{"Sec-WebSocket-Version": "13"}},
		{"missing key", func(r *http.Request) { r.Header.Del("Sec-WebSocket-Key") },
			nil, http.StatusBadRequest, nil},
		{"key not base64", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "not base64 at all!!!!!!!") },
			nil, http.StatusBadRequest, nil},
		{"key of 15 bytes", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "AAAAAAAAAAAAAAAAAAAA") },
			nil, http.StatusBadRequest, nil},
		{"key of 17 bytes", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "AAAAAAAAAAAAAAAAAAAAAAA=") },
			nil, http.StatusBadRequest, nil},
		{"two keys", func(r *http.Request) { r.Header.Add("Sec-WebSocket-Key", "AAAAAAAAAAAAAAAAAAAAAA==") },
			nil, http.StatusBadRequest, nil},
		{"cross-origin rejected by default", func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") },
			nil, http.StatusForbidden, nil},
		{"null origin rejected by default", func(r *http.Request) { r.Header.Set("Origin", "null") },
			nil, http.StatusForbidden, nil},
		{"custom origin check rejects", func(r *http.Request) {},
			&AcceptOptions{CheckOrigin: func(*http.Request) bool { return false }}, http.StatusForbidden, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newUpgradeRequest()
			tt.modify(r)
			w := httptest.NewRecorder()
			hs, err := acceptHandshake(w, r, tt.opts)
			if hs != nil || !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("acceptHandshake = %v, %v; want nil, ErrBadHandshake", hs, err)
			}
			if w.Code != tt.status {
				t.Fatalf("status = %d, want %d", w.Code, tt.status)
			}
			for k, v := range tt.headers {
				if got := w.Header().Get(k); got != v {
					t.Errorf("header %s = %q, want %q", k, got, v)
				}
			}
		})
	}
}

func TestCheckRequestAccepts(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(r *http.Request)
		opts        *AcceptOptions
		subprotocol string
	}{
		{"minimal", func(r *http.Request) {}, nil, ""},
		{"tokens are case-insensitive lists", func(r *http.Request) {
			r.Header.Set("Connection", "keep-alive, UPGRADE")
			r.Header.Set("Upgrade", "WebSocket")
		}, nil, ""},
		{"same origin", func(r *http.Request) { r.Header.Set("Origin", "https://EXAMPLE.com") }, nil, ""},
		{"custom origin check accepts cross-origin", func(r *http.Request) { r.Header.Set("Origin", "https://other.example") },
			&AcceptOptions{CheckOrigin: func(*http.Request) bool { return true }}, ""},
		{"server preference wins", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "a, b") },
			&AcceptOptions{Subprotocols: []string{"b", "a"}}, "b"},
		{"subprotocols across header lines", func(r *http.Request) {
			r.Header.Add("Sec-WebSocket-Protocol", "x")
			r.Header.Add("Sec-WebSocket-Protocol", "y")
		}, &AcceptOptions{Subprotocols: []string{"y"}}, "y"},
		{"no common subprotocol", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "c") },
			&AcceptOptions{Subprotocols: []string{"a"}}, ""},
		{"subprotocols compare exactly", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "A") },
			&AcceptOptions{Subprotocols: []string{"a"}}, ""},
		{"extension offers ignored", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Extensions", "permessage-deflate") }, nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newUpgradeRequest()
			tt.modify(r)
			plan, rej := checkRequest(r, tt.opts)
			if rej != nil {
				t.Fatalf("checkRequest rejected: %v", rej)
			}
			if plan.accept != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
				t.Errorf("accept = %q", plan.accept)
			}
			if plan.subprotocol != tt.subprotocol {
				t.Errorf("subprotocol = %q, want %q", plan.subprotocol, tt.subprotocol)
			}
			if plan.extensions != "" {
				t.Errorf("extensions = %q, want none", plan.extensions)
			}
		})
	}
}

func TestAcceptNotHijackable(t *testing.T) {
	w := httptest.NewRecorder()
	hs, err := acceptHandshake(w, newUpgradeRequest(), nil)
	if hs != nil || err == nil {
		t.Fatalf("acceptHandshake = %v, %v; want an error", hs, err)
	}
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// TestAcceptKeepsBufferedBytes sends the request and the first frame bytes
// in one write, so the server's bufio.Reader reads past the request.
func TestAcceptKeepsBufferedBytes(t *testing.T) {
	got := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hs, err := acceptHandshake(w, r, nil)
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		defer func() { _ = hs.conn.Close() }()
		b := make([]byte, len("early-frame"))
		_, err = io.ReadFull(hs.conn, b)
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		got <- string(b)
	}))
	defer ts.Close()

	c, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	closeQuietly(t, c)
	req := "GET / HTTP/1.1\r\nHost: " + ts.Listener.Addr().String() + "\r\n" +
		"Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\nearly-frame"
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	select {
	case s := <-got:
		if s != "early-frame" {
			t.Fatalf("server read %q, want %q", s, "early-frame")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not read the buffered bytes")
	}
}

func TestAcceptResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Set-Cookie", "session=1")
		w.Header().Set("Content-Length", "99")
		hs, err := acceptHandshake(w, r, &AcceptOptions{Subprotocols: []string{"chat"}})
		if err == nil {
			_ = hs.conn.Close()
		}
	}))
	defer ts.Close()

	c, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	closeQuietly(t, c)
	req := "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Protocol: chat\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n"
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"Upgrade":                  "websocket",
		"Connection":               "Upgrade",
		"Sec-WebSocket-Accept":     "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
		"Sec-WebSocket-Protocol":   "chat",
		"Sec-WebSocket-Extensions": "",
		"Set-Cookie":               "session=1",
		"Content-Length":           "",
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	for k, v := range want {
		if got := strings.Join(resp.Header.Values(k), ","); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
}

// TestAcceptClearsServerDeadlines checks that the server's ReadTimeout does
// not end the upgraded connection.
func TestAcceptClearsServerDeadlines(t *testing.T) {
	got := make(chan error, 1)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hs, err := acceptHandshake(w, r, nil)
		if err != nil {
			got <- err
			return
		}
		defer func() { _ = hs.conn.Close() }()
		_, err = io.ReadFull(hs.conn, make([]byte, 1))
		got <- err
	}))
	ts.Config.ReadTimeout = 20 * time.Millisecond
	ts.Start()
	defer ts.Close()

	hs, _, err := dialHandshake(t.Context(), "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hs.conn.Close() }()
	time.Sleep(100 * time.Millisecond) // well past the server's ReadTimeout
	if _, err := hs.conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := <-got; err != nil {
		t.Fatalf("server read after ReadTimeout: %v", err)
	}
}
