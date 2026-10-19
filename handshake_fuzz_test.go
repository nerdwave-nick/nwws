package nwws

import (
	"bufio"
	"bytes"
	"errors"
	"net/http"
	"slices"
	"testing"
)

const fuzzKey = "dGhlIHNhbXBsZSBub25jZQ=="

// FuzzAcceptRequest parses arbitrary bytes as a handshake request. A request
// the server accepts must yield a 101 response that a client offering the
// same subprotocols accepts; a refused one must carry a rejection status.
func FuzzAcceptRequest(f *testing.F) {
	f.Add([]byte("GET /chat HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + fuzzKey + "\r\nSec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Protocol: chat, superchat\r\nOrigin: http://example.com\r\n\r\n"))
	f.Add([]byte("GET / HTTP/1.1\r\nHost: a\r\nUpgrade: WebSocket\r\nConnection: keep-alive, upgrade\r\n" +
		"Sec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==\r\nSec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\r\n"))
	f.Add([]byte("GET / HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 8\r\n\r\n"))
	f.Add([]byte("POST / HTTP/1.0\r\nHost: a\r\n\r\n"))
	f.Add([]byte("GET / HTTP/1.1\r\nHost: [::1]:443\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + fuzzKey + "\r\nSec-WebSocket-Version: 13\r\nOrigin: https://[::1]\r\n\r\n"))
	opts := &AcceptOptions{Subprotocols: []string{"superchat", "chat"}}
	rejections := []int{http.StatusBadRequest, http.StatusForbidden, http.StatusMethodNotAllowed,
		http.StatusUpgradeRequired, http.StatusHTTPVersionNotSupported}

	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(data)))
		if err != nil {
			return
		}
		plan, rej := checkRequest(r, opts)
		if rej != nil {
			if !slices.Contains(rejections, rej.status) || !errors.Is(rej, ErrBadHandshake) {
				t.Fatalf("rejection %d %v", rej.status, rej)
			}
			return
		}
		key := r.Header.Get("Sec-WebSocket-Key")
		if r.Method != http.MethodGet || r.Header.Get("Sec-WebSocket-Version") != "13" || !validKey(key) {
			t.Fatalf("accepted an invalid request: %q", data)
		}
		offered := headerList(r.Header, "Sec-WebSocket-Protocol")
		if plan.subprotocol != "" && !slices.Contains(offered, plan.subprotocol) {
			t.Fatalf("selected %q, offered %q", plan.subprotocol, offered)
		}
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(switchingProtocols(nil, plan))), nil)
		if err != nil {
			t.Fatalf("server response unreadable: %v", err)
		}
		if err := checkResponse(resp, key, &DialOptions{Subprotocols: offered}); err != nil {
			t.Fatalf("client refused the server's response: %v", err)
		}
	})
}

// FuzzDialResponse parses arbitrary bytes as a handshake response. A response
// the client accepts must be a 101 with the right accept value and an
// offered subprotocol, if any.
func FuzzDialResponse(f *testing.F) {
	f.Add([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey(fuzzKey) + "\r\nSec-WebSocket-Protocol: chat\r\n\r\n"))
	f.Add([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: WEBSOCKET\r\nConnection: upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey(fuzzKey) + "\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n"))
	f.Add([]byte("HTTP/1.1 403 Forbidden\r\nContent-Length: 3\r\n\r\nno\n"))
	opts := &DialOptions{Subprotocols: []string{"chat", "superchat"}}

	f.Fuzz(func(t *testing.T, data []byte) {
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(data)), nil)
		if err != nil {
			return
		}
		if err := checkResponse(resp, fuzzKey, opts); err != nil {
			if !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("error %v does not wrap ErrBadHandshake", err)
			}
			return
		}
		if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Accept") != acceptKey(fuzzKey) {
			t.Fatalf("accepted an invalid response: %q", data)
		}
		if p := resp.Header.Get("Sec-WebSocket-Protocol"); p != "" && !slices.Contains(opts.Subprotocols, p) {
			t.Fatalf("accepted unoffered subprotocol %q", p)
		}
		ext, err := opts.checkExtensions(opts.extensionOffer(), resp.Header.Values("Sec-WebSocket-Extensions"))
		if (err == nil) != (len(resp.Header.Values("Sec-WebSocket-Extensions")) == 0) || ext != "" {
			t.Fatalf("extensions %q accepted as %q, %v", resp.Header.Values("Sec-WebSocket-Extensions"), ext, err)
		}
	})
}
