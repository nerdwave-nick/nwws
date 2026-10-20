package compare

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// serve starts a loopback HTTP server that upgrades every request with
// lib and sends each server conn on the returned channel, which buffers
// up to backlog conns. It returns the server's ws URL.
func serve(tb testing.TB, lib library, backlog int) (string, <-chan conn) {
	tb.Helper()
	accepted := make(chan conn, backlog)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := lib.upgrade(w, r)
		if err != nil {
			return // the library has answered the request
		}
		accepted <- c
	}))
	tb.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), accepted
}

// accept returns the next conn from accepted, or fails after the
// handshake timeout.
func accept(tb testing.TB, accepted <-chan conn) conn {
	tb.Helper()
	select {
	case c := <-accepted:
		return c
	case <-time.After(handshakeTimeout):
		tb.Fatal("no connection accepted")
		return nil
	}
}

// pair returns both sides of a connection of lib.
func pair(tb testing.TB, lib library) (client, server conn) {
	tb.Helper()
	url, accepted := serve(tb, lib, 1)
	client, err := lib.dial(url)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(client.close)
	server = accept(tb, accepted)
	tb.Cleanup(server.close)
	return client, server
}

// echo sends every message that c reads back to the peer, until reading
// or writing fails. It then closes c, so that the peer fails at once
// instead of waiting for its deadline.
func echo(c conn) {
	defer c.close()
	for {
		p, err := c.readReuse()
		if err != nil {
			return
		}
		if err := c.write(p); err != nil {
			return
		}
	}
}

// drain reads n messages of size bytes from c, discarding them. On
// failure it closes c, so that the peer fails at once.
func drain(c conn, n, size int) error {
	for i := range n {
		p, err := c.readReuse()
		if err == nil && len(p) != size {
			err = fmt.Errorf("message %d has %d bytes, want %d", i, len(p), size)
		}
		if err != nil {
			c.close()
			return err
		}
	}
	return nil
}

// rawDial opens a WebSocket connection to url without a WebSocket
// library: it sends a minimal client handshake, checks for 101 Switching
// Protocols, and returns the network connection, with a deadline.
func rawDial(tb testing.TB, url string) net.Conn {
	tb.Helper()
	host := strings.TrimPrefix(url, "ws://")
	nc, err := net.DialTimeout("tcp", host, handshakeTimeout)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = nc.Close() })
	_ = nc.SetDeadline(time.Now().Add(opTimeout()))
	_, err = fmt.Fprintf(nc, "GET / HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", host)
	if err != nil {
		tb.Fatal(err)
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		tb.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		tb.Fatalf("handshake: %s", resp.Status)
	}
	if br.Buffered() != 0 {
		tb.Fatalf("server sent %d bytes after the handshake", br.Buffered())
	}
	return nc
}

// maskedFrame returns a single masked binary frame of n zero bytes, as a
// client sends it.
func maskedFrame(n int) []byte {
	frame := []byte{0x82} // FIN, binary
	switch {
	case n < 126:
		frame = append(frame, 0x80|byte(n))
	case n <= 0xffff:
		frame = append(frame, 0x80|126)
		frame = binary.BigEndian.AppendUint16(frame, uint16(n))
	default:
		frame = append(frame, 0x80|127)
		frame = binary.BigEndian.AppendUint64(frame, uint64(n))
	}
	key := [4]byte{0x12, 0x34, 0x56, 0x78}
	frame = append(frame, key[:]...)
	for i := range n {
		frame = append(frame, key[i%4]) // 0 XOR key
	}
	return frame
}
