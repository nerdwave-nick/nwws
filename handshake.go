package nwws

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// websocketGUID is the fixed GUID of RFC 6455 §1.3 that the server appends to
// the client's key to compute Sec-WebSocket-Accept.
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// ErrBadHandshake reports that the opening handshake (RFC 6455 §4) failed
// because a peer violated or refused it. Errors from such failures wrap it,
// so callers can test for it with [errors.Is].
var ErrBadHandshake = errors.New("nwws: bad handshake")

func handshakeErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrBadHandshake, fmt.Sprintf(format, args...))
}

// handshake is the outcome of a completed opening handshake: an upgraded
// connection ready for framing, plus what the two sides agreed on.
type handshake struct {
	// conn is the upgraded connection. Bytes the handshake read past the
	// HTTP request or response are returned by its first reads.
	conn net.Conn
	// subprotocol is the agreed Sec-WebSocket-Protocol, or empty.
	subprotocol string
	// extensions is the agreed Sec-WebSocket-Extensions value, or empty.
	extensions string
}

// acceptKey computes Sec-WebSocket-Accept for a Sec-WebSocket-Key
// (RFC 6455 §4.2.2, step 5.4).
func acceptKey(key string) string {
	sum := sha1.Sum([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// validKey reports whether key is the base64 encoding of 16 bytes
// (RFC 6455 §4.1, item 7).
func validKey(key string) bool {
	if len(key) != base64.StdEncoding.EncodedLen(16) {
		return false
	}
	b, err := base64.StdEncoding.DecodeString(key)
	return err == nil && len(b) == 16
}

// headerList returns the non-empty elements of the comma-separated list
// header name, across all of its header lines (RFC 9110 §5.6.1).
func headerList(h http.Header, name string) []string {
	var list []string
	for _, v := range h.Values(name) {
		for e := range strings.SplitSeq(v, ",") {
			if e = strings.Trim(e, " \t"); e != "" {
				list = append(list, e)
			}
		}
	}
	return list
}

// hasToken reports whether the list header name contains token, compared
// case-insensitively as RFC 6455 requires for Connection and Upgrade.
func hasToken(h http.Header, name, token string) bool {
	for _, e := range headerList(h, name) {
		if strings.EqualFold(e, token) {
			return true
		}
	}
	return false
}

// bufferedConn is a net.Conn whose reads first drain bytes read ahead by
// the handshake's bufio.Reader. Like net.Conn reads in this package, it is
// read by one goroutine at a time.
type bufferedConn struct {
	net.Conn
	buf []byte
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if len(c.buf) == 0 {
		return c.Conn.Read(p)
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

// withBuffered returns c, wrapped so that bytes already buffered in r are
// read first. It returns c itself when nothing is buffered.
func withBuffered(c net.Conn, r *bufio.Reader) net.Conn {
	n := r.Buffered()
	if n == 0 {
		return c
	}
	b, _ := r.Peek(n) // cannot fail: n bytes are buffered
	return &bufferedConn{Conn: c, buf: bytes.Clone(b)}
}
