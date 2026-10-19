package nwws

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
)

// MessageType is the type of a data message (RFC 6455 §5.6).
type MessageType int

const (
	// MessageText is a text message. Its payload is UTF-8, which is
	// checked on read and on write.
	MessageText = MessageType(opText)
	// MessageBinary is a binary message.
	MessageBinary = MessageType(opBinary)
)

func (t MessageType) String() string {
	switch t {
	case MessageText:
		return "text"
	case MessageBinary:
		return "binary"
	}
	return fmt.Sprintf("MessageType(%d)", int(t))
}

// ErrClosed is returned by Write and Close once the connection is closing
// or closed, and by Read when the connection ended without a close status.
var ErrClosed = errors.New("nwws: connection closed")

// Conn is a WebSocket connection, on the server or the client side. Read,
// Write and Close may be called from any goroutine.
//
// A goroutine of the connection reads frames: it reassembles messages,
// answers pings and takes part in the closing handshake. It holds each
// complete message until Read takes it, and reads no further frames
// meanwhile, so keep calling Read for as long as the connection is in use.
//
// Another goroutine writes every frame, so concurrent writers never
// interleave. Messages queued while a write is in progress are sent
// together in the next one.
type Conn struct {
	nc          net.Conn
	server      bool
	subprotocol string
	cfg         connConfig

	// Read side, owned by readLoop.
	fr        frameReader
	inflate   *inflater     // decompresses messages; nil without compression
	msgs      chan message  // complete messages for Read; closed when readLoop ends
	closeRcvd chan struct{} // closed when the peer's close frame arrives
	readDone  chan struct{}

	// Write side, owned by writeLoop.
	fw        frameWriter
	deflate   *deflater // compresses messages; nil without compression
	bw        *bufio.Writer
	wake      chan struct{} // signals queued frames; buffered
	closeSent chan struct{} // closed once a close frame is on the wire
	writeDone chan struct{}

	wmu     sync.Mutex
	queue   []*writeReq // data and close frames in order, guarded by wmu
	pongs   []*writeReq // pongs to send first, oldest first, guarded by wmu
	wclosed bool        // no more frames are accepted, guarded by wmu

	discard     chan struct{} // closed by Close: unread messages are dropped
	discardOnce sync.Once
	closed      chan struct{} // closed with the network connection
	closeOnce   sync.Once

	errMu sync.Mutex
	err   error // why the connection ended, guarded by errMu
}

// newConn wraps nc, a connection after the opening handshake, and starts
// its read and write goroutines.
func newConn(nc net.Conn, server bool, subprotocol string, cfg connConfig) *Conn {
	c := &Conn{
		nc:          nc,
		server:      server,
		subprotocol: subprotocol,
		cfg:         cfg,
		msgs:        make(chan message),
		closeRcvd:   make(chan struct{}),
		readDone:    make(chan struct{}),
		bw:          bufio.NewWriterSize(nc, writeBufferSize),
		wake:        make(chan struct{}, 1),
		closeSent:   make(chan struct{}),
		writeDone:   make(chan struct{}),
		discard:     make(chan struct{}),
		closed:      make(chan struct{}),
	}
	c.fr = frameReader{r: bufio.NewReader(nc), server: server}
	c.fw = frameWriter{w: c.bw, server: server}
	c.initCompression()
	go c.readLoop()
	go c.writeLoop()
	return c
}

// Accept upgrades an HTTP request to a WebSocket connection, performing
// the server side of the opening handshake (RFC 6455 §4.2). On failure it
// has already answered the request with an HTTP error status, and the
// error wraps ErrBadHandshake when the request was invalid or refused.
// The connection outlives the HTTP handler; close it when done.
func Accept(w http.ResponseWriter, r *http.Request, opts *AcceptOptions) (*Conn, error) {
	hs, err := acceptHandshake(w, r, opts)
	if err != nil {
		return nil, err
	}
	var co *ConnOptions
	if opts != nil {
		co = &opts.ConnOptions
	}
	return newConn(hs.conn, true, hs.subprotocol, co.configFor(hs.extensions, true)), nil
}

// Dial opens a WebSocket connection to a ws or wss URL, performing the
// client side of the opening handshake (RFC 6455 §4.1). Cancelling ctx
// aborts the dial and the handshake; it has no effect on the returned
// connection. The server's handshake response is returned whenever one
// was read, also when the server refused the handshake.
func Dial(ctx context.Context, url string, opts *DialOptions) (*Conn, *http.Response, error) {
	hs, resp, err := dialHandshake(ctx, url, opts)
	if err != nil {
		return nil, resp, err
	}
	var co *ConnOptions
	if opts != nil {
		co = &opts.ConnOptions
	}
	return newConn(hs.conn, false, hs.subprotocol, co.configFor(hs.extensions, false)), resp, nil
}

// Subprotocol returns the subprotocol agreed in the opening handshake, or
// "" if there is none.
func (c *Conn) Subprotocol() string { return c.subprotocol }

// closeNow closes the network connection at once. It records err as the
// reason the connection ended, unless a reason is already recorded.
func (c *Conn) closeNow(err error) {
	c.setErr(err)
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.nc.Close()
	})
}

func (c *Conn) setErr(err error) {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.err == nil {
		c.err = err
	}
}

// endErr returns why the connection ended.
func (c *Conn) endErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.err == nil {
		return ErrClosed
	}
	return c.err
}

// wait waits for the read and write goroutines to end.
func (c *Conn) wait() {
	<-c.readDone
	<-c.writeDone
}
