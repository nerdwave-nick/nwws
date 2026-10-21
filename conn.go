package nwws

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
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
// or closed, by CloseNow when the connection is already closed, and by Read
// when the connection ended without a close status, as after CloseNow.
var ErrClosed = errors.New("nwws: connection closed")

// Conn is a WebSocket connection, on the server or the client side. Read,
// Write, Close and CloseNow may be called from any goroutine.
//
// While no message waits to be read, Read reads frames itself, in the
// calling goroutine: it reassembles the next message, answers pings and
// takes part in the closing handshake. When no Read has run for a couple
// of milliseconds, or after a large message, a goroutine of the
// connection takes over: it reads the next message while the application
// handles the last, until a Read waits for a small one. It queues
// complete messages for Read and keeps reading while they take less than
// the read-ahead budget,
// [ConnOptions.ReadBuffer]. Once the budget is full, it reads no further
// frames until Read makes room, so keep calling Read for as long as the
// connection is in use. Compressed connections, and those whose read
// deadline cannot be set, always read frames in that goroutine.
//
// Frames are written one at a time, so concurrent writers never
// interleave. A Write on an idle connection writes its frame itself;
// messages queued while a write is in progress are sent together in the
// next one, by another goroutine of the connection, which also pings the
// peer. An idle connection holds no write buffer.
type Conn struct {
	nc          net.Conn
	server      bool
	subprotocol string
	cfg         connConfig

	// Read side, owned by the holder of the reader role; see role.
	fr        frameReader
	br        *bufio.Reader // the network connection, read by fr
	rs        readState
	direct    bool          // Read may read frames itself; see newConn
	inflate   *inflater     // decompresses messages; nil without compression
	closeRcvd chan struct{} // closed when the peer's close frame arrives
	readDone  chan struct{}

	// Write side, owned by writeLoop or a direct Write; see writing.
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
	writing bool        // writeLoop or a Write owns the write side, guarded by wmu

	// The pooled buffer of bw and fw, held only while writing or while
	// frames wait to be written; guarded by wmu.
	wbuf *writeBuffer

	// Complete messages read ahead for Read, guarded by rmu.
	rmu        sync.Mutex
	rq         msgQueue
	readers    int           // Reads waiting for a message
	rwait      int           // waitNone, or what readLoop waits for Read to do
	rend       bool          // the read side has ended: no more messages follow
	discarding atomic.Bool   // Close has started: messages are dropped; set under rmu
	pooling    atomic.Bool   // ReadInto was called: message memory comes from the pool
	avail      chan struct{} // wakes a waiting Read; buffered
	room       chan struct{} // wakes readLoop waiting for Read; buffered
	role       int           // who reads frames: roleFree, roleLoop, roleRead or roleEnded
	takeover   *time.Timer   // gives a free reader role to the read goroutine
	armed      bool          // takeover is armed
	freed      uint64        // counts the times the reader role was freed
	seen       uint64        // freed when takeover was armed or last fired

	// Interrupting the network read of a Read whose ctx is done; see watch.
	imu         sync.Mutex
	idone       <-chan struct{} // Done of the ctx with an AfterFunc registered
	istop       func() bool     // unregisters it
	ireading    bool            // a Read reads frames with idone
	interrupted bool            // the read deadline was set to interrupt it

	closed    chan struct{} // closed with the network connection
	closeOnce sync.Once

	errMu sync.Mutex
	err   error // why the connection ended, guarded by errMu

	live *liveness // pings and the pong timeout; nil when off
}

// newConn wraps nc, a connection after the opening handshake, and starts
// its write goroutine. The first Read reads frames itself, or, after the
// takeover delay without one, the read goroutine. Without compression and
// with read deadlines, which interrupt a Read whose ctx is done, Read
// reads frames itself; otherwise only the read goroutine does.
func newConn(nc net.Conn, server bool, subprotocol string, cfg connConfig) *Conn {
	if cfg.readBuffer <= 0 {
		cfg.readBuffer = DefaultReadBuffer
	}
	c := &Conn{
		nc:          nc,
		server:      server,
		subprotocol: subprotocol,
		cfg:         cfg,
		closeRcvd:   make(chan struct{}),
		readDone:    make(chan struct{}),
		wake:        make(chan struct{}, 1),
		closeSent:   make(chan struct{}),
		writeDone:   make(chan struct{}),
		avail:       make(chan struct{}, 1),
		room:        make(chan struct{}, 1),
		closed:      make(chan struct{}),
	}
	c.live = newLiveness(c)
	c.br = bufio.NewReader(c.live.reader(nc))
	c.fr = frameReader{r: c.br, server: server}
	c.fw = frameWriter{server: server} // writes to wbuf; see takeWriteBuffer
	c.initCompression()
	c.direct = c.inflate == nil && nc.SetReadDeadline(time.Time{}) == nil
	c.rmu.Lock()
	if c.direct {
		c.freeRole()
	} else {
		c.startLoop()
	}
	c.rmu.Unlock()
	c.live.start()
	go c.writeLoop()
	return c
}

// Accept upgrades an HTTP request to a WebSocket connection, performing
// the server side of the opening handshake (RFC 6455 §4.2). On failure it
// has already answered the request with an HTTP error status, and the
// error wraps ErrBadHandshake when the request was invalid or refused.
// Invalid options, such as a compression level out of range, are answered
// with 500 Internal Server Error without upgrading. The connection
// outlives the HTTP handler; close it when done.
func Accept(w http.ResponseWriter, r *http.Request, opts *AcceptOptions) (*Conn, error) {
	var co *ConnOptions
	if opts != nil {
		co = &opts.ConnOptions
	}
	if err := co.check(); err != nil {
		http.Error(w, "nwws: invalid server options", http.StatusInternalServerError)
		return nil, err
	}
	hs, err := acceptHandshake(w, r, opts)
	if err != nil {
		return nil, err
	}
	return newConn(hs.conn, true, hs.subprotocol, co.configFor(hs.extensions, true)), nil
}

// Dial opens a WebSocket connection to a ws or wss URL, performing the
// client side of the opening handshake (RFC 6455 §4.1). Cancelling ctx
// aborts the dial and the handshake; it has no effect on the returned
// connection. The server's handshake response is returned whenever one
// was read, also when the server refused the handshake. Invalid options,
// such as a compression level out of range, fail Dial before it dials.
func Dial(ctx context.Context, url string, opts *DialOptions) (*Conn, *http.Response, error) {
	var co *ConnOptions
	if opts != nil {
		co = &opts.ConnOptions
	}
	if err := co.check(); err != nil {
		return nil, nil, err
	}
	hs, resp, err := dialHandshake(ctx, url, opts)
	if err != nil {
		return nil, resp, err
	}
	return newConn(hs.conn, false, hs.subprotocol, co.configFor(hs.extensions, false)), resp, nil
}

// Subprotocol returns the subprotocol agreed in the opening handshake, or
// "" if there is none.
func (c *Conn) Subprotocol() string { return c.subprotocol }

// closeNow closes the network connection at once. It records err as the
// reason the connection ended, unless a reason is already recorded, and
// reports whether this call closed the connection. When nobody reads
// frames, it starts the read goroutine, which then ends the read side
// without waiting for the takeover delay.
func (c *Conn) closeNow(err error) (closed bool) {
	c.setErr(err)
	c.closeOnce.Do(func() {
		closed = true
		close(c.closed)
		_ = c.nc.Close()
	})
	if closed {
		c.rmu.Lock()
		if c.role == roleFree {
			c.startLoop()
		}
		c.rmu.Unlock()
	}
	return closed
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

// wait waits for the goroutines of the connection to end.
func (c *Conn) wait() {
	<-c.readDone
	<-c.writeDone
}
