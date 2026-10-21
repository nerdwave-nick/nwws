package nwws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

var (
	errMessageTooLarge = errors.New("nwws: message exceeds the read limit")
	errInvalidUTF8     = errors.New("nwws: text is not valid UTF-8")
)

// message is a complete data message, or a violation by the peer that
// Read reports in order, after the messages received before it.
type message struct {
	typ   MessageType
	data  []byte
	fail  *protocolError
	inBuf bool // data is the buffer of the ReadInto that read it
}

// protocolError is a violation of RFC 6455 by the peer, for which the
// connection is failed with code (RFC 6455 §7.1.7).
type protocolError struct {
	code StatusCode
	err  error
}

func (e *protocolError) Error() string {
	return fmt.Sprintf("nwws: protocol violation, closing with %v: %v", e.code, e.err)
}

func (e *protocolError) Unwrap() error { return e.err }

func protocolErrorf(code StatusCode, format string, args ...any) *protocolError {
	return &protocolError{code: code, err: fmt.Errorf(format, args...)}
}

// Read returns the next data message. Cancelling ctx, or its deadline,
// makes Read return the context's error; the connection stays usable and
// the next message is kept for a later Read, also when Read had read part
// of it.
//
// While no message has been read ahead, Read reads frames itself, in the
// calling goroutine, and answers the peer's pings meanwhile; see [Conn].
// It interrupts its network read through a read deadline when ctx is
// done, so a connection supplied by [DialOptions.NetDialContext] should
// support read deadlines: without them, the connection reads every
// message in a goroutine of its own and hands it over to Read, which
// takes longer.
//
// Once the connection has ended, Read returns why: a *[CloseError] with the
// peer's close code and reason, a *CloseError with StatusAbnormalClosure
// when the connection was lost without a close frame, ErrClosed after
// [Conn.CloseNow], or another error when this side failed the connection.
//
// A protocol violation by the peer is reported in order, after the
// messages received before it, and its close frame is queued before Read
// returns, so it follows what the application wrote in reply to those
// messages. If Read is not called within the close timeout, the connection
// is failed anyway.
func (c *Conn) Read(ctx context.Context) (MessageType, []byte, error) {
	m, err := c.nextData(ctx, nil, false)
	if err != nil {
		return 0, nil, err
	}
	return m.typ, owned(m.data), nil
}

// nextData returns the next data message for Read and ReadInto, or why
// there is none. A violation is returned as the error. A ReadInto, with
// into, passes its buffer.
func (c *Conn) nextData(ctx context.Context, buf []byte, into bool) (message, error) {
	m, err := c.next(ctx, buf, into)
	switch {
	case err != nil:
		return message{}, err
	case m.fail != nil:
		// Queue the failure's close frame before Read returns, so a
		// Close called after Read cannot replace its code.
		_, _ = c.queueClose(closePayload(m.fail.code, ""))
		return message{}, m.fail
	}
	return m, nil
}

// readLoop reads frames while the read goroutine holds the reader role:
// it queues messages for Read and reads ahead within the budget. It gives
// the role to a Read that waits for a message, and once the connection
// ends it records why, so that Read returns it, and completes the closing
// handshake.
func (c *Conn) readLoop() {
	for {
		m, got, err := c.readOne()
		switch {
		case err != nil:
			c.endRead(err, false)
			return
		case !got:
			continue
		case !c.deliver(m):
			c.endRead(net.ErrClosed, false)
			return
		case c.handOff(m):
			return
		}
	}
}

// endRead ends the read side with err: the peer's close frame as a
// *CloseError, a violation as a *protocolError, or a network error. It
// records why the connection ended and completes the closing handshake.
// A Read that holds the reader role calls it with taken: it reports a
// violation itself, and the rest of the closing handshake runs in a
// goroutine of its own.
func (c *Conn) endRead(err error, taken bool) {
	var ce *CloseError
	var pe *protocolError
	peerClosed := errors.As(err, &ce)
	failed := errors.As(err, &pe)
	if !peerClosed && !failed {
		err = &CloseError{Code: StatusAbnormalClosure, err: err}
	}
	c.setErr(err)
	// Our close frame is queued before Read learns why the connection
	// ended, so a Close called after Read cannot replace its code. Queueing
	// fails when our close frame is already queued.
	switch {
	case peerClosed:
		// The peer started the closing handshake or answered ours.
		var echo []byte
		if ce.Code != StatusNoStatusRcvd {
			echo = closePayload(ce.Code, "")
		}
		_, _ = c.queueClose(echo)
		close(c.closeRcvd)
		// Read reports the peer's close once our close frame is on the
		// wire, so an application that returns then has completed its
		// half of the closing handshake.
		c.awaitCloseSent()
	case failed:
		if !taken {
			c.deliverFailure(pe)
		}
		_, _ = c.queueClose(closePayload(pe.code, ""))
	}
	c.endRole()
	c.endMessages()
	if taken {
		go c.finishRead(peerClosed, failed)
		return
	}
	c.finishRead(peerClosed, failed)
}

// finishRead completes the closing handshake once the read side has
// ended, and closes the network connection.
func (c *Conn) finishRead(peerClosed, failed bool) {
	defer close(c.readDone)
	switch {
	case peerClosed:
		c.awaitCloseSent()
		if !c.server {
			c.awaitEOF() // RFC 6455 §7.1.1: the server closes TCP first
		}
	case failed:
		if c.awaitCloseSent() {
			c.drain()
		}
	}
	c.closeNow(nil)
}

// frameReadError maps a codec error to the close code it fails the
// connection with; other errors are returned as they are.
func frameReadError(err error) error {
	var fe frameError
	switch {
	case errors.Is(err, errFrameTooLarge):
		return &protocolError{code: StatusMessageTooBig, err: err}
	case errors.As(err, &fe):
		return &protocolError{code: StatusProtocolError, err: err}
	}
	return err
}

// readPayload appends the payload of the frame h to msg. The caller has
// checked that the result fits in limit. With pool, msg grows into memory
// from the pool.
func (c *Conn) readPayload(msg []byte, h frameHeader, limit int, pool bool) ([]byte, error) {
	start := len(msg)
	msg = grow(msg, int(h.length), limit, pool)
	if _, err := io.ReadFull(c.fr.r, msg[start:]); err != nil {
		return nil, noEOF(err)
	}
	if h.masked {
		maskBytes(h.key, 0, msg[start:])
	}
	return msg, nil
}

// grow extends b by n bytes. Its capacity grows geometrically but never
// beyond limit, so a message never takes more memory than the read limit.
// With pool, the new memory comes from the pool and b goes back to it.
func grow(b []byte, n, limit int, pool bool) []byte {
	need := len(b) + n
	if need <= cap(b) {
		return b[:need]
	}
	nb := newBuf(min(max(2*cap(b), need), limit), limit, pool)[:need]
	copy(nb, b)
	if pool {
		putBuf(b)
	}
	return nb
}

// readControl reads and handles the control frame h, for the
// decompressor, which reads the frames of a compressed message.
func (c *Conn) readControl(h frameHeader) error {
	payload, err := c.readPayload(nil, h, maxControlPayload, false)
	if err != nil {
		return err
	}
	return c.handleControl(h.opcode, payload)
}

// handleControl handles a control frame with payload (RFC 6455 §5.5),
// which it may keep.
func (c *Conn) handleControl(op opcode, payload []byte) error {
	switch op {
	case opPing:
		c.queuePong(payload)
	case opPong:
		c.live.pong(payload) // unsolicited pongs need no answer (RFC 6455 §5.5.3)
	case opClose:
		return parseClose(payload)
	}
	return nil
}

// awaitCloseSent waits a bounded time for our close frame to be written
// and reports whether it was.
func (c *Conn) awaitCloseSent() bool {
	t := time.NewTimer(c.cfg.closeTimeout)
	defer t.Stop()
	select {
	case <-c.closeSent:
		return true
	case <-c.closed:
	case <-t.C:
	}
	return false
}

// awaitEOF waits a bounded time for the peer to close TCP.
func (c *Conn) awaitEOF() {
	_ = c.nc.SetReadDeadline(time.Now().Add(c.cfg.closeTimeout))
	_, _ = io.Copy(io.Discard, c.fr.r)
}

// drain half-closes the connection and discards what the peer still
// sends until it closes TCP, for a bounded time. The stream may be out of
// sync after a violation, so it is not parsed. Reading on keeps a TCP reset
// from destroying our close frame before the peer reads it.
func (c *Conn) drain() {
	_ = closeWrite(c.nc)
	c.awaitEOF()
}

// closeWrite shuts down the writing side of nc, when it supports that.
func closeWrite(nc net.Conn) error {
	if b, ok := nc.(*bufferedConn); ok {
		nc = b.Conn
	}
	if cw, ok := nc.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}
