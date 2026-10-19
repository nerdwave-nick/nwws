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
	typ  MessageType
	data []byte
	fail *protocolError
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
// the next message is kept for a later Read.
//
// Once the connection has ended, Read returns why: a *[CloseError] with the
// peer's close code and reason, a *CloseError with StatusAbnormalClosure
// when the connection was lost without a close frame, or another error
// when this side failed the connection.
//
// A protocol violation by the peer is reported in order, after the
// messages received before it, and its close frame is queued before Read
// returns, so it follows what the application wrote in reply to those
// messages. If Read is not called within the close timeout, the connection
// is failed anyway.
func (c *Conn) Read(ctx context.Context) (MessageType, []byte, error) {
	select {
	case m, ok := <-c.msgs:
		if !ok {
			return 0, nil, c.endErr()
		}
		if m.fail != nil {
			// Queue the failure's close frame before Read returns, so a
			// Close called after Read cannot replace its code.
			_, _ = c.queueClose(closePayload(m.fail.code, ""))
			return 0, nil, m.fail
		}
		return m.typ, m.data, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

// readLoop reads frames until the connection ends, records why so that
// Read returns it, and then completes the closing handshake.
func (c *Conn) readLoop() {
	defer close(c.readDone)

	err := c.readMessages()
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
	case failed:
		c.deliverFailure(pe)
		_, _ = c.queueClose(closePayload(pe.code, ""))
	}
	close(c.msgs)

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

// readMessages reads frames, reassembles messages (RFC 6455 §5.4) and hands
// them to Read. It returns the peer's close frame as a *CloseError, a
// violation as a *protocolError, or a network error.
func (c *Conn) readMessages() error {
	var (
		msg   []byte
		typ   opcode
		inMsg bool // a message has started and not finished
		dfl   bool // the message is compressed (RFC 7692 §6)
		text  utf8Validator
		limit = c.cfg.readLimit
	)
	for {
		room := limit - int64(len(msg))
		// Control frames up to 125 bytes stay readable when room is less.
		c.fr.maxPayload = max(room, maxControlPayload)
		h, err := c.fr.readHeader()
		if err != nil {
			return frameReadError(err)
		}
		if err := checkCompressedBit(h); err != nil {
			return err
		}
		if h.opcode.isControl() {
			if err := c.readControl(h); err != nil {
				return err
			}
			continue
		}

		switch {
		case h.opcode == opContinuation && !inMsg:
			return protocolErrorf(StatusProtocolError, "continuation frame without a message to continue")
		case h.opcode != opContinuation && inMsg:
			return protocolErrorf(StatusProtocolError, "new message before the previous one finished")
		case h.length > room:
			return &protocolError{code: StatusMessageTooBig, err: fmt.Errorf("%w of %d bytes", errMessageTooLarge, limit)}
		}
		if !inMsg {
			typ, inMsg, text = h.opcode, true, utf8Validator{}
			dfl = h.rsv&rsv1Bit != 0
		}
		if typ == opText && !dfl {
			msg, err = c.readTextPayload(msg, h, int(limit), &text)
		} else {
			msg, err = c.readPayload(msg, h, int(limit))
		}
		if err != nil {
			return err
		}
		if dfl {
			if !h.fin {
				continue // the compressed bytes are buffered in msg
			}
			if msg, err = c.inflate.message(msg, limit); err != nil {
				return err
			}
		}
		if typ == opText && !text.valid(msg, h.fin) {
			return &protocolError{code: StatusInvalidFramePayloadData, err: errInvalidUTF8}
		}
		if !h.fin {
			continue
		}
		if !c.deliver(message{typ: MessageType(typ), data: msg}) {
			return net.ErrClosed
		}
		msg, inMsg = nil, false
	}
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
// checked that the result fits in limit.
func (c *Conn) readPayload(msg []byte, h frameHeader, limit int) ([]byte, error) {
	start := len(msg)
	msg = grow(msg, int(h.length), limit)
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
func grow(b []byte, n, limit int) []byte {
	need := len(b) + n
	if need <= cap(b) {
		return b[:need]
	}
	nb := make([]byte, need, min(max(2*cap(b), need), limit))
	copy(nb, b)
	return nb
}

// readControl reads and handles the control frame h (RFC 6455 §5.5).
func (c *Conn) readControl(h frameHeader) error {
	payload, err := c.readPayload(nil, h, maxControlPayload)
	if err != nil {
		return err
	}
	switch h.opcode {
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
