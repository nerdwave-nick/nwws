package nwws

import (
	"bufio"
	"io"
	"math"
)

// CloseNow closes the network connection at once, without the closing
// handshake, and waits for the goroutines of the connection to end. It
// drops the messages read ahead and not yet read. A Read or Write that is
// pending, or called afterwards, returns ErrClosed, unless the connection
// had already ended for another reason, which they report instead. A
// Close in progress returns early with an error.
//
// CloseNow may be called from any goroutine, any number of times, also
// during or after Close. It returns nil if this call closed the
// connection, and ErrClosed if the connection was already closed.
func (c *Conn) CloseNow() error {
	c.discardMessages()
	closed := c.closeNow(ErrClosed)
	c.wait()
	if !closed {
		return ErrClosed
	}
	return nil
}

// writeErr returns the error a failed network write reports: why the
// connection ended if this side has closed it, so that a Write cut short
// by CloseNow or Close returns ErrClosed, or else StatusAbnormalClosure.
func (c *Conn) writeErr(werr error) error {
	select {
	case <-c.closed:
		return c.endErr()
	default:
		return &CloseError{Code: StatusAbnormalClosure, err: werr}
	}
}

// skipPayload discards the next n bytes of the frame stream, a frame's
// payload, without allocating.
func (c *Conn) skipPayload(n int64) error {
	br, ok := c.fr.r.(*bufio.Reader)
	if !ok {
		_, err := io.CopyN(io.Discard, c.fr.r, n)
		return noEOF(err)
	}
	for n > 0 {
		k := int(min(n, math.MaxInt32))
		if _, err := br.Discard(k); err != nil {
			return noEOF(err)
		}
		n -= int64(k)
	}
	return nil
}
