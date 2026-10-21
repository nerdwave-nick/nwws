package nwws

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
