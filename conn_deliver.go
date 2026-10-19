package nwws

import "time"

// deliver hands m to Read. It drops m once Close has started, and reports
// false if the connection was closed first. While m waits for Read, the
// liveness timeout is suspended.
func (c *Conn) deliver(m message) bool {
	select {
	case c.msgs <- m:
		return true
	default:
	}
	c.live.hold()
	defer c.live.resume()
	select {
	case c.msgs <- m:
		return true
	case <-c.discard:
		return true
	case <-c.closed:
		return false
	}
}

// deliverFailure hands the violation pe to Read after the messages
// before it, so the application can answer those first. It waits at most
// the close timeout, after which the connection is failed anyway.
func (c *Conn) deliverFailure(pe *protocolError) {
	t := time.NewTimer(c.cfg.closeTimeout)
	defer t.Stop()
	c.live.hold()
	defer c.live.resume()
	select {
	case c.msgs <- message{fail: pe}:
	case <-c.discard:
	case <-c.closed:
	case <-t.C:
	}
}
