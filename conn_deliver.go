package nwws

import "time"

// deliver queues m for Read. It drops m once Close has started, and
// reports false if the connection was closed first. Once the queue holds
// its budget, it waits until Read makes room.
func (c *Conn) deliver(m message) bool {
	return c.pushMessage(m) || c.awaitRead(waitRoom, nil)
}

// deliverFailure queues the violation pe for Read after the messages
// before it, so the application can answer those first, and waits until
// Read takes it. It waits at most the close timeout, after which the
// connection is failed anyway.
func (c *Conn) deliverFailure(pe *protocolError) {
	c.pushMessage(message{fail: pe})
	t := time.NewTimer(c.cfg.closeTimeout)
	defer t.Stop()
	c.awaitRead(waitEmpty, t.C)
}
