package nwws

import (
	"context"
	"errors"
	"os"
	"time"
)

// takeoverDelay is how long the reader role stays free, with no Read
// reading frames, before the read goroutine takes it to read ahead,
// answer pings and see the peer's close frame.
//
// It trades read-ahead against timer work. A Read that comes within the
// delay reads the next message itself, with no wake-up of another
// goroutine; one that comes later gets a message the read goroutine
// queued, and takes the role back. A shorter delay makes the timer fire,
// and the read goroutine start, more often in a loop of Reads that does
// some work per message: a 1 ms delay cost about 3 % on 1 MiB ReadInto
// loops against 100 ms. A longer one leaves pings unanswered and nothing
// read ahead for that long after the application stops reading.
const takeoverDelay = 2 * time.Millisecond

// Holders of the reader role, in Conn.role. Whoever holds it reads
// frames, with the position in the stream kept in Conn.rs.
const (
	roleFree  = iota // nobody reads frames; the takeover timer is armed
	roleLoop         // the read goroutine reads frames and queues messages
	roleRead         // a Read reads frames itself, while the queue is empty
	roleEnded        // the read side has ended
)

// readAsCaller reads frames in a Read that has taken the reader role with
// the queue empty, until a data message is complete; a ReadInto reads it
// into buf. It returns the message, a violation as a message, why the
// connection ended, or ctx's error once ctx is done.
func (c *Conn) readAsCaller(ctx context.Context, buf []byte, into bool) (message, error) {
	if into {
		c.rs.userBuf = buf
	}
	var (
		m           message
		got         bool
		err         error
		interrupted bool
	)
	watched := ctx.Done() != nil
	if watched && !c.watch(ctx) {
		err = ctx.Err()
	}
	for !got && err == nil {
		m, got, err = c.readOne()
	}
	if watched {
		interrupted = c.unwatch()
	}
	switch {
	case got:
		c.rs.userBuf = nil
		c.releaseRole()
		return m, nil
	case errors.Is(err, ctx.Err()) || interrupted && errors.Is(err, os.ErrDeadlineExceeded):
		c.rs.keep(int(c.cfg.readLimit))
		c.releaseRole()
		return message{}, ctx.Err()
	}
	c.rs.keep(int(c.cfg.readLimit))
	c.endRead(err, true)
	var pe *protocolError
	if errors.As(err, &pe) {
		return message{fail: pe}, nil
	}
	return message{}, c.endErr()
}

// releaseRole gives up the reader role of a Read: to another waiting
// Read, or, if none comes in time, to the read goroutine.
func (c *Conn) releaseRole() {
	c.rmu.Lock()
	c.freeRole()
	wake := c.readers > 0
	c.rmu.Unlock()
	if wake {
		notify(c.avail)
	}
}

// freeRole frees the reader role and arms the takeover timer. Once Close
// has started, the read goroutine takes the role at once, to see the
// peer's close frame. The caller holds rmu.
func (c *Conn) freeRole() {
	if c.discarding.Load() {
		c.startLoop()
		return
	}
	c.role = roleFree
	c.freedAt = time.Now()
	switch {
	case c.armed:
	case c.takeover == nil:
		c.takeover = time.AfterFunc(takeoverDelay, c.takeoverFired)
	default:
		c.takeover.Reset(takeoverDelay)
	}
	c.armed = true
}

// startLoop gives the reader role to a new read goroutine. The caller
// holds rmu.
func (c *Conn) startLoop() {
	c.role = roleLoop
	go c.readLoop()
}

// takeoverFired runs the read goroutine in the timer's goroutine once the
// reader role has stayed free for takeoverDelay.
func (c *Conn) takeoverFired() {
	c.rmu.Lock()
	if c.role != roleFree {
		c.armed = false
		c.rmu.Unlock()
		return
	}
	if wait := takeoverDelay - time.Since(c.freedAt); wait > 0 {
		c.takeover.Reset(wait) // freed again since the timer was armed
		c.rmu.Unlock()
		return
	}
	c.armed, c.role = false, roleLoop
	c.rmu.Unlock()
	c.readLoop()
}

// handOff gives the reader role of the read goroutine to a Read waiting
// for a message, once the read goroutine has queued one, and reports
// whether it did. That Read takes the message, and the next Read reads
// frames itself.
func (c *Conn) handOff() bool {
	if !c.direct {
		return false
	}
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if c.readers == 0 || c.discarding.Load() {
		return false
	}
	c.freeRole()
	return true
}

// endRole ends the reader role for good, once the read side has ended.
func (c *Conn) endRole() {
	c.rmu.Lock()
	c.role = roleEnded
	if c.takeover != nil {
		c.takeover.Stop()
	}
	c.rmu.Unlock()
	c.stopWatch()
}
