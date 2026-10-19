package nwws

import (
	"context"
	"time"
	"unsafe"
)

// entrySize is the memory a message takes in the queue besides its data.
const entrySize = int(unsafe.Sizeof(message{}))

// msgQueue is a ring buffer of messages waiting for Read. It is guarded
// by Conn.rmu.
type msgQueue struct {
	buf  []message
	head int // index of the oldest message
	n    int // number of messages
	data int // capacity of the queued message data, in bytes
}

// mem returns the memory the queue holds: the message data and the ring.
func (q *msgQueue) mem() int { return q.data + len(q.buf)*entrySize }

// fits reports whether the queue takes one more message within budget:
// its memory, before that message's data, is below budget, and the ring
// has room or can grow within budget. An empty queue always takes a
// message.
func (q *msgQueue) fits(budget int) bool {
	return q.n == 0 || q.mem() < budget && (q.n < len(q.buf) || q.growth(budget) > len(q.buf))
}

// growth returns the ring length to grow to: twice the current one, but
// no more than fits budget besides the message data.
func (q *msgQueue) growth(budget int) int {
	return min(max(4, 2*len(q.buf)), (budget-q.data)/entrySize)
}

// push adds m. If the ring is full, the caller has checked with fits that
// it can grow, unless it is empty.
func (q *msgQueue) push(m message, budget int) {
	if q.n == len(q.buf) {
		nb := make([]message, max(q.growth(budget), q.n+1))
		k := copy(nb, q.buf[q.head:])
		copy(nb[k:], q.buf[:q.head])
		q.buf, q.head = nb, 0
	}
	q.buf[(q.head+q.n)%len(q.buf)] = m
	q.n++
	q.data += cap(m.data)
}

func (q *msgQueue) pop() message {
	m := q.buf[q.head]
	q.buf[q.head] = message{} // the queue must not keep the data alive
	q.head = (q.head + 1) % len(q.buf)
	q.n--
	q.data -= cap(m.data)
	return m
}

// shrink gives the ring of an empty queue back once it takes more than a
// quarter of budget. A smaller ring is kept, so a queue that keeps filling
// and draining does not allocate each time.
func (q *msgQueue) shrink(budget int) {
	if q.n == 0 && len(q.buf)*entrySize > budget/4 {
		q.buf, q.head = nil, 0
	}
}

// clear drops every message.
func (q *msgQueue) clear() { *q = msgQueue{} }

// Waits of the read goroutine for Read, in Conn.rwait.
const (
	waitNone  = iota
	waitRoom  // for the queue to take another message
	waitEmpty // for Read to take every message
)

// pushMessage adds m to the messages for Read, unless Close has started.
// It reports whether the queue can take another message within the
// budget.
func (c *Conn) pushMessage(m message) (more bool) {
	c.rmu.Lock()
	if c.discarding.Load() {
		c.rmu.Unlock()
		return true
	}
	c.rq.push(m, c.cfg.readBuffer)
	wake := c.readers > 0
	more = c.rq.fits(c.cfg.readBuffer)
	c.rmu.Unlock()
	if wake {
		notify(c.avail) // after unlocking, so the woken Read finds rmu free
	}
	return more
}

// awaitRead waits for Read to make room in the queue (waitRoom) or to
// empty it (waitEmpty). It reports false if the connection was closed
// first, or t fired. The read goroutine reads nothing while it waits, so
// the liveness timeout is suspended.
func (c *Conn) awaitRead(want int, t <-chan time.Time) bool {
	c.rmu.Lock()
	if c.waitDone(want) {
		c.rmu.Unlock()
		return true
	}
	c.rwait = want
	c.rmu.Unlock()

	c.live.hold()
	defer c.live.resume()
	select {
	case <-c.room:
		return true
	case <-c.closed:
	case <-t:
	}
	c.rmu.Lock()
	c.rwait = waitNone
	select {
	case <-c.room: // sent while giving up
	default:
	}
	c.rmu.Unlock()
	return false
}

// waitDone reports whether the read goroutine's wait for want is over.
// The caller holds rmu.
func (c *Conn) waitDone(want int) bool {
	switch {
	case c.discarding.Load():
		return true
	case want == waitRoom:
		return c.rq.fits(c.cfg.readBuffer)
	default:
		return c.rq.n == 0
	}
}

// next returns the next message for Read, waiting for one until ctx is
// done. Once the read goroutine has ended and the queue is empty, it
// returns why the connection ended.
func (c *Conn) next(ctx context.Context) (message, error) {
	c.rmu.Lock()
	for {
		if c.rq.n > 0 {
			m := c.rq.pop()
			c.rq.shrink(c.cfg.readBuffer)
			if c.rwait != waitNone && c.waitDone(c.rwait) {
				c.rwait = waitNone
				notify(c.room) // under rmu, so awaitRead can take back a wake-up it gave up on
			}
			wake := c.rq.n > 0 && c.readers > 0
			c.rmu.Unlock()
			if wake {
				notify(c.avail) // for another waiting Read
			}
			return m, nil
		}
		if c.rend {
			if c.readers > 0 {
				notify(c.avail)
			}
			c.rmu.Unlock()
			return message{}, c.endErr()
		}
		c.readers++
		c.rmu.Unlock()
		var err error
		select {
		case <-c.avail:
		case <-ctx.Done():
			err = ctx.Err()
		}
		c.rmu.Lock()
		c.readers--
		if err != nil {
			c.rmu.Unlock()
			return message{}, err
		}
	}
}

// endMessages tells Read that no more messages follow.
func (c *Conn) endMessages() {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	c.rend = true
	if c.readers > 0 {
		notify(c.avail)
	}
}

// discardMessages drops the queued messages and those still to arrive,
// once Close has started.
func (c *Conn) discardMessages() {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	c.discarding.Store(true)
	c.rq.clear()
	if c.rwait != waitNone {
		c.rwait = waitNone
		notify(c.room)
	}
}

// notify sends a wake-up on ch, a channel with a buffer of one, unless
// one is pending.
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
