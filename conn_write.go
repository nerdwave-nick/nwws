package nwws

import (
	"context"
	"fmt"
	"unicode/utf8"
)

// writeBufferSize is the size of a connection's write buffer: frames
// queued together go out in one network write up to this size.
const writeBufferSize = 16 << 10

// writeReq is a frame queued for the write goroutine.
type writeReq struct {
	op      opcode
	payload []byte
	done    chan error // receives the result; nil when nobody waits
	taken   bool       // the write goroutine has taken it, guarded by wmu
}

// Write sends a whole message as a single frame. It returns once the
// message is written to the network, so p may be reused afterwards. Text
// messages must be valid UTF-8.
//
// Cancelling ctx withdraws a message that is still queued; the connection
// stays usable. A message already being written cannot be withdrawn
// without corrupting the stream, so the connection is closed instead.
// Write returns ErrClosed once Close has been called or the peer's close
// frame has arrived, and a *[CloseError] with StatusAbnormalClosure if the
// connection is lost while writing.
func (c *Conn) Write(ctx context.Context, typ MessageType, p []byte) error {
	switch typ {
	case MessageText:
		if !utf8.Valid(p) {
			return errInvalidUTF8
		}
	case MessageBinary:
	default:
		return fmt.Errorf("nwws: invalid message type %d", int(typ))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	req := &writeReq{op: opcode(typ), payload: p, done: make(chan error, 1)}
	if err := c.enqueue(req); err != nil {
		return err
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
	}
	if c.withdraw(req) {
		return ctx.Err()
	}
	select {
	case err := <-req.done: // finished meanwhile
		return err
	default:
	}
	c.closeNow(fmt.Errorf("nwws: write abandoned, connection closed: %w", ctx.Err()))
	<-req.done // the write goroutine no longer uses p
	return ctx.Err()
}

// enqueue queues req for the write goroutine. After a close frame, no
// more frames are accepted (RFC 6455 §5.5.1).
func (c *Conn) enqueue(req *writeReq) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.wclosed {
		return ErrClosed
	}
	c.queue = append(c.queue, req)
	if req.op == opClose {
		c.wclosed = true
	}
	c.signal()
	return nil
}

// queueClose queues a close frame with payload.
func (c *Conn) queueClose(payload []byte) (*writeReq, error) {
	req := &writeReq{op: opClose, payload: payload, done: make(chan error, 1)}
	return req, c.enqueue(req)
}

// queuePong queues a pong answering a ping with payload. Only the latest
// unanswered ping is answered (RFC 6455 §5.5.3), which bounds the queue.
func (c *Conn) queuePong(payload []byte) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.wclosed {
		return
	}
	if c.pong != nil {
		c.pong.payload = payload
		return
	}
	c.pong = &writeReq{op: opPong, payload: payload}
	c.signal()
}

func (c *Conn) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// withdraw removes req from the queue unless the write goroutine has taken
// it, and reports whether it did.
func (c *Conn) withdraw(req *writeReq) bool {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if req.taken {
		return false
	}
	for i, r := range c.queue {
		if r == req {
			c.queue = append(c.queue[:i], c.queue[i+1:]...)
			break
		}
	}
	return true
}

// writeLoop writes queued frames until a close frame is written or the
// connection is closed. All frames queued at the time are written with a
// single flush.
func (c *Conn) writeLoop() {
	defer close(c.writeDone)
	defer c.failQueued()
	for {
		select {
		case <-c.wake:
		case <-c.closed:
			return
		}
		batch := c.takeBatch()
		if len(batch) == 0 {
			continue
		}
		var err error
		if werr := c.writeBatch(batch); werr != nil {
			err = &CloseError{Code: StatusAbnormalClosure, err: werr}
		}
		for _, r := range batch {
			if r.done != nil {
				r.done <- err
			}
		}
		if err != nil {
			c.closeNow(err)
			return
		}
		if batch[len(batch)-1].op == opClose {
			close(c.closeSent)
			return
		}
	}
}

// takeBatch takes every queued frame, the pending pong first.
func (c *Conn) takeBatch() []*writeReq {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	batch := make([]*writeReq, 0, len(c.queue)+1)
	if c.pong != nil {
		batch = append(batch, c.pong)
		c.pong = nil
	}
	batch = append(batch, c.queue...)
	clear(c.queue)
	c.queue = c.queue[:0]
	for _, r := range batch {
		r.taken = true
	}
	return batch
}

func (c *Conn) writeBatch(batch []*writeReq) error {
	for _, r := range batch {
		if r.op == opPing {
			c.live.pingWriting(r.payload)
		}
		if err := c.fw.writeFrame(true, 0, r.op, r.payload); err != nil {
			return err
		}
	}
	return c.bw.Flush()
}

// failQueued stops accepting frames and fails those still queued.
func (c *Conn) failQueued() {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.wclosed = true
	for _, r := range c.queue {
		if r.done != nil {
			r.done <- ErrClosed
		}
	}
	c.queue, c.pong = nil, nil
}
