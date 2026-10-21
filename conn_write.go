package nwws

import (
	"context"
	"fmt"
	"unicode/utf8"
)

// writeBufferSize is the size of a connection's write buffer: frames
// queued together go out in one network write up to this size.
const writeBufferSize = 16 << 10

// maxPendingPongs bounds the pongs waiting for the writer. Beyond it, the
// oldest unanswered pings go unanswered.
const maxPendingPongs = 16

// maxKeptBatch bounds the batch the write goroutine keeps between
// batches; a larger one, left by a burst of writers, is released.
const maxKeptBatch = 64

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
// When no other frame is being written or waits to be, Write writes the
// frame itself. Otherwise it queues the message, and messages queued
// while a write is in progress go out together in the next one.
//
// Cancelling ctx withdraws a message that is still queued; the connection
// stays usable. A message already being written cannot be withdrawn
// without corrupting the stream, so the connection is closed instead.
// Write returns ErrClosed once Close or CloseNow has been called or the
// peer's close frame has arrived, also when CloseNow cuts a pending write
// short, and a *[CloseError] with StatusAbnormalClosure if the connection
// is lost while writing.
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
	req, err := c.ownOrQueue(opcode(typ), p)
	switch {
	case err != nil:
		return err
	case req == nil:
		return c.writeDirect(ctx, opcode(typ), p)
	}
	err = c.awaitQueued(ctx, req)
	putWriteReq(req)
	return err
}

// awaitQueued waits until the write goroutine has written req, or
// withdraws it when ctx is cancelled first.
func (c *Conn) awaitQueued(ctx context.Context, req *writeReq) error {
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
	c.abandonWrite(ctx)
	<-req.done // the write goroutine no longer uses req
	return ctx.Err()
}

// abandonWrite closes the connection because the write in progress was
// cancelled.
func (c *Conn) abandonWrite(ctx context.Context) {
	c.closeNow(fmt.Errorf("nwws: write abandoned, connection closed: %w", ctx.Err()))
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

// queuePong queues a pong answering a ping with payload. Every ping is
// answered, up to maxPendingPongs waiting for the writer; beyond that the
// oldest is dropped, as only the latest must be answered (RFC 6455 §5.5.3).
func (c *Conn) queuePong(payload []byte) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.wclosed {
		return
	}
	if len(c.pongs) == maxPendingPongs {
		c.pongs = append(c.pongs[:0], c.pongs[1:]...)
	}
	c.pongs = append(c.pongs, &writeReq{op: opPong, payload: payload})
	c.signal()
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
