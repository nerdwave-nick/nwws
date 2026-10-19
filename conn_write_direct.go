package nwws

import (
	"context"
	"sync"
)

// writeReqs pools the requests of queued Writes with their done channels.
// A request goes back once its Write has returned: by then the write
// goroutine has either sent its result or never seen it.
var writeReqs = sync.Pool{
	New: func() any { return &writeReq{done: make(chan error, 1)} },
}

func putWriteReq(req *writeReq) {
	req.payload = nil // the pool must not keep the message alive
	writeReqs.Put(req)
}

// ownOrQueue takes ownership of the frame writer for a direct write when
// no frame is being written or waits to be, pending pongs included, and
// returns a nil request then. Otherwise it queues a request for the write
// goroutine and returns it. After a close frame, no more frames are
// accepted (RFC 6455 §5.5.1).
func (c *Conn) ownOrQueue(op opcode, p []byte) (*writeReq, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	switch {
	case c.wclosed:
		return nil, ErrClosed
	case !c.writing && len(c.queue) == 0 && len(c.pongs) == 0:
		c.writing = true
		return nil, nil
	}
	req := writeReqs.Get().(*writeReq)
	req.op, req.payload, req.taken = op, p, false
	c.queue = append(c.queue, req)
	c.signal()
	return req, nil
}

// releaseWriter gives up ownership of the frame writer, and wakes the
// write goroutine for frames queued meanwhile.
func (c *Conn) releaseWriter() {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.writing = false
	if len(c.queue) > 0 || len(c.pongs) > 0 {
		c.signal()
	}
}

// writeDirect writes and flushes one data frame in the calling goroutine,
// which owns the frame writer. Cancelling ctx during the write closes the
// connection, as for a queued write the write goroutine has taken.
func (c *Conn) writeDirect(ctx context.Context, op opcode, p []byte) error {
	stop := func() bool { return true }
	if ctx.Done() != nil {
		stop = context.AfterFunc(ctx, func() { c.abandonWrite(ctx) })
	}
	rsv, payload := c.deflate.frame(op, p)
	err := c.fw.writeFrame(true, rsv, op, payload)
	if err == nil {
		err = c.bw.Flush()
	}
	if !stop() {
		c.abandonWrite(ctx) // returns only once the connection is closed
		return ctx.Err()
	}
	if err != nil {
		err = &CloseError{Code: StatusAbnormalClosure, err: err}
		c.closeNow(err)
		return err
	}
	c.releaseWriter()
	return nil
}
