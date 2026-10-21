package nwws

import "time"

// writerLinger is how long the write goroutine waits for more frames
// after its last batch before it ends; it checks once per linger whether
// it wrote meanwhile, so it ends one to two lingers after its last batch.
// The next frame queued starts another. A burst of concurrent writers
// keeps the one goroutine, so their messages still go out together.
const writerLinger = time.Millisecond

// signal has the write goroutine write the queued frames: it wakes the
// one that runs or starts one. While a Write owns the write side, it does
// neither, as that Write signals again when it is done. The caller holds
// wmu.
func (c *Conn) signal() {
	switch {
	case c.wrun:
		select {
		case c.wake <- struct{}{}:
		default:
		}
	case c.writing || c.wfinal:
	case c.isClosed():
		c.finishWriter()
	default:
		c.wrun = true
		go c.writeLoop()
	}
}

// writeLoop writes queued frames, all those queued at the time with a
// single flush, for as long as frames keep coming. It ends once none came
// for writerLinger, and for good once a close frame is written or the
// connection is closed.
func (c *Conn) writeLoop() {
	linger := time.NewTimer(writerLinger)
	defer linger.Stop()
	wrote := false // a batch was written since linger last fired
	var batch []*writeReq
	for {
		batch = c.takeBatch(batch[:0])
		if len(batch) == 0 {
			select {
			case <-c.wake:
				continue
			case <-linger.C:
				linger.Reset(writerLinger)
				if wrote {
					wrote = false
					continue
				}
			case <-c.closed:
			}
			if c.stopWriter() {
				return
			}
			continue
		}
		wrote = true
		werr := c.writeBatch(batch)
		var err error
		if werr != nil {
			err = c.writeErr(werr)
		}
		closing := batch[len(batch)-1].op == opClose
		if err == nil && !closing {
			c.releaseWriter()
		} else {
			c.endWriter()
		}
		// A request may be reused as soon as its writer is told.
		for i, r := range batch {
			if r.done != nil {
				r.done <- err
			}
			batch[i] = nil
		}
		if err != nil {
			c.closeNow(err)
			c.exitWriter()
			return
		}
		if closing {
			close(c.closeSent)
			c.live.closeWritten()
			c.exitWriter()
			return
		}
		if cap(batch) > maxKeptBatch {
			batch = nil // a burst of writers is over
		}
	}
}

// takeBatch appends every queued frame to batch, the pending pongs first,
// and takes ownership of the frame writer for them. It takes nothing
// while a direct write owns the frame writer, or once the write side has
// ended.
func (c *Conn) takeBatch(batch []*writeReq) []*writeReq {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.writing || c.wfinal {
		return batch
	}
	batch = append(batch, c.pongs...)
	clear(c.pongs)
	c.pongs = c.pongs[:0]
	batch = append(batch, c.queue...)
	clear(c.queue)
	c.queue = c.queue[:0]
	for _, r := range batch {
		r.taken = true
	}
	if len(batch) == 0 {
		c.putWriteBuffer() // kept for requests withdrawn meanwhile
		return batch
	}
	c.writing = true
	c.takeWriteBuffer()
	return batch
}

func (c *Conn) writeBatch(batch []*writeReq) error {
	for _, r := range batch {
		if r.op == opPing {
			c.live.pingWriting(r.payload)
		}
		rsv, payload := c.deflate.frame(r.op, r.payload)
		if err := c.fw.writeFrame(true, rsv, r.op, payload); err != nil {
			return err
		}
	}
	return c.bw.Flush()
}

// stopWriter ends the write goroutine, unless frames it can take are
// queued and the connection is open, and reports whether it did.
func (c *Conn) stopWriter() bool {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if !c.writing && !c.wfinal && (len(c.queue) > 0 || len(c.pongs) > 0) && !c.isClosed() {
		return false
	}
	c.wrun = false
	c.finishWriter()
	return true
}

// exitWriter ends the write goroutine after the write side has ended.
func (c *Conn) exitWriter() {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.wrun = false
	c.finishWriter()
}

// finishWriter finishes the write side once nothing writes and nothing
// will: the write side has ended or the connection is closed, and neither
// a write goroutine nor a Write owns it. It fails the frames still
// queued, returns the write buffer and stops liveness, which can no
// longer ping. writeDone is closed then, or, while a liveness timer
// callback runs, once it returns; see liveness.stop. The caller holds
// wmu.
func (c *Conn) finishWriter() {
	if c.wdone || c.wrun || c.writing || !c.wfinal && !c.isClosed() {
		return
	}
	c.wdone, c.wclosed = true, true
	for _, r := range c.queue {
		if r.done != nil {
			r.done <- ErrClosed
		}
	}
	c.queue, c.pongs = nil, nil
	c.putWriteBuffer()
	if c.live != nil {
		c.live.stop() // closes writeDone
		return
	}
	close(c.writeDone)
}

// isClosed reports whether the network connection is closed.
func (c *Conn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
