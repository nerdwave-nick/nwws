package nwws

import (
	"bufio"
	"sync"
)

// writeBuffer holds the memory a connection writes frames with: the write
// buffer, and the frame writer's scratch space for headers and masking.
// A connection takes one from writeBuffers when it starts writing and
// returns it once nothing is left to write, so an idle connection holds
// none.
type writeBuffer struct {
	bw      *bufio.Writer
	scratch [maxHeaderLen + writeChunk]byte
}

var writeBuffers = sync.Pool{
	New: func() any {
		return &writeBuffer{bw: bufio.NewWriterSize(nil, writeBufferSize)}
	},
}

// takeWriteBuffer gives the frame writer a write buffer, unless it still
// has one. The caller holds wmu and has just taken ownership of the write
// side.
func (c *Conn) takeWriteBuffer() {
	if c.wbuf != nil {
		return
	}
	wb := writeBuffers.Get().(*writeBuffer)
	wb.bw.Reset(c.nc)
	c.wbuf, c.bw = wb, wb.bw
	c.fw.w, c.fw.buf = wb.bw, wb.scratch[:0]
}

// putWriteBuffer returns the write buffer to the pool. The caller holds
// wmu, and nobody writes: either nobody owns the write side, or its owner
// has stopped writing for good.
func (c *Conn) putWriteBuffer() {
	wb := c.wbuf
	if wb == nil {
		return
	}
	wb.bw.Reset(nil) // the pool must not keep the connection alive
	c.wbuf, c.bw = nil, nil
	c.fw.w, c.fw.buf = nil, nil
	writeBuffers.Put(wb)
}

// endWriter returns the write buffer of an owner that stops writing for
// good, after a close frame or a failed write. It keeps ownership, so the
// write side is never taken again.
func (c *Conn) endWriter() {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.putWriteBuffer()
}
