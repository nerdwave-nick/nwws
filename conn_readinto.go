package nwws

import (
	"context"
	"math/bits"
	"sync"
)

// ReadInto is Read into a buffer the caller reuses. It reads the next data
// message into buf[:0] and returns the filled slice. When buf's capacity is
// too small for the message, it returns a new slice instead, which the
// caller owns and can pass to the next ReadInto. A loop that passes back
// what ReadInto returned reads in steady state without allocating:
//
//	var buf []byte
//	for {
//		typ, msg, err := c.ReadInto(ctx, buf)
//		if err != nil {
//			return err
//		}
//		handle(typ, msg) // msg is valid until the next ReadInto
//		buf = msg
//	}
//
// ReadInto waits, ends and fails exactly as Read does, also for a protocol
// violation, which it reports in order. On error it returns buf[:0], so the
// buffer stays available.
//
// While no message has been read ahead, ReadInto reads the message
// straight into buf, as Read reads frames itself then; see [Conn]. Unlike
// Read, it does so for messages of 256 KiB or more too: the next one is
// not read ahead while the application handles the last, and no message
// is copied. A message read ahead, after no Read ran for a couple of
// milliseconds or after a smaller large message, is in memory of the
// connection's own. Once a connection has called ReadInto, it takes
// that memory from a pool shared by all connections, and ReadInto gives
// it back after copying the message into buf. Read and ReadInto may be mixed.
func (c *Conn) ReadInto(ctx context.Context, buf []byte) (MessageType, []byte, error) {
	if !c.pooling.Load() {
		c.pooling.Store(true)
	}
	m, err := c.nextData(ctx, buf, true)
	if err != nil {
		return 0, buf[:0], err
	}
	if m.inBuf {
		return m.typ, m.data, nil // read straight into buf
	}
	if cap(buf) < len(m.data) {
		// The message's own memory becomes the caller's buffer.
		return m.typ, owned(m.data), nil
	}
	buf = append(buf[:0], m.data...)
	putBuf(m.data)
	return m.typ, buf, nil
}

// owned returns b without the capacity beyond its length, which may hold
// data of earlier messages, also of other connections, when b comes from
// the pool.
func owned(b []byte) []byte { return b[:len(b):len(b)] }

// maxPooledBuffer is the capacity of the largest message buffer the pool
// keeps: 4 MiB, four times the default read limit. Larger buffers are left
// to the garbage collector, so a rare huge message does not stay in memory.
const (
	maxPooledBits   = 22
	maxPooledBuffer = 1 << maxPooledBits
)

// msgBufs pools message buffers by capacity: msgBufs[k] holds buffers
// whose capacity is at least 2^k and less than 2^(k+1). It holds pointers,
// so that putting a buffer allocates nothing.
var msgBufs [maxPooledBits + 1]sync.Pool

// boxes pools the pointers msgBufs holds, emptied.
var boxes sync.Pool

// bufClass returns the index in msgBufs for a capacity n > 0.
func bufClass(n int) int { return bits.Len(uint(n)) - 1 }

// newBuf returns a buffer of length n, from the pool when pool is set,
// with a capacity of at most limit, which is at least n.
func newBuf(n, limit int, pool bool) []byte {
	if pool {
		return getBuf(n, limit)
	}
	return make([]byte, n)
}

// getBuf returns a buffer of length n with a capacity of at most limit,
// which is at least n: one from the pool if it holds one, else a new one.
// The buffer may hold earlier data.
func getBuf(n, limit int) []byte {
	if n > 0 && n <= maxPooledBuffer {
		k := bufClass(n)
		if b := takeBuf(k, n, limit); b != nil {
			return b
		}
		if k+1 < len(msgBufs) {
			if b := takeBuf(k+1, n, limit); b != nil {
				return b
			}
		}
	}
	return make([]byte, n)
}

// takeBuf takes a buffer from msgBufs[k] and returns it with length n, if
// its capacity is between n and limit.
func takeBuf(k, n, limit int) []byte {
	p, _ := msgBufs[k].Get().(*[]byte)
	if p == nil {
		return nil
	}
	b := *p
	if cap(b) < n || cap(b) > limit {
		msgBufs[k].Put(p)
		return nil
	}
	*p = nil
	boxes.Put(p)
	return b[:n]
}

// putBuf gives b to the pool. Nothing may use b afterwards.
func putBuf(b []byte) {
	n := cap(b)
	if n == 0 || n > maxPooledBuffer {
		return
	}
	p, _ := boxes.Get().(*[]byte)
	if p == nil {
		p = new([]byte)
	}
	*p = b[:0]
	msgBufs[bufClass(n)].Put(p)
}
