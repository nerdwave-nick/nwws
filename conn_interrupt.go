package nwws

import (
	"context"
	"time"
)

// aLongTimeAgo is a read deadline in the past, which makes a pending
// network read return at once.
var aLongTimeAgo = time.Unix(1, 0)

// watch arranges for ctx being done to interrupt the network read of the
// Read that holds the reader role, through a read deadline in the past.
// It reports false if ctx is done already. Reads usually pass the same
// ctx, or contexts sharing its Done channel, so its AfterFunc stays
// registered between them and costs no allocation. An interrupt is bound
// to that channel, so a stale one never cuts short a later Read.
func (c *Conn) watch(ctx context.Context) bool {
	done := ctx.Done()
	c.imu.Lock()
	defer c.imu.Unlock()
	if done != c.idone {
		if c.istop != nil {
			c.istop()
		}
		c.idone = done
		c.istop = context.AfterFunc(ctx, func() { c.interrupt(done) })
	}
	if ctx.Err() != nil {
		return false // done before the watch: its AfterFunc may have passed
	}
	c.ireading = true
	return true
}

// unwatch ends the watch of a Read that has stopped reading frames, and
// reports whether its network read was interrupted. It clears the read
// deadline then, so the stream can be read on where it stopped.
func (c *Conn) unwatch() bool {
	c.imu.Lock()
	defer c.imu.Unlock()
	c.ireading = false
	if !c.interrupted {
		return false
	}
	c.interrupted = false
	_ = c.nc.SetReadDeadline(time.Time{})
	return true
}

// interrupt makes the network read of the Read holding the reader role
// return, if it reads with done.
func (c *Conn) interrupt(done <-chan struct{}) {
	c.imu.Lock()
	defer c.imu.Unlock()
	if !c.ireading || done != c.idone {
		return
	}
	c.interrupted = true
	_ = c.nc.SetReadDeadline(aLongTimeAgo)
}

// stopWatch drops the AfterFunc of the last watched ctx, once the read
// side has ended.
func (c *Conn) stopWatch() {
	c.imu.Lock()
	defer c.imu.Unlock()
	if c.istop != nil {
		c.istop()
		c.istop, c.idone = nil, nil
	}
}
