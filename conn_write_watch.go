package nwws

import "context"

// watchWrite arranges for ctx being done to abandon the direct write that
// is about to start, which closes the connection. It reports false if ctx
// is done already: nothing is written then, and the connection stays
// usable. Writes usually pass the same ctx, or contexts sharing its Done
// channel, so its AfterFunc stays registered between them and costs no
// allocation. A cancellation is bound to that channel, so a stale one
// never abandons a later write, and it has no effect between writes.
func (c *Conn) watchWrite(ctx context.Context) bool {
	done := ctx.Done()
	if done == nil {
		return true // never done
	}
	c.wimu.Lock()
	defer c.wimu.Unlock()
	if done != c.widone {
		if c.wistop != nil {
			c.wistop()
		}
		c.widone = done
		c.wistop = context.AfterFunc(ctx, func() { c.writeCtxDone(ctx, done) })
	}
	if ctx.Err() != nil {
		return false // done before the watch: its AfterFunc may have passed
	}
	c.wiwriting = true
	return true
}

// unwatchWrite ends the watch of a direct write that has stopped writing,
// and reports whether its ctx was done meanwhile, which has closed the
// connection.
func (c *Conn) unwatchWrite() bool {
	c.wimu.Lock()
	defer c.wimu.Unlock()
	c.wiwriting = false
	fired := c.wifired
	c.wifired = false
	return fired
}

// writeCtxDone abandons the direct write in progress, if it writes with
// done.
func (c *Conn) writeCtxDone(ctx context.Context, done <-chan struct{}) {
	c.wimu.Lock()
	fire := c.wiwriting && done == c.widone
	if fire {
		c.wifired = true
	}
	c.wimu.Unlock()
	if fire {
		c.abandonWrite(ctx) // takes wmu, so not under wimu
	}
}

// stopWriteWatch drops the AfterFunc of the last watched ctx, once the
// write side has finished, so that the ctx no longer keeps the connection
// reachable. The caller holds wmu.
func (c *Conn) stopWriteWatch() {
	c.wimu.Lock()
	defer c.wimu.Unlock()
	if c.wistop != nil {
		c.wistop()
		c.wistop, c.widone = nil, nil
	}
}
