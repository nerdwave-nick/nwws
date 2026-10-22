package nwws

import (
	"context"
	"errors"
	"os"
	"time"
)

// takeoverDelay is how long the reader role stays free, with no Read
// reading frames, before the read goroutine takes it to read ahead,
// answer pings and see the peer's close frame. The takeover comes after
// one to two delays: the timer checks once per delay whether a Read
// freed the role since its last check, which keeps the clock off the
// path of every Read.
//
// It trades read-ahead against timer work. A Read that comes within the
// delay reads the next message itself, with no wake-up of another
// goroutine; one that comes later gets a message the read goroutine
// queued, and takes the role back. A shorter delay makes the timer fire,
// and the read goroutine start, more often in a loop of Reads that does
// some work per message. A longer one leaves pings unanswered and nothing
// read ahead for that long after the application stops reading.
//
// Measured against a 100 ms delay, interleaved on GitHub runners (runs
// 39418354013 and 39442434243): 2 ms costs about 2 to 4 % on loops of
// 1 MiB ReadInto, and nothing measurable on round trips, throughput and
// smaller reads. In the research for this design, 1 ms cost about 3 %
// on the same loops.
const takeoverDelay = 2 * time.Millisecond

// readAheadSize is the message size from which the read goroutine reads
// ahead instead of Read reading frames itself. ReadInto reads messages of
// readIntoCallerSize or more itself again; see readsAhead.
//
// Reading in the caller saves a wake-up of another goroutine per message,
// which is most of what a small message costs: 300 B round trips, Reads
// and ReadIntos measured 24 to 54 % faster that way. For large messages,
// reading the next one while the application handles the last matters
// more: on EPYC 9V CPUs, 64 KiB ReadInto floods were 21 to 37 % slower
// when read in the caller (runs 39442434243 and 39481208605), and reading
// ahead from 64 KiB on brings them back to the speed of reading ahead
// always. A 16 KiB threshold measured the same on the benchmarks, whose
// sizes are 300 B, 64 KiB and 1 MiB (runs 39487695170 and 39487699129);
// 64 KiB keeps caller reads for more message sizes.
const readAheadSize = 64 << 10

// readIntoCallerSize is the message size from which ReadInto reads
// messages itself again, straight into the caller's buffer, instead of
// having them read ahead and copying them out of the connection's memory.
//
// Reading ahead costs ReadInto a copy of every message, a wake-up of the
// read goroutine and a working set of three message buffers instead of
// the caller's one. For 1 MiB messages that costs more than reading the
// next message early gains: read in the caller, interleaved on three EPYC
// 9V45 runners (run 39745595539), loops of 1 MiB ReadInto ran 5 to 10 %
// faster, and nwws's Read1MiB/api=reuse 6 to 9 % faster. For 64 KiB
// messages, reading ahead still wins: read in the caller, ReadInto/64KiB
// ran 23 to 32 % slower in the same run. 256 KiB lies between the two
// measured sizes; the benchmarks have no message size in between.
const readIntoCallerSize = 256 << 10

// readsAhead reports whether a message of n bytes for Read, or for
// ReadInto when into is set, is read ahead by the read goroutine.
func readsAhead(n int, into bool) bool {
	return n >= readAheadSize && (!into || n < readIntoCallerSize)
}

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
	case got && readsAhead(len(m.data), into):
		// Large messages are read ahead: the read goroutine reads the
		// next one while the application handles this one. ReadInto
		// keeps reading the largest itself, straight into its buffer,
		// which saves copying each one out of the connection's memory.
		c.rs.userBuf = nil
		c.rmu.Lock()
		c.startLoop()
		c.rmu.Unlock()
		return m, nil
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
	c.freed++
	switch {
	case c.armed:
		return
	case c.takeover == nil:
		c.takeover = time.AfterFunc(takeoverDelay, c.takeoverFired)
	default:
		c.takeover.Reset(takeoverDelay)
	}
	c.armed, c.seen = true, c.freed
}

// startLoop gives the reader role to a new read goroutine. The caller
// holds rmu.
func (c *Conn) startLoop() {
	c.role = roleLoop
	go c.readLoop()
}

// takeoverFired runs the read goroutine in the timer's goroutine once the
// reader role has stayed free for a whole takeoverDelay.
func (c *Conn) takeoverFired() {
	c.rmu.Lock()
	if c.role != roleFree {
		c.armed = false
		c.rmu.Unlock()
		return
	}
	if c.freed != c.seen {
		c.seen = c.freed // freed again meanwhile: wait a whole delay more
		c.takeover.Reset(takeoverDelay)
		c.rmu.Unlock()
		return
	}
	c.armed, c.role = false, roleLoop
	c.rmu.Unlock()
	c.readLoop()
}

// handOff gives the reader role of the read goroutine to the Reads, once
// it has queued m for a Read that waited, unless m is read ahead (see
// readsAhead), and reports whether it did. A waiting Read is woken: it
// takes a queued message or, when another Read took that, the free role,
// so that it reads frames itself. The Read that waited may have taken m
// and returned already; the free role then waits for its next call.
func (c *Conn) handOff(m message) bool {
	if !c.direct {
		return false
	}
	c.rmu.Lock()
	if !c.readQueued || readsAhead(len(m.data), c.intoQueued) || c.discarding.Load() {
		c.rmu.Unlock()
		return false
	}
	c.freeRole()
	wake := c.readers > 0
	c.rmu.Unlock()
	if wake {
		notify(c.avail)
	}
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
