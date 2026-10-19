package nwws

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

// Liveness defaults, used when ConnOptions leaves them unset.
const (
	DefaultPingInterval = 10 * time.Second
	DefaultPongTimeout  = 30 * time.Second
)

// ErrLivenessTimeout is wrapped by the error of a connection closed
// because the peer did not answer a ping in time. Read returns it inside a
// *[CloseError] with StatusAbnormalClosure. The peer is sent a close frame
// with StatusGoingAway and the reason "liveness timeout", if it can be
// written within a second.
var ErrLivenessTimeout = errors.New("nwws: no answer to a ping within the pong timeout")

// RTT returns the round-trip time last measured from a ping to its pong,
// or zero when none was measured yet or liveness is off.
func (c *Conn) RTT() time.Duration {
	l := c.live
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rtt
}

// liveness pings the peer every interval through the write goroutine and
// closes the connection when, after a ping, nothing arrives from the peer
// within the timeout. Any data received proves the peer alive, not only
// the pong: a pong may wait behind a large frame on a slow link.
//
// It runs in the write goroutine, on a timer in its select; see tick.
// While that goroutine writes a batch, which a peer that reads nothing can
// block, a watch timer ticks in its place; see guard.
//
// While the read goroutine waits for Read, because the read-ahead budget
// is full or a violation waits to be reported, it reads no frames and
// cannot see the peer's pongs. The timeout is suspended then, and restarts
// in full when Read makes room, so a slow application never makes a live
// peer time out.
type liveness struct {
	c        *Conn
	interval time.Duration
	timeout  time.Duration

	mu       sync.Mutex
	nextPing time.Time     // when the next ping is due
	closeBy  time.Time     // after a timeout, when to stop waiting for its close frame; zero before
	waiting  time.Time     // when the ping awaiting a sign of life was queued; zero if none
	held     bool          // reading has stopped until Read makes room
	queued   bool          // a ping waits for the write goroutine
	seq      uint64        // payload of the latest ping
	sentAt   time.Time     // when the latest ping was written; zero once answered
	rtt      time.Duration // the last measured round-trip time
	batch    bool          // the write goroutine is writing a batch
	watch    *time.Timer   // ticks while batch is set; nil until the first batch
}

// newLiveness returns the liveness of c, or nil when it is off.
func newLiveness(c *Conn) *liveness {
	if c.cfg.pingInterval <= 0 || c.cfg.pongTimeout <= 0 {
		return nil
	}
	return &liveness{
		c:        c,
		interval: c.cfg.pingInterval,
		timeout:  c.cfg.pongTimeout,
	}
}

// expired reports whether the timeout has expired. The caller holds mu.
func (l *liveness) expired(now time.Time) bool {
	return !l.held && !l.waiting.IsZero() && now.Sub(l.waiting) >= l.timeout
}

// untilExpiry returns how long until the timeout may expire. While
// reading is held, the timeout restarts in full once Read makes room, so
// it cannot expire sooner than a full timeout from now. The caller holds
// mu.
func (l *liveness) untilExpiry(now time.Time) time.Duration {
	if l.waiting.IsZero() || l.held {
		return l.timeout
	}
	return l.waiting.Add(l.timeout).Sub(now)
}

// ping returns the payload of a new ping to queue, or nil when one is
// already awaiting an answer or the write goroutine has not yet written
// the previous one. The caller holds mu.
func (l *liveness) ping(now time.Time) []byte {
	if !l.waiting.IsZero() || l.queued {
		return nil
	}
	l.waiting = now
	l.queued = true
	l.seq++
	return binary.BigEndian.AppendUint64(nil, l.seq)
}

// pingWriting is called by the write goroutine just before it writes a
// ping.
func (l *liveness) pingWriting(payload []byte) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.queued = false
	if l.isLatest(payload) {
		l.sentAt = time.Now()
	}
}

// pong records the RTT when a pong answers the latest ping. Other pongs,
// unsolicited ones included, are ignored (RFC 6455 §5.5.3).
func (l *liveness) pong(payload []byte) {
	if l == nil {
		return
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.sentAt.IsZero() && l.isLatest(payload) {
		l.rtt = now.Sub(l.sentAt)
		l.sentAt = time.Time{}
	}
}

func (l *liveness) isLatest(payload []byte) bool {
	return len(payload) == 8 && binary.BigEndian.Uint64(payload) == l.seq
}

// alive records that data arrived from the peer.
func (l *liveness) alive() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.waiting = time.Time{}
}

// hold suspends the timeout while reading stops until Read makes room.
func (l *liveness) hold() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = true
}

// resume restarts the timeout once Read has made room. A pong read now may
// have waited during the hold, so the pending ping no longer measures the
// RTT.
func (l *liveness) resume() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = false
	if !l.waiting.IsZero() {
		l.waiting = time.Now()
	}
	l.sentAt = time.Time{}
}

// reader wraps r, the network connection, so that data read from it
// proves the peer alive.
func (l *liveness) reader(r io.Reader) io.Reader {
	if l == nil {
		return r
	}
	return lifeReader{r: r, l: l}
}

type lifeReader struct {
	r io.Reader
	l *liveness
}

func (lr lifeReader) Read(p []byte) (int, error) {
	n, err := lr.r.Read(p)
	if n > 0 {
		lr.l.alive()
	}
	return n, err
}
