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
// closes the connection (see timeOut) when, after a ping, nothing arrives
// from the peer within the timeout. Any data received proves the peer alive, not only
// the pong: a pong may wait behind a large frame on a slow link.
//
// While a complete message waits for Read, the read goroutine reads no
// frames and cannot see the peer's pongs. The timeout is suspended then,
// and restarts in full when Read takes the message, so a slow application
// never makes a live peer time out.
type liveness struct {
	c        *Conn
	interval time.Duration
	timeout  time.Duration
	done     chan struct{} // closed when run ends

	mu      sync.Mutex
	waiting time.Time     // when the ping awaiting a sign of life was queued; zero if none
	held    bool          // a complete message waits for Read
	queued  bool          // a ping waits for the write goroutine
	seq     uint64        // payload of the latest ping
	sentAt  time.Time     // when the latest ping was written; zero once answered
	rtt     time.Duration // the last measured round-trip time
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
		done:     make(chan struct{}),
	}
}

func (l *liveness) start() {
	if l != nil {
		go l.run()
	}
}

// wait waits for the liveness goroutine to end.
func (l *liveness) wait() {
	if l != nil {
		<-l.done
	}
}

// run pings every interval and enforces the timeout until the connection
// closes.
func (l *liveness) run() {
	defer close(l.done)
	nextPing := time.Now().Add(l.interval)
	t := time.NewTimer(l.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-l.c.closed:
			return
		}
		now := time.Now()
		if l.expired(now) {
			l.timeOut()
			return
		}
		if !now.Before(nextPing) {
			l.ping(now)
			nextPing = now.Add(l.interval)
		}
		t.Reset(min(nextPing.Sub(now), l.untilExpiry(now)))
	}
}

func (l *liveness) expired(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.held && !l.waiting.IsZero() && now.Sub(l.waiting) >= l.timeout
}

// untilExpiry returns how long until the timeout may expire. While a
// message is held, the timeout restarts in full once Read takes it, so it
// cannot expire sooner than a full timeout from now.
func (l *liveness) untilExpiry(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.waiting.IsZero() || l.held {
		return l.timeout
	}
	return l.waiting.Add(l.timeout).Sub(now)
}

// ping queues a ping, unless one is already awaiting an answer or the
// write goroutine has not yet written the previous one.
func (l *liveness) ping(now time.Time) {
	l.mu.Lock()
	if !l.waiting.IsZero() || l.queued {
		l.mu.Unlock()
		return
	}
	l.waiting = now
	l.queued = true
	l.seq++
	payload := binary.BigEndian.AppendUint64(nil, l.seq)
	l.mu.Unlock()
	_ = l.c.enqueue(&writeReq{op: opPing, payload: payload}) // fails once closing
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

// hold suspends the timeout while a message waits for Read.
func (l *liveness) hold() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = true
}

// resume restarts the timeout once Read has taken the held message. A pong
// read now may have waited during the hold, so the pending ping no longer
// measures the RTT.
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
