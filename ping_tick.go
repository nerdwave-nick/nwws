package nwws

import "time"

// timer starts the liveness timer of the write goroutine, which calls tick
// when it fires. It returns nil when liveness is off.
func (l *liveness) timer() *time.Timer {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nextPing = time.Now().Add(l.interval)
	return time.NewTimer(l.interval)
}

// tick queues a ping when one is due and enforces the timeout. It returns
// how long until it must run again, or false once it has closed the
// connection. Calling it early or twice is harmless.
func (l *liveness) tick(now time.Time) (time.Duration, bool) {
	l.mu.Lock()
	switch {
	case !l.closeBy.IsZero(): // timed out: waiting for the close frame
		closeBy := l.closeBy
		l.mu.Unlock()
		if now.Before(closeBy) {
			return closeBy.Sub(now), true
		}
		l.c.closeNow(livenessError())
		return 0, false
	case l.expired(now):
		wait := min(livenessCloseWait, l.c.cfg.closeTimeout)
		l.closeBy = now.Add(wait)
		l.mu.Unlock()
		return wait, l.timeOut(now.Add(wait))
	}
	var ping []byte
	if !now.Before(l.nextPing) {
		ping = l.ping(now)
		l.nextPing = now.Add(l.interval)
	}
	next := l.due(now)
	l.mu.Unlock()
	if ping != nil {
		_ = l.c.enqueue(&writeReq{op: opPing, payload: ping}) // fails once closing
	}
	return next, true
}

// due returns how long until tick must run. The caller holds mu.
func (l *liveness) due(now time.Time) time.Duration {
	if !l.closeBy.IsZero() {
		return l.closeBy.Sub(now)
	}
	return min(l.nextPing.Sub(now), l.untilExpiry(now))
}

// guard keeps liveness ticking while the write goroutine writes a batch:
// a peer that reads nothing can block that write, and the liveness timeout
// is what ends it. The watch timer ticks until unguard; it is a goroutine
// only while it fires.
func (l *liveness) guard() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batch = true
	d := l.due(time.Now())
	if l.watch == nil {
		l.watch = time.AfterFunc(d, l.watchFired)
		return
	}
	l.watch.Reset(d)
}

// unguard stops the watch timer once the batch is written.
func (l *liveness) unguard() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batch = false
	if l.watch != nil {
		l.watch.Stop()
	}
}

func (l *liveness) watchFired() {
	l.mu.Lock()
	batch := l.batch
	l.mu.Unlock()
	if !batch {
		return
	}
	d, ok := l.tick(time.Now())
	l.mu.Lock()
	defer l.mu.Unlock()
	if ok && l.batch {
		l.watch.Reset(d)
	}
}

// closeWritten closes the connection once the close frame of a liveness
// timeout is written, without waiting for the peer's close frame.
func (l *liveness) closeWritten() {
	if l == nil {
		return
	}
	l.mu.Lock()
	timedOut := !l.closeBy.IsZero()
	l.mu.Unlock()
	if timedOut {
		l.c.closeNow(livenessError())
	}
}
