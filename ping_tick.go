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
// is what ends it. The watch timer then ticks in the write goroutine's
// place; it is a goroutine only while it fires. Once armed, it stays armed
// across batches until it fires, so a busy writer rarely touches it.
func (l *liveness) guard() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batch = true
	now := time.Now()
	at := now.Add(l.due(now))
	if !l.watchAt.IsZero() && !l.watchAt.After(at) {
		return // armed early enough
	}
	l.watchAt = at
	if l.watch == nil {
		l.watch = time.AfterFunc(at.Sub(now), l.watchFired)
		return
	}
	l.watch.Reset(at.Sub(now))
}

// unguard ends the watch once the batch is written. The timer is left to
// fire, and then does nothing.
func (l *liveness) unguard() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batch = false
}

// stopWatch stops the watch timer when the write goroutine ends.
func (l *liveness) stopWatch() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.watch != nil {
		l.watch.Stop()
	}
	l.watchAt = time.Time{}
}

func (l *liveness) watchFired() {
	l.mu.Lock()
	if !l.batch {
		l.watchAt = time.Time{}
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	now := time.Now()
	d, ok := l.tick(now)
	l.mu.Lock()
	defer l.mu.Unlock()
	if ok && l.batch {
		l.watchAt = now.Add(d)
		l.watch.Reset(d)
		return
	}
	l.watchAt = time.Time{}
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
