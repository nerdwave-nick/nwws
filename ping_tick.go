package nwws

import "time"

// start starts the liveness timer, which calls tick when it fires and is
// a goroutine only then.
func (l *liveness) start() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nextPing = time.Now().Add(l.interval)
	l.armed = true
	l.ticker = time.AfterFunc(l.interval, l.fire)
}

// fire runs tick and sets the timer for its next run. Once the timer is
// stopped, it runs no tick; see stop.
func (l *liveness) fire() {
	l.mu.Lock()
	l.armed = false
	if l.stopped {
		done := l.settle()
		l.mu.Unlock()
		if done {
			close(l.c.writeDone)
		}
		return
	}
	l.firing = true
	l.mu.Unlock()
	next, ok := l.tick(time.Now())
	l.mu.Lock()
	l.firing = false
	if ok && !l.stopped {
		l.armed = true
		l.ticker.Reset(next)
	}
	done := l.settle()
	l.mu.Unlock()
	if done {
		close(l.c.writeDone) // the last thing the callback does
	}
}

// stop stops the liveness timer for good, once no frame can be written
// any more. It closes the connection's writeDone once no timer callback
// runs or is to come: at once, or when the callback that runs, or whose
// timer has just fired, returns. A tick can close the connection, so its
// callback may still run when the write side finishes. The caller holds
// wmu.
func (l *liveness) stop() {
	l.mu.Lock()
	l.stopped = true
	if l.armed && l.ticker.Stop() {
		l.armed = false
	}
	done := l.settle()
	l.mu.Unlock()
	if done {
		close(l.c.writeDone)
	}
}

// settle reports, once, that writeDone is to be closed: the timer is
// stopped, and no callback runs or is to come. The caller holds mu.
func (l *liveness) settle() bool {
	if !l.stopped || l.firing || l.armed || l.settled {
		return false
	}
	l.settled = true
	return true
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
		l.nextPing = l.nextSlot(now)
	}
	next := l.due(now)
	l.mu.Unlock()
	if ping != nil {
		_ = l.c.enqueue(&writeReq{op: opPing, payload: ping}) // fails once closing
	}
	return next, true
}

// nextSlot returns when the next ping is due once the one due at nextPing
// has been handled at now: an interval after nextPing, so that a late tick
// does not delay the pings after it, skipping the slots it missed. The
// caller holds mu.
func (l *liveness) nextSlot(now time.Time) time.Time {
	next := l.nextPing.Add(l.interval)
	if !now.Before(next) {
		next = next.Add((now.Sub(next)/l.interval + 1) * l.interval)
	}
	return next
}

// due returns how long until tick must run. The caller holds mu.
func (l *liveness) due(now time.Time) time.Duration {
	if !l.closeBy.IsZero() {
		return l.closeBy.Sub(now)
	}
	return min(l.nextPing.Sub(now), l.untilExpiry(now))
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
