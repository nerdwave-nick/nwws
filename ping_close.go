package nwws

import "time"

// livenessCloseWait bounds how long a liveness timeout waits to write its
// close frame; the close timeout bounds it too.
const livenessCloseWait = time.Second

// livenessCloseReason is the reason of the close frame a liveness timeout
// sends.
const livenessCloseReason = "liveness timeout"

// timeOut ends the connection after a liveness timeout. It records the
// timeout first, so that Read reports StatusAbnormalClosure with
// ErrLivenessTimeout, which is never sent on the wire. Then it queues a
// best-effort close frame StatusGoingAway through the write goroutine,
// waits a bounded time for it to be written, and closes the network
// connection without waiting for the peer's close frame.
func (l *liveness) timeOut() {
	c := l.c
	err := &CloseError{Code: StatusAbnormalClosure, err: ErrLivenessTimeout}
	c.setErr(err)
	defer c.closeNow(err)
	req, qerr := c.queueClose(closePayload(StatusGoingAway, livenessCloseReason))
	if qerr != nil {
		return // a close frame is already queued
	}
	wait := min(livenessCloseWait, c.cfg.closeTimeout)
	// The deadline also ends a network write the peer blocks; the timer
	// covers a connection that ignores deadlines.
	_ = c.nc.SetWriteDeadline(time.Now().Add(wait))
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-req.done:
	case <-c.closed:
	case <-t.C:
	}
}
