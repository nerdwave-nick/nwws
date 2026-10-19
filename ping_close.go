package nwws

import "time"

// livenessCloseWait bounds how long a liveness timeout waits to write its
// close frame; the close timeout bounds it too.
const livenessCloseWait = time.Second

// livenessCloseReason is the reason of the close frame a liveness timeout
// sends.
const livenessCloseReason = "liveness timeout"

// livenessError is the error a connection ends with after a liveness
// timeout: StatusAbnormalClosure with ErrLivenessTimeout, which is never
// sent on the wire.
func livenessError() error {
	return &CloseError{Code: StatusAbnormalClosure, err: ErrLivenessTimeout}
}

// timeOut ends the connection after a liveness timeout. It records the
// timeout first, so that Read reports livenessError. Then it queues a
// best-effort close frame StatusGoingAway through the write goroutine,
// which closes the network connection once the frame is written, or at
// closeBy, without waiting for the peer's close frame; see closeWritten
// and tick. It reports false if it closed the connection itself, because
// a close frame was already queued.
func (l *liveness) timeOut(closeBy time.Time) bool {
	c := l.c
	err := livenessError()
	c.setErr(err)
	if _, qerr := c.queueClose(closePayload(StatusGoingAway, livenessCloseReason)); qerr != nil {
		c.closeNow(err) // a close frame is already queued
		return false
	}
	// The deadline also ends a network write the peer blocks; closeBy
	// covers a connection that ignores deadlines.
	_ = c.nc.SetWriteDeadline(closeBy)
	return true
}
