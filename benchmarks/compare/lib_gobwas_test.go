package compare

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// gobwas/ws is used at the level of its wsutil package, the "high-level
// wrappers and helpers" its README starts with, not with raw frames:
//
//   - write: wsutil.WriteMessage, one frame per message, into a
//     bufio.Writer that is flushed after every message, under a mutex,
//     because gobwas leaves concurrency to the application;
//   - whole-message read: a new wsutil.Reader per message and
//     io.ReadAll, as wsutil.ReadData does (wsutil/helper.go:248-278);
//   - buffer-reusing read: one wsutil.Reader per connection, read into
//     a reused bytes.Buffer;
//   - control frames: wsutil.ControlHandler, writing under the mutex.
//
// gobwas has no buffering of its own at this level. The server reads and
// writes through the 4 KiB bufio.ReadWriter that ws.UpgradeHTTP returns
// from net/http's hijack; the client uses 4 KiB buffers of its own, the
// net/http and gorilla default. Without them every frame header would
// cost a read system call of its own. UTF-8 checks are on, as in
// wsutil.ReadData; they do not apply to binary messages. The read limit
// is wsutil.Reader.MaxFrameSize, which limits frames, not messages.

const gobwasBufferSize = 4096

type gobwasConn struct {
	nc    net.Conn
	state ws.State
	br    *bufio.Reader
	wmu   sync.Mutex // gobwas leaves concurrent writes to the application
	bw    *bufio.Writer
	rd    wsutil.Reader // for readReuse
	buf   bytes.Buffer
}

func newGobwasConn(nc net.Conn, state ws.State, br *bufio.Reader, bw *bufio.Writer) *gobwasConn {
	deadline := time.Now().Add(opTimeout())
	_ = nc.SetDeadline(deadline)
	x := &gobwasConn{nc: nc, state: state, br: br, bw: bw}
	x.rd = x.reader()
	return x
}

// reader returns a wsutil.Reader of x's messages.
func (x *gobwasConn) reader() wsutil.Reader {
	return wsutil.Reader{
		Source:         x.br,
		State:          x.state,
		CheckUTF8:      true,
		MaxFrameSize:   readLimit,
		OnIntermediate: x.control,
	}
}

// control answers a control frame: pong for ping, close for close.
func (x *gobwasConn) control(h ws.Header, r io.Reader) error {
	x.wmu.Lock()
	defer x.wmu.Unlock()
	err := wsutil.ControlHandler{Src: r, Dst: x.bw, State: x.state, DisableSrcCiphering: true}.Handle(h)
	if ferr := x.bw.Flush(); err == nil {
		err = ferr
	}
	return err
}

func gobwasUpgrade(w http.ResponseWriter, r *http.Request) (conn, error) {
	nc, rw, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		return nil, err
	}
	return newGobwasConn(nc, ws.StateServerSide, rw.Reader, rw.Writer), nil
}

func gobwasDial(url string) (conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), handshakeTimeout)
	defer cancel()
	nc, br, _, err := ws.Dial(ctx, url)
	if err != nil {
		return nil, err
	}
	if br == nil { // the server sent nothing after its response
		br = bufio.NewReaderSize(nc, gobwasBufferSize)
	}
	return newGobwasConn(nc, ws.StateClientSide, br, bufio.NewWriterSize(nc, gobwasBufferSize)), nil
}

func (x *gobwasConn) write(p []byte) error {
	x.wmu.Lock()
	defer x.wmu.Unlock()
	if err := wsutil.WriteMessage(x.bw, x.state, ws.OpBinary, p); err != nil {
		return err
	}
	return x.bw.Flush()
}

// next prepares rd for the next data message, answering control frames
// on the way.
func (x *gobwasConn) next(rd *wsutil.Reader) error {
	for {
		h, err := rd.NextFrame()
		if err != nil {
			return err
		}
		if !h.OpCode.IsControl() {
			return nil
		}
		if err := x.control(h, rd); err != nil {
			return err
		}
	}
}

func (x *gobwasConn) read() ([]byte, error) {
	rd := x.reader()
	if err := x.next(&rd); err != nil {
		return nil, err
	}
	return io.ReadAll(&rd)
}

func (x *gobwasConn) readReuse() ([]byte, error) {
	if err := x.next(&x.rd); err != nil {
		return nil, err
	}
	x.buf.Reset()
	_, err := x.buf.ReadFrom(&x.rd)
	return x.buf.Bytes(), err
}

func (x *gobwasConn) close() { _ = x.nc.Close() }

func (x *gobwasConn) receive(whole bool, f func([]byte) error) <-chan error {
	return pullReceive(x, whole, f)
}

func (x *gobwasConn) idle() { pullIdle(x) }
