package compare

import (
	"bytes"
	"context"
	"flag"
	"net/http"
	"sync"
	"time"

	coder "github.com/coder/websocket"
	gorilla "github.com/gorilla/websocket"
	"github.com/nerdwave-nick/nwws"
)

// readLimit is the read limit of every connection: above the largest
// message, 1 MiB.
const readLimit = 2 << 20

// handshakeTimeout bounds each opening handshake.
const handshakeTimeout = 10 * time.Second

// opTimeout is the deadline of every connection, counted from its opening:
// two minutes, or longer for a -benchtime duration above about a minute.
var opTimeout = sync.OnceValue(func() time.Duration {
	d := 2 * time.Minute
	if f := flag.Lookup("test.benchtime"); f != nil {
		if bt, err := time.ParseDuration(f.Value.String()); err == nil {
			d = max(d, 2*bt+30*time.Second)
		}
	}
	return d
})

// conn is one side of a WebSocket connection of one library.
type conn interface {
	// write sends p as one binary message. Any goroutine may call it.
	write(p []byte) error
	// read returns the next message, read with the library's
	// whole-message API.
	read() ([]byte, error)
	// readReuse returns the next message, read into a buffer that the
	// conn reuses. The message is valid until the next readReuse.
	readReuse() ([]byte, error)
	// close closes the network connection, without the closing handshake.
	close()
}

// library opens connections of one WebSocket library.
type library struct {
	name    string
	upgrade func(w http.ResponseWriter, r *http.Request) (conn, error)
	dial    func(url string) (conn, error)
}

var libraries = []library{
	{"nwws", nwwsUpgrade, nwwsDial},
	{"coder", coderUpgrade, coderDial},
	{"gorilla", gorillaUpgrade, gorillaDial},
}

// nwws

var nwwsOptions = nwws.ConnOptions{ReadLimit: readLimit, DisableLiveness: true}

type nwwsConn struct {
	c      *nwws.Conn
	ctx    context.Context
	cancel context.CancelFunc
	buf    []byte
}

func newNWWSConn(c *nwws.Conn) *nwwsConn {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout())
	return &nwwsConn{c: c, ctx: ctx, cancel: cancel}
}

func nwwsUpgrade(w http.ResponseWriter, r *http.Request) (conn, error) {
	c, err := nwws.Accept(w, r, &nwws.AcceptOptions{ConnOptions: nwwsOptions})
	if err != nil {
		return nil, err
	}
	return newNWWSConn(c), nil
}

func nwwsDial(url string) (conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), handshakeTimeout)
	defer cancel()
	c, _, err := nwws.Dial(ctx, url, &nwws.DialOptions{ConnOptions: nwwsOptions})
	if err != nil {
		return nil, err
	}
	return newNWWSConn(c), nil
}

func (x *nwwsConn) write(p []byte) error {
	return x.c.Write(x.ctx, nwws.MessageBinary, p)
}

func (x *nwwsConn) read() ([]byte, error) {
	_, p, err := x.c.Read(x.ctx)
	return p, err
}

func (x *nwwsConn) readReuse() ([]byte, error) {
	_, p, err := x.c.ReadInto(x.ctx, x.buf)
	x.buf = p
	return p, err
}

func (x *nwwsConn) close() {
	x.cancel()
	_ = x.c.CloseNow()
}

// coder

type coderConn struct {
	c      *coder.Conn
	ctx    context.Context
	cancel context.CancelFunc
	buf    bytes.Buffer
}

func newCoderConn(c *coder.Conn) *coderConn {
	c.SetReadLimit(readLimit)
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout())
	return &coderConn{c: c, ctx: ctx, cancel: cancel}
}

func coderUpgrade(w http.ResponseWriter, r *http.Request) (conn, error) {
	c, err := coder.Accept(w, r, &coder.AcceptOptions{CompressionMode: coder.CompressionDisabled})
	if err != nil {
		return nil, err
	}
	return newCoderConn(c), nil
}

func coderDial(url string) (conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), handshakeTimeout)
	defer cancel()
	c, _, err := coder.Dial(ctx, url, &coder.DialOptions{CompressionMode: coder.CompressionDisabled})
	if err != nil {
		return nil, err
	}
	return newCoderConn(c), nil
}

func (x *coderConn) write(p []byte) error {
	return x.c.Write(x.ctx, coder.MessageBinary, p)
}

func (x *coderConn) read() ([]byte, error) {
	_, p, err := x.c.Read(x.ctx)
	return p, err
}

func (x *coderConn) readReuse() ([]byte, error) {
	_, r, err := x.c.Reader(x.ctx)
	if err != nil {
		return nil, err
	}
	x.buf.Reset()
	_, err = x.buf.ReadFrom(r)
	return x.buf.Bytes(), err
}

func (x *coderConn) close() {
	x.cancel()
	_ = x.c.CloseNow()
}

// gorilla

var (
	gorillaUpgrader = gorilla.Upgrader{EnableCompression: false}
	gorillaDialer   = gorilla.Dialer{HandshakeTimeout: handshakeTimeout, EnableCompression: false}
)

type gorillaConn struct {
	c   *gorilla.Conn
	wmu sync.Mutex // gorilla allows one concurrent writer
	buf bytes.Buffer
}

func newGorillaConn(c *gorilla.Conn) *gorillaConn {
	c.SetReadLimit(readLimit)
	deadline := time.Now().Add(opTimeout())
	_ = c.SetReadDeadline(deadline)
	_ = c.SetWriteDeadline(deadline)
	return &gorillaConn{c: c}
}

func gorillaUpgrade(w http.ResponseWriter, r *http.Request) (conn, error) {
	c, err := gorillaUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return newGorillaConn(c), nil
}

func gorillaDial(url string) (conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), handshakeTimeout)
	defer cancel()
	c, _, err := gorillaDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	return newGorillaConn(c), nil
}

func (x *gorillaConn) write(p []byte) error {
	x.wmu.Lock()
	defer x.wmu.Unlock()
	return x.c.WriteMessage(gorilla.BinaryMessage, p)
}

func (x *gorillaConn) read() ([]byte, error) {
	_, p, err := x.c.ReadMessage()
	return p, err
}

func (x *gorillaConn) readReuse() ([]byte, error) {
	_, r, err := x.c.NextReader()
	if err != nil {
		return nil, err
	}
	x.buf.Reset()
	_, err = x.buf.ReadFrom(r)
	return x.buf.Bytes(), err
}

func (x *gorillaConn) close() {
	_ = x.c.Close() // closes the network connection only
}
