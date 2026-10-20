package compare

import (
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/nbio/logging"
	"github.com/lesismal/nbio/nbhttp"
	"github.com/lesismal/nbio/nbhttp/websocket"
)

func init() {
	// nbio logs to standard output by default, where benchmark results and
	// the idle child's first line go.
	logging.Output = os.Stderr
	logging.SetLevel(logging.LevelError)
}

// nbio (github.com/lesismal/nbio/nbhttp/websocket) is event-driven: an
// nbhttp.Engine reads every connection from epoll poller goroutines
// (runtime.NumCPU()/4 of them, at least one), parses frames there and
// runs the OnMessage handler on a goroutine pool, one handler at a time
// per connection. No goroutine waits per connection, and the
// application never calls a read function. It is used that way here, in
// the default IOModNonBlocking, on both sides:
//
//   - the server is an nbhttp.Engine of its own per test server, not
//     httptest: a net/http server would put nbio in its blocking mode,
//     with a reading goroutine per connection;
//   - clients dial through one nbhttp.Engine shared by the process;
//   - receive installs the handler; readReuse, which only the client of
//     BenchmarkRoundTrip uses, hands each message from the handler to
//     the reading goroutine and holds the handler until the next
//     readReuse, so that the message is not copied;
//   - write is WriteMessage, which any goroutine may call. It writes in
//     the calling goroutine without blocking: what the socket does not
//     take at once, nbio queues without limit by default;
//   - the message buffer is returned to nbio's pool after the handler
//     when the engine has ReleaseWebsocketPayload (the reuse API), and
//     belongs to the application otherwise (the whole-message API). The
//     server chooses per test server, through serve's ownPayload. Client
//     connections never release it: the dialer does not set it.
//
// Settings: binary messages, compression off (nbio's default),
// MessageLengthLimit 2 MiB, and KeepaliveTime 0, so that the server sets
// no read deadline after every message: the other libraries have no
// timeout of that kind, and nwws liveness is off. Buffer sizes are
// nbio's defaults: 64 KiB read buffers per poller, not per connection,
// and frames of at most 32 KiB, so that a 64 KiB message goes out as 2
// frames and a 1 MiB message as 32.

// nbioConn is one side of an nbio connection. Its websocket.Conn holds it
// as the session.
type nbioConn struct {
	c *websocket.Conn

	mu   sync.Mutex
	h    func([]byte) error // receive's handler, or nil
	done chan error         // receive's result
	err  error              // why the connection closed

	closed chan struct{}
	msgs   chan []byte   // from the handler to readReuse
	ack    chan struct{} // from readReuse: the handler may return
	held   bool          // readReuse holds the handler
}

func newNbioConn(c *websocket.Conn) *nbioConn {
	_ = c.SetDeadline(time.Now().Add(opTimeout()))
	return &nbioConn{
		c:      c,
		closed: make(chan struct{}),
		msgs:   make(chan []byte),
		ack:    make(chan struct{}),
	}
}

// nbioOptions returns the handlers and settings of every connection.
func nbioOptions() *websocket.Upgrader {
	u := websocket.NewUpgrader()
	u.KeepaliveTime = 0
	u.MessageLengthLimit = readLimit
	u.OnOpen(func(c *websocket.Conn) { c.SetSession(newNbioConn(c)) })
	u.OnMessage(func(c *websocket.Conn, _ websocket.MessageType, p []byte) {
		c.Session().(*nbioConn).deliver(p)
	})
	u.OnClose(func(c *websocket.Conn, err error) {
		if x, ok := c.Session().(*nbioConn); ok {
			x.onClose(err)
		}
	})
	return u
}

// nbioServe starts an nbhttp.Engine that serves nbio connections on a
// loopback port; see serve.
func nbioServe(tb testing.TB, backlog int, ownPayload bool) (string, <-chan conn) {
	tb.Helper()
	accepted := make(chan conn, backlog)
	u := nbioOptions()
	var addr string
	engine := nbhttp.NewEngine(nbhttp.Config{
		Network:                 "tcp",
		Addrs:                   []string{"127.0.0.1:0"},
		ReleaseWebsocketPayload: !ownPayload,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := u.Upgrade(w, r, nil)
			if err != nil {
				return // the library has answered the request
			}
			accepted <- c.Session().(*nbioConn)
		}),
		Listen: func(network, a string) (net.Listener, error) {
			l, err := net.Listen(network, a)
			if err == nil {
				addr = l.Addr().String()
			}
			return l, err
		},
	})
	if err := engine.Start(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(engine.Stop)
	return "ws://" + addr + "/", accepted
}

// nbioClient is the engine of all client connections.
var nbioClient = sync.OnceValue(func() *nbhttp.Engine {
	engine := nbhttp.NewEngine(nbhttp.Config{ReleaseWebsocketPayload: true})
	if err := engine.Start(); err != nil {
		panic(err)
	}
	return engine
})

func nbioPrepare() { nbioClient() }

func nbioDial(url string) (conn, error) {
	d := websocket.Dialer{Engine: nbioClient(), Upgrader: nbioOptions(), DialTimeout: handshakeTimeout}
	c, _, err := d.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	return c.Session().(*nbioConn), nil
}

func (x *nbioConn) write(p []byte) error {
	return x.c.WriteMessage(websocket.BinaryMessage, p)
}

// deliver passes a message from nbio's handler to receive's handler or to
// readReuse.
func (x *nbioConn) deliver(p []byte) {
	x.mu.Lock()
	h, done := x.h, x.done
	x.mu.Unlock()
	if h == nil {
		select {
		case x.msgs <- p:
		case <-x.closed:
			return
		}
		select {
		case <-x.ack:
		case <-x.closed:
		}
		return
	}
	if err := h(p); err != nil {
		if err == errStop {
			x.finish(done, nil)
			return
		}
		x.close()
		x.finish(done, err)
	}
}

// finish ends the receive whose result is done, unless it has ended.
func (x *nbioConn) finish(done chan error, err error) {
	x.mu.Lock()
	if x.done != done {
		x.mu.Unlock()
		return
	}
	x.h, x.done = nil, nil
	x.mu.Unlock()
	done <- err
}

func (x *nbioConn) onClose(err error) {
	if err == nil {
		err = net.ErrClosed
	}
	x.mu.Lock()
	x.err = err
	done := x.done
	x.mu.Unlock()
	close(x.closed)
	if done != nil {
		x.finish(done, err)
	}
}

// receive ignores whole: the server's engine decides who owns a message.
func (x *nbioConn) receive(_ bool, f func([]byte) error) <-chan error {
	done := make(chan error, 1)
	x.mu.Lock()
	x.h, x.done = f, done
	x.mu.Unlock()
	return done
}

func (x *nbioConn) readReuse() ([]byte, error) {
	if x.held {
		x.held = false
		select {
		case x.ack <- struct{}{}:
		case <-x.closed:
		}
	}
	select {
	case p := <-x.msgs:
		x.held = true
		return p, nil
	case <-x.closed:
		x.mu.Lock()
		defer x.mu.Unlock()
		return nil, x.err
	}
}

func (x *nbioConn) idle() {}

func (x *nbioConn) close() { _ = x.c.Close() }
