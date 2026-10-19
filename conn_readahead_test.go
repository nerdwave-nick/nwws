package nwws

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// readAheadConfig is testConfig with a read-ahead budget of budget bytes.
func readAheadConfig(budget int) connConfig {
	cfg := testConfig()
	cfg.readBuffer = budget
	return cfg
}

// countingConn counts the bytes read from the network connection, which
// shows how far the connection has read ahead.
type countingConn struct {
	net.Conn
	n atomic.Int64
}

func (cc *countingConn) Read(p []byte) (int, error) {
	n, err := cc.Conn.Read(p)
	cc.n.Add(int64(n))
	return n, err
}

// startCounted is startConn on the server side with a countingConn under
// the connection.
func startCounted(t *testing.T, cfg connConfig) (*Conn, *rawPeer, *countingConn) {
	t.Helper()
	s, cl := tcpPair(t)
	cc := &countingConn{Conn: s}
	c := newConn(cc, true, "", cfg)
	t.Cleanup(func() {
		c.closeNow(ErrClosed)
		c.wait()
	})
	return c, newRawPeer(t, cl, false), cc
}

// setBuffers sets the connection's receive buffer and the peer's send
// buffer. Small buffers make the peer's writes block soon once the
// connection stops reading, but slow loopback TCP down a lot.
func setBuffers(t *testing.T, cc *countingConn, p *rawPeer, size int) {
	t.Helper()
	if err := cc.Conn.(*net.TCPConn).SetReadBuffer(size); err != nil {
		t.Fatal(err)
	}
	if err := p.nc.(*net.TCPConn).SetWriteBuffer(size); err != nil {
		t.Fatal(err)
	}
}

// clientFrames encodes messages as masked binary frames, as a client sends
// them, the i-th holding i in its first 4 bytes.
func clientFrames(t *testing.T, sizes []int) []byte {
	t.Helper()
	var wire bytes.Buffer
	fw := frameWriter{w: &wire}
	for i, n := range sizes {
		p := make([]byte, n)
		if n >= 4 {
			binary.BigEndian.PutUint32(p, uint32(i))
		}
		if err := fw.writeFrame(true, 0, opBinary, p); err != nil {
			t.Fatal(err)
		}
	}
	return wire.Bytes()
}

// writeAsync writes wire to the peer's network connection in the
// background.
func writeAsync(p *rawPeer, wire []byte) <-chan error {
	done := make(chan error, 1)
	go func() {
		_ = p.nc.SetWriteDeadline(time.Now().Add(2 * testTimeout))
		_, err := p.nc.Write(wire)
		done <- err
	}()
	return done
}

// stopped waits until the connection has stopped reading because its
// queue holds the budget, and returns how many bytes it has read.
func stopped(t *testing.T, c *Conn, cc *countingConn) int64 {
	t.Helper()
	waitFor(t, "the connection to stop reading", func() bool {
		c.rmu.Lock()
		defer c.rmu.Unlock()
		return c.rwait == waitRoom
	})
	return cc.n.Load()
}

// waitRead waits until the connection has read want bytes from the
// network.
func waitRead(t *testing.T, cc *countingConn, want int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("the connection to read ahead %d bytes", want), func() bool {
		return cc.n.Load() >= int64(want)
	})
}

// queued returns the number of messages queued for Read and the memory
// the queue holds.
func queued(c *Conn) (n, mem int) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	return c.rq.n, c.rq.mem()
}

// waitQueuedMessages waits until n messages are queued for Read.
func waitQueuedMessages(t *testing.T, c *Conn, n int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("%d queued messages", n), func() bool {
		got, _ := queued(c)
		return got == n
	})
}

// assertNoFrame checks that the peer receives no frame for a while.
func assertNoFrame(t *testing.T, p *rawPeer, d time.Duration) {
	t.Helper()
	_ = p.nc.SetReadDeadline(time.Now().Add(d))
	if h, b, err := p.fr.readFrame(); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("peer got opcode %d payload %q err %v, want no frame", h.opcode, b, err)
	}
}

// The queue keeps messages in order across wrapping and growing, accounts
// for their memory, and grows its ring only within the budget.
func TestMsgQueue(t *testing.T) {
	const budget = 1 << 20
	var q msgQueue
	var want [][]byte
	next := 0
	push := func(k int) {
		for range k {
			if !q.fits(budget) {
				t.Fatalf("queue of %d messages in %d bytes refuses a message", q.n, q.mem())
			}
			p := make([]byte, next%7, next%7+next%3)
			q.push(message{typ: MessageBinary, data: p}, budget)
			want = append(want, p)
			next++
		}
	}
	pop := func(k int) {
		for range k {
			m := q.pop()
			if len(m.data) != len(want[0]) || cap(m.data) != cap(want[0]) {
				t.Fatalf("popped %d/%d bytes, want %d/%d", len(m.data), cap(m.data), len(want[0]), cap(want[0]))
			}
			want = want[1:]
		}
	}
	check := func() {
		t.Helper()
		data := 0
		for _, p := range want {
			data += cap(p)
		}
		if q.n != len(want) || q.mem() != data+len(q.buf)*entrySize {
			t.Fatalf("queue of %d messages in %d bytes, want %d messages, %d bytes of data and a ring of %d", q.n, q.mem(), len(want), data, len(q.buf))
		}
	}
	for _, step := range [][2]int{{3, 2}, {5, 4}, {9, 3}, {20, 25}, {300, 100}, {10, 210}, {1, 0}} {
		push(step[0])
		check()
		pop(step[1])
		check()
	}
	pop(len(want))
	check()

	// An emptied ring is kept while it is small against the budget, so a
	// queue that keeps filling and draining does not allocate each time,
	// and given back when it is not.
	q.shrink(budget)
	if len(q.buf) == 0 {
		t.Fatal("an empty queue gave back a ring small against the budget")
	}
	ring := &q.buf[0]
	for range 3 {
		push(400)
		pop(400)
		q.shrink(budget)
	}
	if len(q.buf) == 0 || &q.buf[0] != ring {
		t.Fatal("filling and draining the queue reallocated its ring")
	}
	q.shrink(len(q.buf) * entrySize)
	if len(q.buf) != 0 {
		t.Fatalf("an empty queue keeps a ring of %d against a budget of %d bytes", len(q.buf), len(q.buf)*entrySize)
	}

	// The ring grows only as far as the budget allows.
	small := 10 * entrySize
	q = msgQueue{}
	for q.fits(small) {
		q.push(message{}, small)
	}
	if q.mem() > small || q.mem()+entrySize <= small {
		t.Fatalf("empty messages fill %d bytes of a budget of %d", q.mem(), small)
	}
	q.clear()
	if q.n != 0 || q.mem() != 0 {
		t.Fatalf("cleared queue holds %d messages in %d bytes", q.n, q.mem())
	}
}

func TestConnOptionsReadBuffer(t *testing.T) {
	for _, tc := range []struct {
		opts *ConnOptions
		want int
	}{
		{nil, DefaultReadBuffer},
		{&ConnOptions{}, DefaultReadBuffer},
		{&ConnOptions{ReadBuffer: -1}, DefaultReadBuffer},
		{&ConnOptions{ReadBuffer: 10}, 10},
	} {
		if got := tc.opts.config().readBuffer; got != tc.want {
			t.Errorf("%+v: read buffer %d, want %d", tc.opts, got, tc.want)
		}
	}
}

// Both peers run short liveness timeouts. One application delays Read
// well past the other side's pong timeout, with less than the budget
// queued: its connection keeps reading and answering pings, so the
// connection survives and every message arrives in order.
func TestReadAheadKeepsAnsweringPings(t *testing.T) {
	const interval, timeout = 10 * time.Millisecond, 50 * time.Millisecond
	for _, slowServer := range []bool{false, true} {
		t.Run(map[bool]string{false: "client reads late", true: "server reads late"}[slowServer], func(t *testing.T) {
			s, cl := tcpPair(t)
			srv := newConn(s, true, "", liveConfig(interval, timeout))
			cli := newConn(cl, false, "", liveConfig(interval, timeout))
			t.Cleanup(func() {
				srv.closeNow(ErrClosed)
				cli.closeNow(ErrClosed)
				srv.wait()
				cli.wait()
			})
			slow, fast := cli, srv
			if slowServer {
				slow, fast = srv, cli
			}
			type result struct {
				p   []byte
				err error
			}
			fastRead := make(chan result, 1)
			go func() {
				_, p, err := fast.Read(t.Context())
				fastRead <- result{p, err}
			}()

			const n = 5
			for i := range n {
				mustWrite(t, fast, MessageBinary, fmt.Appendf(nil, "missive %d", i))
			}
			time.Sleep(6 * timeout) // the slow application does not call Read
			select {
			case r := <-fastRead:
				t.Fatalf("the other side's connection ended while Read was delayed: %v", r.err)
			default:
			}
			for i := range n {
				mustRead(t, slow, MessageBinary, fmt.Appendf(nil, "missive %d", i))
			}
			mustWrite(t, slow, MessageText, []byte("still here"))
			select {
			case r := <-fastRead:
				if r.err != nil || string(r.p) != "still here" {
					t.Fatalf("other side's Read = %q %v, want the reply", r.p, r.err)
				}
			case <-time.After(testTimeout):
				t.Fatal("the reply never arrived")
			}
		})
	}
}

// Below the budget the connection keeps reading, so the liveness timeout
// is not suspended by an unread message: a silent peer is still dropped.
// Read then returns the message received before the timeout, and then
// the timeout.
func TestLivenessRunsWhileReadingAhead(t *testing.T) {
	const interval, timeout = 20 * time.Millisecond, 100 * time.Millisecond
	c, peer := startConn(t, true, liveConfig(interval, timeout))
	peer.send(true, opBinary, []byte("unread"))
	if reason := peer.expectClose(StatusGoingAway); reason != livenessCloseReason {
		t.Fatalf("close reason %q, want %q", reason, livenessCloseReason)
	}
	mustRead(t, c, MessageBinary, []byte("unread"))
	_, _, err := readMsg(t, c)
	assertLivenessTimeout(t, err)
}

// With the budget full the connection stops reading and cannot see the
// peer's pongs, so the liveness timeout is suspended until Read makes
// room, and runs again after.
func TestLivenessHeldWhileBudgetFull(t *testing.T) {
	const interval, timeout = 20 * time.Millisecond, 100 * time.Millisecond
	cfg := liveConfig(interval, timeout)
	cfg.readBuffer = 1 // one message fills it
	c, peer := startConn(t, true, cfg)
	peer.send(true, opBinary, []byte("fills the budget"))

	// The peer never answers pings; while the budget is full, the
	// connection does not time it out.
	until := time.Now().Add(4 * timeout)
	for time.Now().Before(until) {
		_ = peer.nc.SetReadDeadline(until)
		h, _, err := peer.fr.readFrame()
		if errors.Is(err, os.ErrDeadlineExceeded) {
			break
		}
		if err != nil || h.opcode == opClose {
			t.Fatalf("peer got opcode %d err %v while the budget was full, want only pings", h.opcode, err)
		}
	}
	mustRead(t, c, MessageBinary, []byte("fills the budget"))
	start := time.Now()
	peer.expectClose(StatusGoingAway)
	if elapsed := time.Since(start); elapsed > interval+timeout+250*time.Millisecond {
		t.Fatalf("timed out %v after Read made room, want about %v", elapsed, timeout)
	}
}

// Pings are answered while a message waits for Read, unless the message
// fills the budget: then the connection stops reading until Read takes it.
// A message larger than the budget is still taken when the queue is empty.
func TestReadAheadAnswersPings(t *testing.T) {
	t.Run("below budget", func(t *testing.T) {
		c, peer := startConn(t, true, testConfig())
		peer.send(true, opBinary, []byte("waits"))
		peer.send(true, opPing, []byte("p"))
		peer.expectFrame(opPong, []byte("p"))
		mustRead(t, c, MessageBinary, []byte("waits"))
	})
	t.Run("budget full", func(t *testing.T) {
		c, peer := startConn(t, true, readAheadConfig(1<<10))
		big := bytes.Repeat([]byte("x"), 64<<10)
		peer.send(true, opBinary, big)
		peer.send(true, opPing, []byte("p"))
		assertNoFrame(t, peer, 200*time.Millisecond)
		mustRead(t, c, MessageBinary, big)
		peer.expectFrame(opPong, []byte("p"))
		peer.send(true, opBinary, big)
		mustRead(t, c, MessageBinary, big)
	})
}

// With the budget full the connection stops reading, so TCP pushes back
// on the peer. The connection reads at most the budget plus one message
// ahead, and resumes once Read drains the queue.
func TestReadAheadBackPressure(t *testing.T) {
	const budget, size, count = 64 << 10, 8 << 10, 512 // 4 MiB in all
	c, peer, cc := startCounted(t, readAheadConfig(budget))
	setBuffers(t, cc, peer, 4<<10)
	sizes := make([]int, count)
	for i := range sizes {
		sizes[i] = size
	}
	wire := clientFrames(t, sizes)
	written := writeAsync(peer, wire)

	read := stopped(t, c, cc)
	if _, mem := queued(c); mem+entrySize <= budget || mem > budget+size {
		t.Fatalf("queue holds %d bytes, want the budget %d filled by at most one message of %d bytes", mem, budget, size)
	}
	select {
	case err := <-written:
		t.Fatalf("the peer wrote all %d bytes (err %v), want it pushed back", len(wire), err)
	default:
	}
	frame := len(wire) / count
	if limit := budget + frame + 4096; read < budget/2 || read > int64(limit) {
		t.Fatalf("read %d bytes ahead, want between %d and %d", read, budget/2, limit)
	}
	setBuffers(t, cc, peer, 1<<20) // read the rest at full speed

	for i := range count {
		_, p, err := readMsg(t, c)
		if err != nil || len(p) != size || binary.BigEndian.Uint32(p) != uint32(i) {
			t.Fatalf("message %d: %d bytes, err %v", i, len(p), err)
		}
	}
	if err := waitErr(t, written); err != nil {
		t.Fatalf("peer write: %v", err)
	}
}

// The connection reads ahead at most the budget plus one message, also
// for a flood of empty messages, which count against the budget too.
func TestReadAheadMemoryBound(t *testing.T) {
	const budget = 16 << 10
	for _, tc := range []struct {
		name  string
		size  int
		count int
	}{
		{"300 B messages", 300, 4000},
		{"empty messages", 0, 50000},
		{"messages larger than the budget", 40 << 10, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer, cc := startCounted(t, readAheadConfig(budget))
			sizes := make([]int, tc.count)
			for i := range sizes {
				sizes[i] = tc.size
			}
			wire := clientFrames(t, sizes)
			written := writeAsync(peer, wire)

			read := stopped(t, c, cc)
			frame := len(wire) / tc.count
			if limit := budget + frame + 4096; read > int64(limit) {
				t.Fatalf("read %d bytes ahead, want at most %d", read, limit)
			}
			n, mem := queued(c)
			if mem > budget+tc.size || mem+entrySize <= budget {
				t.Fatalf("%d messages queued in %d bytes, want the budget %d filled by at most one message of %d bytes", n, mem, budget, tc.size)
			}
			for i := range tc.count {
				_, p, err := readMsg(t, c)
				if err != nil || len(p) != tc.size {
					t.Fatalf("message %d: %d bytes, err %v", i, len(p), err)
				}
			}
			if err := waitErr(t, written); err != nil {
				t.Fatalf("peer write: %v", err)
			}
		})
	}
}

// A violation found while reading ahead waits in the queue behind the
// messages before it: its close frame goes out only after Read has
// returned them and then the violation.
func TestViolationAfterQueuedMessages(t *testing.T) {
	c, peer, cc := startCounted(t, testConfig())
	wire := append(clientFrames(t, []int{10, 20, 30}), reservedOpcodeFrame(true)...)
	peer.sendRaw(wire)
	waitRead(t, cc, len(wire))
	waitQueuedMessages(t, c, 4) // and the violation
	assertNoFrame(t, peer, 100*time.Millisecond)

	for i, n := range []int{10, 20, 30} {
		_, p, err := readMsg(t, c)
		if err != nil || len(p) != n {
			t.Fatalf("message %d: %d bytes, err %v", i, len(p), err)
		}
	}
	_, _, err := readMsg(t, c)
	assertProtocolError(t, err, StatusProtocolError)
	peer.expectClose(StatusProtocolError)
}

// Close drops the messages read ahead and not yet read.
func TestCloseDropsQueuedMessages(t *testing.T) {
	c, peer, cc := startCounted(t, testConfig())
	wire := clientFrames(t, []int{10, 20, 30})
	peer.sendRaw(wire)
	waitRead(t, cc, len(wire))
	waitQueuedMessages(t, c, 3)

	closed := closeAsync(c, StatusNormalClosure, "")
	peer.expectClose(StatusNormalClosure)
	peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
	if err := waitErr(t, closed); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if typ, p, err := readMsg(t, c); err == nil {
		t.Fatalf("Read after Close = %v %d bytes, want the queued messages dropped", typ, len(p))
	}
	if n, mem := queued(c); n != 0 || mem != 0 {
		t.Fatalf("%d messages in %d bytes still queued after Close", n, mem)
	}
}

// When the peer closes while messages are queued, the closing handshake
// completes at once, and Read still returns the messages in order, then
// the peer's close.
func TestReadAfterPeerCloseReturnsQueued(t *testing.T) {
	c, peer, cc := startCounted(t, testConfig())
	var wire bytes.Buffer
	fw := frameWriter{w: &wire}
	for _, m := range []string{"m1", "m2"} {
		if err := fw.writeFrame(true, 0, opText, []byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	if err := fw.writeFrame(true, 0, opClose, closePayload(StatusNormalClosure, "bye")); err != nil {
		t.Fatal(err)
	}
	peer.sendRaw(wire.Bytes())
	peer.expectClose(StatusNormalClosure)
	peer.expectEOF()
	waitRead(t, cc, wire.Len())

	mustRead(t, c, MessageText, []byte("m1"))
	mustRead(t, c, MessageText, []byte("m2"))
	_, _, err := readMsg(t, c)
	assertCloseError(t, err, StatusNormalClosure, "bye")
}

// Reading a message allocates the message and nothing else.
func TestReadAllocations(t *testing.T) {
	const size, runs = 300, 2000
	c, peer := startConn(t, true, testConfig())
	sizes := make([]int, runs+1)
	for i := range sizes {
		sizes[i] = size
	}
	written := writeAsync(peer, clientFrames(t, sizes))
	allocs := testing.AllocsPerRun(runs, func() {
		if _, _, err := c.Read(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
	if err := waitErr(t, written); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if allocs > 1 {
		t.Fatalf("%v allocations per message, want 1", allocs)
	}
}

// Concurrent Reads, some of them giving up on short deadlines, take every
// message exactly once, each Read in queue order.
func TestReadAheadConcurrentReads(t *testing.T) {
	const readers, count = 4, 4000
	c, peer := startConn(t, true, readAheadConfig(4<<10))
	sizes := make([]int, count)
	for i := range sizes {
		sizes[i] = 4 + i%300
	}
	written := writeAsync(peer, clientFrames(t, sizes))

	var taken atomic.Int64
	seen := make([]atomic.Int32, count)
	errs := make(chan error, readers)
	for r := range readers {
		go func() {
			last := -1
			for i := 0; ; i++ {
				ctx, cancel := context.WithTimeout(t.Context(), time.Duration(i%3)*time.Millisecond)
				if r == 0 { // one Read that never gives up
					ctx, cancel = context.WithTimeout(t.Context(), testTimeout)
				}
				_, p, err := c.Read(ctx)
				cancel()
				var ce *CloseError
				switch {
				case errors.Is(err, context.DeadlineExceeded) && r != 0:
					continue
				case errors.As(err, &ce) && taken.Load() == count:
					errs <- nil // the peer closed after the last message
					return
				case err != nil:
					errs <- err
					return
				}
				idx := int(binary.BigEndian.Uint32(p))
				if idx <= last || len(p) != sizes[idx] {
					errs <- fmt.Errorf("reader %d got message %d (%d bytes) after %d", r, idx, len(p), last)
					return
				}
				last = idx
				seen[idx].Add(1)
				taken.Add(1)
			}
		}()
	}
	waitFor(t, "every message to be read", func() bool { return taken.Load() == count })
	if err := waitErr(t, written); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	peer.send(true, opClose, closePayload(StatusNormalClosure, ""))
	for range readers {
		if err := waitErr(t, errs); err != nil {
			t.Fatal(err)
		}
	}
	for i := range seen {
		if n := seen[i].Load(); n != 1 {
			t.Fatalf("message %d read %d times", i, n)
		}
	}
}

// Messages queued together while several Reads wait wake as many of them:
// none is left in the queue while a Read waits.
func TestReadAheadWakesWaitingReads(t *testing.T) {
	const readers, bursts = 4, 200
	c, _ := startConn(t, true, testConfig())
	for i := range bursts {
		got := make(chan error, readers)
		for range readers {
			go func() {
				_, _, err := readMsg(t, c)
				got <- err
			}()
		}
		waitFor(t, "the Reads to wait", func() bool {
			c.rmu.Lock()
			defer c.rmu.Unlock()
			return c.readers == readers
		})
		for range readers { // faster than a waiting Read wakes up
			c.pushMessage(message{typ: MessageBinary})
		}
		for range readers {
			if err := <-got; err != nil {
				t.Fatalf("burst %d: Read: %v", i, err)
			}
		}
	}
}
