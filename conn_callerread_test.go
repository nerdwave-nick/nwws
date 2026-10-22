package nwws

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tlsPair is tcpPair with TLS on top.
func tlsPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	s, cl := tcpPair(t)
	cert, _ := newClientCert(t)
	ts := tls.Server(s, &tls.Config{Certificates: []tls.Certificate{cert}})
	tc := tls.Client(cl, &tls.Config{InsecureSkipVerify: true})
	errc := make(chan error, 1)
	go func() { errc <- ts.Handshake() }()
	if err := tc.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	return ts, tc
}

// clientText returns the client frames of a text message split into the
// given fragments.
func clientText(t *testing.T, frags ...[]byte) []byte {
	t.Helper()
	return clientMessage(t, opText, frags...)
}

// clientMessage returns the client frames of a message of type op split
// into the given fragments.
func clientMessage(t *testing.T, typ opcode, frags ...[]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	fw := frameWriter{w: &buf}
	for i, f := range frags {
		op := opContinuation
		if i == 0 {
			op = typ
		}
		if err := fw.writeFrame(i == len(frags)-1, 0, op, f); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}

// readEither calls ReadInto with buf when into is set, and Read otherwise.
func readEither(ctx context.Context, c *Conn, into bool, buf []byte) ([]byte, error) {
	if into {
		_, p, err := c.ReadInto(ctx, buf)
		return p, err
	}
	_, p, err := c.Read(ctx)
	return p, err
}

// A Read that is cancelled while it reads frames itself leaves the stream
// where it stopped: in a header, in a payload, between the fragments of a
// message or in the middle of a rune. The next Read gets the whole
// message, and the connection stays in sync, over TCP and TLS, for Read
// and ReadInto, for text and binary messages.
func TestReadCancelledMidFrame(t *testing.T) {
	for _, tr := range []struct {
		name string
		pair func(*testing.T) (net.Conn, net.Conn)
	}{{"tcp", tcpPair}, {"tls", tlsPair}} {
		for _, tc := range []struct {
			into bool
			typ  opcode
		}{{false, opText}, {true, opText}, {false, opBinary}, {true, opBinary}} {
			into := tc.into
			name := tr.name + "/Read"
			if into {
				name = tr.name + "/ReadInto"
			}
			name += "/" + MessageType(tc.typ).String()
			t.Run(name, func(t *testing.T) {
				s, cl := tr.pair(t)
				c := newConn(s, true, "", testConfig())
				t.Cleanup(func() { c.closeNow(ErrClosed); c.wait() })
				peer := newRawPeer(t, cl, false)
				a, b := bytes.Repeat([]byte("a"), 3000), bytes.Repeat([]byte("é"), 2000)
				wire := clientMessage(t, tc.typ, a, b)
				want := append(bytes.Clone(a), b...)
				buf := make([]byte, 0, 1<<16)
				// Cut inside the first header, inside the first payload,
				// between the fragments, inside the second header, and
				// inside the second payload within a rune: the second
				// header takes 8 bytes, so 3008+8+1 splits the first é.
				cuts := []int{1, 500, 3008, 3010, 3017, len(wire)}
				prev := 0
				for i, cut := range cuts {
					peer.sendRaw(wire[prev:cut])
					prev = cut
					if i == len(cuts)-1 {
						break
					}
					ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
					_, err := readEither(ctx, c, into, buf)
					cancel()
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("cut at %d: Read = %v, want DeadlineExceeded", cut, err)
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
				defer cancel()
				got, err := readEither(ctx, c, into, buf)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("Read = %d bytes, %v; want the whole message of %d bytes", len(got), err, len(want))
				}
				peer.send(true, opBinary, []byte("next"))
				mustRead(t, c, MessageBinary, []byte("next"))
			})
		}
	}
}

// A Read pending on an idle connection reads frames in the caller's
// goroutine: the connection keeps no read goroutine of its own then.
func TestPendingReadNeedsNoReadGoroutine(t *testing.T) {
	const conns = 20
	before := runtime.NumGoroutine()
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait) // after the messages below end the Reads
	for range conns {
		c, peer := startConn(t, true, testConfig())
		wg.Go(func() {
			// Pending for longer than waitFor polls.
			ctx, cancel := context.WithTimeout(t.Context(), 4*testTimeout)
			defer cancel()
			_, _, _ = c.Read(ctx)
		})
		t.Cleanup(func() { peer.send(true, opBinary, nil) })
	}
	// Per connection: the pending Read and the write goroutine. A read
	// goroutine would add one more each; the slack is for goroutines of
	// earlier tests still ending.
	const slack = conns / 2
	waitFor(t, "the read goroutines to end", func() bool {
		return runtime.NumGoroutine() <= before+2*conns+slack
	})
}

// slowWriteConn delays each network write and counts the bytes written.
type slowWriteConn struct {
	net.Conn
	delay   time.Duration
	written atomic.Int64
}

func (s *slowWriteConn) Write(p []byte) (int, error) {
	time.Sleep(s.delay)
	n, err := s.Conn.Write(p)
	s.written.Add(int64(n))
	return n, err
}

// Read reports the peer's close only once our close frame answering it is
// on the wire, so an application that returns then has completed its half
// of the closing handshake, whoever reads frames.
func TestReadReportsPeerCloseAfterAnswer(t *testing.T) {
	for _, reader := range []string{"Read", "read goroutine"} {
		t.Run(reader, func(t *testing.T) {
			s, cl := tcpPair(t)
			sw := &slowWriteConn{Conn: s, delay: 100 * time.Millisecond}
			c := newConn(sw, true, "", testConfig())
			t.Cleanup(func() { c.closeNow(ErrClosed); c.wait() })
			peer := newRawPeer(t, cl, false)
			if reader == "read goroutine" {
				peer.send(true, opBinary, []byte("queued"))
				waitQueuedMessages(t, c, 1)
				mustRead(t, c, MessageBinary, []byte("queued"))
			}
			peer.send(true, opClose, closePayload(StatusNormalClosure, "bye"))
			_, _, err := readMsg(t, c)
			assertCloseError(t, err, StatusNormalClosure, "bye")
			if n := sw.written.Load(); n == 0 {
				t.Fatal("Read reported the peer's close before our close frame was written")
			}
			peer.expectClose(StatusNormalClosure)
		})
	}
}

// When ReadInto's buffer is too small for the message, it returns memory
// the caller owns. The connection never uses the caller's buffer again,
// even after reading part of the message into it.
func TestReadIntoBufferTooSmall(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	first, second := bytes.Repeat([]byte("1"), 100), bytes.Repeat([]byte("2"), 900)
	peer.sendRaw(clientText(t, first, second))
	buf := make([]byte, 0, 200) // the first fragment fits, the message does not
	_, got, err := readInto(t, c, buf)
	if err != nil {
		t.Fatalf("ReadInto: %v", err)
	}
	want := append(bytes.Clone(first), second...)
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadInto = %d bytes, want %d", len(got), len(want))
	}
	if sameArray(got, buf[:1]) {
		t.Fatal("ReadInto returned the too small buffer")
	}
	sentinel := bytes.Repeat([]byte("x"), cap(buf))
	copy(buf[:cap(buf)], sentinel)
	for i := range 50 {
		msg := bytes.Repeat([]byte{byte('a' + i%26)}, 150)
		peer.send(true, opBinary, msg)
		if i%2 == 0 {
			mustReadInto(t, c, nil, MessageBinary, msg)
		} else {
			mustRead(t, c, MessageBinary, msg)
		}
	}
	if !bytes.Equal(buf[:cap(buf)], sentinel) {
		t.Fatal("the connection wrote into a buffer ReadInto had given back")
	}
	if !bytes.Equal(got, want) {
		t.Fatal("a later read changed the message ReadInto returned")
	}
}

// Pings are answered while the application is not reading: after the
// takeover delay, the read goroutine reads frames, before the first Read
// and between Reads.
func TestPingsAnsweredWithoutRead(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	peer.send(true, opPing, []byte("before"))
	peer.expectFrame(opPong, []byte("before"))

	peer.send(true, opBinary, []byte("msg"))
	mustRead(t, c, MessageBinary, []byte("msg"))
	peer.send(true, opPing, []byte("between"))
	peer.expectFrame(opPong, []byte("between"))
}

// A Read that holds no message answers pings while it waits for one.
func TestPendingReadAnswersPings(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	done := readAsync(t, c)
	waitFor(t, "the Read to read frames", func() bool {
		c.rmu.Lock()
		defer c.rmu.Unlock()
		return c.role == roleRead
	})
	peer.send(true, opPing, []byte("p"))
	peer.expectFrame(opPong, []byte("p"))
	peer.send(true, opBinary, []byte("m"))
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Read: %v", err)
	}
}

// Concurrent Reads take every message exactly once, and each Read gets a
// later message than the previous Read of its goroutine.
func TestConcurrentReadsInOrder(t *testing.T) {
	const readers, count = 4, 2000
	c, peer := startConn(t, true, testConfig())
	sizes := make([]int, count)
	for i := range sizes {
		sizes[i] = 4 + i%500
	}
	written := writeAsync(peer, clientFrames(t, sizes))
	seen := make([]atomic.Int32, count)
	var taken atomic.Int64
	errs := make(chan error, readers)
	for r := range readers {
		go func() {
			last := -1
			var buf []byte
			for {
				ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
				p, err := readEither(ctx, c, r%2 == 1, buf)
				cancel()
				var ce *CloseError
				switch {
				case errors.As(err, &ce) && taken.Load() == count:
					errs <- nil
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
				if r%2 == 1 {
					buf = p
				}
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

// noDeadlineConn is a network connection without read deadlines.
type noDeadlineConn struct{ net.Conn }

func (noDeadlineConn) SetReadDeadline(time.Time) error { return errors.ErrUnsupported }

// A connection whose read deadline cannot be set keeps reading frames in
// a goroutine of its own, so a cancelled Read still returns at once and
// keeps the connection usable, and pings are answered.
func TestReadFallbackWithoutDeadlines(t *testing.T) {
	s, cl := tcpPair(t)
	c := newConn(noDeadlineConn{s}, true, "", testConfig())
	t.Cleanup(func() { c.closeNow(ErrClosed); c.wait() })
	peer := newRawPeer(t, cl, false)

	for range 3 {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		_, _, err := c.Read(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Read = %v, want DeadlineExceeded", err)
		}
	}
	peer.send(true, opPing, []byte("p"))
	peer.expectFrame(opPong, []byte("p"))
	peer.send(true, opBinary, []byte("m"))
	mustRead(t, c, MessageBinary, []byte("m"))
	c.rmu.Lock()
	role := c.role
	c.rmu.Unlock()
	if role != roleLoop {
		t.Fatalf("reader role %d, want the read goroutine (%d)", role, roleLoop)
	}
}

// A Read that waits while the read goroutine holds the reader role is
// woken when the read goroutine hands the role over, also when the
// message the read goroutine queued last went to another Read. A
// read-ahead budget of one byte makes the read goroutine wait for Read
// after each message, which is when that happens. Otherwise each message
// waits for the takeover delay; the messages are small, so that reading
// them takes far less, also with the race detector.
func TestHandOffWakesWaitingRead(t *testing.T) {
	const count, size = 200, 64
	c, peer := startConn(t, true, readAheadConfig(1))
	waitFor(t, "the read goroutine to read frames", func() bool {
		c.rmu.Lock()
		defer c.rmu.Unlock()
		return c.role == roleLoop
	})
	sizes := make([]int, count)
	for i := range sizes {
		sizes[i] = size
	}
	written := writeAsync(peer, clientFrames(t, sizes))
	start := time.Now()
	for range count {
		if _, _, err := readMsg(t, c); err != nil {
			t.Fatal(err)
		}
	}
	// Each message waiting for the takeover delay would take at least
	// that long.
	if d := time.Since(start); d >= count*takeoverDelay*3/4 {
		t.Fatalf("%d messages took %v: Reads waited for the takeover delay", count, d)
	}
	if err := waitErr(t, written); err != nil {
		t.Fatalf("peer write: %v", err)
	}
}

// Once the connection is closed, whatever the reason, the read side ends
// without waiting for the takeover delay: closing starts the read
// goroutine at once when nobody reads frames.
func TestCloseStartsReadGoroutine(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	peer.send(true, opBinary, []byte("m"))
	mustRead(t, c, MessageBinary, []byte("m"))
	c.closeNow(ErrClosed) // as after a failed write or a liveness timeout
	c.rmu.Lock()
	role := c.role
	c.rmu.Unlock()
	if role == roleFree {
		t.Fatal("the reader role stays free after the connection closed")
	}
	c.wait()
}

// roleOf returns who holds the reader role.
func roleOf(c *Conn) int {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	return c.role
}

// Large messages are read ahead: a Read that has read one itself gives
// the reader role to the read goroutine at once, which keeps it while
// large messages follow, also when Reads wait. After a small message, a
// waiting Read takes the role back and reads frames itself.
func TestLargeMessagesReadAhead(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	small, large := []byte("small"), bytes.Repeat([]byte("L"), readAheadSize)

	peer.send(true, opBinary, small)
	mustRead(t, c, MessageBinary, small)
	if r := roleOf(c); r != roleFree {
		t.Fatalf("after a small message read by Read: role %d, want free (%d)", r, roleFree)
	}

	peer.send(true, opBinary, large)
	mustRead(t, c, MessageBinary, large)
	if r := roleOf(c); r != roleLoop && r != roleEnded {
		t.Fatalf("after a large message read by Read: role %d, want the read goroutine (%d)", r, roleLoop)
	}

	// Reads that wait get large messages from the read goroutine, which
	// keeps the role.
	for range 3 {
		done := readAsync(t, c)
		waitFor(t, "the Read to wait", func() bool {
			c.rmu.Lock()
			defer c.rmu.Unlock()
			return c.readers == 1
		})
		peer.send(true, opBinary, large)
		if err := waitErr(t, done); err != nil {
			t.Fatalf("Read: %v", err)
		}
		if r := roleOf(c); r != roleLoop {
			t.Fatalf("after a large message queued for a waiting Read: role %d, want the read goroutine (%d)", r, roleLoop)
		}
	}

	// A small message for a waiting Read gives the role back.
	done := readAsync(t, c)
	waitFor(t, "the Read to wait", func() bool {
		c.rmu.Lock()
		defer c.rmu.Unlock()
		return c.readers == 1
	})
	peer.send(true, opBinary, small)
	if err := waitErr(t, done); err != nil {
		t.Fatalf("Read: %v", err)
	}
	waitFor(t, "the read goroutine to hand the role over", func() bool {
		r := roleOf(c)
		return r == roleFree || r == roleRead
	})
	peer.send(true, opBinary, small)
	mustRead(t, c, MessageBinary, small)
	if r := roleOf(c); r != roleFree {
		t.Fatalf("after a small message read by Read again: role %d, want free (%d)", r, roleFree)
	}
}

// ReadInto reads large messages itself, straight into the caller's buffer:
// unlike Read, it leaves the reader role free after one instead of giving
// it to the read goroutine to read ahead. When the read goroutine holds
// the role, after a large message for Read, it hands the role to a waiting
// ReadInto with the next large message.
func TestLargeMessagesReadIntoInCaller(t *testing.T) {
	c, peer := startConn(t, true, testConfig())
	large := bytes.Repeat([]byte("L"), readAheadSize)
	buf := make([]byte, 0, 2*readAheadSize)

	// readLarge has a ReadInto wait, holding the role or waiting for the
	// read goroutine, before the peer sends a large message: masking it
	// may take longer than the takeover delay.
	readLarge := func(what string) {
		t.Helper()
		type result struct {
			typ MessageType
			msg []byte
			err error
		}
		done := make(chan result, 1)
		go func() {
			typ, msg, err := readInto(t, c, buf)
			done <- result{typ, msg, err}
		}()
		waitFor(t, "the ReadInto to wait", func() bool {
			c.rmu.Lock()
			defer c.rmu.Unlock()
			return c.role == roleRead || c.readers == 1
		})
		peer.send(true, opBinary, large)
		var r result
		select {
		case r = <-done:
		case <-time.After(testTimeout):
			t.Fatalf("%s: ReadInto did not return", what)
		}
		if r.err != nil || r.typ != MessageBinary || !bytes.Equal(r.msg, large) || !sameArray(r.msg, buf) {
			t.Fatalf("%s: ReadInto = %v, %d bytes in buf %v, %v; want %v, %d bytes in buf",
				what, r.typ, len(r.msg), sameArray(r.msg, buf), r.err, MessageBinary, len(large))
		}
		if role := roleOf(c); role != roleFree {
			t.Fatalf("after %s: role %d, want free (%d)", what, role, roleFree)
		}
	}

	readLarge("a large message read by ReadInto")
	readLarge("another large message read by ReadInto")

	// A large message for Read starts the read goroutine.
	peer.send(true, opBinary, large)
	mustRead(t, c, MessageBinary, large)
	if r := roleOf(c); r != roleLoop {
		t.Fatalf("after a large message read by Read: role %d, want the read goroutine (%d)", r, roleLoop)
	}
	readLarge("a large message queued for a waiting ReadInto")
	readLarge("a large message read by ReadInto after the hand-over")
}
