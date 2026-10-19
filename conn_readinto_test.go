package nwws

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

func readInto(t *testing.T, c *Conn, buf []byte) (MessageType, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	return c.ReadInto(ctx, buf)
}

func mustReadInto(t *testing.T, c *Conn, buf []byte, wantType MessageType, want []byte) []byte {
	t.Helper()
	typ, got, err := readInto(t, c, buf)
	if err != nil || typ != wantType || !bytes.Equal(got, want) {
		t.Fatalf("ReadInto = %v %q %v, want %v %q", typ, got, err, wantType, want)
	}
	return got
}

// sameArray reports whether a and b have room for a byte and share their
// first byte.
func sameArray(a, b []byte) bool {
	return cap(a) > 0 && cap(b) > 0 && &a[:1][0] == &b[:1][0]
}

// ReadInto reads each message into buf[:0], in buf's memory, and grows it
// only when its capacity is too small.
func TestReadInto(t *testing.T) {
	roles(t, func(t *testing.T, server bool) {
		c, peer := startConn(t, server, testConfig())
		long := strings.Repeat("x", 100)
		sendAll(peer, []frame{
			{true, opText, "hello"},
			{false, opBinary, "frag"},
			{true, opPing, "p"},
			{true, opContinuation, "mented"},
			{true, opBinary, ""},
			{true, opBinary, long},
			{true, opText, "fits again"},
		})
		buf := make([]byte, 3, 64)
		got := mustReadInto(t, c, buf, MessageText, []byte("hello"))
		if !sameArray(got, buf) {
			t.Fatal("ReadInto did not read into buf, which has room")
		}
		got = mustReadInto(t, c, got, MessageBinary, []byte("fragmented"))
		if !sameArray(got, buf) {
			t.Fatal("ReadInto did not read a fragmented message into buf")
		}
		got = mustReadInto(t, c, got, MessageBinary, nil)
		if len(got) != 0 || cap(got) != cap(buf) {
			t.Fatalf("empty message: %d/%d bytes, want buf[:0]", len(got), cap(got))
		}
		got = mustReadInto(t, c, got, MessageBinary, []byte(long))
		if cap(got) < len(long) {
			t.Fatalf("grown buffer has capacity %d", cap(got))
		}
		grown := got
		got = mustReadInto(t, c, got, MessageText, []byte("fits again"))
		if !sameArray(got, grown) {
			t.Fatal("ReadInto did not reuse the grown buffer")
		}
		peer.expectFrame(opPong, []byte("p"))
	})
}

// ReadInto ends and fails as Read does, and keeps the caller's buffer.
func TestReadIntoErrors(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		c, peer := startConn(t, true, testConfig())
		buf := make([]byte, 5, 32)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		_, got, err := c.ReadInto(ctx, buf)
		if !errors.Is(err, context.DeadlineExceeded) || len(got) != 0 || !sameArray(got, buf) {
			t.Fatalf("ReadInto = %d bytes, %v, want buf[:0] and the deadline", len(got), err)
		}
		peer.send(true, opBinary, []byte("later"))
		mustReadInto(t, c, got, MessageBinary, []byte("later"))
	})
	roles(t, func(t *testing.T, server bool) {
		t.Run("peer close", func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			peer.send(true, opText, []byte("last"))
			peer.send(true, opClose, closePayload(StatusNormalClosure, "bye"))
			buf := mustReadInto(t, c, nil, MessageText, []byte("last"))
			for range 2 {
				_, got, err := readInto(t, c, buf)
				assertCloseError(t, err, StatusNormalClosure, "bye")
				if len(got) != 0 || !sameArray(got, buf) {
					t.Fatalf("ReadInto returned %d bytes on error, want buf[:0]", len(got))
				}
			}
		})
		t.Run("violation in order", func(t *testing.T) {
			for range 20 {
				c, peer := startConn(t, server, testConfig())
				peer.send(true, opText, []byte("hello"))
				peer.sendRaw(reservedOpcodeFrame(server))

				buf := mustReadInto(t, c, make([]byte, 0, 16), MessageText, []byte("hello"))
				mustWrite(t, c, MessageText, []byte("hello"))
				peer.expectFrame(opText, []byte("hello"))
				_, _, err := readInto(t, c, buf)
				assertProtocolError(t, err, StatusProtocolError)
				peer.expectClose(StatusProtocolError)
			}
		})
		t.Run("CloseNow", func(t *testing.T) {
			c, _ := startConn(t, server, testConfig())
			done := make(chan error, 1)
			go func() {
				_, _, err := c.ReadInto(t.Context(), make([]byte, 0, 16))
				done <- err
			}()
			time.Sleep(20 * time.Millisecond) // let ReadInto wait
			if err := c.CloseNow(); err != nil {
				t.Fatalf("CloseNow: %v", err)
			}
			if err := waitErr(t, done); !errors.Is(err, ErrClosed) {
				t.Fatalf("pending ReadInto = %v, want ErrClosed", err)
			}
		})
	})
}

// numberedFrames encodes count binary messages of varying sizes up to max
// as the peer of a Conn on the given side sends them, the i-th holding i
// in its first 4 bytes.
func numberedFrames(t *testing.T, server bool, count, max int) (wire []byte, sizes []int) {
	t.Helper()
	var b bytes.Buffer
	fw := frameWriter{w: &b, server: !server}
	for i := range count {
		n := 4 + (i*7919)%(max-3)
		p := make([]byte, n)
		binary.BigEndian.PutUint32(p, uint32(i))
		p[n-1] = byte(i)
		if err := fw.writeFrame(true, 0, opBinary, p); err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, n)
	}
	return b.Bytes(), sizes
}

// checkNumbered checks that p is message i of numberedFrames.
func checkNumbered(t *testing.T, i int, p []byte, sizes []int) {
	t.Helper()
	if len(p) != sizes[i] || binary.BigEndian.Uint32(p) != uint32(i) || p[len(p)-1] != byte(i) {
		t.Fatalf("message %d: %d bytes, want %d", i, len(p), sizes[i])
	}
}

// queuedData returns the data capacity the read-ahead queue accounts for
// and the capacity its messages actually hold.
func queuedData(c *Conn) (counted, held int) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for i := range c.rq.n {
		held += cap(c.rq.buf[(c.rq.head+i)%len(c.rq.buf)].data)
	}
	return c.rq.data, held
}

// With ReadInto, read-ahead stays within its budget, which counts what the
// queued messages hold, also when their buffers come from the pool. Read
// and ReadInto mix, and a slice Read returned is never reused.
func TestReadIntoReadAhead(t *testing.T) {
	const budget, max, count = 16 << 10, 3000, 1500
	c, peer, cc := startCounted(t, readAheadConfig(budget))
	wire, sizes := numberedFrames(t, true, count, max)

	// Fill the pool with buffers of the sizes to come.
	written := writeAsync(peer, wire)
	buf := make([]byte, 0, max)
	for i := range count {
		var err error
		_, buf, err = readInto(t, c, buf)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		checkNumbered(t, i, buf, sizes)
	}
	if err := waitErr(t, written); err != nil {
		t.Fatalf("peer write: %v", err)
	}

	before := cc.n.Load()
	written = writeAsync(peer, wire)
	read := stopped(t, c, cc) - before
	if limit := budget + max + 4096; read > int64(limit) {
		t.Fatalf("read %d bytes ahead, want at most %d", read, limit)
	}
	counted, held := queuedData(c)
	if counted != held {
		t.Fatalf("the queue accounts for %d bytes of data, its messages hold %d", counted, held)
	}
	if n, mem := queued(c); mem+entrySize <= budget || mem > budget+int(DefaultReadLimit) {
		t.Fatalf("%d messages queued in %d bytes, want the budget %d filled", n, mem, budget)
	}

	var owned [][]byte
	for i := range count {
		if i%3 == 0 {
			_, p, err := readMsg(t, c)
			if err != nil {
				t.Fatalf("message %d: %v", i, err)
			}
			if cap(p) != len(p) {
				t.Fatalf("Read returned %d bytes with capacity %d, want no more", len(p), cap(p))
			}
			owned = append(owned, p)
			continue
		}
		var err error
		_, buf, err = readInto(t, c, buf)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		checkNumbered(t, i, buf, sizes)
	}
	if err := waitErr(t, written); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	for k, p := range owned {
		checkNumbered(t, 3*k, p, sizes)
	}
	if counted, held := queuedData(c); counted != 0 || held != 0 {
		t.Fatalf("drained queue accounts for %d bytes, holds %d", counted, held)
	}
}

// ReadInto reads compressed messages, decompressed as their frames arrive,
// with control frames between fragments, and reports a violation in a
// compressed message in order.
func TestReadIntoCompressed(t *testing.T) {
	roles(t, func(t *testing.T, server bool) {
		c, peer := startDeflateConn(t, server, deflateOn())
		pc := newPeerCompressor(t, true)
		small := compressibleText(10 << 10)
		big := compressibleText(200 << 10)

		peer.sendFrame(true, rsv1Bit, opText, pc.compress(t, small))
		z := pc.compress(t, big)
		third := len(z) / 3
		peer.sendFrame(false, rsv1Bit, opBinary, z[:third])
		peer.send(true, opPing, []byte("p"))
		peer.sendFrame(false, 0, opContinuation, z[third:2*third])
		peer.sendFrame(true, 0, opContinuation, z[2*third:])
		peer.send(true, opBinary, []byte("plain"))
		peer.sendFrame(true, rsv1Bit, opText, pc.compress(t, small))
		peer.sendFrame(true, rsv1Bit, opText, pc.compress(t, []byte("grüß \xff dich")))

		buf := make([]byte, 0, 256<<10)
		got := mustReadInto(t, c, buf, MessageText, small)
		got = mustReadInto(t, c, got, MessageBinary, big)
		got = mustReadInto(t, c, got, MessageBinary, []byte("plain"))
		got = mustReadInto(t, c, got, MessageText, small)
		if !sameArray(got, buf) {
			t.Fatal("ReadInto did not read into buf, which has room")
		}
		peer.expectFrame(opPong, []byte("p"))
		_, _, err := readInto(t, c, got)
		assertProtocolError(t, err, StatusInvalidFramePayloadData)
		peer.expectClose(StatusInvalidFramePayloadData)
	})
}

// The pool hands out buffers within the requested limits, and keeps none
// far larger than a message typically is.
func TestMessageBufferPool(t *testing.T) {
	putBuf(make([]byte, 1000))
	for range 4 {
		if b := getBuf(600, 700); len(b) != 600 || cap(b) > 700 {
			t.Fatalf("getBuf(600, 700) = %d/%d bytes", len(b), cap(b))
		}
	}
	for range 4 {
		putBuf(make([]byte, 2*maxPooledBuffer))
		putBuf(make([]byte, maxPooledBuffer+1))
	}
	for k := range msgBufs {
		for {
			p, _ := msgBufs[k].Get().(*[]byte)
			if p == nil {
				break
			}
			if cap(*p) > maxPooledBuffer {
				t.Fatalf("the pool kept a buffer of %d bytes, more than %d", cap(*p), maxPooledBuffer)
			}
		}
	}
}

// peerFrames encodes count binary messages of n bytes as the peer of a
// Conn on the given side sends them.
func peerFrames(t *testing.T, server bool, n, count int) []byte {
	t.Helper()
	var wire bytes.Buffer
	fw := frameWriter{w: &wire, server: !server}
	p := make([]byte, n)
	for range count {
		if err := fw.writeFrame(true, 0, opBinary, p); err != nil {
			t.Fatal(err)
		}
	}
	return wire.Bytes()
}

// writeMessages writes count binary messages of n bytes from the peer, in
// batches of about 64 KiB: many small writes make loopback TCP very slow
// with GOMAXPROCS=1. It returns how many messages it writes, at least
// count.
func writeMessages(t *testing.T, p *rawPeer, server bool, n, count int) (int, <-chan error) {
	t.Helper()
	perBatch := max(1, (64<<10)/n)
	batches := (count + perBatch - 1) / perBatch
	batch := peerFrames(t, server, n, perBatch)
	done := make(chan error, 1)
	go func() {
		_ = p.nc.SetWriteDeadline(time.Now().Add(2 * testTimeout))
		for range batches {
			if _, err := p.nc.Write(batch); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	return batches * perBatch, done
}

// In steady state, ReadInto into a large enough buffer allocates nothing
// per message, on either side. The first half of the messages warms the
// pool up, until it holds as many buffers as read-ahead keeps in flight.
//
// The test runs with GOMAXPROCS=1, so that the result does not depend on
// sync.Pool's caches per P: with more Ps, a buffer put on one P is taken
// first by goroutines on that P, so the pool needs up to one buffer more
// per P to warm up. BenchmarkReadInto measures that case.
func TestReadIntoAllocations(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops buffers on purpose under the race detector")
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	roles(t, func(t *testing.T, server bool) {
		for _, size := range []struct {
			name     string
			n, count int
		}{{"300B", 300, 20000}, {"64KiB", 64 << 10, 1000}, {"1MiB", 1 << 20, 200}} {
			t.Run(size.name, func(t *testing.T) {
				cfg := testConfig()
				cfg.readLimit = 2 << 20
				c, peer := startConn(t, server, cfg)
				count, written := writeMessages(t, peer, server, size.n, size.count)
				buf := make([]byte, 0, size.n)
				read := func(count int) {
					for range count {
						var err error
						if _, buf, err = c.ReadInto(t.Context(), buf); err != nil {
							t.Fatal(err)
						}
						if len(buf) != size.n {
							t.Fatalf("ReadInto returned %d bytes, want %d", len(buf), size.n)
						}
					}
				}
				// Collect the test's garbage first: a collection empties
				// sync.Pool, after which the pool warms up again.
				runtime.GC()
				warm := count / 2
				read(warm)
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				read(count - warm)
				runtime.ReadMemStats(&after)
				if err := waitErr(t, written); err != nil {
					t.Fatalf("peer write: %v", err)
				}
				msgs := count - warm
				allocs := after.Mallocs - before.Mallocs
				t.Logf("%d allocations, %d bytes for %d messages", allocs, after.TotalAlloc-before.TotalAlloc, msgs)
				if float64(allocs)/float64(msgs) > 0.01 {
					t.Fatalf("%d allocations for %d messages, want none per message", allocs, msgs)
				}
			})
		}
	})
}
