# nwws

[![CI](https://github.com/nerdwave-nick/nwws/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nerdwave-nick/nwws/actions/workflows/ci.yml)

A WebSocket ([RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)) client and
server for Go, built only on the standard library. Released under the
[MIT License](LICENSE).

```sh
go get github.com/nerdwave-nick/nwws
```

## Status and comparison

nwws is young. It is at v0.2.0, has one author and no production users yet, and its API may still change before v1. Treat it accordingly.

So far it has been compared with [coder/websocket](https://github.com/coder/websocket) v1.8.15, [gorilla/websocket](https://github.com/gorilla/websocket) v1.5.3 and [gobwas/ws](https://github.com/gobwas/ws) v1.4.0, in October 2026. Other Go implementations have not been compared, including:
- [gws](https://github.com/lxzan/gws);
- [fasthttp/websocket](https://github.com/fasthttp/websocket);
- `golang.org/x/net/websocket`.

Nothing here claims nwws is better than those.

**Conformance.** All five libraries ran the Autobahn test suite with the same configuration, as server and as client. Each cell shows server / client:

| Library | Plain, 301 cases per side | permessage-deflate, 216 cases per side |
| --- | --- | --- |
| nwws v0.2.0 | 298 OK, 3 INFORMATIONAL / the same | 216 OK / 216 OK |
| coder/websocket v1.8.15 | 222 OK, 76 FAILED, 3 INFORMATIONAL / the same | 180 OK, 36 UNIMPLEMENTED / 144 OK, 72 UNIMPLEMENTED |
| gorilla/websocket v1.5.3 | 223 OK, 75 FAILED, 3 INFORMATIONAL / the same | 216 OK / 36 OK, 108 FAILED, 72 UNIMPLEMENTED |
| gobwas/ws v1.4.0 (wsutil helpers) | 298 OK, 3 INFORMATIONAL / the same | 144 OK, 72 UNIMPLEMENTED / the same |

Notes on the table:
- **UTF-8.** coder and gorilla leave UTF-8 validation to the application. gobwas validates as bytes arrive and drops the connection without a close frame, which the suite accepts.
- **coder** also fails case 7.5.1, a close reason that is not UTF-8.
- **Compression.** gorilla and gobwas negotiate only without context takeover. gobwas declines window-size offers. gorilla's client fails the suite's compressed cases at the handshake. With Go 1.27, gobwas's compression must end messages with `wsflate.Writer.Flush`: `Close`, as in its README, fails.
- **INFORMATIONAL.** The 3 INFORMATIONAL cases are 7.1.6, 7.13.1 and 7.13.2. The suite defines no stricter outcome for these cases.

**Performance.** These results come from the
[comparison benchmarks](benchmarks/compare). They were run with the manual
`bench.yml` workflow (run 39611938175) on a GitHub runner (AMD EPYC 9V74,
4 vCPU, Go 1.27), over plaintext loopback TCP. Each figure is a median of
10 runs. Settings:
- binary messages;
- compression off;
- nwws liveness off;
- each library's own concurrency rules.

Versions: nwws v0.2.0, coder v1.8.15, gorilla v1.5.3, gobwas v1.4.0
(October 2026). gobwas is used through its `wsutil` helpers.

| Benchmark | nwws | coder | gorilla | gobwas |
| --- | --- | --- | --- | --- |
| Round trip, 300 B (lower is better) | 17.2 µs | 20.5 µs | 17.1 µs | **16.6 µs** |
| One writer, 300 B | 38.0 MiB/s | 38.8 MiB/s | 36.4 MiB/s | 40.7 MiB/s |
| One writer, 64 KiB | 1.02 GiB/s | 436 MiB/s | 478 MiB/s | **1.79 GiB/s** |
| 16 concurrent writers, 300 B | **149 MiB/s** | 26.6 MiB/s | 37.0 MiB/s | 39.8 MiB/s |
| 1 MiB reads, whole message | **3.46 GiB/s** | 1.67 GiB/s | 1.70 GiB/s | 1.63 GiB/s |
| 1 MiB reads, reused buffer | 5.09 GiB/s | **6.01 GiB/s** | 5.33 GiB/s | 5.38 GiB/s |
| Allocations per round trip | **0** | 24 | 3 | 3 |
| Idle connection: heap | **7.1 KB** | 10.8 KB | 8.9 KB | 9.1 KB |
| Idle connection: goroutines | 1 | 1 | 1 | 1 |

What the table shows:
- **Round-trip latency.** nwws is level with gorilla (within 1 %), 3 %
  behind gobwas, and 16 % ahead of coder.
  - In v0.1.0 nwws was 2–3.5× slower, because each message woke a thread on
    another CPU when passing from nwws's read goroutine to `Read`.
  - Since v0.2.0, `Read` reads frames itself while nothing is queued.
- **Writing.** nwws is clearly ahead with concurrent writers, because
  writers share flushes. With a single writer of 64 KiB messages, gobwas is
  about 1.75× faster than nwws, and nwws about 2.3× faster than coder and
  gorilla. With 300 B messages, all four are equal within the noise.
- **Reading.**
  - **Whole-message reads of 1 MiB:** nwws is about twice as fast, because it
    allocates once per message at the exact size.
  - **Reads into a reused buffer:** nwws is 5–15 % slower than the others.
    The cause is not yet known; it is not the copy out of nwws's read-ahead
    buffer.
- **Idle connections.** nwws keeps the least heap per connection. All four
  run one goroutine per connection: the application's reader.
- **Allocations.** nwws allocates nothing per round trip or per write.
- **Runner CPUs differ between runs.** Compare numbers only within one
  table.

Run the workflow yourself from the Actions tab ("Benchmarks") to reproduce
these numbers.

## API

| Identifier | Purpose |
| --- | --- |
| `Accept(w, r, *AcceptOptions) (*Conn, error)` | Upgrades an HTTP request on the server. A refused request is answered with an HTTP error status. |
| `Dial(ctx, url, *DialOptions) (*Conn, *http.Response, error)` | Connects to a `ws://` or `wss://` URL. `ctx` bounds the dial and the handshake. |
| `(*Conn).Read(ctx) (MessageType, []byte, error)` | Returns the next whole message. Cancelling `ctx` returns early and leaves the connection usable. |
| `(*Conn).ReadInto(ctx, buf) (MessageType, []byte, error)` | `Read` into a buffer the caller reuses, without allocating in steady state. See [Reading into a reused buffer](#reading-into-a-reused-buffer). |
| `(*Conn).Write(ctx, MessageType, []byte) error` | Sends a whole message and returns once it is on the network. Any goroutine may write. |
| `(*Conn).Close(StatusCode, reason) error` | Runs the closing handshake: it sends a close frame, waits a bounded time for the peer's, then closes TCP. |
| `(*Conn).CloseNow() error` | Closes TCP at once, without the closing handshake. See [Closing](#closing). |
| `(*Conn).Subprotocol() string` | Returns the subprotocol agreed in the handshake. |
| `MessageBinary`, `MessageText` | The message types. Text must be valid UTF-8 on both read and write. |
| `CloseError` | Reports how a connection ended: the peer's close code and reason, or `StatusAbnormalClosure` (1006) if it was lost. |
| `ErrClosed` | Write and Close return it once the connection is closing, and Read and Write after `CloseNow`. |
| `ConnOptions` | The connection options, embedded in `AcceptOptions` and `DialOptions`. |

### Connection behaviour

- **Single writer.** Frames are written one at a time and never
  interleave. A `Write` on an idle connection writes its frame itself, in
  the calling goroutine. While a write is in progress, other frames queue:
  a writer goroutine sends them together in the next network write,
  pending pongs first and a close frame last. That goroutine runs only
  while frames are queued, and ends about a millisecond after the last.
  Pings and close frames always go through the queue.
- **Fragmented messages** are reassembled. Control frames between fragments
  are handled.
- **Pings** are answered with a pong that carries the same payload.
- **Read-ahead.** The connection keeps reading frames while messages wait
  for `Read`, up to a budget of `ConnOptions.ReadBuffer` bytes (default
  `DefaultReadBuffer`, 1 MiB). See [Read-ahead](#read-ahead).
- **Read limit.** `ConnOptions.ReadLimit` caps the size of a message and
  the memory used to read it. The default is `DefaultReadLimit` (1 MiB).
- **Protocol violations** close the connection with the matching code:
  1009 for a message over the read limit, 1007 for invalid UTF-8 in text,
  and 1002 for anything else. `Read` reports a violation in order, after
  the messages received before it, so replies to those go out before the
  close frame. If `Read` is not called within the close timeout, the
  connection is failed anyway. Text is checked for UTF-8 as its bytes
  arrive.
- **Close codes.** A close frame may carry 1000–1003, 1007–1014 or
  3000–4999. Codes 4000–4999 are for applications. Any other code from a
  peer is answered with 1002.
- **Close timeout.** Each wait in the closing handshake is bounded by
  `ConnOptions.CloseTimeout` (default `DefaultCloseTimeout`, 5 seconds).

## Echo server

```go
package main

import (
	"log"
	"net/http"

	"github.com/nerdwave-nick/nwws"
)

func main() {
	http.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		c, err := nwws.Accept(w, r, &nwws.AcceptOptions{
			ConnOptions: nwws.ConnOptions{ReadLimit: 4 << 20},
		})
		if err != nil {
			return // Accept has answered the request
		}
		defer c.Close(nwws.StatusNormalClosure, "")
		for {
			typ, msg, err := c.Read(r.Context())
			if err != nil {
				log.Printf("connection ended: %v", err)
				return
			}
			if err := c.Write(r.Context(), typ, msg); err != nil {
				return
			}
		}
	})
	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}
```

## Client

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nerdwave-nick/nwws"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, _, err := nwws.Dial(ctx, "ws://localhost:8080/echo", nil)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Write(ctx, nwws.MessageText, []byte("hello")); err != nil {
		log.Fatal(err)
	}
	_, reply, err := c.Read(ctx)
	var ce *nwws.CloseError
	switch {
	case errors.As(err, &ce):
		log.Fatalf("server closed: %d %s", ce.Code, ce.Reason)
	case err != nil:
		log.Fatal(err)
	}
	fmt.Printf("%s\n", reply)

	if err := c.Close(nwws.StatusNormalClosure, "bye"); err != nil {
		log.Print(err)
	}
}
```

## Dialing and origins

- **TLS.** For `wss://` with a private CA or client certificates (mutual
  TLS), set `DialOptions.TLSConfig`. It is cloned, never modified. The
  client always offers ALPN `http/1.1` only, whatever `NextProtos` holds,
  because the upgrade needs HTTP/1.1 and a server offered `h2` might pick it.
- **Custom dialer.** `DialOptions.NetDialContext` replaces the default
  `net.Dialer`. It receives `Dial`'s context, network `tcp` and the URL's
  host and port, and may connect anywhere: a Unix socket, a proxy of the
  caller's, or TCP with a custom keepalive. For `wss://`, TLS runs on top of
  the connection it returns. nwws has no built-in proxy support.
- **Origin check.** By default, `Accept` accepts requests without an
  `Origin` header and rejects those whose origin host or port differs from
  the request's `Host` (403). Hosts compare case-insensitively, and a
  missing port is the default port of the origin's scheme: 80 for
  `http`/`ws`, 443 for `https`/`wss`. So `https://a` matches `Host: a:443`,
  but `https://a:8443` does not match `Host: a`. Set
  `AcceptOptions.CheckOrigin` to decide yourself.

```go
c, _, err := nwws.Dial(ctx, "ws://vox-caster/feed", &nwws.DialOptions{
	NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", "/run/vox-caster.sock")
	},
})
```

## Liveness

Each connection checks that its peer is still there. It is on by default,
on both sides.

- **Pings.** The connection pings the peer every `PingInterval` (default
  `DefaultPingInterval`, 10 s), from a timer of its own, also while
  earlier pings still await their pongs. Pings go through the single
  writer, like every other frame; at most one waits for it at a time.
- **Pong timeout.** If nothing arrives from the peer within `PongTimeout`
  (default `DefaultPongTimeout`, 30 s) after a ping, counted from the
  oldest ping still unanswered, the connection closes.
  A pong is the expected answer, but any data from the peer shows that it
  is alive.
  - **The peer** is sent a close frame with `StatusGoingAway` (1001) and
    the reason `liveness timeout`, so a peer that is alive but slow learns
    why it was dropped. The frame goes through the single writer, behind
    any frames already queued. It is best effort: the connection waits at
    most 1 s for it to be written, then closes TCP without waiting for the
    peer's close frame. A peer that blocks writes is still dropped within
    `PongTimeout` plus that second.
  - **Locally**, a pending `Read` returns a `*CloseError` with
    `StatusAbnormalClosure` (1006) that matches `ErrLivenessTimeout` with
    `errors.Is`. 1006 is never sent on the wire.
- **Slow readers.** While messages wait for `Read` within the read-ahead
  budget, the connection keeps reading, so it answers the peer's pings and
  sees its pongs; the timeout runs as usual. Once the budget is full, the
  connection stops reading and cannot see the peer's pongs. The timeout is
  suspended then, and starts again in full when `Read` makes room, so a
  live peer is never timed out because the application is slow to call
  `Read`. The peer's pings go unanswered meanwhile, so a peer with
  liveness on, such as another nwws connection, may close the connection
  after its own pong timeout. Keep calling `Read`.
- **Round-trip time.** `(*Conn).RTT()` returns the time last measured from
  a ping to its pong, or zero before the first pong. Unsolicited pongs are
  accepted and do not count towards it.
- **Closing.** Liveness stops once a close frame is written. The close
  timeout bounds the rest of the closing handshake.
- **Turning it off.** Set `DisableLiveness`. The connection then sends no
  pings, but still answers the peer's.

```go
c, _, err := nwws.Dial(ctx, "wss://example.com/feed", &nwws.DialOptions{
	ConnOptions: nwws.ConnOptions{
		PingInterval: 5 * time.Second,
		PongTimeout:  15 * time.Second,
	},
})
if err != nil {
	return err
}
_, _, err = c.Read(ctx)
if errors.Is(err, nwws.ErrLivenessTimeout) {
	log.Printf("peer stopped answering (last RTT %v)", c.RTT())
}
```

## Idle connections

An idle connection keeps little memory and one goroutine, liveness
included.

- **Write buffer.** The 16 KiB write buffer and the frame writer's scratch
  space come from a pool when frames are written, and go back once nothing
  is left to write. An idle connection holds neither.
- **Goroutines.** One goroutine reads frames, unless a `Read` waits and
  reads them itself. Queued frames are written by a goroutine that runs
  only while there are any. Liveness runs on a timer, which is a goroutine
  only while it fires, so the pong timeout also ends a connection whose
  writes a peer that reads nothing blocks.
- **Measured.** `TestIdleConnectionCost` accepts 1000 connections over
  loopback from a client in another process, with liveness on both sides.
  It requires at most 16 KB of heap and 1 goroutine per server
  connection, plus a few in all for pings and pongs being written, and
  measures about 7–8 KB. No application goroutine reads in
  the test; goroutine stacks are not counted as heap.

```sh
go test -run TestIdleConnectionCost -v
```

## Read-ahead

The connection reads ahead: it keeps reading frames, answering pings and
watching liveness while complete messages wait for `Read`, up to a byte
budget.

- **Budget.** `ConnOptions.ReadBuffer`, in bytes; the default is
  `DefaultReadBuffer` (1 MiB). Each queued message counts its size plus a
  few dozen bytes of bookkeeping, so a flood of empty messages fills it
  too. A message is always queued when none waits, even if it is larger
  than the budget.
- **Back-pressure.** Once the queued messages take the budget, the
  connection stops reading until `Read` makes room. TCP then pushes back on
  the peer, and the peer's pings go unanswered meanwhile.
- **Memory.** Reading takes at most the budget plus one message being
  read, so `ReadBuffer + ReadLimit` per connection.
- **Order.** Messages reach `Read` in the order they arrived. A protocol
  violation is reported after the messages before it.
- **End.** When the connection ends, `Read` still returns the messages
  queued before, then why it ended. The peer's close frame is answered as
  soon as it is read, so `Write` returns `ErrClosed` from then on, even
  while queued messages wait for `Read`.
- **Close** drops the messages that are queued and not yet read.
- **Cost.** Messages are queued without copying their data, and read-ahead
  adds no goroutine.
- **Keep calling `Read`.** Read-ahead lets the application be slow for a
  while, not stop: a peer that keeps sending fills the budget, and then a
  peer with liveness on may close the connection after its own pong
  timeout. Set `ReadBuffer` to the burst the application must ride out.

```go
c, err := nwws.Accept(w, r, &nwws.AcceptOptions{
	ConnOptions: nwws.ConnOptions{ReadBuffer: 4 << 20}, // 4 MiB of read-ahead
})
```

## Reading into a reused buffer

`Read` returns a new slice for every message, which the caller owns.
`ReadInto(ctx, buf)` reads the next message into `buf[:0]` instead and
returns the filled slice. Pass back what it returned, and reading allocates
nothing per message in steady state:

```go
var buf []byte
for {
	typ, msg, err := c.ReadInto(ctx, buf)
	if err != nil {
		return err
	}
	handle(typ, msg) // msg is valid until the next ReadInto
	buf = msg
}
```

- **Growing.** When `buf` is too small for a message, `ReadInto` returns
  a new slice that holds it, which the caller owns. Passing that slice
  back makes the next messages of up to its size fit.
- **Errors** are those of `Read`, also for a protocol violation reported
  in order. On error `ReadInto` returns `buf[:0]`, so the buffer stays
  available.
- **Memory.** The connection reads each message into memory of its own,
  so that it can read ahead. Once a connection has called `ReadInto`, it
  takes that memory from a pool shared by all connections, and `ReadInto`
  gives it back after copying the message into `buf`. The read-ahead
  budget counts the full capacity of pooled memory, so reading still takes
  at most `ReadBuffer + ReadLimit` per connection. The pool keeps no
  buffer larger than 4 MiB, and the garbage collector empties it, as any
  `sync.Pool`. A connection that never calls `ReadInto` does not use the
  pool.
- **Mixing.** `Read` and `ReadInto` may be mixed on one connection. A slice
  `Read` returns is never reused.
- **Compression.** Compressed messages are read into pooled memory too.
  `compress/flate` still allocates while decompressing, mostly Huffman
  tables for each block (see the measurements below).

### Measurements

Allocations per message, from `go test -run '^$' -bench
'ReadThroughput|ReadInto|ReadCompressed'`, median of 10 runs (Go 1.27.2,
linux/amd64, loopback TCP, binary messages):

| | 300 B | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| `Read` | 1 alloc, 320 B | 1 alloc, 64 KiB | 1 alloc, 1 MiB |
| `ReadInto` | **0** allocs, 1 B | **0** allocs, 10 B | **0** allocs, 1.7 KB |
| `Read`, compressed | 2 allocs, 516 B | 8 allocs, 192 KB | 18 allocs, 2.7 MB |
| `ReadInto`, compressed | 1 alloc, 2 B | 5 allocs, 522 B | 13 allocs, 94 KB |

- The few bytes `ReadInto` still allocates on average come from refilling
  the pool: after a garbage collection empties it, and while it warms up,
  up to one read-ahead budget of messages takes new memory. With several
  Ps, `sync.Pool` may need one buffer more per P before it serves every
  message.
- Compressed messages are text, compressed at level 1 without context
  takeover. What remains is allocated inside `compress/flate`, mostly
  Huffman tables for each block.
- `BenchmarkLargeReadInto` compares with `BenchmarkLargeRead` on 1 MiB
  messages. Among runs at a matching speed index (see that benchmark),
  `ReadInto` read 2.9–3.3 GB/s against 2.5–2.7 GB/s for `Read`
  (medians per role).

## Closing

- **`Close(code, reason)`** runs the closing handshake. It sends a close
  frame after the messages already queued, waits for the peer's close
  frame and closes TCP. A client then waits for the server to close TCP
  first (RFC 6455 §7.1.1).
- **Close timeout.** `ConnOptions.CloseTimeout` bounds each of those
  waits; zero or less means `DefaultCloseTimeout` (5 s). It also bounds
  the handshake the connection completes by itself when the peer closes
  or violates the protocol, on the server and the client.
- **No allocation for incoming data while closing.** Once `Close` has started, messages
  read ahead are dropped, and data frames that still arrive are skipped
  unread, without allocating; control frames are still handled, so the
  peer's close frame ends the wait. A peer flooding 1 MiB messages during
  `Close` causes a few kilobytes of allocation, not one allocation per
  message. Only a message already being read when `Close` starts is read
  to its end.
- **`CloseNow()`** closes TCP at once, without a close frame, and waits for
  the connection's goroutines to end. A pending `Read` or `Write`, and any
  later one, returns `ErrClosed`, unless the connection had already ended
  for another reason, which it reports instead. Queued messages are
  dropped. A `Close` in progress returns early with an error.
- **Any goroutine, any number of times.** `CloseNow` returns nil from the
  call that closed the connection and `ErrClosed` from every other call,
  also during or after `Close`.

```go
// Give a slow peer one second for the closing handshake.
c, err := nwws.Accept(w, r, &nwws.AcceptOptions{
	ConnOptions: nwws.ConnOptions{CloseTimeout: time.Second},
})
// ...
if err := c.Close(nwws.StatusGoingAway, "shutting down"); err != nil {
	log.Printf("close: %v", err) // the peer did not answer in time
}
```

```go
// Abort at once, for example when the server shuts down.
go func() {
	<-ctx.Done()
	_ = c.CloseNow()
}()
```

## Autobahn conformance gate

The [Autobahn test suite](https://github.com/crossbario/autobahn-testsuite)
tests the nwws server and client in Docker: every section without
compression, and sections 12 and 13 with permessage-deflate on both sides:

```sh
./autobahn/run.sh
```

The gate is strict: the script exits non-zero unless every case is `OK`,
or `INFORMATIONAL` where the suite defines no stricter outcome, and every
report has its full case count (301 per side plain, 216 per side
deflate), so a run that lost a case fails.
`AUTOBAHN_PASSES=plain|deflate|all` (default `all`) selects the passes.
`go test ./...` does not run the suite and needs no Docker. See
[autobahn/README.md](autobahn/README.md) for what passing means and the
committed report summaries.

## Compression

nwws supports the permessage-deflate extension
([RFC 7692](https://www.rfc-editor.org/rfc/rfc7692)) on `compress/flate`.
It is **off by default**. Set `ConnOptions.Compression` on both sides to
enable it; it is negotiated in the opening handshake only when both sides
enable it, and the connection stays uncompressed otherwise.

```go
opts := &nwws.AcceptOptions{
	ConnOptions: nwws.ConnOptions{Compression: &nwws.CompressionOptions{}},
}
```

| `CompressionOptions` field | Effect |
| --- | --- |
| `Threshold` | Messages shorter than this many bytes are sent uncompressed. The default is `DefaultCompressionThreshold` (128). |
| `ServerNoContextTakeover` | The server compresses each message on its own (`server_no_context_takeover`), which saves memory at the cost of ratio. |
| `ClientNoContextTakeover` | The same for the client (`client_no_context_takeover`). |
| `Level` | The `compress/flate` level: `flate.HuffmanOnly` or 1 (`flate.BestSpeed`) to 9 (`flate.BestCompression`). Zero means `flate.BestSpeed`. `Accept` and `Dial` refuse any other value before upgrading or dialing. |

- **Context takeover** is used in both directions unless either side asks
  for no context takeover. It compresses small, similar messages far
  better, and costs a compressor and a 32 KiB window per connection (see
  *Memory* below). Without context takeover, compressors are pooled
  between messages, one pool per level.
- **Level.** Higher levels compress better and cost more CPU time on the
  connection's single writer. The default, `flate.BestSpeed`, suits
  messages sent as they happen.
- **Window size.** `compress/flate` always compresses with a 32 KiB window.
  If the peer limits nwws's window to fewer than 15 bits
  (`server_max_window_bits` or `client_max_window_bits`), nwws accepts and
  compresses with Huffman coding only, which uses no window, whatever the
  level. Decompression accepts any window up to 15 bits.
- **Streaming decompression.** A compressed message is decompressed as its
  frames arrive, without buffering its compressed form. Pings between its
  frames are answered meanwhile.
- **Read limit.** `ReadLimit` applies to the decompressed size only, so an
  incompressible message up to the limit is read although its compressed
  form is slightly larger. Decompression stops as soon as a message exceeds
  the limit, which closes the connection with 1009, so a small compressed
  message cannot inflate into a large allocation.
- **Invalid data.** Compressed data that does not decompress closes the
  connection with 1007, as does compressed text that is not UTF-8. Both
  fail as soon as the decompressed output shows it, without waiting for the
  rest of the message. The decompressor emits output at least at every
  flush and every 32 KiB.

### Memory

With context takeover, each connection keeps its `compress/flate`
compressor between messages, and the window of the peer's messages for
decompression. Measured heap retained per connection, with both sides
compressing (Go 1.27.2, linux/amd64):

| Compression | Retained per connection |
| --- | --- |
| off | 27 KB |
| level 1 (`flate.BestSpeed`, the default) | 0.91 MB |
| level 6 | 1.17 MB |
| level 9 | 1.24 MB |

Almost all of it is the compressor. A lone `flate.Writer`, measured the
same way after compressing about 64 KiB, holds 0.81 MB at level 1,
1.08 MB at level 6 and 1.14 MB at level 9. For many mostly
idle connections, `ServerNoContextTakeover` (on the server) or
`ClientNoContextTakeover` (on the client) returns the compressor to a pool
after each message, at the cost of compression ratio.

**Method.** `BenchmarkDeflateRetainedMemory` opens 32 connection pairs
over loopback TCP, with context takeover in both directions at the given
level. Each pair exchanges one 64 KiB text message each way, so that every
compressor and window is in use. After `runtime.GC`, the growth of
`runtime.MemStats.HeapAlloc` since before the pairs were opened, divided
by the 64 connections, is the `B/conn` metric:

```sh
go test -run '^$' -bench DeflateRetainedMemory -benchtime 5x -count 3
```
