# nwws

A WebSocket ([RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)) client and
server for Go, built only on the standard library.

```sh
go get github.com/nerdwave-nick/nwws
```

## API

| Identifier | Purpose |
| --- | --- |
| `Accept(w, r, *AcceptOptions) (*Conn, error)` | Upgrades an HTTP request on the server. A refused request is answered with an HTTP error status. |
| `Dial(ctx, url, *DialOptions) (*Conn, *http.Response, error)` | Connects to a `ws://` or `wss://` URL. `ctx` bounds the dial and the handshake. |
| `(*Conn).Read(ctx) (MessageType, []byte, error)` | Returns the next whole message. Cancelling `ctx` returns early and leaves the connection usable. |
| `(*Conn).Write(ctx, MessageType, []byte) error` | Sends a whole message and returns once it is on the network. Any goroutine may write. |
| `(*Conn).Close(StatusCode, reason) error` | Runs the closing handshake: it sends a close frame, waits a bounded time for the peer's, then closes TCP. |
| `(*Conn).Subprotocol() string` | Returns the subprotocol agreed in the handshake. |
| `MessageBinary`, `MessageText` | The message types. Text must be valid UTF-8 on both read and write. |
| `CloseError` | Reports how a connection ended: the peer's close code and reason, or `StatusAbnormalClosure` (1006) if it was lost. |
| `ErrClosed` | Write and Close return it once the connection is closing. |
| `ConnOptions` | The connection options, embedded in `AcceptOptions` and `DialOptions`. |

### Connection behaviour

- **Single writer.** Frames are written one at a time and never
  interleave. A `Write` on an idle connection writes its frame itself, in
  the calling goroutine. While a write is in progress, other frames queue:
  the connection's writer goroutine sends them together in the next
  network write, pending pongs first and a close frame last. Pings and
  close frames always go through the queue.
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
  5 seconds.

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
  `DefaultPingInterval`, 10 s). Pings go through the single writer, like
  every other frame.
- **Pong timeout.** If nothing arrives from the peer within `PongTimeout`
  (default `DefaultPongTimeout`, 30 s) after a ping, the connection closes.
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

## Autobahn conformance gate

The [Autobahn test suite](https://github.com/crossbario/autobahn-testsuite)
tests the nwws server and client in Docker: every section without
compression, and sections 12 and 13 with permessage-deflate on both sides:

```sh
./autobahn/run.sh
```

The gate is strict: the script exits non-zero unless every case is `OK`,
or `INFORMATIONAL` where the suite defines no stricter outcome.
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
