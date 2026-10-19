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

- **Single writer.** One goroutine per connection writes all frames: data,
  pongs and close. Messages queued while a write is in progress go out
  together in the next network write.
- **Fragmented messages** are reassembled. Control frames between fragments
  are handled.
- **Pings** are answered with a pong that carries the same payload.
- **Keep reading.** While a complete message waits for `Read`, the
  connection reads no further frames, so pings and close frames wait too.
- **Read limit.** `ConnOptions.ReadLimit` caps the size of a message and
  the memory used to read it. The default is `DefaultReadLimit` (1 MiB).
- **Protocol violations** close the connection with the matching code:
  1009 for a message over the read limit, 1007 for invalid UTF-8 in text,
  and 1002 for anything else.
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

For `wss://` with a private CA or client certificates (mutual TLS), set
`DialOptions.TLSConfig`.

## Autobahn conformance gate

The [Autobahn test suite](https://github.com/crossbario/autobahn-testsuite)
tests the nwws server and client in Docker: every section without
compression, and sections 12 and 13 with permessage-deflate on both sides:

```sh
./autobahn/run.sh
```

The script exits non-zero unless every case passes. `go test ./...` does
not run the suite and needs no Docker. See
[autobahn/README.md](autobahn/README.md) for what passing means, the
NON-STRICT cases and their reasons, and the committed report summaries.

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

- **Context takeover** is used in both directions unless either side asks
  for no context takeover. It compresses small, similar messages far
  better, and costs a compressor and a 32 KiB window per connection.
- **Window size.** `compress/flate` always compresses with a 32 KiB window.
  If the peer limits nwws's window to fewer than 15 bits
  (`server_max_window_bits` or `client_max_window_bits`), nwws accepts and
  compresses with Huffman coding only, which uses no window. Decompression
  accepts any window up to 15 bits.
- **Read limit.** `ReadLimit` applies to the decompressed size, and also to
  the compressed size. Decompression stops as soon as a message exceeds it,
  so a small compressed message cannot inflate into a large allocation.
- Compressed messages that do not decompress close the connection with
  1007, like invalid UTF-8.
