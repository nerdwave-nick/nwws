// Package nwws is a WebSocket (RFC 6455) client and server on the standard
// library.
//
// A server upgrades an HTTP request with [Accept]; a client connects with
// [Dial]. Both return a [*Conn], which reads and writes whole messages,
// binary or text:
//
//	http.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
//		c, err := nwws.Accept(w, r, nil)
//		if err != nil {
//			return // Accept has answered the request
//		}
//		defer c.Close(nwws.StatusNormalClosure, "")
//		for {
//			typ, msg, err := c.Read(r.Context())
//			if err != nil {
//				return
//			}
//			if err := c.Write(r.Context(), typ, msg); err != nil {
//				return
//			}
//		}
//	})
//
// A client dials a ws or wss URL:
//
//	c, _, err := nwws.Dial(ctx, "wss://example.com/echo", nil)
//	if err != nil {
//		return err
//	}
//	defer c.Close(nwws.StatusNormalClosure, "")
//	if err := c.Write(ctx, nwws.MessageBinary, []byte("hello")); err != nil {
//		return err
//	}
//	_, reply, err := c.Read(ctx)
//
// Any goroutine may write: a single writer serialises all frames and sends
// messages queued together in one network write. A connection answers
// pings by itself. Keep calling Read while the connection is open: control
// frames are processed only while no message waits for Read.
//
// A connection pings its peer every 10 seconds and closes when no answer
// arrives within 30 seconds of a ping; [ConnOptions] changes both or turns
// liveness off, and [Conn.RTT] reports the last round-trip time. The
// timeout is suspended while a message waits for Read.
//
// When a connection ends, Read reports why with a [*CloseError], which
// holds the peer's close code and reason, or StatusAbnormalClosure when
// the connection was lost without a close frame. A peer that violates the
// protocol is disconnected with the matching close code: 1009 for a
// message beyond the read limit, 1007 for invalid UTF-8 in text, 1002
// otherwise. Read reports the violation after the messages received
// before it.
package nwws
