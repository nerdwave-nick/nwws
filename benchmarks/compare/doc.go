// Package compare benchmarks nwws against github.com/coder/websocket,
// github.com/gorilla/websocket, github.com/gobwas/ws and
// github.com/lesismal/nbio. It is a module of its own, so that nwws
// itself keeps depending on the standard library only, and it holds
// benchmarks only:
//
//	cd benchmarks/compare
//	go test -run '^$' -bench . -count 10 | tee bench.txt
//	benchstat -col /lib bench.txt
//
// Every benchmark runs once per library, as a sub-benchmark named
// lib=nwws, lib=coder, lib=gorilla, lib=gobwas or lib=nbio, so that
// benchstat compares the libraries side by side. The benchmarks that
// allocate per operation report B/op and allocs/op themselves; -benchmem
// would only add zero B/op and allocs/op to BenchmarkIdle, which has no
// operations.
//
// All connections are plaintext WebSocket over loopback TCP, opened with
// each library's own handshake, and the settings are the same for all
// libraries:
//
//   - messages are binary;
//   - compression is off;
//   - nwws liveness (pings and the pong timeout) is off, because the
//     others have none built in, and nbio's per-message read deadline
//     (KeepaliveTime) is off for the same reason;
//   - the read limit is 2 MiB, above the largest message;
//   - buffer sizes are each library's defaults; gobwas has none at the
//     level used, so it gets 4 KiB, the net/http and gorilla default.
//
// Each library is used as its documentation says. gorilla and gobwas
// allow one concurrent writer, so their connections write under a mutex,
// also when a single goroutine writes; the cost of the mutex is part of
// their numbers. coder, nwws and nbio allow concurrent writes themselves.
//
// The libraries do not share one programming model, and the harness
// keeps each one's own:
//
//   - nwws, coder, gorilla and gobwas are pull-based: the application
//     reads each message with a call, in a goroutine of its own. Their
//     servers are httptest servers.
//   - gobwas is used through its wsutil helpers, not raw frames; see
//     lib_gobwas_test.go.
//   - nbio is event-driven: epoll poller goroutines read all
//     connections, and a goroutine pool runs the application's message
//     handler. Its server is an nbhttp.Engine, and its clients share
//     one; see lib_nbio_test.go. Where a benchmark reads with a call,
//     the handler hands each message to the calling goroutine.
//
// The conn type hides the difference: receive passes messages to a
// function, from a reading goroutine or from nbio's handler.
//
// Every connection has a deadline, through a context for nwws and coder
// and SetDeadline or SetReadDeadline and SetWriteDeadline for the
// others, so that a benchmark that stops making progress fails instead
// of hanging.
package compare
