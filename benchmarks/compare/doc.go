// Package compare benchmarks nwws against github.com/coder/websocket,
// github.com/gorilla/websocket and github.com/gobwas/ws. It is a module
// of its own, so that nwws itself keeps depending on the standard
// library only, and it holds benchmarks only:
//
//	cd benchmarks/compare
//	go test -run '^$' -bench . -count 10 | tee bench.txt
//	benchstat -col /lib bench.txt
//
// Every benchmark runs once per library, as a sub-benchmark named
// lib=nwws, lib=coder, lib=gorilla or lib=gobwas, so that
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
//     others have none built in;
//   - the read limit is 2 MiB, above the largest message;
//   - buffer sizes are each library's defaults; gobwas has none at the
//     level used, so it gets 4 KiB, the net/http and gorilla default.
//
// Each library is used as its documentation says. gorilla and gobwas
// allow one concurrent writer, so their connections write under a mutex,
// also when a single goroutine writes; the cost of the mutex is part of
// their numbers. coder and nwws allow concurrent writes themselves.
//
// All four libraries are pull-based: the application reads each message
// with a call, in a goroutine of its own, and their servers are httptest
// servers. gobwas is used through its wsutil helpers, not raw frames; see
// lib_gobwas_test.go.
//
// Every connection has a deadline, through a context for nwws and coder
// and SetDeadline or SetReadDeadline and SetWriteDeadline for the
// others, so that a benchmark that stops making progress fails instead
// of hanging.
package compare
