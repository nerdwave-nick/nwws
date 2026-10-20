// Package compare benchmarks nwws against github.com/coder/websocket and
// github.com/gorilla/websocket. It is a module of its own, so that nwws
// itself keeps depending on the standard library only, and it holds
// benchmarks only:
//
//	cd benchmarks/compare
//	go test -run '^$' -bench . -count 10 | tee bench.txt
//	benchstat -col /lib bench.txt
//
// Every benchmark runs once per library, as a sub-benchmark named
// lib=nwws, lib=coder or lib=gorilla, so that benchstat compares the
// libraries side by side. The benchmarks that allocate per operation
// report B/op and allocs/op themselves; -benchmem would only add zero
// B/op and allocs/op to BenchmarkIdle, which has no operations.
//
// All connections are plaintext WebSocket over loopback TCP, opened with
// each library's own handshake through an httptest server, and the
// settings are the same for all three libraries:
//
//   - messages are binary;
//   - compression is off;
//   - nwws liveness (pings and the pong timeout) is off, because coder and
//     gorilla have none built in;
//   - the read limit is 2 MiB, above the largest message;
//   - buffer sizes are each library's defaults.
//
// Each library is used as its documentation says. gorilla allows one
// concurrent writer, so its connections write under a mutex, also when a
// single goroutine writes; the cost of the mutex is part of its numbers.
// coder and nwws allow concurrent writes themselves.
//
// Every connection has a deadline, through a context for nwws and coder
// and SetReadDeadline and SetWriteDeadline for gorilla, so that a
// benchmark that stops making progress fails instead of hanging.
package compare
