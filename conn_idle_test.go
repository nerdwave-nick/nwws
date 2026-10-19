package nwws

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Idle connection cost: the heap and goroutines a server connection keeps
// while it is idle, with liveness on. The bounds come from the comparison
// with coder/websocket (14 KB, 1 goroutine) and gorilla/websocket (12 KB,
// 1 goroutine) at N = 1000; neither has built-in liveness.
const (
	idleConns          = 1000
	idleMaxHeap        = 16_000 // bytes per connection
	idleMaxGoroutines  = 2      // per connection
	idlePingInterval   = time.Second
	idleSettle         = 5 * idlePingInterval / 2
	idleClientEnv      = "NWWS_IDLE_CLIENT"
	idleClientReadyMsg = "ready"
)

// TestIdleConnectionCost accepts idleConns WebSocket connections over
// loopback TCP from a client in a separate process, so that only the
// server side is measured, and reports the heap and goroutines each
// connection keeps once idle. Both sides have liveness on, with a ping
// interval short enough that every connection has written pings and
// pongs before the measurement, and long enough that few frames are in
// flight during it. No application goroutine reads: a reader
// blocked in Read is the application's cost, not the library's.
//
// Heap is HeapAlloc after two collections, which also empty sync.Pool;
// goroutine stacks are not included. Run it alone to see the numbers:
//
//	go test -run TestIdleConnectionCost -v
func TestIdleConnectionCost(t *testing.T) {
	if testing.Short() {
		t.Skip("accepts 1000 connections")
	}
	var mu sync.Mutex
	conns := make([]*Conn, 0, idleConns)
	opts := &AcceptOptions{ConnOptions: ConnOptions{PingInterval: idlePingInterval, PongTimeout: time.Minute}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r, opts)
		if err != nil {
			return
		}
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
	}))
	defer srv.Close()
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.CloseNow()
		}
	}()

	heap0, goroutines0 := idleSample()
	client, stdin, stdout := startIdleClient(t, "ws"+strings.TrimPrefix(srv.URL, "http"))
	defer func() {
		_ = stdin.Close() // the client exits
		_ = client.Wait()
	}()
	ready, err := stdout.ReadString('\n')
	if strings.TrimSpace(ready) != idleClientReadyMsg {
		t.Fatalf("client: %q, %v", ready, err)
	}
	waitFor(t, "every connection to be accepted", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(conns) == idleConns
	})
	time.Sleep(idleSettle) // pings and pongs in both directions
	heap1, goroutines1 := idleSample()
	mu.Lock()
	for i, c := range conns {
		if c.RTT() == 0 {
			t.Fatalf("connection %d has measured no RTT: liveness did not run", i)
		}
	}
	mu.Unlock()

	heap := (int64(heap1) - int64(heap0)) / idleConns
	goroutines := float64(goroutines1-goroutines0) / idleConns
	t.Logf("server, N = %d, liveness on: %d B heap and %.2f goroutines per idle connection", idleConns, heap, goroutines)
	if heap > idleMaxHeap {
		t.Errorf("%d B heap per idle connection, want at most %d", heap, idleMaxHeap)
	}
	if goroutines > idleMaxGoroutines {
		t.Errorf("%.2f goroutines per idle connection, want at most %d", goroutines, idleMaxGoroutines)
	}
	mu.Lock()
	runtime.KeepAlive(conns)
	mu.Unlock()
}

// idleSample returns the live heap after two collections and the number
// of goroutines.
func idleSample() (heap uint64, goroutines int) {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc, runtime.NumGoroutine()
}

// startIdleClient runs TestIdleClient in a child process, which dials url
// idleConns times and holds the connections until its stdin closes.
func startIdleClient(t *testing.T, url string) (*exec.Cmd, io.WriteCloser, *bufio.Reader) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestIdleClient$", "-test.timeout=5m")
	cmd.Env = append(os.Environ(), idleClientEnv+"="+url)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, stdin, bufio.NewReader(stdout)
}

// TestIdleClient is the client process of TestIdleConnectionCost. It does
// nothing in a normal test run.
func TestIdleClient(t *testing.T) {
	url := os.Getenv(idleClientEnv)
	if url == "" {
		t.Skip("runs as the client process of TestIdleConnectionCost")
	}
	opts := &DialOptions{ConnOptions: ConnOptions{PingInterval: idlePingInterval, PongTimeout: time.Minute}}
	conns := make([]*Conn, 0, idleConns)
	for i := range idleConns {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		c, _, err := Dial(ctx, url, opts)
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, "dial", strconv.Itoa(i)+":", err)
			os.Exit(1)
		}
		conns = append(conns, c)
	}
	fmt.Println(idleClientReadyMsg)
	_, _ = io.Copy(io.Discard, os.Stdin) // until the parent closes stdin
	for _, c := range conns {
		_ = c.CloseNow()
	}
	os.Exit(0)
}
