package compare

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	idleConns   = 1000
	idleSettle  = 500 * time.Millisecond
	idleRounds  = 5  // samples, of which the lowest counts
	idleCounts  = 20 // goroutine counts per sample, a millisecond apart
	idleRoleEnv = "COMPARE_IDLE_ROLE"
	idleLibEnv  = "COMPARE_IDLE_LIB"
	idleURLEnv  = "COMPARE_IDLE_URL"
	idleReady   = "ready"
)

// BenchmarkIdle measures what 1000 idle connections of each library keep
// on one side, side=server or side=client: heap bytes (heap-B/conn) and
// goroutines (goroutines/conn) per connection. The other side runs in a child process, so that only
// the measured side is counted.
//
// Every connection of a pull-based library has one application
// goroutine blocked in a read, on both sides: those libraries need the
// application to keep reading, coder, gorilla and gobwas to process
// control frames at all. That goroutine is part of the numbers.
//
// The heap is HeapAlloc after two collections, which also empty
// sync.Pool. Goroutine stacks are not included: the runtime keeps the
// freed stacks of earlier benchmarks for reuse, which makes their growth
// unreliable to measure; goroutines/conn stands for them. Each figure is
// the lowest of several samples, taken before the connections open and
// once they have settled, so that a timer or a collection in flight does
// not count. The benchmark measures once per -count and reports no ns/op.
func BenchmarkIdle(b *testing.B) {
	forEachLib(b, func(b *testing.B, lib library) {
		b.Run("side=server", func(b *testing.B) { idleServer(b, lib) })
		b.Run("side=client", func(b *testing.B) { idleClient(b, lib) })
	})
}

// idleServer measures idleConns server connections of lib, dialed by a
// child process.
func idleServer(b *testing.B, lib library) {
	url, accepted := serve(b, lib, idleConns)
	conns := make([]conn, 0, idleConns)
	b.Cleanup(func() {
		for _, c := range conns {
			c.close()
		}
	})
	before := idleSample()
	child := startIdleChild(b, "client", lib.name, url)
	for range idleConns {
		c := accept(b, accepted)
		conns = append(conns, c)
		c.idle()
	}
	if line := child.line(b); line != idleReady {
		b.Fatalf("child: %q", line)
	}
	time.Sleep(idleSettle)
	after := idleSample()
	for b.Loop() {
	}
	reportIdle(b, before, after)
	runtime.KeepAlive(conns)
}

// idleClient measures idleConns client connections of lib to a server in
// a child process.
func idleClient(b *testing.B, lib library) {
	child := startIdleChild(b, "server", lib.name, "")
	url := child.line(b)
	conns := make([]conn, 0, idleConns)
	b.Cleanup(func() {
		for _, c := range conns {
			c.close()
		}
	})
	before := idleSample()
	for i := range idleConns {
		c, err := lib.dial(url)
		if err != nil {
			b.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
		c.idle()
	}
	time.Sleep(idleSettle)
	after := idleSample()
	for b.Loop() {
	}
	reportIdle(b, before, after)
	runtime.KeepAlive(conns)
}

// idleStats is a sample of the process's heap and goroutines.
type idleStats struct {
	heap       uint64
	goroutines int
}

// idleSample returns the lowest heap and goroutine figures of idleRounds
// samples, each taken after two collections.
func idleSample() idleStats {
	s := idleStats{math.MaxUint64, math.MaxInt}
	for range idleRounds {
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		s.heap = min(s.heap, ms.HeapAlloc)
		for range idleCounts {
			s.goroutines = min(s.goroutines, runtime.NumGoroutine())
			time.Sleep(time.Millisecond)
		}
	}
	return s
}

func reportIdle(b *testing.B, before, after idleStats) {
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(float64(int64(after.heap)-int64(before.heap))/idleConns, "heap-B/conn")
	b.ReportMetric(float64(after.goroutines-before.goroutines)/idleConns, "goroutines/conn")
}

// idleChild is the other side of an idle benchmark: the test binary
// running TestIdleHelper.
type idleChild struct {
	lines chan string
}

// startIdleChild starts TestIdleHelper in a child process, in role
// "server" or "client" with lib, and stops it when b ends.
func startIdleChild(b *testing.B, role, lib, url string) *idleChild {
	b.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestIdleHelper$", "-test.timeout=5m")
	cmd.Env = append(os.Environ(), idleRoleEnv+"="+role, idleLibEnv+"="+lib, idleURLEnv+"="+url)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		b.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		b.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = stdin.Close() // the child exits
		exited := make(chan error, 1)
		go func() { exited <- cmd.Wait() }()
		select {
		case err := <-exited:
			if err != nil {
				b.Errorf("child: %v", err)
			}
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			b.Errorf("child did not exit")
		}
	})
	c := &idleChild{lines: make(chan string, 1)}
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		c.lines <- strings.TrimSpace(line)
	}()
	return c
}

// line returns the first line the child writes: the server's URL or
// idleReady. It fails if none comes within a minute.
func (c *idleChild) line(b *testing.B) string {
	b.Helper()
	select {
	case line := <-c.lines:
		if line == "" {
			b.Fatal("the child exited without a line")
		}
		return line
	case <-time.After(time.Minute):
		b.Fatal("the child wrote no line")
		return ""
	}
}

// TestIdleHelper is the child process of BenchmarkIdle. As "server", it
// serves the library's connections and writes its URL; as "client", it
// dials idleConns connections and writes idleReady. Every connection
// idles, as in BenchmarkIdle. It exits when its stdin closes. It does
// nothing in a normal test run.
func TestIdleHelper(t *testing.T) {
	role := os.Getenv(idleRoleEnv)
	if role == "" {
		t.Skip("runs as the child process of BenchmarkIdle")
	}
	var lib library
	for _, l := range libraries {
		if l.name == os.Getenv(idleLibEnv) {
			lib = l
		}
	}
	if lib.name == "" {
		fail("unknown library %q", os.Getenv(idleLibEnv))
	}
	switch role {
	case "server":
		url, accepted := serve(t, lib, 1)
		go func() {
			for c := range accepted {
				c.idle()
			}
		}()
		fmt.Println(url)
	case "client":
		for i := range idleConns {
			c, err := lib.dial(os.Getenv(idleURLEnv))
			if err != nil {
				fail("dial %d: %v", i, err)
			}
			c.idle()
		}
		fmt.Println(idleReady)
	default:
		fail("unknown role %q", role)
	}
	_, _ = io.Copy(io.Discard, os.Stdin) // until the parent closes stdin
	os.Exit(0)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
