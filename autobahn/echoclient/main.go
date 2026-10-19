// Command echoclient is the nwws client under test by the Autobahn
// fuzzingserver. It asks the server for the number of cases, runs each one
// by echoing every message back, and then has the server write its reports.
//
// Usage:
//
//	echoclient [-url ws://127.0.0.1:9702] [-agent nwws] [-read-limit 33554432] [-compress]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/nerdwave-nick/nwws"
)

func main() {
	base := flag.String("url", "ws://127.0.0.1:9702", "URL of the fuzzingserver")
	agent := flag.String("agent", "nwws", "agent name in the reports")
	readLimit := flag.Int64("read-limit", 32<<20, "largest message to read, in bytes")
	compress := flag.Bool("compress", false, "enable permessage-deflate and compress every non-empty message")
	flag.Parse()

	c := client{base: *base, agent: *agent, opts: &nwws.DialOptions{
		ConnOptions: nwws.ConnOptions{ReadLimit: *readLimit},
	}}
	if *compress {
		// A threshold of one byte puts every echo through the compressor.
		c.opts.Compression = &nwws.CompressionOptions{Threshold: 1}
	}
	n, err := c.caseCount()
	if err != nil {
		log.Fatalf("getCaseCount: %v", err)
	}
	log.Printf("running %d cases against %s", n, *base)
	for i := 1; i <= n; i++ {
		if err := c.runCase(i); err != nil {
			log.Printf("case %d: %v", i, err)
		}
	}
	if err := c.updateReports(); err != nil {
		log.Fatalf("updateReports: %v", err)
	}
	log.Printf("reports updated")
}

// client speaks the fuzzingserver's protocol.
type client struct {
	base  string
	agent string
	opts  *nwws.DialOptions
}

// stepTimeout bounds the dial and each read of a control exchange; a case
// may run longer, so cases have caseTimeout.
const (
	stepTimeout = 30 * time.Second
	caseTimeout = 5 * time.Minute
)

func (c *client) url(path string, query url.Values) string {
	return c.base + path + "?" + query.Encode()
}

// caseCount asks for the number of cases the server runs.
func (c *client) caseCount() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
	defer cancel()
	conn, _, err := nwws.Dial(ctx, c.url("/getCaseCount", nil), c.opts)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(nwws.StatusNormalClosure, "") }()
	_, msg, err := conn.Read(ctx)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(string(msg))
	if err != nil {
		return 0, fmt.Errorf("case count %q: %w", msg, err)
	}
	return n, nil
}

// runCase runs case i: it echoes every message until the connection ends.
func (c *client) runCase(i int) error {
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	q := url.Values{"case": {strconv.Itoa(i)}, "agent": {c.agent}}
	conn, _, err := nwws.Dial(ctx, c.url("/runCase", q), c.opts)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(nwws.StatusNormalClosure, "") }()
	for {
		typ, msg, err := conn.Read(ctx)
		if err != nil {
			return nil // the case decides how the connection ends
		}
		if err := conn.Write(ctx, typ, msg); err != nil {
			return nil
		}
	}
}

// updateReports has the server write its reports for the agent.
func (c *client) updateReports() error {
	ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
	defer cancel()
	q := url.Values{"agent": {c.agent}}
	conn, _, err := nwws.Dial(ctx, c.url("/updateReports", q), c.opts)
	if err != nil {
		return err
	}
	// The server closes the connection once the reports are written.
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
	}
}
