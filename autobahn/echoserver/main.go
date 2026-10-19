// Command echoserver is the nwws server under test by the Autobahn
// fuzzingclient: it echoes every message back to the client.
//
// Usage:
//
//	echoserver [-addr 127.0.0.1:9701] [-read-limit 33554432]
package main

import (
	"context"
	"flag"
	"log"
	"net/http"

	"github.com/nerdwave-nick/nwws"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9701", "address to listen on")
	readLimit := flag.Int64("read-limit", 32<<20, "largest message to read, in bytes")
	flag.Parse()

	opts := &nwws.AcceptOptions{
		// The fuzzingclient is not a browser; any origin is fine.
		CheckOrigin: func(*http.Request) bool { return true },
		ConnOptions: nwws.ConnOptions{ReadLimit: *readLimit},
	}
	log.Printf("echoserver listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := nwws.Accept(w, r, opts)
		if err != nil {
			return // Accept has answered the request
		}
		echo(c)
	})))
}

// echo writes every message read from c back to it, until c ends.
func echo(c *nwws.Conn) {
	ctx := context.Background()
	defer func() { _ = c.Close(nwws.StatusNormalClosure, "") }()
	for {
		typ, msg, err := c.Read(ctx)
		if err != nil {
			return
		}
		if err := c.Write(ctx, typ, msg); err != nil {
			return
		}
	}
}
