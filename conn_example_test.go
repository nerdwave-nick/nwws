package nwws_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/nerdwave-nick/nwws"
)

// echo is a WebSocket echo server.
func echo(w http.ResponseWriter, r *http.Request) {
	c, err := nwws.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = c.Close(nwws.StatusNormalClosure, "") }()
	for {
		typ, msg, err := c.Read(r.Context())
		if err != nil {
			return
		}
		if err := c.Write(r.Context(), typ, msg); err != nil {
			return
		}
	}
}

func Example() {
	srv := httptest.NewServer(http.HandlerFunc(echo))
	defer srv.Close()
	ctx := context.Background()

	c, _, err := nwws.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, msg := range []string{"hello", "world"} {
		if err := c.Write(ctx, nwws.MessageText, []byte(msg)); err != nil {
			fmt.Println(err)
			return
		}
		typ, reply, err := c.Read(ctx)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%v: %s\n", typ, reply)
	}
	if err := c.Close(nwws.StatusNormalClosure, "done"); err != nil {
		fmt.Println(err)
	}
	// Output:
	// text: hello
	// text: world
}
