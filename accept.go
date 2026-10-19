package nwws

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// AcceptOptions configures the server side of the opening handshake.
// The zero value accepts same-origin requests without a subprotocol and
// uses the default connection options.
type AcceptOptions struct {
	// Subprotocols lists the subprotocols the server supports, most
	// preferred first. The first one the client also offers is selected;
	// when none matches, the handshake succeeds without a subprotocol and
	// the client decides whether to continue (RFC 6455 §4.2.2). Values are
	// compared exactly.
	Subprotocols []string

	// CheckOrigin decides whether to accept a request, typically from its
	// Origin header. A rejected request is answered with 403 Forbidden.
	// If nil, requests without an Origin header are accepted, and requests
	// whose Origin host (with port) differs from the request's Host, compared
	// case-insensitively, are rejected. Browsers always send Origin, so the
	// default blocks cross-site WebSocket hijacking.
	CheckOrigin func(r *http.Request) bool

	// ConnOptions configures the accepted connection.
	ConnOptions
}

// rejectError is a refused handshake request, answered with status before
// any hijack.
type rejectError struct {
	status int
	reason string
}

func (e *rejectError) Error() string { return ErrBadHandshake.Error() + ": " + e.reason }
func (e *rejectError) Unwrap() error { return ErrBadHandshake }

// acceptPlan holds the response values for a valid handshake request.
type acceptPlan struct {
	accept      string
	subprotocol string
	extensions  string
}

// checkRequest validates an opening handshake request (RFC 6455 §4.2.1)
// and plans the server's response, without writing anything.
func checkRequest(r *http.Request, opts *AcceptOptions) (acceptPlan, *rejectError) {
	reject := func(status int, reason string) (acceptPlan, *rejectError) {
		return acceptPlan{}, &rejectError{status: status, reason: reason}
	}
	switch {
	case r.ProtoMajor != 1 || r.ProtoMinor < 1:
		return reject(http.StatusHTTPVersionNotSupported, "HTTP/1.1 required, got "+r.Proto)
	case r.Method != http.MethodGet:
		return reject(http.StatusMethodNotAllowed, "method must be GET")
	case !hasToken(r.Header, "Connection", "upgrade"):
		return reject(http.StatusUpgradeRequired, `Connection header lacks "upgrade"`)
	case !hasToken(r.Header, "Upgrade", "websocket"):
		return reject(http.StatusUpgradeRequired, `Upgrade header lacks "websocket"`)
	}
	if v := r.Header.Values("Sec-WebSocket-Version"); len(v) != 1 || v[0] != "13" {
		return reject(http.StatusUpgradeRequired, "Sec-WebSocket-Version must be 13")
	}
	keys := r.Header.Values("Sec-WebSocket-Key")
	if len(keys) != 1 || !validKey(keys[0]) {
		return reject(http.StatusBadRequest, "Sec-WebSocket-Key must be one base64-encoded 16-byte value")
	}
	if !opts.checkOrigin(r) {
		return reject(http.StatusForbidden, "origin not allowed")
	}
	return acceptPlan{
		accept:      acceptKey(keys[0]),
		subprotocol: opts.selectSubprotocol(headerList(r.Header, "Sec-WebSocket-Protocol")),
		extensions:  opts.negotiateExtensions(r.Header.Values("Sec-WebSocket-Extensions")),
	}, nil
}

func (o *AcceptOptions) checkOrigin(r *http.Request) bool {
	if o != nil && o.CheckOrigin != nil {
		return o.CheckOrigin(r)
	}
	return sameOrigin(r)
}

// sameOrigin is the default origin check described on AcceptOptions.
func sameOrigin(r *http.Request) bool {
	origins := r.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) > 1 {
		return false
	}
	u, err := url.Parse(origins[0])
	return err == nil && u.Host != "" && strings.EqualFold(u.Host, r.Host)
}

func (o *AcceptOptions) selectSubprotocol(offered []string) string {
	if o == nil {
		return ""
	}
	for _, p := range o.Subprotocols {
		if slices.Contains(offered, p) {
			return p
		}
	}
	return ""
}

// acceptHandshake performs the server side of the opening handshake. On an
// invalid or refused request it answers with an HTTP error status and
// returns an error wrapping ErrBadHandshake, without hijacking. Otherwise it
// hijacks the connection, clears the server's deadlines on it and writes
// the 101 response, which includes headers already set on w (such as
// Set-Cookie) except those the handshake owns.
func acceptHandshake(w http.ResponseWriter, r *http.Request, opts *AcceptOptions) (*handshake, error) {
	plan, rej := checkRequest(r, opts)
	if rej != nil {
		writeRejection(w, rej)
		return nil, rej
	}
	conn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		http.Error(w, "websocket upgrade not supported", http.StatusInternalServerError)
		return nil, fmt.Errorf("nwws: hijack: %w", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("nwws: clear deadline: %w", err)
	}
	if _, err := conn.Write(switchingProtocols(w.Header(), plan)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("nwws: write handshake response: %w", err)
	}
	return &handshake{
		conn:        withBuffered(conn, brw.Reader),
		subprotocol: plan.subprotocol,
		extensions:  plan.extensions,
	}, nil
}

func writeRejection(w http.ResponseWriter, rej *rejectError) {
	h := w.Header()
	switch rej.status {
	case http.StatusMethodNotAllowed:
		h.Set("Allow", http.MethodGet)
	case http.StatusUpgradeRequired:
		// RFC 9110 §15.5.22 and RFC 6455 §4.4.
		h.Set("Upgrade", "websocket")
		h.Set("Connection", "Upgrade")
		h.Set("Sec-WebSocket-Version", "13")
	}
	http.Error(w, rej.Error(), rej.status)
}

// handshakeOwned lists the response headers that switchingProtocols writes
// itself or that must not appear on a 101 response.
var handshakeOwned = []string{
	"Upgrade", "Connection", "Sec-WebSocket-Accept", "Sec-WebSocket-Protocol",
	"Sec-WebSocket-Extensions", "Content-Length", "Transfer-Encoding",
}

// switchingProtocols renders the 101 response of RFC 6455 §4.2.2.
func switchingProtocols(extra http.Header, plan acceptPlan) []byte {
	var b strings.Builder
	b.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n")
	b.WriteString("Sec-WebSocket-Accept: " + plan.accept + "\r\n")
	if plan.subprotocol != "" {
		b.WriteString("Sec-WebSocket-Protocol: " + plan.subprotocol + "\r\n")
	}
	if plan.extensions != "" {
		b.WriteString("Sec-WebSocket-Extensions: " + plan.extensions + "\r\n")
	}
	h := extra.Clone()
	for _, k := range handshakeOwned {
		h.Del(k)
	}
	_ = h.Write(&b) // a strings.Builder never fails
	b.WriteString("\r\n")
	return []byte(b.String())
}
