package nwws

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// DialOptions configures the client side of the opening handshake.
// The zero value dials without TLS customisation, extra headers or
// subprotocols.
type DialOptions struct {
	// TLSConfig configures wss connections. It is cloned, never modified.
	// Set Certificates for client certificates (mutual TLS) and RootCAs for
	// a private CA. ServerName defaults to the URL's host. If nil, the
	// default configuration is used. NextProtos is ignored: the client
	// always offers ALPN "http/1.1" only, because the upgrade needs
	// HTTP/1.1 and a server offered h2 might select it.
	TLSConfig *tls.Config

	// Header holds extra request headers, such as Origin or Authorization.
	// A Host entry overrides the request's Host. Upgrade, Connection and
	// the Sec-WebSocket-* headers belong to the handshake and are ignored.
	Header http.Header

	// Subprotocols lists the subprotocols to offer, most preferred first.
	// The server may select one of them or none.
	Subprotocols []string

	// NetDialContext, if set, opens the connection the handshake runs on,
	// in place of a net.Dialer. It is called with network "tcp" and the
	// URL's host and port (80 for ws and 443 for wss by default), and may
	// connect anywhere, for example to a Unix socket, through a SOCKS or
	// HTTP CONNECT proxy of the caller's, or with a custom TCP keepalive.
	// It receives Dial's context and should give up when it ends. For wss,
	// TLS runs on top of the returned connection.
	NetDialContext func(ctx context.Context, network, addr string) (net.Conn, error)

	// ConnOptions configures the dialed connection.
	ConnOptions
}

// maxRejectionBody bounds how much of a non-101 response body is kept.
const maxRejectionBody = 1024

// dialHandshake connects to a ws or wss URL and performs the client side of
// the opening handshake. Cancelling ctx aborts the dial, the TLS handshake
// and the HTTP exchange. The server's response is returned whenever one was
// read, including on a refused handshake, where its body holds up to 1 KiB
// of the server's explanation.
func dialHandshake(ctx context.Context, rawURL string, opts *DialOptions) (*handshake, *http.Response, error) {
	u, addr, useTLS, err := parseDialURL(rawURL)
	if err != nil {
		return nil, nil, err
	}
	conn, err := opts.netDial(ctx, addr)
	if err != nil {
		return nil, nil, fmt.Errorf("nwws: dial %s: %w", addr, err)
	}
	// Unblock reads and writes when ctx ends during the handshake.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	hs, resp, err := clientHandshake(ctx, conn, u, useTLS, opts)
	if !stop() && err == nil {
		err = ctx.Err() // ctx ended as the handshake finished: deadline is set
	}
	if err != nil {
		_ = conn.Close()
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
			err = fmt.Errorf("nwws: dial %s: %w (%v)", addr, ctxErr, err)
		}
		return nil, resp, err
	}
	return hs, resp, nil
}

// parseDialURL validates a ws or wss URL (RFC 6455 §3) and returns the TCP
// address to dial.
func parseDialURL(rawURL string) (u *url.URL, addr string, useTLS bool, err error) {
	u, err = url.Parse(rawURL)
	if err != nil {
		return nil, "", false, fmt.Errorf("nwws: invalid URL: %w", err)
	}
	port := ""
	switch u.Scheme {
	case "ws":
		port = "80"
	case "wss":
		port, useTLS = "443", true
	default:
		return nil, "", false, fmt.Errorf("nwws: URL scheme %q is not ws or wss", u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, "", false, fmt.Errorf("nwws: URL %q has no host", rawURL)
	}
	if u.Fragment != "" || strings.HasSuffix(rawURL, "#") {
		return nil, "", false, fmt.Errorf("nwws: URL %q has a fragment", rawURL)
	}
	if p := u.Port(); p != "" {
		port = p
	}
	return u, net.JoinHostPort(u.Hostname(), port), useTLS, nil
}

// netDial opens the connection to addr with NetDialContext or, if that is
// unset, a net.Dialer.
func (o *DialOptions) netDial(ctx context.Context, addr string) (net.Conn, error) {
	if o == nil || o.NetDialContext == nil {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
	conn, err := o.NetDialContext(ctx, "tcp", addr)
	if err == nil && conn == nil {
		err = errors.New("NetDialContext returned no connection")
	}
	return conn, err
}

func clientHandshake(ctx context.Context, conn net.Conn, u *url.URL, useTLS bool, opts *DialOptions) (*handshake, *http.Response, error) {
	c := conn
	if useTLS {
		tc := tls.Client(conn, opts.tlsConfig(u.Hostname()))
		if err := tc.HandshakeContext(ctx); err != nil {
			return nil, nil, fmt.Errorf("nwws: TLS handshake: %w", err)
		}
		c = tc
	}
	key := newKey()
	offer := opts.extensionOffer()
	req := opts.request(u, key, offer)
	if err := req.Write(c); err != nil {
		return nil, nil, fmt.Errorf("nwws: write handshake request: %w", err)
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		return nil, nil, fmt.Errorf("nwws: read handshake response: %w", err)
	}
	if err := checkResponse(resp, key, opts); err != nil {
		if resp.StatusCode != http.StatusSwitchingProtocols {
			keepBody(resp)
		}
		return nil, resp, err
	}
	extensions, err := opts.checkExtensions(offer, resp.Header.Values("Sec-WebSocket-Extensions"))
	if err != nil {
		return nil, resp, err
	}
	return &handshake{
		conn:        withBuffered(c, br),
		subprotocol: resp.Header.Get("Sec-WebSocket-Protocol"),
		extensions:  extensions,
	}, resp, nil
}

func (o *DialOptions) tlsConfig(host string) *tls.Config {
	var cfg *tls.Config
	if o != nil && o.TLSConfig != nil {
		cfg = o.TLSConfig.Clone()
	} else {
		cfg = &tls.Config{}
	}
	if cfg.ServerName == "" {
		cfg.ServerName = host
	}
	// A new slice: the clone shares the caller's backing array.
	cfg.NextProtos = []string{"http/1.1"}
	return cfg
}

// request builds the opening handshake request of RFC 6455 §4.1.
func (o *DialOptions) request(u *url.URL, key, extensionOffer string) *http.Request {
	h := http.Header{}
	var subprotocols []string
	if o != nil {
		h = o.Header.Clone()
		if h == nil {
			h = http.Header{}
		}
		subprotocols = o.Subprotocols
	}
	host := u.Host
	if v := h.Get("Host"); v != "" {
		host = v
	}
	h.Del("Sec-WebSocket-Protocol")
	h.Del("Sec-WebSocket-Extensions")
	h.Set("Upgrade", "websocket")
	h.Set("Connection", "Upgrade")
	h.Set("Sec-WebSocket-Key", key)
	h.Set("Sec-WebSocket-Version", "13")
	if len(subprotocols) > 0 {
		h.Set("Sec-WebSocket-Protocol", strings.Join(subprotocols, ", "))
	}
	if extensionOffer != "" {
		h.Set("Sec-WebSocket-Extensions", extensionOffer)
	}
	return &http.Request{
		Method:     http.MethodGet,
		URL:        &url.URL{Path: u.Path, RawPath: u.RawPath, RawQuery: u.RawQuery},
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     h,
		Host:       host,
	}
}

// newKey returns a fresh Sec-WebSocket-Key: 16 random bytes, base64-encoded.
func newKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error
	return base64.StdEncoding.EncodeToString(b[:])
}

// checkResponse validates a 101 response to a request made with key
// (RFC 6455 §4.1, client requirements 1–6), except for extensions.
func checkResponse(resp *http.Response, key string, opts *DialOptions) error {
	switch {
	case resp.StatusCode != http.StatusSwitchingProtocols:
		return handshakeErrorf("server answered %q", resp.Status)
	case !hasToken(resp.Header, "Upgrade", "websocket"):
		return handshakeErrorf(`Upgrade header lacks "websocket"`)
	case !hasToken(resp.Header, "Connection", "upgrade"):
		return handshakeErrorf(`Connection header lacks "upgrade"`)
	}
	if v := resp.Header.Values("Sec-WebSocket-Accept"); len(v) != 1 || v[0] != acceptKey(key) {
		return handshakeErrorf("Sec-WebSocket-Accept does not match the key")
	}
	switch v := resp.Header.Values("Sec-WebSocket-Protocol"); {
	case len(v) == 0:
	case len(v) > 1 || opts == nil || !slices.Contains(opts.Subprotocols, v[0]):
		return handshakeErrorf("server selected subprotocol %q that was not offered", v)
	}
	return nil
}

// keepBody replaces resp.Body with up to maxRejectionBody bytes of it, read
// while the connection is still open.
func keepBody(resp *http.Response) {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxRejectionBody))
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(b))
}
