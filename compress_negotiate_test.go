package nwws

import (
	"compress/flate"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The server negotiates permessage-deflate from the client's offers
// (RFC 7692 §5, §7.1).
func TestNegotiateDeflateServer(t *testing.T) {
	on := &CompressionOptions{}
	for _, tc := range []struct {
		name   string
		opts   *CompressionOptions
		offers []string
		want   string
	}{
		{"compression off", nil, []string{"permessage-deflate"}, ""},
		{"no offer", on, nil, ""},
		{"other extension", on, []string{"x-webkit-deflate-frame"}, ""},
		{"plain offer", on, []string{"permessage-deflate"}, "permessage-deflate"},
		{"name case-insensitive", on, []string{"PerMessage-Deflate"}, "permessage-deflate"},
		{"after another extension", on, []string{"foo; bar=1, permessage-deflate"}, "permessage-deflate"},
		{"across header lines", on, []string{"foo", "permessage-deflate; client_max_window_bits"}, "permessage-deflate"},
		// §7.1.1.1: an offered server_no_context_takeover must be accepted.
		{"server_no_context_takeover", on, []string{"permessage-deflate; server_no_context_takeover"},
			"permessage-deflate; server_no_context_takeover"},
		// §7.1.1.2: the client's hint is confirmed, so the server need
		// not keep a decompression context.
		{"client_no_context_takeover", on, []string{"permessage-deflate; client_no_context_takeover"},
			"permessage-deflate; client_no_context_takeover"},
		// §7.1.2.2: the server does not limit the client's window.
		{"client_max_window_bits", on, []string{"permessage-deflate; client_max_window_bits=10"}, "permessage-deflate"},
		// §7.1.2.1: accepting server_max_window_bits echoes it. Below 15,
		// nwws compresses with Huffman coding only, which needs no window.
		{"server_max_window_bits 15", on, []string{"permessage-deflate; server_max_window_bits=15"},
			"permessage-deflate; server_max_window_bits=15"},
		{"server_max_window_bits 8", on, []string{"permessage-deflate; server_max_window_bits=8"},
			"permessage-deflate; server_max_window_bits=8"},
		{"quoted value", on, []string{`permessage-deflate; server_max_window_bits="9"`},
			"permessage-deflate; server_max_window_bits=9"},
		{"autobahn default offer", on, []string{"permessage-deflate; client_no_context_takeover; client_max_window_bits"},
			"permessage-deflate; client_no_context_takeover"},
		{"server option", &CompressionOptions{ServerNoContextTakeover: true}, []string{"permessage-deflate"},
			"permessage-deflate; server_no_context_takeover"},
		{"client option", &CompressionOptions{ClientNoContextTakeover: true}, []string{"permessage-deflate"},
			"permessage-deflate; client_no_context_takeover"},
		// §5: invalid offers are declined; the next acceptable one is used.
		{"unknown parameter", on, []string{"permessage-deflate; foo"}, ""},
		{"duplicate parameter", on, []string{"permessage-deflate; server_no_context_takeover; server_no_context_takeover"}, ""},
		{"value on flag", on, []string{"permessage-deflate; server_no_context_takeover=1"}, ""},
		{"window bits missing", on, []string{"permessage-deflate; server_max_window_bits"}, ""},
		{"window bits too small", on, []string{"permessage-deflate; server_max_window_bits=7"}, ""},
		{"window bits too large", on, []string{"permessage-deflate; client_max_window_bits=16"}, ""},
		{"window bits leading zero", on, []string{"permessage-deflate; server_max_window_bits=08"}, ""},
		{"window bits not a number", on, []string{"permessage-deflate; server_max_window_bits=a"}, ""},
		{"first acceptable offer", on, []string{"permessage-deflate; foo, permessage-deflate; server_no_context_takeover, permessage-deflate"},
			"permessage-deflate; server_no_context_takeover"},
		{"malformed header", on, []string{"permessage-deflate; =x"}, ""},
		{"unterminated quote", on, []string{`permessage-deflate; server_max_window_bits="9`}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := &AcceptOptions{ConnOptions: ConnOptions{Compression: tc.opts}}
			if got := opts.negotiateExtensions(tc.offers); got != tc.want {
				t.Errorf("negotiateExtensions(%q) = %q, want %q", tc.offers, got, tc.want)
			}
		})
	}
}

func TestDeflateOffer(t *testing.T) {
	for _, tc := range []struct {
		opts *CompressionOptions
		want string
	}{
		{nil, ""},
		// client_max_window_bits: nwws honours any window the server asks for.
		{&CompressionOptions{}, "permessage-deflate; client_max_window_bits"},
		{&CompressionOptions{ServerNoContextTakeover: true, ClientNoContextTakeover: true},
			"permessage-deflate; server_no_context_takeover; client_no_context_takeover; client_max_window_bits"},
	} {
		opts := &DialOptions{ConnOptions: ConnOptions{Compression: tc.opts}}
		if got := opts.extensionOffer(); got != tc.want {
			t.Errorf("%+v: extensionOffer() = %q, want %q", tc.opts, got, tc.want)
		}
	}
}

// The client validates the server's response (RFC 7692 §5, §7.1).
func TestDeflateCheckResponse(t *testing.T) {
	on := &CompressionOptions{}
	for _, tc := range []struct {
		name     string
		opts     *CompressionOptions
		response []string
		want     string
		wantErr  bool
	}{
		{"declined", on, nil, "", false},
		{"accepted", on, []string{"permessage-deflate"}, "permessage-deflate", false},
		{"all parameters", on, []string{"permessage-deflate;client_max_window_bits=9; server_max_window_bits=10;client_no_context_takeover; server_no_context_takeover"},
			"permessage-deflate; server_no_context_takeover; client_no_context_takeover; server_max_window_bits=10; client_max_window_bits=9", false},
		{"not offered", nil, []string{"permessage-deflate"}, "", true},
		{"other extension", on, []string{"foo"}, "", true},
		{"two extensions", on, []string{"permessage-deflate, permessage-deflate"}, "", true},
		{"unknown parameter", on, []string{"permessage-deflate; foo"}, "", true},
		{"duplicate parameter", on, []string{"permessage-deflate; client_no_context_takeover; client_no_context_takeover"}, "", true},
		// §7.1.2.2: in a response, client_max_window_bits needs a value.
		{"client window without value", on, []string{"permessage-deflate; client_max_window_bits"}, "", true},
		{"window out of range", on, []string{"permessage-deflate; server_max_window_bits=16"}, "", true},
		{"malformed", on, []string{"permessage-deflate;"}, "", true},
		// §7.1.1.1: a server must accept server_no_context_takeover.
		{"takeover request ignored", &CompressionOptions{ServerNoContextTakeover: true}, []string{"permessage-deflate"}, "", true},
		{"takeover request accepted", &CompressionOptions{ServerNoContextTakeover: true},
			[]string{"permessage-deflate; server_no_context_takeover"}, "permessage-deflate; server_no_context_takeover", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := &DialOptions{ConnOptions: ConnOptions{Compression: tc.opts}}
			got, err := opts.checkExtensions(opts.extensionOffer(), tc.response)
			if tc.wantErr {
				if err == nil || !errors.Is(err, ErrBadHandshake) {
					t.Fatalf("checkExtensions(%q) = %q, %v; want an ErrBadHandshake", tc.response, got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("checkExtensions(%q) = %q, %v; want %q", tc.response, got, err, tc.want)
			}
		})
	}
}

// The agreed parameters decide what each side does (RFC 7692 §7.1).
func TestDeflateNegotiatedConfig(t *testing.T) {
	for _, tc := range []struct {
		name       string
		opts       *CompressionOptions
		extensions string
		server     bool
		want       *deflateConfig
	}{
		{"none", &CompressionOptions{}, "", true, nil},
		{"server defaults", &CompressionOptions{}, "permessage-deflate", true,
			&deflateConfig{threshold: DefaultCompressionThreshold, level: flate.BestSpeed, writeTakeover: true, writeWindowBits: 15, readTakeover: true}},
		{"client threshold", &CompressionOptions{Threshold: 1}, "permessage-deflate", false,
			&deflateConfig{threshold: 1, level: flate.BestSpeed, writeTakeover: true, writeWindowBits: 15, readTakeover: true}},
		{"server side", &CompressionOptions{}, "permessage-deflate; server_no_context_takeover; server_max_window_bits=9", true,
			&deflateConfig{threshold: DefaultCompressionThreshold, level: flate.BestSpeed, writeTakeover: false, writeWindowBits: 9, readTakeover: true}},
		{"client side", &CompressionOptions{}, "permessage-deflate; server_no_context_takeover; server_max_window_bits=9", false,
			&deflateConfig{threshold: DefaultCompressionThreshold, level: flate.BestSpeed, writeTakeover: true, writeWindowBits: 15, readTakeover: false}},
		{"client window", &CompressionOptions{}, "permessage-deflate; client_no_context_takeover; client_max_window_bits=10", false,
			&deflateConfig{threshold: DefaultCompressionThreshold, level: flate.BestSpeed, writeTakeover: false, writeWindowBits: 10, readTakeover: true}},
		{"server reads client", &CompressionOptions{}, "permessage-deflate; client_no_context_takeover", true,
			&deflateConfig{threshold: DefaultCompressionThreshold, level: flate.BestSpeed, writeTakeover: true, writeWindowBits: 15, readTakeover: false}},
		{"server level", &CompressionOptions{Level: flate.BestCompression}, "permessage-deflate", true,
			&deflateConfig{threshold: DefaultCompressionThreshold, level: flate.BestCompression, writeTakeover: true, writeWindowBits: 15, readTakeover: true}},
		{"client Huffman only", &CompressionOptions{Level: flate.HuffmanOnly}, "permessage-deflate", false,
			&deflateConfig{threshold: DefaultCompressionThreshold, level: flate.HuffmanOnly, writeTakeover: true, writeWindowBits: 15, readTakeover: true}},
		// A client that offered client_no_context_takeover keeps to it even
		// if the server's response leaves it out (§7.1.1.2).
		{"client hint", &CompressionOptions{ClientNoContextTakeover: true}, "permessage-deflate", false,
			&deflateConfig{threshold: DefaultCompressionThreshold, level: flate.BestSpeed, writeTakeover: false, writeWindowBits: 15, readTakeover: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.opts.negotiated(tc.extensions, tc.server)
			switch {
			case got == nil && tc.want == nil:
			case got == nil || tc.want == nil || *got != *tc.want:
				t.Fatalf("negotiated = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// Compression is negotiated through the opening handshake only when both
// sides enable it.
func TestDeflateHandshake(t *testing.T) {
	on := &CompressionOptions{}
	for _, tc := range []struct {
		name           string
		server, client *CompressionOptions
		want           string
	}{
		{"both", on, on, "permessage-deflate"},
		{"server only", on, nil, ""},
		{"client only", nil, on, ""},
		{"neither", nil, nil, ""},
		{"no context takeover", on, &CompressionOptions{ServerNoContextTakeover: true, ClientNoContextTakeover: true},
			"permessage-deflate; server_no_context_takeover; client_no_context_takeover"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ended := make(chan error, 1)
			ts := httptest.NewServer(echoConnHandler(&AcceptOptions{ConnOptions: ConnOptions{Compression: tc.server}}, ended))
			defer ts.Close()
			c, resp, err := Dial(t.Context(), wsURL(ts.URL), &DialOptions{ConnOptions: ConnOptions{Compression: tc.client}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close(StatusNormalClosure, "") }()
			if got := strings.Join(resp.Header.Values("Sec-WebSocket-Extensions"), ", "); got != tc.want {
				t.Errorf("Sec-WebSocket-Extensions = %q, want %q", got, tc.want)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("status %d", resp.StatusCode)
			}
			msg := []byte(strings.Repeat("compressible ", 100))
			mustWrite(t, c, MessageText, msg)
			mustRead(t, c, MessageText, msg)
		})
	}
}
