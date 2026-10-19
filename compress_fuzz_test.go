package nwws

import (
	"bytes"
	"compress/flate"
	"errors"
	"testing"
)

// FuzzNegotiateDeflate negotiates arbitrary offers. Whatever the server
// answers, a client must accept, and both sides must derive a usable
// configuration from it.
func FuzzNegotiateDeflate(f *testing.F) {
	for _, s := range []string{
		"permessage-deflate",
		"permessage-deflate; client_no_context_takeover; client_max_window_bits",
		`foo; bar="baz", permessage-deflate; server_max_window_bits="10"; server_no_context_takeover`,
		"permessage-deflate; server_max_window_bits=08, permessage-deflate; client_max_window_bits=15",
		"permessage-deflate;;",
		`permessage-deflate; x="\"`,
	} {
		f.Add(s)
	}
	options := []*CompressionOptions{{}, {ServerNoContextTakeover: true, ClientNoContextTakeover: true}}
	f.Fuzz(func(t *testing.T, offer string) {
		for _, o := range options {
			resp := o.acceptOffer([]string{offer})
			if resp == "" {
				continue
			}
			got, err := (&CompressionOptions{}).checkResponse([]string{resp})
			if err != nil || got != resp {
				t.Fatalf("offer %q: client refused response %q: %q, %v", offer, resp, got, err)
			}
			for _, server := range []bool{true, false} {
				cfg := o.negotiated(resp, server)
				if cfg == nil || cfg.writeWindowBits < 8 || cfg.writeWindowBits > maxWindowBits {
					t.Fatalf("offer %q, response %q: server %v config %+v", offer, resp, server, cfg)
				}
			}
		}
	})
}

// FuzzInflate decompresses arbitrary data as a message. It either fails
// with 1007 or 1009, or yields at most the read limit, and the connection
// can go on to the next message.
func FuzzInflate(f *testing.F) {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.BestSpeed)
	_, _ = w.Write(bytes.Repeat([]byte("hello, "), 100))
	_ = w.Flush()
	f.Add(bytes.TrimSuffix(buf.Bytes(), syncFlushTail))
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff})
	f.Add([]byte{0x02, 0x00}) // empty static block
	const limit = 4096
	f.Fuzz(func(t *testing.T, data []byte) {
		d := &inflater{takeover: true}
		for range 2 {
			msg, err := d.message(bytes.Clone(data), limit)
			var pe *protocolError
			switch {
			case err == nil:
				if len(msg) > limit {
					t.Fatalf("message of %d bytes exceeds the limit", len(msg))
				}
			case !errors.As(err, &pe) || pe.code != StatusInvalidFramePayloadData && pe.code != StatusMessageTooBig:
				t.Fatalf("error %v, want a protocol error with 1007 or 1009", err)
			}
			if len(d.dict) > windowSize {
				t.Fatalf("dictionary of %d bytes", len(d.dict))
			}
		}
	})
}
