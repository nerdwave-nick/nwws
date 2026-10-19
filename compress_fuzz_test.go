package nwws

import (
	"bufio"
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"math"
	"testing"
	"unicode/utf8"
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

// FuzzInflate decompresses arbitrary data as a message in one frame. It
// either fails with 1007 or 1009, or yields at most the read limit having
// read the whole frame, and the connection can go on to the next message.
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
			src := bytes.NewReader(data)
			d.src.r = src
			msg, err := d.read(frameHeader{fin: true, length: int64(len(data))}, false, limit)
			checkInflated(t, msg, err, limit)
			if err == nil && src.Len() != 0 {
				t.Fatalf("%d bytes of the frame left unread", src.Len())
			}
			if len(d.dict) > windowSize {
				t.Fatalf("dictionary of %d bytes", len(d.dict))
			}
		}
	})
}

// checkInflated checks the outcome of decompressing a message: at most
// limit bytes, or a protocol error with 1007 or 1009.
func checkInflated(t *testing.T, msg []byte, err error, limit int) {
	t.Helper()
	var pe *protocolError
	switch {
	case err == nil:
		if len(msg) > limit {
			t.Fatalf("message of %d bytes exceeds the limit", len(msg))
		}
	case !errors.As(err, &pe) || pe.code != StatusInvalidFramePayloadData && pe.code != StatusMessageTooBig:
		t.Fatalf("error %v, want a protocol error with 1007 or 1009", err)
	}
}

// FuzzInflateFrames decompresses arbitrary data as a message split into
// frames as cuts says, with pings between them, read from the wire. The
// outcome must be the same as for the data in one frame. A message that
// decompresses must be read to its end and no further, with every ping
// answered, so the next frame is read intact.
func FuzzInflateFrames(f *testing.F) {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.BestSpeed)
	_, _ = w.Write(bytes.Repeat([]byte("grüß dich, "), 100))
	_ = w.Flush()
	_, _ = w.Write(bytes.Repeat([]byte{0x00, 0xff}, 3000))
	_ = w.Close()
	stream := buf.Bytes()
	f.Add(stream, []byte{3, 0, 0x81, 200, 0x80, 7})
	f.Add(bytes.TrimSuffix(stream, syncFlushTail), []byte{0, 0, 0})
	f.Add([]byte{0x02, 0x00}, []byte{0x81, 0x01})
	f.Add([]byte{0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00}, []byte{6, 0x81}) // RFC 7692 §7.2.3.4
	const limit = 4096
	f.Fuzz(func(t *testing.T, data, cuts []byte) {
		// Each cut is the length of a frame, its top bit a ping after it.
		var wire bytes.Buffer
		fw := frameWriter{w: &wire, rsv: rsv1Bit} // a client: masked frames
		rsv, op, rest, pings := rsv1Bit, opBinary, data, 0
		for _, cut := range cuts {
			n := min(len(rest), int(cut&0x7f))
			_ = fw.writeFrame(false, rsv, op, rest[:n])
			rest, rsv, op = rest[n:], 0, opContinuation
			if cut&0x80 != 0 {
				_ = fw.writeFrame(true, 0, opPing, []byte{byte(pings)})
				pings++
			}
		}
		_ = fw.writeFrame(true, rsv, op, rest)
		_ = fw.writeFrame(true, 0, opBinary, []byte("next"))

		fr := frameReader{r: bufio.NewReader(&wire), server: true, rsv: rsv1Bit}
		answered := 0
		control := func(h frameHeader) error {
			if _, err := io.CopyN(io.Discard, fr.r, h.length); err != nil {
				return err
			}
			answered++
			return nil
		}
		d := &inflater{src: frameSource{
			r:    fr.r.(flate.Reader),
			next: func() (frameHeader, error) { return nextFragment(&fr, control) },
		}}
		fr.maxPayload = math.MaxInt64
		h, err := fr.readHeader()
		if err != nil {
			t.Fatal(err)
		}
		msg, err := d.read(h, false, limit)
		checkInflated(t, msg, err, limit)

		one := &inflater{}
		one.src.r = bytes.NewReader(data)
		want, wantErr := one.read(frameHeader{fin: true, length: int64(len(data))}, false, limit)
		var pe, wantPE *protocolError
		switch {
		case (err == nil) != (wantErr == nil):
			t.Fatalf("in frames: %v; in one frame: %v", err, wantErr)
		case err == nil && !bytes.Equal(msg, want):
			t.Fatalf("in frames %d bytes; in one frame %d bytes", len(msg), len(want))
		case err != nil && errors.As(err, &pe) && errors.As(wantErr, &wantPE) && pe.code != wantPE.code:
			t.Fatalf("in frames: %v; in one frame: %v", err, wantErr)
		case err != nil:
			return
		}
		if answered != pings {
			t.Fatalf("%d of %d pings handled", answered, pings)
		}
		if h, p, err := fr.readFrame(); err != nil || h.opcode != opBinary || string(p) != "next" {
			t.Fatalf("next frame: %+v %q %v", h, p, err)
		}

		// Text is checked as it is decompressed, with the same outcome.
		one.src.r = bytes.NewReader(data)
		txt, err := one.read(frameHeader{fin: true, length: int64(len(data))}, true, limit)
		checkInflated(t, txt, err, limit)
		if valid := utf8.Valid(want); valid != (err == nil) {
			t.Fatalf("text: valid UTF-8 %v, error %v", valid, err)
		}
	})
}
