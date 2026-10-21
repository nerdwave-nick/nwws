package nwws

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"
)

// FuzzReadFrame reads frames from arbitrary input on both sides, with and
// without RSV1 negotiated. Every frame read must satisfy RFC 6455 §5.2 and
// §5.5 and survive a write and read round trip; every failure must be a
// frameError or a truncation. The seed corpus is in testdata/fuzz.
func FuzzReadFrame(f *testing.F) {
	const limit = 1 << 16
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, server := range []bool{true, false} {
			for _, allowed := range []byte{0, rsv1Bit} {
				fr := &frameReader{r: bytes.NewReader(data), server: server, rsv: allowed, maxPayload: limit}
				for {
					h, p, err := fr.readFrame()
					if err != nil {
						var fe frameError
						if err != io.EOF && err != io.ErrUnexpectedEOF && !errors.As(err, &fe) {
							t.Fatalf("readFrame: unexpected error %v", err)
						}
						break
					}
					checkFuzzedFrame(t, h, p, server, allowed, limit)
				}
				checkPeekedHeader(t, data, server, allowed, limit)
			}
		}
	})
}

// checkPeekedHeader checks that peekHeader decodes the first header of
// data as readHeader does, and consumes the header's bytes only when it
// succeeds.
func checkPeekedHeader(t *testing.T, data []byte, server bool, allowed byte, limit int64) {
	t.Helper()
	r := bytes.NewReader(data)
	fr := &frameReader{r: r, server: server, rsv: allowed, maxPayload: limit}
	want, werr := fr.readHeader()
	headerLen := len(data) - r.Len()

	br := bufio.NewReader(bytes.NewReader(data))
	pr := &frameReader{server: server, rsv: allowed, maxPayload: limit}
	got, gerr := pr.peekHeader(br)
	if got != want || (gerr == nil) != (werr == nil) || gerr != nil && gerr.Error() != werr.Error() {
		t.Fatalf("peekHeader = %+v, %v; readHeader = %+v, %v", got, gerr, want, werr)
	}
	rest, _ := io.ReadAll(br)
	consumed := len(data) - len(rest)
	switch {
	case gerr == nil && consumed != headerLen:
		t.Fatalf("peekHeader consumed %d bytes of a %d-byte header", consumed, headerLen)
	case gerr != nil && consumed != 0:
		t.Fatalf("peekHeader consumed %d bytes, then failed: %v", consumed, gerr)
	}
}

func checkFuzzedFrame(t *testing.T, h frameHeader, p []byte, server bool, allowed byte, limit int64) {
	t.Helper()
	switch {
	case int64(len(p)) != h.length || h.length > limit:
		t.Fatalf("payload of %d bytes, header length %d, limit %d", len(p), h.length, limit)
	case h.opcode.isReserved():
		t.Fatalf("reserved opcode %#x accepted", byte(h.opcode))
	case h.opcode.isControl() && (!h.fin || h.length > maxControlPayload):
		t.Fatalf("invalid control frame accepted: %+v", h)
	case h.masked != server:
		t.Fatalf("masked=%v accepted on server=%v", h.masked, server)
	case h.rsv&^allowed != 0:
		t.Fatalf("RSV %#x accepted with %#x allowed", h.rsv, allowed)
	}

	var wire bytes.Buffer
	fw := &frameWriter{w: &wire, server: !server, rsv: allowed}
	if err := fw.writeFrame(h.fin, h.rsv, h.opcode, p); err != nil {
		t.Fatalf("writeFrame of a frame read back: %v", err)
	}
	back := &frameReader{r: &wire, server: server, rsv: allowed, maxPayload: limit}
	h2, p2, err := back.readFrame()
	if err != nil {
		t.Fatalf("reading a written frame: %v", err)
	}
	if h2.fin != h.fin || h2.rsv != h.rsv || h2.opcode != h.opcode || h2.length != h.length || !bytes.Equal(p2, p) {
		t.Fatalf("round trip changed the frame: %+v -> %+v", h, h2)
	}
	if wire.Len() != 0 {
		t.Fatalf("%d bytes left after the round trip", wire.Len())
	}
}
