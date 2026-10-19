package nwws

import (
	"bytes"
	"strings"
	"testing"
)

// encodeFrame returns a single text frame with payload as it appears on
// the wire, masked when it goes to the server side, and the length of its
// header.
func encodeFrame(t *testing.T, toServer bool, payload []byte) (wire []byte, header int) {
	t.Helper()
	var buf bytes.Buffer
	fw := frameWriter{w: &buf, server: !toServer}
	if err := fw.writeFrame(true, 0, opText, payload); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), buf.Len() - len(payload)
}

// Autobahn 6.4.3 and 6.4.4: invalid UTF-8 fails the connection with 1007
// as soon as its bytes arrive, without waiting for the rest of the frame.
// The peer sends the frame in chops and stalls after the invalid byte.
func TestReadInvalidUTF8BeforeFrameEnds(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			const rest = "the rest of the frame never arrives"
			wire, hdr := encodeFrame(t, server, []byte("κόσμε\xff"+rest))

			// The first chop ends inside a rune, which is still valid.
			peer.sendRaw(wire[:hdr+3])
			peer.sendRaw(wire[hdr+3 : len(wire)-len(rest)])
			_, _, err := readMsg(t, c)
			assertProtocolError(t, err, StatusInvalidFramePayloadData)
			peer.expectClose(StatusInvalidFramePayloadData)
		})
	}
}

// A text frame that arrives in chops of any size, with runes and the
// masking key split across them, is read whole and intact.
func TestReadTextInChops(t *testing.T) {
	for _, server := range []bool{true, false} {
		t.Run(map[bool]string{true: "server", false: "client"}[server], func(t *testing.T) {
			c, peer := startConn(t, server, testConfig())
			text := []byte(strings.Repeat("aκόσμε€𝄞", 10000))
			wire, _ := encodeFrame(t, server, text)

			sent := make(chan error, 1)
			go func() {
				for i := 0; len(wire) > 0; i++ {
					n := min(len(wire), []int{1, 7, 3, 1000, 4093, 2}[i%6])
					if _, err := peer.nc.Write(wire[:n]); err != nil {
						sent <- err
						return
					}
					wire = wire[n:]
				}
				sent <- nil
			}()
			mustRead(t, c, MessageText, text)
			if err := <-sent; err != nil {
				t.Fatalf("peer write: %v", err)
			}
		})
	}
}
