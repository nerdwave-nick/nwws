package nwws

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestWriteFrameServer(t *testing.T) {
	big := bytes.Repeat([]byte{0xab}, 1<<16)
	tests := []struct {
		name    string
		fin     bool
		rsv     byte
		op      opcode
		payload []byte
		want    []byte
	}{
		{name: "§5.7 single-frame unmasked text", fin: true, op: opText, payload: []byte("Hello"), want: cat([]byte{0x81, 0x05}, []byte("Hello"))},
		{name: "§5.7 first fragment", op: opText, payload: []byte("Hel"), want: cat([]byte{0x01, 0x03}, []byte("Hel"))},
		{name: "§5.7 final continuation", fin: true, op: opContinuation, payload: []byte("lo"), want: cat([]byte{0x80, 0x02}, []byte("lo"))},
		{name: "§5.7 unmasked ping", fin: true, op: opPing, payload: []byte("Hello"), want: cat([]byte{0x89, 0x05}, []byte("Hello"))},
		{name: "§5.7 256 bytes binary", fin: true, op: opBinary, payload: big[:256], want: cat([]byte{0x82, 0x7e, 0x01, 0x00}, big[:256])},
		{name: "§5.7 64KiB binary", fin: true, op: opBinary, payload: big, want: cat([]byte{0x82, 0x7f, 0, 0, 0, 0, 0, 1, 0, 0}, big)},
		{name: "§5.2 empty close", fin: true, op: opClose, want: []byte{0x88, 0x00}},
		{name: "§5.2 RSV1 with a negotiated extension", fin: true, rsv: rsv1Bit, op: opBinary, payload: []byte{1}, want: []byte{0xc2, 0x01, 0x01}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			fw := &frameWriter{w: &buf, server: true, rsv: rsv1Bit}
			if err := fw.writeFrame(tt.fin, tt.rsv, tt.op, tt.payload); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf.Bytes(), tt.want) {
				t.Errorf("wrote % x, want % x", truncate(buf.Bytes()), truncate(tt.want))
			}
		})
	}
}

func truncate(b []byte) []byte { return b[:min(len(b), 16)] }

// §5.2: writing always uses the minimal length encoding.
func TestWriteFrameMinimalLength(t *testing.T) {
	for _, tc := range []struct{ n, header int }{
		{0, 2}, {1, 2}, {125, 2}, {126, 4}, {127, 4}, {65535, 4}, {65536, 10}, {70000, 10},
	} {
		for _, server := range []bool{true, false} {
			var buf bytes.Buffer
			fw := &frameWriter{w: &buf, server: server}
			if err := fw.writeFrame(true, 0, opBinary, make([]byte, tc.n)); err != nil {
				t.Fatal(err)
			}
			want := tc.header + tc.n
			if !server {
				want += 4
			}
			if buf.Len() != want {
				t.Errorf("server=%v length %d: wrote %d bytes, want %d", server, tc.n, buf.Len(), want)
			}
		}
	}
}

// §5.3: a client masks every frame with a fresh key and leaves the caller's
// payload untouched; a server does not mask.
func TestWriteFrameClientMasks(t *testing.T) {
	payload := bytes.Repeat([]byte("nwws"), 3000)
	orig := bytes.Clone(payload)
	var buf bytes.Buffer
	fw := &frameWriter{w: &buf}
	keys := map[[4]byte]bool{}
	for range 8 {
		buf.Reset()
		if err := fw.writeFrame(true, 0, opBinary, payload); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(payload, orig) {
			t.Fatal("writeFrame modified the payload")
		}
		wire := buf.Bytes()
		if wire[1]&maskBit == 0 {
			t.Fatal("client frame is not masked")
		}
		var key [4]byte
		copy(key[:], wire[4:8])
		keys[key] = true
		if bytes.Contains(wire, []byte("nwwsnwws")) {
			t.Fatal("payload appears unmasked on the wire")
		}
		fr := &frameReader{r: bytes.NewReader(wire), server: true}
		_, got, err := fr.readFrame()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("unmasked payload differs")
		}
	}
	if len(keys) < 8 {
		t.Errorf("8 frames used only %d distinct masking keys", len(keys))
	}
}

func TestWriteFrameErrors(t *testing.T) {
	type tc struct {
		name    string
		fin     bool
		rsv     byte
		op      opcode
		payload []byte
		want    error
	}
	tests := []tc{
		{name: "§5.5 fragmented ping", op: opPing, want: errFragmentedControl},
		{name: "§5.5 close longer than 125 bytes", fin: true, op: opClose, payload: make([]byte, 126), want: errControlTooLong},
		{name: "§5.2 RSV1 without extension", fin: true, rsv: rsv1Bit, op: opText, want: errReservedBits},
		{name: "§5.2 RSV3 without extension", fin: true, rsv: rsv3Bit, op: opText, want: errReservedBits},
		{name: "§5.2 RSV value outside the RSV bits", fin: true, rsv: 0x01, op: opText, want: errReservedBits},
	}
	for op := opcode(0x3); op <= 0xf; op++ {
		if op.isReserved() {
			tests = append(tests, tc{name: fmt.Sprintf("§5.2 reserved opcode 0x%x", byte(op)), fin: true, op: op, want: errReservedOpcode})
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			fw := &frameWriter{w: &buf}
			err := fw.writeFrame(tt.fin, tt.rsv, tt.op, tt.payload)
			if !errors.Is(err, tt.want) {
				t.Fatalf("writeFrame: %v, want %v", err, tt.want)
			}
			if buf.Len() != 0 {
				t.Errorf("wrote %d bytes for a rejected frame", buf.Len())
			}
		})
	}
}

func TestWriteFrameWriterError(t *testing.T) {
	boom := errors.New("boom")
	for _, server := range []bool{true, false} {
		fw := &frameWriter{w: errWriter{boom}, server: server}
		if err := fw.writeFrame(true, 0, opBinary, make([]byte, 10000)); !errors.Is(err, boom) {
			t.Errorf("server=%v: writeFrame: %v, want %v", server, err, boom)
		}
	}
}

type errWriter struct{ err error }

func (w errWriter) Write([]byte) (int, error) { return 0, w.err }

// Frames written by one side read back identically on the other.
func TestFrameRoundTrip(t *testing.T) {
	type frame struct {
		fin     bool
		rsv     byte
		op      opcode
		payload []byte
	}
	var frames []frame
	for _, n := range []int{0, 1, 125, 126, 127, 4095, 4096, 4097, 65535, 65536, 70000} {
		p := make([]byte, n)
		for i := range p {
			p[i] = byte(i * 31)
		}
		frames = append(frames, frame{true, 0, opBinary, p}, frame{false, rsv1Bit, opText, p})
	}
	frames = append(frames,
		frame{true, 0, opContinuation, []byte("tail")},
		frame{true, 0, opPing, []byte("ping")},
		frame{true, 0, opPong, bytes.Repeat([]byte{7}, 125)},
		frame{true, 0, opClose, []byte{0x03, 0xe8}},
	)
	for _, server := range []bool{true, false} {
		t.Run(fmt.Sprintf("server=%v", server), func(t *testing.T) {
			var buf bytes.Buffer
			fw := &frameWriter{w: &buf, server: server, rsv: rsv1Bit}
			for _, f := range frames {
				if err := fw.writeFrame(f.fin, f.rsv, f.op, f.payload); err != nil {
					t.Fatal(err)
				}
			}
			fr := &frameReader{r: &buf, server: !server, rsv: rsv1Bit}
			for i, f := range frames {
				h, p, err := fr.readFrame()
				if err != nil {
					t.Fatalf("frame %d: %v", i, err)
				}
				if h.fin != f.fin || h.rsv != f.rsv || h.opcode != f.op || h.masked == server || h.length != int64(len(f.payload)) {
					t.Errorf("frame %d: header %+v, want fin=%v rsv=%#x op=%v len=%d", i, h, f.fin, f.rsv, f.op, len(f.payload))
				}
				if !bytes.Equal(p, f.payload) {
					t.Errorf("frame %d: payload differs", i)
				}
			}
			if _, _, err := fr.readFrame(); err != io.EOF {
				t.Errorf("after the last frame: %v, want io.EOF", err)
			}
		})
	}
}
