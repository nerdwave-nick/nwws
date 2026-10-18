package nwws

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"testing"
)

// hello is "Hello" masked with key 37 fa 21 3d, as in RFC 6455 §5.7.
var hello = []byte{0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func len16(n int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(n)) }

func len64(n uint64) []byte { return binary.BigEndian.AppendUint64(nil, n) }

func TestReadFrame(t *testing.T) {
	big := bytes.Repeat([]byte{0xab}, 1<<16)
	tests := []struct {
		name    string
		server  bool
		rsv     byte
		max     int64
		in      []byte
		want    frameHeader
		payload []byte
	}{
		{
			name: "§5.7 single-frame unmasked text",
			in:   cat([]byte{0x81, 0x05}, []byte("Hello")),
			want: frameHeader{fin: true, opcode: opText, length: 5}, payload: []byte("Hello"),
		},
		{
			name: "§5.7 single-frame masked text", server: true,
			in:   cat([]byte{0x81, 0x85}, hello),
			want: frameHeader{fin: true, opcode: opText, masked: true, key: [4]byte{0x37, 0xfa, 0x21, 0x3d}, length: 5}, payload: []byte("Hello"),
		},
		{
			name: "§5.7 first fragment of unmasked text",
			in:   cat([]byte{0x01, 0x03}, []byte("Hel")),
			want: frameHeader{opcode: opText, length: 3}, payload: []byte("Hel"),
		},
		{
			name: "§5.7 final continuation fragment",
			in:   cat([]byte{0x80, 0x02}, []byte("lo")),
			want: frameHeader{fin: true, opcode: opContinuation, length: 2}, payload: []byte("lo"),
		},
		{
			name: "§5.7 unmasked ping",
			in:   cat([]byte{0x89, 0x05}, []byte("Hello")),
			want: frameHeader{fin: true, opcode: opPing, length: 5}, payload: []byte("Hello"),
		},
		{
			name: "§5.7 masked pong", server: true,
			in:   cat([]byte{0x8a, 0x85}, hello),
			want: frameHeader{fin: true, opcode: opPong, masked: true, key: [4]byte{0x37, 0xfa, 0x21, 0x3d}, length: 5}, payload: []byte("Hello"),
		},
		{
			name: "§5.7 256 bytes binary, 16-bit length",
			in:   cat([]byte{0x82, 0x7e}, len16(256), big[:256]),
			want: frameHeader{fin: true, opcode: opBinary, length: 256}, payload: big[:256],
		},
		{
			name: "§5.7 64KiB binary, 64-bit length",
			in:   cat([]byte{0x82, 0x7f}, len64(1<<16), big),
			want: frameHeader{fin: true, opcode: opBinary, length: 1 << 16}, payload: big,
		},
		{
			name: "§5.2 7-bit length 125",
			in:   cat([]byte{0x82, 0x7d}, big[:125]),
			want: frameHeader{fin: true, opcode: opBinary, length: 125}, payload: big[:125],
		},
		{
			name: "§5.2 empty close",
			in:   []byte{0x88, 0x00},
			want: frameHeader{fin: true, opcode: opClose}, payload: []byte{},
		},
		{
			name: "§5.2 close with 125-byte payload",
			in:   cat([]byte{0x88, 0x7d}, big[:125]),
			want: frameHeader{fin: true, opcode: opClose, length: 125}, payload: big[:125],
		},
		{
			name: "§5.2 non-minimal 16-bit length is accepted",
			in:   cat([]byte{0x82, 0x7e}, len16(5), []byte("Hello")),
			want: frameHeader{fin: true, opcode: opBinary, length: 5}, payload: []byte("Hello"),
		},
		{
			name: "§5.2 non-minimal 64-bit length is accepted",
			in:   cat([]byte{0x82, 0x7f}, len64(5), []byte("Hello")),
			want: frameHeader{fin: true, opcode: opBinary, length: 5}, payload: []byte("Hello"),
		},
		{
			name: "§5.5 control frame with non-minimal length within 125",
			in:   cat([]byte{0x89, 0x7e}, len16(5), []byte("Hello")),
			want: frameHeader{fin: true, opcode: opPing, length: 5}, payload: []byte("Hello"),
		},
		{
			name: "§5.2 RSV1 allowed by a negotiated extension", rsv: rsv1Bit,
			in:   cat([]byte{0xc1, 0x05}, []byte("Hello")),
			want: frameHeader{fin: true, rsv: rsv1Bit, opcode: opText, length: 5}, payload: []byte("Hello"),
		},
		{
			name: "payload exactly at the limit", max: 5,
			in:   cat([]byte{0x82, 0x05}, []byte("Hello")),
			want: frameHeader{fin: true, opcode: opBinary, length: 5}, payload: []byte("Hello"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fr := &frameReader{r: bytes.NewReader(tt.in), server: tt.server, rsv: tt.rsv, maxPayload: tt.max}
			h, p, err := fr.readFrame()
			if err != nil {
				t.Fatalf("readFrame: %v", err)
			}
			if h != tt.want {
				t.Errorf("header = %+v, want %+v", h, tt.want)
			}
			if !bytes.Equal(p, tt.payload) {
				t.Errorf("payload = %q, want %q", p, tt.payload)
			}
			if _, _, err := fr.readFrame(); err != io.EOF {
				t.Errorf("next readFrame: %v, want io.EOF", err)
			}
		})
	}
}

func TestReadFrameErrors(t *testing.T) {
	type tc struct {
		name   string
		server bool
		rsv    byte
		max    int64
		in     []byte
		want   error
	}
	tests := []tc{
		{name: "§5.2 64-bit length with most significant bit", in: cat([]byte{0x82, 0x7f}, len64(1<<63|5)), want: errLengthMSB},
		{name: "§5.5 fragmented ping", in: []byte{0x09, 0x00}, want: errFragmentedControl},
		{name: "§5.5 fragmented close", in: []byte{0x08, 0x00}, want: errFragmentedControl},
		{name: "§5.5 close longer than 125 bytes", in: cat([]byte{0x88, 0x7e}, len16(126), make([]byte, 126)), want: errControlTooLong},
		{name: "§5.5 pong with 64-bit length", in: cat([]byte{0x8a, 0x7f}, len64(1<<16)), want: errControlTooLong},
		{name: "§5.1 server rejects unmasked frame", server: true, in: cat([]byte{0x81, 0x05}, []byte("Hello")), want: errMaskRequired},
		{name: "§5.1 client rejects masked frame", in: cat([]byte{0x81, 0x85}, hello), want: errUnexpectedMask},
		{name: "§5.2 RSV1 without extension", in: []byte{0xc1, 0x00}, want: errReservedBits},
		{name: "§5.2 RSV2 without extension", in: []byte{0xa1, 0x00}, want: errReservedBits},
		{name: "§5.2 RSV3 without extension", in: []byte{0x91, 0x00}, want: errReservedBits},
		{name: "§5.2 RSV2 when only RSV1 is negotiated", rsv: rsv1Bit, in: []byte{0xe1, 0x00}, want: errReservedBits},
		{name: "payload over the limit", max: 4, in: cat([]byte{0x82, 0x05}, []byte("Hello")), want: errFrameTooLarge},
		{name: "payload over the default limit", in: cat([]byte{0x82, 0x7f}, len64(defaultMaxFramePayload+1)), want: errFrameTooLarge},
		{name: "empty input", in: nil, want: io.EOF},
		{name: "truncated header", in: []byte{0x81}, want: io.ErrUnexpectedEOF},
		{name: "truncated extended length", in: []byte{0x82, 0x7e, 0x01}, want: io.ErrUnexpectedEOF},
		{name: "truncated masking key", server: true, in: []byte{0x81, 0x85, 0x37, 0xfa}, want: io.ErrUnexpectedEOF},
		{name: "truncated payload", in: []byte{0x81, 0x05, 'H', 'e'}, want: io.ErrUnexpectedEOF},
	}
	for _, op := range []byte{0x3, 0x4, 0x5, 0x6, 0x7, 0xb, 0xc, 0xd, 0xe, 0xf} {
		tests = append(tests, tc{name: "§5.2 reserved opcode " + string("0123456789abcdef"[op]), in: []byte{0x80 | op, 0x00}, want: errReservedOpcode})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fr := &frameReader{r: bytes.NewReader(tt.in), server: tt.server, rsv: tt.rsv, maxPayload: tt.max}
			_, _, err := fr.readFrame()
			if !errors.Is(err, tt.want) {
				t.Fatalf("readFrame: %v, want %v", err, tt.want)
			}
			var fe frameError
			if isFrame := errors.As(err, &fe); isFrame != (tt.want != io.EOF && tt.want != io.ErrUnexpectedEOF) {
				t.Errorf("errors.As(%v, frameError) = %v", err, isFrame)
			}
		})
	}
}

// The limit is checked before the payload is allocated, so a peer cannot make
// the reader allocate a large buffer by announcing a large frame.
func TestReadFrameLimitBeforeAllocation(t *testing.T) {
	const limit = 1 << 20
	in := cat([]byte{0x82, 0x7f}, len64(limit+1))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 10 {
		fr := &frameReader{r: bytes.NewReader(in), maxPayload: limit}
		if _, _, err := fr.readFrame(); !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("readFrame: %v, want errFrameTooLarge", err)
		}
	}
	runtime.ReadMemStats(&after)
	if got := after.TotalAlloc - before.TotalAlloc; got >= limit {
		t.Errorf("allocated %d bytes for 10 rejected frames, want < %d", got, limit)
	}
}

// The header is available on its own, so a caller can stream a payload, and
// header errors surface before any payload is read.
func TestReadHeaderStopsBeforePayload(t *testing.T) {
	in := cat([]byte{0x82, 0x7e}, len16(300), bytes.Repeat([]byte{1}, 300))
	r := bytes.NewReader(in)
	fr := &frameReader{r: r}
	h, err := fr.readHeader()
	if err != nil {
		t.Fatal(err)
	}
	if h.length != 300 || h.opcode != opBinary || !h.fin {
		t.Errorf("header = %+v", h)
	}
	if r.Len() != 300 {
		t.Errorf("readHeader left %d bytes, want the 300-byte payload", r.Len())
	}
}
