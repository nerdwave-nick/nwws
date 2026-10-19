package nwws

import (
	"crypto/rand"
	"encoding/binary"
	"io"
)

// writeChunk is the payload size a client masks and writes at a time, so
// masking a large payload needs no copy of it.
const writeChunk = 4096

// frameWriter writes single frames to w. It is not safe for concurrent use;
// the connection's single writer owns it.
type frameWriter struct {
	w io.Writer

	// server is true on the server side, which does not mask; the client
	// side masks every frame with a fresh key from crypto/rand (RFC 6455
	// §5.1, §5.3).
	server bool

	// rsv holds the RSV bits allowed by negotiated extensions.
	rsv byte

	buf []byte // header and masked payload, allocated on first use unless set
}

// writeFrame writes one frame with the given fields and payload, using the
// minimal length encoding (RFC 6455 §5.2). It validates the fields as
// frameReader does and writes nothing for an invalid frame. The payload is
// not modified.
func (fw *frameWriter) writeFrame(fin bool, rsv byte, op opcode, payload []byte) error {
	if err := checkHeader(fin, rsv, op, fw.rsv&rsvBits); err != nil {
		return err
	}
	if op.isControl() && len(payload) > maxControlPayload {
		return errControlTooLong
	}
	if fw.buf == nil {
		fw.buf = make([]byte, 0, maxHeaderLen+writeChunk)
	}

	b := fw.buf[:0]
	first := rsv | byte(op)
	if fin {
		first |= finBit
	}
	var mask byte
	if !fw.server {
		mask = maskBit
	}
	switch n := len(payload); {
	case n <= maxControlPayload:
		b = append(b, first, mask|byte(n))
	case n <= 0xffff:
		b = append(b, first, mask|126)
		b = binary.BigEndian.AppendUint16(b, uint16(n))
	default:
		b = append(b, first, mask|127)
		b = binary.BigEndian.AppendUint64(b, uint64(n))
	}

	if fw.server {
		if len(payload) <= cap(b)-len(b) {
			return fw.write(append(b, payload...))
		}
		if err := fw.write(b); err != nil {
			return err
		}
		return fw.write(payload)
	}

	// crypto/rand.Read never returns an error and always fills the key.
	var key [4]byte
	_, _ = rand.Read(key[:])
	b = append(b, key[:]...)
	pos := 0
	for {
		start := len(b)
		n := min(len(payload), cap(b)-start)
		b = append(b, payload[:n]...)
		pos = maskBytes(key, pos, b[start:])
		payload = payload[n:]
		if err := fw.write(b); err != nil {
			return err
		}
		if len(payload) == 0 {
			return nil
		}
		b = b[:0]
	}
}

func (fw *frameWriter) write(b []byte) error {
	n, err := fw.w.Write(b)
	if err == nil && n < len(b) {
		err = io.ErrShortWrite
	}
	return err
}
