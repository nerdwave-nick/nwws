package nwws

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

// peekHeader is readHeader on a bufio.Reader, which consumes the header
// only once all of it has arrived and is valid. A read that fails, as on
// a read deadline, leaves br at the start of the frame, so that reading
// can resume there.
func (fr *frameReader) peekHeader(br *bufio.Reader) (frameHeader, error) {
	b, err := br.Peek(2)
	if err != nil {
		if len(b) > 0 {
			err = noEOF(err)
		}
		return frameHeader{}, err
	}
	h, n, err := fr.headerStart(b[0], b[1])
	if err != nil {
		return frameHeader{}, err
	}
	if n > 2 {
		if b, err = br.Peek(n); err != nil {
			return frameHeader{}, noEOF(err)
		}
	}
	if err := fr.headerEnd(&h, b); err != nil {
		return frameHeader{}, err
	}
	_, _ = br.Discard(n)
	return h, nil
}

// headerStart decodes and validates the first two bytes of a frame
// header, b0 and b1, and returns the header's length n. The payload
// length is left for headerEnd, unless it fits in b1.
func (fr *frameReader) headerStart(b0, b1 byte) (h frameHeader, n int, err error) {
	h = frameHeader{
		fin:    b0&finBit != 0,
		rsv:    b0 & rsvBits,
		opcode: opcode(b0 & 0x0f),
		masked: b1&maskBit != 0,
		length: int64(b1 &^ maskBit),
	}
	if err := checkHeader(h.fin, h.rsv, h.opcode, fr.rsv); err != nil {
		return frameHeader{}, 0, err
	}
	if h.masked != fr.server {
		if fr.server {
			return frameHeader{}, 0, errMaskRequired
		}
		return frameHeader{}, 0, errUnexpectedMask
	}
	// A 7-bit length of 126 or 127 announces a 16- or 64-bit length.
	n = 2
	switch h.length {
	case 126:
		n += 2
	case 127:
		n += 8
	}
	if h.masked {
		n += 4
	}
	return h, n, nil
}

// headerEnd completes h from b, the whole header that headerStart began,
// and checks its payload length. Longer length encodings than needed are
// accepted.
func (fr *frameReader) headerEnd(h *frameHeader, b []byte) error {
	k := 2
	switch h.length {
	case 126:
		h.length = int64(binary.BigEndian.Uint16(b[2:]))
		k += 2
	case 127:
		l := binary.BigEndian.Uint64(b[2:])
		if l>>63 != 0 {
			return errLengthMSB
		}
		h.length = int64(l)
		k += 8
	}
	if h.masked {
		copy(h.key[:], b[k:])
	}
	if h.opcode.isControl() && h.length > maxControlPayload {
		return errControlTooLong
	}
	if limit := fr.limit(); h.length > limit {
		return fmt.Errorf("%w: %d bytes, limit %d", errFrameTooLarge, h.length, limit)
	}
	return nil
}
