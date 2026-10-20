package nwws

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// opcode is the 4-bit frame opcode (RFC 6455 §5.2, §11.8).
type opcode byte

const (
	opContinuation opcode = 0x0
	opText         opcode = 0x1
	opBinary       opcode = 0x2
	opClose        opcode = 0x8
	opPing         opcode = 0x9
	opPong         opcode = 0xA
)

// isControl reports whether op is a control opcode (RFC 6455 §5.5).
func (op opcode) isControl() bool { return op&0x8 != 0 }

// isReserved reports whether op is one of the reserved opcodes 0x3–0x7 and
// 0xB–0xF (RFC 6455 §5.2).
func (op opcode) isReserved() bool {
	switch op {
	case opContinuation, opText, opBinary, opClose, opPing, opPong:
		return false
	}
	return true
}

// RSV bits as they appear in the first header byte (RFC 6455 §5.2). An
// extension negotiated at the handshake, such as permessage-deflate for
// rsv1Bit (RFC 7692 §6), allows its bits.
const (
	rsv1Bit byte = 0x40
	rsv2Bit byte = 0x20
	rsv3Bit byte = 0x10
	rsvBits      = rsv1Bit | rsv2Bit | rsv3Bit
)

const (
	finBit  byte = 0x80
	maskBit byte = 0x80

	// maxControlPayload is the largest control frame payload (RFC 6455 §5.5).
	maxControlPayload = 125

	// maxHeaderLen is a header with a 64-bit length and a masking key.
	maxHeaderLen = 2 + 8 + 4

	// defaultMaxFramePayload applies when a frameReader has no limit set.
	defaultMaxFramePayload = 16 << 20
)

// frameHeader holds the fields of one frame header (RFC 6455 §5.2).
type frameHeader struct {
	fin    bool
	rsv    byte // RSV1–3 as rsv1Bit, rsv2Bit and rsv3Bit
	opcode opcode
	masked bool
	key    [4]byte // masking key, valid when masked
	length int64   // payload length
}

// frameError is a protocol violation found in a frame. Each kind is a
// distinct value, so callers tell them apart with errors.Is and recognise
// any of them with errors.As.
type frameError string

func (e frameError) Error() string { return string(e) }

const (
	errReservedOpcode    frameError = "nwws: reserved opcode"
	errReservedBits      frameError = "nwws: RSV bits set without a negotiated extension"
	errFragmentedControl frameError = "nwws: fragmented control frame"
	errControlTooLong    frameError = "nwws: control frame payload longer than 125 bytes"
	errLengthMSB         frameError = "nwws: 64-bit payload length has its most significant bit set"
	errMaskRequired      frameError = "nwws: unmasked frame from a client"
	errUnexpectedMask    frameError = "nwws: masked frame from a server"
	errFrameTooLarge     frameError = "nwws: frame payload exceeds the limit"
)

// frameReader reads single frames from r.
type frameReader struct {
	r io.Reader

	// server is true on the server side, which requires masked frames; the
	// client side requires unmasked frames (RFC 6455 §5.1).
	server bool

	// maxPayload limits a frame's payload length; zero or less means
	// defaultMaxFramePayload. It is checked before the payload is allocated.
	maxPayload int64

	// rsv holds the RSV bits allowed by negotiated extensions.
	rsv byte

	buf [maxHeaderLen]byte
}

// readHeader reads and validates one frame header (RFC 6455 §5.2). It returns
// io.EOF only when the input ends before the first header byte, and leaves
// the reader at the start of the payload.
func (fr *frameReader) readHeader() (frameHeader, error) {
	b := fr.buf[:]
	if _, err := io.ReadFull(fr.r, b[:2]); err != nil {
		return frameHeader{}, err
	}
	h := frameHeader{
		fin:    b[0]&finBit != 0,
		rsv:    b[0] & rsvBits,
		opcode: opcode(b[0] & 0x0f),
		masked: b[1]&maskBit != 0,
	}
	if err := checkHeader(h.fin, h.rsv, h.opcode, fr.rsv); err != nil {
		return frameHeader{}, err
	}
	if h.masked != fr.server {
		if fr.server {
			return frameHeader{}, errMaskRequired
		}
		return frameHeader{}, errUnexpectedMask
	}

	// A 7-bit length of 126 or 127 announces a 16- or 64-bit length. Longer
	// encodings than needed are accepted.
	length7 := b[1] &^ maskBit
	extLen := 0
	switch length7 {
	case 126:
		extLen = 2
	case 127:
		extLen = 8
	}
	n := extLen
	if h.masked {
		n += 4
	}
	if _, err := io.ReadFull(fr.r, b[2:2+n]); err != nil {
		return frameHeader{}, noEOF(err)
	}
	switch extLen {
	case 0:
		h.length = int64(length7)
	case 2:
		h.length = int64(binary.BigEndian.Uint16(b[2:]))
	case 8:
		l := binary.BigEndian.Uint64(b[2:])
		if l>>63 != 0 {
			return frameHeader{}, errLengthMSB
		}
		h.length = int64(l)
	}
	if h.masked {
		copy(h.key[:], b[2+extLen:])
	}

	if h.opcode.isControl() && h.length > maxControlPayload {
		return frameHeader{}, errControlTooLong
	}
	if limit := fr.limit(); h.length > limit {
		return frameHeader{}, fmt.Errorf("%w: %d bytes, limit %d", errFrameTooLarge, h.length, limit)
	}
	return h, nil
}

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

// readFrame reads one frame and returns its header and unmasked payload.
func (fr *frameReader) readFrame() (frameHeader, []byte, error) {
	h, err := fr.readHeader()
	if err != nil {
		return frameHeader{}, nil, err
	}
	// readHeader has checked the length against the limit.
	payload := make([]byte, h.length)
	if _, err := io.ReadFull(fr.r, payload); err != nil {
		return frameHeader{}, nil, noEOF(err)
	}
	if h.masked {
		maskBytes(h.key, 0, payload)
	}
	return h, payload, nil
}

// limit is the effective payload limit; it never exceeds what a slice can
// hold, which matters on 32-bit platforms.
func (fr *frameReader) limit() int64 {
	if fr.maxPayload > 0 {
		return min(fr.maxPayload, math.MaxInt)
	}
	return defaultMaxFramePayload
}

// checkHeader validates the fields of a frame's first byte, on read and on
// write: RSV bits against those the extensions allow (RFC 6455 §5.2),
// reserved opcodes (§5.2) and fragmented control frames (§5.5).
func checkHeader(fin bool, rsv byte, op opcode, allowed byte) error {
	if rsv&^allowed != 0 {
		return errReservedBits
	}
	if op.isReserved() {
		return fmt.Errorf("%w 0x%x", errReservedOpcode, byte(op))
	}
	if op.isControl() && !fin {
		return errFragmentedControl
	}
	return nil
}

// noEOF turns io.EOF into io.ErrUnexpectedEOF for reads inside a frame.
func noEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
