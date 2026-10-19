package nwws

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"sync"
)

// syncFlushTail ends a DEFLATE sync flush. A compressed message is sent
// without it, so the receiver appends it (RFC 7692 §7.2.2).
var syncFlushTail = []byte{0x00, 0x00, 0xff, 0xff}

// windowSize is the LZ77 window of compress/flate: decompression keeps
// this much of the previous messages for a peer using context takeover.
const windowSize = 1 << maxWindowBits

// flateReaders pools decompressors. Each message resets one with the
// connection's dictionary, so they can be shared.
var flateReaders sync.Pool

var errCorruptCompressed = errors.New("nwws: compressed message is not valid DEFLATE data")

// inflater decompresses the messages a connection reads. The read
// goroutine owns it.
type inflater struct {
	// takeover keeps dict, the end of the previous messages, for a peer
	// using context takeover (RFC 7692 §7.1.1).
	takeover bool
	dict     []byte
	src      frameSource
}

// checkCompressedBit enforces RFC 7692 §6.1: RSV1 may be set only on the
// first frame of a data message. The frame codec has already refused RSV1
// without negotiated compression.
func checkCompressedBit(h frameHeader) error {
	if h.rsv&rsv1Bit == 0 {
		return nil
	}
	if h.opcode.isControl() {
		return protocolErrorf(StatusProtocolError, "RSV1 set on a control frame")
	}
	if h.opcode == opContinuation {
		return protocolErrorf(StatusProtocolError, "RSV1 set on a continuation frame")
	}
	return nil
}

// read decompresses a message whose first frame has the header h, as its
// frames arrive (RFC 7692 §7.2.2). The read goroutine has just read h.
// Decompression stops as soon as the output exceeds limit, which fails the
// connection with 1009. Invalid data fails it with 1007, as does text that
// is not valid UTF-8, as soon as the output shows it.
func (d *inflater) read(h frameHeader, text bool, limit int64) ([]byte, error) {
	d.src.start(h)
	r := d.reader()
	defer d.release(r)

	// An incompressible message is no longer than its frames, so the
	// first frame's length is a fair first guess.
	msg := make([]byte, 0, min(limit, max(512, h.length)))
	var (
		v     utf8Validator
		probe [1]byte
	)
	for {
		buf := msg[len(msg):cap(msg)]
		if len(buf) == 0 {
			// One more byte decides whether to grow msg, or whether the
			// message is too big.
			buf = probe[:]
		}
		n, err := r.Read(buf)
		switch {
		case n == 0:
		case len(msg) < cap(msg):
			msg = msg[:len(msg)+n]
		case int64(len(msg)) == limit:
			return nil, &protocolError{code: StatusMessageTooBig, err: fmt.Errorf("%w of %d bytes once decompressed", errMessageTooLarge, limit)}
		default:
			msg = grow(msg, 1, int(limit))
			msg[len(msg)-1] = probe[0]
		}
		if text && !v.valid(msg, false) {
			return nil, &protocolError{code: StatusInvalidFramePayloadData, err: errInvalidUTF8}
		}
		if err != nil {
			if err = d.end(err); err != nil {
				return nil, err
			}
			break
		}
	}
	if text && !v.valid(msg, true) {
		return nil, &protocolError{code: StatusInvalidFramePayloadData, err: errInvalidUTF8}
	}
	if d.takeover {
		d.keep(msg)
	}
	return msg, nil
}

// end maps the error that ended decompression. Running out of data after
// the appended sync flush ends the message (RFC 7692 §7.2.2). So does a
// final block, after which the rest of the message is ignored (§7.2.3.4).
// A failure to read the frames is returned as it is; anything else is
// invalid data.
func (d *inflater) end(err error) error {
	switch {
	case d.src.err != nil:
		return d.src.err
	case err == io.EOF:
		return d.src.skip()
	case errors.Is(err, io.ErrUnexpectedEOF) && d.src.eof:
		return nil
	}
	return &protocolError{code: StatusInvalidFramePayloadData, err: fmt.Errorf("%w: %v", errCorruptCompressed, err)}
}

// reader returns a decompressor reading d.src, preset with d.dict.
func (d *inflater) reader() io.ReadCloser {
	if r, ok := flateReaders.Get().(io.ReadCloser); ok {
		_ = r.(flate.Resetter).Reset(&d.src, d.dict) // always succeeds
		return r
	}
	return flate.NewReaderDict(&d.src, d.dict)
}

// noSource is what pooled decompressors are reset to read, so that they
// keep no connection alive.
var noSource = bytes.NewReader(nil)

// release returns the decompressor r to the pool.
func (d *inflater) release(r io.ReadCloser) {
	_ = r.(flate.Resetter).Reset(noSource, nil)
	flateReaders.Put(r)
}

// keep appends msg to the dictionary, keeping its last windowSize bytes.
func (d *inflater) keep(msg []byte) {
	if len(msg) >= windowSize {
		d.dict = append(d.dict[:0], msg[len(msg)-windowSize:]...)
		return
	}
	if drop := len(d.dict) + len(msg) - windowSize; drop > 0 {
		d.dict = append(d.dict[:0], d.dict[drop:]...)
	}
	d.dict = append(d.dict, msg...)
}
