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
	src      bytes.Reader
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

// message decompresses a message from its compressed payload, which it may
// modify. Decompression stops as soon as the output exceeds limit, which
// fails the connection with 1009; invalid data fails it with 1007.
func (d *inflater) message(compressed []byte, limit int64) ([]byte, error) {
	d.src.Reset(append(compressed, syncFlushTail...))
	r := d.reader()
	defer func() {
		d.src.Reset(nil) // the pooled reader must not keep the message alive
		flateReaders.Put(r)
	}()

	msg := make([]byte, 0, min(limit, max(512, 4*int64(len(compressed)))))
	for {
		if len(msg) == cap(msg) {
			if int64(len(msg)) == limit {
				// Room is used up: one more byte means too much.
				var probe [1]byte
				n, err := r.Read(probe[:])
				if n > 0 {
					return nil, &protocolError{code: StatusMessageTooBig, err: fmt.Errorf("%w of %d bytes once decompressed", errMessageTooLarge, limit)}
				}
				if err = d.end(err); err != nil {
					return nil, err
				}
				break
			}
			msg = grow(msg, cap(msg)-len(msg)+1, int(limit))[:len(msg)]
		}
		n, err := r.Read(msg[len(msg):cap(msg)])
		msg = msg[:len(msg)+n]
		if err != nil {
			if err = d.end(err); err != nil {
				return nil, err
			}
			break
		}
	}
	if d.takeover {
		d.keep(msg)
	}
	return msg, nil
}

// end maps the error that ended decompression: the end of a final block,
// or the source running out after the appended sync flush, ends the message
// (RFC 7692 §7.2.2); anything else is invalid data.
func (d *inflater) end(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF) && d.src.Len() == 0:
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
