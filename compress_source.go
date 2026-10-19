package nwws

import (
	"compress/flate"
	"io"
	"math"
)

// frameSource is a compressed message as the decompressor reads it: the
// payloads of its frames, unmasked, followed by the sync flush tail the
// sender removed (RFC 7692 §7.2.2), then io.EOF. It reads each frame only
// when the decompressor gets to it, so the compressed message is never
// buffered whole.
//
// It implements flate.Reader. Otherwise compress/flate would wrap it in a
// bufio.Reader of its own, which reads past the end of the message.
type frameSource struct {
	r    flate.Reader                // the frames, at the payload of h
	next func() (frameHeader, error) // reads the header of the message's next frame

	h    frameHeader // the current frame
	left int64       // payload bytes of h not read yet
	pos  int         // position in h's masking key
	tail int         // bytes of the tail returned after the final frame
	eof  bool        // io.EOF was returned: the message is used up
	err  error       // why reading the frames failed
}

// start begins a message whose first frame has the header h.
func (s *frameSource) start(h frameHeader) {
	*s = frameSource{r: s.r, next: s.next, h: h, left: h.length}
}

// fill makes payload bytes of the current frame available, reading frame
// headers as needed. It reports false after the final frame's payload, or
// when reading the next frame failed, as s.err then says.
func (s *frameSource) fill() bool {
	for s.left == 0 {
		if s.h.fin || s.err != nil {
			return false
		}
		h, err := s.next()
		if err != nil {
			s.err = err
			return false
		}
		s.h, s.left, s.pos = h, h.length, 0
	}
	return true
}

// after reads what follows the payload: the tail, then io.EOF. When
// reading the frames failed, it returns why instead.
func (s *frameSource) after(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	if s.tail == len(syncFlushTail) {
		s.eof = true
		return 0, io.EOF
	}
	n := copy(p, syncFlushTail[s.tail:])
	s.tail += n
	return n, nil
}

func (s *frameSource) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !s.fill() {
		return s.after(p)
	}
	if int64(len(p)) > s.left {
		p = p[:s.left]
	}
	n, err := s.r.Read(p)
	if s.h.masked {
		s.pos = maskBytes(s.h.key, s.pos, p[:n])
	}
	s.left -= int64(n)
	if err != nil && s.left > 0 {
		s.err = noEOF(err)
		return n, s.err
	}
	return n, nil
}

func (s *frameSource) ReadByte() (byte, error) {
	if !s.fill() {
		var b [1]byte
		_, err := s.after(b[:])
		return b[0], err
	}
	b, err := s.r.ReadByte()
	if err != nil {
		s.err = noEOF(err)
		return 0, s.err
	}
	if s.h.masked {
		b ^= s.h.key[s.pos]
		s.pos = (s.pos + 1) & 3
	}
	s.left--
	return b, nil
}

// skip discards the rest of the message's payload.
func (s *frameSource) skip() error {
	for s.fill() {
		n, err := io.CopyN(io.Discard, s.r, s.left)
		s.left -= n
		if err != nil {
			s.err = noEOF(err)
		}
	}
	return s.err
}

// nextFragment reads the header of the next frame of a message, which must
// be a continuation frame (RFC 6455 §5.4). It hands the control frames
// before it to control.
func nextFragment(fr *frameReader, control func(frameHeader) error) (frameHeader, error) {
	for {
		// Only the decompressed size of a compressed message is limited.
		fr.maxPayload = math.MaxInt64
		h, err := fr.readHeader()
		if err != nil {
			return h, noEOF(frameReadError(err))
		}
		if err := checkCompressedBit(h); err != nil {
			return h, err
		}
		if h.opcode.isControl() {
			if err := control(h); err != nil {
				return h, err
			}
			continue
		}
		if h.opcode != opContinuation {
			return h, protocolErrorf(StatusProtocolError, "new message before the previous one finished")
		}
		return h, nil
	}
}
