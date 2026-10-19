package nwws

import "unicode/utf8"

// utf8Validator checks a text message for valid UTF-8 as its fragments
// arrive, so invalid text fails the connection without waiting for the
// rest of the message (RFC 6455 §8.1).
type utf8Validator struct {
	done int // length of the prefix already checked
}

// valid reports whether msg, of which earlier calls checked a prefix, is
// valid UTF-8 so far. Unless final, msg may end in an incomplete rune
// that is a valid start of one.
func (v *utf8Validator) valid(msg []byte, final bool) bool {
	p := msg[v.done:]
	n := len(p)
	if !final {
		n = completeRunes(p)
	}
	if !utf8.Valid(p[:n]) {
		return false
	}
	v.done += n
	return true
}

// completeRunes returns the length of p without a trailing incomplete rune
// that may still be completed. utf8.FullRune reports true for an invalid
// start, which utf8.Valid then rejects.
func completeRunes(p []byte) int {
	for i := len(p) - 1; i >= 0 && i >= len(p)-utf8.UTFMax+1; i-- {
		if utf8.RuneStart(p[i]) {
			if utf8.FullRune(p[i:]) {
				return len(p)
			}
			return i
		}
	}
	return len(p)
}

// readTextPayload is readPayload for a text frame that is not compressed:
// it checks UTF-8 after every read from the network, unmasking as the bytes
// arrive, so invalid text fails the connection at the first bad bytes
// instead of when the frame is complete.
func (c *Conn) readTextPayload(msg []byte, h frameHeader, limit int, v *utf8Validator) ([]byte, error) {
	off := len(msg)
	msg = grow(msg, int(h.length), limit)
	pos := 0 // position in the masking key
	for off < len(msg) {
		n, err := c.fr.r.Read(msg[off:])
		if h.masked {
			pos = maskBytes(h.key, pos, msg[off:off+n])
		}
		off += n
		if !v.valid(msg[:off], false) {
			return nil, &protocolError{code: StatusInvalidFramePayloadData, err: errInvalidUTF8}
		}
		if err != nil && off < len(msg) {
			return nil, noEOF(err)
		}
	}
	return msg, nil
}
