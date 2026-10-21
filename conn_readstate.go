package nwws

import (
	"bytes"
	"fmt"
	"math"
)

// readState is the position in the frame stream. It is kept in the
// connection rather than on a goroutine's stack, so that whoever holds
// the reader role, the read goroutine or a Read, resumes where the
// previous holder stopped, also in the middle of a frame after a
// cancelled Read.
type readState struct {
	// The message being reassembled.
	msg   []byte
	typ   opcode
	inMsg bool // a message has started and not finished
	pool  bool // msg comes from the pool
	user  bool // msg is the buffer of the ReadInto holding the role
	text  utf8Validator

	// userBuf is the buffer of the ReadInto holding the role, for the
	// next message it starts.
	userBuf []byte

	// The frame being read, once its header is read.
	h       frameHeader
	inFrame bool
	off     int64                   // payload bytes of h read so far
	skip    bool                    // its payload is skipped, as Close has started
	ctl     [maxControlPayload]byte // a control frame's payload
}

// readOne reads one frame, or the rest of one. It returns a complete data
// message with true. It returns the peer's close frame as a *CloseError,
// a violation as a *protocolError, or a read error; after a read error
// caused by a read deadline, reading may resume.
func (c *Conn) readOne() (message, bool, error) {
	rs := &c.rs
	if !rs.inFrame {
		h, compressed, err := c.readFrameHeader()
		if err != nil {
			return message{}, false, err
		}
		if compressed {
			// The first frame of a compressed message (RFC 7692 §6.1),
			// which is decompressed whole as its frames arrive. Only
			// the read goroutine reads compressed connections.
			msg, err := c.inflate.read(h, h.opcode == opText, c.cfg.readLimit, c.pooling.Load())
			if err != nil {
				return message{}, false, err
			}
			return message{typ: MessageType(h.opcode), data: msg}, true, nil
		}
		rs.h, rs.inFrame, rs.off = h, true, 0
	}
	h := rs.h
	switch {
	case h.opcode.isControl():
		return message{}, false, c.readControlFrame()
	case rs.skip:
		for rs.off < h.length {
			n, err := c.br.Discard(int(min(h.length-rs.off, math.MaxInt32)))
			rs.off += int64(n)
			if err != nil {
				return message{}, false, noEOF(err)
			}
		}
		rs.inFrame, rs.skip, rs.inMsg = false, false, !h.fin
		return message{}, false, nil
	}
	if err := c.readDataPayload(); err != nil {
		return message{}, false, err
	}
	if !h.fin {
		return message{}, false, nil
	}
	m := message{typ: MessageType(rs.typ), data: rs.msg, inBuf: rs.user}
	rs.msg, rs.inMsg, rs.user = nil, false, false
	return m, true, nil
}

// readFrameHeader reads the next frame header and checks it against the
// message being reassembled (RFC 6455 §5.4), and prepares the reading of
// its payload. It reports the first frame of a compressed message, which
// it leaves to the caller.
func (c *Conn) readFrameHeader() (h frameHeader, compressed bool, err error) {
	rs := &c.rs
	limit := c.cfg.readLimit
	room := limit - int64(len(rs.msg))
	// Control frames up to 125 bytes stay readable when room is less. The
	// first frame of a message may be compressed, which limits only the
	// decompressed size; the check below limits the others.
	c.fr.maxPayload = max(room, maxControlPayload)
	if !rs.inMsg {
		c.fr.maxPayload = math.MaxInt64
	}
	h, err = c.fr.peekHeader(c.br)
	if err != nil {
		return h, false, frameReadError(err)
	}
	if err := checkCompressedBit(h); err != nil {
		return h, false, err
	}
	switch {
	case h.opcode.isControl():
	case h.opcode == opContinuation && !rs.inMsg:
		return h, false, protocolErrorf(StatusProtocolError, "continuation frame without a message to continue")
	case h.opcode != opContinuation && rs.inMsg:
		return h, false, protocolErrorf(StatusProtocolError, "new message before the previous one finished")
	case c.discarding.Load():
		// Close has started: data is dropped unread, so it takes no
		// memory, and control frames are still handled.
		rs.dropMsg()
		rs.skip = true
	case h.rsv&rsv1Bit != 0:
		return h, true, nil
	case h.length > room:
		return h, false, &protocolError{code: StatusMessageTooBig, err: fmt.Errorf("%w of %d bytes", errMessageTooLarge, limit)}
	default:
		if !rs.inMsg {
			rs.typ, rs.inMsg, rs.text, rs.pool = h.opcode, true, utf8Validator{}, c.pooling.Load()
			if rs.userBuf != nil {
				rs.msg, rs.user = rs.userBuf[:0], true
			}
		}
		rs.grow(int(h.length), int(limit))
	}
	return h, false, nil
}

// readControlFrame reads the rest of a control frame and handles it (RFC
// 6455 §5.5).
func (c *Conn) readControlFrame() error {
	rs := &c.rs
	payload := rs.ctl[:rs.h.length]
	for rs.off < rs.h.length {
		if err := c.readChunk(payload, true); err != nil {
			return err
		}
	}
	rs.inFrame = false
	return c.handleControl(rs.h.opcode, bytes.Clone(payload))
}

// readDataPayload reads the rest of a data frame's payload into the
// message. Text is unmasked and checked for UTF-8 after every read from
// the network, so invalid text fails the connection at the first bad
// bytes. Binary payloads are unmasked once the frame is complete, so that
// reading drains the network connection without pausing to unmask.
func (c *Conn) readDataPayload() error {
	rs := &c.rs
	start := len(rs.msg) - int(rs.h.length)
	payload := rs.msg[start:]
	text := rs.typ == opText
	for rs.off < rs.h.length {
		err := c.readChunk(payload, text)
		if text && !rs.text.valid(rs.msg[:start+int(rs.off)], false) {
			return &protocolError{code: StatusInvalidFramePayloadData, err: errInvalidUTF8}
		}
		if err != nil {
			return err
		}
	}
	rs.inFrame = false
	if text && !rs.text.valid(rs.msg, rs.h.fin) {
		return &protocolError{code: StatusInvalidFramePayloadData, err: errInvalidUTF8}
	}
	if !text && rs.h.masked {
		maskBytes(rs.h.key, 0, payload)
	}
	return nil
}

// readChunk reads what the network delivers of payload, the current
// frame's whole payload, from rs.off on, and unmasks it with unmask.
func (c *Conn) readChunk(payload []byte, unmask bool) error {
	rs := &c.rs
	p := payload[rs.off:]
	n, err := c.br.Read(p)
	if unmask && rs.h.masked {
		maskBytes(rs.h.key, int(rs.off&3), p[:n])
	}
	rs.off += int64(n)
	if err != nil && rs.off < int64(len(payload)) {
		return noEOF(err)
	}
	return nil
}

// grow extends the message by n bytes within limit. The buffer of a
// ReadInto is never given to the pool: when the message outgrows it, the
// message moves to memory of its own.
func (rs *readState) grow(n, limit int) {
	if !rs.user || len(rs.msg)+n <= cap(rs.msg) {
		rs.msg = grow(rs.msg, n, limit, rs.pool && !rs.user)
		return
	}
	need := len(rs.msg) + n
	nb := newBuf(min(max(2*cap(rs.msg), need), limit), limit, rs.pool)[:need]
	copy(nb, rs.msg)
	rs.msg, rs.user = nb, false
}

// keep moves a partly read message out of the buffer of a ReadInto that
// gives up the reader role before the message is complete.
func (rs *readState) keep(limit int) {
	rs.userBuf = nil
	if !rs.user {
		return
	}
	nb := newBuf(len(rs.msg), max(limit, len(rs.msg)), rs.pool)
	copy(nb, rs.msg)
	rs.msg, rs.user = nb, false
}

// dropMsg forgets the message being reassembled.
func (rs *readState) dropMsg() {
	if rs.pool && !rs.user {
		putBuf(rs.msg)
	}
	rs.msg, rs.user = nil, false
}
