package nwws

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"
)

// StatusCode is a close status code (RFC 6455 §7.4). Codes 4000–4999 are
// for applications to define.
type StatusCode int

// Close status codes of RFC 6455 §7.4.1 and the IANA WebSocket Close Code
// Number Registry. StatusNoStatusRcvd, StatusAbnormalClosure and
// StatusTLSHandshake are reported locally and never sent; 1004 is reserved.
const (
	StatusNormalClosure           StatusCode = 1000
	StatusGoingAway               StatusCode = 1001
	StatusProtocolError           StatusCode = 1002
	StatusUnsupportedData         StatusCode = 1003
	StatusNoStatusRcvd            StatusCode = 1005
	StatusAbnormalClosure         StatusCode = 1006
	StatusInvalidFramePayloadData StatusCode = 1007
	StatusPolicyViolation         StatusCode = 1008
	StatusMessageTooBig           StatusCode = 1009
	StatusMandatoryExtension      StatusCode = 1010
	StatusInternalError           StatusCode = 1011
	StatusServiceRestart          StatusCode = 1012
	StatusTryAgainLater           StatusCode = 1013
	StatusBadGateway              StatusCode = 1014
	StatusTLSHandshake            StatusCode = 1015
)

var statusNames = map[StatusCode]string{
	StatusNormalClosure:           "normal closure",
	StatusGoingAway:               "going away",
	StatusProtocolError:           "protocol error",
	StatusUnsupportedData:         "unsupported data",
	StatusNoStatusRcvd:            "no status received",
	StatusAbnormalClosure:         "abnormal closure",
	StatusInvalidFramePayloadData: "invalid frame payload data",
	StatusPolicyViolation:         "policy violation",
	StatusMessageTooBig:           "message too big",
	StatusMandatoryExtension:      "mandatory extension",
	StatusInternalError:           "internal error",
	StatusServiceRestart:          "service restart",
	StatusTryAgainLater:           "try again later",
	StatusBadGateway:              "bad gateway",
	StatusTLSHandshake:            "TLS handshake",
}

// String returns the code and, for a registered code, its name.
func (s StatusCode) String() string {
	if name, ok := statusNames[s]; ok {
		return strconv.Itoa(int(s)) + " (" + name + ")"
	}
	return strconv.Itoa(int(s))
}

// validOnWire reports whether code may appear in a close frame: 1000–1003,
// 1007–1014 and 3000–4999 (RFC 6455 §7.4, the IANA registry, and the
// Autobahn suite's case 7.9).
func validOnWire(code StatusCode) bool {
	return code >= 1000 && code <= 1003 ||
		code >= 1007 && code <= 1014 ||
		code >= 3000 && code <= 4999
}

// maxCloseReason is the longest close reason that fits a control frame.
const maxCloseReason = maxControlPayload - 2

// CloseError reports how a connection ended: the status code and reason of
// the peer's close frame (RFC 6455 §7.1.5–§7.1.6), StatusNoStatusRcvd if
// that frame had no code, or StatusAbnormalClosure if the connection was
// lost without a close frame.
type CloseError struct {
	Code   StatusCode
	Reason string

	err error // the cause of an abnormal closure
}

func (e *CloseError) Error() string {
	s := "nwws: connection closed with status " + e.Code.String()
	if e.Reason != "" {
		s += ": " + e.Reason
	}
	if e.err != nil {
		s += ": " + e.err.Error()
	}
	return s
}

// Unwrap returns the cause of an abnormal closure, or nil.
func (e *CloseError) Unwrap() error { return e.err }

// closePayload encodes a close frame payload (RFC 6455 §5.5.1).
func closePayload(code StatusCode, reason string) []byte {
	b := binary.BigEndian.AppendUint16(make([]byte, 0, 2+len(reason)), uint16(code))
	return append(b, reason...)
}

// parseClose decodes the payload of a peer's close frame into a
// *CloseError, or returns a *protocolError if it is invalid.
func parseClose(p []byte) error {
	switch len(p) {
	case 0:
		return &CloseError{Code: StatusNoStatusRcvd}
	case 1:
		return protocolErrorf(StatusProtocolError, "close frame with a 1-byte payload")
	}
	code := StatusCode(binary.BigEndian.Uint16(p))
	if !validOnWire(code) {
		return protocolErrorf(StatusProtocolError, "invalid close code %d", int(code))
	}
	if !utf8.Valid(p[2:]) {
		return &protocolError{code: StatusInvalidFramePayloadData, err: fmt.Errorf("close reason: %w", errInvalidUTF8)}
	}
	return &CloseError{Code: code, Reason: string(p[2:])}
}

// Close performs the closing handshake (RFC 6455 §7.1.2): it sends a close
// frame with code and reason after any messages already queued, waits a
// bounded time for the peer's close frame, and closes the network
// connection. Messages that arrive meanwhile are discarded.
//
// The code must be valid on the wire: 1000–1003, 1007–1014 or 3000–4999.
// The reason must be UTF-8 and at most 123 bytes. Close returns ErrClosed
// if the connection is already closing, and an error if the peer did not
// answer in time.
func (c *Conn) Close(code StatusCode, reason string) error {
	switch {
	case !validOnWire(code):
		return fmt.Errorf("nwws: close code %d may not be sent", int(code))
	case len(reason) > maxCloseReason:
		return fmt.Errorf("nwws: close reason of %d bytes exceeds %d", len(reason), maxCloseReason)
	case !utf8.ValidString(reason):
		return fmt.Errorf("nwws: close reason: %w", errInvalidUTF8)
	}
	req, err := c.queueClose(closePayload(code, reason))
	if err != nil {
		return err
	}
	c.discardMessages()
	defer c.wait()
	defer c.closeNow(ErrClosed)

	timeout := time.NewTimer(c.cfg.closeTimeout)
	defer timeout.Stop()
	select {
	case err := <-req.done:
		if err != nil {
			return err
		}
	case <-timeout.C:
		return errors.New("nwws: timed out sending the close frame")
	}
	select {
	case <-c.closeRcvd:
	case <-c.readDone:
		select {
		case <-c.closeRcvd:
		default:
			return c.endErr() // ended without the peer's close frame
		}
	case <-timeout.C:
		return errors.New("nwws: timed out waiting for the peer's close frame")
	}
	// The read goroutine finishes the handshake; on the client it waits
	// for the server to close TCP (RFC 6455 §7.1.1).
	select {
	case <-c.readDone:
	case <-timeout.C:
	}
	return nil
}
