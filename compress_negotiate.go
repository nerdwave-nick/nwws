package nwws

import (
	"errors"
	"strconv"
	"strings"
)

// deflateName is the extension token of permessage-deflate (RFC 7692 §7).
const deflateName = "permessage-deflate"

// Parameter names of permessage-deflate (RFC 7692 §7.1).
const (
	paramServerNoContextTakeover = "server_no_context_takeover"
	paramClientNoContextTakeover = "client_no_context_takeover"
	paramServerMaxWindowBits     = "server_max_window_bits"
	paramClientMaxWindowBits     = "client_max_window_bits"
)

// deflateParams are the parameters of a permessage-deflate offer or
// response (RFC 7692 §7.1). A window size of zero means it is absent.
type deflateParams struct {
	serverNoContextTakeover bool
	clientNoContextTakeover bool
	serverMaxWindowBits     int
	clientMaxWindowBits     int
}

// String renders p as a permessage-deflate response value.
func (p deflateParams) String() string {
	var b strings.Builder
	b.WriteString(deflateName)
	if p.serverNoContextTakeover {
		b.WriteString("; " + paramServerNoContextTakeover)
	}
	if p.clientNoContextTakeover {
		b.WriteString("; " + paramClientNoContextTakeover)
	}
	if p.serverMaxWindowBits != 0 {
		b.WriteString("; " + paramServerMaxWindowBits + "=" + strconv.Itoa(p.serverMaxWindowBits))
	}
	if p.clientMaxWindowBits != 0 {
		b.WriteString("; " + paramClientMaxWindowBits + "=" + strconv.Itoa(p.clientMaxWindowBits))
	}
	return b.String()
}

// parseDeflateParams validates the parameters of an offer, or of a
// response when response is true, as RFC 7692 §7.1 requires: each known
// parameter at most once, flags without a value, window sizes 8–15 without
// leading zeros. Only an offer may give client_max_window_bits no value.
func parseDeflateParams(params []extensionParam, response bool) (deflateParams, error) {
	var p deflateParams
	seen := map[string]bool{}
	for _, e := range params {
		if seen[e.name] {
			return p, errors.New("duplicate parameter " + e.name)
		}
		seen[e.name] = true
		var err error
		switch e.name {
		case paramServerNoContextTakeover:
			p.serverNoContextTakeover, err = true, flagParam(e)
		case paramClientNoContextTakeover:
			p.clientNoContextTakeover, err = true, flagParam(e)
		case paramServerMaxWindowBits:
			p.serverMaxWindowBits, err = windowBitsParam(e)
		case paramClientMaxWindowBits:
			if !e.hasValue && !response {
				continue // the client supports the parameter
			}
			p.clientMaxWindowBits, err = windowBitsParam(e)
		default:
			err = errors.New("unknown parameter " + e.name)
		}
		if err != nil {
			return p, err
		}
	}
	return p, nil
}

func flagParam(e extensionParam) error {
	if e.hasValue {
		return errors.New(e.name + " takes no value")
	}
	return nil
}

func windowBitsParam(e extensionParam) (int, error) {
	v := e.value
	if !e.hasValue || len(v) != 1 && len(v) != 2 || v[0] == '0' {
		return 0, errors.New(e.name + " needs a window size of 8 to 15")
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 8 || n > maxWindowBits {
		return 0, errors.New(e.name + " needs a window size of 8 to 15")
	}
	return n, nil
}

// acceptOffer answers the client's Sec-WebSocket-Extensions offer lines
// with a response value, or "" to decline compression. The first valid
// permessage-deflate offer is accepted; invalid ones are declined (RFC 7692
// §5). An offered server_max_window_bits is accepted at the offered size:
// below 15 bits, messages are Huffman-coded only, which needs no window.
func (o *CompressionOptions) acceptOffer(offers []string) string {
	exts, err := parseExtensions(offers)
	if err != nil {
		return ""
	}
	for _, e := range exts {
		if !strings.EqualFold(e.name, deflateName) {
			continue
		}
		offer, err := parseDeflateParams(e.params, false)
		if err != nil {
			continue
		}
		return deflateParams{
			serverNoContextTakeover: offer.serverNoContextTakeover || o.ServerNoContextTakeover,
			// Confirming the client's hint saves keeping its context.
			clientNoContextTakeover: offer.clientNoContextTakeover || o.ClientNoContextTakeover,
			serverMaxWindowBits:     offer.serverMaxWindowBits,
		}.String()
	}
	return ""
}

// offer returns the client's permessage-deflate offer. It always includes
// client_max_window_bits: any window size the server asks for is honoured.
func (o *CompressionOptions) offer() string {
	s := deflateName
	if o.ServerNoContextTakeover {
		s += "; " + paramServerNoContextTakeover
	}
	if o.ClientNoContextTakeover {
		s += "; " + paramClientNoContextTakeover
	}
	return s + "; " + paramClientMaxWindowBits
}

// checkResponse validates the server's Sec-WebSocket-Extensions response
// lines to the offer and returns the agreed value in canonical form.
func (o *CompressionOptions) checkResponse(response []string) (string, error) {
	exts, err := parseExtensions(response)
	if err != nil {
		return "", handshakeErrorf("Sec-WebSocket-Extensions %q: %v", response, err)
	}
	if len(exts) != 1 || !strings.EqualFold(exts[0].name, deflateName) {
		return "", handshakeErrorf("server selected extensions %q that were not offered", response)
	}
	p, err := parseDeflateParams(exts[0].params, true)
	if err != nil {
		return "", handshakeErrorf("permessage-deflate response %q: %v", response, err)
	}
	if o.ServerNoContextTakeover && !p.serverNoContextTakeover {
		return "", handshakeErrorf("server did not accept %s", paramServerNoContextTakeover)
	}
	return p.String(), nil
}
