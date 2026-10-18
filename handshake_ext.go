package nwws

// This file is the extension seam of the opening handshake
// (RFC 6455 §9.1). No extension is implemented yet: the server ignores
// every Sec-WebSocket-Extensions offer and the client offers none, so it
// refuses any extension a server selects. permessage-deflate (RFC 7692)
// plugs in here by negotiating from its options.

// negotiateExtensions answers a client's Sec-WebSocket-Extensions offer
// header lines with the response header value, or "" to accept none.
func (o *AcceptOptions) negotiateExtensions(offers []string) string {
	return ""
}

// extensionOffer returns the client's Sec-WebSocket-Extensions offer, or
// "" to offer none.
func (o *DialOptions) extensionOffer() string {
	return ""
}

// checkExtensions validates the server's Sec-WebSocket-Extensions response
// header lines against offer and returns the agreed value.
func (o *DialOptions) checkExtensions(offer string, response []string) (string, error) {
	if len(response) > 0 {
		return "", handshakeErrorf("server selected extensions %q that were not offered", response)
	}
	return "", nil
}
