package nwws

import (
	"compress/flate"
	"fmt"
)

// DefaultCompressionThreshold is the compression threshold, in bytes, of
// CompressionOptions that leave Threshold unset.
const DefaultCompressionThreshold = 128

// maxWindowBits is the LZ77 window of compress/flate: 32 KiB, 2^15 bytes.
const maxWindowBits = 15

// CompressionOptions enables the permessage-deflate extension (RFC 7692).
// It is negotiated only when both sides enable it; otherwise the connection
// is uncompressed. The zero value enables compression with context takeover
// in both directions, the default threshold and level flate.BestSpeed.
type CompressionOptions struct {
	// Threshold is the size, in bytes, from which a message is sent
	// compressed. Shorter messages are sent as they are. Zero or less means
	// DefaultCompressionThreshold.
	Threshold int

	// ServerNoContextTakeover keeps the server from carrying its
	// compression context from one message to the next
	// (server_no_context_takeover). That saves the server memory and costs
	// compression ratio. A client with it set offers the parameter and
	// refuses a server that does not accept it.
	ServerNoContextTakeover bool

	// ClientNoContextTakeover is the same for the client's compression
	// context (client_no_context_takeover). A client with it set never uses
	// context takeover; a server with it set asks the client not to.
	ClientNoContextTakeover bool

	// Level is the compress/flate compression level: flate.HuffmanOnly or
	// 1 (flate.BestSpeed) to 9 (flate.BestCompression). Zero means
	// flate.BestSpeed. Accept and Dial refuse any other value with an
	// error. If the peer limits the window below 15 bits, messages are
	// Huffman-coded only, whatever the level.
	Level int
}

// check reports an error for options that Accept and Dial refuse before
// any upgrade or dial: a compression level out of range.
func (o *ConnOptions) check() error {
	if o == nil || o.Compression == nil {
		return nil
	}
	switch l := o.Compression.Level; {
	case l == 0, l == flate.HuffmanOnly, flate.BestSpeed <= l && l <= flate.BestCompression:
		return nil
	default:
		return fmt.Errorf("nwws: compression level %d is not 0, flate.HuffmanOnly or 1 to 9", l)
	}
}

// deflateConfig is the effective permessage-deflate configuration of a
// connection, from the parameters both sides agreed on.
type deflateConfig struct {
	threshold int
	// level is the compression level of a 15-bit window.
	level int
	// writeTakeover keeps the compression context between the messages
	// this side sends.
	writeTakeover bool
	// writeWindowBits is the largest LZ77 window this side may compress
	// with. Below maxWindowBits, messages are Huffman-coded only.
	writeWindowBits int
	// readTakeover reports that the peer keeps its compression context, so
	// the decompression context is kept too.
	readTakeover bool
}

// negotiated returns the configuration agreed in the extensions value of a
// completed handshake, or nil when permessage-deflate was not agreed.
func (o *CompressionOptions) negotiated(extensions string, server bool) *deflateConfig {
	if o == nil || extensions == "" {
		return nil
	}
	// The handshake has validated the value and put it in canonical form.
	exts, err := parseExtensions([]string{extensions})
	if err != nil || len(exts) != 1 {
		return nil
	}
	p, err := parseDeflateParams(exts[0].params, true)
	if err != nil {
		return nil
	}
	cfg := &deflateConfig{threshold: o.Threshold, level: o.Level}
	if cfg.threshold <= 0 {
		cfg.threshold = DefaultCompressionThreshold
	}
	if cfg.level == 0 {
		cfg.level = flate.BestSpeed
	}
	if server {
		cfg.writeTakeover = !p.serverNoContextTakeover
		cfg.writeWindowBits = p.serverMaxWindowBits
		cfg.readTakeover = !p.clientNoContextTakeover
	} else {
		// A client that offered client_no_context_takeover keeps to it
		// (RFC 7692 §7.1.1.2).
		cfg.writeTakeover = !p.clientNoContextTakeover && !o.ClientNoContextTakeover
		cfg.writeWindowBits = p.clientMaxWindowBits
		cfg.readTakeover = !p.serverNoContextTakeover
	}
	if cfg.writeWindowBits == 0 {
		cfg.writeWindowBits = maxWindowBits
	}
	return cfg
}

// configFor returns the configuration of a connection whose handshake
// agreed on extensions.
func (o *ConnOptions) configFor(extensions string, server bool) connConfig {
	cfg := o.config()
	if o != nil {
		cfg.deflate = o.Compression.negotiated(extensions, server)
	}
	return cfg
}

// initCompression sets up permessage-deflate when it was negotiated: RSV1
// marks compressed messages (RFC 7692 §6).
func (c *Conn) initCompression() {
	cfg := c.cfg.deflate
	if cfg == nil {
		return
	}
	c.fr.rsv, c.fw.rsv = rsv1Bit, rsv1Bit
	c.inflate = &inflater{takeover: cfg.readTakeover}
	c.deflate = &deflater{cfg: *cfg}
}
