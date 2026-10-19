package nwws

import (
	"math"
	"time"
)

// DefaultReadLimit is the read limit, in bytes, of a connection whose
// options leave ReadLimit unset.
const DefaultReadLimit = 1 << 20

// defaultCloseTimeout bounds each wait of the closing handshake.
const defaultCloseTimeout = 5 * time.Second

// ConnOptions configures a connection on either side. [AcceptOptions] and
// [DialOptions] embed it. The zero value uses the defaults.
type ConnOptions struct {
	// ReadLimit is the size, in bytes, of the largest message the
	// connection reads. A larger message closes the connection with
	// StatusMessageTooBig (1009). The limit also bounds the memory a
	// message being read takes. For a compressed message, it applies to
	// the compressed and to the decompressed size. Zero or less means
	// DefaultReadLimit.
	ReadLimit int64

	// PingInterval is how often the connection pings the peer. Zero or
	// less means DefaultPingInterval.
	PingInterval time.Duration

	// PongTimeout is how long the connection waits, after a ping, for the
	// peer to send a pong or any other data. Then it closes the
	// connection, and Read returns an error matching ErrLivenessTimeout.
	// The wait is suspended while a message waits for Read. Zero or less
	// means DefaultPongTimeout.
	PongTimeout time.Duration

	// DisableLiveness turns pings and the pong timeout off. Pings from the
	// peer are still answered.
	DisableLiveness bool

	// Compression enables permessage-deflate compression (RFC 7692). It
	// is used only when the peer enables it too. If nil, it is off.
	Compression *CompressionOptions
}

// connConfig is the effective configuration of a connection.
type connConfig struct {
	readLimit    int64
	closeTimeout time.Duration
	pingInterval time.Duration // zero when liveness is off
	pongTimeout  time.Duration
	deflate      *deflateConfig // nil without compression
}

func (o *ConnOptions) config() connConfig {
	cfg := connConfig{
		readLimit:    DefaultReadLimit,
		closeTimeout: defaultCloseTimeout,
		pingInterval: DefaultPingInterval,
		pongTimeout:  DefaultPongTimeout,
	}
	if o == nil {
		return cfg
	}
	if o.ReadLimit > 0 {
		// A message must fit in a slice, which matters on 32-bit platforms.
		cfg.readLimit = min(o.ReadLimit, math.MaxInt)
	}
	if o.PingInterval > 0 {
		cfg.pingInterval = o.PingInterval
	}
	if o.PongTimeout > 0 {
		cfg.pongTimeout = o.PongTimeout
	}
	if o.DisableLiveness {
		cfg.pingInterval, cfg.pongTimeout = 0, 0
	}
	return cfg
}
