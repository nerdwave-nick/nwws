package nwws

import (
	"math"
	"time"
)

// DefaultReadLimit is the read limit, in bytes, of a connection whose
// options leave ReadLimit unset.
const DefaultReadLimit = 1 << 20

// DefaultReadBuffer is the read-ahead budget, in bytes, of a connection
// whose options leave ReadBuffer unset.
const DefaultReadBuffer = 1 << 20

// defaultCloseTimeout bounds each wait of the closing handshake.
const defaultCloseTimeout = 5 * time.Second

// ConnOptions configures a connection on either side. [AcceptOptions] and
// [DialOptions] embed it. The zero value uses the defaults.
type ConnOptions struct {
	// ReadLimit is the size, in bytes, of the largest message the
	// connection reads. A larger message closes the connection with
	// StatusMessageTooBig (1009). The limit also bounds the memory a
	// message being read takes. For a compressed message, it applies to
	// the decompressed size only. Zero or less means DefaultReadLimit.
	ReadLimit int64

	// PingInterval is how often the connection pings the peer. Zero or
	// less means DefaultPingInterval.
	PingInterval time.Duration

	// PongTimeout is how long the connection waits, after a ping, for the
	// peer to send a pong or any other data. Then it sends a close frame
	// with StatusGoingAway (1001), without waiting for the peer's, closes
	// the connection, and Read returns an error matching
	// ErrLivenessTimeout.
	// The wait is suspended while the connection has stopped reading
	// until Read makes room in the full ReadBuffer budget. Zero or less
	// means DefaultPongTimeout.
	PongTimeout time.Duration

	// ReadBuffer is the read-ahead budget, in bytes. The connection keeps
	// reading frames, and so answering pings, while complete messages wait
	// for Read, as long as they take less memory than ReadBuffer. Each
	// message counts its size plus a few dozen bytes of bookkeeping. A
	// message is always queued when none waits, even if it is larger. Once
	// the budget is full, the connection stops reading until Read makes
	// room, and TCP pushes back on the peer. Memory for reading is bounded
	// by ReadBuffer plus one message being read. Zero or less means
	// DefaultReadBuffer.
	ReadBuffer int64

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
	readBuffer   int // read-ahead budget in bytes; zero means DefaultReadBuffer
	closeTimeout time.Duration
	pingInterval time.Duration // zero when liveness is off
	pongTimeout  time.Duration
	deflate      *deflateConfig // nil without compression
}

func (o *ConnOptions) config() connConfig {
	cfg := connConfig{
		readLimit:    DefaultReadLimit,
		readBuffer:   DefaultReadBuffer,
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
	if o.ReadBuffer > 0 {
		cfg.readBuffer = int(min(o.ReadBuffer, math.MaxInt))
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
