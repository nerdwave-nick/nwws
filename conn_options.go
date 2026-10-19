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
	// message being read takes. Zero or less means DefaultReadLimit.
	ReadLimit int64
}

// connConfig is the effective configuration of a connection.
type connConfig struct {
	readLimit    int64
	closeTimeout time.Duration
}

func (o *ConnOptions) config() connConfig {
	cfg := connConfig{readLimit: DefaultReadLimit, closeTimeout: defaultCloseTimeout}
	if o != nil && o.ReadLimit > 0 {
		// A message must fit in a slice, which matters on 32-bit platforms.
		cfg.readLimit = min(o.ReadLimit, math.MaxInt)
	}
	return cfg
}
