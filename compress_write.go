package nwws

import (
	"bytes"
	"compress/flate"
	"io"
	"sync"
)

// maxKeptBuffer bounds the compression buffer a connection keeps between
// messages; a larger one, left by a large message, is released.
const maxKeptBuffer = 64 << 10

// flateWriters pools compressors without context takeover, one pool per
// level so that a connection never gets a compressor of another level:
// index 0 for flate.HuffmanOnly, 1–9 for the levels 1–9.
var flateWriters [flate.BestCompression + 1]sync.Pool

// deflater compresses the messages a connection sends. The write goroutine
// owns it.
type deflater struct {
	cfg deflateConfig
	buf bytes.Buffer
	w   *flate.Writer // kept between messages with context takeover
}

// frame returns the RSV bits and payload of a frame with opcode op and
// payload p: compressed with RSV1 set for a data message of at least the
// threshold, as it is otherwise. A nil deflater compresses nothing. The
// compressed payload is valid until the next call.
func (d *deflater) frame(op opcode, p []byte) (byte, []byte) {
	if d == nil || op.isControl() || len(p) < d.cfg.threshold {
		return 0, p
	}
	return rsv1Bit, d.compress(p)
}

// compress returns p compressed, ending with a sync flush whose empty
// stored block 0x00 0x00 0xff 0xff is removed (RFC 7692 §7.2.1).
func (d *deflater) compress(p []byte) []byte {
	if d.buf.Cap() > maxKeptBuffer {
		d.buf = bytes.Buffer{}
	}
	d.buf.Reset()
	w := d.w
	if w == nil {
		w = d.newWriter()
	}
	// Writing to a bytes.Buffer cannot fail.
	_, _ = w.Write(p)
	_ = w.Flush()
	if d.cfg.writeTakeover {
		d.w = w
	} else {
		w.Reset(io.Discard) // the pooled writer must not keep d.buf alive
		d.pool().Put(w)
	}
	b := d.buf.Bytes()
	return b[:len(b)-len(syncFlushTail)]
}

// level returns the compression level of d. compress/flate always uses a
// 32 KiB window. When the peer allows less (RFC 7692 §7.1.2), Huffman
// coding only, without LZ77 matches, honours any window size.
func (d *deflater) level() int {
	if d.cfg.writeWindowBits < maxWindowBits {
		return flate.HuffmanOnly
	}
	return d.cfg.level
}

// pool returns the compressor pool of d's level.
func (d *deflater) pool() *sync.Pool {
	if l := d.level(); l != flate.HuffmanOnly {
		return &flateWriters[l]
	}
	return &flateWriters[0]
}

// newWriter returns a compressor at d's level writing to d.buf.
func (d *deflater) newWriter() *flate.Writer {
	if w, ok := d.pool().Get().(*flate.Writer); ok {
		w.Reset(&d.buf)
		return w
	}
	w, _ := flate.NewWriter(&d.buf, d.level()) // Accept and Dial checked the level
	return w
}
