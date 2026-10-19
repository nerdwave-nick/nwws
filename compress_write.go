package nwws

import (
	"bytes"
	"compress/flate"
	"io"
	"sync"
)

// compressionLevel trades compression ratio for speed in favour of speed,
// as messages are compressed on the connection's single writer.
const compressionLevel = flate.BestSpeed

// maxKeptBuffer bounds the compression buffer a connection keeps between
// messages; a larger one, left by a large message, is released.
const maxKeptBuffer = 64 << 10

// flateWriters pools compressors without context takeover: index 0 for
// compressionLevel, 1 for Huffman coding only.
var flateWriters [2]sync.Pool

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
		flateWriters[d.pool()].Put(w)
	}
	b := d.buf.Bytes()
	return b[:len(b)-len(syncFlushTail)]
}

// pool returns the index of the compressor pool for d.
func (d *deflater) pool() int {
	if d.cfg.writeWindowBits < maxWindowBits {
		return 1
	}
	return 0
}

// newWriter returns a compressor writing to d.buf. compress/flate always
// uses a 32 KiB window. When the peer allows less (RFC 7692 §7.1.2),
// Huffman coding only, without LZ77 matches, honours any window size.
func (d *deflater) newWriter() *flate.Writer {
	if w, ok := flateWriters[d.pool()].Get().(*flate.Writer); ok {
		w.Reset(&d.buf)
		return w
	}
	level := compressionLevel
	if d.pool() == 1 {
		level = flate.HuffmanOnly
	}
	w, _ := flate.NewWriter(&d.buf, level) // the level is valid
	return w
}
