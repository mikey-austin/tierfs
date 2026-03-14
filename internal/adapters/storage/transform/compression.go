package transform

import (
	"compress/gzip"
	"fmt"
	"io"
)

// GzipTransform applies gzip compression on the write path and decompression
// on the read path. It implements [Transform].
//
// For already-compressed media files (H.264, H.265, AAC, JPEG, PNG with
// compression) the compression ratio will be near 1.0 and the overhead is
// minimal. Consider skipping compression for pure video/audio workloads and
// only enabling it for metadata-heavy or text-based storage tiers.
//
// To replace gzip with zstd (much faster at equivalent ratios), substitute
// klauspost/compress/zstd — the Writer/Reader signatures are compatible.
type GzipTransform struct {
	level int
}

// NewGzip returns a GzipTransform at the given compression level.
// level is one of the compress/gzip constants:
//   - gzip.NoCompression     = 0
//   - gzip.BestSpeed         = 1
//   - gzip.DefaultCompression = -1
//   - gzip.BestCompression    = 9
//
// Level 0 (NoCompression) wraps data in a gzip container without compressing —
// useful for testing or when you want the gzip checksum without the CPU cost.
// Use gzip.DefaultCompression (-1) unless you have specific latency requirements.
func NewGzip(level int) (*GzipTransform, error) {
	// Validate by constructing a dummy writer.
	w, err := gzip.NewWriterLevel(io.Discard, level)
	if err != nil {
		return nil, fmt.Errorf("gzip: invalid level %d: %w", level, err)
	}
	w.Close()
	return &GzipTransform{level: level}, nil
}

// Name returns "gzip".
func (g *GzipTransform) Name() string { return "gzip" }

// Writer returns a gzip.Writer that compresses data written to it into dst.
// The returned WriteCloser must be closed to write the gzip trailer (CRC32
// and size); failing to close will produce a corrupt gzip stream.
func (g *GzipTransform) Writer(dst io.Writer) (io.WriteCloser, error) {
	w, err := gzip.NewWriterLevel(dst, g.level)
	if err != nil {
		return nil, fmt.Errorf("gzip writer: %w", err)
	}
	return w, nil
}

// Reader returns a gzip.Reader that decompresses data read from src.
// The returned ReadCloser must be closed after use; it validates the gzip
// checksum on Close, so any corruption will be surfaced at that point.
func (g *GzipTransform) Reader(src io.Reader) (io.ReadCloser, error) {
	r, err := gzip.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	return r, nil
}
