package transform

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// ZstdTransform applies Zstandard compression on the write path and
// decompression on the read path. It implements [Transform].
//
// Zstd is strictly superior to gzip for this workload:
//   - 2–3× faster at equivalent compression ratio
//   - Better ratio on mixed data (SQLite databases, JSON, text logs)
//   - Negligible overhead on incompressible data (H.264, H.265, JPEG)
//   - Pure Go assembly via klauspost/compress — no CGO, no libzstd
//
// For Frigate NVR recordings (H.264/H.265):
//   - Video segments: ratio ≈ 1.0, overhead ~0.1% CPU — use SpeedFastest
//   - SQLite event database: ratio 3–6×, use SpeedDefault
//   - JSON metadata / logs: ratio 5–15×, use SpeedDefault or SpeedBestCompression
//
// Dictionary compression (zstd.WithEncoderDict / WithDecoderDict) can
// improve ratio significantly for small, repetitive Frigate JSON events.
// Dictionary training is out of scope here but the encoder accepts it.
type ZstdTransform struct {
	level zstd.EncoderLevel
}

// NewZstd returns a ZstdTransform at the given encoder speed level.
// level is one of the zstd.Speed* constants:
//
//	zstd.SpeedFastest         — ~550 MB/s encode, ratio ≈ gzip-1
//	zstd.SpeedDefault         — ~200 MB/s encode, ratio ≈ gzip-6  (recommended)
//	zstd.SpeedBetterCompression — ~80 MB/s encode, ratio ≈ gzip-9
//	zstd.SpeedBestCompression  — ~30 MB/s encode, maximum ratio
//
// Use SpeedFastest when CPU is constrained (e.g. a fanless N100).
// Use SpeedDefault for a good balance on a NAS or MinIO tier.
func NewZstd(level zstd.EncoderLevel) (*ZstdTransform, error) {
	// Validate by constructing a trial encoder.
	enc, err := zstd.NewWriter(io.Discard, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, fmt.Errorf("zstd: invalid level %v: %w", level, err)
	}
	enc.Close()
	return &ZstdTransform{level: level}, nil
}

// Name returns "zstd".
func (z *ZstdTransform) Name() string { return "zstd" }

// Writer returns a zstd encoder that compresses data written to it into dst.
// The returned WriteCloser MUST be closed to flush the zstd frame trailer.
func (z *ZstdTransform) Writer(dst io.Writer) (io.WriteCloser, error) {
	enc, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(z.level))
	if err != nil {
		return nil, fmt.Errorf("zstd writer: %w", err)
	}
	return enc, nil
}

// Reader returns a zstd decoder that decompresses data read from src.
// The returned ReadCloser releases the decoder's internal buffers on Close.
func (z *ZstdTransform) Reader(src io.Reader) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("zstd reader: %w", err)
	}
	return &zstdReadCloser{dec: dec}, nil
}

// zstdReadCloser wraps zstd.Decoder (which has Close but returns no error)
// into an io.ReadCloser.
type zstdReadCloser struct {
	dec *zstd.Decoder
}

func (z *zstdReadCloser) Read(p []byte) (int, error) { return z.dec.Read(p) }
func (z *zstdReadCloser) Close() error               { z.dec.Close(); return nil }
