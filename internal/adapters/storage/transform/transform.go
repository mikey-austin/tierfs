// Package transform provides a domain.Backend decorator that applies a
// pipeline of reversible byte-level transformations to file data at rest.
//
// Usage:
//
//	inner := file.New(root)          // or s3.New(...)
//	comp  := transform.NewGzip(gzip.BestSpeed)
//	enc   := transform.NewAES256GCM(key)
//	tb    := transform.New(inner, transform.Pipeline(comp, enc))
//
// The pipeline is applied in declaration order on the write path and reversed
// on the read path:
//
//	Write:  plaintext → comp → enc → inner.Put()
//	Read:   inner.Get() → dec → decomp → plaintext
//
// Use [NewPipeline] to build a pipeline from config; it enforces
// compression-before-encryption ordering regardless of config order.
//
// IMPORTANT: LocalPath always returns ("", false) on a TransformBackend.
// Transformed files cannot be served directly to the FUSE layer via the
// kernel page cache; reads always go through the full decode pipeline.
package transform

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/domain"
)

// Transform describes a reversible byte-level transformation applied to file
// data at rest. The forward direction (write path) is applied by Writer; the
// inverse (read path) is applied by Reader. Implementations must be safe for
// concurrent use with multiple simultaneous calls to Writer and Reader.
type Transform interface {
	// Name is a short identifier used in log messages and error strings.
	Name() string

	// Writer wraps dst to apply the forward transformation.
	// All writes to the returned WriteCloser are transformed before reaching dst.
	// The returned WriteCloser MUST be closed to flush any buffered data
	// (e.g. gzip trailer, GCM authentication tag).
	Writer(dst io.Writer) (io.WriteCloser, error)

	// Reader wraps src to apply the inverse transformation.
	// Reads from the returned ReadCloser are decoded from src.
	// The caller must close the returned ReadCloser when done.
	Reader(src io.Reader) (io.ReadCloser, error)
}

// Backend wraps a domain.Backend and applies a pipeline of Transforms to all
// file data. Put applies the pipeline in order; Get applies it in reverse.
type Backend struct {
	inner    domain.Backend
	pipeline []Transform
	log      *zap.Logger
}

// New constructs a TransformBackend. pipeline is applied in order on write and
// in reverse on read. Use [Pipeline] or [NewPipeline] to build the pipeline.
func New(inner domain.Backend, pipeline []Transform, log *zap.Logger) *Backend {
	return &Backend{
		inner:    inner,
		pipeline: pipeline,
		log:      log.Named("transform-backend").With(zap.String("inner", inner.URI(""))),
	}
}

// Scheme delegates to the inner backend.
func (b *Backend) Scheme() string { return b.inner.Scheme() }

// URI delegates to the inner backend.
func (b *Backend) URI(relPath string) string { return b.inner.URI(relPath) }

// LocalPath always returns ("", false). Transformed files cannot be served
// directly to FUSE callers — they must be decoded first.
func (b *Backend) LocalPath(_ string) (string, bool) { return "", false }

// IsFinal delegates to the inner backend if it implements domain.Finalizer.
// A TransformBackend wrapping a null:// backend is itself a final tier.
func (b *Backend) IsFinal() bool {
	if fin, ok := b.inner.(domain.Finalizer); ok {
		return fin.IsFinal()
	}
	return false
}

// Put transforms data from r through the pipeline (in order) and writes the
// result to the inner backend.
//
// Because transformation changes the data size (compression ratio, encryption
// overhead), the transformed output is first written to a temp file so the
// actual byte count can be determined before calling inner.Put(). This avoids
// loading potentially gigabyte-sized files into memory.
func (b *Backend) Put(ctx context.Context, relPath string, r io.Reader, _ int64) error {
	// Write transformed data to a temp file to capture the actual output size.
	tmp, err := os.CreateTemp("", "tierfs-transform-*")
	if err != nil {
		return fmt.Errorf("transform put %q: create temp: %w", relPath, err)
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()

	// Build the writer chain from innermost to outermost.
	// pipeline = [A, B, C] → write chain: r → A → B → C → tmp
	if err := b.applyWritePipeline(r, tmp); err != nil {
		return fmt.Errorf("transform put %q: %w", relPath, err)
	}

	// Determine actual output size.
	stat, err := tmp.Stat()
	if err != nil {
		return fmt.Errorf("transform put %q: stat temp: %w", relPath, err)
	}
	transformedSize := stat.Size()

	// Seek back to the start for reading.
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("transform put %q: seek temp: %w", relPath, err)
	}

	b.log.Debug("transform put",
		zap.String("path", relPath),
		zap.Int64("transformed_bytes", transformedSize),
	)

	return b.inner.Put(ctx, relPath, tmp, transformedSize)
}

// Get reads from the inner backend and applies the pipeline in reverse
// (decrypt, then decompress) before returning data to the caller.
func (b *Backend) Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error) {
	rc, _, err := b.inner.Get(ctx, relPath)
	if err != nil {
		return nil, 0, err
	}

	// Build the reader chain from innermost (stored) to outermost (plaintext).
	// pipeline = [A, B, C] → read chain: inner → C⁻¹ → B⁻¹ → A⁻¹ → caller
	decoded, err := b.applyReadPipeline(rc)
	if err != nil {
		rc.Close()
		return nil, 0, fmt.Errorf("transform get %q: %w", relPath, err)
	}

	// Size is unknown after decoding — return -1.
	// Callers (FUSE layer, replicator) must not rely on the returned size for
	// anything other than informational logging.
	return decoded, -1, nil
}

// Stat delegates to the inner backend. The returned size is the *transformed*
// (stored) size, not the original plaintext size. This is intentional: the
// evictor and replicator use the stored size for capacity accounting.
func (b *Backend) Stat(ctx context.Context, relPath string) (*domain.FileInfo, error) {
	return b.inner.Stat(ctx, relPath)
}

// Delete delegates directly to the inner backend.
func (b *Backend) Delete(ctx context.Context, relPath string) error {
	return b.inner.Delete(ctx, relPath)
}

// List delegates to the inner backend. Returned FileInfo sizes are the stored
// (transformed) sizes.
func (b *Backend) List(ctx context.Context, prefix string) ([]domain.FileInfo, error) {
	return b.inner.List(ctx, prefix)
}

// ── Pipeline helpers ──────────────────────────────────────────────────────────

// applyWritePipeline builds and runs the write chain from r through all
// transforms in order, writing the final output to dst.
func (b *Backend) applyWritePipeline(r io.Reader, dst io.Writer) error {
	// Build a stack of writers: [w0, w1, ..., wN] where w0 wraps dst.
	// pipeline = [A, B, C], so chain = r → A.Writer → B.Writer → C.Writer → dst
	// We build from the end: C wraps dst, B wraps C, A wraps B.
	writers := make([]io.WriteCloser, len(b.pipeline))
	current := dst

	for i := len(b.pipeline) - 1; i >= 0; i-- {
		wc, err := b.pipeline[i].Writer(current)
		if err != nil {
			// Close any writers already opened.
			for j := i + 1; j < len(writers); j++ {
				writers[j].Close()
			}
			return fmt.Errorf("open %s writer: %w", b.pipeline[i].Name(), err)
		}
		writers[i] = wc
		current = wc
	}

	// Copy source data through the outermost writer (pipeline[0]).
	if _, err := io.Copy(current, r); err != nil {
		for _, w := range writers {
			if w != nil {
				w.Close()
			}
		}
		return fmt.Errorf("copy through pipeline: %w", err)
	}

	// Close writers from outermost to innermost to flush in correct order.
	for i := 0; i < len(writers); i++ {
		if err := writers[i].Close(); err != nil {
			// Close remaining even if one errors.
			for j := i + 1; j < len(writers); j++ {
				writers[j].Close()
			}
			return fmt.Errorf("close %s writer: %w", b.pipeline[i].Name(), err)
		}
	}
	return nil
}

// applyReadPipeline wraps src with all transforms in reverse order,
// returning a ReadCloser that decodes src on each Read call.
// pipeline = [A, B, C] → chain: src → C.Reader → B.Reader → A.Reader → caller
func (b *Backend) applyReadPipeline(src io.ReadCloser) (io.ReadCloser, error) {
	readers := make([]io.ReadCloser, 0, len(b.pipeline)+1)
	readers = append(readers, src) // always close the inner reader

	current := io.Reader(src)
	for i := len(b.pipeline) - 1; i >= 0; i-- {
		rc, err := b.pipeline[i].Reader(current)
		if err != nil {
			for _, r := range readers {
				r.Close()
			}
			return nil, fmt.Errorf("open %s reader: %w", b.pipeline[i].Name(), err)
		}
		readers = append(readers, rc)
		current = rc
	}

	// Return a ReadCloser that closes all layers in reverse.
	return &multiCloser{r: current, closers: readers}, nil
}

// multiCloser wraps a reader and closes a stack of ReadClosers on Close().
// Closers are closed in reverse order (outermost decode layer first).
type multiCloser struct {
	r       io.Reader
	closers []io.ReadCloser
}

func (m *multiCloser) Read(p []byte) (int, error) { return m.r.Read(p) }

func (m *multiCloser) Close() error {
	var first error
	for i := len(m.closers) - 1; i >= 0; i-- {
		if err := m.closers[i].Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ── Pipeline constructor ──────────────────────────────────────────────────────

// Pipeline returns a pipeline slice from the given transforms in the order
// provided. For config-driven construction use NewPipeline, which enforces
// correct ordering and elides redundant transforms automatically.
func Pipeline(transforms ...Transform) []Transform {
	return transforms
}

// Config holds the optional transform configuration for a backend.
// All fields are optional and independent; set whichever you need.
// NewPipeline figures out the correct ordering and redundancy elimination:
//
//	Canonical write-path order: compress → checksum → encrypt
//
// Rules applied by NewPipeline regardless of which fields are set:
//
//  1. Compression always precedes checksum and encryption.
//     (Compressing encrypted data yields no size reduction.)
//
//  2. Checksum is silently elided when Encryption is also configured.
//     AES-256-GCM provides per-chunk authenticated integrity with stronger
//     guarantees than a standalone checksum; adding both is redundant.
//
//  3. Encryption is always last — it must wrap all other transforms so the
//     entire stored payload is confidential.
//
// The user does not need to know or remember these rules.
type Config struct {
	// Compression selects and configures the compression algorithm.
	// Nil means no compression.
	Compression *CompressionConfig

	// Checksum enables bit-rot detection for unencrypted backends.
	// Automatically elided when Encryption is also set (AEAD makes it redundant).
	// Nil means no checksum.
	Checksum *ChecksumConfig

	// Encryption configures AES-256-GCM authenticated encryption.
	// When set, Checksum is automatically elided.
	// Nil means no encryption.
	Encryption *EncryptionConfig
}

// CompressionConfig selects the compression algorithm and its parameters.
type CompressionConfig struct {
	// Algorithm selects the compression codec: "gzip" or "zstd" (default: "zstd").
	// zstd is recommended for all new deployments — it is faster at equivalent
	// ratios and handles incompressible data (H.264/H.265 video) with lower
	// overhead than gzip.
	Algorithm string

	// Level is the compression level. Interpretation depends on Algorithm:
	//   gzip: 1 (BestSpeed) – 9 (BestCompression), -1 = default (≈6)
	//   zstd: 1 (SpeedFastest) – 4 (SpeedBestCompression), 0 = default (SpeedDefault=2)
	// For video workloads, use level 1 (fastest). For metadata/logs, use default.
	Level int
}

// ChecksumConfig configures the checksum transform. Currently no fields are
// required — the algorithm is fixed to xxhash3-128.
type ChecksumConfig struct{}

// EncryptionConfig configures the AES-256-GCM encryption transform.
type EncryptionConfig struct {
	// KeyEnv is the name of an environment variable containing the 64-char hex key.
	// Takes precedence over KeyHex if both are set. Preferred for production.
	KeyEnv string

	// KeyHex is a 64-character lowercase hex string encoding a 32-byte AES-256 key.
	// Avoid committing to version control; use KeyEnv instead.
	KeyHex string
}

// PipelineReason documents why a specific transform ordering was chosen.
// Returned alongside the pipeline by NewPipeline for logging/debugging.
type PipelineReason struct {
	// Order is the human-readable canonical pipeline description.
	Order string
	// ChecksumElided is true when a ChecksumConfig was provided but omitted
	// because Encryption is also configured (AEAD makes it redundant).
	ChecksumElided bool
}

// NewPipeline builds a correctly ordered, redundancy-free transform pipeline
// from Config. It returns the pipeline and a PipelineReason explaining any
// automatic decisions made (e.g. checksum elision).
//
// The caller does not need to know the ordering rules — this function is the
// single authoritative source of pipeline construction logic.
//
// Canonical write-path order: [compress] → [checksum] → [encrypt]
// Read path is always the exact reverse.
func NewPipeline(cfg Config) ([]Transform, PipelineReason, error) {
	var (
		pipeline []Transform
		reason   PipelineReason
		names    []string
	)

	// Stage 1 (write-first): Compression.
	if cfg.Compression != nil {
		t, err := buildCompression(cfg.Compression)
		if err != nil {
			return nil, reason, fmt.Errorf("compression: %w", err)
		}
		pipeline = append(pipeline, t)
		names = append(names, t.Name())
	}

	// Stage 2: Checksum — only if encryption is NOT also configured.
	// AES-256-GCM provides authenticated integrity per chunk; a standalone
	// xxhash3 checksum on top is redundant and wastes space/CPU.
	if cfg.Checksum != nil {
		if cfg.Encryption != nil {
			// Silently elide — inform caller via reason.
			reason.ChecksumElided = true
		} else {
			pipeline = append(pipeline, NewChecksum())
			names = append(names, "checksum-xxh3-128")
		}
	}

	// Stage 3 (write-last): Encryption.
	if cfg.Encryption != nil {
		key, err := resolveKey(cfg.Encryption)
		if err != nil {
			return nil, reason, fmt.Errorf("encryption: %w", err)
		}
		pipeline = append(pipeline, NewAES256GCM(key))
		names = append(names, "aes-256-gcm")
	}

	// Build human-readable description.
	if len(names) == 0 {
		reason.Order = "passthrough (no transforms)"
	} else {
		reason.Order = joinNames(names)
	}

	return pipeline, reason, nil
}

func joinNames(names []string) string {
	s := ""
	for i, n := range names {
		if i > 0 {
			s += " → "
		}
		s += n
	}
	return s
}

// buildCompression constructs the appropriate compression Transform from config.
func buildCompression(cfg *CompressionConfig) (Transform, error) {
	algo := cfg.Algorithm
	if algo == "" {
		algo = "zstd" // default to zstd for new deployments
	}
	switch algo {
	case "zstd":
		level := zstdLevel(cfg.Level)
		return NewZstd(level)
	case "gzip":
		return NewGzip(cfg.Level)
	default:
		return nil, fmt.Errorf("unknown compression algorithm %q (supported: zstd, gzip)", algo)
	}
}

// zstdLevel maps a generic int level (0–4) to a zstd.EncoderLevel.
// 0 or unset → SpeedDefault.
func zstdLevel(level int) zstd.EncoderLevel {
	switch level {
	case 1:
		return zstd.SpeedFastest
	case 3:
		return zstd.SpeedBetterCompression
	case 4:
		return zstd.SpeedBestCompression
	default:
		return zstd.SpeedDefault // level 0 or 2
	}
}

// resolveKey extracts the 32-byte AES key from EncryptionConfig.
// KeyEnv takes precedence over KeyHex.
func resolveKey(cfg *EncryptionConfig) ([]byte, error) {
	hexStr := cfg.KeyHex
	if cfg.KeyEnv != "" {
		envVal := os.Getenv(cfg.KeyEnv)
		if envVal == "" {
			return nil, fmt.Errorf("environment variable %q is empty or unset", cfg.KeyEnv)
		}
		hexStr = envVal
	}
	if hexStr == "" {
		return nil, fmt.Errorf("no key provided: set key_hex or key_env")
	}
	return decodeHexKey(hexStr)
}

// ── Janitor for staging temp files ───────────────────────────────────────────

// SweepStagingDir removes transform temp files older than maxAge from dir.
// Call periodically if the process can crash mid-Put, leaving orphaned temps.
func SweepStagingDir(dir string, maxAge time.Duration) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	cutoff := time.Now().Add(-maxAge)
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			os.Remove(dir + "/" + e.Name()) //nolint:errcheck
		}
	}
	return nil
}
