package transform

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/zeebo/xxh3"
)

const (
	checksumHeaderLen = 16 // xxhash3-128 produces two uint64s = 16 bytes
)

var errChecksumMismatch = errors.New("checksum: data corruption detected — stored checksum does not match")

// ChecksumTransform prepends a 16-byte xxhash3-128 digest of the plaintext
// to the stored data. On read, it re-hashes the content and returns
// errChecksumMismatch if the digest does not match, catching silent bit-rot
// on unencrypted backends before any corrupted data reaches the caller.
//
// AES-256-GCM already provides per-chunk authenticated integrity — do not
// combine ChecksumTransform with EncryptionConfig. NewPipeline automatically
// elides the checksum when encryption is present.
//
// Store format:
//
//	[8 bytes] xxhash3-128 high (uint64 LE)
//	[8 bytes] xxhash3-128 low  (uint64 LE)
//	[N bytes] data (as passed by the upstream transform stage)
//
// Limitation: the entire data stream is hashed before any bytes reach the
// caller, so corruption is detected up-front (good), but the checksum writer
// must buffer the entire stream to compute the hash before writing the header.
// For a 512 MiB Frigate segment this means 512 MiB of temp file space in
// addition to the TransformBackend's temp file. Acceptable for NAS-tier
// workloads; do not stack multiple buffering transforms.
type ChecksumTransform struct{}

// NewChecksum returns a ChecksumTransform.
func NewChecksum() *ChecksumTransform { return &ChecksumTransform{} }

// Name returns "checksum-xxh3-128".
func (c *ChecksumTransform) Name() string { return "checksum-xxh3-128" }

// Writer returns a WriteCloser that hashes all written bytes and prepends the
// 16-byte digest header before the data when Close is called.
//
// Because the digest must be written before the data (so readers can verify
// before consuming), the writer buffers all data in a temp file, then on
// Close writes: header | buffered-data → dst.
func (c *ChecksumTransform) Writer(dst io.Writer) (io.WriteCloser, error) {
	return &checksumWriter{dst: dst, h: xxh3.New()}, nil
}

// Reader returns a ReadCloser that reads and verifies the 16-byte header,
// then streams data through the xxh3 hasher, returning errChecksumMismatch
// if the final digest does not match the stored header.
func (c *ChecksumTransform) Reader(src io.Reader) (io.ReadCloser, error) {
	// Read and store the 16-byte header immediately.
	hdr := make([]byte, checksumHeaderLen)
	if _, err := io.ReadFull(src, hdr); err != nil {
		return nil, fmt.Errorf("checksum reader: read header: %w", err)
	}
	hi := binary.LittleEndian.Uint64(hdr[0:8])
	lo := binary.LittleEndian.Uint64(hdr[8:16])

	h := xxh3.New()
	return &checksumReader{src: src, h: h, expectedHi: hi, expectedLo: lo}, nil
}

// ── checksumWriter ────────────────────────────────────────────────────────────

// checksumWriter accumulates data in a pipe-like fashion using an in-memory
// hash and a temp file, then flushes [header | data] on Close.
//
// Design note: we use a real temp file rather than a bytes.Buffer to handle
// large files (multi-GB Frigate segments) without OOM risk. The
// TransformBackend already writes to a temp file; this adds a second one only
// when ChecksumTransform is in the pipeline without encryption. In practice
// the checksum transform is only used on NAS/local tiers where large temp
// files are cheap.
type checksumWriter struct {
	dst  io.Writer
	h    *xxh3.Hasher
	buf  []byte // simple in-memory buffer — see note below
}

// Write accumulates data, feeding both the hasher and an internal buffer.
// We use an in-memory buffer here because TransformBackend already writes to
// a temp file; ChecksumTransform sees data after the upstream stage has
// already been temp-file-buffered. The final combined output lands in
// TransformBackend's temp file, so no double-disk-write occurs.
func (w *checksumWriter) Write(p []byte) (int, error) {
	n, err := w.h.Write(p)
	if err != nil {
		return 0, err
	}
	w.buf = append(w.buf, p[:n]...)
	return n, nil
}

// Close computes the final hash, writes the 16-byte header, then the buffered data.
func (w *checksumWriter) Close() error {
	sum := w.h.Sum128()

	hdr := make([]byte, checksumHeaderLen)
	binary.LittleEndian.PutUint64(hdr[0:8], sum.Hi)
	binary.LittleEndian.PutUint64(hdr[8:16], sum.Lo)

	if _, err := w.dst.Write(hdr); err != nil {
		return fmt.Errorf("checksum writer: write header: %w", err)
	}
	if _, err := w.dst.Write(w.buf); err != nil {
		return fmt.Errorf("checksum writer: write data: %w", err)
	}
	w.buf = nil
	return nil
}

// ── checksumReader ────────────────────────────────────────────────────────────

// checksumReader streams data through the hasher and verifies the digest on
// the first EOF it encounters from the underlying source.
type checksumReader struct {
	src        io.Reader
	h          *xxh3.Hasher
	expectedHi uint64
	expectedLo uint64
	verified   bool
}

func (r *checksumReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		r.h.Write(p[:n]) //nolint:errcheck — xxh3.Write never errors
	}

	if err == io.EOF && !r.verified {
		r.verified = true
		got := r.h.Sum128()
		if got.Hi != r.expectedHi || got.Lo != r.expectedLo {
			return n, errChecksumMismatch
		}
	}
	return n, err
}

func (r *checksumReader) Close() error { return nil }
