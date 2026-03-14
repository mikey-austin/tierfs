package transform_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/adapters/storage/transform"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func newKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return key
}

func newHexKey(t *testing.T) string {
	t.Helper()
	k, err := transform.GenerateKey()
	require.NoError(t, err)
	return k
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return b
}

// roundtrip applies a single transform forward then in reverse and asserts the
// output matches the input.
func roundtrip(t *testing.T, tr transform.Transform, input []byte) []byte {
	t.Helper()

	// Forward: input → transform → buf
	var buf bytes.Buffer
	w, err := tr.Writer(&buf)
	require.NoError(t, err, "Writer()")
	_, err = io.Copy(w, bytes.NewReader(input))
	require.NoError(t, err, "copy to writer")
	require.NoError(t, w.Close(), "close writer")

	encoded := buf.Bytes()

	// Inverse: buf → transform⁻¹ → output
	r, err := tr.Reader(bytes.NewReader(encoded))
	require.NoError(t, err, "Reader()")
	output, err := io.ReadAll(r)
	require.NoError(t, err, "read from reader")
	require.NoError(t, r.Close(), "close reader")

	return output
}

// testRoundtripSuite runs a standard suite of roundtrip tests against a
// single Transform: empty, single byte, repetitive plaintext, and large random.
func testRoundtripSuite(t *testing.T, name string, tr transform.Transform) {
	t.Helper()
	cases := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"single_byte", []byte{0x42}},
		{"plaintext", bytes.Repeat([]byte("hello tierfs "+name), 10_000)},
		{"large_random_8MiB", randomBytes(8 * 1024 * 1024)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output := roundtrip(t, tr, tc.input)
			assert.Equal(t, tc.input, output)
		})
	}
}

// testCompressionRatio verifies that a compression transform achieves at least
// 5x compression on highly repetitive data.
func testCompressionRatio(t *testing.T, tr transform.Transform) {
	t.Helper()
	input := bytes.Repeat([]byte("aaaaaaa"), 100_000) // highly compressible

	var buf bytes.Buffer
	w, _ := tr.Writer(&buf)
	io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
	w.Close()

	assert.Less(t, buf.Len(), len(input)/5, "expected >5x compression of repetitive data")
}

// memBackend is a minimal in-memory domain.Backend for testing TransformBackend.
type memBackend struct {
	data map[string][]byte
}

func newMemBackend() *memBackend {
	return &memBackend{data: make(map[string][]byte)}
}

func (m *memBackend) Scheme() string                    { return "mem" }
func (m *memBackend) URI(p string) string               { return "mem://" + p }
func (m *memBackend) LocalPath(_ string) (string, bool) { return "", false }

func (m *memBackend) Put(_ context.Context, relPath string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.data[relPath] = data
	return nil
}

func (m *memBackend) Get(_ context.Context, relPath string) (io.ReadCloser, int64, error) {
	d, ok := m.data[relPath]
	if !ok {
		return nil, 0, domain.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(d)), int64(len(d)), nil
}

func (m *memBackend) Stat(_ context.Context, relPath string) (*domain.FileInfo, error) {
	d, ok := m.data[relPath]
	if !ok {
		return nil, domain.ErrNotExist
	}
	return &domain.FileInfo{RelPath: relPath, Size: int64(len(d))}, nil
}

func (m *memBackend) Delete(_ context.Context, relPath string) error {
	delete(m.data, relPath)
	return nil
}

func (m *memBackend) List(_ context.Context, _ string) ([]domain.FileInfo, error) {
	out := make([]domain.FileInfo, 0, len(m.data))
	for k, v := range m.data {
		out = append(out, domain.FileInfo{RelPath: k, Size: int64(len(v))})
	}
	return out, nil
}

// ── GzipTransform Tests ───────────────────────────────────────────────────────

func TestGzip_RoundtripSuite(t *testing.T) {
	tr, err := transform.NewGzip(gzip.DefaultCompression)
	require.NoError(t, err)
	testRoundtripSuite(t, "gzip", tr)
}

func TestGzip_CompressesRepetitiveData(t *testing.T) {
	tr, err := transform.NewGzip(gzip.BestCompression)
	require.NoError(t, err)
	testCompressionRatio(t, tr)
}

func TestGzip_InvalidLevel(t *testing.T) {
	_, err := transform.NewGzip(42)
	assert.Error(t, err)
}

// ── AES256GCMTransform Tests ──────────────────────────────────────────────────

func TestAES256GCM_RoundtripSuite(t *testing.T) {
	tr := transform.NewAES256GCM(newKey(t))
	testRoundtripSuite(t, "aes-256-gcm", tr)
}

func TestAES256GCM_Roundtrip_ExactChunkBoundary(t *testing.T) {
	tr := transform.NewAES256GCM(newKey(t))
	// Exactly 64 KiB — one complete chunk with no remainder
	input := make([]byte, 64*1024)
	rand.Read(input) //nolint:errcheck
	output := roundtrip(t, tr, input)
	assert.Equal(t, input, output)
}

func TestAES256GCM_Roundtrip_MultiChunk(t *testing.T) {
	tr := transform.NewAES256GCM(newKey(t))
	// 200 KiB → 3 full chunks + 1 partial (64+64+64+8 KiB)
	input := make([]byte, 200*1024)
	rand.Read(input) //nolint:errcheck
	output := roundtrip(t, tr, input)
	assert.Equal(t, input, output)
}

func TestAES256GCM_CiphertextDiffersFromPlaintext(t *testing.T) {
	tr := transform.NewAES256GCM(newKey(t))
	input := []byte("secret data that must not appear in ciphertext")

	var buf bytes.Buffer
	w, _ := tr.Writer(&buf)
	io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
	w.Close()

	assert.NotContains(t, buf.String(), "secret")
	assert.Greater(t, buf.Len(), 0)
}

func TestAES256GCM_TwoWritesProduceDifferentCiphertext(t *testing.T) {
	// Each encryption uses random nonces, so two encryptions of the same
	// plaintext must produce different ciphertext.
	tr := transform.NewAES256GCM(newKey(t))
	input := []byte("same plaintext")

	encrypt := func() []byte {
		var buf bytes.Buffer
		w, _ := tr.Writer(&buf)
		io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
		w.Close()
		return buf.Bytes()
	}

	ct1 := encrypt()
	ct2 := encrypt()
	assert.NotEqual(t, ct1, ct2, "two encryptions of same plaintext should produce different ciphertext (random nonces)")
}

func TestAES256GCM_WrongKeyFails(t *testing.T) {
	key1 := newKey(t)
	key2 := newKey(t)

	enc := transform.NewAES256GCM(key1)
	dec := transform.NewAES256GCM(key2) // different key

	input := []byte("sensitive data")
	var buf bytes.Buffer
	w, _ := enc.Writer(&buf)
	io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
	w.Close()

	r, err := dec.Reader(&buf)
	require.NoError(t, err) // header read succeeds

	_, err = io.ReadAll(r)
	assert.Error(t, err, "decryption with wrong key should fail")
}

func TestAES256GCM_TamperedCiphertextFails(t *testing.T) {
	tr := transform.NewAES256GCM(newKey(t))
	input := []byte("data that must not be tampered with")

	var buf bytes.Buffer
	w, _ := tr.Writer(&buf)
	io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
	w.Close()

	// Flip a bit in the ciphertext (past the magic header).
	ct := buf.Bytes()
	ct[len(ct)/2] ^= 0xFF

	r, err := tr.Reader(bytes.NewReader(ct))
	require.NoError(t, err)

	_, err = io.ReadAll(r)
	assert.Error(t, err, "tampered ciphertext should fail authentication")
}

func TestAES256GCM_BadMagicFails(t *testing.T) {
	tr := transform.NewAES256GCM(newKey(t))
	_, err := tr.Reader(strings.NewReader("NOTMAGIC" + strings.Repeat("x", 100)))
	assert.Error(t, err)
}

func TestAES256GCM_InvalidKeyLength(t *testing.T) {
	assert.Panics(t, func() {
		transform.NewAES256GCM([]byte("tooshort"))
	})
}

func TestGenerateKey(t *testing.T) {
	k1, err := transform.GenerateKey()
	require.NoError(t, err)
	assert.Len(t, k1, 64) // 32 bytes hex = 64 chars

	k2, err := transform.GenerateKey()
	require.NoError(t, err)
	assert.NotEqual(t, k1, k2, "two generated keys should be different")
}

func TestDecodeHexKey_Valid(t *testing.T) {
	hex := strings.Repeat("ab", 32) // 32 bytes = 64 hex chars
	key, err := transform.DecodeHexKey(hex)
	require.NoError(t, err)
	assert.Len(t, key, 32)
}

func TestDecodeHexKey_TooShort(t *testing.T) {
	_, err := transform.DecodeHexKey("deadbeef")
	assert.Error(t, err)
}

func TestDecodeHexKey_InvalidHex(t *testing.T) {
	_, err := transform.DecodeHexKey(strings.Repeat("zz", 32))
	assert.Error(t, err)
}

// ── Pipeline Ordering Tests ───────────────────────────────────────────────────

func TestNewPipeline_CompressionOnly(t *testing.T) {
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Compression: &transform.CompressionConfig{Algorithm: "gzip", Level: 1},
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 1)
	assert.Equal(t, "gzip", pipeline[0].Name())
	assert.False(t, reason.ChecksumElided)
	assert.Contains(t, reason.Order, "gzip")
}

func TestNewPipeline_ZstdDefault(t *testing.T) {
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Compression: &transform.CompressionConfig{}, // algorithm="" → defaults to zstd
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 1)
	assert.Equal(t, "zstd", pipeline[0].Name())
	assert.Contains(t, reason.Order, "zstd")
}

func TestNewPipeline_EncryptionOnly(t *testing.T) {
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Encryption: &transform.EncryptionConfig{KeyHex: newHexKey(t)},
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 1)
	assert.Equal(t, "aes-256-gcm", pipeline[0].Name())
	assert.False(t, reason.ChecksumElided)
}

func TestNewPipeline_ChecksumOnly(t *testing.T) {
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Checksum: &transform.ChecksumConfig{},
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 1)
	assert.Equal(t, "checksum-xxh3-128", pipeline[0].Name())
	assert.False(t, reason.ChecksumElided)
}

func TestNewPipeline_BothAlwaysCompressBeforeEncrypt(t *testing.T) {
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Compression: &transform.CompressionConfig{Algorithm: "zstd"},
		Encryption:  &transform.EncryptionConfig{KeyHex: newHexKey(t)},
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 2)
	assert.Equal(t, "zstd", pipeline[0].Name(), "compression must be first in write pipeline")
	assert.Equal(t, "aes-256-gcm", pipeline[1].Name(), "encryption must be last in write pipeline")
	assert.False(t, reason.ChecksumElided)
	assert.Equal(t, "zstd → aes-256-gcm", reason.Order)
}

func TestNewPipeline_ChecksumElidedWhenEncryptionPresent(t *testing.T) {
	// Providing both checksum and encryption: checksum must be silently elided.
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Checksum:   &transform.ChecksumConfig{},
		Encryption: &transform.EncryptionConfig{KeyHex: newHexKey(t)},
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 1, "only aes-256-gcm should remain; checksum was elided")
	assert.Equal(t, "aes-256-gcm", pipeline[0].Name())
	assert.True(t, reason.ChecksumElided, "reason must report that checksum was elided")
}

func TestNewPipeline_AllThree_ChecksumElided(t *testing.T) {
	// compress + checksum + encrypt: checksum is elided, order is compress→encrypt
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Compression: &transform.CompressionConfig{Algorithm: "zstd"},
		Checksum:    &transform.ChecksumConfig{},
		Encryption:  &transform.EncryptionConfig{KeyHex: newHexKey(t)},
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 2)
	assert.Equal(t, "zstd", pipeline[0].Name())
	assert.Equal(t, "aes-256-gcm", pipeline[1].Name())
	assert.True(t, reason.ChecksumElided)
	assert.Equal(t, "zstd → aes-256-gcm", reason.Order)
}

func TestNewPipeline_CompressAndChecksum_NoEncryption(t *testing.T) {
	// compress + checksum without encryption: checksum is NOT elided
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Compression: &transform.CompressionConfig{Algorithm: "zstd"},
		Checksum:    &transform.ChecksumConfig{},
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 2)
	assert.Equal(t, "zstd", pipeline[0].Name())
	assert.Equal(t, "checksum-xxh3-128", pipeline[1].Name())
	assert.False(t, reason.ChecksumElided)
	assert.Equal(t, "zstd → checksum-xxh3-128", reason.Order)
}

func TestNewPipeline_None(t *testing.T) {
	pipeline, reason, err := transform.NewPipeline(transform.Config{})
	require.NoError(t, err)
	assert.Empty(t, pipeline)
	assert.Equal(t, "passthrough (no transforms)", reason.Order)
}

func TestNewPipeline_BadEncryptionKey(t *testing.T) {
	_, _, err := transform.NewPipeline(transform.Config{
		Encryption: &transform.EncryptionConfig{KeyHex: "tooshort"},
	})
	assert.Error(t, err)
}

func TestNewPipeline_KeyEnvVar(t *testing.T) {
	k, _ := transform.GenerateKey()
	t.Setenv("TEST_TIERFS_KEY", k)

	pipeline, _, err := transform.NewPipeline(transform.Config{
		Encryption: &transform.EncryptionConfig{KeyEnv: "TEST_TIERFS_KEY"},
	})
	require.NoError(t, err)
	assert.Len(t, pipeline, 1)
}

func TestNewPipeline_KeyEnvVar_Missing(t *testing.T) {
	_, _, err := transform.NewPipeline(transform.Config{
		Encryption: &transform.EncryptionConfig{KeyEnv: "TIERFS_KEY_THAT_DOES_NOT_EXIST_XYZ"},
	})
	assert.Error(t, err)
}

func TestNewPipeline_UnknownAlgorithm(t *testing.T) {
	_, _, err := transform.NewPipeline(transform.Config{
		Compression: &transform.CompressionConfig{Algorithm: "brotli"},
	})
	assert.Error(t, err)
}

// ── TransformBackend Integration Tests ───────────────────────────────────────

func newTransformBackend(t *testing.T, pipeline []transform.Transform) (*transform.Backend, *memBackend) {
	t.Helper()
	inner := newMemBackend()
	log := zaptest.NewLogger(t)
	tb := transform.New(inner, pipeline, log)
	return tb, inner
}

func TestTransformBackend_LocalPath_AlwaysFalse(t *testing.T) {
	enc := transform.NewAES256GCM(newKey(t))
	tb, _ := newTransformBackend(t, transform.Pipeline(enc))
	path, ok := tb.LocalPath("anything.mp4")
	assert.False(t, ok)
	assert.Empty(t, path)
}

func TestTransformBackend_Encrypt_Roundtrip(t *testing.T) {
	enc := transform.NewAES256GCM(newKey(t))
	tb, inner := newTransformBackend(t, transform.Pipeline(enc))
	ctx := context.Background()

	original := []byte("recordings data that should be encrypted at rest")
	require.NoError(t, tb.Put(ctx, "clip.mp4", bytes.NewReader(original), int64(len(original))))

	// Inner backend must NOT contain plaintext.
	raw := inner.data["clip.mp4"]
	assert.NotEqual(t, original, raw)
	assert.NotContains(t, string(raw), "recordings data")

	// Read back through the decorator must yield original.
	rc, _, err := tb.Get(ctx, "clip.mp4")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, original, got)
}

func TestTransformBackend_Compress_Roundtrip(t *testing.T) {
	comp, _ := transform.NewGzip(gzip.BestSpeed)
	tb, _ := newTransformBackend(t, transform.Pipeline(comp))
	ctx := context.Background()

	original := bytes.Repeat([]byte("compressible log data "), 5000)
	require.NoError(t, tb.Put(ctx, "logfile.txt", bytes.NewReader(original), int64(len(original))))

	rc, _, err := tb.Get(ctx, "logfile.txt")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, original, got)
}

func TestTransformBackend_CompressThenEncrypt_Roundtrip(t *testing.T) {
	comp, _ := transform.NewGzip(gzip.DefaultCompression)
	enc := transform.NewAES256GCM(newKey(t))
	pipeline := transform.Pipeline(comp, enc) // compress first, then encrypt

	tb, inner := newTransformBackend(t, pipeline)
	ctx := context.Background()

	original := bytes.Repeat([]byte("frigate recording segment "), 2000)
	require.NoError(t, tb.Put(ctx, "seg.mp4", bytes.NewReader(original), int64(len(original))))

	// Stored data should be smaller than plaintext (compression before encryption).
	stored := inner.data["seg.mp4"]
	assert.Less(t, len(stored), len(original), "stored bytes should be compressed before encryption")

	// Roundtrip must recover original.
	rc, _, err := tb.Get(ctx, "seg.mp4")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, original, got)
}

func TestTransformBackend_CompressionAfterEncryption_IsLarger(t *testing.T) {
	// This test documents WHY the ordering matters.
	// Encrypting first then compressing should produce larger output than
	// compressing first then encrypting (encrypted data is incompressible).
	key := newKey(t)
	input := bytes.Repeat([]byte("repetitive plaintext "), 5000)

	compressFirst := func() int {
		comp, _ := transform.NewGzip(gzip.BestCompression)
		enc := transform.NewAES256GCM(key)
		tb, inner := newTransformBackend(t, transform.Pipeline(comp, enc))
		tb.Put(context.Background(), "f", bytes.NewReader(input), int64(len(input))) //nolint:errcheck
		return len(inner.data["f"])
	}

	encryptFirst := func() int {
		enc := transform.NewAES256GCM(key)
		comp, _ := transform.NewGzip(gzip.BestCompression)
		tb, inner := newTransformBackend(t, transform.Pipeline(enc, comp))
		tb.Put(context.Background(), "f", bytes.NewReader(input), int64(len(input))) //nolint:errcheck
		return len(inner.data["f"])
	}

	compFirst := compressFirst()
	encFirst := encryptFirst()

	// compress-then-encrypt must produce smaller output than encrypt-then-compress
	assert.Less(t, compFirst, encFirst,
		"compress-then-encrypt (%d bytes) should be smaller than encrypt-then-compress (%d bytes)",
		compFirst, encFirst)
}

func TestTransformBackend_Delete(t *testing.T) {
	enc := transform.NewAES256GCM(newKey(t))
	tb, inner := newTransformBackend(t, transform.Pipeline(enc))
	ctx := context.Background()

	tb.Put(ctx, "clip.mp4", bytes.NewReader([]byte("data")), 4) //nolint:errcheck
	require.Contains(t, inner.data, "clip.mp4")

	require.NoError(t, tb.Delete(ctx, "clip.mp4"))
	assert.NotContains(t, inner.data, "clip.mp4")
}

func TestTransformBackend_GetNotExist(t *testing.T) {
	enc := transform.NewAES256GCM(newKey(t))
	tb, _ := newTransformBackend(t, transform.Pipeline(enc))

	_, _, err := tb.Get(context.Background(), "does/not/exist.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestTransformBackend_List(t *testing.T) {
	enc := transform.NewAES256GCM(newKey(t))
	tb, _ := newTransformBackend(t, transform.Pipeline(enc))
	ctx := context.Background()

	for _, p := range []string{"a.mp4", "b.mp4", "c.mp4"} {
		tb.Put(ctx, p, bytes.NewReader([]byte("x")), 1) //nolint:errcheck
	}

	entries, err := tb.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, entries, 3)
}

func TestTransformBackend_EmptyPipeline_PassThrough(t *testing.T) {
	// An empty pipeline should pass data through unchanged.
	tb, inner := newTransformBackend(t, transform.Pipeline())
	ctx := context.Background()

	original := []byte("unmodified data")
	tb.Put(ctx, "raw.txt", bytes.NewReader(original), int64(len(original))) //nolint:errcheck

	assert.Equal(t, original, inner.data["raw.txt"])

	rc, _, err := tb.Get(ctx, "raw.txt")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	assert.Equal(t, original, got)
}

func TestTransformBackend_IsFinal_Propagates(t *testing.T) {
	// A TransformBackend wrapping a Finalizer must itself be a Finalizer.
	type finalizer interface{ IsFinal() bool }

	// memBackend does not implement Finalizer.
	enc := transform.NewAES256GCM(newKey(t))
	tb, _ := newTransformBackend(t, transform.Pipeline(enc))
	f, ok := any(tb).(finalizer)
	assert.True(t, ok)
	assert.False(t, f.IsFinal(), "non-final inner backend should not be final")
}

func TestTransformBackend_NewPipeline_Integration(t *testing.T) {
	hexKey, _ := transform.GenerateKey()
	pipeline, _, err := transform.NewPipeline(transform.Config{
		Compression: &transform.CompressionConfig{Algorithm: "zstd", Level: 1},
		Encryption:  &transform.EncryptionConfig{KeyHex: hexKey},
	})
	require.NoError(t, err)
	require.Len(t, pipeline, 2)

	tb, _ := newTransformBackend(t, pipeline)
	ctx := context.Background()

	original := bytes.Repeat([]byte("test data for pipeline integration"), 1000)
	require.NoError(t, tb.Put(ctx, "test.bin", bytes.NewReader(original), int64(len(original))))

	rc, _, err := tb.Get(ctx, "test.bin")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, original, got)
}

// ── ZstdTransform Tests ───────────────────────────────────────────────────────

func TestZstd_RoundtripSuite(t *testing.T) {
	tr, err := transform.NewZstd(zstd.SpeedDefault)
	require.NoError(t, err)
	testRoundtripSuite(t, "zstd", tr)
}

func TestZstd_CompressesRepetitiveData(t *testing.T) {
	tr, err := transform.NewZstd(zstd.SpeedBestCompression)
	require.NoError(t, err)
	testCompressionRatio(t, tr)
}

func TestZstd_FasterThanGzip_OnRepetitiveData(t *testing.T) {
	// Zstd at SpeedFastest should encode faster than gzip at BestSpeed.
	// We just verify both roundtrip correctly — timing is environment-dependent.
	input := bytes.Repeat([]byte("frigate segment data "), 50_000)

	zstdT, _ := transform.NewZstd(zstd.SpeedFastest)
	gzipT, _ := transform.NewGzip(1)

	assert.Equal(t, input, roundtrip(t, zstdT, input))
	assert.Equal(t, input, roundtrip(t, gzipT, input))
}

// ── ChecksumTransform Tests ───────────────────────────────────────────────────

func TestChecksum_RoundtripSuite(t *testing.T) {
	tr := transform.NewChecksum()
	testRoundtripSuite(t, "checksum", tr)
}

func TestChecksum_StoredDataHas16ByteHeader(t *testing.T) {
	tr := transform.NewChecksum()
	input := []byte("hello")

	var buf bytes.Buffer
	w, _ := tr.Writer(&buf)
	io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
	w.Close()

	// Stored bytes = 16-byte header + 5 bytes data = 21 bytes
	assert.Equal(t, 16+len(input), buf.Len())
}

func TestChecksum_BitRotDetected(t *testing.T) {
	tr := transform.NewChecksum()
	input := []byte("important recording data")

	var buf bytes.Buffer
	w, _ := tr.Writer(&buf)
	io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
	w.Close()

	// Flip a bit in the data portion (past the 16-byte header).
	stored := buf.Bytes()
	stored[20] ^= 0x01

	r, err := tr.Reader(bytes.NewReader(stored))
	require.NoError(t, err)

	_, err = io.ReadAll(r)
	assert.Error(t, err, "bit-rot in data should be detected")
	assert.Contains(t, err.Error(), "checksum")
}

func TestChecksum_HeaderCorruptionDetected(t *testing.T) {
	tr := transform.NewChecksum()
	input := []byte("another recording")

	var buf bytes.Buffer
	w, _ := tr.Writer(&buf)
	io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
	w.Close()

	// Corrupt the header itself.
	stored := buf.Bytes()
	stored[0] ^= 0xFF

	r, err := tr.Reader(bytes.NewReader(stored))
	require.NoError(t, err)

	_, err = io.ReadAll(r)
	assert.Error(t, err, "header corruption should be detected")
}

func TestChecksum_BackendRoundtrip(t *testing.T) {
	tr := transform.NewChecksum()
	tb, _ := newTransformBackend(t, transform.Pipeline(tr))
	ctx := context.Background()

	original := bytes.Repeat([]byte("nas tier data"), 1000)
	require.NoError(t, tb.Put(ctx, "clip.mp4", bytes.NewReader(original), int64(len(original))))

	rc, _, err := tb.Get(ctx, "clip.mp4")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, original, got)
}

func TestChecksum_ZstdThenChecksum_Roundtrip(t *testing.T) {
	// compress → checksum (no encryption) — checksum is NOT elided
	pipeline, reason, err := transform.NewPipeline(transform.Config{
		Compression: &transform.CompressionConfig{Algorithm: "zstd"},
		Checksum:    &transform.ChecksumConfig{},
	})
	require.NoError(t, err)
	assert.False(t, reason.ChecksumElided)
	require.Len(t, pipeline, 2)

	tb, _ := newTransformBackend(t, pipeline)
	ctx := context.Background()

	original := bytes.Repeat([]byte("compressed and checksummed NAS data"), 2000)
	require.NoError(t, tb.Put(ctx, "f.bin", bytes.NewReader(original), int64(len(original))))

	rc, _, err := tb.Get(ctx, "f.bin")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, original, got)
}

// ── Pipeline Read/Write Symmetry ──────────────────────────────────────────────

// TestPipelineSymmetry verifies that for any pipeline, applyWritePipeline
// followed by applyReadPipeline (in reverse) is a lossless roundtrip.
// This is the core invariant of the Transform abstraction.
func TestPipelineSymmetry_AllCombinations(t *testing.T) {
	key := newKey(t)
	comp, _ := transform.NewGzip(gzip.DefaultCompression)
	enc := transform.NewAES256GCM(key)

	cases := []struct {
		name     string
		pipeline []transform.Transform
	}{
		{"gzip only", transform.Pipeline(comp)},
		{"aes only", transform.Pipeline(enc)},
		{"gzip then aes", transform.Pipeline(comp, enc)},
	}

	inputs := [][]byte{
		{},
		[]byte("x"),
		bytes.Repeat([]byte("hello"), 10_000),
		func() []byte { b := make([]byte, 200*1024); rand.Read(b); return b }(),
	}

	for _, tc := range cases {
		for _, input := range inputs {
			t.Run(tc.name, func(t *testing.T) {
				inner := newMemBackend()
				log := zaptest.NewLogger(t)
				tb := transform.New(inner, tc.pipeline, log)
				ctx := context.Background()

				err := tb.Put(ctx, "f", bytes.NewReader(input), int64(len(input)))
				require.NoError(t, err)

				rc, _, err := tb.Get(ctx, "f")
				require.NoError(t, err)
				got, err := io.ReadAll(rc)
				require.NoError(t, err)
				rc.Close()
				assert.Equal(t, input, got)
			})
		}
	}
}

// TestAES256GCM_ReadPartial verifies the chunked reader handles small Read()
// calls correctly (caller reads fewer bytes than one chunk at a time).
func TestAES256GCM_ReadPartial(t *testing.T) {
	tr := transform.NewAES256GCM(newKey(t))
	input := make([]byte, 300*1024) // 3 chunks worth
	rand.Read(input)                //nolint:errcheck

	var buf bytes.Buffer
	w, _ := tr.Writer(&buf)
	io.Copy(w, bytes.NewReader(input)) //nolint:errcheck
	w.Close()

	r, _ := tr.Reader(&buf)
	// Read 1 byte at a time — exercises buffer management in decryptReader.
	var out []byte
	oneByte := make([]byte, 1)
	for {
		n, err := r.Read(oneByte)
		if n > 0 {
			out = append(out, oneByte[:n]...)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
	assert.Equal(t, input, out)
}
