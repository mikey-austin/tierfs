package transform

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

const (
	// chunkPlainSize is the plaintext chunk size for streaming encryption.
	// Each chunk is encrypted independently with its own nonce.
	// 64 KiB balances memory usage against per-chunk overhead.
	chunkPlainSize = 64 * 1024

	// chunkCipherSize is the ciphertext size per chunk: plaintext + GCM tag.
	chunkCipherSize = chunkPlainSize + gcmTagSize

	gcmNonceSize = 12
	gcmTagSize   = 16
	aes256KeyLen = 32

	// streamMagic identifies the encrypted stream format. Change this if the
	// format ever changes incompatibly so old readers fail fast rather than
	// producing garbage.
	streamMagic = "TFSAEAD1"
)

var (
	streamMagicBytes = []byte(streamMagic)
	errBadMagic      = errors.New("aes-256-gcm: not a tierfs encrypted stream (bad magic)")
	errAuthFailed    = errors.New("aes-256-gcm: authentication failed — data is corrupt or key is wrong")
	errChunkTooLarge = errors.New("aes-256-gcm: chunk size exceeds maximum")
)

// AES256GCMTransform implements [Transform] using AES-256-GCM authenticated
// encryption in a chunked streaming format. Each 64 KiB plaintext chunk is
// independently encrypted with its own random 12-byte nonce and a 16-byte
// GCM authentication tag, providing both confidentiality and integrity.
//
// Stream format (written by encryptWriter, read by decryptReader):
//
//	[8 bytes]  magic = "TFSAEAD1"
//	[repeating chunks]
//	  [12 bytes] nonce (random, per-chunk)
//	  [4 bytes]  uint32 LE: ciphertext length (plaintext + 16-byte tag)
//	  [N bytes]  ciphertext (AES-256-GCM encrypted plaintext + appended tag)
//	[terminal chunk]
//	  [12 bytes] nonce = all zeros
//	  [4 bytes]  uint32 LE = 0
//
// Each chunk's AAD (additional authenticated data) is its 8-byte little-endian
// chunk index, preventing chunk reordering attacks even if an adversary can
// rearrange ciphertext blocks.
//
// The key must be exactly 32 bytes (AES-256). Generate one with:
//
//	openssl rand -hex 32
type AES256GCMTransform struct {
	key []byte
}

// NewAES256GCM returns an AES256GCMTransform using the given 32-byte key.
// Panics if key is not exactly 32 bytes — call [DecodeHexKey] to parse user input.
func NewAES256GCM(key []byte) *AES256GCMTransform {
	if len(key) != aes256KeyLen {
		panic(fmt.Sprintf("aes-256-gcm: key must be %d bytes, got %d", aes256KeyLen, len(key)))
	}
	k := make([]byte, aes256KeyLen)
	copy(k, key)
	return &AES256GCMTransform{key: k}
}

// Name returns "aes-256-gcm".
func (t *AES256GCMTransform) Name() string { return "aes-256-gcm" }

// Writer returns a WriteCloser that encrypts data written to it and writes the
// ciphertext to dst. Close MUST be called to write the terminal chunk sentinel.
func (t *AES256GCMTransform) Writer(dst io.Writer) (io.WriteCloser, error) {
	block, err := aes.NewCipher(t.key)
	if err != nil {
		return nil, fmt.Errorf("aes-256-gcm writer: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aes-256-gcm writer: %w", err)
	}

	// Write magic header.
	if _, err := dst.Write(streamMagicBytes); err != nil {
		return nil, fmt.Errorf("aes-256-gcm writer: write magic: %w", err)
	}

	return &encryptWriter{
		dst: dst,
		gcm: gcm,
		buf: make([]byte, 0, chunkPlainSize),
	}, nil
}

// Reader returns a ReadCloser that decrypts and authenticates data read from src.
// Returns errAuthFailed if any chunk fails GCM authentication.
func (t *AES256GCMTransform) Reader(src io.Reader) (io.ReadCloser, error) {
	block, err := aes.NewCipher(t.key)
	if err != nil {
		return nil, fmt.Errorf("aes-256-gcm reader: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aes-256-gcm reader: %w", err)
	}

	// Read and verify magic header.
	magic := make([]byte, len(streamMagicBytes))
	if _, err := io.ReadFull(src, magic); err != nil {
		return nil, fmt.Errorf("aes-256-gcm reader: read magic: %w", err)
	}
	if string(magic) != streamMagic {
		return nil, errBadMagic
	}

	return &decryptReader{
		src: src,
		gcm: gcm,
	}, nil
}

// ── encryptWriter ─────────────────────────────────────────────────────────────

type encryptWriter struct {
	dst        io.Writer
	gcm        cipher.AEAD
	buf        []byte // plaintext accumulation buffer
	chunkIndex uint64
	closed     bool
}

func (w *encryptWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("write to closed encryptWriter")
	}
	total := 0
	for len(p) > 0 {
		// How much space remains in the current chunk buffer?
		space := chunkPlainSize - len(w.buf)
		n := len(p)
		if n > space {
			n = space
		}
		w.buf = append(w.buf, p[:n]...)
		p = p[n:]
		total += n

		if len(w.buf) == chunkPlainSize {
			if err := w.flushChunk(); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

func (w *encryptWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Flush any remaining buffered plaintext (may be a partial chunk).
	if len(w.buf) > 0 {
		if err := w.flushChunk(); err != nil {
			return err
		}
	}

	// Write terminal sentinel: 12-byte zero nonce + uint32(0) length.
	sentinel := make([]byte, gcmNonceSize+4)
	_, err := w.dst.Write(sentinel)
	return err
}

func (w *encryptWriter) flushChunk() error {
	plaintext := w.buf

	// Generate a random 96-bit nonce.
	nonce := make([]byte, gcmNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("aes-256-gcm: generate nonce: %w", err)
	}

	// AAD = chunk index as little-endian uint64 (prevents reordering attacks).
	aad := make([]byte, 8)
	binary.LittleEndian.PutUint64(aad, w.chunkIndex)

	// Encrypt: GCM appends the 16-byte tag to the ciphertext.
	ciphertext := w.gcm.Seal(nil, nonce, plaintext, aad)

	// Write: nonce | uint32(len(ciphertext)) | ciphertext
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(ciphertext)))

	if _, err := w.dst.Write(nonce); err != nil {
		return fmt.Errorf("aes-256-gcm: write nonce: %w", err)
	}
	if _, err := w.dst.Write(lenBuf); err != nil {
		return fmt.Errorf("aes-256-gcm: write chunk length: %w", err)
	}
	if _, err := w.dst.Write(ciphertext); err != nil {
		return fmt.Errorf("aes-256-gcm: write ciphertext: %w", err)
	}

	w.chunkIndex++
	w.buf = w.buf[:0]
	return nil
}

// ── decryptReader ─────────────────────────────────────────────────────────────

type decryptReader struct {
	src        io.Reader
	gcm        cipher.AEAD
	plainBuf   []byte // decrypted plaintext ready to serve
	chunkIndex uint64
	done       bool // terminal chunk seen
}

func (r *decryptReader) Read(p []byte) (int, error) {
	if len(r.plainBuf) > 0 {
		n := copy(p, r.plainBuf)
		r.plainBuf = r.plainBuf[n:]
		return n, nil
	}
	if r.done {
		return 0, io.EOF
	}

	// Read next chunk header: nonce (12 bytes) + length (4 bytes).
	header := make([]byte, gcmNonceSize+4)
	if _, err := io.ReadFull(r.src, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, fmt.Errorf("aes-256-gcm: unexpected end of stream reading chunk header: %w", err)
		}
		return 0, err
	}

	nonce := header[:gcmNonceSize]
	cipherLen := binary.LittleEndian.Uint32(header[gcmNonceSize:])

	// Check for terminal sentinel: all-zero nonce + zero length.
	if cipherLen == 0 && isZero(nonce) {
		r.done = true
		return 0, io.EOF
	}

	// Sanity check: ciphertext cannot be larger than one chunk + tag overhead.
	if cipherLen > chunkCipherSize {
		return 0, errChunkTooLarge
	}

	// Read ciphertext.
	ciphertext := make([]byte, cipherLen)
	if _, err := io.ReadFull(r.src, ciphertext); err != nil {
		return 0, fmt.Errorf("aes-256-gcm: read chunk ciphertext: %w", err)
	}

	// AAD must match what was used during encryption.
	aad := make([]byte, 8)
	binary.LittleEndian.PutUint64(aad, r.chunkIndex)

	plaintext, err := r.gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return 0, fmt.Errorf("%w: chunk %d", errAuthFailed, r.chunkIndex)
	}

	r.chunkIndex++
	r.plainBuf = plaintext

	n := copy(p, r.plainBuf)
	r.plainBuf = r.plainBuf[n:]
	return n, nil
}

func (r *decryptReader) Close() error { return nil }

// ── Key helpers ───────────────────────────────────────────────────────────────

// DecodeHexKey parses a 64-character hex string into a 32-byte AES-256 key.
// This is the public entry point for user-supplied key strings.
func DecodeHexKey(hexStr string) ([]byte, error) {
	return decodeHexKey(hexStr)
}

func decodeHexKey(hexStr string) ([]byte, error) {
	if len(hexStr) != aes256KeyLen*2 {
		return nil, fmt.Errorf("aes-256-gcm: key must be %d hex chars (32 bytes), got %d",
			aes256KeyLen*2, len(hexStr))
	}
	key, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("aes-256-gcm: invalid hex key: %w", err)
	}
	return key, nil
}

// GenerateKey generates a cryptographically secure random 32-byte key and
// returns it as a lowercase hex string. Use this to create new keys:
//
//	key, _ := transform.GenerateKey()
//	fmt.Println(key) // store in Ansible Vault or secret manager
func GenerateKey() (string, error) {
	key := make([]byte, aes256KeyLen)
	if _, err := rand.Read(key); err != nil {
		return "", fmt.Errorf("aes-256-gcm: generate key: %w", err)
	}
	return hex.EncodeToString(key), nil
}

// isZero reports whether all bytes in b are zero.
func isZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
