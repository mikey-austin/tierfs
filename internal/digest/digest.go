// Package digest provides xxhash3-128 based file fingerprinting.
// xxhash3 is chosen for its exceptional throughput on modern CPUs (>30 GB/s
// with SIMD), making digest verification negligible compared to I/O time.
package digest

import (
	"fmt"
	"io"
	"os"

	"github.com/zeebo/xxh3"
)

// Compute hashes the reader using xxhash3-128 and returns a hex string.
func Compute(r io.Reader) (string, error) {
	h := xxh3.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("digest: copy: %w", err)
	}
	sum := h.Sum128()
	return fmt.Sprintf("%016x%016x", sum.Hi, sum.Lo), nil
}

// ComputeFile opens path and hashes it. Uses a 4 MiB read buffer to match
// typical NVMe/NFS read granularity.
func ComputeFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("digest file: open %q: %w", path, err)
	}
	defer f.Close()

	h := xxh3.New()
	buf := make([]byte, 4*1024*1024)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", fmt.Errorf("digest file: hash %q: %w", path, err)
	}
	sum := h.Sum128()
	return fmt.Sprintf("%016x%016x", sum.Hi, sum.Lo), nil
}

// Verify returns nil if the digest of path matches expected.
func Verify(path, expected string) error {
	got, err := ComputeFile(path)
	if err != nil {
		return err
	}
	if got != expected {
		return fmt.Errorf("digest mismatch for %q: want %s got %s", path, expected, got)
	}
	return nil
}
