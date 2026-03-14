package digest_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/mikey-austin/tierfs/internal/digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompute_Deterministic(t *testing.T) {
	data := []byte("hello tierfs storage tiering")
	d1, err := digest.Compute(bytes.NewReader(data))
	require.NoError(t, err)
	d2, err := digest.Compute(bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, d1, d2)
	assert.Len(t, d1, 32) // 128-bit hex = 32 chars
}

func TestCompute_DifferentData(t *testing.T) {
	d1, _ := digest.Compute(bytes.NewReader([]byte("aaa")))
	d2, _ := digest.Compute(bytes.NewReader([]byte("bbb")))
	assert.NotEqual(t, d1, d2)
}

func TestComputeFile(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 8*1024*1024) // 8 MiB
	f, err := os.CreateTemp(t.TempDir(), "digest-*.bin")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	f.Close()

	fromFile, err := digest.ComputeFile(f.Name())
	require.NoError(t, err)

	fromReader, err := digest.Compute(bytes.NewReader(data))
	require.NoError(t, err)

	assert.Equal(t, fromReader, fromFile)
}

func TestVerify_Match(t *testing.T) {
	data := []byte("verify me")
	p := filepath.Join(t.TempDir(), "v.bin")
	require.NoError(t, os.WriteFile(p, data, 0o644))

	d, err := digest.Compute(bytes.NewReader(data))
	require.NoError(t, err)
	assert.NoError(t, digest.Verify(p, d))
}

func TestHasher_MatchesCompute(t *testing.T) {
	data := []byte("hello tierfs storage tiering")
	expected, err := digest.Compute(bytes.NewReader(data))
	require.NoError(t, err)

	h := digest.NewHasher()
	_, err = h.Write(data)
	require.NoError(t, err)
	got := h.Sum()
	assert.Equal(t, expected, got)
}

func TestHasher_IncrementalWrites(t *testing.T) {
	data := bytes.Repeat([]byte("chunk"), 1000)
	expected, err := digest.Compute(bytes.NewReader(data))
	require.NoError(t, err)

	h := digest.NewHasher()
	for i := 0; i < len(data); i += 7 {
		end := i + 7
		if end > len(data) {
			end = len(data)
		}
		h.Write(data[i:end])
	}
	assert.Equal(t, expected, h.Sum())
}

func TestVerify_Mismatch(t *testing.T) {
	p := filepath.Join(t.TempDir(), "v.bin")
	require.NoError(t, os.WriteFile(p, []byte("real data"), 0o644))
	err := digest.Verify(p, "000000000000000000000000000000ff")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mismatch")
}
