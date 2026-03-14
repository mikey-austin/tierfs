package null_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/mikey-austin/tierfs/internal/adapters/storage/null"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_Scheme(t *testing.T) {
	b := null.New()
	assert.Equal(t, "null", b.Scheme())
}

func TestBackend_URI(t *testing.T) {
	b := null.New()
	assert.Equal(t, "null://recordings/cam1/clip.mp4", b.URI("recordings/cam1/clip.mp4"))
}

func TestBackend_LocalPath(t *testing.T) {
	b := null.New()
	path, ok := b.LocalPath("anything")
	assert.False(t, ok)
	assert.Empty(t, path)
}

func TestBackend_IsFinal(t *testing.T) {
	b := null.New()
	// Verify it satisfies Finalizer and returns true.
	type finalizer interface{ IsFinal() bool }
	f, ok := any(b).(finalizer)
	require.True(t, ok, "null.Backend should implement domain.Finalizer")
	assert.True(t, f.IsFinal())
}

func TestBackend_Put_DrainsReader(t *testing.T) {
	b := null.New()
	data := bytes.Repeat([]byte("tierfs"), 10_000)
	r := bytes.NewReader(data)

	err := b.Put(context.Background(), "any/path.mp4", r, int64(len(data)))
	require.NoError(t, err)

	// Reader should be fully consumed.
	remaining, _ := io.ReadAll(r)
	assert.Empty(t, remaining)
}

func TestBackend_Put_LargeFile(t *testing.T) {
	b := null.New()
	// 32 MiB — exercises chunked io.Copy behaviour.
	size := 32 * 1024 * 1024
	r := io.LimitReader(strings.NewReader(strings.Repeat("x", size)), int64(size))
	err := b.Put(context.Background(), "recordings/large.mp4", r, int64(size))
	assert.NoError(t, err)
}

func TestBackend_Get_ReturnsNotExist(t *testing.T) {
	b := null.New()
	rc, size, err := b.Get(context.Background(), "any/path.mp4")
	assert.Nil(t, rc)
	assert.Zero(t, size)
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestBackend_Stat_ReturnsNotExist(t *testing.T) {
	b := null.New()
	fi, err := b.Stat(context.Background(), "any/path.mp4")
	assert.Nil(t, fi)
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestBackend_Delete_IsNoop(t *testing.T) {
	b := null.New()
	// Should not error regardless of whether the file "exists".
	assert.NoError(t, b.Delete(context.Background(), "any/path.mp4"))
	assert.NoError(t, b.Delete(context.Background(), "does/not/exist.mp4"))
}

func TestBackend_List_ReturnsEmpty(t *testing.T) {
	b := null.New()
	entries, err := b.List(context.Background(), "")
	assert.NoError(t, err)
	assert.Empty(t, entries)
}

func TestBackend_Put_TeeReaderCompatible(t *testing.T) {
	// Simulates the Replicator's io.TeeReader path: Put drains the tee'd reader,
	// which also writes into hashBuf. Verifies the buffer is fully populated
	// after Put returns.
	b := null.New()
	data := []byte("hello null backend")
	hashBuf := &bytes.Buffer{}
	tee := io.TeeReader(bytes.NewReader(data), hashBuf)

	err := b.Put(context.Background(), "clip.mp4", tee, int64(len(data)))
	require.NoError(t, err)

	// hashBuf should have received all bytes via the tee.
	assert.Equal(t, data, hashBuf.Bytes())
}

func TestBackend_ConcurrentPut(t *testing.T) {
	b := null.New()
	data := bytes.Repeat([]byte("concurrent"), 1000)

	done := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			err := b.Put(context.Background(),
				"recordings/"+string(rune('a'+i))+".mp4",
				bytes.NewReader(data),
				int64(len(data)),
			)
			done <- err
		}(i)
	}
	for i := 0; i < 20; i++ {
		assert.NoError(t, <-done)
	}
}
