package file_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mikey-austin/tierfs/internal/adapters/storage/file"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBackend(t *testing.T) *file.Backend {
	t.Helper()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	return b
}

func TestPutAndGet(t *testing.T) {
	b := newBackend(t)
	ctx := context.Background()
	data := []byte("hello tierfs")

	require.NoError(t, b.Put(ctx, "a/b/c.mp4", bytes.NewReader(data), int64(len(data))))

	rc, size, err := b.Get(ctx, "a/b/c.mp4")
	require.NoError(t, err)
	defer rc.Close()
	assert.Equal(t, int64(len(data)), size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestPut_Atomic(t *testing.T) {
	// Verify no temp files are left behind after a successful put.
	b := newBackend(t)
	ctx := context.Background()
	require.NoError(t, b.Put(ctx, "x.mp4", bytes.NewReader([]byte("data")), 4))

	entries, _ := os.ReadDir(b.Root())
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tierfs-put-")
	}
}

func TestGet_NotExist(t *testing.T) {
	b := newBackend(t)
	_, _, err := b.Get(context.Background(), "missing.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestStat(t *testing.T) {
	b := newBackend(t)
	ctx := context.Background()
	data := []byte("stat me")
	require.NoError(t, b.Put(ctx, "stat/test.mp4", bytes.NewReader(data), int64(len(data))))

	fi, err := b.Stat(ctx, "stat/test.mp4")
	require.NoError(t, err)
	assert.Equal(t, "stat/test.mp4", fi.RelPath)
	assert.Equal(t, int64(len(data)), fi.Size)
	assert.False(t, fi.IsDir)
}

func TestStat_NotExist(t *testing.T) {
	b := newBackend(t)
	_, err := b.Stat(context.Background(), "nope.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestDelete(t *testing.T) {
	b := newBackend(t)
	ctx := context.Background()
	require.NoError(t, b.Put(ctx, "del/me.mp4", bytes.NewReader([]byte("x")), 1))
	require.NoError(t, b.Delete(ctx, "del/me.mp4"))

	_, _, err := b.Get(ctx, "del/me.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)

	// Parent directory should have been pruned.
	_, err = os.Stat(filepath.Join(b.Root(), "del"))
	assert.True(t, os.IsNotExist(err), "empty dir should be pruned")
}

func TestDelete_NotExist(t *testing.T) {
	b := newBackend(t)
	err := b.Delete(context.Background(), "missing.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestList(t *testing.T) {
	b := newBackend(t)
	ctx := context.Background()

	files := []string{
		"recordings/cam1/2026-03/13/10/30.mp4",
		"recordings/cam1/2026-03/13/10/31.mp4",
		"thumbnails/cam1/thumb.jpg",
	}
	for _, f := range files {
		require.NoError(t, b.Put(ctx, f, bytes.NewReader([]byte("x")), 1))
	}

	all, err := b.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, all, 3)

	rec, err := b.List(ctx, "recordings")
	require.NoError(t, err)
	assert.Len(t, rec, 2)

	thumbs, err := b.List(ctx, "thumbnails")
	require.NoError(t, err)
	assert.Len(t, thumbs, 1)
}

func TestLocalPath(t *testing.T) {
	b := newBackend(t)
	p, ok := b.LocalPath("recordings/foo.mp4")
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(b.Root(), "recordings", "foo.mp4"), p)
}

func TestSchemeAndURI(t *testing.T) {
	b := newBackend(t)
	assert.Equal(t, "file", b.Scheme())
	uri := b.URI("foo/bar.mp4")
	assert.Contains(t, uri, "file://")
	assert.Contains(t, uri, "foo/bar.mp4")
}

func TestRename(t *testing.T) {
	b := newBackend(t)
	ctx := context.Background()
	data := []byte("rename me")
	require.NoError(t, b.Put(ctx, "old/path.mp4", bytes.NewReader(data), int64(len(data))))
	require.NoError(t, b.Rename(ctx, "old/path.mp4", "new/path.mp4"))

	_, _, err := b.Get(ctx, "old/path.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)

	rc, _, err := b.Get(ctx, "new/path.mp4")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, data, got)
}
