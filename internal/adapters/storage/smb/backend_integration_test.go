//go:build integration

// Run with:
//   TIERFS_SMB_TEST_URI=smb://admin:pass@nas.lan/testshare \
//   go test ./internal/adapters/storage/smb/... -tags integration -v -run TestSMB

package smb_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	smbbackend "github.com/mikey-austin/tierfs/internal/adapters/storage/smb"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend(t *testing.T) *smbbackend.Backend {
	t.Helper()
	uri := os.Getenv("TIERFS_SMB_TEST_URI")
	if uri == "" {
		t.Skip("TIERFS_SMB_TEST_URI not set")
	}

	_, share, prefix, uriUser, uriPass, err := smbbackend.ParseURI(uri)
	_ = share
	_ = prefix

	cfg := smbbackend.Config{
		Name:     "integration-test",
		URI:      uri,
		Username: uriUser,
		Password: uriPass,
	}
	// Override with env vars if set.
	if u := os.Getenv("TIERFS_SMB_USER"); u != "" {
		cfg.Username = u
	}
	if p := os.Getenv("TIERFS_SMB_PASS"); p != "" {
		cfg.Password = p
	}

	log := zaptest.NewLogger(t)
	b, err := smbbackend.New(cfg, log)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	return b
}

// testPrefix returns a unique subdirectory path for this test run.
func testPrefix(t *testing.T) string {
	return fmt.Sprintf("tierfs-integration-test-%d", time.Now().UnixNano())
}

func TestSMB_PutAndGet(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	relPath := prefix + "/cam1/clip.mp4"
	data := bytes.Repeat([]byte("hello smb backend"), 1000)

	require.NoError(t, b.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() { b.Delete(ctx, relPath) })

	rc, size, err := b.Get(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, data, got)
}

func TestSMB_Stat(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	relPath := prefix + "/stat-test.mp4"
	data := []byte("stat test data")

	require.NoError(t, b.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() { b.Delete(ctx, relPath) })

	fi, err := b.Stat(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), fi.Size)
	assert.Equal(t, relPath, fi.RelPath)
}

func TestSMB_Stat_NotExist(t *testing.T) {
	b := newTestBackend(t)
	_, err := b.Stat(context.Background(), "does/not/exist/file.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestSMB_Get_NotExist(t *testing.T) {
	b := newTestBackend(t)
	_, _, err := b.Get(context.Background(), "does/not/exist.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestSMB_Delete(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	relPath := prefix + "/delete-me.mp4"
	data := []byte("delete test")

	require.NoError(t, b.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, b.Delete(ctx, relPath))

	_, err := b.Stat(ctx, relPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestSMB_Delete_NotExist_IsNoop(t *testing.T) {
	b := newTestBackend(t)
	err := b.Delete(context.Background(), "never/existed/file.mp4")
	assert.NoError(t, err)
}

func TestSMB_List(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)

	paths := []string{
		prefix + "/cam1/a.mp4",
		prefix + "/cam1/b.mp4",
		prefix + "/cam2/c.mp4",
	}
	for _, p := range paths {
		require.NoError(t, b.Put(ctx, p, bytes.NewReader([]byte("x")), 1))
	}
	t.Cleanup(func() {
		for _, p := range paths {
			b.Delete(ctx, p)
		}
	})

	entries, err := b.List(ctx, prefix)
	require.NoError(t, err)
	relPaths := make([]string, len(entries))
	for i, e := range entries {
		relPaths[i] = e.RelPath
	}
	for _, p := range paths {
		assert.Contains(t, relPaths, p)
	}
}

func TestSMB_Rename(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	oldPath := prefix + "/old.mp4"
	newPath := prefix + "/new.mp4"
	data := []byte("rename test data")

	require.NoError(t, b.Put(ctx, oldPath, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() {
		b.Delete(ctx, oldPath)
		b.Delete(ctx, newPath)
	})

	require.NoError(t, b.Rename(ctx, oldPath, newPath))

	_, err := b.Stat(ctx, oldPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)

	fi, err := b.Stat(ctx, newPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), fi.Size)
}

func TestSMB_LargeFile(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	relPath := prefix + "/large.bin"

	// 32 MiB — larger than typical SMB write buffer
	size := 32 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	require.NoError(t, b.Put(ctx, relPath, bytes.NewReader(data), int64(size)))
	t.Cleanup(func() { b.Delete(ctx, relPath) })

	rc, gotSize, err := b.Get(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, int64(size), gotSize)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, data, got)
}

func TestSMB_AtomicWrite_TempFileNotVisible(t *testing.T) {
	// Put uses a temp file + rename. List should never return the temp file.
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	relPath := prefix + "/atomic.mp4"
	data := bytes.Repeat([]byte("atomic"), 10_000)

	require.NoError(t, b.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() { b.Delete(ctx, relPath) })

	entries, err := b.List(ctx, prefix)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name, ".tierfs-tmp-",
			"temp files should not appear in List output")
	}
}

func TestSMB_URI_NoCredentials(t *testing.T) {
	b := newTestBackend(t)
	uri := b.URI("recordings/cam1/clip.mp4")
	assert.Contains(t, uri, "smb://")
	assert.NotContains(t, uri, "secret", "URI should not contain password")
	assert.NotContains(t, uri, "pass", "URI should not contain password")
}

func TestSMB_LocalPath_AlwaysFalse(t *testing.T) {
	b := newTestBackend(t)
	path, ok := b.LocalPath("any/file.mp4")
	assert.False(t, ok)
	assert.Empty(t, path)
}
