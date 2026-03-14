//go:build integration

// Run with:
//   TIERFS_SFTP_TEST_URI=sftp://user@host/base/path \
//   TIERFS_SFTP_KEY_PATH=~/.ssh/id_ed25519 \
//   go test ./internal/adapters/storage/sftp/... -tags integration -v -run TestSFTP

package sftp_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	sftpbackend "github.com/mikey-austin/tierfs/internal/adapters/storage/sftp"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend(t *testing.T) *sftpbackend.Backend {
	t.Helper()
	uri := os.Getenv("TIERFS_SFTP_TEST_URI")
	if uri == "" {
		t.Skip("TIERFS_SFTP_TEST_URI not set")
	}

	_, _, _, uriUser, err := sftpbackend.ParseURI(uri)
	require.NoError(t, err)

	cfg := sftpbackend.Config{
		Name:           "integration-test",
		URI:            uri,
		Username:       uriUser,
		KeyPath:        os.Getenv("TIERFS_SFTP_KEY_PATH"),
		KeyPassphrase:  os.Getenv("TIERFS_SFTP_KEY_PASSPHRASE"),
		Password:       os.Getenv("TIERFS_SFTP_PASS"),
		HostKey:        os.Getenv("TIERFS_SFTP_HOST_KEY"),
	}
	log := zaptest.NewLogger(t)
	b, err := sftpbackend.New(cfg, log)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	return b
}

func testPrefix(t *testing.T) string {
	return fmt.Sprintf("tierfs-int-test-%d", time.Now().UnixNano())
}

func TestSFTP_PutAndGet(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	p := testPrefix(t) + "/cam1/clip.mp4"
	data := bytes.Repeat([]byte("sftp put/get test"), 500)

	require.NoError(t, b.Put(ctx, p, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() { b.Delete(ctx, p) })

	rc, size, err := b.Get(ctx, p)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, data, got)
}

func TestSFTP_Stat(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	p := testPrefix(t) + "/stat.mp4"
	data := []byte("stat test")

	require.NoError(t, b.Put(ctx, p, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() { b.Delete(ctx, p) })

	fi, err := b.Stat(ctx, p)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), fi.Size)
	assert.Equal(t, p, fi.RelPath)
}

func TestSFTP_Stat_NotExist(t *testing.T) {
	b := newTestBackend(t)
	_, err := b.Stat(context.Background(), "does/not/exist/file.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestSFTP_Get_NotExist(t *testing.T) {
	b := newTestBackend(t)
	_, _, err := b.Get(context.Background(), "does/not/exist.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestSFTP_Delete(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	p := testPrefix(t) + "/delete.mp4"
	data := []byte("delete me")

	require.NoError(t, b.Put(ctx, p, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, b.Delete(ctx, p))

	_, err := b.Stat(ctx, p)
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestSFTP_Delete_NotExist_IsNoop(t *testing.T) {
	b := newTestBackend(t)
	assert.NoError(t, b.Delete(context.Background(), "never/existed/file.mp4"))
}

func TestSFTP_List(t *testing.T) {
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
	var got []string
	for _, e := range entries {
		got = append(got, e.RelPath)
	}
	for _, p := range paths {
		assert.Contains(t, got, p)
	}
}

func TestSFTP_Rename(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	old := prefix + "/old.mp4"
	new := prefix + "/new.mp4"
	data := []byte("rename data")

	require.NoError(t, b.Put(ctx, old, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() {
		b.Delete(ctx, old)
		b.Delete(ctx, new)
	})

	require.NoError(t, b.Rename(ctx, old, new))

	_, err := b.Stat(ctx, old)
	assert.ErrorIs(t, err, domain.ErrNotExist)

	fi, err := b.Stat(ctx, new)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), fi.Size)
}

func TestSFTP_LargeFile(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	p := testPrefix(t) + "/large.bin"
	size := 32 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	require.NoError(t, b.Put(ctx, p, bytes.NewReader(data), int64(size)))
	t.Cleanup(func() { b.Delete(ctx, p) })

	rc, gotSize, err := b.Get(ctx, p)
	require.NoError(t, err)
	assert.Equal(t, int64(size), gotSize)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, data, got)
}

func TestSFTP_AtomicWrite_NoTempFilesInList(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	p := prefix + "/atomic.mp4"
	data := bytes.Repeat([]byte("atomic write"), 1000)

	require.NoError(t, b.Put(ctx, p, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() { b.Delete(ctx, p) })

	entries, err := b.List(ctx, prefix)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name, ".tierfs-tmp-",
			"temp files must not appear in List output")
	}
}

func TestSFTP_URI_NoCredentials(t *testing.T) {
	b := newTestBackend(t)
	uri := b.URI("recordings/cam1/clip.mp4")
	assert.Contains(t, uri, "sftp://")
	// Password must not appear in display URI.
	pass := os.Getenv("TIERFS_SFTP_PASS")
	if pass != "" {
		assert.NotContains(t, uri, pass)
	}
}

func TestSFTP_LocalPath_AlwaysFalse(t *testing.T) {
	b := newTestBackend(t)
	path, ok := b.LocalPath("any/file.mp4")
	assert.False(t, ok)
	assert.Empty(t, path)
}

func TestSFTP_NestedDirectoryCreation(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	p := testPrefix(t) + "/a/b/c/d/deep.mp4"
	data := []byte("deep path")

	require.NoError(t, b.Put(ctx, p, bytes.NewReader(data), int64(len(data))))
	t.Cleanup(func() { b.Delete(ctx, p) })

	rc, _, err := b.Get(ctx, p)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, data, got)
}

func TestSFTP_ConcurrentPuts(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	prefix := testPrefix(t)
	n := 8
	errc := make(chan error, n)
	paths := make([]string, n)

	for i := 0; i < n; i++ {
		paths[i] = fmt.Sprintf("%s/concurrent/%02d.mp4", prefix, i)
		go func(i int) {
			data := bytes.Repeat([]byte{byte(i)}, 1024)
			errc <- b.Put(ctx, paths[i], bytes.NewReader(data), int64(len(data)))
		}(i)
	}
	t.Cleanup(func() {
		for _, p := range paths {
			b.Delete(ctx, p)
		}
	})
	for i := 0; i < n; i++ {
		assert.NoError(t, <-errc, "concurrent put %d should succeed", i)
	}

	entries, err := b.List(ctx, prefix)
	require.NoError(t, err)
	assert.Len(t, entries, n)
}
