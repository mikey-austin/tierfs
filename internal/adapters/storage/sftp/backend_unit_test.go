package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/ssh"

	"github.com/mikey-austin/tierfs/internal/domain"
)

// ── Mock sftpClient ───────────────────────────────────────────────────────────

// mockSFTPClient is a minimal mock that satisfies the sftpClient interface.
// Most tests only need the client for withReconnect plumbing and do not call
// real methods, so the default implementations return nil/zero.
type mockSFTPClient struct {
	closeErr error
	closed   atomic.Bool
}

func (m *mockSFTPClient) Open(_ string) (*sftp.File, error) { return nil, nil }
func (m *mockSFTPClient) OpenFile(_ string, _ int) (*sftp.File, error) {
	return nil, nil
}
func (m *mockSFTPClient) Stat(_ string) (os.FileInfo, error)      { return nil, nil }
func (m *mockSFTPClient) Mkdir(_ string) error                    { return nil }
func (m *mockSFTPClient) Rename(_, _ string) error                { return nil }
func (m *mockSFTPClient) Remove(_ string) error                   { return nil }
func (m *mockSFTPClient) RemoveDirectory(_ string) error          { return nil }
func (m *mockSFTPClient) ReadDir(_ string) ([]os.FileInfo, error) { return nil, nil }
func (m *mockSFTPClient) Close() error {
	m.closed.Store(true)
	return m.closeErr
}

// ── Helper to build a Backend quickly ─────────────────────────────────────────

func testBackend(t *testing.T, mock sftpClient, p parsedURI) *Backend {
	t.Helper()
	log := zaptest.NewLogger(t)
	return newForTest(mock, p, log)
}

// ── withReconnect tests ───────────────────────────────────────────────────────

func TestWithReconnect_FirstAttemptSucceeds(t *testing.T) {
	mock := &mockSFTPClient{}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	calls := 0
	err := b.withReconnect(context.Background(), func(_ sftpClient) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "should call fn exactly once when it succeeds")
}

func TestWithReconnect_NonTransportError_NoReconnect(t *testing.T) {
	mock := &mockSFTPClient{}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	sentinel := errors.New("file permission denied")
	calls := 0
	err := b.withReconnect(context.Background(), func(_ sftpClient) error {
		calls++
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, calls, "non-transport error must not trigger reconnect")
}

func TestWithReconnect_TransportError_Reconnects(t *testing.T) {
	mock := &mockSFTPClient{}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	var calls atomic.Int32
	err := b.withReconnect(context.Background(), func(_ sftpClient) error {
		n := calls.Add(1)
		if n <= 2 {
			// First two calls return transport error (read-lock attempt + write-lock attempt).
			return io.EOF
		}
		return nil // third call (after reconnect) succeeds
	})
	require.NoError(t, err)
	// withReconnect tries: (1) under RLock, (2) under Lock (re-check), then disconnects,
	// reconnects, and (3) calls fn again. Total = 3.
	assert.Equal(t, int32(3), calls.Load(), "transport error should cause reconnect and retry")
}

func TestWithReconnect_TransportError_ReconnectFails(t *testing.T) {
	mock := &mockSFTPClient{}
	log := zaptest.NewLogger(t)
	b := &Backend{
		cfg:    Config{Name: "test"},
		p:      parsedURI{basePath: "/data"},
		log:    log,
		client: mock,
	}
	// Make connectFn fail.
	b.connectFn = func() (sftpClient, *ssh.Client, error) {
		return nil, nil, errors.New("dial failed")
	}

	err := b.withReconnect(context.Background(), func(_ sftpClient) error {
		return io.EOF
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sftp reconnect")
	assert.Contains(t, err.Error(), "dial failed")
}

func TestWithReconnect_ContextCancelled_DuringDelay(t *testing.T) {
	mock := &mockSFTPClient{}
	log := zaptest.NewLogger(t)
	b := &Backend{
		cfg:    Config{Name: "test"},
		p:      parsedURI{basePath: "/data"},
		log:    log,
		client: mock,
	}
	// connectFn should not be called when context is cancelled.
	b.connectFn = func() (sftpClient, *ssh.Client, error) {
		t.Fatal("connectFn should not be called after context cancellation")
		return nil, nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := b.withReconnect(ctx, func(_ sftpClient) error {
		return io.EOF
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestWithReconnect_NilClient_Reconnects(t *testing.T) {
	mock := &mockSFTPClient{}
	log := zaptest.NewLogger(t)
	b := &Backend{
		cfg:    Config{Name: "test"},
		p:      parsedURI{basePath: "/data"},
		log:    log,
		client: nil, // simulate disconnected state
	}
	b.connectFn = func() (sftpClient, *ssh.Client, error) {
		b.client = mock
		return mock, nil, nil
	}

	calls := 0
	err := b.withReconnect(context.Background(), func(_ sftpClient) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

// ── isNotExist table tests ────────────────────────────────────────────────────

func TestIsNotExist_Table(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{"nil", nil, false},
		{"fs.ErrNotExist", fs.ErrNotExist, true},
		{"wrapped fs.ErrNotExist", fmt.Errorf("open: %w", fs.ErrNotExist), true},
		{"os.ErrNotExist", os.ErrNotExist, true},
		{"sftp StatusError code 2", &sftp.StatusError{Code: 2}, true},
		{"sftp StatusError code 4", &sftp.StatusError{Code: 4}, false},
		{"string does not exist", errors.New("path does not exist"), true},
		{"string no such file", errors.New("no such file or directory"), true},
		{"string not found", errors.New("key not found"), true},
		{"unrelated error", errors.New("permission denied"), false},
		{"wrapped sftp code 2", fmt.Errorf("get: %w", &sftp.StatusError{Code: 2}), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isNotExist(tc.err)
			assert.Equal(t, tc.expect, got)
		})
	}
}

// ── isTransportError table tests ──────────────────────────────────────────────

// netTimeoutError implements net.Error for testing.
type netTimeoutError struct{ msg string }

func (e *netTimeoutError) Error() string   { return e.msg }
func (e *netTimeoutError) Timeout() bool   { return true }
func (e *netTimeoutError) Temporary() bool { return false }

// Verify it satisfies net.Error at compile time.
var _ net.Error = (*netTimeoutError)(nil)

func TestIsTransportError_Table(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{"nil", nil, false},
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"wrapped EOF", fmt.Errorf("read: %w", io.EOF), true},
		{"net.Error", &netTimeoutError{"connection timed out"}, true},
		{"wrapped net.Error", fmt.Errorf("dial: %w", &netTimeoutError{"timeout"}), true},
		{"string connection lost", errors.New("sftp: connection lost"), true},
		{"string connection reset", errors.New("connection reset by peer"), true},
		{"string broken pipe", errors.New("write: broken pipe"), true},
		{"string closed connection", errors.New("use of closed network connection"), true},
		{"string EOF in message", errors.New("unexpected EOF in stream"), true},
		{"permission denied", errors.New("permission denied"), false},
		{"sftp StatusError", &sftp.StatusError{Code: 4}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isTransportError(tc.err)
			assert.Equal(t, tc.expect, got)
		})
	}
}

// ── isExist table tests ───────────────────────────────────────────────────────

func TestIsExist_Table(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{"nil", nil, false},
		{"fs.ErrExist", fs.ErrExist, true},
		{"os.ErrExist", os.ErrExist, true},
		{"sftp StatusError code 11", &sftp.StatusError{Code: 11}, true},
		{"sftp StatusError code 4", &sftp.StatusError{Code: 4}, false},
		{"string already exists", errors.New("file already exists"), true},
		{"string file exists", errors.New("file exists"), true},
		{"unrelated", errors.New("timeout"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, isExist(tc.err))
		})
	}
}

// ── remotePath tests ──────────────────────────────────────────────────────────

func TestRemotePath(t *testing.T) {
	tests := []struct {
		name    string
		base    string
		prefix  string
		relPath string
		want    string
	}{
		{
			name:    "base only, no prefix, no rel",
			base:    "/data",
			prefix:  "",
			relPath: "",
			want:    "/data",
		},
		{
			name:    "base + rel, no prefix",
			base:    "/data",
			prefix:  "",
			relPath: "cam1/clip.mp4",
			want:    "/data/cam1/clip.mp4",
		},
		{
			name:    "base + prefix + rel",
			base:    "/mnt",
			prefix:  "storage/cctv",
			relPath: "cam1/clip.mp4",
			want:    "/mnt/storage/cctv/cam1/clip.mp4",
		},
		{
			name:    "base + prefix, no rel",
			base:    "/mnt",
			prefix:  "storage/cctv",
			relPath: "",
			want:    "/mnt/storage/cctv",
		},
		{
			name:    "deep prefix",
			base:    "/srv",
			prefix:  "tier1/cctv/frigate",
			relPath: "recording.mp4",
			want:    "/srv/tier1/cctv/frigate/recording.mp4",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := &Backend{
				p: parsedURI{basePath: tc.base, prefix: tc.prefix},
			}
			got := b.remotePath(tc.relPath)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ── resolveStr tests ──────────────────────────────────────────────────────────

func TestResolveStr(t *testing.T) {
	t.Run("config wins", func(t *testing.T) {
		t.Setenv("TEST_RESOLVE_KEY", "envval")
		assert.Equal(t, "configval", resolveStr("configval", "TEST_RESOLVE_KEY", "fallback"))
	})
	t.Run("env wins when config empty", func(t *testing.T) {
		t.Setenv("TEST_RESOLVE_KEY", "envval")
		assert.Equal(t, "envval", resolveStr("", "TEST_RESOLVE_KEY", "fallback"))
	})
	t.Run("fallback when both empty", func(t *testing.T) {
		t.Setenv("TEST_RESOLVE_KEY", "")
		assert.Equal(t, "fallback", resolveStr("", "TEST_RESOLVE_KEY", "fallback"))
	})
	t.Run("empty fallback", func(t *testing.T) {
		t.Setenv("TEST_RESOLVE_KEY", "")
		assert.Equal(t, "", resolveStr("", "TEST_RESOLVE_KEY", ""))
	})
}

// ── sftpErr tests ─────────────────────────────────────────────────────────────

func TestSftpErr_MapsNotExist(t *testing.T) {
	err := sftpErr(fs.ErrNotExist, "/data/file.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestSftpErr_WrapsOther(t *testing.T) {
	orig := errors.New("permission denied")
	err := sftpErr(orig, "/data/file.mp4")
	assert.NotErrorIs(t, err, domain.ErrNotExist)
	assert.Contains(t, err.Error(), "/data/file.mp4")
	assert.Contains(t, err.Error(), "permission denied")
}

// ── URI display tests ─────────────────────────────────────────────────────────

func TestURI_StripsDefaultPort(t *testing.T) {
	b := &Backend{
		p: parsedURI{
			hostPort: "nas.lan:22",
			basePath: "/data",
			prefix:   "cctv",
		},
	}
	uri := b.URI("cam1/clip.mp4")
	assert.Equal(t, "sftp://nas.lan/data/cctv/cam1/clip.mp4", uri)
}

func TestURI_KeepsNonDefaultPort(t *testing.T) {
	b := &Backend{
		p: parsedURI{
			hostPort: "nas.lan:2222",
			basePath: "/data",
			prefix:   "",
		},
	}
	uri := b.URI("file.mp4")
	assert.Equal(t, "sftp://nas.lan:2222/data/file.mp4", uri)
}

// ── Scheme and LocalPath ──────────────────────────────────────────────────────

func TestScheme(t *testing.T) {
	b := &Backend{}
	assert.Equal(t, "sftp", b.Scheme())
}

func TestLocalPath_AlwaysFalse(t *testing.T) {
	b := &Backend{}
	p, ok := b.LocalPath("any/path")
	assert.False(t, ok)
	assert.Empty(t, p)
}

// ── Stat via mock ─────────────────────────────────────────────────────────────

type statMockClient struct {
	mockSFTPClient
	statFn func(string) (os.FileInfo, error)
}

func (m *statMockClient) Stat(p string) (os.FileInfo, error) {
	return m.statFn(p)
}

type fakeFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (f *fakeFileInfo) Name() string       { return f.name }
func (f *fakeFileInfo) Size() int64        { return f.size }
func (f *fakeFileInfo) Mode() fs.FileMode  { return 0644 }
func (f *fakeFileInfo) ModTime() time.Time { return f.modTime }
func (f *fakeFileInfo) IsDir() bool        { return f.isDir }
func (f *fakeFileInfo) Sys() any           { return nil }

func TestStat_ReturnsFileInfo(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mock := &statMockClient{
		statFn: func(p string) (os.FileInfo, error) {
			assert.Equal(t, "/data/cctv/cam1/clip.mp4", p)
			return &fakeFileInfo{
				name:    "clip.mp4",
				size:    42000,
				modTime: now,
			}, nil
		},
	}
	b := testBackend(t, mock, parsedURI{basePath: "/data", prefix: "cctv"})

	fi, err := b.Stat(context.Background(), "cam1/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, "cam1/clip.mp4", fi.RelPath)
	assert.Equal(t, "clip.mp4", fi.Name)
	assert.Equal(t, int64(42000), fi.Size)
	assert.Equal(t, now, fi.ModTime)
	assert.False(t, fi.IsDir)
}

func TestStat_NotExist(t *testing.T) {
	mock := &statMockClient{
		statFn: func(_ string) (os.FileInfo, error) {
			return nil, fs.ErrNotExist
		},
	}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	_, err := b.Stat(context.Background(), "missing.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

// ── Delete via mock ───────────────────────────────────────────────────────────

type deleteMockClient struct {
	mockSFTPClient
	removeFn    func(string) error
	readDirFn   func(string) ([]os.FileInfo, error)
	removeDirFn func(string) error
}

func (m *deleteMockClient) Remove(p string) error {
	if m.removeFn != nil {
		return m.removeFn(p)
	}
	return nil
}

func (m *deleteMockClient) ReadDir(p string) ([]os.FileInfo, error) {
	if m.readDirFn != nil {
		return m.readDirFn(p)
	}
	return nil, fs.ErrNotExist
}

func (m *deleteMockClient) RemoveDirectory(p string) error {
	if m.removeDirFn != nil {
		return m.removeDirFn(p)
	}
	return nil
}

func TestDelete_Success(t *testing.T) {
	var removedPath string
	mock := &deleteMockClient{
		removeFn: func(p string) error {
			removedPath = p
			return nil
		},
	}
	b := testBackend(t, mock, parsedURI{basePath: "/data", prefix: "cctv"})

	err := b.Delete(context.Background(), "cam1/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, "/data/cctv/cam1/clip.mp4", removedPath)
}

func TestDelete_NotExist_IsNoop(t *testing.T) {
	mock := &deleteMockClient{
		removeFn: func(_ string) error {
			return fs.ErrNotExist
		},
	}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	err := b.Delete(context.Background(), "missing.mp4")
	assert.NoError(t, err)
}

func TestDelete_OtherError_Returns(t *testing.T) {
	mock := &deleteMockClient{
		removeFn: func(_ string) error {
			return errors.New("permission denied")
		},
	}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	err := b.Delete(context.Background(), "locked.mp4")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
}

// ── List via mock ─────────────────────────────────────────────────────────────

type listMockClient struct {
	mockSFTPClient
	tree map[string][]os.FileInfo
}

func (m *listMockClient) ReadDir(p string) ([]os.FileInfo, error) {
	entries, ok := m.tree[p]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return entries, nil
}

func TestList_ReturnsFilesRecursively(t *testing.T) {
	now := time.Now()
	mock := &listMockClient{
		tree: map[string][]os.FileInfo{
			"/data/cctv": {
				&fakeFileInfo{name: "cam1", isDir: true},
				&fakeFileInfo{name: "readme.txt", size: 100, modTime: now},
			},
			"/data/cctv/cam1": {
				&fakeFileInfo{name: "clip.mp4", size: 5000, modTime: now},
			},
		},
	}
	b := testBackend(t, mock, parsedURI{basePath: "/data", prefix: "cctv"})

	entries, err := b.List(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
		assert.False(t, e.IsDir)
	}
	assert.Contains(t, names, "readme.txt")
	assert.Contains(t, names, "clip.mp4")
}

func TestList_EmptyDir(t *testing.T) {
	mock := &listMockClient{
		tree: map[string][]os.FileInfo{
			"/data": {},
		},
	}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	entries, err := b.List(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestList_NotExistDir(t *testing.T) {
	mock := &listMockClient{tree: map[string][]os.FileInfo{}}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	entries, err := b.List(context.Background(), "missing")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// ── randHex ───────────────────────────────────────────────────────────────────

func TestRandHex_Length(t *testing.T) {
	for _, n := range []int{4, 8, 16} {
		h := randHex(n)
		assert.Len(t, h, n, "randHex(%d) should return %d characters", n, n)
	}
}

func TestRandHex_Unique(t *testing.T) {
	a := randHex(16)
	b := randHex(16)
	assert.NotEqual(t, a, b, "two calls should produce different values")
}

// ── newForTest ────────────────────────────────────────────────────────────────

func TestNewForTest_SetsClient(t *testing.T) {
	mock := &mockSFTPClient{}
	log := zap.NewNop()
	b := newForTest(mock, parsedURI{basePath: "/x"}, log)
	assert.NotNil(t, b.client)
	assert.Equal(t, "/x", b.p.basePath)
}

// ── Close ─────────────────────────────────────────────────────────────────────

func TestClose_CallsClientClose(t *testing.T) {
	mock := &mockSFTPClient{}
	b := testBackend(t, mock, parsedURI{basePath: "/data"})

	err := b.Close()
	require.NoError(t, err)
	assert.True(t, mock.closed.Load(), "Close should call client.Close()")
	assert.Nil(t, b.client, "client should be nil after Close")
}
