package smb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/domain"
)

// ── Mock types ────────────────────────────────────────────────────────────────

// mockFileInfo implements os.FileInfo for test use.
type mockFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (m mockFileInfo) Name() string      { return m.name }
func (m mockFileInfo) Size() int64       { return m.size }
func (m mockFileInfo) Mode() os.FileMode { return m.mode }
func (m mockFileInfo) ModTime() time.Time {
	if m.modTime.IsZero() {
		return time.Now()
	}
	return m.modTime
}
func (m mockFileInfo) IsDir() bool      { return m.isDir }
func (m mockFileInfo) Sys() interface{} { return nil }

// mockFile implements smbFile backed by a bytes.Buffer.
type mockFile struct {
	buf    *bytes.Buffer
	info   os.FileInfo
	closed bool
}

func newMockFile(data []byte, info os.FileInfo) *mockFile {
	return &mockFile{buf: bytes.NewBuffer(data), info: info}
}

func (f *mockFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, errors.New("file closed")
	}
	return f.buf.Read(p)
}

func (f *mockFile) Write(p []byte) (int, error) {
	if f.closed {
		return 0, errors.New("file closed")
	}
	return f.buf.Write(p)
}

func (f *mockFile) Close() error {
	f.closed = true
	return nil
}

func (f *mockFile) Stat() (os.FileInfo, error) {
	return f.info, nil
}

// mockShare implements smbShare for unit testing.
type mockShare struct {
	files   map[string][]byte // path -> content
	dirs    map[string]bool   // tracks directories

	// Error injection.
	openErr     error
	openFileErr error
	statErr     error
	removeErr   error
	renameErr   error
	mkdirErr    error
	readDirErr  error
	umountErr   error

	// Call tracking.
	mkdirCalls    []string
	removeCalls   []string
	renameCalls   [][2]string
	readDirCalls  []string
}

func newMockShare() *mockShare {
	return &mockShare{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
}

func (m *mockShare) OpenFile(name string, flag int, perm os.FileMode) (smbFile, error) {
	if m.openFileErr != nil {
		return nil, m.openFileErr
	}
	if flag&os.O_CREATE != 0 {
		m.files[name] = nil
		// Ensure parent dir exists in tracking.
	}
	data := m.files[name]
	info := mockFileInfo{name: name, size: int64(len(data))}
	f := newMockFile(nil, info) // write-only for Put
	// Capture writes: we'll read them back via the buffer.
	f.buf = bytes.NewBuffer(nil)
	// Store reference so we can retrieve written data.
	// We use a closure trick: on Close, save buffer contents.
	return &captureFile{mockFile: f, store: m, path: name}, nil
}

// captureFile wraps mockFile to capture written data on close.
type captureFile struct {
	*mockFile
	store *mockShare
	path  string
}

func (c *captureFile) Close() error {
	c.store.files[c.path] = c.buf.Bytes()
	c.closed = true
	return nil
}

func (m *mockShare) Open(name string) (smbFile, error) {
	if m.openErr != nil {
		return nil, m.openErr
	}
	data, ok := m.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	info := mockFileInfo{name: name, size: int64(len(data))}
	return newMockFile(append([]byte(nil), data...), info), nil
}

func (m *mockShare) Stat(name string) (os.FileInfo, error) {
	if m.statErr != nil {
		return nil, m.statErr
	}
	data, ok := m.files[name]
	if ok {
		return mockFileInfo{name: name, size: int64(len(data))}, nil
	}
	if m.dirs[name] {
		return mockFileInfo{name: name, isDir: true}, nil
	}
	return nil, os.ErrNotExist
}

func (m *mockShare) Remove(name string) error {
	m.removeCalls = append(m.removeCalls, name)
	if m.removeErr != nil {
		return m.removeErr
	}
	delete(m.files, name)
	delete(m.dirs, name)
	return nil
}

func (m *mockShare) Rename(oldpath, newpath string) error {
	m.renameCalls = append(m.renameCalls, [2]string{oldpath, newpath})
	if m.renameErr != nil {
		return m.renameErr
	}
	data, ok := m.files[oldpath]
	if !ok {
		return os.ErrNotExist
	}
	m.files[newpath] = data
	delete(m.files, oldpath)
	return nil
}

func (m *mockShare) Mkdir(name string, perm os.FileMode) error {
	m.mkdirCalls = append(m.mkdirCalls, name)
	if m.mkdirErr != nil {
		return m.mkdirErr
	}
	if m.dirs[name] {
		return os.ErrExist
	}
	m.dirs[name] = true
	return nil
}

func (m *mockShare) ReadDir(name string) ([]os.FileInfo, error) {
	m.readDirCalls = append(m.readDirCalls, name)
	if m.readDirErr != nil {
		return nil, m.readDirErr
	}
	if !m.dirs[name] {
		// Check if there are any files under this path.
		hasEntries := false
		for p := range m.files {
			if strings.HasPrefix(p, name+"/") {
				hasEntries = true
				break
			}
		}
		if !hasEntries {
			return nil, os.ErrNotExist
		}
	}
	var entries []os.FileInfo
	seen := make(map[string]bool)
	prefix := name + "/"
	for p, data := range m.files {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := strings.TrimPrefix(p, prefix)
		parts := strings.SplitN(rest, "/", 2)
		entry := parts[0]
		if seen[entry] {
			continue
		}
		seen[entry] = true
		if len(parts) > 1 {
			// It's a subdirectory.
			entries = append(entries, mockFileInfo{name: entry, isDir: true})
		} else {
			entries = append(entries, mockFileInfo{name: entry, size: int64(len(data))})
		}
	}
	// Also include tracked dirs that are direct children.
	for d := range m.dirs {
		if !strings.HasPrefix(d, prefix) {
			continue
		}
		rest := strings.TrimPrefix(d, prefix)
		if !strings.Contains(rest, "/") && rest != "" && !seen[rest] {
			seen[rest] = true
			entries = append(entries, mockFileInfo{name: rest, isDir: true})
		}
	}
	return entries, nil
}

func (m *mockShare) Umount() error {
	return m.umountErr
}

// ── Helper function tests ─────────────────────────────────────────────────────

func TestSharePath(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		relPath string
		want    string
	}{
		{"no prefix", "", "cam1/clip.mp4", "cam1/clip.mp4"},
		{"with prefix", "frigate", "cam1/clip.mp4", "frigate/cam1/clip.mp4"},
		{"deep prefix", "cctv/frigate", "cam1/clip.mp4", "cctv/frigate/cam1/clip.mp4"},
		{"empty relpath no prefix", "", "", ""},
		{"empty relpath with prefix", "data", "", "data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Backend{p: parsedURI{prefix: tt.prefix}}
			got := b.sharePath(tt.relPath)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMkdirAll(t *testing.T) {
	t.Run("creates nested directories", func(t *testing.T) {
		ms := newMockShare()
		err := mkdirAll(ms, "a/b/c")
		require.NoError(t, err)
		assert.True(t, ms.dirs["a"])
		assert.True(t, ms.dirs["a/b"])
		assert.True(t, ms.dirs["a/b/c"])
		assert.Equal(t, []string{"a", "a/b", "a/b/c"}, ms.mkdirCalls)
	})

	t.Run("ignores existing directories", func(t *testing.T) {
		ms := newMockShare()
		ms.dirs["a"] = true
		ms.dirs["a/b"] = true
		// mkdirAll should not fail when dirs already exist.
		err := mkdirAll(ms, "a/b/c")
		require.NoError(t, err)
		assert.True(t, ms.dirs["a/b/c"])
	})

	t.Run("noop for empty path", func(t *testing.T) {
		ms := newMockShare()
		require.NoError(t, mkdirAll(ms, ""))
		require.NoError(t, mkdirAll(ms, "."))
		require.NoError(t, mkdirAll(ms, "/"))
		assert.Empty(t, ms.mkdirCalls)
	})

	t.Run("propagates mkdir error", func(t *testing.T) {
		ms := newMockShare()
		ms.mkdirErr = errors.New("permission denied")
		err := mkdirAll(ms, "a/b")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "permission denied")
	})
}

func TestPruneEmptyDirs(t *testing.T) {
	t.Run("removes empty dirs up to boundary", func(t *testing.T) {
		ms := newMockShare()
		ms.dirs["prefix"] = true
		ms.dirs["prefix/a"] = true
		ms.dirs["prefix/a/b"] = true
		// No files under prefix/a/b, prefix/a — both are empty.
		pruneEmptyDirs(ms, "prefix/a/b", "prefix")
		assert.False(t, ms.dirs["prefix/a/b"], "should be removed")
		assert.False(t, ms.dirs["prefix/a"], "should be removed")
		assert.True(t, ms.dirs["prefix"], "boundary should not be removed")
	})

	t.Run("stops at non-empty directory", func(t *testing.T) {
		ms := newMockShare()
		ms.dirs["a"] = true
		ms.dirs["a/b"] = true
		ms.dirs["a/b/c"] = true
		ms.files["a/b/file.txt"] = []byte("data")
		// a/b/c is empty → remove. a/b has file.txt → stop.
		pruneEmptyDirs(ms, "a/b/c", "")
		assert.False(t, ms.dirs["a/b/c"], "empty dir should be removed")
		assert.True(t, ms.dirs["a/b"], "non-empty dir should remain")
	})

	t.Run("noop for boundary paths", func(t *testing.T) {
		ms := newMockShare()
		pruneEmptyDirs(ms, "", "")
		pruneEmptyDirs(ms, ".", ".")
		pruneEmptyDirs(ms, "/", "/")
		assert.Empty(t, ms.readDirCalls)
	})
}

// ── isNotExist table tests ────────────────────────────────────────────────────

func TestIsNotExist_Table(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"os.ErrNotExist", os.ErrNotExist, true},
		{"wrapped os.ErrNotExist", fmt.Errorf("open: %w", os.ErrNotExist), true},
		{"STATUS_OBJECT_NAME_NOT_FOUND", errors.New("STATUS_OBJECT_NAME_NOT_FOUND"), true},
		{"STATUS_NO_SUCH_FILE", errors.New("STATUS_NO_SUCH_FILE"), true},
		{"STATUS_OBJECT_PATH_NOT_FOUND", errors.New("STATUS_OBJECT_PATH_NOT_FOUND"), true},
		{"not found string", errors.New("file not found"), true},
		{"does not exist string", errors.New("path does not exist"), true},
		{"random error", errors.New("permission denied"), false},
		{"transport error", io.EOF, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isNotExist(tt.err))
		})
	}
}

// ── isExist table tests ───────────────────────────────────────────────────────

func TestIsExist_Table(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"wrapped os.ErrExist", fmt.Errorf("mkdir: %w", os.ErrExist), true},
		{"STATUS_OBJECT_NAME_COLLISION", errors.New("STATUS_OBJECT_NAME_COLLISION"), true},
		{"already exists", errors.New("file already exists"), true},
		{"random error", errors.New("permission denied"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isExist(tt.err))
		})
	}
}

// ── isTransportError table tests ──────────────────────────────────────────────

// tempNetErr satisfies net.Error for testing.
type tempNetErr struct{ msg string }

func (e *tempNetErr) Error() string   { return e.msg }
func (e *tempNetErr) Timeout() bool   { return false }
func (e *tempNetErr) Temporary() bool { return true }

// Verify tempNetErr implements net.Error at compile time.
var _ net.Error = (*tempNetErr)(nil)

func TestIsTransportError_Table(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"io.EOF", io.EOF, true},
		{"wrapped io.EOF", fmt.Errorf("read: %w", io.EOF), true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"wrapped io.ErrUnexpectedEOF", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{"net.Error", &tempNetErr{msg: "network timeout"}, true},
		{"wrapped net.Error", fmt.Errorf("op: %w", &tempNetErr{msg: "timeout"}), true},
		{"closed connection string", errors.New("use of closed network connection"), true},
		{"connection reset", errors.New("connection reset by peer"), true},
		{"broken pipe", errors.New("broken pipe"), true},
		{"STATUS_CONNECTION_DISCONNECTED", errors.New("STATUS_CONNECTION_DISCONNECTED"), true},
		{"STATUS_CONNECTION_RESET", errors.New("STATUS_CONNECTION_RESET"), true},
		{"STATUS_NETWORK_NAME_DELETED", errors.New("STATUS_NETWORK_NAME_DELETED"), true},
		{"file not found", errors.New("file not found"), false},
		{"permission denied", errors.New("permission denied"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isTransportError(tt.err))
		})
	}
}

// ── withReconnect tests ───────────────────────────────────────────────────────

func TestWithReconnect_FirstAttemptSucceeds(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	calls := 0
	err := b.withReconnect(context.Background(), func(s smbShare) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "fn should be called exactly once on success")
}

func TestWithReconnect_TransportError_Reconnects(t *testing.T) {
	ms := newMockShare()
	ms2 := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	attempt := 0
	// After first failure, connectFn should swap in ms2.
	b.connectFn = func() error {
		b.share = ms2
		return nil
	}

	err := b.withReconnect(context.Background(), func(s smbShare) error {
		attempt++
		if attempt <= 2 {
			// First two calls use the old share and return transport errors.
			return fmt.Errorf("read failed: %w", io.EOF)
		}
		// Third call should receive the new share (ms2) after reconnect.
		assert.Equal(t, ms2, s, "post-reconnect attempt should use new share")
		return nil
	})
	require.NoError(t, err)
	// attempt 1: fast path, fn(ms) returns EOF (transport error)
	// attempt 2: write-lock path, fn(ms) returns EOF again
	// disconnect + reconnect -> connectFn sets b.share = ms2
	// attempt 3: fn(ms2) succeeds
	assert.Equal(t, 3, attempt, "fn should be called 3 times: fast-path + write-lock retry + post-reconnect")
}

func TestWithReconnect_NonTransportError_NoReconnect(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	reconnected := false
	b.connectFn = func() error {
		reconnected = true
		return nil
	}

	calls := 0
	err := b.withReconnect(context.Background(), func(s smbShare) error {
		calls++
		return domain.ErrNotExist
	})
	assert.ErrorIs(t, err, domain.ErrNotExist)
	assert.Equal(t, 1, calls, "fn should be called once for non-transport errors")
	assert.False(t, reconnected, "should not reconnect on non-transport error")
}

func TestWithReconnect_NilShare_Reconnects(t *testing.T) {
	b := newForTest(nil, "", zaptest.NewLogger(t))

	ms := newMockShare()
	b.connectFn = func() error {
		b.share = ms
		return nil
	}

	calls := 0
	err := b.withReconnect(context.Background(), func(s smbShare) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "fn should be called once after reconnect")
}

func TestWithReconnect_ContextCancelled(t *testing.T) {
	b := newForTest(nil, "", zaptest.NewLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	err := b.withReconnect(ctx, func(s smbShare) error {
		t.Fatal("fn should not be called when context is cancelled")
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWithReconnect_ReconnectFails(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	b.connectFn = func() error {
		return errors.New("tcp dial failed")
	}

	err := b.withReconnect(context.Background(), func(s smbShare) error {
		return fmt.Errorf("op: %w", io.EOF)
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "smb reconnect failed")
}

// ── CRUD tests using mock share ───────────────────────────────────────────────

func TestPut_WritesAndRenames(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	data := []byte("hello world")
	err := b.Put(context.Background(), "cam1/clip.mp4", bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	// The file should exist at the final path.
	got, ok := ms.files["cam1/clip.mp4"]
	require.True(t, ok, "file should exist after Put")
	assert.Equal(t, data, got)

	// A rename should have occurred (temp -> final).
	require.NotEmpty(t, ms.renameCalls)
	lastRename := ms.renameCalls[len(ms.renameCalls)-1]
	assert.Equal(t, "cam1/clip.mp4", lastRename[1])
	assert.Contains(t, lastRename[0], ".tierfs-tmp-")
}

func TestPut_WithPrefix(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "frigate", zaptest.NewLogger(t))

	data := []byte("prefixed data")
	err := b.Put(context.Background(), "cam1/clip.mp4", bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	got, ok := ms.files["frigate/cam1/clip.mp4"]
	require.True(t, ok, "file should be stored under prefix")
	assert.Equal(t, data, got)
}

func TestPut_CreatesDirs(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	err := b.Put(context.Background(), "a/b/c/file.mp4", bytes.NewReader([]byte("x")), 1)
	require.NoError(t, err)

	// mkdirAll should have been called for a/b/c.
	assert.Contains(t, ms.mkdirCalls, "a")
	assert.Contains(t, ms.mkdirCalls, "a/b")
	assert.Contains(t, ms.mkdirCalls, "a/b/c")
}

func TestGet_ReturnsData(t *testing.T) {
	ms := newMockShare()
	ms.files["cam1/clip.mp4"] = []byte("video data")
	b := newForTest(ms, "", zaptest.NewLogger(t))

	rc, size, err := b.Get(context.Background(), "cam1/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, int64(10), size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, []byte("video data"), got)
}

func TestGet_NotExist(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	_, _, err := b.Get(context.Background(), "no/such/file.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestGet_WithPrefix(t *testing.T) {
	ms := newMockShare()
	ms.files["prefix/file.mp4"] = []byte("data")
	b := newForTest(ms, "prefix", zaptest.NewLogger(t))

	rc, _, err := b.Get(context.Background(), "file.mp4")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, []byte("data"), got)
}

func TestStat_ReturnsFileInfo(t *testing.T) {
	ms := newMockShare()
	ms.files["cam1/clip.mp4"] = []byte("stat data")
	b := newForTest(ms, "", zaptest.NewLogger(t))

	fi, err := b.Stat(context.Background(), "cam1/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, "cam1/clip.mp4", fi.RelPath)
	assert.Equal(t, "clip.mp4", fi.Name)
	assert.Equal(t, int64(9), fi.Size)
	assert.False(t, fi.IsDir)
}

func TestStat_NotExist(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	_, err := b.Stat(context.Background(), "missing.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestDelete_RemovesFile(t *testing.T) {
	ms := newMockShare()
	ms.files["file.mp4"] = []byte("delete me")
	b := newForTest(ms, "", zaptest.NewLogger(t))

	err := b.Delete(context.Background(), "file.mp4")
	require.NoError(t, err)
	_, ok := ms.files["file.mp4"]
	assert.False(t, ok, "file should be deleted")
}

func TestDelete_NotExist_IsNoop(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zaptest.NewLogger(t))

	err := b.Delete(context.Background(), "no/such/file.mp4")
	assert.NoError(t, err, "delete of non-existent file should succeed")
}

func TestRename_MovesFile(t *testing.T) {
	ms := newMockShare()
	ms.files["old.mp4"] = []byte("rename me")
	b := newForTest(ms, "", zaptest.NewLogger(t))

	err := b.Rename(context.Background(), "old.mp4", "new.mp4")
	require.NoError(t, err)
	_, ok := ms.files["old.mp4"]
	assert.False(t, ok, "old file should not exist")
	assert.Equal(t, []byte("rename me"), ms.files["new.mp4"])
}

func TestRename_CreatesDstDir(t *testing.T) {
	ms := newMockShare()
	ms.files["old.mp4"] = []byte("data")
	b := newForTest(ms, "", zaptest.NewLogger(t))

	err := b.Rename(context.Background(), "old.mp4", "new/dir/file.mp4")
	require.NoError(t, err)
	assert.Contains(t, ms.mkdirCalls, "new")
	assert.Contains(t, ms.mkdirCalls, "new/dir")
}

func TestList_ReturnsFiles(t *testing.T) {
	ms := newMockShare()
	ms.files["data/cam1/a.mp4"] = []byte("a")
	ms.files["data/cam1/b.mp4"] = []byte("bb")
	ms.files["data/cam2/c.mp4"] = []byte("ccc")
	ms.dirs["data"] = true
	ms.dirs["data/cam1"] = true
	ms.dirs["data/cam2"] = true
	b := newForTest(ms, "data", zaptest.NewLogger(t))

	entries, err := b.List(context.Background(), "")
	require.NoError(t, err)

	relPaths := make(map[string]bool)
	for _, e := range entries {
		relPaths[e.RelPath] = true
	}
	assert.True(t, relPaths["cam1/a.mp4"])
	assert.True(t, relPaths["cam1/b.mp4"])
	assert.True(t, relPaths["cam2/c.mp4"])
}

func TestList_WithSubPrefix(t *testing.T) {
	ms := newMockShare()
	ms.files["root/sub/file.mp4"] = []byte("x")
	ms.dirs["root"] = true
	ms.dirs["root/sub"] = true
	b := newForTest(ms, "root", zaptest.NewLogger(t))

	entries, err := b.List(context.Background(), "sub")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "sub/file.mp4", entries[0].RelPath)
}

func TestList_EmptyDir(t *testing.T) {
	ms := newMockShare()
	ms.dirs["data"] = true
	ms.dirs["data/empty"] = true
	b := newForTest(ms, "data", zaptest.NewLogger(t))

	entries, err := b.List(context.Background(), "empty")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// ── Scheme / URI / LocalPath tests ────────────────────────────────────────────

func TestScheme(t *testing.T) {
	b := &Backend{}
	assert.Equal(t, "smb", b.Scheme())
}

func TestLocalPath_AlwaysFalse(t *testing.T) {
	b := &Backend{}
	p, ok := b.LocalPath("any/file.mp4")
	assert.False(t, ok)
	assert.Empty(t, p)
}

func TestURI_NoCredentials(t *testing.T) {
	b := &Backend{
		p: parsedURI{
			host:      "nas.lan:445",
			shareName: "recordings",
			prefix:    "frigate",
		},
	}
	uri := b.URI("cam1/clip.mp4")
	assert.Equal(t, "smb://nas.lan/recordings/frigate/cam1/clip.mp4", uri)
}

func TestURI_NonDefaultPort(t *testing.T) {
	b := &Backend{
		p: parsedURI{
			host:      "nas.lan:4455",
			shareName: "data",
		},
	}
	uri := b.URI("file.mp4")
	assert.Equal(t, "smb://nas.lan:4455/data/file.mp4", uri)
}

// ── resolveCredential tests ───────────────────────────────────────────────────

func TestResolveCredential(t *testing.T) {
	t.Run("config wins", func(t *testing.T) {
		t.Setenv("TIERFS_SMB_USER", "envuser")
		got := resolveCredential("cfguser", "TIERFS_SMB_USER", "fallback")
		assert.Equal(t, "cfguser", got)
	})

	t.Run("env beats fallback", func(t *testing.T) {
		t.Setenv("TIERFS_SMB_USER", "envuser")
		got := resolveCredential("", "TIERFS_SMB_USER", "fallback")
		assert.Equal(t, "envuser", got)
	})

	t.Run("fallback when all empty", func(t *testing.T) {
		got := resolveCredential("", "TIERFS_SMB_NONEXISTENT_KEY", "fallback")
		assert.Equal(t, "fallback", got)
	})

	t.Run("empty when all empty", func(t *testing.T) {
		got := resolveCredential("", "TIERFS_SMB_NONEXISTENT_KEY", "")
		assert.Empty(t, got)
	})
}

// ── Close tests ───────────────────────────────────────────────────────────────

func TestClose(t *testing.T) {
	ms := newMockShare()
	b := newForTest(ms, "", zap.NewNop())
	err := b.Close()
	assert.NoError(t, err)
	assert.Nil(t, b.share, "share should be nil after Close")
}

// ── walkShare tests ───────────────────────────────────────────────────────────

func TestWalkShare_NotExist_ReturnsNil(t *testing.T) {
	ms := newMockShare()
	var collected []domain.FileInfo
	err := walkShare(ms, "nonexistent", "", func(fi domain.FileInfo) {
		collected = append(collected, fi)
	})
	require.NoError(t, err)
	assert.Empty(t, collected)
}
