// Package file implements the Backend port for local filesystems (file:// URIs).
// Writes are atomic: data lands in a sibling temp file and is renamed into place.
// Reads are served with O_NOATIME to avoid mtime churn on surveillance media.
package file

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mikey-austin/tierfs/internal/domain"
)

// Backend is a file:// storage backend.
type Backend struct {
	root string
}

// New creates a Backend rooted at root. The directory is created if absent.
func New(root string) (*Backend, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("file backend: mkdir %q: %w", root, err)
	}
	return &Backend{root: root}, nil
}

func (b *Backend) Scheme() string { return "file" }

func (b *Backend) URI(relPath string) string {
	return "file://" + b.abs(relPath)
}

func (b *Backend) LocalPath(relPath string) (string, bool) {
	return b.abs(relPath), true
}

// Put writes data from r into the backend atomically.
// It first writes to a temp file in the same directory, then renames.
// size is accepted but ignored for file backends (io.Copy drives the write).
func (b *Backend) Put(_ context.Context, relPath string, r io.Reader, _ int64) error {
	dst := b.abs(relPath)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("file put: mkdirall: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tierfs-put-*")
	if err != nil {
		return fmt.Errorf("file put: create temp: %w", err)
	}
	tmpName := tmp.Name()

	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}

	if _, err := io.Copy(tmp, r); err != nil {
		cleanup()
		return fmt.Errorf("file put: copy: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("file put: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("file put: close: %w", err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("file put: rename: %w", err)
	}
	return nil
}

// Get opens relPath for reading. Caller must close the returned ReadCloser.
func (b *Backend) Get(_ context.Context, relPath string) (io.ReadCloser, int64, error) {
	f, err := os.OpenFile(b.abs(relPath), os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, domain.ErrNotExist
		}
		return nil, 0, fmt.Errorf("file get: open: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, fmt.Errorf("file get: stat: %w", err)
	}
	return f, st.Size(), nil
}

// Stat returns file metadata for relPath.
func (b *Backend) Stat(_ context.Context, relPath string) (*domain.FileInfo, error) {
	st, err := os.Stat(b.abs(relPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, domain.ErrNotExist
		}
		return nil, fmt.Errorf("file stat: %w", err)
	}
	return &domain.FileInfo{
		RelPath: relPath,
		Name:    filepath.Base(relPath),
		Size:    st.Size(),
		ModTime: st.ModTime(),
		IsDir:   st.IsDir(),
	}, nil
}

// Delete removes relPath and any now-empty parent directories up to root.
func (b *Backend) Delete(_ context.Context, relPath string) error {
	p := b.abs(relPath)
	if err := os.Remove(p); err != nil {
		if os.IsNotExist(err) {
			return domain.ErrNotExist
		}
		return fmt.Errorf("file delete: %w", err)
	}
	// Prune empty parent directories up to (but not including) root.
	b.pruneEmptyDirs(filepath.Dir(p))
	return nil
}

// List returns FileInfo for all regular files recursively under prefix.
// prefix="" lists all files in the backend.
func (b *Backend) List(_ context.Context, prefix string) ([]domain.FileInfo, error) {
	base := b.root
	if prefix != "" {
		base = filepath.Join(b.root, filepath.FromSlash(prefix))
	}

	var out []domain.FileInfo
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Skip unreadable directories gracefully.
			if os.IsPermission(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Skip temp files left by interrupted Put operations.
		if strings.HasPrefix(d.Name(), ".tierfs-put-") {
			return nil
		}
		rel, err := filepath.Rel(b.root, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, domain.FileInfo{
			RelPath: filepath.ToSlash(rel),
			Name:    d.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		})
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("file list: walk: %w", err)
	}
	return out, nil
}

// Rename moves a file from oldRel to newRel within the backend.
// This is used by the FUSE rename handler. Not part of the Backend interface
// (rename is handled by the FUSE layer for file backends directly).
func (b *Backend) Rename(_ context.Context, oldRel, newRel string) error {
	src := b.abs(oldRel)
	dst := b.abs(newRel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("file rename: mkdir: %w", err)
	}
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("file rename: %w", err)
	}
	b.pruneEmptyDirs(filepath.Dir(src))
	return nil
}

// OpenFile opens a file for direct FUSE access; used by handle.go.
func (b *Backend) OpenFile(relPath string, flags int) (*os.File, error) {
	f, err := os.OpenFile(b.abs(relPath), flags, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, domain.ErrNotExist
		}
		return nil, fmt.Errorf("file open: %w", err)
	}
	return f, nil
}

// CreateFile creates a new file (O_CREATE|O_WRONLY|O_TRUNC) and all parent dirs.
func (b *Backend) CreateFile(relPath string, mode os.FileMode) (*os.File, error) {
	p := b.abs(relPath)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, fmt.Errorf("file create: mkdirall: %w", err)
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return nil, fmt.Errorf("file create: %w", err)
	}
	return f, nil
}

// Utimes sets access and modification times for relPath.
func (b *Backend) Utimes(relPath string, atime, mtime time.Time) error {
	return os.Chtimes(b.abs(relPath), atime, mtime)
}

func (b *Backend) abs(relPath string) string {
	return filepath.Join(b.root, filepath.FromSlash(relPath))
}

func (b *Backend) Root() string { return b.root }

func (b *Backend) pruneEmptyDirs(dir string) {
	for dir != b.root && dir != filepath.Dir(dir) {
		if err := os.Remove(dir); err != nil {
			break // not empty or permission error; stop
		}
		dir = filepath.Dir(dir)
	}
}
