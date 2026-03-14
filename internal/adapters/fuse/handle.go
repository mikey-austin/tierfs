package fuse

import (
	"context"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/domain"
)

// writeHandle wraps an *os.File that is being written to by a FUSE client.
// On Release, it calls TierService.OnWriteComplete to finalise metadata and
// enqueue replication.
//
// For remote backends (S3 etc.) stageFile holds the path to a local temp copy
// that will be Push-ed to the backend on release.
type writeHandle struct {
	nodefs.File

	mu        sync.Mutex
	f         *os.File
	relPath   string
	tierName  string
	svc       *app.TierService
	log       *zap.Logger

	// stageFile is non-empty when writing to a remote backend.
	stageFile string
	backend   domain.Backend
}

// newWriteHandle creates a writeHandle and registers the open write with
// the TierService's WriteGuard.
func newWriteHandle(
	f *os.File,
	relPath, tierName string,
	svc *app.TierService,
	log *zap.Logger,
	stageFile string,
	backend domain.Backend,
) *writeHandle {
	h := &writeHandle{
		File:      nodefs.NewDefaultFile(),
		f:         f,
		relPath:   relPath,
		tierName:  tierName,
		svc:       svc,
		log:       log,
		stageFile: stageFile,
		backend:   backend,
	}
	svc.Guard().Open(relPath)
	return h
}

func (h *writeHandle) Read(buf []byte, off int64) (gofuse.ReadResult, gofuse.Status) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n, err := h.f.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, gofuse.EIO
	}
	return gofuse.ReadResultData(buf[:n]), gofuse.OK
}

func (h *writeHandle) Write(data []byte, off int64) (uint32, gofuse.Status) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n, err := h.f.WriteAt(data, off)
	if err != nil {
		h.log.Error("write", zap.String("path", h.relPath), zap.Error(err))
		return 0, gofuse.EIO
	}
	return uint32(n), gofuse.OK
}

func (h *writeHandle) Fsync(flags int) gofuse.Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.f.Sync(); err != nil {
		return gofuse.EIO
	}
	return gofuse.OK
}

func (h *writeHandle) Flush() gofuse.Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.f.Sync(); err != nil {
		return gofuse.EIO
	}
	return gofuse.OK
}

func (h *writeHandle) Release() {
	// Signal the WriteGuard that this write handle is being closed. The
	// quiescence window starts from this moment; the Replicator will not
	// start copying until the window elapses.
	h.svc.Guard().Close(h.relPath)

	h.mu.Lock()
	defer h.mu.Unlock()

	stat, err := h.f.Stat()
	h.f.Close()
	if err != nil {
		h.log.Error("stat on release", zap.String("path", h.relPath), zap.Error(err))
		return
	}

	size := stat.Size()
	modTime := stat.ModTime()

	// If writing to a remote backend, push the staged file now.
	if h.stageFile != "" && h.backend != nil {
		go func() {
			h.pushStage(size, modTime)
		}()
		return
	}

	ctx := context.Background()
	if err := h.svc.OnWriteComplete(ctx, h.relPath, h.tierName, size, modTime); err != nil {
		h.log.Error("on write complete",
			zap.String("path", h.relPath),
			zap.Error(err),
		)
	}
}

func (h *writeHandle) pushStage(size int64, modTime time.Time) {
	ctx := context.Background()
	f, err := os.Open(h.stageFile)
	if err != nil {
		h.log.Error("open stage for push", zap.String("path", h.relPath), zap.Error(err))
		return
	}
	defer f.Close()

	if err := h.backend.Put(ctx, h.relPath, f, size); err != nil {
		h.log.Error("push stage to backend",
			zap.String("path", h.relPath),
			zap.String("tier", h.tierName),
			zap.Error(err),
		)
		return
	}
	os.Remove(h.stageFile) //nolint:errcheck

	if err := h.svc.OnWriteComplete(ctx, h.relPath, h.tierName, size, modTime); err != nil {
		h.log.Error("on write complete after push", zap.String("path", h.relPath), zap.Error(err))
	}
}

func (h *writeHandle) GetAttr(out *gofuse.Attr) gofuse.Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	stat, err := h.f.Stat()
	if err != nil {
		return gofuse.EIO
	}
	out.Size = uint64(stat.Size())
	out.Mode = syscall.S_IFREG | 0o644
	out.Mtime = uint64(stat.ModTime().Unix())
	return gofuse.OK
}
