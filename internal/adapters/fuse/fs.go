// Package fuse implements the FUSE filesystem using hanwen/go-fuse/v2.
// It is a thin translation layer: all file state lives in the MetadataStore
// and all data lives in the backends. The FS node tree is rebuilt from
// metadata on mount, not persisted separately.
//
// Design decisions:
//   - Writes always land on the hot tier via the OS page cache; the FUSE layer
//     wraps a real *os.File on the local backend for zero-copy kernel buffering.
//   - Reads from local tiers use a direct fd; reads from remote tiers stage
//     to a temp file in stageDir (transparent to Frigate/callers).
//   - Directories are purely virtual: synthesised by ListDir from metadata.
//   - Inodes are assigned from a monotonic counter; the mapping is in-memory only.
package fuse

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
)

// TierFS implements pathfs.FileSystem, the higher-level go-fuse interface.
// All path arguments are relative to the mount root.
type TierFS struct {
	pathfs.FileSystem // embed DefaultFileSystem for no-op defaults
	svc               *app.TierService
	meta              domain.MetadataStore
	stager            *app.Stager
	log               *zap.Logger
	reg               *metrics.Registry

	// inodeMap assigns stable inode numbers to relative paths.
	inodeMu    sync.RWMutex
	inodeMap   map[string]uint64
	nextIno    atomic.Uint64
	stageGroup singleflight.Group

	// owner is captured at mount time so GetAttr returns the correct uid/gid
	// for default_permissions enforcement.
	uid uint32
	gid uint32

	// virtualDirs tracks directories created via Mkdir that don't yet have
	// files beneath them. Entries are removed lazily when no longer needed.
	virtualDirsMu sync.RWMutex
	virtualDirs   map[string]struct{}
}

// New creates a TierFS. The returned value should be wrapped with
// pathfs.NewPathNodeFs and mounted via fuse.NewServer.
func New(svc *app.TierService, meta domain.MetadataStore, stager *app.Stager, log *zap.Logger, reg *metrics.Registry) *TierFS {
	fs := &TierFS{
		FileSystem: pathfs.NewDefaultFileSystem(),
		svc:        svc,
		meta:       meta,
		stager:     stager,
		log:        log.Named("fuse"),
		reg:        reg,
		inodeMap:   make(map[string]uint64),
	}
	fs.nextIno.Store(2) // 1 is reserved for root
	fs.uid = uint32(os.Getuid())
	fs.gid = uint32(os.Getgid())
	fs.virtualDirs = make(map[string]struct{})
	return fs
}

// fuseOp records a FUSE operation's outcome and duration in Prometheus metrics.
func (fs *TierFS) fuseOp(op string, status gofuse.Status, start time.Time) {
	if fs.reg == nil {
		return
	}
	outcome := "ok"
	if status != gofuse.OK {
		outcome = "error"
	}
	fs.reg.FuseOps.WithLabelValues(op, outcome).Inc()
	fs.reg.FuseDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}

// ── Directory operations ──────────────────────────────────────────────────────

// GetAttr implements stat(2). Directories are synthetic; files come from metadata.
func (fs *TierFS) GetAttr(name string, ctx *gofuse.Context) (attr *gofuse.Attr, status gofuse.Status) {
	start := time.Now()
	defer func() { fs.fuseOp("GetAttr", status, start) }()
	if name == "" {
		// Root directory.
		return &gofuse.Attr{
			Mode:  gofuse.S_IFDIR | 0o755,
			Ino:   1,
			Nlink: 2,
			Owner: gofuse.Owner{Uid: fs.uid, Gid: fs.gid},
		}, gofuse.OK
	}

	goCtx := context.Background()
	f, err := fs.meta.GetFile(goCtx, name)
	if err == nil {
		// It's a known file.
		return &gofuse.Attr{
			Mode:  gofuse.S_IFREG | 0o644,
			Size:  uint64(f.Size),
			Mtime: uint64(f.ModTime.Unix()),
			Ino:   fs.inode(name),
			Nlink: 1,
			Owner: gofuse.Owner{Uid: fs.uid, Gid: fs.gid},
		}, gofuse.OK
	}

	// Hot-tier fallback: the file may exist on disk but metadata hasn't
	// caught up yet (e.g. FUSE Release/OnWriteComplete is still running
	// asynchronously when ffmpeg re-opens for -movflags +faststart).
	if hotBackend, berr := fs.svc.BackendFor(fs.svc.HottestTierName()); berr == nil {
		if localPath, ok := hotBackend.LocalPath(name); ok {
			if st, serr := os.Stat(localPath); serr == nil && !st.IsDir() {
				return &gofuse.Attr{
					Mode:  gofuse.S_IFREG | 0o644,
					Size:  uint64(st.Size()),
					Mtime: uint64(st.ModTime().Unix()),
					Ino:   fs.inode(name),
					Nlink: 1,
					Owner: gofuse.Owner{Uid: fs.uid, Gid: fs.gid},
				}, gofuse.OK
			}
		}
	}

	// Check if it's a virtual directory (has files below it).
	entries, derr := fs.meta.ListDir(goCtx, name)
	if derr == nil && len(entries) > 0 {
		return &gofuse.Attr{
			Mode:  gofuse.S_IFDIR | 0o755,
			Ino:   fs.inode(name),
			Nlink: 2,
			Owner: gofuse.Owner{Uid: fs.uid, Gid: fs.gid},
		}, gofuse.OK
	}

	// Check explicitly-created directories that don't have files yet.
	fs.virtualDirsMu.RLock()
	_, isVirtual := fs.virtualDirs[name]
	fs.virtualDirsMu.RUnlock()
	if isVirtual {
		return &gofuse.Attr{
			Mode:  gofuse.S_IFDIR | 0o755,
			Ino:   fs.inode(name),
			Nlink: 2,
			Owner: gofuse.Owner{Uid: fs.uid, Gid: fs.gid},
		}, gofuse.OK
	}

	return nil, gofuse.ENOENT
}

// OpenDir returns the immediate children of the directory name.
func (fs *TierFS) OpenDir(name string, ctx *gofuse.Context) (_ []gofuse.DirEntry, status gofuse.Status) {
	start := time.Now()
	defer func() { fs.fuseOp("OpenDir", status, start) }()
	entries, err := fs.meta.ListDir(context.Background(), name)
	if err != nil {
		fs.log.Error("ListDir", zap.String("dir", name), zap.Error(err))
		return nil, gofuse.EIO
	}

	out := make([]gofuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := gofuse.S_IFREG
		if e.IsDir {
			mode = gofuse.S_IFDIR
		}
		out = append(out, gofuse.DirEntry{
			Name: e.Name,
			Ino:  fs.inode(e.RelPath),
			Mode: uint32(mode),
		})
	}
	return out, gofuse.OK
}

// Mkdir creates a virtual directory. TierFS directories are implicit, so
// this is a no-op that returns OK (Frigate creates directories before writing files).
func (fs *TierFS) Mkdir(name string, mode uint32, ctx *gofuse.Context) gofuse.Status {
	fs.log.Debug("mkdir (virtual)", zap.String("name", name))
	fs.virtualDirsMu.Lock()
	fs.virtualDirs[name] = struct{}{}
	fs.virtualDirsMu.Unlock()
	return gofuse.OK
}

// Chmod is a no-op — TierFS does not track per-file permission bits.
func (fs *TierFS) Chmod(name string, mode uint32, ctx *gofuse.Context) gofuse.Status {
	return gofuse.OK
}

// Chown is a no-op — TierFS does not track per-file ownership.
func (fs *TierFS) Chown(name string, uid uint32, gid uint32, ctx *gofuse.Context) gofuse.Status {
	return gofuse.OK
}

// Rmdir removes a virtual directory. Only succeeds if no files exist below it.
func (fs *TierFS) Rmdir(name string, ctx *gofuse.Context) gofuse.Status {
	entries, err := fs.meta.ListDir(context.Background(), name)
	if err != nil {
		return gofuse.EIO
	}
	if len(entries) > 0 {
		return gofuse.Status(syscall.ENOTEMPTY)
	}
	fs.virtualDirsMu.Lock()
	delete(fs.virtualDirs, name)
	fs.virtualDirsMu.Unlock()
	return gofuse.OK
}

// ── File operations ───────────────────────────────────────────────────────────

// Create opens a new file for writing. Data lands on the hot tier.
func (fs *TierFS) Create(name string, flags uint32, mode uint32, ctx *gofuse.Context) (_ nodefs.File, status gofuse.Status) {
	start := time.Now()
	defer func() { fs.fuseOp("Create", status, start) }()
	goCtx := context.Background()
	backend, tierName, err := fs.svc.WriteTarget(name)
	if err != nil {
		fs.log.Error("write target", zap.String("name", name), zap.Error(err))
		return nil, gofuse.EIO
	}

	// For local backends, create the file directly in the backend's directory.
	// This avoids the staging+async-push path so that the file is immediately
	// visible on disk — required for ffmpeg -movflags +faststart which
	// closes then immediately re-opens the output file.
	// We use LocalPath (which works through the observability decorator)
	// instead of the fileCreator interface which the decorator doesn't expose.
	if localPath, ok := backend.LocalPath(name); ok {
		if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
			fs.log.Error("create parent dirs", zap.String("path", localPath), zap.Error(err))
			return nil, gofuse.EIO
		}
		f, ferr := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(mode))
		if ferr != nil {
			fs.log.Error("create file", zap.String("name", name), zap.Error(ferr))
			return nil, gofuse.EIO
		}

		// Record the file in metadata immediately so reads during write work.
		_ = fs.meta.UpsertFile(goCtx, domain.File{
			RelPath:     name,
			CurrentTier: tierName,
			State:       domain.StateWriting,
			ModTime:     time.Now(),
		})

		wh := newWriteHandle(f, name, tierName, fs.svc, fs.log, "", nil)
		return wh, gofuse.OK
	}

	// Remote backend: buffer to stage dir then push on release.
	stagePath := fs.stager.StagePath(name)
	if err := os.MkdirAll(filepath.Dir(stagePath), 0o755); err != nil {
		return nil, gofuse.EIO
	}
	f, ferr := os.OpenFile(stagePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(mode))
	if ferr != nil {
		return nil, gofuse.EIO
	}

	_ = fs.meta.UpsertFile(goCtx, domain.File{
		RelPath:     name,
		CurrentTier: tierName,
		State:       domain.StateWriting,
		ModTime:     time.Now(),
	})

	wh := newWriteHandle(f, name, tierName, fs.svc, fs.log, stagePath, backend)
	return wh, gofuse.OK
}

// isWriteFlags reports whether FUSE open flags indicate write intent.
func isWriteFlags(flags uint32) bool {
	const (
		oWRONLY = 1
		oRDWR   = 2
	)
	accMode := flags & 3
	return accMode == oWRONLY || accMode == oRDWR
}

// Open opens an existing file for reading (or reading+writing).
func (fs *TierFS) Open(name string, flags uint32, ctx *gofuse.Context) (_ nodefs.File, status gofuse.Status) {
	start := time.Now()
	defer func() { fs.fuseOp("Open", status, start) }()
	goCtx := context.Background()
	backend, localPath, fileInfo, err := fs.svc.ReadTarget(goCtx, name)
	if err != nil {
		if errors.Is(err, domain.ErrNotExist) {
			// Hot-tier fallback: file may exist on disk while async
			// Release/OnWriteComplete is still updating metadata
			// (e.g. ffmpeg -movflags +faststart re-open after close).
			if hotBackend, berr := fs.svc.BackendFor(fs.svc.HottestTierName()); berr == nil {
				if hp, ok := hotBackend.LocalPath(name); ok {
					if _, serr := os.Stat(hp); serr == nil {
						backend = hotBackend
						localPath = hp
						fileInfo = domain.File{RelPath: name, CurrentTier: fs.svc.HottestTierName()}
						err = nil
					}
				}
			}
			if err != nil {
				return nil, gofuse.ENOENT
			}
		} else {
			fs.log.Error("read target", zap.String("name", name), zap.Error(err))
			return nil, gofuse.EIO
		}
	}

	// ── Write-open path ───────────────────────────────────────────────────────
	// If the caller is opening for writing and the file is NOT already on the
	// hot tier, promote it first. This handles two sub-cases:
	//
	//   1. file:// cold tier: without promotion, writes go directly to the cold
	//      tier file but OnWriteComplete is never called — metadata becomes
	//      stale and the tiering system loses track of the change.
	//
	//   2. remote cold tier (S3, SMB): without promotion, writes go to stageDir
	//      and are silently discarded on Release — the data never reaches the
	//      backend.
	//
	// Promotion copies the current content to tier0 synchronously, then we open
	// the tier0 copy as a writeHandle. OnWriteComplete fires on Release, updates
	// the digest and size, and re-enqueues replication. The cold-tier copy is
	// left intact and will be cleaned up by normal eviction once tier0 is
	// re-replicated and verified.
	if isWriteFlags(flags) {
		hotBackend, hotPath, perr := fs.svc.PromoteToHot(goCtx, name)
		if perr != nil {
			fs.log.Error("promote for write-open failed",
				zap.String("name", name),
				zap.Error(perr),
			)
			return nil, gofuse.EIO
		}

		// PromoteToHot returns a local path (tier0 is always a file backend).
		// If hotPath is empty the hot tier is somehow non-local — fall through
		// to the remote staging path rather than silently losing the write.
		if hotPath != "" {
			f, ferr := os.OpenFile(hotPath, int(flags)&^os.O_CREATE, 0o644)
			if ferr != nil {
				fs.log.Error("open promoted file", zap.String("name", name), zap.Error(ferr))
				return nil, gofuse.EIO
			}
			hotTier := fs.svc.HottestTierName()
			wh := newWriteHandle(f, name, hotTier, fs.svc, fs.log, "", hotBackend)
			fs.log.Debug("write-open promoted to hot tier",
				zap.String("name", name),
				zap.String("tier", hotTier),
			)
			return wh, gofuse.OK
		}
	}

	// ── Read-only path (and write-open fallback for non-local hot tier) ───────

	// Fast path: local file backend.
	if localPath != "" {
		f, ferr := os.OpenFile(localPath, int(flags)&^os.O_CREATE, 0)
		if ferr != nil {
			if os.IsNotExist(ferr) {
				return nil, gofuse.ENOENT
			}
			return nil, gofuse.EIO
		}
		return nodefs.NewLoopbackFile(f), gofuse.OK
	}

	// Slow path: remote backend — stage locally then serve.
	stagePath := fs.stager.StagePath(name)
	needStage := false
	if _, serr := os.Stat(stagePath); os.IsNotExist(serr) {
		needStage = true
	} else if fs.stager.IsStale(stagePath, fileInfo.Digest, fileInfo.ModTime, fileInfo.Size) {
		fs.log.Debug("stale staged file, re-staging", zap.String("name", name))
		fs.stager.CleanStale(stagePath)
		needStage = true
	}
	if needStage {
		_, serr, _ := fs.stageGroup.Do(name, func() (interface{}, error) {
			return nil, fs.stage(goCtx, backend, name, stagePath, fileInfo)
		})
		if serr != nil {
			fs.log.Error("stage remote file", zap.String("name", name), zap.Error(serr))
			return nil, gofuse.EIO
		}
	}
	f, ferr := os.Open(stagePath)
	if ferr != nil {
		return nil, gofuse.EIO
	}
	return nodefs.NewLoopbackFile(f), gofuse.OK
}

// Unlink deletes a file.
func (fs *TierFS) Unlink(name string, ctx *gofuse.Context) (status gofuse.Status) {
	start := time.Now()
	defer func() { fs.fuseOp("Unlink", status, start) }()
	if err := fs.svc.OnDelete(context.Background(), name); err != nil {
		if errors.Is(err, domain.ErrNotExist) {
			return gofuse.ENOENT
		}
		fs.log.Error("unlink", zap.String("name", name), zap.Error(err))
		return gofuse.EIO
	}
	// Remove any staged copy and its sidecar.
	fs.stager.CleanStale(fs.stager.StagePath(name))
	return gofuse.OK
}

// Rename moves a file to a new path.
func (fs *TierFS) Rename(oldName, newName string, ctx *gofuse.Context) gofuse.Status {
	if err := fs.svc.OnRename(context.Background(), oldName, newName); err != nil {
		if errors.Is(err, domain.ErrNotExist) {
			return gofuse.ENOENT
		}
		fs.log.Error("rename", zap.String("old", oldName), zap.String("new", newName), zap.Error(err))
		return gofuse.EIO
	}
	return gofuse.OK
}

// Utimens sets access and modification times.
func (fs *TierFS) Utimens(name string, atime *time.Time, mtime *time.Time, ctx *gofuse.Context) gofuse.Status {
	goCtx := context.Background()
	f, err := fs.meta.GetFile(goCtx, name)
	if err != nil {
		return gofuse.ENOENT
	}
	if mtime != nil {
		f.ModTime = *mtime
		_ = fs.meta.UpsertFile(goCtx, *f)
	}
	// Propagate to local backend if possible.
	backend, _, _, berr := fs.svc.ReadTarget(goCtx, name)
	if berr == nil {
		if u, ok := backend.(domain.Utimer); ok && mtime != nil {
			at := time.Now()
			if atime != nil {
				at = *atime
			}
			_ = u.Utimes(name, at, *mtime)
		}
	}
	return gofuse.OK
}

// StatFs reports filesystem capacity. Returns the hot tier's underlying
// filesystem stats so that callers like Frigate's storage_maintainer can
// check free space with shutil.disk_usage / statvfs.
func (fs *TierFS) StatFs(name string) *gofuse.StatfsOut {
	hotBackend, err := fs.svc.BackendFor(fs.svc.HottestTierName())
	if err != nil {
		return nil
	}
	// Use the backend's root directory for statvfs.
	localPath, ok := hotBackend.LocalPath("")
	if !ok {
		return nil
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(localPath, &st); err != nil {
		return nil
	}
	out := &gofuse.StatfsOut{}
	out.Blocks = st.Blocks
	out.Bfree = st.Bfree
	out.Bavail = st.Bavail
	out.Files = st.Files
	out.Ffree = st.Ffree
	out.Bsize = uint32(st.Bsize)
	out.NameLen = uint32(st.Namelen)
	out.Frsize = uint32(st.Frsize)
	return out
}

// Truncate sets a file's size. Routes through the post-write path to
// recompute the digest, reset state to local, and enqueue replication.
func (fs *TierFS) Truncate(name string, size uint64, ctx *gofuse.Context) gofuse.Status {
	goCtx := context.Background()

	// Promote to hot tier if needed (same as write-open path).
	_, localPath, err := fs.svc.PromoteToHot(goCtx, name)
	if err != nil {
		if errors.Is(err, domain.ErrNotExist) {
			return gofuse.ENOENT
		}
		fs.log.Error("promote for truncate failed",
			zap.String("name", name), zap.Error(err))
		return gofuse.EIO
	}
	if localPath == "" {
		return gofuse.EACCES
	}

	if terr := os.Truncate(localPath, int64(size)); terr != nil {
		fs.log.Error("truncate local file", zap.String("path", localPath), zap.Error(terr))
		return gofuse.EIO
	}

	tierName := fs.svc.HottestTierName()
	if werr := fs.svc.OnWriteComplete(goCtx, name, tierName, int64(size), time.Now()); werr != nil {
		fs.log.Error("on write complete after truncate",
			zap.String("name", name), zap.Error(werr))
		return gofuse.EIO
	}

	return gofuse.OK
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (fs *TierFS) inode(relPath string) uint64 {
	fs.inodeMu.RLock()
	if ino, ok := fs.inodeMap[relPath]; ok {
		fs.inodeMu.RUnlock()
		return ino
	}
	fs.inodeMu.RUnlock()

	fs.inodeMu.Lock()
	defer fs.inodeMu.Unlock()
	if ino, ok := fs.inodeMap[relPath]; ok {
		return ino
	}
	ino := fs.nextIno.Add(1)
	fs.inodeMap[relPath] = ino
	return ino
}

func (fs *TierFS) stage(ctx context.Context, backend domain.Backend, relPath, stagePath string, fileInfo domain.File) error {
	rc, _, err := backend.Get(ctx, relPath)
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := os.MkdirAll(filepath.Dir(stagePath), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(stagePath), ".stage-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()

	buf := make([]byte, 4*1024*1024)
	if _, err := copyBuf(tmp, rc, buf); err != nil {
		return err
	}
	tmp.Sync() //nolint:errcheck
	tmp.Close()
	if err := os.Rename(tmpName, stagePath); err != nil {
		return err
	}
	if werr := fs.stager.WriteMeta(stagePath, app.StageMeta{
		Digest:  fileInfo.Digest,
		ModTime: fileInfo.ModTime,
		Size:    fileInfo.Size,
	}); werr != nil {
		fs.log.Warn("write stage meta", zap.String("path", relPath), zap.Error(werr))
	}
	return nil
}

func copyBuf(dst *os.File, src interface{ Read([]byte) (int, error) }, buf []byte) (int64, error) {
	var total int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}
	}
}
