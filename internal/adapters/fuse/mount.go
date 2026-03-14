package fuse

import (
	"fmt"
	"os"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.uber.org/zap"
)

// CacheConfig holds FUSE kernel cache TTLs.
type CacheConfig struct {
	EntryTimeout    time.Duration
	AttrTimeout     time.Duration
	NegativeTimeout time.Duration
}

// Mount mounts the TierFS at mountPath and returns the running server.
// The caller is responsible for calling server.Unmount() on shutdown.
func Mount(fs *TierFS, mountPath string, cache CacheConfig, log *zap.Logger) (*gofuse.Server, error) {
	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		return nil, fmt.Errorf("fuse: create mount dir %q: %w", mountPath, err)
	}

	nfs := pathfs.NewPathNodeFs(fs, &pathfs.PathNodeFsOptions{
		ClientInodes: true,
	})
	conn := nodefs.NewFileSystemConnector(nfs.Root(), &nodefs.Options{
		EntryTimeout:    cache.EntryTimeout,
		AttrTimeout:     cache.AttrTimeout,
		NegativeTimeout: cache.NegativeTimeout,
	})

	opts := &gofuse.MountOptions{
		Name:          "tierfs",
		FsName:        "tierfs",
		MaxBackground: 64,
		MaxWrite:      1 << 20,
		Debug:         false,
		Options:       []string{"default_permissions"},
	}

	server, err := gofuse.NewServer(conn.RawFS(), mountPath, opts)
	if err != nil {
		return nil, fmt.Errorf("fuse: mount %q: %w", mountPath, err)
	}

	log.Info("FUSE filesystem mounted",
		zap.String("path", mountPath),
		zap.Duration("entry_timeout", cache.EntryTimeout),
		zap.Duration("attr_timeout", cache.AttrTimeout),
	)
	return server, nil
}
