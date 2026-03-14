package fuse

import (
	"fmt"
	"os"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.uber.org/zap"
)

// Mount mounts the TierFS at mountPath and returns the running server.
// The caller is responsible for calling server.Unmount() on shutdown.
func Mount(fs *TierFS, mountPath string, log *zap.Logger) (*gofuse.Server, error) {
	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		return nil, fmt.Errorf("fuse: create mount dir %q: %w", mountPath, err)
	}

	nfs := pathfs.NewPathNodeFs(fs, &pathfs.PathNodeFsOptions{
		ClientInodes: true,
	})
	conn := nodefs.NewFileSystemConnector(nfs.Root(), &nodefs.Options{
		EntryTimeout:    0, // no kernel attribute caching - metadata always fresh
		AttrTimeout:     0,
		NegativeTimeout: 0,
	})

	opts := &gofuse.MountOptions{
		Name:          "tierfs",
		FsName:        "tierfs",
		MaxBackground: 64,     // concurrent FUSE requests in flight
		MaxWrite:      1 << 20, // 1 MiB max write size
		Debug:         false,
		Options:       []string{"default_permissions"},
	}

	server, err := gofuse.NewServer(conn.RawFS(), mountPath, opts)
	if err != nil {
		return nil, fmt.Errorf("fuse: mount %q: %w", mountPath, err)
	}

	log.Info("FUSE filesystem mounted", zap.String("path", mountPath))
	return server, nil
}
