// Package smb implements a domain.Backend for SMB2/SMB3 file shares using a
// pure-Go SMB client. No kernel mount, no root privileges, no CIFS module.
//
// # URI format
//
//	smb://[user[:password]@]host[:port]/share[/optional/prefix]
//
// Examples:
//
//	smb://nas.lan/recordings
//	smb://admin:secret@192.168.1.10/cctv/frigate
//	smb://nas.lan:445/data
//
// Credentials may be omitted from the URI and supplied via environment
// variables or config fields instead (see [Config]).
//
// # Connection model
//
// Each Backend holds a single authenticated SMB2 session and share handle,
// protected by a sync.RWMutex. If any operation detects a dropped connection
// (ErrClosed, io.EOF from the transport, or net.Error.Temporary()), the
// backend reconnects once and retries the operation. This handles NAS reboots
// and switch failovers transparently. Concurrent reads are multiplexed over
// the single connection; SMB2 supports request compounding natively.
//
// # Atomic writes
//
// Put writes data to a hidden temp file (<path>.tierfs-tmp-<random>) then
// renames it into place. This prevents readers from seeing partial writes.
// The temp file is cleaned up on rename failure.
package smb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/hirochachacha/go-smb2"
	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/domain"
)

const (
	defaultPort        = "445"
	dialTimeout        = 30 * time.Second
	// maxReconnectDelay between reconnection attempts on fatal transport errors.
	maxReconnectDelay  = 5 * time.Second
)

// Config holds the configuration for an SMB backend.
// Credentials are resolved in this order: env vars > config fields > URI userinfo.
type Config struct {
	// Name is the tier name, used in log messages.
	Name string

	// URI is the SMB share URI: smb://[user[:pass]@]host[:port]/share[/prefix]
	URI string

	// Username for NTLM authentication.
	// If empty, falls back to the URI userinfo or TIERFS_SMB_USER env var.
	Username string

	// Password for NTLM authentication.
	// If empty, falls back to the URI userinfo or TIERFS_SMB_PASS env var.
	Password string

	// Domain is the Windows/Active Directory domain for NTLM auth. Usually empty
	// for workgroup NAS devices (Synology, TrueNAS, QNAP). Required for AD joins.
	Domain string

	// RequireEncryption requests SMB3 encryption on the session. Requires the
	// server to support SMB 3.0+ (most NAS firmware from 2015+ qualifies).
	// Default: false (signing only, which SMB2 provides by default).
	RequireEncryption bool
}

// Parsed holds the parts extracted from Config.URI.
type parsed struct {
	host      string // host:port
	shareName string
	prefix    string // optional prefix within the share
}

// Backend is a domain.Backend backed by an SMB2/SMB3 share.
// All exported methods are safe for concurrent use.
type Backend struct {
	cfg    Config
	p      parsedURI
	log    *zap.Logger

	mu      sync.RWMutex
	share   *smb2.Share   // nil when not connected
	session *smb2.Session // held so we can Close() on reconnect
	conn    net.Conn
}

// New creates an SMB Backend from cfg and establishes the initial connection.
func New(cfg Config, log *zap.Logger) (*Backend, error) {
	p, err := parseURI(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("smb backend %q: parse URI: %w", cfg.Name, err)
	}

	// Resolve credentials: env > config > URI.
	cfg.Username = resolveCredential(cfg.Username, "TIERFS_SMB_USER", p.uriUsername)
	cfg.Password = resolveCredential(cfg.Password, "TIERFS_SMB_PASS", p.uriPassword)

	b := &Backend{
		cfg: cfg,
		p:   *p,
		log: log.Named("smb").With(
			zap.String("tier", cfg.Name),
			zap.String("host", p.host),
			zap.String("share", p.shareName),
		),
	}

	if err := b.connect(); err != nil {
		return nil, fmt.Errorf("smb backend %q: initial connect: %w", cfg.Name, err)
	}

	b.log.Info("SMB backend connected",
		zap.String("prefix", b.p.prefix),
		zap.Bool("encryption", cfg.RequireEncryption),
	)
	return b, nil
}

// ParseURI extracts the Config fields embedded in an SMB URI for use by main.go.
func ParseURI(uri string) (host, share, prefix, uriUser, uriPass string, err error) {
	p, err := parseURI(uri)
	if err != nil {
		return "", "", "", "", "", err
	}
	return p.host, p.shareName, p.prefix, p.uriUsername, p.uriPassword, nil
}

// Scheme returns "smb".
func (b *Backend) Scheme() string { return "smb" }

// URI returns the display URI for relPath, without credentials.
func (b *Backend) URI(relPath string) string {
	host := b.p.host
	if !strings.Contains(host, ":") || strings.HasSuffix(host, ":"+defaultPort) {
		host = strings.TrimSuffix(host, ":"+defaultPort)
	}
	full := path.Join(b.p.shareName, b.p.prefix, relPath)
	return "smb://" + host + "/" + full
}

// LocalPath always returns ("", false) — SMB files cannot be directly memory-mapped.
func (b *Backend) LocalPath(_ string) (string, bool) { return "", false }

// Put writes r to the share at relPath, using atomic temp-file + rename.
// If the destination directory does not exist it is created recursively.
func (b *Backend) Put(ctx context.Context, relPath string, r io.Reader, size int64) error {
	sharePath := b.sharePath(relPath)
	tmpPath := sharePath + ".tierfs-tmp-" + randHex(8)

	return b.withReconnect(ctx, func(share *smb2.Share) error {
		// Ensure parent directory exists.
		dir := path.Dir(sharePath)
		if err := mkdirAll(share, dir); err != nil {
			return fmt.Errorf("mkdirAll %q: %w", dir, err)
		}

		// Write to temp file.
		f, err := share.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("create temp %q: %w", tmpPath, err)
		}

		_, copyErr := io.Copy(f, r)
		closeErr := f.Close()
		if copyErr != nil {
			share.Remove(tmpPath) //nolint:errcheck
			return fmt.Errorf("write data: %w", copyErr)
		}
		if closeErr != nil {
			share.Remove(tmpPath) //nolint:errcheck
			return fmt.Errorf("close temp file: %w", closeErr)
		}

		// Atomic rename into final path.
		// SMB2 Rename is atomic within the same share.
		if err := share.Rename(tmpPath, sharePath); err != nil {
			share.Remove(tmpPath) //nolint:errcheck
			return fmt.Errorf("rename %q → %q: %w", tmpPath, sharePath, err)
		}
		return nil
	})
}

// Get returns a reader for the file at relPath.
// The caller must close the returned ReadCloser.
func (b *Backend) Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error) {
	sharePath := b.sharePath(relPath)

	var rc io.ReadCloser
	var size int64
	err := b.withReconnect(ctx, func(share *smb2.Share) error {
		f, err := share.Open(sharePath)
		if err != nil {
			if isNotExist(err) {
				return domain.ErrNotExist
			}
			return fmt.Errorf("open %q: %w", sharePath, err)
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return fmt.Errorf("stat %q: %w", sharePath, err)
		}
		rc = f
		size = info.Size()
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return rc, size, nil
}

// Stat returns metadata for the file at relPath.
func (b *Backend) Stat(ctx context.Context, relPath string) (*domain.FileInfo, error) {
	sharePath := b.sharePath(relPath)
	var fi *domain.FileInfo
	err := b.withReconnect(ctx, func(share *smb2.Share) error {
		info, err := share.Stat(sharePath)
		if err != nil {
			if isNotExist(err) {
				return domain.ErrNotExist
			}
			return fmt.Errorf("stat %q: %w", sharePath, err)
		}
		fi = &domain.FileInfo{
			RelPath: relPath,
			Name:    path.Base(relPath),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		}
		return nil
	})
	return fi, err
}

// Delete removes the file at relPath from the share.
// Returns nil if the file does not exist.
func (b *Backend) Delete(ctx context.Context, relPath string) error {
	sharePath := b.sharePath(relPath)
	return b.withReconnect(ctx, func(share *smb2.Share) error {
		err := share.Remove(sharePath)
		if err != nil && !isNotExist(err) {
			return fmt.Errorf("remove %q: %w", sharePath, err)
		}
		// Best-effort: prune empty parent directories.
		pruneEmptyDirs(share, path.Dir(sharePath), b.p.prefix)
		return nil
	})
}

// List returns all files recursively under prefix on the share.
func (b *Backend) List(ctx context.Context, prefix string) ([]domain.FileInfo, error) {
	sharePath := b.sharePath(prefix)
	var out []domain.FileInfo
	err := b.withReconnect(ctx, func(share *smb2.Share) error {
		out = nil
		return walkShare(share, sharePath, b.p.prefix, func(fi domain.FileInfo) {
			if !fi.IsDir {
				out = append(out, fi)
			}
		})
	})
	return out, err
}

// Rename moves oldRel to newRel on the share. Implements the optional Rename
// extension used by TierService.OnRename().
func (b *Backend) Rename(ctx context.Context, oldRel, newRel string) error {
	oldPath := b.sharePath(oldRel)
	newPath := b.sharePath(newRel)
	return b.withReconnect(ctx, func(share *smb2.Share) error {
		dir := path.Dir(newPath)
		if err := mkdirAll(share, dir); err != nil {
			return fmt.Errorf("mkdirAll %q: %w", dir, err)
		}
		if err := share.Rename(oldPath, newPath); err != nil {
			return fmt.Errorf("rename %q → %q: %w", oldPath, newPath, err)
		}
		pruneEmptyDirs(share, path.Dir(oldPath), b.p.prefix)
		return nil
	})
}

// Close disconnects from the SMB share and session cleanly.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.disconnect()
}

// ── Connection management ─────────────────────────────────────────────────────

// connect establishes a new TCP connection, SMB session, and share mount.
// Must be called with b.mu held for writing, or before the backend is shared.
func (b *Backend) connect() error {
	// Dial TCP with timeout.
	conn, err := net.DialTimeout("tcp", b.p.host, dialTimeout)
	if err != nil {
		return fmt.Errorf("tcp dial %q: %w", b.p.host, err)
	}

	initiator := &smb2.NTLMInitiator{
		User:     b.cfg.Username,
		Password: b.cfg.Password,
		Domain:   b.cfg.Domain,
	}

	d := &smb2.Dialer{Initiator: initiator}
	if b.cfg.RequireEncryption {
		d.Negotiator.RequireMessageSigning = true
	}

	session, err := d.Dial(conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("smb2 dial: %w", err)
	}

	share, err := session.Mount(b.p.shareName)
	if err != nil {
		session.Logoff() //nolint:errcheck
		conn.Close()
		return fmt.Errorf("mount share %q: %w", b.p.shareName, err)
	}

	b.conn = conn
	b.session = session
	b.share = share
	return nil
}

// disconnect closes the share, session, and TCP connection.
// Must be called with b.mu held for writing.
func (b *Backend) disconnect() error {
	var first error
	if b.share != nil {
		if err := b.share.Umount(); err != nil && first == nil {
			first = err
		}
		b.share = nil
	}
	if b.session != nil {
		if err := b.session.Logoff(); err != nil && first == nil {
			first = err
		}
		b.session = nil
	}
	if b.conn != nil {
		if err := b.conn.Close(); err != nil && first == nil {
			first = err
		}
		b.conn = nil
	}
	return first
}

// withReconnect runs fn with the current share handle. If fn returns a
// transport error, it reconnects once and retries fn. This handles temporary
// NAS unavailability (reboots, failover) transparently.
func (b *Backend) withReconnect(ctx context.Context, fn func(*smb2.Share) error) error {
	// Fast path: try with existing share under read lock.
	b.mu.RLock()
	share := b.share
	b.mu.RUnlock()

	if share != nil {
		err := fn(share)
		if err == nil || !isTransportError(err) {
			return err
		}
		b.log.Warn("SMB transport error, attempting reconnect", zap.Error(err))
	}

	// Slow path: reconnect under write lock then retry once.
	b.mu.Lock()
	defer b.mu.Unlock()

	// Another goroutine may have already reconnected.
	if b.share != nil {
		if err := fn(b.share); err == nil || !isTransportError(err) {
			return err
		}
	}

	// Disconnect whatever remains.
	b.disconnect() //nolint:errcheck

	// Brief pause before reconnect to avoid tight loops on hard failures.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
	}

	b.log.Info("reconnecting to SMB share")
	if err := b.connect(); err != nil {
		return fmt.Errorf("smb reconnect failed: %w", err)
	}
	b.log.Info("SMB share reconnected")
	return fn(b.share)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// sharePath joins the share prefix with relPath to produce the path as seen
// by the SMB share root. Always uses forward slashes.
func (b *Backend) sharePath(relPath string) string {
	if b.p.prefix == "" {
		return relPath
	}
	return path.Join(b.p.prefix, relPath)
}

// mkdirAll creates the full directory tree on the share, ignoring
// "already exists" errors at each step.
func mkdirAll(share *smb2.Share, dirPath string) error {
	if dirPath == "" || dirPath == "." || dirPath == "/" {
		return nil
	}
	// Create from root down.
	parts := strings.Split(strings.Trim(dirPath, "/"), "/")
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		if current == "" {
			current = part
		} else {
			current = current + "/" + part
		}
		err := share.Mkdir(current, 0o755)
		if err != nil && !isExist(err) {
			return fmt.Errorf("mkdir %q: %w", current, err)
		}
	}
	return nil
}

// pruneEmptyDirs walks up from dirPath toward stopAt, removing empty
// directories. Stops when a non-empty directory or stopAt is reached.
func pruneEmptyDirs(share *smb2.Share, dirPath, stopAt string) {
	for {
		if dirPath == "" || dirPath == "." || dirPath == stopAt || dirPath == "/" {
			return
		}
		entries, err := share.ReadDir(dirPath)
		if err != nil || len(entries) > 0 {
			return
		}
		if err := share.Remove(dirPath); err != nil {
			return
		}
		dirPath = path.Dir(dirPath)
	}
}

// walkShare recursively visits all entries under root, calling fn for each.
// The relPath in FileInfo is relative to the share prefix, not the share root.
func walkShare(share *smb2.Share, root, prefix string, fn func(domain.FileInfo)) error {
	entries, err := share.ReadDir(root)
	if err != nil {
		if isNotExist(err) {
			return nil
		}
		return fmt.Errorf("readdir %q: %w", root, err)
	}
	for _, e := range entries {
		fullPath := path.Join(root, e.Name())
		relPath := strings.TrimPrefix(fullPath, prefix)
		relPath = strings.TrimPrefix(relPath, "/")
		fi := domain.FileInfo{
			RelPath: relPath,
			Name:    e.Name(),
			Size:    e.Size(),
			ModTime: e.ModTime(),
			IsDir:   e.IsDir(),
		}
		fn(fi)
		if e.IsDir() {
			if err := walkShare(share, fullPath, prefix, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// isNotExist reports whether err is a "file not found" error from the SMB layer
// or the OS layer.
func isNotExist(err error) bool {
	if err == nil {
		return false
	}
	if os.IsNotExist(err) {
		return true
	}
	// go-smb2 wraps STATUS_OBJECT_NAME_NOT_FOUND and STATUS_NO_SUCH_FILE
	// as *smb2.ResponseError; check the string as a fallback.
	errStr := err.Error()
	return strings.Contains(errStr, "STATUS_OBJECT_NAME_NOT_FOUND") ||
		strings.Contains(errStr, "STATUS_NO_SUCH_FILE") ||
		strings.Contains(errStr, "STATUS_OBJECT_PATH_NOT_FOUND") ||
		strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "does not exist")
}

// isExist reports whether err indicates the path already exists.
func isExist(err error) bool {
	if err == nil {
		return false
	}
	if os.IsExist(err) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "STATUS_OBJECT_NAME_COLLISION") ||
		strings.Contains(errStr, "already exists")
}

// isTransportError reports whether err is a network/session-level error
// (as opposed to a file-level error like not-found) that warrants reconnection.
func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "STATUS_CONNECTION_DISCONNECTED") ||
		strings.Contains(errStr, "STATUS_CONNECTION_RESET") ||
		strings.Contains(errStr, "STATUS_NETWORK_NAME_DELETED")
}

// randHex returns n random hex characters.
func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)[:n]
}

// ── URI parsing ───────────────────────────────────────────────────────────────

type parsedURI struct {
	host        string
	shareName   string
	prefix      string
	uriUsername string
	uriPassword string
}

// parseURI decodes smb://[user[:pass]@]host[:port]/share[/prefix...]
func parseURI(rawURI string) (*parsedURI, error) {
	u, err := url.Parse(rawURI)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", rawURI, err)
	}
	if u.Scheme != "smb" {
		return nil, fmt.Errorf("expected smb:// URI, got %q", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("URI %q: missing host", rawURI)
	}
	port := u.Port()
	if port == "" {
		port = defaultPort
	}
	hostPort := net.JoinHostPort(host, port)

	// Path: /share[/prefix...]
	trimmed := strings.TrimPrefix(u.Path, "/")
	if trimmed == "" {
		return nil, fmt.Errorf("URI %q: missing share name", rawURI)
	}
	parts := strings.SplitN(trimmed, "/", 2)
	shareName := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = parts[1]
	}

	var uriUser, uriPass string
	if u.User != nil {
		uriUser = u.User.Username()
		uriPass, _ = u.User.Password()
	}

	return &parsedURI{
		host:        hostPort,
		shareName:   shareName,
		prefix:      prefix,
		uriUsername: uriUser,
		uriPassword: uriPass,
	}, nil
}

// resolveCredential returns the first non-empty value from: configVal, env var, fallback.
func resolveCredential(configVal, envKey, fallback string) string {
	if configVal != "" {
		return configVal
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return fallback
}
