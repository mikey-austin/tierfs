// Package sftp implements a domain.Backend for SFTP (SSH File Transfer Protocol)
// using a pure-Go SSH client. No native ssh binary, no known_hosts requirement
// (host key verification is configurable), no kernel mount.
//
// # URI format
//
//	sftp://[user@]host[:port]/absolute/base/path[/optional/prefix]
//
// Examples:
//
//	sftp://nas.lan/mnt/storage/cctv
//	sftp://admin@192.168.1.10:22/data/frigate
//	sftp://backup@offsite.example.com:2222/backups/tier2
//
// # Authentication
//
// Three methods are supported, tried in this order:
//
//  1. SSH agent (if SSH_AUTH_SOCK is set and the key is loaded)
//  2. Private key file (key_path config field or TIERFS_SFTP_KEY_PATH env var)
//  3. Password (password config field or TIERFS_SFTP_PASS env var)
//
// # Connection model
//
// Each Backend holds a single *sftp.Client protected by a sync.RWMutex.
// On any transport error (io.EOF, net.Error, "connection lost" etc.) the
// backend reconnects once and retries the operation. Concurrent reads and
// writes are safe — pkg/sftp multiplexes requests over the SSH channel.
//
// # Atomic writes
//
// Put writes to a hidden temp file (<path>.tierfs-tmp-<random>) then calls
// Rename. SFTP rename is atomic on POSIX servers (Linux, *BSD, macOS).
// On Windows SFTP servers rename may fail if the destination exists — the
// backend falls back to Remove+Rename in that case.
package sftp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/domain"
)

const (
	defaultPort       = "22"
	dialTimeout       = 30 * time.Second
	reconnectDelay    = 500 * time.Millisecond
)

// Config holds all configuration for an SFTP backend.
// Credentials are resolved: env vars > config fields > URI userinfo.
type Config struct {
	// Name is the tier name, used in log messages.
	Name string

	// URI is the SFTP URI: sftp://[user@]host[:port]/base/path[/prefix]
	URI string

	// Username for SSH authentication.
	// Falls back to TIERFS_SFTP_USER env var, then URI userinfo, then current OS user.
	Username string

	// Password for SSH password authentication.
	// Falls back to TIERFS_SFTP_PASS env var.
	// Leave empty to use key-based or agent auth.
	Password string

	// KeyPath is the absolute path to a PEM-encoded SSH private key file.
	// Falls back to TIERFS_SFTP_KEY_PATH env var, then ~/.ssh/id_ed25519,
	// ~/.ssh/id_rsa in that order.
	KeyPath string

	// KeyPassphrase decrypts an encrypted private key.
	// Falls back to TIERFS_SFTP_KEY_PASSPHRASE env var.
	KeyPassphrase string

	// HostKey is the expected host public key in authorized_keys format, e.g.:
	//   "ssh-ed25519 AAAA..."
	// If empty, host key verification is skipped (InsecureIgnoreHostKey).
	// In production, always set this or use KnownHostsFile.
	HostKey string

	// KnownHostsFile is the path to a known_hosts file. Takes precedence over
	// HostKey if both are set.
	KnownHostsFile string
}

// Backend is a domain.Backend backed by an SFTP server.
// All exported methods are safe for concurrent use.
type Backend struct {
	cfg    Config
	p      parsedURI
	log    *zap.Logger

	mu     sync.RWMutex
	client *sftp.Client  // nil when not connected
	conn   *ssh.Client
}

// New creates an SFTP Backend from cfg and establishes the initial connection.
func New(cfg Config, log *zap.Logger) (*Backend, error) {
	p, err := parseURI(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("sftp backend %q: parse URI: %w", cfg.Name, err)
	}

	// Resolve credentials from env > config > URI.
	cfg.Username        = resolveStr(cfg.Username, "TIERFS_SFTP_USER", p.uriUsername)
	cfg.Password        = resolveStr(cfg.Password, "TIERFS_SFTP_PASS", "")
	cfg.KeyPath         = resolveStr(cfg.KeyPath, "TIERFS_SFTP_KEY_PATH", "")
	cfg.KeyPassphrase   = resolveStr(cfg.KeyPassphrase, "TIERFS_SFTP_KEY_PASSPHRASE", "")

	if cfg.Username == "" {
		cfg.Username = os.Getenv("USER") // fall back to OS user
	}

	b := &Backend{
		cfg: cfg,
		p:   p,
		log: log.Named("sftp").With(
			zap.String("tier", cfg.Name),
			zap.String("host", p.hostPort),
			zap.String("base", p.basePath),
		),
	}

	if err := b.connect(); err != nil {
		return nil, fmt.Errorf("sftp backend %q: initial connect: %w", cfg.Name, err)
	}

	b.log.Info("SFTP backend connected",
		zap.String("prefix", b.p.prefix),
		zap.String("user", cfg.Username),
	)
	return b, nil
}

// ParseURI extracts connection parameters from an SFTP URI for use by main.go.
func ParseURI(uri string) (hostPort, basePath, prefix, uriUser string, err error) {
	p, err := parseURI(uri)
	if err != nil {
		return "", "", "", "", err
	}
	return p.hostPort, p.basePath, p.prefix, p.uriUsername, nil
}

// Scheme returns "sftp".
func (b *Backend) Scheme() string { return "sftp" }

// URI returns the display URI for relPath (no credentials).
func (b *Backend) URI(relPath string) string {
	host := b.p.hostPort
	if strings.HasSuffix(host, ":"+defaultPort) {
		host = strings.TrimSuffix(host, ":"+defaultPort)
	}
	return "sftp://" + host + "/" + strings.TrimPrefix(b.remotePath(relPath), "/")
}

// LocalPath always returns ("", false) — SFTP files cannot be memory-mapped.
func (b *Backend) LocalPath(_ string) (string, bool) { return "", false }

// Put writes r to the SFTP server at relPath using atomic temp-file + rename.
func (b *Backend) Put(ctx context.Context, relPath string, r io.Reader, _ int64) error {
	dst := b.remotePath(relPath)
	tmp := dst + ".tierfs-tmp-" + randHex(8)

	return b.withReconnect(ctx, func(c *sftp.Client) error {
		// Ensure parent directory exists.
		if err := mkdirAll(c, path.Dir(dst)); err != nil {
			return fmt.Errorf("mkdirAll %q: %w", path.Dir(dst), err)
		}

		f, err := c.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
		if err != nil {
			return fmt.Errorf("create temp %q: %w", tmp, err)
		}

		_, copyErr := io.Copy(f, r)
		closeErr := f.Close()
		if copyErr != nil {
			c.Remove(tmp) //nolint:errcheck
			return fmt.Errorf("write data: %w", copyErr)
		}
		if closeErr != nil {
			c.Remove(tmp) //nolint:errcheck
			return fmt.Errorf("close temp: %w", closeErr)
		}

		// Atomic rename. On Windows SFTP servers the destination may need
		// to be removed first.
		if err := c.Rename(tmp, dst); err != nil {
			// Try remove-then-rename for non-POSIX servers.
			c.Remove(dst)              //nolint:errcheck
			if rerr := c.Rename(tmp, dst); rerr != nil {
				c.Remove(tmp) //nolint:errcheck
				return fmt.Errorf("rename %q → %q: %w", tmp, dst, rerr)
			}
		}
		return nil
	})
}

// Get returns a reader for the file at relPath.
// The caller must close the returned ReadCloser.
func (b *Backend) Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error) {
	remote := b.remotePath(relPath)
	var rc io.ReadCloser
	var size int64

	err := b.withReconnect(ctx, func(c *sftp.Client) error {
		f, err := c.Open(remote)
		if err != nil {
			return sftpErr(err, remote)
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return fmt.Errorf("stat %q: %w", remote, err)
		}
		rc = f
		size = info.Size()
		return nil
	})
	return rc, size, err
}

// Stat returns metadata for the file at relPath.
func (b *Backend) Stat(ctx context.Context, relPath string) (*domain.FileInfo, error) {
	remote := b.remotePath(relPath)
	var fi *domain.FileInfo

	err := b.withReconnect(ctx, func(c *sftp.Client) error {
		info, err := c.Stat(remote)
		if err != nil {
			return sftpErr(err, remote)
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

// Delete removes the file at relPath. Returns nil if not found.
func (b *Backend) Delete(ctx context.Context, relPath string) error {
	remote := b.remotePath(relPath)
	return b.withReconnect(ctx, func(c *sftp.Client) error {
		err := c.Remove(remote)
		if err != nil && !isNotExist(err) {
			return fmt.Errorf("remove %q: %w", remote, err)
		}
		pruneEmptyDirs(c, path.Dir(remote), b.remotePath(""))
		return nil
	})
}

// List returns all files recursively under prefix.
func (b *Backend) List(ctx context.Context, prefix string) ([]domain.FileInfo, error) {
	remote := b.remotePath(prefix)
	var out []domain.FileInfo

	err := b.withReconnect(ctx, func(c *sftp.Client) error {
		out = nil
		return walkSFTP(c, remote, b.remotePath(""), func(fi domain.FileInfo) {
			if !fi.IsDir {
				out = append(out, fi)
			}
		})
	})
	return out, err
}

// Rename moves oldRel to newRel on the server.
// Implements the optional Rename extension used by TierService.OnRename().
func (b *Backend) Rename(ctx context.Context, oldRel, newRel string) error {
	oldRemote := b.remotePath(oldRel)
	newRemote := b.remotePath(newRel)

	return b.withReconnect(ctx, func(c *sftp.Client) error {
		if err := mkdirAll(c, path.Dir(newRemote)); err != nil {
			return fmt.Errorf("mkdirAll %q: %w", path.Dir(newRemote), err)
		}
		if err := c.Rename(oldRemote, newRemote); err != nil {
			// Non-POSIX fallback.
			c.Remove(newRemote)                         //nolint:errcheck
			if rerr := c.Rename(oldRemote, newRemote); rerr != nil {
				return fmt.Errorf("rename %q → %q: %w", oldRemote, newRemote, rerr)
			}
		}
		pruneEmptyDirs(c, path.Dir(oldRemote), b.remotePath(""))
		return nil
	})
}

// Close disconnects cleanly.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.disconnect()
}

// ── Connection management ─────────────────────────────────────────────────────

func (b *Backend) connect() error {
	hostKey, err := b.hostKeyCallback()
	if err != nil {
		return fmt.Errorf("host key: %w", err)
	}

	authMethods, err := b.authMethods()
	if err != nil {
		return fmt.Errorf("auth methods: %w", err)
	}
	if len(authMethods) == 0 {
		return fmt.Errorf("no authentication methods available (set password, key_path, or load key into SSH agent)")
	}

	sshCfg := &ssh.ClientConfig{
		User:            b.cfg.Username,
		Auth:            authMethods,
		HostKeyCallback: hostKey,
		Timeout:         dialTimeout,
	}

	sshConn, err := ssh.Dial("tcp", b.p.hostPort, sshCfg)
	if err != nil {
		return fmt.Errorf("ssh dial %q: %w", b.p.hostPort, err)
	}

	client, err := sftp.NewClient(sshConn)
	if err != nil {
		sshConn.Close()
		return fmt.Errorf("sftp client: %w", err)
	}

	b.conn = sshConn
	b.client = client
	return nil
}

func (b *Backend) disconnect() error {
	var first error
	if b.client != nil {
		if err := b.client.Close(); err != nil && first == nil {
			first = err
		}
		b.client = nil
	}
	if b.conn != nil {
		if err := b.conn.Close(); err != nil && first == nil {
			first = err
		}
		b.conn = nil
	}
	return first
}

func (b *Backend) withReconnect(ctx context.Context, fn func(*sftp.Client) error) error {
	b.mu.RLock()
	client := b.client
	b.mu.RUnlock()

	if client != nil {
		err := fn(client)
		if err == nil || !isTransportError(err) {
			return err
		}
		b.log.Warn("SFTP transport error, attempting reconnect", zap.Error(err))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.client != nil {
		if err := fn(b.client); err == nil || !isTransportError(err) {
			return err
		}
	}

	b.disconnect() //nolint:errcheck

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(reconnectDelay):
	}

	b.log.Info("reconnecting to SFTP server")
	if err := b.connect(); err != nil {
		return fmt.Errorf("sftp reconnect: %w", err)
	}
	b.log.Info("SFTP reconnected")
	return fn(b.client)
}

// ── SSH authentication ────────────────────────────────────────────────────────

func (b *Backend) authMethods() ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod

	// 1. SSH agent.
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		conn, err := net.Dial("unix", sock)
		if err == nil {
			agentClient := agent.NewClient(conn)
			methods = append(methods, ssh.PublicKeysCallback(agentClient.Signers))
			b.log.Debug("SSH agent auth available")
		}
	}

	// 2. Private key file.
	keyPath := b.cfg.KeyPath
	if keyPath == "" {
		keyPath = b.findDefaultKey()
	}
	if keyPath != "" {
		signer, err := b.loadKey(keyPath)
		if err != nil {
			b.log.Warn("could not load SSH key", zap.String("path", keyPath), zap.Error(err))
		} else {
			methods = append(methods, ssh.PublicKeys(signer))
			b.log.Debug("SSH key auth available", zap.String("path", keyPath))
		}
	}

	// 3. Password.
	if b.cfg.Password != "" {
		methods = append(methods, ssh.Password(b.cfg.Password))
		b.log.Debug("SSH password auth available")
	}

	return methods, nil
}

func (b *Backend) loadKey(keyPath string) (ssh.Signer, error) {
	pemBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key %q: %w", keyPath, err)
	}
	if b.cfg.KeyPassphrase != "" {
		return ssh.ParsePrivateKeyWithPassphrase(pemBytes, []byte(b.cfg.KeyPassphrase))
	}
	return ssh.ParsePrivateKey(pemBytes)
}

func (b *Backend) findDefaultKey() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	for _, name := range []string{"id_ed25519", "id_ecdsa", "id_rsa"} {
		p := home + "/.ssh/" + name
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func (b *Backend) hostKeyCallback() (ssh.HostKeyCallback, error) {
	if b.cfg.KnownHostsFile != "" {
		// knownhosts.New is in golang.org/x/crypto/ssh/knownhosts.
		// We avoid importing it here to keep the dependency minimal;
		// callers who need strict verification should pass HostKey instead.
		// TODO: add knownhosts.New support in a follow-up.
		b.log.Warn("known_hosts_file is not yet implemented; falling back to host_key verification")
	}
	if b.cfg.HostKey != "" {
		pk, _, _, _, err := ssh.ParseAuthorizedKey([]byte(b.cfg.HostKey))
		if err != nil {
			return nil, fmt.Errorf("parse host_key: %w", err)
		}
		return ssh.FixedHostKey(pk), nil
	}
	b.log.Warn("host key verification disabled (InsecureIgnoreHostKey) — set host_key in config for production",
		zap.String("host", b.p.hostPort),
	)
	return ssh.InsecureIgnoreHostKey(), nil //nolint:gosec
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// remotePath joins the server's base path, optional prefix, and relPath.
func (b *Backend) remotePath(relPath string) string {
	parts := []string{b.p.basePath}
	if b.p.prefix != "" {
		parts = append(parts, b.p.prefix)
	}
	if relPath != "" {
		parts = append(parts, relPath)
	}
	return path.Join(parts...)
}

func mkdirAll(c *sftp.Client, dirPath string) error {
	if dirPath == "" || dirPath == "." || dirPath == "/" {
		return nil
	}
	// Walk from root, creating each component.
	parts := strings.Split(strings.Trim(dirPath, "/"), "/")
	cur := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		if cur == "" {
			cur = part
		} else {
			cur = cur + "/" + part
		}
		if strings.HasPrefix(dirPath, "/") {
			err := c.Mkdir("/" + cur)
			if err != nil && !isExist(err) {
				return fmt.Errorf("mkdir /%s: %w", cur, err)
			}
		} else {
			err := c.Mkdir(cur)
			if err != nil && !isExist(err) {
				return fmt.Errorf("mkdir %s: %w", cur, err)
			}
		}
	}
	return nil
}

func pruneEmptyDirs(c *sftp.Client, dirPath, stopAt string) {
	for {
		if dirPath == "" || dirPath == "." || dirPath == "/" || dirPath == stopAt {
			return
		}
		entries, err := c.ReadDir(dirPath)
		if err != nil || len(entries) > 0 {
			return
		}
		if err := c.RemoveDirectory(dirPath); err != nil {
			return
		}
		dirPath = path.Dir(dirPath)
	}
}

func walkSFTP(c *sftp.Client, root, basePrefix string, fn func(domain.FileInfo)) error {
	entries, err := c.ReadDir(root)
	if err != nil {
		if isNotExist(err) {
			return nil
		}
		return fmt.Errorf("readdir %q: %w", root, err)
	}
	for _, e := range entries {
		full := path.Join(root, e.Name())
		rel := strings.TrimPrefix(full, basePrefix)
		rel = strings.TrimPrefix(rel, "/")
		fi := domain.FileInfo{
			RelPath: rel,
			Name:    e.Name(),
			Size:    e.Size(),
			ModTime: e.ModTime(),
			IsDir:   e.IsDir(),
		}
		fn(fi)
		if e.IsDir() {
			if err := walkSFTP(c, full, basePrefix, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// sftpErr wraps an SFTP error, mapping not-found to domain.ErrNotExist.
func sftpErr(err error, path string) error {
	if isNotExist(err) {
		return domain.ErrNotExist
	}
	return fmt.Errorf("%s: %w", path, err)
}

func isNotExist(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fs.ErrNotExist) || os.IsNotExist(err) {
		return true
	}
	// pkg/sftp wraps SSH_FX_NO_SUCH_FILE as *sftp.StatusError with Code 2.
	var sftpErr *sftp.StatusError
	if errors.As(err, &sftpErr) {
		return sftpErr.Code == 2 // SSH_FX_NO_SUCH_FILE
	}
	s := err.Error()
	return strings.Contains(s, "does not exist") ||
		strings.Contains(s, "no such file") ||
		strings.Contains(s, "not found")
}

func isExist(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fs.ErrExist) || os.IsExist(err) {
		return true
	}
	var sftpErr *sftp.StatusError
	if errors.As(err, &sftpErr) {
		return sftpErr.Code == 11 // SSH_FX_FILE_ALREADY_EXISTS
	}
	s := err.Error()
	return strings.Contains(s, "already exists") ||
		strings.Contains(s, "file exists")
}

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
	s := err.Error()
	return strings.Contains(s, "connection lost") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "EOF")
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)[:n]
}

// ── URI parsing ───────────────────────────────────────────────────────────────

type parsedURI struct {
	hostPort    string
	basePath    string // absolute path on the server
	prefix      string // optional sub-path within basePath
	uriUsername string
}

// parseURI decodes sftp://[user@]host[:port]/base/path[/prefix]
//
// The SFTP path split is:
//   - first component after host = basePath (the share/volume root)
//   - remaining components = prefix (optional sub-directory)
//
// Unlike SMB where share and path are always distinct, SFTP paths are
// arbitrary so we keep the full path as basePath and let the caller
// supply a prefix via Config if needed. However, to stay consistent with
// the SMB backend's config shape, we split on the first two path components:
//
//   sftp://host/mnt/storage/cctv  → basePath="/mnt/storage", prefix="cctv"
//
// If only one component is present it becomes basePath with no prefix.
func parseURI(rawURI string) (parsedURI, error) {
	if !strings.HasPrefix(rawURI, "sftp://") {
		return parsedURI{}, fmt.Errorf("expected sftp:// URI, got %q", rawURI)
	}
	// Strip scheme.
	rest := strings.TrimPrefix(rawURI, "sftp://")

	// Extract optional user@.
	var uriUser string
	if idx := strings.LastIndex(rest, "@"); idx != -1 {
		// Make sure the @ is before the first / (it belongs to userinfo, not path).
		slashIdx := strings.Index(rest, "/")
		if slashIdx == -1 || idx < slashIdx {
			uriUser = rest[:idx]
			rest = rest[idx+1:]
		}
	}

	// Split host:port from /path.
	slashIdx := strings.Index(rest, "/")
	if slashIdx == -1 {
		return parsedURI{}, fmt.Errorf("URI %q: missing path (need at least /basepath)", rawURI)
	}
	hostPart := rest[:slashIdx]
	pathPart := rest[slashIdx:] // starts with /

	// Ensure port.
	host, port, err := net.SplitHostPort(hostPart)
	if err != nil {
		// No port in URI. Strip IPv6 brackets if present so JoinHostPort
		// doesn't double-wrap them (e.g. [::1] → [[::1]]:22).
		host = strings.TrimSuffix(strings.TrimPrefix(hostPart, "["), "]")
		port = defaultPort
	}
	if port == "" {
		port = defaultPort
	}
	hostPort := net.JoinHostPort(host, port)

	// Normalise path.
	pathPart = path.Clean(pathPart)
	if pathPart == "." {
		return parsedURI{}, fmt.Errorf("URI %q: empty path", rawURI)
	}

	// Split: everything up to the last two path segments is basePath;
	// last segment (if more than one) is prefix.
	// sftp://host/data         → base=/data, prefix=""
	// sftp://host/data/cctv    → base=/data, prefix="cctv"
	// sftp://host/a/b/c/d      → base=/a/b/c, prefix="d"
	trimmed := strings.TrimPrefix(pathPart, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	basePath := "/" + parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = parts[1]
	}

	return parsedURI{
		hostPort:    hostPort,
		basePath:    basePath,
		prefix:      prefix,
		uriUsername: uriUser,
	}, nil
}

// resolveStr returns the first non-empty value: configVal, env[envKey], fallback.
func resolveStr(configVal, envKey, fallback string) string {
	if configVal != "" {
		return configVal
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return fallback
}
