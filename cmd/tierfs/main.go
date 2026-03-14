package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/adapters/fuse"
	filerbackend "github.com/mikey-austin/tierfs/internal/adapters/storage/file"
	nullbackend "github.com/mikey-austin/tierfs/internal/adapters/storage/null"
	s3backend "github.com/mikey-austin/tierfs/internal/adapters/storage/s3"
	sftpbackend "github.com/mikey-austin/tierfs/internal/adapters/storage/sftp"
	smbbackend "github.com/mikey-austin/tierfs/internal/adapters/storage/smb"
	"github.com/mikey-austin/tierfs/internal/adapters/storage/transform"
	"github.com/mikey-austin/tierfs/internal/adapters/meta/sqlite"
	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/observability"
)

func main() {
	cfgPath := flag.String("config", "/etc/tierfs/tierfs.toml", "path to TOML config file")
	flag.Parse()

	if err := run(*cfgPath); err != nil {
		fmt.Fprintf(os.Stderr, "tierfs: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath string) error {
	// ── Config ───────────────────────────────────────────────────────────────
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ── Observability ────────────────────────────────────────────────────────
	obs, err := observability.Wire(cfg.Observability, "tierfs")
	if err != nil {
		return fmt.Errorf("wire observability: %w", err)
	}
	log := obs.Log
	defer obs.Shutdown(context.Background())

	log.Info("tierfs starting",
		zap.String("config", cfgPath),
		zap.String("mount", cfg.Mount.Path),
		zap.Int("tiers", len(cfg.Tiers)),
	)

	// ── Metadata store ───────────────────────────────────────────────────────
	rawMeta, err := sqlite.Open(cfg.Mount.MetaDB)
	if err != nil {
		return fmt.Errorf("open metadata store: %w", err)
	}
	meta := obs.WrapMetaStore(rawMeta)
	defer meta.Close()

	// ── Stage directory ──────────────────────────────────────────────────────
	if err := os.MkdirAll(cfg.Mount.StageDir, 0o755); err != nil {
		return fmt.Errorf("create stage dir: %w", err)
	}

	// ── Backends ─────────────────────────────────────────────────────────────
	// Build one backend per tier, wrapping each with observability.
	backends := make(map[string]domain.Backend, len(cfg.Tiers))
	for _, tier := range cfg.Tiers {
		bcfg := tier.Backend
		raw, berr := buildBackend(bcfg, log)
		if berr != nil {
			return fmt.Errorf("build backend for tier %q: %w", tier.Name, berr)
		}

		// Wrap with transforms if configured. The transform package enforces
		// compress-before-encrypt ordering regardless of config order.
		wrapped, werr := applyTransforms(raw, bcfg.Transform, log)
		if werr != nil {
			return fmt.Errorf("build transforms for tier %q: %w", tier.Name, werr)
		}

		backends[tier.Name] = obs.WrapBackend(wrapped, tier.Name)
		log.Info("backend ready",
			zap.String("tier", tier.Name),
			zap.String("uri", bcfg.URI),
		)
	}

	// ── Application layer ─────────────────────────────────────────────────────
	svc := app.NewTierService(cfg, meta, backends, log)
	stager := app.NewStager(cfg.Mount.StageDir, log)
	svc.Start()
	defer svc.Stop()

	// ── FUSE mount ────────────────────────────────────────────────────────────
	tierFS := fuse.New(svc, meta, stager, log)
	server, err := fuse.Mount(tierFS, cfg.Mount.Path, log)
	if err != nil {
		return fmt.Errorf("mount fuse: %w", err)
	}

	// Serve FUSE requests in a background goroutine.
	go server.Serve()
	log.Info("ready", zap.String("mount", cfg.Mount.Path))

	// ── Signal handling ───────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	log.Info("shutdown signal received", zap.String("signal", sig.String()))

	// Unmount first so no new FUSE requests arrive.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Unmount(); err != nil {
		log.Warn("fuse unmount error", zap.Error(err))
	}
	log.Info("fuse unmounted")

	// Flush observability.
	obs.Shutdown(shutdownCtx)
	log.Info("tierfs stopped cleanly")
	return nil
}

// buildBackend constructs a concrete Backend from BackendConfig.
func buildBackend(cfg config.BackendConfig, log *zap.Logger) (domain.Backend, error) {
	u, err := url.Parse(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("parse URI %q: %w", cfg.URI, err)
	}
	switch u.Scheme {
	case "file":
		return filerbackend.New(u.Path)
	case "s3":
		bucket, prefix, err := s3backend.ParseURI(cfg.URI)
		if err != nil {
			return nil, err
		}
		return s3backend.New(s3backend.Config{
			Name:      cfg.Name,
			Bucket:    bucket,
			Prefix:    prefix,
			Endpoint:  cfg.Endpoint,
			Region:    cfg.Region,
			PathStyle: cfg.PathStyle,
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
		})
	case "sftp":
		return sftpbackend.New(sftpbackend.Config{
			Name:              cfg.Name,
			URI:               cfg.URI,
			Username:          cfg.SFTPUsername,
			Password:          cfg.SFTPPassword,
			KeyPath:           cfg.SFTPKeyPath,
			KeyPassphrase:     cfg.SFTPKeyPassphrase,
			HostKey:           cfg.SFTPHostKey,
			KnownHostsFile:    cfg.SFTPKnownHostsFile,
		}, log)
	case "smb":
		return smbbackend.New(smbbackend.Config{
			Name:              cfg.Name,
			URI:               cfg.URI,
			Username:          cfg.SMBUsername,
			Password:          cfg.SMBPassword,
			Domain:            cfg.SMBDomain,
			RequireEncryption: cfg.SMBRequireEncryption,
		}, log)
	case "null":
		return nullbackend.New(), nil
	default:
		return nil, fmt.Errorf("unsupported backend scheme %q", u.Scheme)
	}
}

// applyTransforms wraps b with a TransformBackend if any transforms are
// configured. Returns b unchanged if no transforms are configured.
// Logs the resolved pipeline order and any automatic decisions (e.g. checksum
// elision) at info level so operators can verify what they actually got.
func applyTransforms(b domain.Backend, cfg config.BackendTransformConfig, log *zap.Logger) (domain.Backend, error) {
	if cfg.Compression == nil && cfg.Checksum == nil && cfg.Encryption == nil {
		return b, nil
	}

	tcfg := transform.Config{}
	if cfg.Compression != nil {
		tcfg.Compression = &transform.CompressionConfig{
			Algorithm: cfg.Compression.Algorithm,
			Level:     cfg.Compression.Level,
		}
	}
	if cfg.Checksum != nil {
		tcfg.Checksum = &transform.ChecksumConfig{}
	}
	if cfg.Encryption != nil {
		tcfg.Encryption = &transform.EncryptionConfig{
			KeyHex: cfg.Encryption.KeyHex,
			KeyEnv: cfg.Encryption.KeyEnv,
		}
	}

	pipeline, reason, err := transform.NewPipeline(tcfg)
	if err != nil {
		return nil, err
	}

	log.Info("backend transform pipeline resolved",
		zap.String("backend", b.URI("")),
		zap.String("pipeline", reason.Order),
		zap.Bool("checksum_elided", reason.ChecksumElided),
	)
	if reason.ChecksumElided {
		log.Info("checksum elided: AES-256-GCM provides stronger per-chunk integrity via AEAD")
	}

	return transform.New(b, pipeline, log), nil
}
