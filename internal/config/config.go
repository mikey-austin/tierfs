package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mikey-austin/tierfs/internal/domain"
)

// Config is the root configuration structure, loaded from TOML.
type Config struct {
	Mount           MountConfig          `toml:"mount"`
	Replication     ReplicationConfig    `toml:"replication"`
	Eviction        EvictionConfig       `toml:"eviction"`
	Observability   ObservabilityConfig  `toml:"observability"`
	Backends        []BackendConfig      `toml:"backend"`
	Tiers           []TierConfig         `toml:"tier"`
	Rules           []RuleConfig         `toml:"rule"`
}

type MountConfig struct {
	Path     string `toml:"path"`
	MetaDB   string `toml:"meta_db"`
	StageDir string `toml:"stage_dir"`
}

type ReplicationConfig struct {
	Workers         int           `toml:"workers"`
	RetryInterval   string        `toml:"retry_interval"`
	MaxRetries      int           `toml:"max_retries"`
	Verify          string        `toml:"verify"`          // none | size | digest
	WriteQuiescence string        `toml:"write_quiescence"` // min idle time after last write close before replication
}

type EvictionConfig struct {
	CheckInterval     string  `toml:"check_interval"`
	CapacityThreshold float64 `toml:"capacity_threshold"`
	CapacityHeadroom  float64 `toml:"capacity_headroom"`
}

type BackendConfig struct {
	Name      string `toml:"name"`
	URI       string `toml:"uri"`
	Endpoint  string `toml:"endpoint"`   // S3: custom endpoint URL
	Region    string `toml:"region"`     // S3: region
	PathStyle bool   `toml:"path_style"` // S3: force path-style (MinIO, Ceph)
	AccessKey string `toml:"access_key"` // S3: prefer env vars in practice
	SecretKey string `toml:"secret_key"` // S3: prefer env vars in practice
	// SMB-specific fields (only for smb:// URIs)
	SMBUsername          string `toml:"smb_username"`           // prefer TIERFS_SMB_USER env var
	SMBPassword          string `toml:"smb_password"`           // prefer TIERFS_SMB_PASS env var
	SMBDomain            string `toml:"smb_domain"`             // Windows/AD domain; empty for workgroup NAS
	SMBRequireEncryption bool   `toml:"smb_require_encryption"` // require SMB3 encryption
	// SFTP-specific fields (only for sftp:// URIs)
	SFTPUsername       string `toml:"sftp_username"`        // prefer TIERFS_SFTP_USER env var
	SFTPPassword       string `toml:"sftp_password"`        // prefer TIERFS_SFTP_PASS env var
	SFTPKeyPath        string `toml:"sftp_key_path"`        // path to PEM private key; prefer TIERFS_SFTP_KEY_PATH
	SFTPKeyPassphrase  string `toml:"sftp_key_passphrase"`  // decrypts encrypted key; prefer TIERFS_SFTP_KEY_PASSPHRASE
	SFTPHostKey        string `toml:"sftp_host_key"`        // expected host key in authorized_keys format
	SFTPKnownHostsFile string `toml:"sftp_known_hosts_file"` // path to known_hosts file
	// Transform applies compression and/or encryption to data at rest.
	// Compression is always applied before encryption on the write path.
	Transform BackendTransformConfig `toml:"transform"`
}

// BackendTransformConfig holds optional transform configuration for a backend.
// Set any combination of Compression, Checksum, and Encryption; NewPipeline
// figures out the correct ordering and elides redundant transforms:
//
//   - Compression always comes before checksum and encryption.
//   - Checksum is automatically elided when Encryption is also set
//     (AES-256-GCM provides stronger per-chunk integrity via AEAD).
//   - Encryption is always last.
//
// Example — encrypted + compressed S3 tier:
//
//	[backend.transform.compression]
//	algorithm = "zstd"
//	level = 1
//
//	[backend.transform.encryption]
//	key_env = "TIERFS_S3_KEY"
//
// Example — checksum-protected unencrypted NAS tier:
//
//	[backend.transform.checksum]
//	# no fields needed
type BackendTransformConfig struct {
	Compression *CompressionTransformConfig `toml:"compression"`
	Checksum    *ChecksumTransformConfig    `toml:"checksum"`
	Encryption  *EncryptionTransformConfig  `toml:"encryption"`
}

// CompressionTransformConfig configures the compression transform.
type CompressionTransformConfig struct {
	// Algorithm selects the codec: "zstd" (default) or "gzip".
	// zstd is recommended — it is faster at equivalent ratios and handles
	// incompressible media (H.264/H.265) with lower overhead.
	Algorithm string `toml:"algorithm"`
	// Level is the compression level (algorithm-specific).
	// zstd: 0=default, 1=fastest, 2=default, 3=better, 4=best
	// gzip: -1=default, 1=fastest, 9=best
	Level int `toml:"level"`
}

// ChecksumTransformConfig enables xxhash3-128 bit-rot detection.
// Automatically elided when Encryption is also configured (redundant with AEAD).
// No fields are currently required.
type ChecksumTransformConfig struct{}

// EncryptionTransformConfig configures AES-256-GCM encryption.
type EncryptionTransformConfig struct {
	// KeyEnv is an environment variable name containing the 64-char hex key.
	// Takes precedence over KeyHex if both are set.
	KeyEnv string `toml:"key_env"`
	// KeyHex is the 64-character lowercase hex-encoded 32-byte AES-256 key.
	// Prefer KeyEnv for production to avoid storing secrets in the config file.
	KeyHex string `toml:"key_hex"`
}

type TierConfig struct {
	Name     string `toml:"name"`
	Backend  string `toml:"backend"`  // backend name reference
	Capacity string `toml:"capacity"` // e.g. "500GiB", "8TiB", "unlimited"
	Priority int    `toml:"priority"` // 0 = hottest; lower wins writes
}

type RuleConfig struct {
	Name          string           `toml:"name"`
	Match         string           `toml:"match"`
	PinTier       string           `toml:"pin_tier"`
	EvictSchedule []EvictStepToml  `toml:"evict_schedule"`
	PromoteOnRead promoteOnReadVal `toml:"promote_on_read"` // false | tier name string
	Replicate     *bool            `toml:"replicate"`       // nil = true
}

type EvictStepToml struct {
	After string `toml:"after"`
	To    string `toml:"to"`
}

// promoteOnReadVal handles "promote_on_read = false" vs "promote_on_read = \"tier0\"".
type promoteOnReadVal struct {
	Enabled    bool
	TargetTier string
}

func (p *promoteOnReadVal) UnmarshalTOML(data interface{}) error {
	switch v := data.(type) {
	case bool:
		p.Enabled = v
	case string:
		p.Enabled = true
		p.TargetTier = v
	default:
		return fmt.Errorf("promote_on_read must be bool or tier name string")
	}
	return nil
}

// Resolved holds the fully validated and resolved configuration,
// with all name references replaced by pointers and durations parsed.
type Resolved struct {
	Mount         MountConfig
	Replication   ReplicationResolved
	Eviction      EvictionResolved
	Observability ObservabilityConfig
	Backends      map[string]BackendConfig
	Tiers         []TierResolved  // sorted by priority ascending
	TiersByName   map[string]*TierResolved
	Policy        *domain.PolicyEngine
}

type ReplicationResolved struct {
	Workers         int
	RetryInterval   time.Duration
	MaxRetries      int
	Verify          string
	WriteQuiescence time.Duration
}

type EvictionResolved struct {
	CheckInterval     time.Duration
	CapacityThreshold float64
	CapacityHeadroom  float64
}

type TierResolved struct {
	Name     string
	Backend  BackendConfig
	Capacity CapacityResolved
	Priority int
}

type CapacityResolved struct {
	Bytes     int64
	Unlimited bool
}

// Load reads and resolves a TOML config from path.
func Load(path string) (*Resolved, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var cfg Config
	if _, err := toml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	return cfg.Resolve()
}

// Resolve validates and resolves the raw config into a Resolved struct.
func (cfg *Config) Resolve() (*Resolved, error) {
	r := &Resolved{}

	// ── Mount ────────────────────────────────────────────────────────────────
	if cfg.Mount.Path == "" {
		return nil, fmt.Errorf("mount.path is required")
	}
	if cfg.Mount.MetaDB == "" {
		return nil, fmt.Errorf("mount.meta_db is required")
	}
	if cfg.Mount.StageDir == "" {
		cfg.Mount.StageDir = "/tmp/tierfs-stage"
	}
	r.Mount = cfg.Mount

	// ── Observability ────────────────────────────────────────────────────────
	cfg.Observability.Defaults()
	r.Observability = cfg.Observability

	// ── Replication ──────────────────────────────────────────────────────────
	ri, err := cfg.resolveReplication()
	if err != nil {
		return nil, err
	}
	r.Replication = ri

	// ── Eviction ─────────────────────────────────────────────────────────────
	ei, err := cfg.resolveEviction()
	if err != nil {
		return nil, err
	}
	r.Eviction = ei

	// ── Backends ─────────────────────────────────────────────────────────────
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("at least one [[backend]] is required")
	}
	backendMap := make(map[string]BackendConfig, len(cfg.Backends))
	for _, b := range cfg.Backends {
		if b.Name == "" {
			return nil, fmt.Errorf("backend missing name")
		}
		if b.URI == "" {
			return nil, fmt.Errorf("backend %q: uri is required", b.Name)
		}
		u, err := url.Parse(b.URI)
		if err != nil {
			return nil, fmt.Errorf("backend %q: invalid uri %q: %w", b.Name, b.URI, err)
		}
		switch u.Scheme {
		case "file", "s3":
		default:
			return nil, fmt.Errorf("backend %q: unsupported scheme %q (supported: file, s3)", b.Name, u.Scheme)
		}
		if _, exists := backendMap[b.Name]; exists {
			return nil, fmt.Errorf("duplicate backend name %q", b.Name)
		}
		backendMap[b.Name] = b
	}
	r.Backends = backendMap

	// ── Tiers ────────────────────────────────────────────────────────────────
	if len(cfg.Tiers) == 0 {
		return nil, fmt.Errorf("at least one [[tier]] is required")
	}
	tiersByName := make(map[string]*TierResolved, len(cfg.Tiers))
	tiers := make([]TierResolved, 0, len(cfg.Tiers))
	for _, t := range cfg.Tiers {
		if t.Name == "" {
			return nil, fmt.Errorf("tier missing name")
		}
		b, ok := backendMap[t.Backend]
		if !ok {
			return nil, fmt.Errorf("tier %q: unknown backend %q", t.Name, t.Backend)
		}
		cap, err := parseCapacity(t.Capacity)
		if err != nil {
			return nil, fmt.Errorf("tier %q: %w", t.Name, err)
		}
		tr := TierResolved{
			Name:     t.Name,
			Backend:  b,
			Capacity: cap,
			Priority: t.Priority,
		}
		tiers = append(tiers, tr)
		tiersByName[t.Name] = &tiers[len(tiers)-1]
	}
	r.Tiers = tiers
	r.TiersByName = tiersByName

	// ── Rules ────────────────────────────────────────────────────────────────
	if len(cfg.Rules) == 0 {
		return nil, fmt.Errorf("at least one [[rule]] is required")
	}
	last := cfg.Rules[len(cfg.Rules)-1]
	if last.Match != "**" {
		return nil, fmt.Errorf("last [[rule]] must be a catch-all with match = \"**\"")
	}

	domainRules := make([]domain.Rule, 0, len(cfg.Rules))
	for i, rc := range cfg.Rules {
		dr, err := cfg.resolveRule(rc, tiersByName, i)
		if err != nil {
			return nil, err
		}
		domainRules = append(domainRules, dr)
	}
	r.Policy = domain.NewPolicyEngine(domainRules)

	return r, nil
}

func (cfg *Config) resolveReplication() (ReplicationResolved, error) {
	rc := cfg.Replication
	workers := rc.Workers
	if workers <= 0 {
		workers = 4
	}
	retryInterval := 30 * time.Second
	if rc.RetryInterval != "" {
		d, err := time.ParseDuration(rc.RetryInterval)
		if err != nil {
			return ReplicationResolved{}, fmt.Errorf("replication.retry_interval: %w", err)
		}
		retryInterval = d
	}
	maxRetries := rc.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}
	verify := rc.Verify
	switch verify {
	case "", "digest":
		verify = "digest"
	case "size", "none":
	default:
		return ReplicationResolved{}, fmt.Errorf("replication.verify: unknown value %q (none|size|digest)", verify)
	}
	var writeQuiescence time.Duration
	if rc.WriteQuiescence != "" {
		d, err := time.ParseDuration(rc.WriteQuiescence)
		if err != nil {
			return ReplicationResolved{}, fmt.Errorf("replication.write_quiescence: %w", err)
		}
		if d < 0 {
			return ReplicationResolved{}, fmt.Errorf("replication.write_quiescence: must be non-negative, got %q", rc.WriteQuiescence)
		}
		writeQuiescence = d
	}
	return ReplicationResolved{
		Workers:         workers,
		RetryInterval:   retryInterval,
		MaxRetries:      maxRetries,
		Verify:          verify,
		WriteQuiescence: writeQuiescence,
	}, nil
}

func (cfg *Config) resolveEviction() (EvictionResolved, error) {
	ec := cfg.Eviction
	checkInterval := 5 * time.Minute
	if ec.CheckInterval != "" {
		d, err := time.ParseDuration(ec.CheckInterval)
		if err != nil {
			return EvictionResolved{}, fmt.Errorf("eviction.check_interval: %w", err)
		}
		checkInterval = d
	}
	threshold := ec.CapacityThreshold
	if threshold == 0 {
		threshold = 0.85
	}
	headroom := ec.CapacityHeadroom
	if headroom == 0 {
		headroom = 0.70
	}
	return EvictionResolved{
		CheckInterval:     checkInterval,
		CapacityThreshold: threshold,
		CapacityHeadroom:  headroom,
	}, nil
}

func (cfg *Config) resolveRule(rc RuleConfig, tiers map[string]*TierResolved, idx int) (domain.Rule, error) {
	if rc.Name == "" {
		return domain.Rule{}, fmt.Errorf("rule[%d]: name is required", idx)
	}
	if rc.Match == "" {
		return domain.Rule{}, fmt.Errorf("rule %q: match is required", rc.Name)
	}

	if rc.PinTier != "" {
		if _, ok := tiers[rc.PinTier]; !ok {
			return domain.Rule{}, fmt.Errorf("rule %q: unknown pin_tier %q", rc.Name, rc.PinTier)
		}
	}

	steps := make([]domain.EvictStep, 0, len(rc.EvictSchedule))
	for i, s := range rc.EvictSchedule {
		d, err := domain.ParseDuration(s.After)
		if err != nil {
			return domain.Rule{}, fmt.Errorf("rule %q step[%d]: %w", rc.Name, i, err)
		}
		if _, ok := tiers[s.To]; !ok {
			return domain.Rule{}, fmt.Errorf("rule %q step[%d]: unknown tier %q", rc.Name, i, s.To)
		}
		steps = append(steps, domain.EvictStep{After: d, ToTier: s.To})
	}

	replicate := true
	if rc.Replicate != nil {
		replicate = *rc.Replicate
	}

	return domain.Rule{
		Name:          rc.Name,
		Match:         rc.Match,
		PinTier:       rc.PinTier,
		EvictSchedule: steps,
		PromoteOnRead: domain.PromotePolicy{
			Enabled:    rc.PromoteOnRead.Enabled,
			TargetTier: rc.PromoteOnRead.TargetTier,
		},
		Replicate: replicate,
	}, nil
}

// HottestTier returns the tier with the lowest priority value (the write target).
func (r *Resolved) HottestTier() *TierResolved {
	var hot *TierResolved
	for i := range r.Tiers {
		if hot == nil || r.Tiers[i].Priority < hot.Priority {
			hot = &r.Tiers[i]
		}
	}
	return hot
}

// parseCapacity parses strings like "500GiB", "8TiB", "unlimited".
func parseCapacity(s string) (CapacityResolved, error) {
	if s == "" || strings.EqualFold(s, "unlimited") {
		return CapacityResolved{Unlimited: true}, nil
	}
	// Ordered longest-suffix-first so "GIB" matches before "B".
	type unit struct {
		suffix string
		mult   int64
	}
	units := []unit{
		{"TIB", 1024 * 1024 * 1024 * 1024},
		{"GIB", 1024 * 1024 * 1024},
		{"MIB", 1024 * 1024},
		{"KIB", 1024},
		{"TB", 1000 * 1000 * 1000 * 1000},
		{"GB", 1000 * 1000 * 1000},
		{"MB", 1000 * 1000},
		{"KB", 1000},
		{"B", 1},
	}
	s = strings.TrimSpace(s)
	upper := strings.ToUpper(s)
	for _, u := range units {
		if strings.HasSuffix(upper, u.suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(u.suffix)])
			var n int64
			if _, err := fmt.Sscanf(numStr, "%d", &n); err != nil {
				return CapacityResolved{}, fmt.Errorf("capacity %q: invalid number", s)
			}
			return CapacityResolved{Bytes: n * u.mult}, nil
		}
	}
	return CapacityResolved{}, fmt.Errorf("capacity %q: unrecognised unit", s)
}
