package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validTOML = `
[mount]
path    = "/share/CCTV"
meta_db = "/var/lib/tierfs/meta.db"

[replication]
workers        = 4
retry_interval = "30s"
max_retries    = 5
verify         = "digest"

[eviction]
check_interval     = "5m"
capacity_threshold = 0.85
capacity_headroom  = 0.70

[[backend]]
name = "ssd"
uri  = "file:///local/CCTV"

[[backend]]
name = "nas"
uri  = "file:///mnt/nas/CCTV"

[[backend]]
name      = "minio"
uri       = "s3://nvr-archive/cctv"
endpoint  = "https://minio.lan:9000"
region    = "us-east-1"
path_style = true

[[tier]]
name     = "tier0"
backend  = "ssd"
capacity = "500GiB"
priority = 0

[[tier]]
name     = "tier1"
backend  = "nas"
capacity = "8TiB"
priority = 1

[[tier]]
name     = "tier2"
backend  = "minio"
capacity = "unlimited"
priority = 2

[[rule]]
name  = "thumbnails"
match = "thumbnails/**"
evict_schedule = [{after = "0s", to = "tier1"}]
promote_on_read = false

[[rule]]
name  = "recordings"
match = "recordings/**"
evict_schedule = [
  {after = "24h",  to = "tier1"},
  {after = "720h", to = "tier2"},
]
promote_on_read = false

[[rule]]
name           = "default"
match          = "**"
evict_schedule = [{after = "48h", to = "tier1"}]
promote_on_read = false
`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "tierfs-*.toml")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestLoad_Valid(t *testing.T) {
	path := writeConfig(t, validTOML)
	r, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, "/share/CCTV", r.Mount.Path)
	assert.Equal(t, 4, r.Replication.Workers)
	assert.Equal(t, "digest", r.Replication.Verify)
	assert.Equal(t, 3, len(r.Tiers))
	assert.Equal(t, 3, len(r.Backends))
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := config.Load(filepath.Join(t.TempDir(), "nonexistent.toml"))
	require.Error(t, err)
}

func TestResolve_MissingMountPath(t *testing.T) {
	const toml = `
[[backend]]
name = "ssd"
uri  = "file:///tmp"

[[tier]]
name = "tier0"
backend = "ssd"
priority = 0

[[rule]]
name = "default"
match = "**"
`
	path := writeConfig(t, toml)
	_, err := config.Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mount.path")
}

func TestResolve_MissingCatchAll(t *testing.T) {
	const toml = `
[mount]
path    = "/mnt"
meta_db = "/tmp/meta.db"

[[backend]]
name = "ssd"
uri  = "file:///tmp"

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0

[[rule]]
name  = "recordings"
match = "recordings/**"
`
	path := writeConfig(t, toml)
	_, err := config.Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "catch-all")
}

func TestResolve_UnknownBackend(t *testing.T) {
	const toml = `
[mount]
path    = "/mnt"
meta_db = "/tmp/meta.db"

[[backend]]
name = "ssd"
uri  = "file:///tmp"

[[tier]]
name     = "tier0"
backend  = "doesnotexist"
priority = 0

[[rule]]
name  = "default"
match = "**"
`
	path := writeConfig(t, toml)
	_, err := config.Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown backend")
}

func TestResolve_UnknownScheme(t *testing.T) {
	const toml = `
[mount]
path    = "/mnt"
meta_db = "/tmp/meta.db"

[[backend]]
name = "ftp"
uri  = "ftp://somehost/path"

[[tier]]
name     = "tier0"
backend  = "ftp"
priority = 0

[[rule]]
name  = "default"
match = "**"
`
	path := writeConfig(t, toml)
	_, err := config.Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported scheme")
}

func TestResolve_CapacityParsing(t *testing.T) {
	cases := []struct {
		s       string
		wantB   int64
		unlimited bool
	}{
		{"500GiB", 500 * 1024 * 1024 * 1024, false},
		{"8TiB", 8 * 1024 * 1024 * 1024 * 1024, false},
		{"unlimited", 0, true},
		{"", 0, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.s, func(t *testing.T) {
			tomlStr := `
[mount]
path    = "/mnt"
meta_db = "/tmp/meta.db"

[[backend]]
name = "ssd"
uri  = "file:///tmp"

[[tier]]
name     = "tier0"
backend  = "ssd"
capacity = "` + tc.s + `"
priority = 0

[[rule]]
name  = "default"
match = "**"
`
			path := writeConfig(t, tomlStr)
			r, err := config.Load(path)
			require.NoError(t, err)
			tier := r.TiersByName["tier0"]
			assert.Equal(t, tc.unlimited, tier.Capacity.Unlimited)
			if !tc.unlimited {
				assert.Equal(t, tc.wantB, tier.Capacity.Bytes)
			}
		})
	}
}

func TestHottestTier(t *testing.T) {
	path := writeConfig(t, validTOML)
	r, err := config.Load(path)
	require.NoError(t, err)
	hot := r.HottestTier()
	assert.Equal(t, "tier0", hot.Name)
}
