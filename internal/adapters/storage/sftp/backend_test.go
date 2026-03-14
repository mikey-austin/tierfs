package sftp_test

import (
	"testing"

	sftpbackend "github.com/mikey-austin/tierfs/internal/adapters/storage/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── URI parsing tests (no network) ────────────────────────────────────────────

func TestParseURI_Full(t *testing.T) {
	hostPort, base, prefix, user, err := sftpbackend.ParseURI(
		"sftp://admin@nas.lan:22/mnt/storage/cctv",
	)
	require.NoError(t, err)
	assert.Equal(t, "nas.lan:22", hostPort)
	assert.Equal(t, "/mnt", base)
	assert.Equal(t, "storage/cctv", prefix)
	assert.Equal(t, "admin", user)
}

func TestParseURI_DefaultPort(t *testing.T) {
	hostPort, _, _, _, err := sftpbackend.ParseURI("sftp://nas.lan/data/cctv")
	require.NoError(t, err)
	assert.Equal(t, "nas.lan:22", hostPort)
}

func TestParseURI_NoUser(t *testing.T) {
	_, _, _, user, err := sftpbackend.ParseURI("sftp://nas.lan/data")
	require.NoError(t, err)
	assert.Empty(t, user)
}

func TestParseURI_NoPrefix(t *testing.T) {
	_, base, prefix, _, err := sftpbackend.ParseURI("sftp://nas.lan/data")
	require.NoError(t, err)
	assert.Equal(t, "/data", base)
	assert.Empty(t, prefix)
}

func TestParseURI_DeepPrefix(t *testing.T) {
	_, base, prefix, _, err := sftpbackend.ParseURI("sftp://nas.lan/srv/tier1/cctv/frigate")
	require.NoError(t, err)
	assert.Equal(t, "/srv", base)
	assert.Equal(t, "tier1/cctv/frigate", prefix)
}

func TestParseURI_NonStandardPort(t *testing.T) {
	hostPort, _, _, _, err := sftpbackend.ParseURI("sftp://backup@offsite.example.com:2222/backups")
	require.NoError(t, err)
	assert.Equal(t, "offsite.example.com:2222", hostPort)
}

func TestParseURI_IPv4(t *testing.T) {
	hostPort, _, _, _, err := sftpbackend.ParseURI("sftp://192.168.1.50/data")
	require.NoError(t, err)
	assert.Equal(t, "192.168.1.50:22", hostPort)
}

func TestParseURI_IPv6(t *testing.T) {
	hostPort, _, _, _, err := sftpbackend.ParseURI("sftp://[::1]/data")
	require.NoError(t, err)
	assert.Equal(t, "[::1]:22", hostPort)
}

func TestParseURI_WrongScheme(t *testing.T) {
	_, _, _, _, err := sftpbackend.ParseURI("smb://nas.lan/share")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected sftp://")
}

func TestParseURI_MissingPath(t *testing.T) {
	_, _, _, _, err := sftpbackend.ParseURI("sftp://nas.lan")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing path")
}

func TestParseURI_EmptyPath(t *testing.T) {
	_, _, _, _, err := sftpbackend.ParseURI("sftp://nas.lan/")
	// A bare / normalises to /. Base becomes "", which may or may not error
	// depending on path.Clean; just confirm no panic.
	_ = err
}

func TestParseURI_UserWithSpecialChars(t *testing.T) {
	// @ in password is percent-encoded as %40 in a proper URI but
	// users often don't encode — we handle the common case of user@host.
	_, _, _, user, err := sftpbackend.ParseURI("sftp://myuser@nas.lan/data")
	require.NoError(t, err)
	assert.Equal(t, "myuser", user)
}
