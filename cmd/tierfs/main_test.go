package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/adapters/storage/transform"
	"github.com/mikey-austin/tierfs/internal/config"
)

var log = zap.NewNop()

func TestBuildBackend_FileScheme(t *testing.T) {
	dir := t.TempDir()
	b, err := buildBackend(config.BackendConfig{URI: "file://" + dir}, log)
	require.NoError(t, err)
	assert.Equal(t, "file", b.Scheme())
}

func TestBuildBackend_NullScheme(t *testing.T) {
	b, err := buildBackend(config.BackendConfig{URI: "null://discard"}, log)
	require.NoError(t, err)
	assert.Equal(t, "null", b.Scheme())
}

func TestBuildBackend_UnsupportedScheme(t *testing.T) {
	_, err := buildBackend(config.BackendConfig{URI: "ftp://example.com/data"}, log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported backend scheme")
}

func TestBuildBackend_InvalidURI(t *testing.T) {
	_, err := buildBackend(config.BackendConfig{URI: "://bad"}, log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse URI")
}

func TestApplyTransforms_NilConfig(t *testing.T) {
	inner, err := buildBackend(config.BackendConfig{URI: "null://discard"}, log)
	require.NoError(t, err)

	got, err := applyTransforms(inner, config.BackendTransformConfig{}, log)
	require.NoError(t, err)
	assert.Equal(t, inner, got, "no transforms → same backend returned")
}

func TestApplyTransforms_CompressionOnly(t *testing.T) {
	inner, err := buildBackend(config.BackendConfig{URI: "null://discard"}, log)
	require.NoError(t, err)

	cfg := config.BackendTransformConfig{
		Compression: &config.CompressionTransformConfig{Algorithm: "zstd"},
	}
	got, err := applyTransforms(inner, cfg, log)
	require.NoError(t, err)
	assert.NotEqual(t, inner, got, "should wrap with transform backend")
}

func TestApplyTransforms_EncryptionOnly(t *testing.T) {
	inner, err := buildBackend(config.BackendConfig{URI: "null://discard"}, log)
	require.NoError(t, err)

	key, err := transform.GenerateKey()
	require.NoError(t, err)

	cfg := config.BackendTransformConfig{
		Encryption: &config.EncryptionTransformConfig{KeyHex: key},
	}
	got, err := applyTransforms(inner, cfg, log)
	require.NoError(t, err)
	assert.NotEqual(t, inner, got)
}

func TestApplyTransforms_AllTransforms(t *testing.T) {
	inner, err := buildBackend(config.BackendConfig{URI: "null://discard"}, log)
	require.NoError(t, err)

	key, err := transform.GenerateKey()
	require.NoError(t, err)

	cfg := config.BackendTransformConfig{
		Compression: &config.CompressionTransformConfig{Algorithm: "zstd"},
		Checksum:    &config.ChecksumTransformConfig{},
		Encryption:  &config.EncryptionTransformConfig{KeyHex: key},
	}
	got, err := applyTransforms(inner, cfg, log)
	require.NoError(t, err)
	assert.NotEqual(t, inner, got)
}

func TestApplyTransforms_InvalidEncryptionKey(t *testing.T) {
	inner, err := buildBackend(config.BackendConfig{URI: "null://discard"}, log)
	require.NoError(t, err)

	cfg := config.BackendTransformConfig{
		Encryption: &config.EncryptionTransformConfig{KeyHex: "not-a-valid-hex-key"},
	}
	_, err = applyTransforms(inner, cfg, log)
	require.Error(t, err)
}
