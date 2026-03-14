package tracing_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/observability/tracing"
)

// These tests set the global OTel provider, so they must NOT run in parallel.

func TestNew_Disabled_ReturnsNoopProvider(t *testing.T) {
	cfg := config.TracingConfig{
		Enabled: false,
	}
	prov, err := tracing.New(cfg)
	require.NoError(t, err)
	require.NotNil(t, prov)

	// Shutdown on a noop provider should return nil.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.NoError(t, prov.Shutdown(ctx))
}

func TestNew_Enabled_WithEndpoint_Succeeds(t *testing.T) {
	cfg := config.TracingConfig{
		Enabled:     true,
		Endpoint:    "localhost:4317",
		ServiceName: "tierfs-test",
		SampleRate:  1.0,
		Insecure:    true,
	}
	// gRPC dial is lazy, so this should succeed even without a running collector.
	prov, err := tracing.New(cfg)
	require.NoError(t, err)
	require.NotNil(t, prov)

	// Clean up.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = prov.Shutdown(ctx)
}

func TestProvider_Shutdown_NoError(t *testing.T) {
	// Build a disabled provider (noop).
	prov, err := tracing.New(config.TracingConfig{Enabled: false})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.NoError(t, prov.Shutdown(ctx))
}
