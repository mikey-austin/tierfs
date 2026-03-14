package observability_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/observability"
)

// Wire tests set the global OTel provider, so they must NOT run in parallel.

func TestWire_DefaultConfig_Succeeds(t *testing.T) {
	cfg := config.ObservabilityConfig{}
	// Metrics server is disabled by default (Enabled=false), so no port conflict.
	stack, err := observability.Wire(cfg, "tierfs-test")
	require.NoError(t, err)
	require.NotNil(t, stack)

	assert.NotNil(t, stack.Log)
	assert.NotNil(t, stack.Metrics)
	assert.NotNil(t, stack.Tracer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stack.Shutdown(ctx)
}

func TestWire_Shutdown_NoError(t *testing.T) {
	cfg := config.ObservabilityConfig{}
	stack, err := observability.Wire(cfg, "tierfs-test")
	require.NoError(t, err)

	// Shutdown should not panic or hang.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.NotPanics(t, func() {
		stack.Shutdown(ctx)
	})
}

func TestWire_BadLoggingConfig_Error(t *testing.T) {
	cfg := config.ObservabilityConfig{
		Logging: config.LoggingConfig{
			Level: "banana",
		},
	}
	stack, err := observability.Wire(cfg, "tierfs-test")
	require.Error(t, err)
	assert.Nil(t, stack)
	assert.Contains(t, err.Error(), "unknown log level")
}
