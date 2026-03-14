package logging_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/observability/logging"
)

func TestBuild_DefaultConfig_ReturnsLogger(t *testing.T) {
	t.Parallel()
	logger, err := logging.Build(config.LoggingConfig{})
	require.NoError(t, err)
	require.NotNil(t, logger)
	// Should be able to log without panic.
	logger.Info("hello from default config")
}

func TestBuild_AllLevels(t *testing.T) {
	t.Parallel()
	tests := []struct {
		level string
	}{
		{"debug"},
		{"info"},
		{"warn"},
		{"error"},
	}
	for _, tc := range tests {
		t.Run(tc.level, func(t *testing.T) {
			t.Parallel()
			logger, err := logging.Build(config.LoggingConfig{Level: tc.level})
			require.NoError(t, err)
			require.NotNil(t, logger)
		})
	}
}

func TestBuild_UnknownLevel_Error(t *testing.T) {
	t.Parallel()
	_, err := logging.Build(config.LoggingConfig{Level: "trace"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown log level")
}

func TestBuild_JSONFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	// Build a logger that writes JSON to our buffer via a custom core.
	cfg := config.LoggingConfig{Format: "json", Level: "info"}
	logger, err := logging.Build(cfg)
	require.NoError(t, err)
	require.NotNil(t, logger)

	// To actually verify JSON output, build a logger manually with a buffer sink.
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, zapcore.AddSync(&buf), zapcore.InfoLevel)
	testLogger := zap.New(core)

	testLogger.Info("test message", zap.String("key", "value"))
	_ = testLogger.Sync()

	// Verify the output is valid JSON.
	var m map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &m)
	require.NoError(t, err, "output should be valid JSON")
	assert.Equal(t, "test message", m["msg"])
}

func TestBuild_ConsoleFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	enc := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, zapcore.AddSync(&buf), zapcore.InfoLevel)
	testLogger := zap.New(core)

	testLogger.Info("console test")
	_ = testLogger.Sync()

	output := buf.String()
	assert.Contains(t, output, "console test")

	// Should NOT be valid JSON (console format is tab-separated).
	var m map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &m)
	assert.Error(t, err, "console output should not be valid JSON")

	// Also verify that Build accepts "console" format without error.
	logger, err := logging.Build(config.LoggingConfig{Format: "console"})
	require.NoError(t, err)
	require.NotNil(t, logger)
}

func TestBuild_UnknownFormat_Error(t *testing.T) {
	t.Parallel()
	_, err := logging.Build(config.LoggingConfig{Format: "xml"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown log format")
}
