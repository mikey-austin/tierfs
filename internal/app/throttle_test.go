package app

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThrottledReader_LimitsRate(t *testing.T) {
	data := make([]byte, 4096)
	limiter := newTokenBucket(1024) // 1 KiB/s

	// Drain initial burst tokens.
	limiter.take(int(limiter.maxTokens))

	r := newThrottledReader(context.Background(), bytes.NewReader(data), limiter)

	start := time.Now()
	_, err := io.ReadAll(r)
	elapsed := time.Since(start)

	require.NoError(t, err)
	// 4096 bytes at 1024 bytes/s should take ~4 seconds (minus burst).
	assert.Greater(t, elapsed, 2*time.Second, "should be rate limited")
}

func TestThrottledReader_UnlimitedPassthrough(t *testing.T) {
	data := make([]byte, 1024*1024)
	// No limiter = no throttle (test with a very high rate).
	limiter := newTokenBucket(1024 * 1024 * 1024) // 1 GiB/s
	r := newThrottledReader(context.Background(), bytes.NewReader(data), limiter)

	start := time.Now()
	_, err := io.ReadAll(r)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Less(t, elapsed, 1*time.Second, "high limit should not delay")
}

func TestThrottledReader_RespectsContext(t *testing.T) {
	data := make([]byte, 8192)
	limiter := newTokenBucket(100) // Very slow: 100 bytes/s
	limiter.take(int(limiter.maxTokens)) // drain burst

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	r := newThrottledReader(ctx, bytes.NewReader(data), limiter)
	_, err := io.ReadAll(r)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestTokenBucket_SharedAcrossReaders(t *testing.T) {
	limiter := newTokenBucket(2048) // 2 KiB/s shared
	limiter.take(int(limiter.maxTokens)) // drain burst

	data := make([]byte, 2048) // Each reader gets 2 KiB

	start := time.Now()

	r1 := newThrottledReader(context.Background(), bytes.NewReader(data), limiter)
	io.ReadAll(r1)

	r2 := newThrottledReader(context.Background(), bytes.NewReader(data), limiter)
	io.ReadAll(r2)

	elapsed := time.Since(start)
	// 4096 total bytes at 2048 bytes/s = ~2s.
	assert.Greater(t, elapsed, 1*time.Second, "shared limiter should throttle cumulative reads")
}
