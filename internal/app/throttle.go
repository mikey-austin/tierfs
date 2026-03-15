package app

import (
	"context"
	"io"
	"sync"
	"time"
)

// throttledReader wraps an io.Reader and rate-limits reads to bytesPerSec.
// Uses a simple token-bucket approach with time.Sleep. The limiter is shared
// across all workers that use the same instance for a global rate limit.
type throttledReader struct {
	r       io.Reader
	ctx     context.Context
	limiter *tokenBucket
}

// tokenBucket implements a thread-safe token bucket rate limiter.
type tokenBucket struct {
	mu        sync.Mutex
	tokens    float64
	maxTokens float64
	rate      float64 // tokens per second
	lastTime  time.Time
}

func newTokenBucket(bytesPerSec int64) *tokenBucket {
	burst := float64(bytesPerSec)
	if burst > 1<<20 {
		burst = 1 << 20 // cap burst at 1 MiB
	}
	return &tokenBucket{
		tokens:    burst,
		maxTokens: burst,
		rate:      float64(bytesPerSec),
		lastTime:  time.Now(),
	}
}

func (tb *tokenBucket) take(n int) time.Duration {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastTime).Seconds()
	tb.lastTime = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}

	tb.tokens -= float64(n)
	if tb.tokens >= 0 {
		return 0
	}

	// Calculate wait time for deficit.
	wait := time.Duration(-tb.tokens / tb.rate * float64(time.Second))
	return wait
}

func newThrottledReader(ctx context.Context, r io.Reader, limiter *tokenBucket) io.Reader {
	return &throttledReader{r: r, ctx: ctx, limiter: limiter}
}

func (t *throttledReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		if wait := t.limiter.take(n); wait > 0 {
			select {
			case <-t.ctx.Done():
				return n, t.ctx.Err()
			case <-time.After(wait):
			}
		}
	}
	return n, err
}
