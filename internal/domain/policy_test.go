package domain_test

import (
	"testing"
	"time"

	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRule(name, match string) domain.Rule {
	return domain.Rule{Name: name, Match: match, Replicate: true}
}

func TestPolicyEngine_FirstMatchWins(t *testing.T) {
	engine := domain.NewPolicyEngine([]domain.Rule{
		makeRule("thumbs", "thumbnails/**"),
		makeRule("recs", "recordings/**"),
		makeRule("default", "**"),
	})

	r, err := engine.Match("thumbnails/cam1/foo.jpg")
	require.NoError(t, err)
	assert.Equal(t, "thumbs", r.Name)

	r, err = engine.Match("recordings/cam1/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, "recs", r.Name)

	r, err = engine.Match("something/else.txt")
	require.NoError(t, err)
	assert.Equal(t, "default", r.Name)
}

func TestPolicyEngine_NoRule(t *testing.T) {
	engine := domain.NewPolicyEngine([]domain.Rule{
		makeRule("thumbs", "thumbnails/**"),
	})
	_, err := engine.Match("recordings/cam1/clip.mp4")
	assert.ErrorIs(t, err, domain.ErrNoRule)
}

func TestPolicyEngine_DeepGlob(t *testing.T) {
	engine := domain.NewPolicyEngine([]domain.Rule{
		makeRule("recs", "recordings/**"),
		makeRule("default", "**"),
	})
	r, err := engine.Match("recordings/cam1/2026-03/13/10/00.mp4")
	require.NoError(t, err)
	assert.Equal(t, "recs", r.Name)
}

func TestParseDuration_Never(t *testing.T) {
	d, err := domain.ParseDuration("never")
	require.NoError(t, err)
	assert.True(t, d.Never)
	assert.Equal(t, "never", d.String())
}

func TestParseDuration_Values(t *testing.T) {
	cases := []struct{ s string; want time.Duration }{
		{"0s", 0},
		{"24h", 24 * time.Hour},
		{"30m", 30 * time.Minute},
	}
	for _, tc := range cases {
		d, err := domain.ParseDuration(tc.s)
		require.NoError(t, err)
		assert.False(t, d.Never)
		assert.Equal(t, tc.want, d.D)
	}
}

func TestParseDuration_Invalid(t *testing.T) {
	_, err := domain.ParseDuration("fortnight")
	assert.Error(t, err)
}
