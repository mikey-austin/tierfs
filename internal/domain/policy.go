package domain

import (
	"fmt"
	"time"

	"github.com/bmatcuk/doublestar/v4"
)

// Duration wraps time.Duration to support the "never" sentinel.
type Duration struct {
	D     time.Duration
	Never bool
}

// ParseDuration parses a duration string, accepting "never" as a sentinel
// meaning the action should never occur automatically.
func ParseDuration(s string) (Duration, error) {
	if s == "never" {
		return Duration{Never: true}, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return Duration{}, fmt.Errorf("parse duration %q: %w", s, err)
	}
	return Duration{D: d}, nil
}

func (d Duration) String() string {
	if d.Never {
		return "never"
	}
	return d.D.String()
}

// EvictStep represents one step in an eviction schedule.
// After a file has been on its current tier for After, it is moved to ToTier.
type EvictStep struct {
	After  Duration
	ToTier string // resolved tier name
}

// PromotePolicy controls whether a file is promoted on read.
type PromotePolicy struct {
	Enabled    bool
	TargetTier string // empty = hottest (lowest priority) tier available
}

// Rule matches file paths to tiering directives.
type Rule struct {
	Name string
	// Match is a doublestar glob against the file's path relative to mount root.
	Match string
	// PinTier, if set, causes writes to land here instead of tier0.
	// The file is never automatically moved from its pin tier.
	PinTier string
	// EvictSchedule is an ordered list of (age, target-tier) pairs.
	// Each step is evaluated against the time the file arrived on its current tier.
	// First qualifying step wins.
	EvictSchedule []EvictStep
	// PromoteOnRead pulls a file back to a hotter tier when it is opened for read.
	PromoteOnRead PromotePolicy
	// Replicate controls whether files matching this rule are copied to other tiers.
	// When false, the file stays on its write tier indefinitely.
	Replicate bool
}

// PolicyEngine evaluates rules in declaration order; first match wins.
type PolicyEngine struct {
	rules []Rule
}

// NewPolicyEngine creates a PolicyEngine. Rules are evaluated in slice order.
func NewPolicyEngine(rules []Rule) *PolicyEngine {
	return &PolicyEngine{rules: rules}
}

// Match returns the first rule whose glob matches relPath.
// relPath must be relative to mount root without a leading slash.
// Returns ErrNoRule if no rule matches (caller should validate config has a catch-all).
func (e *PolicyEngine) Match(relPath string) (Rule, error) {
	for _, r := range e.rules {
		ok, err := doublestar.Match(r.Match, relPath)
		if err != nil {
			return Rule{}, fmt.Errorf("rule %q: bad glob: %w", r.Name, err)
		}
		if ok {
			return r, nil
		}
	}
	return Rule{}, ErrNoRule
}

// Rules returns a copy of the rule slice (for validation and inspection).
func (e *PolicyEngine) Rules() []Rule {
	out := make([]Rule, len(e.rules))
	copy(out, e.rules)
	return out
}
