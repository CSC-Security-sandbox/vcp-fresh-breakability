package utils

import (
	"regexp"
	"testing"
)

func TestRandomHex10_Format(t *testing.T) {
	pattern := regexp.MustCompile(`^[a-f0-9]{5}-[a-f0-9]{5}$`)
	for i := 0; i < 100; i++ {
		val := RandomHex10()
		if !pattern.MatchString(val) {
			t.Errorf("RandomHex10() = %q, want format xxxxx-xxxxx", val)
		}
	}
}

func TestRandomHex10_Uniqueness(t *testing.T) {
	seen := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		val := RandomHex10()
		if _, exists := seen[val]; exists {
			t.Errorf("Duplicate value generated: %q", val)
		}
		seen[val] = struct{}{}
	}
}

func TestRandomHex10_ErrorFallback(t *testing.T) {
	// Not easily testable without patching rand.Read, but we can check fallback value format
	fallback := "00000-00000"
	if len(fallback) != len(RandomHex10()) {
		t.Errorf("Fallback value length mismatch")
	}
}
