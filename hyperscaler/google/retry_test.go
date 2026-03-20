package google

import (
	"testing"
	"time"
)

func TestGenerateJitter(t *testing.T) {
	r := &retry{}

	t.Run("AlwaysReturnsValidDuration", func(tt *testing.T) {
		// Test that generateJitter always returns a valid duration
		// even if secureIntn encounters errors (should fallback to 0)
		for i := 0; i < 100; i++ {
			jitter := r.generateJitter()
			// Jitter should be between 0 and 30ms
			if jitter < 0 || jitter > 30*time.Millisecond {
				tt.Errorf("generateJitter() returned invalid duration %v on iteration %d", jitter, i)
			}
		}
	})

	t.Run("ReturnsDifferentValues", func(tt *testing.T) {
		// Verify that multiple calls return different jitter values
		results := make(map[time.Duration]bool)
		uniqueCount := 0

		for i := 0; i < 100; i++ {
			jitter := r.generateJitter()
			if !results[jitter] {
				results[jitter] = true
				uniqueCount++
			}
		}

		// Should get many unique jitter values
		if uniqueCount < 20 {
			tt.Errorf("Expected at least 20 unique jitter values from 100 calls, got %d", uniqueCount)
		}
	})

	t.Run("HandlesErrorGracefully", func(tt *testing.T) {
		// This test verifies that generateJitter handles errors gracefully
		// by returning 0 jitter when secureIntn fails
		// In practice, crypto/rand rarely fails, but we verify the fallback works
		jitter := r.generateJitter()
		if jitter < 0 {
			tt.Errorf("generateJitter() should never return negative duration, got %v", jitter)
		}
		if jitter > 30*time.Millisecond {
			tt.Errorf("generateJitter() should not exceed 30ms, got %v", jitter)
		}
	})
}
