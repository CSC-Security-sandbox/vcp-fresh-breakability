package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExponentialBackoffWithJitter(t *testing.T) {
	t.Run("ReturnsExpectedBaseWithoutJitter", func(tt *testing.T) {
		d := ExponentialBackoffWithJitter(0, time.Second, 10*time.Second, 0)
		assert.Equal(tt, time.Second, d)

		d = ExponentialBackoffWithJitter(1, time.Second, 10*time.Second, 0)
		assert.Equal(tt, 2*time.Second, d)

		d = ExponentialBackoffWithJitter(2, time.Second, 10*time.Second, 0)
		assert.Equal(tt, 4*time.Second, d)
	})

	t.Run("CapsAtMaxBackoff", func(tt *testing.T) {
		d := ExponentialBackoffWithJitter(10, time.Second, 5*time.Second, 0)
		assert.Equal(tt, 5*time.Second, d)
	})

	t.Run("AddsJitterWithinExpectedRange", func(tt *testing.T) {
		for i := 0; i < 50; i++ {
			d := ExponentialBackoffWithJitter(0, time.Second, 10*time.Second, 500)
			assert.GreaterOrEqual(tt, d, time.Second)
			assert.LessOrEqual(tt, d, time.Second+500*time.Millisecond)
		}
	})

	t.Run("CappedBackoffPlusJitter", func(tt *testing.T) {
		for i := 0; i < 50; i++ {
			d := ExponentialBackoffWithJitter(20, time.Second, 5*time.Second, 500)
			assert.GreaterOrEqual(tt, d, 5*time.Second)
			assert.LessOrEqual(tt, d, 5*time.Second+500*time.Millisecond)
		}
	})

	t.Run("NegativeAttemptClampedToZero", func(tt *testing.T) {
		d := ExponentialBackoffWithJitter(-1, time.Second, 10*time.Second, 0)
		assert.Equal(tt, time.Second, d)

		d = ExponentialBackoffWithJitter(-100, time.Second, 10*time.Second, 0)
		assert.Equal(tt, time.Second, d)
	})

	t.Run("VeryLargeAttemptCapsWithoutOverflow", func(tt *testing.T) {
		d := ExponentialBackoffWithJitter(100, time.Second, 5*time.Second, 0)
		assert.Equal(tt, 5*time.Second, d)

		d = ExponentialBackoffWithJitter(1000, time.Second, 5*time.Second, 0)
		assert.Equal(tt, 5*time.Second, d)
	})

	t.Run("BaseExceedsMaxBackoff", func(tt *testing.T) {
		d := ExponentialBackoffWithJitter(0, 10*time.Second, 5*time.Second, 0)
		assert.Equal(tt, 5*time.Second, d)
	})
}
