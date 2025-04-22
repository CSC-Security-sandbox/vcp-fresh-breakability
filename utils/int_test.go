package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetConstraintInteger(t *testing.T) {
	t.Run("WhenValueBelowMin", func(tt *testing.T) {
		assert.Equal(tt, 15, GetConstraintInteger(-1, 0, 120, 15))
	})
	t.Run("WhenValueAtMin", func(tt *testing.T) {
		assert.Equal(tt, 0, GetConstraintInteger(0, 0, 120, 15))
	})
	t.Run("WhenValueAboveMax", func(tt *testing.T) {
		assert.Equal(tt, 15, GetConstraintInteger(121, 0, 120, 15))
	})
	t.Run("WhenValueAtMax", func(tt *testing.T) {
		assert.Equal(tt, 120, GetConstraintInteger(120, 0, 120, 15))
	})
	t.Run("WhenInRange", func(tt *testing.T) {
		assert.Equal(tt, 35, GetConstraintInteger(35, 0, 120, 15))
	})
}

func TestConstrainedCastUint64(t *testing.T) {
	t.Run("WhenValueAboveMax", func(tt *testing.T) {
		assert.Equal(tt, int64(math.MaxInt64), ConstrainedCastUint64(uint64(math.MaxInt64+10)))
	})
	t.Run("WhenInRange", func(tt *testing.T) {
		assert.Equal(tt, int64(65), ConstrainedCastUint64(uint64(65)))
	})
}
