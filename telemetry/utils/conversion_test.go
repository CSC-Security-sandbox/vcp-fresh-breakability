package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMibToBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected int64
	}{
		{
			name:     "zero MiB",
			input:    0,
			expected: 0,
		},
		{
			name:     "one MiB",
			input:    1,
			expected: 1024 * 1024, // 1048576 bytes
		},
		{
			name:     "fractional MiB",
			input:    0.5,
			expected: 524288, // 0.5 * 1024 * 1024
		},
		{
			name:     "large value",
			input:    1024,
			expected: 1073741824, // 1024 * 1024 * 1024
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MibToBytes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMibHoursToGibHours(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected int64
	}{
		{
			name:     "zero MiB hours",
			input:    0,
			expected: 0,
		},
		{
			name:     "exact conversion",
			input:    1024,
			expected: 1, // 1024 MiB = 1 GiB
		},
		{
			name:     "fractional result truncated",
			input:    1536, // 1.5 GiB
			expected: 1,    // Truncated to 1
		},
		{
			name:     "large value",
			input:    2048,
			expected: 2,
		},
		{
			name:     "small value",
			input:    512,
			expected: 0, // 0.5 truncated to 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MibHoursToGibHours(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMibHoursToGibHoursWithRoundOff(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected int64
	}{
		{
			name:     "zero MiB hours",
			input:    0,
			expected: 0,
		},
		{
			name:     "exact conversion",
			input:    1024,
			expected: 1, // 1024 MiB = 1 GiB
		},
		{
			name:     "fractional with rounding",
			input:    1536, // 1.5 GiB
			expected: 1,    // Rounded using big.Float precision
		},
		{
			name:     "large value",
			input:    2048,
			expected: 2,
		},
		{
			name:     "small fractional value",
			input:    512,
			expected: 0, // 0.5 rounded
		},
		{
			name:     "precision test",
			input:    1024.5,
			expected: 1, // Should handle decimal precision
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MibHoursToGibHoursWithRoundOff(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
