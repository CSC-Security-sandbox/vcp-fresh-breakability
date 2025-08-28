package validators

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestLargeCapacityPoolValidator_ValidateSize(t *testing.T) {
	validator := &LargeCapacityPoolValidator{}

	testCases := []struct {
		name           string
		sizeInBytes    uint64
		allowAutoTier  bool
		expectedError  bool
		errorSubstring string
	}{
		// Valid cases
		{
			name:          "Valid size at minimum boundary (12TiB)",
			sizeInBytes:   minLvCoolTierCapacity,
			allowAutoTier: false,
			expectedError: false,
		},
		{
			name:          "Valid size within normal range (100TiB)",
			sizeInBytes:   100 * utils.TiBInBytes,
			allowAutoTier: false,
			expectedError: false,
		},
		{
			name:          "Valid size at maximum boundary without autoTier (5PiB)",
			sizeInBytes:   maxLvHotTierCapacity,
			allowAutoTier: false,
			expectedError: false,
		},
		{
			name:          "Valid size at maximum boundary with autoTier (20PiB)",
			sizeInBytes:   maxLvPoolCapacity,
			allowAutoTier: true,
			expectedError: false,
		},
		{
			name:          "Valid size within autoTier range (1PiB)",
			sizeInBytes:   1 * utils.PiBInBytes,
			allowAutoTier: true,
			expectedError: false,
		},

		// Invalid cases - too small
		{
			name:           "Size below minimum (11TiB)",
			sizeInBytes:    11 * utils.TiBInBytes,
			allowAutoTier:  false,
			expectedError:  true,
			errorSubstring: "must be at least 12TiB",
		},
		{
			name:           "Size way below minimum (1TiB)",
			sizeInBytes:    1 * utils.TiBInBytes,
			allowAutoTier:  false,
			expectedError:  true,
			errorSubstring: "must be at least 12TiB",
		},
		{
			name:           "Size zero",
			sizeInBytes:    0,
			allowAutoTier:  false,
			expectedError:  true,
			errorSubstring: "must be at least 12TiB",
		},

		// Invalid cases - too large without autoTier
		{
			name:           "Size above maximum without autoTier (6PiB)",
			sizeInBytes:    6 * utils.PiBInBytes,
			allowAutoTier:  false,
			expectedError:  true,
			errorSubstring: "must be less than or equal to",
		},
		{
			name:           "Size way above maximum without autoTier (100PiB)",
			sizeInBytes:    100 * utils.PiBInBytes,
			allowAutoTier:  false,
			expectedError:  true,
			errorSubstring: "must be less than or equal to",
		},

		// Invalid cases - too large with autoTier
		{
			name:           "Size above maximum with autoTier (21PiB)",
			sizeInBytes:    21 * utils.PiBInBytes,
			allowAutoTier:  true,
			expectedError:  true,
			errorSubstring: "when AllowAutoTiering is true",
		},
		{
			name:           "Size way above maximum with autoTier (25PiB)",
			sizeInBytes:    25 * utils.PiBInBytes,
			allowAutoTier:  true,
			expectedError:  true,
			errorSubstring: "when AllowAutoTiering is true",
		},

		// Edge cases - just over the boundaries
		{
			name:           "Size one byte below minimum",
			sizeInBytes:    minLvCoolTierCapacity - 1,
			allowAutoTier:  false,
			expectedError:  true,
			errorSubstring: "must be at least 12TiB",
		},
		{
			name:           "Size one byte above maximum without autoTier",
			sizeInBytes:    maxLvHotTierCapacity + 1,
			allowAutoTier:  false,
			expectedError:  true,
			errorSubstring: "must be less than or equal to",
		},
		{
			name:           "Size one byte above maximum with autoTier",
			sizeInBytes:    maxLvPoolCapacity + 1,
			allowAutoTier:  true,
			expectedError:  true,
			errorSubstring: "when AllowAutoTiering is true",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			params := &commonparams.CreatePoolParams{
				SizeInBytes:      tc.sizeInBytes,
				AllowAutoTiering: tc.allowAutoTier,
			}

			err := validator.ValidateSize(params)

			if tc.expectedError {
				assert.Error(t, err)
				if tc.errorSubstring != "" {
					assert.Contains(t, err.Error(), tc.errorSubstring)
				}
				// Verify it's a user input validation error
				var userInputErr *customerrors.UserInputValidationErr
				assert.ErrorAs(t, err, &userInputErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLargeCapacityPoolValidator_ValidateThroughput(t *testing.T) {
	validator := &LargeCapacityPoolValidator{}

	testCases := []struct {
		name            string
		throughputMibps int64
		expectedError   bool
		errorSubstring  string
	}{
		// Valid cases
		{
			name:            "Valid throughput at minimum boundary (64 MiBps)",
			throughputMibps: int64(minLvThroughput),
			expectedError:   false,
		},
		{
			name:            "Valid throughput in middle range (5000 MiBps)",
			throughputMibps: 5000,
			expectedError:   false,
		},
		{
			name:            "Valid throughput at maximum boundary (60000 MiBps)",
			throughputMibps: int64(maxLvThroughput),
			expectedError:   false,
		},
		{
			name:            "Valid throughput near maximum (59999 MiBps)",
			throughputMibps: int64(maxLvThroughput - 1),
			expectedError:   false,
		},
		{
			name:            "Valid throughput just above minimum (65 MiBps)",
			throughputMibps: int64(minLvThroughput + 1),
			expectedError:   false,
		},

		// Invalid cases - too low
		{
			name:            "Throughput below minimum (63 MiBps)",
			throughputMibps: int64(minLvThroughput - 1),
			expectedError:   true,
			errorSubstring:  fmt.Sprintf("must be between %d and %d MiBps", minLvThroughput, maxLvThroughput),
		},
		{
			name:            "Throughput way below minimum (32 MiBps)",
			throughputMibps: 32,
			expectedError:   true,
			errorSubstring:  fmt.Sprintf("must be between %d and %d MiBps", minLvThroughput, maxLvThroughput),
		},
		{
			name:            "Throughput zero",
			throughputMibps: 0,
			expectedError:   true,
			errorSubstring:  fmt.Sprintf("must be between %d and %d MiBps", minLvThroughput, maxLvThroughput),
		},
		{
			name:            "Throughput negative",
			throughputMibps: -100,
			expectedError:   true,
			errorSubstring:  fmt.Sprintf("must be between %d and %d MiBps", minLvThroughput, maxLvThroughput),
		},

		// Invalid cases - too high
		{
			name:            "Throughput above maximum (60001 MiBps)",
			throughputMibps: int64(maxLvThroughput + 1),
			expectedError:   true,
			errorSubstring:  fmt.Sprintf("must be between %d and %d MiBps", minLvThroughput, maxLvThroughput),
		},
		{
			name:            "Throughput way above maximum (100000 MiBps)",
			throughputMibps: 100000,
			expectedError:   true,
			errorSubstring:  fmt.Sprintf("must be between %d and %d MiBps", minLvThroughput, maxLvThroughput),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			params := &commonparams.CreatePoolParams{
				CustomPerformanceParams: &commonparams.CustomPerformanceParams{
					ThroughputMibps: tc.throughputMibps,
				},
			}

			err := validator.ValidateThroughput(params)

			if tc.expectedError {
				assert.Error(t, err)
				if tc.errorSubstring != "" {
					assert.Contains(t, err.Error(), tc.errorSubstring)
				}
				// Verify it's a user input validation error
				var userInputErr *customerrors.UserInputValidationErr
				assert.ErrorAs(t, err, &userInputErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLargeCapacityPoolValidator_ValidateThroughput_NilCustomPerformanceParams(t *testing.T) {
	validator := &LargeCapacityPoolValidator{}

	params := &commonparams.CreatePoolParams{
		CustomPerformanceParams: nil,
	}

	err := validator.ValidateThroughput(params)

	// Should handle nil CustomPerformanceParams gracefully - no error expected
	// QoS validation is done in common params validation
	assert.NoError(t, err)
}

func TestLargeCapacityPoolValidator_IntegrationTests(t *testing.T) {
	validator := &LargeCapacityPoolValidator{}

	t.Run("Valid large capacity pool configuration", func(t *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes:      50 * utils.TiBInBytes, // 50TiB
			AllowAutoTiering: false,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: 1000, // 1000 MiBps
			},
		}

		sizeErr := validator.ValidateSize(params)
		throughputErr := validator.ValidateThroughput(params)

		assert.NoError(t, sizeErr)
		assert.NoError(t, throughputErr)
	})

	t.Run("Valid large capacity pool with auto-tiering", func(t *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes:      10 * utils.PiBInBytes, // 10PiB (within 20PiB limit for auto-tiering)
			AllowAutoTiering: true,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: 30000, // 30000 MiBps
			},
		}

		sizeErr := validator.ValidateSize(params)
		throughputErr := validator.ValidateThroughput(params)

		assert.NoError(t, sizeErr)
		assert.NoError(t, throughputErr)
	})

	t.Run("Invalid large capacity pool - size and throughput both invalid", func(t *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes:      1 * utils.TiBInBytes, // 1TiB (too small)
			AllowAutoTiering: false,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: 32, // 32 MiBps (too low)
			},
		}

		sizeErr := validator.ValidateSize(params)
		throughputErr := validator.ValidateThroughput(params)

		assert.Error(t, sizeErr)
		assert.Error(t, throughputErr)
		assert.Contains(t, sizeErr.Error(), "must be at least 12TiB")
		assert.Contains(t, throughputErr.Error(), "must be between 64 and 60000 MiBps")
	})

	t.Run("Edge case - maximum values without autoTiering", func(t *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes:      maxLvHotTierCapacity, // 5PiB (maximum without autoTier)
			AllowAutoTiering: false,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: int64(maxLvThroughput), // 60000 MiBps (maximum)
			},
		}

		sizeErr := validator.ValidateSize(params)
		throughputErr := validator.ValidateThroughput(params)

		assert.NoError(t, sizeErr)
		assert.NoError(t, throughputErr)
	})

	t.Run("Edge case - maximum values with autoTiering", func(t *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes:      maxLvPoolCapacity, // 20PiB (maximum with autoTier)
			AllowAutoTiering: true,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: int64(maxLvThroughput), // 60000 MiBps (maximum)
			},
		}

		sizeErr := validator.ValidateSize(params)
		throughputErr := validator.ValidateThroughput(params)

		assert.NoError(t, sizeErr)
		assert.NoError(t, throughputErr)
	})
}
