package validators

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestStandardPoolValidator_ValidateSize(t *testing.T) {
	validator := &StandardPoolValidator{}

	t.Run("ValidatesSizeWithinRange", func(tt *testing.T) {
		testCases := []struct {
			name          string
			sizeInBytes   uint64
			expectedError bool
		}{
			{
				name:          "Valid size at minimum boundary",
				sizeInBytes:   minQuotaInBytesPool,
				expectedError: false,
			},
			{
				name:          "Valid size above minimum",
				sizeInBytes:   minQuotaInBytesPool * 2,
				expectedError: false,
			},
			{
				name:          "Valid size at maximum boundary",
				sizeInBytes:   maxQuotaInBytesPool,
				expectedError: false,
			},
			{
				name:          "Valid size below maximum",
				sizeInBytes:   maxQuotaInBytesPool - 1,
				expectedError: false,
			},
			{
				name:          "Valid size in middle range",
				sizeInBytes:   minQuotaInBytesPool + (maxQuotaInBytesPool-minQuotaInBytesPool)/2,
				expectedError: false,
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				params := &commonparams.CreatePoolParams{
					SizeInBytes: tc.sizeInBytes,
				}

				err := validator.ValidateSize(params)
				if tc.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("ReturnsErrorForSizeBelowMinimum", func(tt *testing.T) {
		testCases := []struct {
			name           string
			sizeInBytes    uint64
			errorSubstring string
		}{
			{
				name:           "Size just below minimum",
				sizeInBytes:    minQuotaInBytesPool - 1,
				errorSubstring: "must be greater than",
			},
			{
				name:           "Size way below minimum",
				sizeInBytes:    minQuotaInBytesPool / 2,
				errorSubstring: "must be greater than",
			},
			{
				name:           "Size zero",
				sizeInBytes:    0,
				errorSubstring: "must be greater than",
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				params := &commonparams.CreatePoolParams{
					SizeInBytes: tc.sizeInBytes,
				}

				err := validator.ValidateSize(params)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorSubstring)
				assert.Contains(t, err.Error(), "multiple of 1GiB")
			})
		}
	})

	t.Run("ReturnsErrorForSizeAboveMaximum", func(tt *testing.T) {
		testCases := []struct {
			name           string
			sizeInBytes    uint64
			errorSubstring string
		}{
			{
				name:           "Size just above maximum",
				sizeInBytes:    maxQuotaInBytesPool + 1,
				errorSubstring: "must be less than",
			},
			{
				name:           "Size way above maximum",
				sizeInBytes:    maxQuotaInBytesPool * 2,
				errorSubstring: "must be less than",
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				params := &commonparams.CreatePoolParams{
					SizeInBytes: tc.sizeInBytes,
				}

				err := validator.ValidateSize(params)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorSubstring)
			})
		}
	})

	t.Run("ReturnsUserInputValidationError", func(tt *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes: minQuotaInBytesPool - 1, // Invalid size
		}

		err := validator.ValidateSize(params)
		assert.Error(t, err)

		// Check that it's the correct error type
		var userInputErr *customerrors.UserInputValidationErr
		assert.ErrorAs(t, err, &userInputErr)
	})

	t.Run("ErrorMessagesIncludeFormattedSize", func(tt *testing.T) {
		// Test minimum size error
		params := &commonparams.CreatePoolParams{
			SizeInBytes: minQuotaInBytesPool - 1,
		}

		err := validator.ValidateSize(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "1TiB") // The formatted value should be 1TiB

		// Test maximum size error
		params.SizeInBytes = maxQuotaInBytesPool + 1
		err = validator.ValidateSize(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "425TiB") // The actual formatted value
	})
}

func TestStandardPoolValidator_ValidateThroughput(t *testing.T) {
	validator := &StandardPoolValidator{}

	t.Run("ValidatesThroughputAboveMinimum", func(tt *testing.T) {
		testCases := []struct {
			name            string
			throughputMibps int64
			expectedError   bool
		}{
			{
				name:            "Valid throughput at minimum boundary",
				throughputMibps: int64(minCustomThroughput),
				expectedError:   false,
			},
			{
				name:            "Valid throughput above minimum",
				throughputMibps: int64(minCustomThroughput + 10),
				expectedError:   false,
			},
			{
				name:            "Valid throughput much above minimum",
				throughputMibps: int64(minCustomThroughput * 2),
				expectedError:   false,
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				params := &commonparams.CreatePoolParams{
					CustomPerformanceParams: &commonparams.CustomPerformanceParams{
						ThroughputMibps: tc.throughputMibps,
					},
				}

				err := validator.ValidateThroughput(params)
				if tc.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("ReturnsErrorForThroughputBelowMinimum", func(tt *testing.T) {
		testCases := []struct {
			name            string
			throughputMibps int64
			errorSubstring  string
		}{
			{
				name:            "Throughput just below minimum",
				throughputMibps: int64(minCustomThroughput - 1),
				errorSubstring:  "must be between",
			},
			{
				name:            "Throughput way below minimum",
				throughputMibps: int64(minCustomThroughput / 2),
				errorSubstring:  "must be between",
			},
			{
				name:            "Throughput zero",
				throughputMibps: 0,
				errorSubstring:  "must be between",
			},
			{
				name:            "Throughput negative",
				throughputMibps: -100,
				errorSubstring:  "must be greater than 0",
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				params := &commonparams.CreatePoolParams{
					CustomPerformanceParams: &commonparams.CustomPerformanceParams{
						ThroughputMibps: tc.throughputMibps,
					},
				}

				err := validator.ValidateThroughput(params)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorSubstring)
				if tc.name != "Throughput negative" {
					assert.Contains(t, err.Error(), "MiBps")
				}
			})
		}
	})

	t.Run("ReturnsUserInputValidationError", func(tt *testing.T) {
		params := &commonparams.CreatePoolParams{
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: int64(minCustomThroughput - 1), // Invalid throughput
			},
		}

		err := validator.ValidateThroughput(params)
		assert.Error(t, err)

		// Check that it's the correct error type
		var userInputErr *customerrors.UserInputValidationErr
		assert.ErrorAs(t, err, &userInputErr)
	})

	t.Run("ErrorMessagesIncludeMinimumThroughput", func(tt *testing.T) {
		params := &commonparams.CreatePoolParams{
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: int64(minCustomThroughput - 1),
			},
		}

		err := validator.ValidateThroughput(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("%d", minCustomThroughput))
	})

	t.Run("HandlesNilCustomPerformanceParams", func(tt *testing.T) {
		params := &commonparams.CreatePoolParams{
			CustomPerformanceParams: nil,
		}

		err := validator.ValidateThroughput(params)
		assert.NoError(t, err) // Should pass - QoS validation is done in common params
	})
}

func TestPoolVariables(t *testing.T) {
	t.Run("minCustomThroughputIsSet", func(tt *testing.T) {
		assert.Greater(tt, minCustomThroughput, uint64(0))
	})

	t.Run("minQuotaInBytesPoolIsSet", func(tt *testing.T) {
		assert.Greater(tt, minQuotaInBytesPool, uint64(0))
		assert.Equal(tt, uint64(1*utils.TiBInBytes), minQuotaInBytesPool) // Default value should be 1 TiB
	})

	t.Run("maxQuotaInBytesPoolIsSet", func(tt *testing.T) {
		assert.Greater(tt, maxQuotaInBytesPool, uint64(0))
		assert.Equal(tt, uint64(425*utils.TiBInBytes), maxQuotaInBytesPool) // Default value should be 425 TiB
	})

	t.Run("maxQuotaInBytesPoolIsGreaterThanminQuotaInBytesPool", func(tt *testing.T) {
		assert.Greater(tt, maxQuotaInBytesPool, minQuotaInBytesPool)
	})
}

func TestStandardPoolValidator_Integration(t *testing.T) {
	validator := &StandardPoolValidator{}

	t.Run("ValidatesCompletePoolParams", func(tt *testing.T) {
		// Valid case - all parameters are correct
		params := &commonparams.CreatePoolParams{
			SizeInBytes: minQuotaInBytesPool * 2,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: int64(minCustomThroughput + 10),
			},
		}

		// Test size validation
		err := validator.ValidateSize(params)
		assert.NoError(t, err)

		// Test throughput validation
		err = validator.ValidateThroughput(params)
		assert.NoError(t, err)
	})

	t.Run("FailsWithInvalidSizeAndThroughput", func(tt *testing.T) {
		// Invalid case - both size and throughput are wrong
		params := &commonparams.CreatePoolParams{
			SizeInBytes: minQuotaInBytesPool - 1, // Invalid size
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: int64(minCustomThroughput - 1), // Invalid throughput
			},
		}

		// Test size validation
		err := validator.ValidateSize(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be greater than")

		// Test throughput validation
		err = validator.ValidateThroughput(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between")
	})
}
