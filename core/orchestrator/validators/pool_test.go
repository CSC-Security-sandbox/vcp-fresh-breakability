package validators

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// Tests for StandardPoolValidator
func TestStandardPoolValidator_ValidateSize(t *testing.T) {
	validator := &StandardPoolValidator{}

	testCases := []struct {
		name           string
		perf           *CustomPerformance
		expectedError  bool
		errorSubstring string
	}{
		// Valid cases
		{
			name: "Valid size at minimum boundary",
			perf: &CustomPerformance{
				SizeInBytes:   minQuotaInBytesPool,
				LargeCapacity: false,
			},
			expectedError: false,
		},
		{
			name: "Valid size above minimum",
			perf: &CustomPerformance{
				SizeInBytes:   minQuotaInBytesPool * 2,
				LargeCapacity: false,
			},
			expectedError: false,
		},
		{
			name: "Valid size at maximum boundary",
			perf: &CustomPerformance{
				SizeInBytes:   maxQuotaInBytesPool,
				LargeCapacity: false,
			},
			expectedError: false,
		},
		{
			name: "Valid size below maximum",
			perf: &CustomPerformance{
				SizeInBytes:   maxQuotaInBytesPool - 1,
				LargeCapacity: false,
			},
			expectedError: false,
		},
		{
			name: "Valid size in middle range",
			perf: &CustomPerformance{
				SizeInBytes:   minQuotaInBytesPool + (maxQuotaInBytesPool-minQuotaInBytesPool)/2,
				LargeCapacity: false,
			},
			expectedError: false,
		},

		// Invalid cases - too small
		{
			name: "Size just below minimum",
			perf: &CustomPerformance{
				SizeInBytes:   minQuotaInBytesPool - 1,
				LargeCapacity: false,
			},
			expectedError:  true,
			errorSubstring: "must be greater than",
		},
		{
			name: "Size way below minimum",
			perf: &CustomPerformance{
				SizeInBytes:   minQuotaInBytesPool / 2,
				LargeCapacity: false,
			},
			expectedError:  true,
			errorSubstring: "must be greater than",
		},
		{
			name: "Size zero",
			perf: &CustomPerformance{
				SizeInBytes:   0,
				LargeCapacity: false,
			},
			expectedError:  true,
			errorSubstring: "must be greater than",
		},

		// Invalid cases - too large
		{
			name: "Size above maximum",
			perf: &CustomPerformance{
				SizeInBytes:   maxQuotaInBytesPool + 1,
				LargeCapacity: false,
			},
			expectedError:  true,
			errorSubstring: "must be less than",
		},
		{
			name: "Size way above maximum",
			perf: &CustomPerformance{
				SizeInBytes:   maxQuotaInBytesPool * 2,
				LargeCapacity: false,
			},
			expectedError:  true,
			errorSubstring: "must be less than",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			err := validator.ValidateSize(tc.perf)

			if tc.expectedError {
				assert.Error(tt, err)
				if tc.errorSubstring != "" {
					assert.Contains(tt, err.Error(), tc.errorSubstring)
				}
				// Verify it's a user input validation error
				var userInputErr *customerrors.UserInputValidationErr
				assert.ErrorAs(tt, err, &userInputErr)
			} else {
				assert.NoError(tt, err)
			}
		})
	}
}

func TestStandardPoolValidator_ValidateThroughput(t *testing.T) {
	validator := &StandardPoolValidator{}

	testCases := []struct {
		name           string
		perf           *CustomPerformance
		expectedError  bool
		errorSubstring string
	}{
		// Valid cases
		{
			name: "Valid throughput at minimum boundary",
			perf: &CustomPerformance{
				ThroughputMibps: int64(minCustomThroughput),
				LargeCapacity:   false,
			},
			expectedError: false,
		},
		{
			name: "Valid throughput within range",
			perf: &CustomPerformance{
				ThroughputMibps: 1000, // 1 GiBps
				LargeCapacity:   false,
			},
			expectedError: false,
		},
		{
			name: "Valid throughput at maximum boundary",
			perf: &CustomPerformance{
				ThroughputMibps: int64(maxCustomThroughput),
				LargeCapacity:   false,
			},
			expectedError: false,
		},

		// Invalid cases
		{
			name: "Negative throughput",
			perf: &CustomPerformance{
				ThroughputMibps: -1,
				LargeCapacity:   false,
			},
			expectedError:  true,
			errorSubstring: "must be set and must be greater than 0",
		},
		{
			name: "Zero throughput",
			perf: &CustomPerformance{
				ThroughputMibps: 0,
				LargeCapacity:   false,
			},
			expectedError:  true,
			errorSubstring: "must be between",
		},
		{
			name: "Throughput below minimum",
			perf: &CustomPerformance{
				ThroughputMibps: int64(minCustomThroughput - 1),
				LargeCapacity:   false,
			},
			expectedError:  true,
			errorSubstring: "must be between",
		},
		{
			name: "Throughput above maximum",
			perf: &CustomPerformance{
				ThroughputMibps: int64(maxCustomThroughput + 1),
				LargeCapacity:   false,
			},
			expectedError:  true,
			errorSubstring: "must be between",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			err := validator.ValidateThroughput(tc.perf)

			if tc.expectedError {
				assert.Error(tt, err)
				if tc.errorSubstring != "" {
					assert.Contains(tt, err.Error(), tc.errorSubstring)
				}
				// Verify it's a user input validation error
				var userInputErr *customerrors.UserInputValidationErr
				assert.ErrorAs(tt, err, &userInputErr)
			} else {
				assert.NoError(tt, err)
			}
		})
	}
}

func TestStandardPoolValidator_ValidateIops(t *testing.T) {
	validator := &StandardPoolValidator{}

	testCases := []struct {
		name           string
		perf           *CustomPerformance
		expectedError  bool
		errorSubstring string
	}{
		// Valid cases
		{
			name: "Valid IOPS within range",
			perf: &CustomPerformance{
				ThroughputMibps: 1000,
				Iops:            nillable.ToPointer(int64(20000)), // Must be at least 16 * throughput
				LargeCapacity:   false,
			},
			expectedError: false,
		},
		{
			name: "Valid IOPS at minimum boundary",
			perf: &CustomPerformance{
				ThroughputMibps: int64(minCustomThroughput),
				Iops:            nillable.ToPointer(int64(minCustomIops)),
				LargeCapacity:   false,
			},
			expectedError: false,
		},
		{
			name: "Valid IOPS at maximum boundary",
			perf: &CustomPerformance{
				ThroughputMibps: int64(maxCustomThroughput),
				Iops:            nillable.ToPointer(int64(maxCustomIops)),
				LargeCapacity:   false,
			},
			expectedError: false,
		},
		{
			name: "Nil IOPS with throughput - should calculate automatically",
			perf: &CustomPerformance{
				ThroughputMibps: 1000,
				Iops:            nil, // Will be calculated from throughput
				LargeCapacity:   false,
			},
			expectedError: false,
		},

		// Invalid cases
		{
			name: "Negative IOPS",
			perf: &CustomPerformance{
				ThroughputMibps: 1000,
				Iops:            nillable.ToPointer(int64(-1)),
				LargeCapacity:   false,
			},
			expectedError:  true,
			errorSubstring: "must be greater than 0",
		},
		{
			name: "IOPS below minimum",
			perf: &CustomPerformance{
				ThroughputMibps: 1000,
				Iops:            nillable.ToPointer(int64(minCustomIops - 1)),
				LargeCapacity:   false,
			},
			expectedError:  true,
			errorSubstring: "must be between",
		},
		{
			name: "IOPS above maximum",
			perf: &CustomPerformance{
				ThroughputMibps: 1000,
				Iops:            nillable.ToPointer(int64(maxCustomIops + 1)),
				LargeCapacity:   false,
			},
			expectedError:  true,
			errorSubstring: "must be between",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			err := validator.ValidateIops(tc.perf)

			if tc.expectedError {
				assert.Error(tt, err)
				if tc.errorSubstring != "" {
					assert.Contains(tt, err.Error(), tc.errorSubstring)
				}
				// Verify it's a user input validation error
				var userInputErr *customerrors.UserInputValidationErr
				assert.ErrorAs(tt, err, &userInputErr)
			} else {
				assert.NoError(tt, err)
			}
		})
	}
}

// Tests for StandardPoolUpdateValidator

// Integration tests for the validators
func TestStandardPoolValidator_Integration(t *testing.T) {
	validator := &StandardPoolValidator{}

	t.Run("Valid standard pool configuration", func(t *testing.T) {
		perf := &CustomPerformance{
			SizeInBytes:      2 * utils.TiBInBytes, // 2TiB
			AllowAutoTiering: false,
			ThroughputMibps:  128, // 128 MiBps
			Iops:             nillable.ToPointer(int64(2048)),
			LargeCapacity:    false,
		}

		sizeErr := validator.ValidateSize(perf)
		throughputErr := validator.ValidateThroughput(perf)
		iopsErr := validator.ValidateIops(perf)

		assert.NoError(t, sizeErr)
		assert.NoError(t, throughputErr)
		assert.NoError(t, iopsErr)
	})

	t.Run("Valid standard pool with auto-tiering", func(t *testing.T) {
		perf := &CustomPerformance{
			SizeInBytes:        5 * utils.TiBInBytes, // 5TiB
			AllowAutoTiering:   true,
			ThroughputMibps:    256, // 256 MiBps
			Iops:               nillable.ToPointer(int64(4096)),
			HotTierSizeInBytes: 1 * utils.TiBInBytes,
			LargeCapacity:      false,
		}

		sizeErr := validator.ValidateSize(perf)
		throughputErr := validator.ValidateThroughput(perf)
		iopsErr := validator.ValidateIops(perf)

		assert.NoError(t, sizeErr)
		assert.NoError(t, throughputErr)
		assert.NoError(t, iopsErr)
	})

	t.Run("Invalid standard pool - size and throughput both invalid", func(t *testing.T) {
		perf := &CustomPerformance{
			SizeInBytes:      1 * utils.GiBInBytes, // 1GiB (too small)
			AllowAutoTiering: false,
			ThroughputMibps:  32, // 32 MiBps (too low)
			Iops:             nillable.ToPointer(int64(100)),
			LargeCapacity:    false,
		}

		sizeErr := validator.ValidateSize(perf)
		throughputErr := validator.ValidateThroughput(perf)
		iopsErr := validator.ValidateIops(perf)

		assert.Error(t, sizeErr)
		assert.Error(t, throughputErr)
		assert.Error(t, iopsErr)
		assert.Contains(t, sizeErr.Error(), "must be greater than")
		assert.Contains(t, throughputErr.Error(), "must be between")
	})

	t.Run("Edge case - minimum valid values", func(t *testing.T) {
		perf := &CustomPerformance{
			SizeInBytes:      minQuotaInBytesPool, // Minimum size
			AllowAutoTiering: false,
			ThroughputMibps:  int64(minCustomThroughput),               // Minimum throughput
			Iops:             nillable.ToPointer(int64(minCustomIops)), // Minimum IOPS
			LargeCapacity:    false,
		}

		sizeErr := validator.ValidateSize(perf)
		throughputErr := validator.ValidateThroughput(perf)
		iopsErr := validator.ValidateIops(perf)

		assert.NoError(t, sizeErr)
		assert.NoError(t, throughputErr)
		assert.NoError(t, iopsErr)
	})

	t.Run("Edge case - maximum valid values", func(t *testing.T) {
		perf := &CustomPerformance{
			SizeInBytes:      maxQuotaInBytesPool, // Maximum size
			AllowAutoTiering: false,
			ThroughputMibps:  int64(maxCustomThroughput),               // Maximum throughput
			Iops:             nillable.ToPointer(int64(maxCustomIops)), // Maximum IOPS
			LargeCapacity:    false,
		}

		sizeErr := validator.ValidateSize(perf)
		throughputErr := validator.ValidateThroughput(perf)
		iopsErr := validator.ValidateIops(perf)

		assert.NoError(t, sizeErr)
		assert.NoError(t, throughputErr)
		assert.NoError(t, iopsErr)
	})
}

// Tests for pool constants and variables
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

	t.Run("minCustomIopsIsSet", func(tt *testing.T) {
		assert.Greater(tt, minCustomIops, uint64(0))
	})

	t.Run("maxCustomIopsIsSet", func(tt *testing.T) {
		assert.Greater(tt, maxCustomIops, uint64(0))
		assert.Greater(tt, maxCustomIops, minCustomIops)
	})

	t.Run("iopsPerMiBpsIsSet", func(tt *testing.T) {
		assert.Greater(tt, iopsPerMiBps, uint64(0))
	})
}
