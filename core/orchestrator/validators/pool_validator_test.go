package validators

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestNewPoolValidator(t *testing.T) {
	t.Run("ReturnsStandardPoolValidatorWhenIsLargeCapacityIsFalse", func(tt *testing.T) {
		validator := NewPoolValidator(false)
		assert.IsType(tt, &StandardPoolValidator{}, validator)
	})

	t.Run("ReturnsLargeCapacityPoolValidatorWhenIsLargeCapacityIsTrue", func(tt *testing.T) {
		validator := NewPoolValidator(true)
		assert.IsType(tt, &LargeCapacityPoolValidator{}, validator)
	})
}

func TestNewValidationPipeline(t *testing.T) {
	t.Run("CreatesValidationPipelineWithCorrectValidator", func(tt *testing.T) {
		validator := &StandardPoolValidator{}
		pipeline := NewValidationPipeline(validator)

		assert.NotNil(tt, pipeline)
		assert.Equal(tt, validator, pipeline.validator)
		assert.Len(tt, pipeline.steps, 3)
	})
}

func TestValidationPipeline_Execute(t *testing.T) {
	t.Run("ExecutesAllStepsSuccessfully", func(tt *testing.T) {
		validator := &StandardPoolValidator{}
		pipeline := NewValidationPipeline(validator)

		iopsValue := int64(minCustomThroughput+10) * 16 // Ensure IOPS meets minimum requirement (throughput * 16)
		params := &commonparams.CreatePoolParams{
			SizeInBytes: minQuotaInBytesPool * 2,
			QosType:     "auto",
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				Iops:            &iopsValue,
				ThroughputMibps: int64(minCustomThroughput + 10),
			},
		}

		err := pipeline.Execute(params)
		assert.NoError(tt, err)
	})

	t.Run("StopsExecutionOnFirstError", func(tt *testing.T) {
		validator := &StandardPoolValidator{}
		pipeline := NewValidationPipeline(validator)

		// Use invalid size to trigger error in first step
		iopsValue := int64(minCustomThroughput+10) * 16 // Valid IOPS for the throughput
		params := &commonparams.CreatePoolParams{
			SizeInBytes: minQuotaInBytesPool - 1, // Invalid size
			QosType:     "auto",
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				Iops:            &iopsValue,
				ThroughputMibps: int64(minCustomThroughput + 10),
			},
		}

		err := pipeline.Execute(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "must be greater than")
	})
}

func TestValidateCommonPoolParams(t *testing.T) {
	t.Run("ValidatesSizeGranularity", func(tt *testing.T) {
		testCases := []struct {
			name           string
			sizeInBytes    uint64
			expectedError  bool
			errorSubstring string
		}{
			{
				name:          "Valid size that is multiple of granularity",
				sizeInBytes:   minSizeGranularity * 2,
				expectedError: false,
			},
			{
				name:          "Valid size equal to granularity",
				sizeInBytes:   minSizeGranularity,
				expectedError: false,
			},
			{
				name:           "Invalid size not multiple of granularity",
				sizeInBytes:    minSizeGranularity + 1,
				expectedError:  true,
				errorSubstring: "must be a multiple of",
			},
			{
				name:          "Valid size zero",
				sizeInBytes:   0,
				expectedError: false,
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				iopsValue := int64(minCustomIops + 100)
				params := &commonparams.CreatePoolParams{
					SizeInBytes: tc.sizeInBytes,
					QosType:     "auto",
					CustomPerformanceParams: &commonparams.CustomPerformanceParams{
						Iops: &iopsValue,
					},
				}

				err := ValidateCommonPoolParams(params)
				if tc.expectedError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tc.errorSubstring)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("ValidatesQosType", func(tt *testing.T) {
		testCases := []struct {
			name          string
			qosType       string
			expectedError bool
		}{
			{
				name:          "Valid QoS type 'auto'",
				qosType:       "auto",
				expectedError: false,
			},
			{
				name:          "Invalid QoS type 'manual'",
				qosType:       "manual",
				expectedError: true,
			},
			{
				name:          "Invalid QoS type 'performance'",
				qosType:       "performance",
				expectedError: true,
			},
			{
				name:          "Invalid QoS type empty string",
				qosType:       "",
				expectedError: true,
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				iopsValue := int64(minCustomIops + 100)
				params := &commonparams.CreatePoolParams{
					SizeInBytes: minSizeGranularity * 2,
					QosType:     tc.qosType,
					CustomPerformanceParams: &commonparams.CustomPerformanceParams{
						Iops: &iopsValue,
					},
				}

				err := ValidateCommonPoolParams(params)
				if tc.expectedError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), "QoS type not supported")
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	// Note: ValidateCommonPoolParams doesn't validate IOPS anymore - that's done by individual validators

	t.Run("ValidatesAllParametersTogether", func(tt *testing.T) {
		// Valid case - all parameters are correct
		params := &commonparams.CreatePoolParams{
			SizeInBytes: minSizeGranularity * 2,
			QosType:     "auto",
		}

		err := ValidateCommonPoolParams(params)
		assert.NoError(t, err)
	})

	t.Run("ReturnsUserInputValidationError", func(tt *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes: minSizeGranularity + 1, // Invalid size
			QosType:     "auto",
		}

		err := ValidateCommonPoolParams(params)
		assert.Error(t, err)

		// Check that it's the correct error type
		var userInputErr *customerrors.UserInputValidationErr
		assert.ErrorAs(t, err, &userInputErr)
	})
}

func TestConstants(t *testing.T) {
	t.Run("GibInBytesHasCorrectValue", func(tt *testing.T) {
		expected := int(1073741824) // 1 GiB in bytes
		assert.Equal(tt, expected, utils.GiBInBytes)
	})

	t.Run("TibInBytesHasCorrectValue", func(tt *testing.T) {
		expected := int(1099511627776) // 1 TiB in bytes
		assert.Equal(tt, expected, utils.TiBInBytes)
	})

	t.Run("PibInBytesHasCorrectValue", func(tt *testing.T) {
		expected := int(1125899906842624) // 1 PiB in bytes
		assert.Equal(tt, expected, utils.PiBInBytes)
	})
}

func TestVariables(t *testing.T) {
	t.Run("minCustomIopsIsSet", func(tt *testing.T) {
		assert.Greater(tt, minCustomIops, uint64(0))
	})

	t.Run("minSizeGranularityIsSet", func(tt *testing.T) {
		assert.Greater(tt, minSizeGranularity, uint64(0))
		assert.Equal(tt, uint64(utils.GiBInBytes), minSizeGranularity) // Default value should be 1 GiB
	})
}
