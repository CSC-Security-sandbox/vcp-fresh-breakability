package validators

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// Tests for CustomPerformance struct and factory functions
func TestNewCustomPerformanceFromCreate(t *testing.T) {
	t.Run("WithCustomPerformanceParams", func(t *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes:        2 * utils.TiBInBytes,
			AllowAutoTiering:   true,
			HotTierSizeInBytes: 1 * utils.TiBInBytes,
			QosType:            utils.QosTypeAuto,
			LargeCapacity:      false,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
		}

		perf := NewCustomPerformanceFromCreate(params)

		assert.Equal(t, params.SizeInBytes, perf.SizeInBytes)
		assert.Equal(t, params.CustomPerformanceParams.ThroughputMibps, perf.ThroughputMibps)
		assert.Equal(t, params.CustomPerformanceParams.Iops, perf.Iops)
		assert.Equal(t, params.AllowAutoTiering, perf.AllowAutoTiering)
		assert.Equal(t, params.HotTierSizeInBytes, perf.HotTierSizeInBytes)
		assert.Equal(t, params.QosType, perf.QosType)
		assert.Equal(t, params.LargeCapacity, perf.LargeCapacity)
	})

	t.Run("WithoutCustomPerformanceParams", func(t *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes:        1 * utils.TiBInBytes,
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            utils.QosTypeAuto,
			LargeCapacity:      true,
		}

		perf := NewCustomPerformanceFromCreate(params)

		assert.Equal(t, params.SizeInBytes, perf.SizeInBytes)
		assert.Equal(t, int64(0), perf.ThroughputMibps)
		assert.Nil(t, perf.Iops)
		assert.Equal(t, params.AllowAutoTiering, perf.AllowAutoTiering)
		assert.Equal(t, params.HotTierSizeInBytes, perf.HotTierSizeInBytes)
		assert.Equal(t, params.QosType, perf.QosType)
		assert.Equal(t, params.LargeCapacity, perf.LargeCapacity)
	})

	t.Run("WithNilCustomPerformanceParams", func(t *testing.T) {
		params := &commonparams.CreatePoolParams{
			SizeInBytes:             1 * utils.TiBInBytes,
			AllowAutoTiering:        false,
			HotTierSizeInBytes:      0,
			QosType:                 utils.QosTypeAuto,
			LargeCapacity:           false,
			CustomPerformanceParams: nil,
		}

		perf := NewCustomPerformanceFromCreate(params)

		assert.Equal(t, params.SizeInBytes, perf.SizeInBytes)
		assert.Equal(t, int64(0), perf.ThroughputMibps)
		assert.Nil(t, perf.Iops)
		assert.Equal(t, params.AllowAutoTiering, perf.AllowAutoTiering)
		assert.Equal(t, params.HotTierSizeInBytes, perf.HotTierSizeInBytes)
		assert.Equal(t, params.QosType, perf.QosType)
		assert.Equal(t, params.LargeCapacity, perf.LargeCapacity)
	})
}

func TestNewCustomPerformanceFromUpdate(t *testing.T) {
	t.Run("WithAllParameters", func(t *testing.T) {
		params := &commonparams.UpdatePoolParams{
			SizeInBytes:          3 * utils.TiBInBytes,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			AllowAutoTiering:     true,
			HotTierSizeInBytes:   1 * utils.TiBInBytes,
			QosType:              utils.QosTypeAuto,
			LargeCapacity:        true,
		}

		perf := NewCustomPerformanceFromUpdate(params)

		assert.Equal(t, params.SizeInBytes, perf.SizeInBytes)
		assert.Equal(t, params.TotalThroughputMibps, perf.ThroughputMibps)
		assert.Equal(t, params.TotalIops, perf.Iops)
		assert.Equal(t, params.AllowAutoTiering, perf.AllowAutoTiering)
		assert.Equal(t, params.HotTierSizeInBytes, perf.HotTierSizeInBytes)
		assert.Equal(t, params.QosType, perf.QosType)
		assert.Equal(t, params.LargeCapacity, perf.LargeCapacity)
	})

	t.Run("WithNilIops", func(t *testing.T) {
		params := &commonparams.UpdatePoolParams{
			SizeInBytes:          1 * utils.TiBInBytes,
			TotalThroughputMibps: 64,
			TotalIops:            nil,
			AllowAutoTiering:     false,
			HotTierSizeInBytes:   0,
			QosType:              utils.QosTypeAuto,
			LargeCapacity:        false,
		}

		perf := NewCustomPerformanceFromUpdate(params)

		assert.Equal(t, params.SizeInBytes, perf.SizeInBytes)
		assert.Equal(t, params.TotalThroughputMibps, perf.ThroughputMibps)
		assert.Nil(t, perf.Iops)
		assert.Equal(t, params.AllowAutoTiering, perf.AllowAutoTiering)
		assert.Equal(t, params.HotTierSizeInBytes, perf.HotTierSizeInBytes)
		assert.Equal(t, params.QosType, perf.QosType)
		assert.Equal(t, params.LargeCapacity, perf.LargeCapacity)
	})
}

// Tests for NewPoolValidator factory function
func TestNewPoolValidator(t *testing.T) {
	t.Run("CreatesStandardPoolValidator", func(t *testing.T) {
		validator := NewPoolValidator(false)
		assert.IsType(t, &StandardPoolValidator{}, validator)
	})

	t.Run("CreatesLargeCapacityPoolValidator", func(t *testing.T) {
		validator := NewPoolValidator(true)
		assert.IsType(t, &LargeCapacityPoolValidator{}, validator)
	})

	t.Run("StandardValidatorImplementsInterface", func(t *testing.T) {
		validator := NewPoolValidator(false)
		var _ PoolValidator = validator
	})

	t.Run("LargeCapacityValidatorImplementsInterface", func(t *testing.T) {
		validator := NewPoolValidator(true)
		var _ PoolValidator = validator
	})
}

// Tests for ValidationPipeline
func TestValidationPipeline(t *testing.T) {
	t.Run("NewValidationPipeline", func(t *testing.T) {
		validator := &StandardPoolValidator{}
		pipeline := NewValidationPipeline(validator)

		assert.NotNil(t, pipeline)
		assert.Equal(t, validator, pipeline.validator)
		assert.Len(t, pipeline.steps, 3)
	})

	t.Run("ExecuteSuccess", func(t *testing.T) {
		validator := &StandardPoolValidator{}
		pipeline := NewValidationPipeline(validator)

		perf := &CustomPerformance{
			SizeInBytes:      2 * utils.TiBInBytes,
			ThroughputMibps:  128,
			Iops:             nillable.ToPointer(int64(2048)),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := pipeline.Execute(perf)
		assert.NoError(t, err)
	})

	t.Run("ExecuteFailureOnFirstStep", func(t *testing.T) {
		validator := &StandardPoolValidator{}
		pipeline := NewValidationPipeline(validator)

		perf := &CustomPerformance{
			SizeInBytes:      0, // Invalid size
			ThroughputMibps:  128,
			Iops:             nillable.ToPointer(int64(2048)),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := pipeline.Execute(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be greater than")
	})

	t.Run("ExecuteFailureOnSecondStep", func(t *testing.T) {
		validator := &StandardPoolValidator{}
		pipeline := NewValidationPipeline(validator)

		perf := &CustomPerformance{
			SizeInBytes:      2 * utils.TiBInBytes,
			ThroughputMibps:  -1, // Invalid throughput
			Iops:             nillable.ToPointer(int64(2048)),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := pipeline.Execute(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be set and must be greater than 0")
	})

	t.Run("ExecuteFailureOnThirdStep", func(t *testing.T) {
		validator := &StandardPoolValidator{}
		pipeline := NewValidationPipeline(validator)

		perf := &CustomPerformance{
			SizeInBytes:      2 * utils.TiBInBytes,
			ThroughputMibps:  128,
			Iops:             nillable.ToPointer(int64(-1)), // Invalid IOPS
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := pipeline.Execute(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be greater than 0")
	})
}

// Tests for validateIopsCommon function
func TestValidateIopsCommon(t *testing.T) {
	t.Run("NilIOPSWithThroughput", func(t *testing.T) {
		perf := &CustomPerformance{
			ThroughputMibps: 1000,
			Iops:            nil,
		}

		err := validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, "")
		assert.NoError(t, err)
		assert.NotNil(t, perf.Iops)
		// Should calculate IOPS as throughput * iopsPerMiBps
		expectedIops := int64(1000 * iopsPerMiBps)
		assert.Equal(t, expectedIops, *perf.Iops)
	})

	t.Run("NilIOPSWithThroughputExceedingMax", func(t *testing.T) {
		perf := &CustomPerformance{
			ThroughputMibps: 10000, // Very high throughput
			Iops:            nil,
		}

		err := validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, "")
		assert.NoError(t, err)
		assert.NotNil(t, perf.Iops)
		// Should cap at maxCustomIops
		assert.Equal(t, int64(maxCustomIops), *perf.Iops)
	})

	t.Run("NegativeIOPS", func(t *testing.T) {
		perf := &CustomPerformance{
			ThroughputMibps: 1000,
			Iops:            nillable.ToPointer(int64(-1)),
		}

		err := validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be greater than 0")
	})

	t.Run("IOPSBelowMinimum", func(t *testing.T) {
		perf := &CustomPerformance{
			ThroughputMibps: 1000,
			Iops:            nillable.ToPointer(int64(minCustomIops - 1)),
		}

		err := validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between")
	})

	t.Run("IOPSAboveMaximum", func(t *testing.T) {
		perf := &CustomPerformance{
			ThroughputMibps: 1000,
			Iops:            nillable.ToPointer(int64(maxCustomIops + 1)),
		}

		err := validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between")
	})

	t.Run("IOPSBelowMinimumForThroughput", func(t *testing.T) {
		perf := &CustomPerformance{
			ThroughputMibps: 1000,
			Iops:            nillable.ToPointer(int64(1000)), // Below 16 * throughput
		}

		err := validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between")
	})

	t.Run("ValidIOPS", func(t *testing.T) {
		perf := &CustomPerformance{
			ThroughputMibps: 1000,
			Iops:            nillable.ToPointer(int64(20000)), // Above 16 * throughput
		}

		err := validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, "")
		assert.NoError(t, err)
	})

	t.Run("WithErrorSuffix", func(t *testing.T) {
		perf := &CustomPerformance{
			ThroughputMibps: 1000,
			Iops:            nillable.ToPointer(int64(-1)),
		}

		err := validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, " for large capacity pools")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "for large capacity pools")
	})
}

// Tests for ValidateCommonPoolParams function
func TestValidateCommonPoolParams(t *testing.T) {
	t.Run("ValidParameters", func(t *testing.T) {
		perf := &CustomPerformance{
			SizeInBytes:      2 * utils.TiBInBytes,
			QosType:          utils.QosTypeAuto,
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := ValidateCommonPoolParams(perf)
		assert.NoError(t, err)
	})

	t.Run("InvalidSizeGranularity", func(t *testing.T) {
		perf := &CustomPerformance{
			SizeInBytes:      2*utils.TiBInBytes + 1, // Not multiple of granularity
			QosType:          utils.QosTypeAuto,
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := ValidateCommonPoolParams(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be a multiple of")
	})

	t.Run("InvalidQosType", func(t *testing.T) {
		perf := &CustomPerformance{
			SizeInBytes:      2 * utils.TiBInBytes,
			QosType:          "performance", // Invalid QoS type
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := ValidateCommonPoolParams(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "QoS type not supported")
		assert.Contains(t, err.Error(), "Supported QoS type is auto")
	})

	t.Run("ValidSizeGranularity", func(t *testing.T) {
		// Test with size that is a multiple of minSizeGranularity
		perf := &CustomPerformance{
			SizeInBytes:      minSizeGranularity * 2, // Multiple of granularity
			QosType:          utils.QosTypeAuto,
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := ValidateCommonPoolParams(perf)
		assert.NoError(t, err)
	})
}

// Tests for validateAutoTieringParams function
func TestValidateAutoTieringParams(t *testing.T) {
	t.Run("AutoTieringDisabled", func(t *testing.T) {
		perf := &CustomPerformance{
			AllowAutoTiering:   false,
			SizeInBytes:        2 * utils.TiBInBytes,
			HotTierSizeInBytes: 1 * utils.TiBInBytes,
		}

		err := validateAutoTieringParams(perf)
		assert.NoError(t, err)
	})

	t.Run("ValidAutoTieringParameters", func(t *testing.T) {
		perf := &CustomPerformance{
			AllowAutoTiering:   true,
			SizeInBytes:        2 * utils.TiBInBytes,
			HotTierSizeInBytes: 1 * utils.TiBInBytes, // Less than pool size, >= minHotTierSize
		}

		err := validateAutoTieringParams(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("HotTierSizeExceedsPoolSize", func(t *testing.T) {
		perf := &CustomPerformance{
			AllowAutoTiering:   true,
			SizeInBytes:        1 * utils.TiBInBytes,
			HotTierSizeInBytes: 2 * utils.TiBInBytes, // Greater than pool size
		}

		err := validateAutoTieringParams(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("HotTierSizeBelowMinimum", func(t *testing.T) {
		perf := &CustomPerformance{
			AllowAutoTiering:   true,
			SizeInBytes:        2 * utils.TiBInBytes,
			HotTierSizeInBytes: minHotTierSize - 1, // Below minimum
		}

		err := validateAutoTieringParams(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("HotTierSizeAtMinimum", func(t *testing.T) {
		perf := &CustomPerformance{
			AllowAutoTiering:   true,
			SizeInBytes:        2 * utils.TiBInBytes,
			HotTierSizeInBytes: minHotTierSize, // At minimum
		}

		err := validateAutoTieringParams(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("HotTierSizeAtPoolSize", func(t *testing.T) {
		perf := &CustomPerformance{
			AllowAutoTiering:   true,
			SizeInBytes:        2 * utils.TiBInBytes,
			HotTierSizeInBytes: 2 * utils.TiBInBytes, // Equal to pool size
		}

		err := validateAutoTieringParams(perf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-Tiering feature is currently not enabled")
	})
}

// Tests for ValidateThroughputRange function
func TestValidateThroughputRange(t *testing.T) {
	t.Run("ValidThroughput", func(t *testing.T) {
		err := ValidateThroughputRange(1000, 64, 5120)
		assert.NoError(t, err)
	})

	t.Run("ThroughputAtMinimum", func(t *testing.T) {
		err := ValidateThroughputRange(64, 64, 5120)
		assert.NoError(t, err)
	})

	t.Run("ThroughputAtMaximum", func(t *testing.T) {
		err := ValidateThroughputRange(5120, 64, 5120)
		assert.NoError(t, err)
	})

	t.Run("ThroughputBelowMinimum", func(t *testing.T) {
		err := ValidateThroughputRange(63, 64, 5120)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between 64 and 5120 MiBps")
	})

	t.Run("ThroughputAboveMaximum", func(t *testing.T) {
		err := ValidateThroughputRange(5121, 64, 5120)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between 64 and 5120 MiBps")
	})

	t.Run("NegativeThroughput", func(t *testing.T) {
		err := ValidateThroughputRange(-100, 64, 5120)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between 64 and 5120 MiBps")
	})
}

// Tests for ValidateIopsRange function
func TestValidateIopsRange(t *testing.T) {
	t.Run("ValidIOPS", func(t *testing.T) {
		err := ValidateIopsRange(10000, 1000, 100000)
		assert.NoError(t, err)
	})

	t.Run("IOPSAtMinimum", func(t *testing.T) {
		err := ValidateIopsRange(1000, 1000, 100000)
		assert.NoError(t, err)
	})

	t.Run("IOPSAtMaximum", func(t *testing.T) {
		err := ValidateIopsRange(100000, 1000, 100000)
		assert.NoError(t, err)
	})

	t.Run("IOPSBelowMinimum", func(t *testing.T) {
		err := ValidateIopsRange(999, 1000, 100000)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between 1000 and 100000 IOPS")
	})

	t.Run("IOPSAboveMaximum", func(t *testing.T) {
		err := ValidateIopsRange(100001, 1000, 100000)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between 1000 and 100000 IOPS")
	})

	t.Run("NegativeIOPS", func(t *testing.T) {
		err := ValidateIopsRange(-100, 1000, 100000)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be between 1000 and 100000 IOPS")
	})
}

// Tests for constants
func TestPoolValidatorConstants(t *testing.T) {
	t.Run("PoolSizeLimits", func(t *testing.T) {
		assert.Greater(t, minQuotaInBytesPool, uint64(0))
		assert.Greater(t, maxQuotaInBytesPool, uint64(0))
		assert.Greater(t, maxQuotaInBytesPool, minQuotaInBytesPool)
		assert.Greater(t, minSizeGranularity, uint64(0))
		assert.Greater(t, minHotTierSize, uint64(0))
	})

	t.Run("PerformanceLimits", func(t *testing.T) {
		assert.Greater(t, minCustomThroughput, uint64(0))
		assert.Greater(t, maxCustomThroughput, uint64(0))
		assert.Greater(t, maxCustomThroughput, minCustomThroughput)
		assert.Greater(t, minCustomIops, uint64(0))
		assert.Greater(t, maxCustomIops, uint64(0))
		assert.Greater(t, maxCustomIops, minCustomIops)
		assert.Greater(t, iopsPerMiBps, uint64(0))
	})

	t.Run("LargeCapacityLimits", func(t *testing.T) {
		assert.Greater(t, minLvCoolTierCapacity, uint64(0))
		assert.Greater(t, maxLvPoolCapacity, uint64(0))
		assert.Greater(t, maxLvPoolCapacity, minLvCoolTierCapacity)
		assert.Greater(t, maxLvHotTierCapacity, uint64(0))
		assert.Greater(t, minLvThroughput, uint64(0))
		assert.Greater(t, maxLvThroughput, uint64(0))
		assert.Greater(t, maxLvThroughput, minLvThroughput)
	})
}

// Integration tests
func TestPoolValidatorIntegration(t *testing.T) {
	t.Run("CompleteValidationPipeline", func(t *testing.T) {
		validator := NewPoolValidator(false) // Standard pool
		pipeline := NewValidationPipeline(validator)

		perf := &CustomPerformance{
			SizeInBytes:        2 * utils.TiBInBytes,
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false, // Disable auto-tiering to avoid the global flag issue
			HotTierSizeInBytes: 0,
			QosType:            utils.QosTypeAuto,
			LargeCapacity:      false,
		}

		// Validate common parameters first
		err := ValidateCommonPoolParams(perf)
		assert.NoError(t, err)

		// Then run the validation pipeline
		err = pipeline.Execute(perf)
		assert.NoError(t, err)
	})

	t.Run("LargeCapacityValidationPipeline", func(t *testing.T) {
		validator := NewPoolValidator(true) // Large capacity pool
		pipeline := NewValidationPipeline(validator)

		perf := &CustomPerformance{
			SizeInBytes:        20 * utils.TiBInBytes,
			ThroughputMibps:    512,
			Iops:               nillable.ToPointer(int64(8192)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            utils.QosTypeAuto,
			LargeCapacity:      true,
		}

		// Validate common parameters first
		err := ValidateCommonPoolParams(perf)
		assert.NoError(t, err)

		// Then run the validation pipeline
		err = pipeline.Execute(perf)
		assert.NoError(t, err)
	})
}
