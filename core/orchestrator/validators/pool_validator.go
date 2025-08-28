package validators

import (
	"fmt"

	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// Common validation constants used across validators
var (
	// Pool size limits
	minQuotaInBytesPool = utils.MinQuotaInBytesPool
	maxQuotaInBytesPool = utils.MaxQuotaInBytesPool
	minSizeGranularity  = utils.MinSizeGranularity
	minHotTierSize      = utils.MinHotTierSize

	// Performance limits (shared)
	minCustomThroughput = utils.MinCustomThroughput
	maxCustomThroughput = utils.MaxCustomThroughput
	minCustomIops       = utils.MinCustomIops
	maxCustomIops       = utils.MaxCustomIops
	iopsPerMiBps        = utils.IopsPerMiBps

	// Large capacity specific limits
	minLvCoolTierCapacity = utils.MinLvCoolTierCapacity
	maxLvPoolCapacity     = utils.MaxLvPoolCapacity
	maxLvHotTierCapacity  = utils.MaxLvHotTierCapacity
	minLvThroughput       = utils.MinLvThroughput
	maxLvThroughput       = utils.MaxLvThroughput
)

// PoolValidator defines the interface for pool validation strategies
type PoolValidator interface {
	ValidateSize(params *commonparams.CreatePoolParams) error
	ValidateThroughput(params *commonparams.CreatePoolParams) error
	ValidateIops(params *commonparams.CreatePoolParams) error
}

// NewPoolValidator creates the appropriate validator based on pool type
func NewPoolValidator(isLargeCapacity bool) PoolValidator {
	if isLargeCapacity {
		return &LargeCapacityPoolValidator{}
	}
	return &StandardPoolValidator{}
}

// ValidationPipeline represents a sequence of validation steps
type ValidationPipeline struct {
	validator PoolValidator
	steps     []func(PoolValidator, *commonparams.CreatePoolParams) error
}

func NewValidationPipeline(validator PoolValidator) *ValidationPipeline {
	return &ValidationPipeline{
		validator: validator,
		steps: []func(PoolValidator, *commonparams.CreatePoolParams) error{
			func(v PoolValidator, p *commonparams.CreatePoolParams) error { return v.ValidateSize(p) },
			func(v PoolValidator, p *commonparams.CreatePoolParams) error { return v.ValidateThroughput(p) },
			func(v PoolValidator, p *commonparams.CreatePoolParams) error { return v.ValidateIops(p) },
		},
	}
}

func (vp *ValidationPipeline) Execute(params *commonparams.CreatePoolParams) error {
	for _, step := range vp.steps {
		if err := step(vp.validator, params); err != nil {
			return err
		}
	}
	return nil
}

// Common IOPS validation function to eliminate duplication
func validateIopsCommon(params *commonparams.CreatePoolParams, minIops, maxIops uint64, iopsPerMiBpsValue uint64, errorSuffix string) error {
	// Handle nil CustomPerformanceParams - skip validation
	if params.CustomPerformanceParams == nil {
		return nil
	}

	// Check if IOPS needs to be calculated from throughput (when IOPS is nil)
	if params.CustomPerformanceParams.Iops == nil && params.CustomPerformanceParams.ThroughputMibps > 0 {
		// Calculate IOPS from throughput
		calculatedIops := params.CustomPerformanceParams.ThroughputMibps * int64(iopsPerMiBpsValue)
		if calculatedIops > int64(maxIops) {
			calculatedIops = int64(maxIops)
		}
		params.CustomPerformanceParams.Iops = &calculatedIops
	}

	// Check for negative values (after potential calculation)
	if params.CustomPerformanceParams.Iops != nil && *params.CustomPerformanceParams.Iops < 0 {
		baseMsg := "TotalIops must be set and must be greater than 0"
		if errorSuffix != "" {
			baseMsg = fmt.Sprintf("Iops must be set and must be greater than %d%s", minIops, errorSuffix)
		}
		return customerrors.NewUserInputValidationErr(baseMsg)
	}

	// Validate IOPS range and business rules (only if IOPS is set)
	if params.CustomPerformanceParams.Iops != nil {
		if err := ValidateIopsRange(*params.CustomPerformanceParams.Iops, minIops, maxIops); err != nil {
			if errorSuffix != "" {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("%s%s", err.Error(), errorSuffix))
			}
			return err
		}

		// Validate IOPS meets minimum requirement for throughput (throughput * 16)
		if params.CustomPerformanceParams.ThroughputMibps > 0 {
			minimumIops := params.CustomPerformanceParams.ThroughputMibps * 16
			if *params.CustomPerformanceParams.Iops < minimumIops {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf(
					"TotalIops must be at least %d IOPS (minimum for %d MiBps throughput)%s",
					minimumIops, params.CustomPerformanceParams.ThroughputMibps, errorSuffix))
			}
		}
	}

	return nil
}

// ValidateCommonPoolParams validates parameters that are common to all pool types
func ValidateCommonPoolParams(params *commonparams.CreatePoolParams) error {
	if params.SizeInBytes%minSizeGranularity != 0 {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("Given pool size must be a multiple of %s", utils.FmtUint64Bytes(minSizeGranularity)))
	}

	if params.QosType != utils.QosTypeAuto {
		return customerrors.NewUserInputValidationErr(
			"Given QoS type not supported for Unified Flex Storage Pool. Supported QoS type is auto")
	}

	// Validate auto-tiering numerical parameters (moved from endpoint)
	if err := validateAutoTieringParams(params); err != nil {
		return err
	}

	return nil
}

// validateAutoTieringParams validates auto-tiering numerical parameters
func validateAutoTieringParams(params *commonparams.CreatePoolParams) error {
	if !params.AllowAutoTiering {
		return nil // No validation needed if auto-tiering is disabled
	}

	// 1. HotTierSizeInBytes must be less than or equal to pool size
	if params.HotTierSizeInBytes > params.SizeInBytes {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("Hot-tier size %s exceeds the total storage pool capacity %s.",
				utils.FmtUint64Bytes(params.HotTierSizeInBytes),
				utils.FmtUint64Bytes(params.SizeInBytes)))
	}

	// 2. HotTierSizeInBytes must be >= minimum size (1TB)
	if params.HotTierSizeInBytes < minHotTierSize {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("HotTierSizeInBytes must be between %s and a value less than the pool size %s.",
				utils.FmtUint64Bytes(minHotTierSize),
				utils.FmtUint64Bytes(params.SizeInBytes)))
	}

	return nil
}

// Helper functions for performance parameters calculation and validation
func ValidateThroughputRange(throughput int64, minThroughput uint64, maxThroughput uint64) error {
	if t := uint64(throughput); t < minThroughput || t > maxThroughput {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf(
			"TotalThroughputMibps must be between %d and %d MiBps", minThroughput, maxThroughput))
	}
	return nil
}

func ValidateIopsRange(iops int64, minIops uint64, maxIops uint64) error {
	if t := uint64(iops); t < minIops || t > maxIops {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf(
			"TotalIops must be between %d and %d IOPS", minIops, maxIops))
	}
	return nil
}
