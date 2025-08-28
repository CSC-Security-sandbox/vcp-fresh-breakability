package validators

import (
	"fmt"

	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// LargeCapacityPoolValidator implements validation for large capacity pools
type LargeCapacityPoolValidator struct{}

func (v *LargeCapacityPoolValidator) ValidateSize(params *commonparams.CreatePoolParams) error {
	if params.SizeInBytes < minLvCoolTierCapacity {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("SizeInBytes must be at least %s (%d bytes) for Large Capacity pools",
				utils.FmtUint64Bytes(minLvCoolTierCapacity), minLvCoolTierCapacity))
	}

	maxCapacity := maxLvHotTierCapacity
	if params.AllowAutoTiering {
		maxCapacity = maxLvPoolCapacity
	}

	if params.SizeInBytes > maxCapacity {
		tierMsg := ""
		if params.AllowAutoTiering {
			tierMsg = " when AllowAutoTiering is true"
		}
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("SizeInBytes must be less than or equal to %s (%d bytes)%s for Large Capacity pools",
				utils.FmtUint64Bytes(maxCapacity), maxCapacity, tierMsg))
	}

	return nil
}

func (v *LargeCapacityPoolValidator) ValidateThroughput(params *commonparams.CreatePoolParams) error {
	// Handle nil CustomPerformanceParams - skip validation (QoS validation done in common params)
	if params.CustomPerformanceParams == nil {
		return nil
	}

	throughputMibps := params.CustomPerformanceParams.ThroughputMibps
	if err := ValidateThroughputRange(throughputMibps, minLvThroughput, maxLvThroughput); err != nil {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("%s for Large Capacity pools", err))
	}

	return nil
}

func (v *LargeCapacityPoolValidator) ValidateIops(params *commonparams.CreatePoolParams) error {
	// Use shared IOPS validation logic with large capacity pool parameters
	return validateIopsCommon(params, minCustomIops, maxCustomIops, utils.IopsPerMiBps, " for Large Capacity pools")
}
