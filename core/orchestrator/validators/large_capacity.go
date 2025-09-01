package validators

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// LargeCapacityPoolValidator implements validation for large capacity pools
type LargeCapacityPoolValidator struct{}

func (v *LargeCapacityPoolValidator) ValidateSize(perf *CustomPerformance) error {
	if perf.SizeInBytes < minLvCoolTierCapacity {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("SizeInBytes must be at least %s (%d bytes) for Large Capacity pools",
				utils.FmtUint64Bytes(minLvCoolTierCapacity), minLvCoolTierCapacity))
	}

	maxCapacity := maxLvHotTierCapacity
	if perf.AllowAutoTiering {
		maxCapacity = maxLvPoolCapacity
	}

	if perf.SizeInBytes > maxCapacity {
		tierMsg := ""
		if perf.AllowAutoTiering {
			tierMsg = " when AllowAutoTiering is true"
		}
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("SizeInBytes must be less than or equal to %s (%d bytes)%s for Large Capacity pools",
				utils.FmtUint64Bytes(maxCapacity), maxCapacity, tierMsg))
	}

	return nil
}

func (v *LargeCapacityPoolValidator) ValidateThroughput(perf *CustomPerformance) error {
	// Check for negative values first
	if perf.ThroughputMibps < 0 {
		return customerrors.NewUserInputValidationErr(
			"TotalThroughputMibps must be set and must be greater than 0")
	}

	throughputMibps := perf.ThroughputMibps
	if err := ValidateThroughputRange(throughputMibps, minLvThroughput, maxLvThroughput); err != nil {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("%s for Large Capacity pools", err))
	}

	return nil
}

func (v *LargeCapacityPoolValidator) ValidateIops(perf *CustomPerformance) error {
	// Use shared IOPS validation logic with large capacity pool parameters
	return validateIopsCommon(perf, minCustomIops, maxCustomIops, utils.IopsPerMiBps, " for Large Capacity pools")
}
