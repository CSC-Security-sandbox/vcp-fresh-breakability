package validators

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// StandardPoolValidator implements validation for standard pools
type StandardPoolValidator struct{}

func (v *StandardPoolValidator) ValidateSize(perf *CustomPerformance) error {
	if perf.SizeInBytes < minQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("Given pool size not supported. Pool size must be greater than %s and a multiple of 1GiB",
				utils.FmtUint64Bytes(minQuotaInBytesPool)))
	}

	if perf.SizeInBytes > maxQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("Given pool size not supported. Pool size must be less than %s",
				utils.FmtUint64Bytes(maxQuotaInBytesPool)))
	}
	return nil
}

func (v *StandardPoolValidator) ValidateThroughput(perf *CustomPerformance) error {
	// Check for negative values first
	if perf.ThroughputMibps < 0 {
		return customerrors.NewUserInputValidationErr(
			"TotalThroughputMibps must be set and must be greater than 0")
	}

	if err := ValidateThroughputRange(perf.ThroughputMibps, minCustomThroughput, maxCustomThroughput); err != nil {
		return err
	}

	return nil
}

func (v *StandardPoolValidator) ValidateIops(perf *CustomPerformance) error {
	// Use shared IOPS validation logic with standard pool parameters
	return validateIopsCommon(perf, minCustomIops, maxCustomIops, iopsPerMiBps, "")
}

func (v *StandardPoolValidator) ValidateAutoTierParams(perf *CustomPerformance) error {
	return validateAutoTierCommon(perf, minHotTierSize)
}
