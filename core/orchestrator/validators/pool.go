package validators

import (
	"fmt"

	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// StandardPoolValidator implements validation for standard pools
type StandardPoolValidator struct{}

func (v *StandardPoolValidator) ValidateSize(params *commonparams.CreatePoolParams) error {
	if params.SizeInBytes < minQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("Given pool size not supported. Pool size must be greater than %s and a multiple of 1GiB",
				utils.FmtUint64Bytes(minQuotaInBytesPool)))
	}

	if params.SizeInBytes > maxQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("Given pool size not supported. Pool size must be less than %s",
				utils.FmtUint64Bytes(maxQuotaInBytesPool)))
	}
	return nil
}

func (v *StandardPoolValidator) ValidateThroughput(params *commonparams.CreatePoolParams) error {
	// Handle nil CustomPerformanceParams - skip validation (QoS validation done in common params)
	if params.CustomPerformanceParams == nil {
		return nil
	}

	// Check for negative values first
	if params.CustomPerformanceParams.ThroughputMibps < 0 {
		return customerrors.NewUserInputValidationErr(
			"TotalThroughputMibps must be set and must be greater than 0")
	}

	if err := ValidateThroughputRange(params.CustomPerformanceParams.ThroughputMibps, minCustomThroughput, maxCustomThroughput); err != nil {
		return err
	}

	return nil
}

func (v *StandardPoolValidator) ValidateIops(params *commonparams.CreatePoolParams) error {
	// Use shared IOPS validation logic with standard pool parameters
	return validateIopsCommon(params, minCustomIops, maxCustomIops, iopsPerMiBps, "")
}
