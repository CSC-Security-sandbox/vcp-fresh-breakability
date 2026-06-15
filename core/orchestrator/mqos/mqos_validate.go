package mqos

import (
	"context"
	"fmt"
	"math"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	enableMqos                             = env.GetBool("ENABLE_MQOS", true)
	enableInferredIops                     = env.GetBool("ENABLE_INFERRED_IOPS", false)
	enableVolumePerformanceGroupAssignment = env.GetBool("ENABLE_VOLUME_PERFORMANCE_GROUP_ASSIGNMENT", false)
	// Default mirrors the ONTAP QoS policy-group regime. A value <= 0 disables the check.
	maxVolumePerformanceGroupsPerPool = env.GetInt("MAX_VOLUME_PERFORMANCE_GROUPS_PER_POOL", 12000)
)

type PoolQosInput struct {
	// Determines if we are validating for replication or not - replication uses a different error message
	ForReplication      bool
	QosType             string
	PoolThroughputMibps int64
	PoolIops            int64
}

func ValidateVolumeQosParams(pool PoolQosInput, throughputMibps *int64, iops *int64, vpgID *string) (calculatedIops *int64, err error) {
	hasThroughput := throughputMibps != nil
	hasVpgId := vpgID != nil
	hasIops := iops != nil

	// Check mutually exclusive parameters: VPG cannot be combined with throughput or iops
	if hasVpgId && (hasThroughput || hasIops) {
		return nil, customerrors.NewUserInputValidationErr(utils.ErrMsgVpgMutuallyExclusiveWithQos)
	}

	// Auto pools ALWAYS reject throughputMibps and iops (regardless of feature flag)
	if pool.QosType == utils.QosTypeAuto {
		if hasThroughput {
			return nil, customerrors.NewUserInputValidationErr(utils.ErrMsgPoolAutoQosTypeCannotSpecifyThroughput)
		}
		if hasIops {
			return nil, customerrors.NewUserInputValidationErr(utils.ErrMsgPoolAutoQosTypeCannotSpecifyIops)
		}
		if hasVpgId {
			return nil, customerrors.NewUserInputValidationErr(utils.ErrMsgPoolAutoQosTypeCannotSpecifyVpgId)
		}
	}

	// Manual pools require either throughputMibps or volumePerformanceGroupId (if MQOS is enabled)
	if pool.QosType == utils.QosTypeManual {
		if enableMqos && !hasThroughput && !hasVpgId {
			errMsg := utils.ErrMsgPoolManualQosTypeRequiresThroughputOrVpg
			if pool.ForReplication {
				errMsg = utils.ErrMsgMQoSDestPoolNotAllowed
			}
			return nil, customerrors.NewUserInputValidationErr(errMsg)
		}
	}

	// If QoS parameters provided, validate feature flag
	if !enableMqos {
		if hasThroughput {
			return nil, customerrors.NewUserInputValidationErr(utils.ErrMsgMqosNotEnabledThroughput)
		}
		if hasIops {
			return nil, customerrors.NewUserInputValidationErr(utils.ErrMsgMqosNotEnabledIops)
		}
		if hasVpgId {
			return nil, customerrors.NewUserInputValidationErr(utils.ErrMsgMqosNotEnabledVpgId)
		}
	}

	// VPG assignment feature flag check
	if hasVpgId && !enableVolumePerformanceGroupAssignment {
		return nil, customerrors.NewUserInputValidationErr(utils.ErrMsgVpgAssignmentNotEnabled)
	}

	// IOPS must be provided when throughput is set and inferred IOPS is disabled
	if !enableInferredIops && hasThroughput && !hasIops {
		return nil, customerrors.NewUserInputValidationErr("IOPS inference is disabled. IOPS must be provided explicitly when throughputMibps is specified.")
	}

	// Validate throughput range if provided
	if throughputMibps != nil {
		minThroughput := int64(utils.MinVolumeThroughput)
		if *throughputMibps < minThroughput {
			return nil, customerrors.NewUserInputValidationErr(fmt.Sprintf("throughputMibps must be at least %d", minThroughput))
		}
	}

	// Calculate IOPS for pool capacity validation when MQOS is enabled and throughput is set
	if enableMqos && hasThroughput {
		if enableInferredIops {
			if pool.PoolThroughputMibps == 0 {
				return nil, customerrors.NewUserInputValidationErr("pool throughput totals are required for inferred IOPS calculation")
			}
			calculatedIopsValue := calculateIopsFromThroughput(*throughputMibps, pool.PoolThroughputMibps, pool.PoolIops)
			calculatedIops = &calculatedIopsValue
		} else {
			// Inferred IOPS is disabled - use the provided IOPS value
			calculatedIops = iops
		}
	}

	return calculatedIops, nil
}

// calculateIopsFromThroughput computes volume IOPS proportionally from throughput and pool totals.
func calculateIopsFromThroughput(throughputMibps, totalThroughputMibps, totalIops int64) int64 {
	if totalThroughputMibps == 0 {
		return 0
	}
	ratio := float64(throughputMibps) / float64(totalThroughputMibps)
	return int64(math.Floor(float64(totalIops) * ratio))
}

// ValidatePoolCapacityForVolume validates that adding/updating a volume's throughput/IOPS
// won't exceed the pool's total capacity. Used by both create and update flows.
// excludeVolumeID: if set, excludes this volume from the sum calculation (used for updates).
func ValidatePoolCapacityForVolume(ctx context.Context, se database.Storage, poolUUID string, newThroughputMibps *int64, newIops *int64, excludeVolumeID *int64) error {
	logger := util.GetLogger(ctx)

	pool, err := se.GetPoolByUUID(ctx, poolUUID)
	if err != nil {
		return err
	}

	if pool.PoolAttributes == nil || pool.PoolAttributes.ThroughputMibps == 0 {
		logger.Debug("Pool capacity validation skipped - no custom performance enabled", "pool_id", poolUUID)
		return nil
	}

	totalPoolThroughput := pool.PoolAttributes.ThroughputMibps
	totalPoolIops := pool.PoolAttributes.Iops

	poolView, err := se.DescribePool(ctx, pool.UUID, pool.AccountID)
	if err != nil {
		return err
	}
	if poolView == nil {
		return fmt.Errorf("pool view not found for pool ID %s", poolUUID)
	}

	totalConfiguredThroughput := int64(poolView.Throughput)
	totalConfiguredIops := poolView.Iops

	if excludeVolumeID != nil {
		volume, err := se.GetVolumeByIDAndAccountID(ctx, *excludeVolumeID, pool.AccountID)
		if err != nil {
			return err
		}
		if volume.VolumePerformanceGroup != nil {
			shouldSubtract, err := ShouldSubtractCurrentVpgContribution(ctx, se, volume)
			if err != nil {
				return err
			}
			if shouldSubtract {
				totalConfiguredThroughput -= volume.VolumePerformanceGroup.ThroughputMibps
				totalConfiguredIops -= volume.VolumePerformanceGroup.Iops
			}
		}
	}

	if newThroughputMibps != nil {
		totalConfiguredThroughput += *newThroughputMibps
	}
	if newIops != nil {
		totalConfiguredIops += *newIops
	}

	logger.Debug("Pool capacity validation",
		"pool_id", poolUUID,
		"total_configured_throughput", totalConfiguredThroughput,
		"total_pool_throughput", totalPoolThroughput,
		"total_configured_iops", totalConfiguredIops,
		"total_pool_iops", totalPoolIops)

	if totalConfiguredThroughput > totalPoolThroughput {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf(
			"Sum of configured throughput (%d MiBps) would exceed pool's total throughput (%d MiBps)",
			totalConfiguredThroughput, totalPoolThroughput))
	}
	if totalConfiguredIops > totalPoolIops {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf(
			"Sum of configured IOPS (%d) would exceed pool's total IOPS (%d)",
			totalConfiguredIops, totalPoolIops))
	}
	return nil
}

// ShouldSubtractCurrentVpgContribution reports whether the volume's current VPG contribution
// should be subtracted from pool totals (e.g. when updating a volume's VPG assignment).
// For shared VPGs with more than one member, removing a single volume does not reduce
// the shared VPG's contribution.
func ShouldSubtractCurrentVpgContribution(ctx context.Context, se database.Storage, volume *datamodel.Volume) (bool, error) {
	if volume == nil || volume.VolumePerformanceGroup == nil {
		return false, nil
	}
	if volume.VolumePerformanceGroup.IsPerVolume() {
		return true, nil
	}
	if volume.VolumePerformanceGroup.ID == 0 {
		return false, nil
	}
	volumesInCurrentVPG, err := se.GetVolumeCountByVolumePerformanceGroupID(ctx, volume.VolumePerformanceGroup.ID)
	if err != nil {
		return false, err
	}
	return volumesInCurrentVPG <= 1, nil
}

// ValidateVPGCountForPool is a fast-fail pre-flight; CreateVolumePerformanceGroupAtomic enforces the cap at insert.
func ValidateVPGCountForPool(ctx context.Context, se database.Storage, poolID int64) error {
	if maxVolumePerformanceGroupsPerPool <= 0 {
		return nil
	}
	if se == nil {
		return fmt.Errorf("storage is required for VPG count validation")
	}
	count, err := se.CountVolumePerformanceGroupsByPoolID(ctx, poolID)
	if err != nil {
		return err
	}
	if count >= int64(maxVolumePerformanceGroupsPerPool) {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf(
			"Pool has reached the maximum number of Volume Performance Groups (%d). "+
				"Delete unused VPGs to proceed.",
			maxVolumePerformanceGroupsPerPool))
	}
	return nil
}

// CreateVolumePerformanceGroupAtomic inserts a VPG with atomic cap enforcement (MAX_VOLUME_PERFORMANCE_GROUPS_PER_POOL; <=0 disables).
func CreateVolumePerformanceGroupAtomic(ctx context.Context, se database.Storage, vpg *datamodel.VolumePerformanceGroup) (*datamodel.VolumePerformanceGroup, error) {
	if se == nil {
		return nil, fmt.Errorf("storage is required for atomic VPG create")
	}
	return se.CreateVolumePerformanceGroupWithCap(ctx, vpg, maxVolumePerformanceGroupsPerPool)
}

// ShouldAddNewVpgContribution reports whether the target VPG's contribution should
// be added to pool totals when assigning/reassigning a volume.
// For shared VPGs, only the first member should add contribution.
func ShouldAddNewVpgContribution(ctx context.Context, se database.Storage, vpg *datamodel.VolumePerformanceGroup) (bool, error) {
	if vpg == nil {
		return false, nil
	}
	if vpg.IsPerVolume() {
		return true, nil
	}
	if vpg.ID == 0 {
		return true, nil
	}
	volumesInTargetVPG, err := se.GetVolumeCountByVolumePerformanceGroupID(ctx, vpg.ID)
	if err != nil {
		return false, err
	}
	return volumesInTargetVPG == 0, nil
}
