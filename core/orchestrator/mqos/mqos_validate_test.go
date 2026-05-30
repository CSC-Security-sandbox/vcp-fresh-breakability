package mqos

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func int64Ptr(i int64) *int64 {
	return &i
}

func strPtr(s string) *string {
	return &s
}

// TestValidateVolumeQosParams tests ValidateVolumeQosParams (MQoS rules) with minimal setup.
// Feature flags are overridden in subtests to cover all branches.
func TestValidateVolumeQosParams(t *testing.T) {
	manualPoolWithTotals := PoolQosInput{
		QosType:             utils.QosTypeManual,
		PoolThroughputMibps: 10000,
		PoolIops:            50000,
	}

	t.Run("Rejects Throughput With VPG ID", func(tt *testing.T) {
		_, err := ValidateVolumeQosParams(manualPoolWithTotals, int64Ptr(100), nil, strPtr("vpg-uuid"))
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgVpgMutuallyExclusiveWithQos)
	})

	t.Run("Auto Pool Rejects VPG ID", func(tt *testing.T) {
		pool := PoolQosInput{QosType: utils.QosTypeAuto}
		_, err := ValidateVolumeQosParams(pool, nil, nil, strPtr("vpg-uuid"))
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgPoolAutoQosTypeCannotSpecifyVpgId)
	})

	t.Run("Manual Pool Requires Throughput Or VPG When MQOS Enabled", func(tt *testing.T) {
		_, err := ValidateVolumeQosParams(manualPoolWithTotals, nil, nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgPoolManualQosTypeRequiresThroughputOrVpg)
	})

	t.Run("Manual Pool blocked for replication when throughput and VPG not provided", func(tt *testing.T) {
		replicationPoolWithTotals := PoolQosInput{
			QosType:             utils.QosTypeManual,
			PoolThroughputMibps: 10000,
			PoolIops:            50000,
			ForReplication:      true,
		}
		_, err := ValidateVolumeQosParams(replicationPoolWithTotals, nil, nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgMQoSDestPoolNotAllowed)
	})

	t.Run("Manual Pool Allows VPG ID Without Throughput", func(tt *testing.T) {
		orig := enableVolumePerformanceGroupAssignment
		enableVolumePerformanceGroupAssignment = true
		defer func() { enableVolumePerformanceGroupAssignment = orig }()

		_, err := ValidateVolumeQosParams(manualPoolWithTotals, nil, nil, strPtr("vpg-uuid"))
		assert.NoError(tt, err)
	})

	t.Run("Rejects VPG ID When VPG Assignment Feature Flag Disabled", func(tt *testing.T) {
		orig := enableVolumePerformanceGroupAssignment
		enableVolumePerformanceGroupAssignment = false
		defer func() { enableVolumePerformanceGroupAssignment = orig }()

		_, err := ValidateVolumeQosParams(manualPoolWithTotals, nil, nil, strPtr("vpg-uuid"))
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgVpgAssignmentNotEnabled)
	})

	t.Run("Rejects IOPS When MQOS Disabled", func(tt *testing.T) {
		orig := enableMqos
		enableMqos = false
		defer func() { enableMqos = orig }()

		_, err := ValidateVolumeQosParams(manualPoolWithTotals, nil, int64Ptr(1000), nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgMqosNotEnabledIops)
	})

	t.Run("Rejects Throughput Without IOPS When Inference Disabled", func(tt *testing.T) {
		orig := enableInferredIops
		enableInferredIops = false
		defer func() { enableInferredIops = orig }()

		_, err := ValidateVolumeQosParams(manualPoolWithTotals, int64Ptr(100), nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "IOPS inference is disabled")
	})

	t.Run("Rejects Throughput Below Minimum", func(tt *testing.T) {
		orig := enableInferredIops
		enableInferredIops = true // so we reach throughput range check (not "IOPS required")
		defer func() { enableInferredIops = orig }()

		_, err := ValidateVolumeQosParams(manualPoolWithTotals, int64Ptr(0), nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "throughputMibps must be at least")
	})

	t.Run("Rejects When Pool Totals Missing For Inferred IOPS", func(tt *testing.T) {
		orig := enableInferredIops
		enableInferredIops = true
		defer func() { enableInferredIops = orig }()

		pool := PoolQosInput{QosType: utils.QosTypeManual, PoolThroughputMibps: 0, PoolIops: 0}
		_, err := ValidateVolumeQosParams(pool, int64Ptr(100), nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "pool throughput totals are required for inferred IOPS calculation")
	})

	t.Run("Uses Provided IOPS When Inference Disabled", func(tt *testing.T) {
		orig := enableInferredIops
		enableInferredIops = false
		defer func() { enableInferredIops = orig }()

		throughput := int64(100)
		iops := int64(2000)
		calculatedIops, err := ValidateVolumeQosParams(manualPoolWithTotals, &throughput, &iops, nil)
		assert.NoError(tt, err)
		assert.NotNil(tt, calculatedIops)
		assert.Equal(tt, int64(2000), *calculatedIops)
	})

	t.Run("Auto Pool Rejects Throughput", func(tt *testing.T) {
		pool := PoolQosInput{QosType: utils.QosTypeAuto}
		_, err := ValidateVolumeQosParams(pool, int64Ptr(100), nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgPoolAutoQosTypeCannotSpecifyThroughput)
	})

	t.Run("Auto Pool Rejects IOPS", func(tt *testing.T) {
		pool := PoolQosInput{QosType: utils.QosTypeAuto}
		_, err := ValidateVolumeQosParams(pool, nil, int64Ptr(1000), nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgPoolAutoQosTypeCannotSpecifyIops)
	})

	t.Run("Rejects Throughput When MQOS Disabled", func(tt *testing.T) {
		orig := enableMqos
		enableMqos = false
		defer func() { enableMqos = orig }()

		_, err := ValidateVolumeQosParams(manualPoolWithTotals, int64Ptr(100), nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgMqosNotEnabledThroughput)
	})

	t.Run("Rejects VPG When MQOS Disabled", func(tt *testing.T) {
		orig := enableMqos
		enableMqos = false
		defer func() { enableMqos = orig }()

		_, err := ValidateVolumeQosParams(manualPoolWithTotals, nil, nil, strPtr("vpg-uuid"))
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), utils.ErrMsgMqosNotEnabledVpgId)
	})

	t.Run("Allows Throughput Above 5120 When MQOS Enabled And Pool Manual", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableInferredIops := enableInferredIops
		enableMqos = true
		enableInferredIops = true
		defer func() {
			enableMqos = origEnableMqos
			enableInferredIops = origEnableInferredIops
		}()

		pool := PoolQosInput{
			QosType:             utils.QosTypeManual,
			PoolThroughputMibps: 20000,
			PoolIops:            100000,
		}
		throughput := int64(8192) // Above 5120
		calculatedIops, err := ValidateVolumeQosParams(pool, &throughput, nil, nil)
		assert.NoError(tt, err)
		assert.NotNil(tt, calculatedIops)
	})

	t.Run("Allows Throughput 10000 When Pool Capacity Allows", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableInferredIops := enableInferredIops
		enableMqos = true
		enableInferredIops = true
		defer func() {
			enableMqos = origEnableMqos
			enableInferredIops = origEnableInferredIops
		}()

		pool := PoolQosInput{
			QosType:             utils.QosTypeManual,
			PoolThroughputMibps: 15000,
			PoolIops:            75000,
		}
		throughput := int64(10000) // Above 5120, within pool capacity
		calculatedIops, err := ValidateVolumeQosParams(pool, &throughput, nil, nil)
		assert.NoError(tt, err)
		assert.NotNil(tt, calculatedIops)
	})
}

// TestValidatePoolCapacityForVolume tests ValidatePoolCapacityForVolume.
func TestValidatePoolCapacityForVolume(t *testing.T) {
	ctx := context.Background()

	t.Run("Successful Validation - Empty Pool (Create)", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 0,
			Iops:       0,
		}
		poolView.VendorID = pool.VendorID

		newThroughput := int64Ptr(1000)
		newIops := int64Ptr(5000)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, poolUUID, newThroughput, newIops, nil)
		assert.NoError(tt, err)
	})

	t.Run("Successful Validation - Within Capacity (Create)", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 5000,
			Iops:       25000,
		}

		newThroughput := int64Ptr(4000)
		newIops := int64Ptr(20000)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, pool.UUID, newThroughput, newIops, nil)
		assert.NoError(tt, err)
	})

	t.Run("Successful Validation - Exact Capacity (Create)", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 5000,
			Iops:       25000,
		}

		newThroughput := int64Ptr(5000)
		newIops := int64Ptr(25000)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, pool.UUID, newThroughput, newIops, nil)
		assert.NoError(tt, err)
	})

	t.Run("Error - Throughput Exceeds Pool Capacity (Create)", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 5000,
			Iops:       25000,
		}

		newThroughput := int64Ptr(6000)
		newIops := int64Ptr(30000)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, pool.UUID, newThroughput, newIops, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Sum of configured throughput (11000 MiBps) would exceed pool's total throughput (10000 MiBps)")
	})

	t.Run("Error - IOPS Exceeds Pool Capacity (Create)", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 5000,
			Iops:       25000,
		}

		newThroughput := int64Ptr(4000)
		newIops := int64Ptr(30000)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, pool.UUID, newThroughput, newIops, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Sum of configured IOPS (55000) would exceed pool's total IOPS (50000)")
	})

	t.Run("Successful Validation - Update Excluding Current Volume", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 5000,
			Iops:       25000,
		}

		existingVPG1 := &datamodel.VolumePerformanceGroup{
			ThroughputMibps: 3000,
			Iops:            15000,
		}

		volume1 := &datamodel.Volume{
			BaseModel:              datamodel.BaseModel{ID: 1},
			VolumePerformanceGroup: existingVPG1,
		}

		newThroughput := int64Ptr(4000)
		newIops := int64Ptr(20000)
		excludeVolumeID := int64Ptr(int64(1))

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)
		mockStorage.On("GetVolumeByIDAndAccountID", ctx, *excludeVolumeID, pool.AccountID).Return(volume1, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, pool.UUID, newThroughput, newIops, excludeVolumeID)
		assert.NoError(tt, err)
	})

	t.Run("Error - Update Exceeds Capacity (Excluding Current Volume)", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 8000,
			Iops:       40000,
		}

		existingVPG1 := &datamodel.VolumePerformanceGroup{
			ThroughputMibps: 3000,
			Iops:            15000,
		}

		volume1 := &datamodel.Volume{
			BaseModel:              datamodel.BaseModel{ID: 1},
			VolumePerformanceGroup: existingVPG1,
		}

		newThroughput := int64Ptr(6000)
		newIops := int64Ptr(30000)
		excludeVolumeID := int64Ptr(int64(1))

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)
		mockStorage.On("GetVolumeByIDAndAccountID", ctx, *excludeVolumeID, pool.AccountID).Return(volume1, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, pool.UUID, newThroughput, newIops, excludeVolumeID)
		assert.Error(tt, err)
	})

	t.Run("Skip Validation - Pool Without Custom Performance", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 0,
			},
		}

		newThroughput := int64Ptr(1000)
		newIops := int64Ptr(5000)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, pool.UUID, newThroughput, newIops, nil)
		assert.NoError(tt, err)
	})

	t.Run("Multiple Volumes - Complex Scenario", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 50000,
				Iops:            250000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 30000,
			Iops:       150000,
		}

		newThroughput := int64Ptr(15000)
		newIops := int64Ptr(75000)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, poolUUID, newThroughput, newIops, nil)
		assert.NoError(tt, err)
	})

	t.Run("Volumes Without VPGs", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		accountID := int64(10)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: accountID,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 0,
			Iops:       0,
		}
		poolView.VendorID = pool.VendorID

		newThroughput := int64Ptr(1000)
		newIops := int64Ptr(5000)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, pool.UUID, newThroughput, newIops, nil)
		assert.NoError(tt, err)
	})

	t.Run("Error - GetPoolByUUID Fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolUUID := "pool-uuid"

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(nil, errors.New("pool error"))

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, poolUUID, nil, nil, nil)
		assert.Error(tt, err)
	})

	t.Run("Error - DescribePool Fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: 10,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(nil, errors.New("describe error"))

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, poolUUID, nil, nil, nil)
		assert.Error(tt, err)
	})

	t.Run("Error - PoolView Nil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: 10,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(nil, nil)

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, poolUUID, nil, nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "pool view not found")
	})

	t.Run("Error - Exclude Volume Lookup Fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		poolID := int64(1)
		poolUUID := "pool-uuid"
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: poolID, UUID: poolUUID},
			AccountID: 10,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 10000,
				Iops:            50000,
			},
		}
		poolView := &datamodel.PoolView{
			Pool:       *pool,
			Throughput: 0,
			Iops:       0,
		}
		poolView.VendorID = pool.VendorID
		volumeID := int64(99)

		mockStorage.On("GetPoolByUUID", ctx, poolUUID).Return(pool, nil)
		mockStorage.On("DescribePool", ctx, pool.UUID, pool.AccountID).Return(poolView, nil)
		mockStorage.On("GetVolumeByIDAndAccountID", ctx, volumeID, pool.AccountID).Return(nil, errors.New("volume lookup error"))

		err := ValidatePoolCapacityForVolume(ctx, mockStorage, poolUUID, nil, nil, &volumeID)
		assert.Error(tt, err)
	})
}

func TestShouldAddNewVpgContribution(t *testing.T) {
	ctx := context.Background()

	t.Run("NilVPG", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		shouldAdd, err := ShouldAddNewVpgContribution(ctx, mockStorage, nil)
		assert.NoError(tt, err)
		assert.False(tt, shouldAdd)
	})

	t.Run("NonSharedVPG", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{ID: 1},
			IsShared:  false,
		}
		shouldAdd, err := ShouldAddNewVpgContribution(ctx, mockStorage, vpg)
		assert.NoError(tt, err)
		assert.True(tt, shouldAdd)
	})

	t.Run("SharedVPG_WithZeroVolumes", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{ID: 1},
			IsShared:  true,
		}
		mockStorage.On("GetVolumeCountByVolumePerformanceGroupID", ctx, int64(1)).Return(int64(0), nil)

		shouldAdd, err := ShouldAddNewVpgContribution(ctx, mockStorage, vpg)
		assert.NoError(tt, err)
		assert.True(tt, shouldAdd)
	})

	t.Run("SharedVPG_WithOneVolume", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{ID: 1},
			IsShared:  true,
		}
		mockStorage.On("GetVolumeCountByVolumePerformanceGroupID", ctx, int64(1)).Return(int64(1), nil)

		shouldAdd, err := ShouldAddNewVpgContribution(ctx, mockStorage, vpg)
		assert.NoError(tt, err)
		assert.False(tt, shouldAdd)
	})

	t.Run("SharedVPG_WithMultipleVolumes", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{ID: 1},
			IsShared:  true,
		}
		mockStorage.On("GetVolumeCountByVolumePerformanceGroupID", ctx, int64(1)).Return(int64(5), nil)

		shouldAdd, err := ShouldAddNewVpgContribution(ctx, mockStorage, vpg)
		assert.NoError(tt, err)
		assert.False(tt, shouldAdd)
	})

	t.Run("SharedVPG_ZeroID", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{ID: 0},
			IsShared:  true,
		}
		shouldAdd, err := ShouldAddNewVpgContribution(ctx, mockStorage, vpg)
		assert.NoError(tt, err)
		assert.True(tt, shouldAdd)
	})

	t.Run("SharedVPG_DBError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{ID: 1},
			IsShared:  true,
		}
		mockStorage.On("GetVolumeCountByVolumePerformanceGroupID", ctx, int64(1)).Return(int64(0), errors.New("db error"))

		shouldAdd, err := ShouldAddNewVpgContribution(ctx, mockStorage, vpg)
		assert.Error(tt, err)
		assert.Equal(tt, "db error", err.Error())
		assert.False(tt, shouldAdd)
	})
}

func TestValidateVPGCountForPool(t *testing.T) {
	ctx := context.Background()
	const poolID int64 = 42

	t.Run("BelowCap_ReturnsNil", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 100
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("CountVolumePerformanceGroupsByPoolID", ctx, poolID).Return(int64(50), nil)

		err := ValidateVPGCountForPool(ctx, mockStorage, poolID)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("OneBelowCap_BoundaryAllowed", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 100
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("CountVolumePerformanceGroupsByPoolID", ctx, poolID).Return(int64(99), nil)

		err := ValidateVPGCountForPool(ctx, mockStorage, poolID)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("AtCap_BoundaryRejected", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 100
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("CountVolumePerformanceGroupsByPoolID", ctx, poolID).Return(int64(100), nil)

		err := ValidateVPGCountForPool(ctx, mockStorage, poolID)
		assert.Error(tt, err)
		assert.True(tt, customerrors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "100")
		assert.Contains(tt, err.Error(), "Delete unused VPGs")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("AboveCap_Rejected", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 5
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("CountVolumePerformanceGroupsByPoolID", ctx, poolID).Return(int64(7), nil)

		err := ValidateVPGCountForPool(ctx, mockStorage, poolID)
		assert.Error(tt, err)
		assert.True(tt, customerrors.IsUserInputValidationErr(err))
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CapZero_DisablesCheck_DoesNotCallStorage", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 0
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)

		err := ValidateVPGCountForPool(ctx, mockStorage, poolID)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CapNegative_DisablesCheck_DoesNotCallStorage", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = -1
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)

		err := ValidateVPGCountForPool(ctx, mockStorage, poolID)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("DBError_PropagatesUnchanged", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 100
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		dbErr := errors.New("db error: connection lost")
		mockStorage.On("CountVolumePerformanceGroupsByPoolID", ctx, poolID).Return(int64(0), dbErr)

		err := ValidateVPGCountForPool(ctx, mockStorage, poolID)
		assert.Error(tt, err)
		assert.Equal(tt, dbErr, err)
		assert.False(tt, customerrors.IsUserInputValidationErr(err))
		mockStorage.AssertExpectations(tt)
	})

	t.Run("NilStorage_ReturnsDefensiveError", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 100
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		err := ValidateVPGCountForPool(ctx, nil, poolID)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "storage is required")
	})
}

func TestCreateVolumePerformanceGroupAtomic(t *testing.T) {
	ctx := context.Background()

	t.Run("InjectsConfiguredCap_AndReturnsRow", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 250
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		req := &datamodel.VolumePerformanceGroup{PoolID: 7, Name: "vpg-atomic"}
		expected := &datamodel.VolumePerformanceGroup{BaseModel: datamodel.BaseModel{UUID: "out"}, PoolID: 7, Name: "vpg-atomic"}
		mockStorage.On("CreateVolumePerformanceGroupWithCap", ctx, req, 250).Return(expected, nil)

		got, err := CreateVolumePerformanceGroupAtomic(ctx, mockStorage, req)
		assert.NoError(tt, err)
		assert.Same(tt, expected, got)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CapZero_IsForwardedAndDisablesEnforcement", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 0
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		req := &datamodel.VolumePerformanceGroup{PoolID: 7, Name: "vpg-disabled"}
		expected := &datamodel.VolumePerformanceGroup{BaseModel: datamodel.BaseModel{UUID: "out2"}, PoolID: 7, Name: "vpg-disabled"}
		mockStorage.On("CreateVolumePerformanceGroupWithCap", ctx, req, 0).Return(expected, nil)

		got, err := CreateVolumePerformanceGroupAtomic(ctx, mockStorage, req)
		assert.NoError(tt, err)
		assert.Same(tt, expected, got)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("StorageError_Propagates", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 100
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		req := &datamodel.VolumePerformanceGroup{PoolID: 7, Name: "vpg-err"}
		dbErr := errors.New("transient db failure")
		mockStorage.On("CreateVolumePerformanceGroupWithCap", ctx, req, 100).Return(nil, dbErr)

		got, err := CreateVolumePerformanceGroupAtomic(ctx, mockStorage, req)
		assert.Nil(tt, got)
		assert.Equal(tt, dbErr, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CapValidationError_PropagatesAsUserInputValidationErr", func(tt *testing.T) {
		orig := maxVolumePerformanceGroupsPerPool
		maxVolumePerformanceGroupsPerPool = 100
		defer func() { maxVolumePerformanceGroupsPerPool = orig }()

		mockStorage := database.NewMockStorage(tt)
		req := &datamodel.VolumePerformanceGroup{PoolID: 7, Name: "vpg-overflow"}
		validationErr := customerrors.NewUserInputValidationErr("Pool has reached the maximum number of Volume Performance Groups (100). ...")
		mockStorage.On("CreateVolumePerformanceGroupWithCap", ctx, req, 100).Return(nil, validationErr)

		got, err := CreateVolumePerformanceGroupAtomic(ctx, mockStorage, req)
		assert.Nil(tt, got)
		assert.Error(tt, err)
		assert.True(tt, customerrors.IsUserInputValidationErr(err))
	})

	t.Run("NilStorage_ReturnsDefensiveError", func(tt *testing.T) {
		got, err := CreateVolumePerformanceGroupAtomic(ctx, nil, &datamodel.VolumePerformanceGroup{PoolID: 1, Name: "x"})
		assert.Nil(tt, got)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "storage is required")
	})
}
