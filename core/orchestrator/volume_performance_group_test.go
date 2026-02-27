package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestCreateVolumePerformanceGroup(t *testing.T) {
	ctx := context.Background()

	t.Run("SuccessfullyCreatesVPGAndStartsWorkflow", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orchestrator := NewOrchestrator(mockStorage, mockTemporal)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				QosType:   utils.QosTypeManual,
			},
		}
		createdVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid-1"},
			Name:            "test-vpg",
			PoolID:          1,
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
			IsAutoGen:       false,
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("CreateVolumePerformanceGroup", ctx, mock.MatchedBy(func(vpg *datamodel.VolumePerformanceGroup) bool {
			return vpg != nil && vpg.Name == "test-vpg" && vpg.PoolID == 1 && vpg.ThroughputMibps == 100 &&
				vpg.Iops == 1000 && !vpg.IsShared && !vpg.IsAutoGen
		})).Return(createdVPG, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, "vpg-uuid-1").Return(nil, nil)

		params := &common.CreateVolumePerformanceGroupParams{
			AccountName:     "test-account",
			PoolID:          "test-pool-id",
			Name:            "test-vpg",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
		}

		result, err := orchestrator.CreateVolumePerformanceGroup(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "vpg-uuid-1", result.UUID)
		assert.Equal(tt, "test-vpg", result.Name)
		assert.Equal(tt, int64(100), result.ThroughputMibps)
		assert.Equal(tt, int64(1000), result.Iops)
		assert.False(tt, result.IsShared)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ReturnsErrorWhenExecuteWorkflowFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orchestrator := NewOrchestrator(mockStorage, mockTemporal)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				QosType:   utils.QosTypeManual,
			},
		}
		createdVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid-1"},
			Name:            "test-vpg",
			PoolID:          1,
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
			IsAutoGen:       false,
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("CreateVolumePerformanceGroup", ctx, mock.Anything).Return(createdVPG, nil)
		mockStorage.On("DeleteVolumePerformanceGroup", ctx, createdVPG).Return(nil)
		mockTemporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, "vpg-uuid-1").Return(nil, errors.New("workflow start failed"))

		params := &common.CreateVolumePerformanceGroupParams{
			AccountName:     "test-account",
			PoolID:          "test-pool-id",
			Name:            "test-vpg",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
		}

		result, err := orchestrator.CreateVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "workflow start failed", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ReturnsErrorWhenAccountNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orchestrator := NewOrchestrator(mockStorage, mockTemporal)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &common.CreateVolumePerformanceGroupParams{
			AccountName:     "test-account",
			PoolID:          "test-pool-id",
			Name:            "test-vpg",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
		}

		result, err := orchestrator.CreateVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "account not found", err.Error())
	})

	t.Run("ReturnsErrorWhenDescribePoolFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orchestrator := NewOrchestrator(mockStorage, mockTemporal)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(nil, errors.New("pool not found"))

		params := &common.CreateVolumePerformanceGroupParams{
			AccountName:     "test-account",
			PoolID:          "test-pool-id",
			Name:            "test-vpg",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
		}

		result, err := orchestrator.CreateVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "pool not found")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ReturnsErrorWhenPoolQosTypeIsNotManual", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orchestrator := NewOrchestrator(mockStorage, mockTemporal)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				QosType:   utils.QosTypeAuto, // Not manual
			},
		}
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)

		params := &common.CreateVolumePerformanceGroupParams{
			AccountName:     "test-account",
			PoolID:          "test-pool-id",
			Name:            "test-vpg",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
		}

		result, err := orchestrator.CreateVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "manual QoS type")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ReturnsErrorWhenCreateVolumePerformanceGroupFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orchestrator := NewOrchestrator(mockStorage, mockTemporal)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				QosType:   utils.QosTypeManual,
			},
		}
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("CreateVolumePerformanceGroup", ctx, mock.Anything).Return(nil, errors.New("db create failed"))

		params := &common.CreateVolumePerformanceGroupParams{
			AccountName:     "test-account",
			PoolID:          "test-pool-id",
			Name:            "test-vpg",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
		}

		result, err := orchestrator.CreateVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "db create failed")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("DeferLogsErrorWhenWorkflowFailsAndDeleteVPGFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orchestrator := NewOrchestrator(mockStorage, mockTemporal)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				QosType:   utils.QosTypeManual,
			},
		}
		createdVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid-1"},
			Name:            "test-vpg",
			PoolID:          1,
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
			IsAutoGen:       false,
		}
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("CreateVolumePerformanceGroup", ctx, mock.Anything).Return(createdVPG, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, "vpg-uuid-1").Return(nil, errors.New("workflow start failed"))
		// Defer runs and tries to delete VPG; delete also fails (covers logger.Error in defer)
		mockStorage.On("DeleteVolumePerformanceGroup", ctx, createdVPG).Return(errors.New("delete vpg failed"))

		params := &common.CreateVolumePerformanceGroupParams{
			AccountName:     "test-account",
			PoolID:          "test-pool-id",
			Name:            "test-vpg",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        false,
		}

		result, err := orchestrator.CreateVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockStorage.AssertExpectations(tt)
	})
}

func TestListVolumePerformanceGroups(t *testing.T) {
	ctx := context.Background()

	t.Run("SuccessfullyListsVPGs", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
			},
		}
		vpgs := []*datamodel.VolumePerformanceGroup{
			{
				BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid-1"},
				Name:            "vpg-1",
				PoolID:          1,
				ThroughputMibps: 1000,
				Iops:            5000,
				IsShared:        true,
				IsAutoGen:       false,
			},
			{
				BaseModel:       datamodel.BaseModel{ID: 2, UUID: "vpg-uuid-2"},
				Name:            "vpg-2",
				PoolID:          1,
				ThroughputMibps: 2000,
				Iops:            10000,
				IsShared:        false,
				IsAutoGen:       false,
			},
			{
				BaseModel:       datamodel.BaseModel{ID: 3, UUID: "vpg-uuid-auto"},
				Name:            "autoGenerated-volume-123",
				PoolID:          1,
				ThroughputMibps: 500,
				Iops:            2500,
				IsShared:        true,
				IsAutoGen:       true, // This should be filtered out
			},
		}

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("ListVolumePerformanceGroupsByPoolID", ctx, int64(1)).Return(vpgs, nil)

		params := &common.ListVolumePerformanceGroupsParams{
			AccountName: "test-account",
			PoolID:      "test-pool-id",
		}

		result, err := orchestrator.ListVolumePerformanceGroups(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should only return 2 VPGs (auto-generated one is filtered out)
		assert.Len(tt, result, 2)
		assert.Equal(tt, "vpg-1", result[0].Name)
		assert.Equal(tt, "vpg-2", result[1].Name)
		assert.Equal(tt, int64(1000), result[0].ThroughputMibps)
		assert.Equal(tt, int64(2000), result[1].ThroughputMibps)
		assert.Equal(tt, int64(5000), result[0].Iops)
		assert.Equal(tt, int64(10000), result[1].Iops)
		assert.True(tt, result[0].IsShared)
		assert.False(tt, result[1].IsShared)
	})

	t.Run("ReturnsEmptyListWhenNoVPGs", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
			},
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("ListVolumePerformanceGroupsByPoolID", ctx, int64(1)).Return([]*datamodel.VolumePerformanceGroup{}, nil)

		params := &common.ListVolumePerformanceGroupsParams{
			AccountName: "test-account",
			PoolID:      "test-pool-id",
		}

		result, err := orchestrator.ListVolumePerformanceGroups(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)
	})

	t.Run("ReturnsEmptyListWhenOnlyAutoGeneratedVPGs", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
			},
		}
		// Only auto-generated VPGs
		vpgs := []*datamodel.VolumePerformanceGroup{
			{
				BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid-auto-1"},
				Name:            "autoGenerated-volume-123",
				PoolID:          1,
				ThroughputMibps: 500,
				Iops:            2500,
				IsShared:        true,
				IsAutoGen:       true,
			},
			{
				BaseModel:       datamodel.BaseModel{ID: 2, UUID: "vpg-uuid-auto-2"},
				Name:            "autoGenerated-volume-456",
				PoolID:          1,
				ThroughputMibps: 750,
				Iops:            3750,
				IsShared:        false,
				IsAutoGen:       true,
			},
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("ListVolumePerformanceGroupsByPoolID", ctx, int64(1)).Return(vpgs, nil)

		params := &common.ListVolumePerformanceGroupsParams{
			AccountName: "test-account",
			PoolID:      "test-pool-id",
		}

		result, err := orchestrator.ListVolumePerformanceGroups(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return empty list since all VPGs are auto-generated
		assert.Len(tt, result, 0)
	})

	t.Run("ReturnsErrorWhenAccountNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &common.ListVolumePerformanceGroupsParams{
			AccountName: "test-account",
			PoolID:      "test-pool-id",
		}

		result, err := orchestrator.ListVolumePerformanceGroups(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "account not found")
	})

	t.Run("ReturnsErrorWhenPoolNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(nil, errors.New("pool not found"))

		params := &common.ListVolumePerformanceGroupsParams{
			AccountName: "test-account",
			PoolID:      "test-pool-id",
		}

		result, err := orchestrator.ListVolumePerformanceGroups(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "pool not found")
	})

	t.Run("ReturnsErrorWhenListVPGsFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
			},
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("ListVolumePerformanceGroupsByPoolID", ctx, int64(1)).Return(nil, errors.New("list vpgs failed"))

		params := &common.ListVolumePerformanceGroupsParams{
			AccountName: "test-account",
			PoolID:      "test-pool-id",
		}

		result, err := orchestrator.ListVolumePerformanceGroups(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "list vpgs failed")
	})
}

func TestGetVolumePerformanceGroup(t *testing.T) {
	ctx := context.Background()

	t.Run("SuccessfullyGetsVPG", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
			},
		}
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
			Name:            "test-vpg",
			PoolID:          1,
			ThroughputMibps: 1500,
			Iops:            7500,
			IsShared:        true,
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("GetVolumePerformanceGroupByUUID", ctx, "vpg-uuid").Return(vpg, nil)

		params := &common.GetVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "vpg-uuid",
		}

		result, err := orchestrator.GetVolumePerformanceGroup(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-vpg", result.Name)
		assert.Equal(tt, "vpg-uuid", result.UUID)
		assert.Equal(tt, int64(1500), result.ThroughputMibps)
		assert.Equal(tt, int64(7500), result.Iops)
		assert.True(tt, result.IsShared)
	})

	t.Run("ReturnsErrorWhenVPGNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
			},
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("GetVolumePerformanceGroupByUUID", ctx, "vpg-uuid").Return(nil, errors.New("vpg not found"))

		params := &common.GetVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "vpg-uuid",
		}

		result, err := orchestrator.GetVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "vpg not found")
	})

	t.Run("ReturnsErrorWhenVPGBelongsToDifferentPool", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
			},
		}
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
			Name:            "test-vpg",
			PoolID:          2, // Different pool ID
			ThroughputMibps: 1500,
			Iops:            7500,
			IsShared:        true,
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("GetVolumePerformanceGroupByUUID", ctx, "vpg-uuid").Return(vpg, nil)

		params := &common.GetVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "vpg-uuid",
		}

		result, err := orchestrator.GetVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "volume performance group does not belong to the specified pool")
	})

	t.Run("ReturnsErrorWhenAccountNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &common.GetVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "vpg-uuid",
		}

		result, err := orchestrator.GetVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "account not found")
	})

	t.Run("ReturnsErrorWhenPoolNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(nil, errors.New("pool not found"))

		params := &common.GetVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "vpg-uuid",
		}

		result, err := orchestrator.GetVolumePerformanceGroup(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "pool not found")
	})

	t.Run("HandlesVPGWithZeroIOPS", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
			},
		}
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
			Name:            "test-vpg",
			PoolID:          1,
			ThroughputMibps: 1500,
			Iops:            0, // Zero IOPS
			IsShared:        true,
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.On("DescribePool", ctx, "test-pool-id", int64(1)).Return(pool, nil)
		mockStorage.On("GetVolumePerformanceGroupByUUID", ctx, "vpg-uuid").Return(vpg, nil)

		params := &common.GetVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "vpg-uuid",
		}

		result, err := orchestrator.GetVolumePerformanceGroup(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, int64(0), result.Iops) // IOPS should be 0 when zero
	})
}

func TestConvertDatastoreVPGToModel(t *testing.T) {
	t.Run("ReturnsNilWhenInputNil", func(tt *testing.T) {
		assert.Nil(tt, convertDatastoreVPGToModel(nil))
	})

	t.Run("ConvertsFields", func(tt *testing.T) {
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{ID: 7, UUID: "vpg-uuid"},
			Name:            "vpg-name",
			PoolID:          9,
			ThroughputMibps: 3000,
			Iops:            9000,
			IsShared:        true,
		}

		result := convertDatastoreVPGToModel(vpg)
		assert.NotNil(tt, result)
		assert.Equal(tt, int64(7), result.ID)
		assert.Equal(tt, "vpg-uuid", result.UUID)
		assert.Equal(tt, "vpg-name", result.Name)
		assert.Equal(tt, int64(3000), result.ThroughputMibps)
		assert.Equal(tt, int64(9000), result.Iops)
		assert.True(tt, result.IsShared)
	})
}

func TestUpdateVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		params := &common.UpdateVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "test-vpg-id",
			Name:                     "updated-vpg",
			ThroughputMibps:          200,
			Iops:                     5000,
		}

		result, err := orchestrator.UpdateVolumePerformanceGroup(context.Background(), params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "updating volume performance group is not implemented", err.Error())
	})
}

func TestDeleteVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		params := &common.DeleteVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "test-vpg-id",
		}

		err := orchestrator.DeleteVolumePerformanceGroup(context.Background(), params)
		assert.Error(tt, err)
		assert.Equal(tt, "deleting volume performance group is not implemented", err.Error())
	})
}
