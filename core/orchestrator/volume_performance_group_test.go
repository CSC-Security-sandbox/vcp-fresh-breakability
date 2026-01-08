package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestCreateVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		params := &common.CreateVolumePerformanceGroupParams{
			AccountName:     "test-account",
			PoolID:          "test-pool-id",
			Name:            "test-vpg",
			ThroughputMibps: 100.0,
			Iops:            nil,
			IsShared:        false,
		}

		result, err := orchestrator.CreateVolumePerformanceGroup(context.Background(), params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "volume performance group creation is not implemented", err.Error())
	})
}

func TestListVolumePerformanceGroups(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		params := &common.ListVolumePerformanceGroupsParams{
			AccountName: "test-account",
			PoolID:      "test-pool-id",
		}

		result, err := orchestrator.ListVolumePerformanceGroups(context.Background(), params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "listing volume performance groups is not implemented", err.Error())
	})
}

func TestGetVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		params := &common.GetVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "test-vpg-id",
		}

		result, err := orchestrator.GetVolumePerformanceGroup(context.Background(), params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "get volume performance group is not implemented", err.Error())
	})
}

func TestUpdateVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStorage}

		throughput := float32(200.0)
		iops := int32(5000)
		params := &common.UpdateVolumePerformanceGroupParams{
			AccountName:              "test-account",
			PoolID:                   "test-pool-id",
			VolumePerformanceGroupID: "test-vpg-id",
			Name:                     "updated-vpg",
			ThroughputMibps:          &throughput,
			Iops:                     &iops,
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
