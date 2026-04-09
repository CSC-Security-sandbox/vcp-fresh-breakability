package oci

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowenginemock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestNewOCIOrchestrator(t *testing.T) {
	t.Run("Success_CreatesOrchestrator", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowenginemock.NewMockTemporalTestClient(tt)

		orch := NewOCIOrchestrator(mockStorage, mockTemporal)

		assert.NotNil(tt, orch)
		assert.Equal(tt, mockStorage, orch.storage)
		assert.Equal(tt, mockTemporal, orch.temporal)
	})
}

func TestOCIOrchestrator_UpdatePool(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdatePoolParams{}

		result, jobID, err := orch.UpdatePool(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_DescribePool(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.DescribePool(ctx, "pool-id", "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetExpertModePoolCreds(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetExpertModePoolCreds(ctx, "pool-id", "account-name", "user-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DeletePool(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeletePoolParams{}

		result, jobID, err := orch.DeletePool(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetMultiplePools(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultiplePools(ctx, "account-name", []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetPoolByVendorID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetPoolByVendorID(ctx, "vendor-id", "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetPoolByName(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetPoolByName(ctx, "pool-name", "account-name", 1)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ListPools(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.ListPools(ctx, "account-name", false)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ListAllPools(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.ListAllPools(ctx)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateHostGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateHostGroupParams{}

		result, err := orch.CreateHostGroup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetHostGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetHostGroup(ctx, "hostgroup-uuid", "account-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DeleteHostGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.DeleteHostGroup(ctx, "hostgroup-uuid", "account-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateHostGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateHostGroupParams{}

		result, jobID, err := orch.UpdateHostGroup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetMultipleHostGroups(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultipleHostGroups(ctx, "account-name", []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetHostGroupsByUUIDs(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetHostGroupsByUUIDs(ctx, []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateVolumeParams{}

		result, jobID, err := orch.CreateVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_CreateFlexCacheVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateVolumeParams{}

		result, jobID, err := orch.CreateFlexCacheVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_RevertVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.RevertVolumeParams{}

		result, jobID, err := orch.RevertVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetVolume(ctx, "volume-id", false)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateVolumeParams{}

		result, jobID, err := orch.UpdateVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_UpdateVolumeV2(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateVolumeParams{}

		result, jobID, err := orch.UpdateVolumeV2(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetVolumeCount(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetVolumeCount(ctx, "project-number")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Equal(tt, int64(0), result)
	})
}

func TestOCIOrchestrator_DeleteVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, jobID, err := orch.DeleteVolume(ctx, "volume-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetMultipleVolumes(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultipleVolumes(ctx, []string{"id1", "id2"}, "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ListVolumes(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.ListVolumes(ctx, "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_EstablishFlexCacheVolumePeering(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.EstablishVolumePeeringParams{}

		result, jobID, err := orch.EstablishFlexCacheVolumePeering(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_EstablishReplicationPeering(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.EstablishReplicationPeeringParams{}

		result, jobID, err := orch.EstablishReplicationPeering(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_RestoreFilesFromBackup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.RestoreFilesFromBackupParams{}

		result, err := orch.RestoreFilesFromBackup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_SplitCloneVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.SplitStartVolumeParams{}

		result, jobID, err := orch.SplitStartVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetJob(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetJob(ctx, "operation-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetReplicationJobs(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetReplicationJobs(ctx, "project-name", "pool-uuid")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetJobByResourceUUID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetJobByResourceUUID(ctx, "resource-uuid", "job-type")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateJob(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateJobParams{}

		result, err := orch.CreateJob(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateJobStatus(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		err := orch.UpdateJobStatus(ctx, "job-id", "status", 0, "error-details")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_UpdateJobAttributes(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		err := orch.UpdateJobAttributes(ctx, "job-id", nil)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_CreateSnapshot(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateSnapshotParams{}

		result, jobID, err := orch.CreateSnapshot(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetSnapshot(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetSnapshotParams{}

		result, err := orch.GetSnapshot(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DeleteSnapshot(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteSnapshotParams{}

		result, jobID, err := orch.DeleteSnapshot(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_ListSnapshots(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ListSnapshotsParams{}

		result, err := orch.ListSnapshots(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateSnapshot(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateSnapshotParams{}

		result, jobID, err := orch.UpdateSnapshot(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetMultipleSnapshots(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultipleSnapshots(ctx, "volume-uuid", "account-name", []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DeleteSnapmirrorSnapshots(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.SnapshotsInternalDeleteParams{}

		result, err := orch.DeleteSnapmirrorSnapshots(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_CreateQuotaRule(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateQuotaRulesParam{}

		result, jobID, err := orch.CreateQuotaRule(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_CreateQuotaRuleInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateQuotaRulesParam{}

		result, job, err := orch.CreateQuotaRuleInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_UpdateQuotaRule(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateQuotaRulesParam{}

		result, jobID, err := orch.UpdateQuotaRule(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_UpdateQuotaRuleInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateQuotaRulesParam{}

		result, job, err := orch.UpdateQuotaRuleInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_DeleteQuotaRule(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteQuotaRulesParam{}

		result, jobID, err := orch.DeleteQuotaRule(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_DeleteQuotaRuleInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteQuotaRulesParam{}

		result, job, err := orch.DeleteQuotaRuleInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_ListQuotaRules(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ListQuotaRulesParams{}

		result, err := orch.ListQuotaRules(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetMultipleQuotaRules(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultipleQuotaRules(ctx, "volume-uuid", "account-name", []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DescribeQuotaRule(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.DescribeQuotaRule(ctx, "volume-uuid", "account-name", "quota-rule-uuid")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateVolumeReplicationInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateVolumeReplicationInternalParams{}

		result, job, err := orch.CreateVolumeReplicationInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_GetReplicationCount(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetReplicationCount(ctx, "project-number")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Equal(tt, int64(0), result)
	})
}

func TestOCIOrchestrator_CreateVolumeReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateVolumeReplicationParams{}

		result, jobID, err := orch.CreateVolumeReplication(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_UpdateVolumeReplicationInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateVolumeReplicationInternalParams{}

		result, job, err := orch.UpdateVolumeReplicationInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_UpdateVolumeReplicationAttributes(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := models.UpdateVolumeReplicationAttributesParams{}

		result, err := orch.UpdateVolumeReplicationAttributes(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateVolumeReplicationState(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := models.UpdateVolumeReplicationStateParams{}

		result, err := orch.UpdateVolumeReplicationState(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetMultipleReplicationsInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultipleReplicationsInternal(ctx, "account-name", []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetMultipleReplications(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := common.GetMultipleReplicationsParams{}

		result, err := orch.GetMultipleReplications(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetMultipleReplicationsByExternalUUID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := common.GetMultipleReplicationsByExternalUUIDParams{}

		result, err := orch.GetMultipleReplicationsByExternalUUID(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_AcceptClusterPeer(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ClusterPeerParams{}

		result, job, err := orch.AcceptClusterPeer(ctx, params, "pool-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_PerformMountCheck(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.PerformMountCheck(ctx, "replication-uuid", "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ResumeReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ResumeReplicationParams{}

		result, jobID, err := orch.ResumeReplication(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_UpdateReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateReplicationParams{}

		result, jobID, err := orch.UpdateReplication(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_ResumeReplicationInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, job, err := orch.ResumeReplicationInternal(ctx, "volume-replication-id", "account-name", false)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_ReverseReplicationInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, job, err := orch.ReverseReplicationInternal(ctx, "volume-replication-id", "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_GetReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetReplication(ctx, "volume-replication-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ReleaseVolumeReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, job, err := orch.ReleaseVolumeReplication(ctx, "replication-uuid")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_DeleteReplicationInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, job, err := orch.DeleteReplicationInternal(ctx, "volume-replication-id", false, false)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_StopReplicationInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, job, err := orch.StopReplicationInternal(ctx, "replication-uuid", "account-name", false)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_StopReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.StopReplicationParams{}

		result, jobID, err := orch.StopReplication(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_DeleteReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteReplicationParams{}

		result, jobID, err := orch.DeleteReplication(ctx, params, "cleanup-job-id", false)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_SyncReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ResumeReplicationParams{}

		result, jobID, err := orch.SyncReplication(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_ReverseAndResumeReplication(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ReverseAndResumeReplicationParams{}

		result, jobID, err := orch.ReverseAndResumeReplication(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, jobID)
	})
}

func TestOCIOrchestrator_CreateKmsConfig(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateKmsConfigParams{}

		result, jobID, err := orch.CreateKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetKmsConfig(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetKmsConfigParams{}

		result, err := orch.GetKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetKmsConfigByKeyFullPath(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetKmsConfigParams{}

		result, err := orch.GetKmsConfigByKeyFullPath(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetMultipleKMSConfigs(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultipleKMSConfigs(ctx, []string{"id1", "id2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateKmsConfig(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateKmsConfigParams{}

		result, err := orch.UpdateKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CheckAndUpdateKmsConfigHealth(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &models.KmsConfigCheck{}

		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_AccessCryptoKeyAndEncryptDataWithImpersonation(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		kmsConfig := &models.KmsConfig{}

		err := orch.AccessCryptoKeyAndEncryptDataWithImpersonation(ctx, kmsConfig)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_DeleteKmsConfig(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteKmsConfigParams{}

		result, jobID, err := orch.DeleteKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_MigrateKmsConfig(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.MigrateKmsConfigParams{}

		result, err := orch.MigrateKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_RotateKmsConfig(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.RotateKmsConfigParams{}

		result, job, err := orch.RotateKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Nil(tt, job)
	})
}

func TestOCIOrchestrator_CreateAndSyncKmsConfig(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateKmsConfigParams{}

		result, err := orch.CreateAndSyncKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetSDEKmsConfiguration(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetKmsConfigParams{}

		result, err := orch.GetSDEKmsConfiguration(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetBackupVaultByNameAndOwnerID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupVaultByNameAndOwnerID(ctx, "bv-name", "owner-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetBackupPolicyByNameAndOwnerID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupPolicyByNameAndOwnerID(ctx, "policy-name", "owner-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetBackupPolicyByUUIDAndOwnerID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupPolicyByUUIDAndOwnerID(ctx, "uuid", "owner-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateBackupPolicy(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateBackupPolicyParams{}

		result, jobID, err := orch.UpdateBackupPolicy(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_ListBackupPoliciesAndVolumeCount(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		volumeCounts, policies, err := orch.ListBackupPoliciesAndVolumeCount(ctx, "owner-id", []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, volumeCounts)
		assert.Nil(tt, policies)
	})
}

func TestOCIOrchestrator_DeleteBackupPolicy(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteBackupPolicyParams{}

		result, jobID, err := orch.DeleteBackupPolicy(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetBackupPolicyUUIDsFromBackupVaultUUID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupPolicyUUIDsFromBackupVaultUUID(ctx, "backup-vault-uuid", "owner-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ListBackupVaults(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.ListBackupVaults(ctx, "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetBackupVaultByUUID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupVaultByUUID(ctx, "bv-uuid", "owner-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateBackupVault(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.BackupVaultParams{}

		result, jobID, err := orch.UpdateBackupVault(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetMultipleBackupVaults(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultipleBackupVaults(ctx, []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DeleteBackupVault(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.BackupVaultParams{}

		result, jobID, err := orch.DeleteBackupVault(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_DeleteBackupVaultInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.BackupVaultParams{}

		result, err := orch.DeleteBackupVaultInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_UpdateBackupVaultInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.BackupVaultParams{}

		result, jobID, err := orch.UpdateBackupVaultInternal(ctx, params, false)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_IsBackupVaultAttachedToVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.IsBackupVaultAttachedToVolume(ctx, "backup-vault-uuid")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.False(tt, result)
	})
}

func TestOCIOrchestrator_GetBackupVaultUUIDsFromBackupPolicyUUID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupVaultUUIDsFromBackupPolicyUUID(ctx, "backup-policy-uuid", "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateBackupVaultEntryInVCP(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		bv := &datamodel.BackupVault{}

		result, err := orch.CreateBackupVaultEntryInVCP(ctx, bv, &common.BackupVaultParams{})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateBackupVault(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateBackupVaultParams{}

		jobID, err := orch.CreateBackupVault(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, jobID)
	})

	t.Run("ReturnsNotImplementedError_nilParams", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		jobID, err := orch.CreateBackupVault(ctx, nil)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetBackupVaultByExternalUUIDAndOwnerID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupVaultByExternalUUIDAndOwnerID(ctx, "external-uuid", "owner-id")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetAccount(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetAccount(ctx, "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateResourceState(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateResourceStateParams{}

		result, err := orch.UpdateResourceState(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_CreateBackup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateBackupParams{}

		result, jobID, err := orch.CreateBackup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_CreateBackupInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateBackupParams{}

		result, jobID, err := orch.CreateBackupInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetBackup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetBackupParams{}

		result, err := orch.GetBackup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetBackupByExternalUUID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupByExternalUUID(ctx, "backup-vault-uuid", "external-uuid", "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DeleteBackup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteBackupParams{}

		result, jobID, err := orch.DeleteBackup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_DeleteBackupInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteBackupParams{}

		result, err := orch.DeleteBackupInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_ListBackups(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.ListBackups(ctx, "backup-vault-id", "owner-id", nil)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateBackup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateBackupParams{}

		result, jobID, err := orch.UpdateBackup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_UpdateBackupInternal(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateBackupParams{}

		result, jobID, err := orch.UpdateBackupInternal(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetBackupsUnderBackupVault(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupsUnderBackupVault(ctx, "backup-vault-id", "owner-id", []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateBackupLatestLogicalBackupSizeByVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		err := orch.UpdateBackupLatestLogicalBackupSizeByVolume(ctx, "volume-uuid", "backup-uuid")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_RotateCmekBackupsForBackupVault(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.BackupVaultParams{}

		result, err := orch.RotateCmekBackupsForBackupVault(ctx, params, "primary-key-version")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_CreateOrGetStartProjectEventJob(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.StartProjectEventParams{}

		result, err := orch.CreateOrGetStartProjectEventJob(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_CreateOrGetFinishProjectEventJob(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.FinishProjectEventParams{}

		result, err := orch.CreateOrGetFinishProjectEventJob(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_CreateActiveDirectory(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateActiveDirectoryParams{}

		result, jobID, err := orch.CreateActiveDirectory(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_UpdateActiveDirectory(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateActiveDirectoryParams{}

		result, jobID, err := orch.UpdateActiveDirectory(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_UpgradeCluster(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpgradeClusterParams{}

		result, jobID, err := orch.UpgradeCluster(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})
}

func TestOCIOrchestrator_GetClusterUpgradeStatus(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetClusterUpgradeStatus(ctx, "job-uuid")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ListAvailableVersions(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.ListAvailableVersions(ctx)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateImageVersion(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.CreateImageVersion(ctx, "ontap-version", "vsa-image-path", "vsa-name", "mediator-name", false)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DeleteImageVersion(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		err := orch.DeleteImageVersion(ctx, "ontap-version")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_GetActiveDirectory(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetADParams{}

		result, err := orch.GetActiveDirectory(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ListActiveDirectories(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.ListActiveDirectories(ctx, "account-name")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetMultipleActiveDirectories(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetMultipleActiveDirectories(ctx, []string{"uuid1", "uuid2"})

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetADConfig(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetADParams{}

		result, err := orch.GetADConfig(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetSDEActiveDirectory(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetADParams{}

		result, err := orch.GetSDEActiveDirectory(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_DeleteActiveDirectory(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteActiveDirectoryParams{}

		result, err := orch.DeleteActiveDirectory(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_GetExpertModeVolumeByExternalUUID(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetExpertModeVolumeByExternalUUID(ctx, "volume-uuid")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateExpertModeVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ExpertModeVolumeParams{}

		err := orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_UpdateExpertModeVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ExpertModeVolumeParams{}

		err := orch.UpdateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_DeleteExpertModeVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ExpertModeVolumeParams{}

		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_RenameExpertModeVolume(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ExpertModeVolumeRenameParams{}

		err := orch.RenameExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
	})
}

func TestOCIOrchestrator_UpdateRbacForPools(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.UpdateRbacForPools(ctx)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, result)
	})
}

func TestOCIOrchestrator_GetBackupConfigsForPool(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()

		result, err := orch.GetBackupConfigsForPool(ctx, "pool-id", "account-name", "us-west1-a")

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_CreateVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.CreateVolumePerformanceGroupParams{}

		result, err := orch.CreateVolumePerformanceGroup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_ListVolumePerformanceGroups(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.ListVolumePerformanceGroupsParams{}

		result, err := orch.ListVolumePerformanceGroups(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_GetVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.GetVolumePerformanceGroupParams{}

		result, err := orch.GetVolumePerformanceGroup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}

func TestOCIOrchestrator_UpdateVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.UpdateVolumePerformanceGroupParams{}

		result, jobUUID, err := orch.UpdateVolumePerformanceGroup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
		assert.Empty(tt, jobUUID)
	})
}

func TestOCIOrchestrator_DeleteVolumePerformanceGroup(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		params := &common.DeleteVolumePerformanceGroupParams{}

		_, jobUUID, err := orch.DeleteVolumePerformanceGroup(ctx, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Empty(tt, jobUUID)
	})
}

func TestOCIOrchestrator_ReplaceDstQuotaRulesWithSrc(t *testing.T) {
	t.Run("ReturnsNotImplementedError", func(tt *testing.T) {
		orch := &OCIOrchestrator{}
		ctx := context.Background()
		req := &common.UpdateDstWithSrcQuotaRulesV1beta{}
		params := common.V1betaUpdateDestinationQuotaRulesVCPParams{}

		result, err := orch.ReplaceDstQuotaRulesWithSrc(ctx, req, params)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNotImplementedYetErr(err))
		assert.Nil(tt, result)
	})
}
