package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func makeBaseSplitJob() *datamodel.Job {
	return &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-split-1"},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "volume-1",
		},
	}
}

func makeVolumeWithSplitJob(splitJobUUID string, pool *datamodel.Pool) *datamodel.Volume {
	v := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-1"},
		Pool:      pool,
		VolumeAttributes: &datamodel.VolumeAttributes{
			SplitJobUUID: splitJobUUID,
		},
	}
	return v
}

// ─── PrepareWorkflowArgs ───────────────────────────────────────────────────────

func TestSplitVolumeArgs_PrepareWorkflowArgs_GetVolumeFails(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	mockSE.On("GetVolume", ctx, "volume-1").Return(nil, errors.New("db error"))

	s := &SplitVolumeArgs{}
	args, err := s.PrepareWorkflowArgs(ctx, mockSE, makeBaseSplitJob())

	assert.Nil(t, args)
	assert.ErrorContains(t, err, "failed to get volume volume-1")
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_PrepareWorkflowArgs_MissingSplitJobUUID(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10, UUID: "pool-1"}}
	volume := makeVolumeWithSplitJob("", pool) // empty SplitJobUUID

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)

	s := &SplitVolumeArgs{}
	args, err := s.PrepareWorkflowArgs(ctx, mockSE, makeBaseSplitJob())

	assert.Nil(t, args)
	assert.ErrorContains(t, err, "has no SplitJobUUID")
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_PrepareWorkflowArgs_NilVolumeAttributes(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "volume-1"},
		VolumeAttributes: nil,
	}

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)

	s := &SplitVolumeArgs{}
	args, err := s.PrepareWorkflowArgs(ctx, mockSE, makeBaseSplitJob())

	assert.Nil(t, args)
	assert.ErrorContains(t, err, "has no SplitJobUUID")
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_PrepareWorkflowArgs_MissingPool(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	volume := makeVolumeWithSplitJob("ontap-job-1", nil) // nil pool

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)

	s := &SplitVolumeArgs{}
	args, err := s.PrepareWorkflowArgs(ctx, mockSE, makeBaseSplitJob())

	assert.Nil(t, args)
	assert.ErrorContains(t, err, "has no associated pool")
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_PrepareWorkflowArgs_GetNodesFails(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10, UUID: "pool-1"}}
	volume := makeVolumeWithSplitJob("ontap-job-1", pool)

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)
	mockSE.On("GetNodesByPoolID", ctx, int64(10)).Return(nil, errors.New("nodes db error"))

	s := &SplitVolumeArgs{}
	args, err := s.PrepareWorkflowArgs(ctx, mockSE, makeBaseSplitJob())

	assert.Nil(t, args)
	assert.ErrorContains(t, err, "failed to get nodes for pool 10")
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_PrepareWorkflowArgs_NoNodes(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10, UUID: "pool-1"}}
	volume := makeVolumeWithSplitJob("ontap-job-1", pool)

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)
	mockSE.On("GetNodesByPoolID", ctx, int64(10)).Return([]*datamodel.Node{}, nil)

	s := &SplitVolumeArgs{}
	args, err := s.PrepareWorkflowArgs(ctx, mockSE, makeBaseSplitJob())

	assert.Nil(t, args)
	assert.ErrorContains(t, err, "no nodes found for pool pool-1")
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_PrepareWorkflowArgs_Success(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{ID: 10, UUID: "pool-1"},
		DeploymentName: "test-deploy",
	}
	volume := makeVolumeWithSplitJob("ontap-job-99", pool)
	dbNodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{UUID: "node-1"}}}

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)
	mockSE.On("GetNodesByPoolID", ctx, int64(10)).Return(dbNodes, nil)

	fakeNode := &models.Node{DeploymentName: "test-deploy"}
	originalCreateNode := vsa.CreateNodeForProvider
	defer func() { vsa.CreateNodeForProvider = originalCreateNode }()
	vsa.CreateNodeForProvider = func(inp vsa.NodeProviderInput) *models.Node {
		return fakeNode
	}

	s := &SplitVolumeArgs{}
	args, err := s.PrepareWorkflowArgs(ctx, mockSE, makeBaseSplitJob())

	assert.NoError(t, err)
	assert.Len(t, args, 3)
	assert.Equal(t, volume, args[0])
	assert.Equal(t, fakeNode, args[1])
	assert.Equal(t, "ontap-job-99", args[2])
	mockSE.AssertExpectations(t)
}

// ─── FailedWorkflowJob ────────────────────────────────────────────────────────

func TestSplitVolumeArgs_FailedWorkflowJob_GetVolumeFails(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	mockSE.On("GetVolume", ctx, "volume-1").Return(nil, errors.New("not found"))

	s := &SplitVolumeArgs{}
	err := s.FailedWorkflowJob(ctx, mockSE, makeBaseSplitJob(), "retry exhausted")

	assert.ErrorContains(t, err, "failed to get volume volume-1")
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_FailedWorkflowJob_NilCloneParentInfo(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: nil, // no clone info
		},
	}

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)
	// UpdateVolumeFields must NOT be called

	s := &SplitVolumeArgs{}
	err := s.FailedWorkflowJob(ctx, mockSE, makeBaseSplitJob(), "retry exhausted")

	assert.NoError(t, err)
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_FailedWorkflowJob_NilVolumeAttributes(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "volume-1"},
		VolumeAttributes: nil,
	}

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)

	s := &SplitVolumeArgs{}
	err := s.FailedWorkflowJob(ctx, mockSE, makeBaseSplitJob(), "retry exhausted")

	assert.NoError(t, err)
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_FailedWorkflowJob_UpdateVolumeFieldsFails(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{State: datamodel.CloneStateSplitting},
		},
	}

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)
	mockSE.On("UpdateVolumeFields", ctx, "volume-1", mock.Anything).Return(errors.New("update failed"))

	s := &SplitVolumeArgs{}
	err := s.FailedWorkflowJob(ctx, mockSE, makeBaseSplitJob(), "retry exhausted")

	assert.ErrorContains(t, err, "failed to update clone state")
	mockSE.AssertExpectations(t)
}

func TestSplitVolumeArgs_FailedWorkflowJob_SetsCloneStateToErrorInSplitting(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)

	reason := "retry exhausted after 10 attempts"
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{State: datamodel.CloneStateSplitting},
		},
	}

	mockSE.On("GetVolume", ctx, "volume-1").Return(volume, nil)
	mockSE.On("UpdateVolumeFields", ctx, "volume-1",
		mock.MatchedBy(func(updates map[string]interface{}) bool {
			attrs, ok := updates["volume_attributes"].(*datamodel.VolumeAttributes)
			if !ok {
				return false
			}
			return attrs.CloneParentInfo != nil &&
				attrs.CloneParentInfo.State == datamodel.CloneStateErrorInSplitting &&
				attrs.CloneParentInfo.StateDetails == reason
		}),
	).Return(nil)

	s := &SplitVolumeArgs{}
	err := s.FailedWorkflowJob(ctx, mockSE, makeBaseSplitJob(), reason)

	assert.NoError(t, err)
	mockSE.AssertExpectations(t)
}
