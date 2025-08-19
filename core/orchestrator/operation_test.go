package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestGetReplicationJobs(t *testing.T) {
	t.Run("ReturnsErrorWhenGetAccountFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetAccount(mock.Anything, "test-project").Return(nil, errors.New("account not found"))

		orchestrator := &Orchestrator{storage: mockStorage}
		jobs, err := orchestrator.GetReplicationJobs(context.Background(), "test-project", "test-pool")
		assert.Error(tt, err)
		assert.Nil(tt, jobs)
		assert.Equal(tt, "account not found", err.Error())
	})
	t.Run("ReturnsErrorWhenGetJobsWithConditionFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{
			ID: 1,
		}}
		mockStorage.EXPECT().GetAccount(mock.Anything, "test-project").Return(mockAccount, nil)
		mockStorage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(nil, errors.New("failed to fetch jobs"))

		orchestrator := &Orchestrator{storage: mockStorage}
		jobs, err := orchestrator.GetReplicationJobs(context.Background(), "test-project", "test-pool")
		assert.Error(tt, err)
		assert.Nil(tt, jobs)
		assert.Equal(tt, "failed to fetch jobs", err.Error())
	})
	t.Run("ReturnsEmptyListWhenNoJobsMatchPoolUUID", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{
			ID: 1,
		}}
		mockJobs := []*datamodel.Job{
			{JobAttributes: &datamodel.JobAttributes{PoolUUID: "other-pool"}},
		}
		mockStorage.EXPECT().GetAccount(mock.Anything, "test-project").Return(mockAccount, nil)
		mockStorage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(mockJobs, nil)

		orchestrator := &Orchestrator{storage: mockStorage}
		jobs, err := orchestrator.GetReplicationJobs(context.Background(), "test-project", "test-pool")
		assert.NoError(tt, err)
		assert.Empty(tt, jobs)
	})
	t.Run("ReturnsJobsMatchingPoolUUID", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{
			ID: 1,
		}}
		mockJobs := []*datamodel.Job{
			{BaseModel: datamodel.BaseModel{UUID: "job-1"}, JobAttributes: &datamodel.JobAttributes{PoolUUID: "test-pool"}},
			{BaseModel: datamodel.BaseModel{UUID: "job-2"}, JobAttributes: &datamodel.JobAttributes{PoolUUID: "test-pool"}},
		}
		mockStorage.EXPECT().GetAccount(mock.Anything, "test-project").Return(mockAccount, nil)
		mockStorage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(mockJobs, nil)

		orchestrator := &Orchestrator{storage: mockStorage}
		jobs, err := orchestrator.GetReplicationJobs(context.Background(), "test-project", "test-pool")
		assert.NoError(tt, err)
		assert.Len(tt, jobs, 2)
		assert.Equal(tt, "job-1", jobs[0].UUID)
		assert.Equal(tt, "job-2", jobs[1].UUID)
	})
	t.Run("ReturnsAllJobsWhenPoolUUIDIsEmpty", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{
			ID: 1,
		}}
		mockJobs := []*datamodel.Job{
			{BaseModel: datamodel.BaseModel{UUID: "job-1"}, JobAttributes: &datamodel.JobAttributes{PoolUUID: "pool-1"}},
			{BaseModel: datamodel.BaseModel{UUID: "job-2"}, JobAttributes: &datamodel.JobAttributes{PoolUUID: "pool-2"}},
			{BaseModel: datamodel.BaseModel{UUID: "job-3"}, JobAttributes: &datamodel.JobAttributes{PoolUUID: "pool-1"}},
		}
		mockStorage.EXPECT().GetAccount(mock.Anything, "test-project").Return(mockAccount, nil)
		mockStorage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(mockJobs, nil)

		orchestrator := &Orchestrator{storage: mockStorage}
		jobs, err := orchestrator.GetReplicationJobs(context.Background(), "test-project", "")
		assert.NoError(tt, err)
		assert.Len(tt, jobs, 3)
		assert.Equal(tt, "job-1", jobs[0].UUID)
		assert.Equal(tt, "job-2", jobs[1].UUID)
		assert.Equal(tt, "job-3", jobs[2].UUID)
	})
	t.Run("ReturnsPoolJobsWhenPoolUUIDIsNotEmpty", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{
			ID: 1,
		}}
		mockJobs := []*datamodel.Job{
			{BaseModel: datamodel.BaseModel{UUID: "job-1"}, JobAttributes: &datamodel.JobAttributes{PoolUUID: "pool-1"}},
			{BaseModel: datamodel.BaseModel{UUID: "job-2"}, JobAttributes: &datamodel.JobAttributes{PoolUUID: "pool-2"}},
			{BaseModel: datamodel.BaseModel{UUID: "job-3"}, JobAttributes: &datamodel.JobAttributes{PoolUUID: "pool-1"}},
		}
		mockStorage.EXPECT().GetAccount(mock.Anything, "test-project").Return(mockAccount, nil)
		mockStorage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(mockJobs, nil)

		orchestrator := &Orchestrator{storage: mockStorage}
		jobs, err := orchestrator.GetReplicationJobs(context.Background(), "test-project", "pool-2")
		assert.NoError(tt, err)
		assert.Len(tt, jobs, 1)
		assert.Equal(tt, "job-2", jobs[0].UUID)
	})
}

func TestGetJobByResourceUUID(t *testing.T) {
	t.Run("ReturnsErrorWhenStorageFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		customErr := errors.New("job not found")
		mockStorage.EXPECT().GetJobByResourceUUID(mock.Anything, "invalid-resource", mock.Anything).Return(nil, customErr)
		orchestrator := &Orchestrator{storage: mockStorage}
		job, err := orchestrator.GetJobByResourceUUID(context.Background(), "invalid-resource", "")
		assert.Error(tt, err)
		assert.Nil(tt, job)
		assert.Equal(tt, "job not found", err.Error())
	})

	t.Run("ReturnsJobWhenStorageSucceeds", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dummyJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "job-uuid",
			},
			CorrelationID: "corr-1",
			TrackingID:    1,
			Type:          "type",
			State:         models.LifeCycleStateCreating,
			JobAttributes: &datamodel.JobAttributes{
				PoolUUID:     "pool-1",
				ResourceUUID: "resource-1",
			},
			ResourceName: "resource",
		}
		mockStorage.EXPECT().GetJobByResourceUUID(mock.Anything, "resource-1", mock.Anything).Return(dummyJob, nil)
		orchestrator := &Orchestrator{storage: mockStorage}
		job, err := orchestrator.GetJobByResourceUUID(context.Background(), "resource-1", "")
		assert.NoError(tt, err)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid", job.UUID)
		assert.Equal(tt, "corr-1", job.CorrelationID)
		assert.Equal(tt, 1, job.TrackingID)
	})
}
