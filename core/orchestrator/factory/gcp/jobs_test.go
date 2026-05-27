package gcp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	common "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestCreateJobValidation(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: storage}

	t.Run("NilParams", func(t *testing.T) {
		job, err := orch.CreateJob(ctx, nil)
		require.Nil(t, job)
		require.Error(t, err)
		require.Equal(t, "create job params cannot be nil", err.Error())
		storage.AssertNotCalled(t, "CreateJob", mock.Anything, mock.Anything)
	})

	t.Run("MissingAccountName", func(t *testing.T) {
		params := &common.CreateJobParams{
			Type: models.JobTypeCreatePool,
		}
		job, err := orch.CreateJob(ctx, params)
		require.Nil(t, job)
		require.Error(t, err)
		require.Equal(t, "account name is required to create job", err.Error())
		storage.AssertNotCalled(t, "CreateJob", mock.Anything, mock.Anything)
	})

	t.Run("MissingJobType", func(t *testing.T) {
		params := &common.CreateJobParams{
			AccountName: "project-1",
		}
		job, err := orch.CreateJob(ctx, params)
		require.Nil(t, job)
		require.Error(t, err)
		require.Equal(t, "job type is required to create job", err.Error())
		storage.AssertNotCalled(t, "CreateJob", mock.Anything, mock.Anything)
	})
}

func TestCreateJobAccountLookupError(t *testing.T) {
	storage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: storage}

	originalGetOrCreate := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, _ database.Storage, _ string) (*datamodel.Account, error) {
		return nil, errors.New("lookup failed")
	}
	defer func() { getOrCreateAccount = originalGetOrCreate }()

	params := &common.CreateJobParams{
		AccountName: "project-1",
		Type:        models.JobTypeCreatePool,
	}

	job, err := orch.CreateJob(context.Background(), params)
	require.Nil(t, job)
	require.Error(t, err)
	require.Equal(t, "lookup failed", err.Error())
	storage.AssertNotCalled(t, "CreateJob", mock.Anything, mock.Anything)
}

func TestCreateJobDefaultsAndContextFallback(t *testing.T) {
	storage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: storage}

	originalGetOrCreate := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, _ database.Storage, accountName string) (*datamodel.Account, error) {
		require.Equal(t, "project-1", accountName)
		return &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}}, nil
	}
	defer func() { getOrCreateAccount = originalGetOrCreate }()

	jobAttributes := &datamodel.JobAttributes{ResourceUUID: "resource-uuid"}
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
	}

	storage.EXPECT().
		CreateJob(mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
			require.Equal(t, string(models.JobTypeCreatePool), job.Type)
			require.Equal(t, string(models.JobsStateNEW), job.State)
			require.Equal(t, "resource-name", job.ResourceName)
			require.Equal(t, int64(42), job.AccountID.Int64)
			require.True(t, job.AccountID.Valid)
			require.Equal(t, jobAttributes, job.JobAttributes)
			require.Equal(t, "ctx-corr", job.CorrelationID)
			require.Equal(t, "ctx-request", job.RequestID)
			require.False(t, job.IsAdminJob)
			return true
		})).
		Return(expectedJob, nil).
		Once()

	fields := log.Fields{
		string(middleware.RequestCorrelationID): "ctx-corr",
		string(middleware.RequestID):            "ctx-request",
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)

	params := &common.CreateJobParams{
		AccountName:   "project-1",
		Type:          models.JobTypeCreatePool,
		ResourceName:  "resource-name",
		JobAttributes: jobAttributes,
	}

	job, err := orch.CreateJob(ctx, params)
	require.NoError(t, err)
	require.Equal(t, expectedJob, job)
}

func TestCreateJobHonoursExplicitParams(t *testing.T) {
	storage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: storage}

	originalGetOrCreate := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, _ database.Storage, accountName string) (*datamodel.Account, error) {
		return &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 7}}, nil
	}
	defer func() { getOrCreateAccount = originalGetOrCreate }()

	jobAttributes := &datamodel.JobAttributes{ResourceUUID: "explicit-resource"}
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid-explicit"},
	}

	storage.EXPECT().
		CreateJob(mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
			require.Equal(t, string(models.JobTypeDeletePool), job.Type)
			require.Equal(t, string(models.JobsStatePROCESSING), job.State)
			require.Equal(t, "named-resource", job.ResourceName)
			require.Equal(t, jobAttributes, job.JobAttributes)
			require.Equal(t, "explicit-corr", job.CorrelationID)
			require.Equal(t, "explicit-request", job.RequestID)
			require.True(t, job.IsAdminJob)
			return true
		})).
		Return(expectedJob, nil).
		Once()

	params := &common.CreateJobParams{
		AccountName:   "project-1",
		Type:          models.JobTypeDeletePool,
		State:         models.JobsStatePROCESSING,
		ResourceName:  "named-resource",
		JobAttributes: jobAttributes,
		CorrelationID: "explicit-corr",
		RequestID:     "explicit-request",
		IsAdminJob:    true,
	}

	job, err := orch.CreateJob(context.Background(), params)
	require.NoError(t, err)
	require.Equal(t, expectedJob, job)
}
