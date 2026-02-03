package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"gorm.io/gorm"
)

func TestCreateVolume_JobUpdateOnWorkflowFailure(t *testing.T) {
	t.Run("ShouldMarkJobAsErrorWhenWorkflowFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:   datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				Name:        "test-pool",
				AccountID:   1,
				Account:     account,
				SizeInBytes: 1000000000000,
				Network:     "test-network",
				State:       models.LifeCycleStateREADY,
				VendorID:    "/projects/test-project/locations/us-west1/pools/test-pool",
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-west1-a",
				},
				APIAccessMode: common.DEFAULTMode,
			},
			QuotaInBytes: 0,
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "svm-uuid"},
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			AccountName:  "test-account",
			Zone:         "us-west1-a",
			VendorID:     "/projects/test-project/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			PoolID:       "pool-uuid",
			QuotaInBytes: 100000000000,
			Network:      "test-network",
			Protocols:    []string{"iscsi"},
		}

		// Mock successful setup calls
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		mockStorage.On("GetPool", ctx, params.PoolID, account.ID).Return(pool, nil)
		mockStorage.On("GetVolumeByNameAccountIDAndZone", ctx, params.Name, account.ID, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.IsRegionalHA).Return(nil, errors.New("volume not found"))
		mockStorage.On("GetSvmForPoolID", ctx, pool.ID).Return(svm, nil)
	mockStorage.On("CreateVolume", ctx, mock.AnythingOfType("*datamodel.Volume")).Return(&datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Account:   account,
		AccountID: account.ID,
		Pool:      &pool.Pool,
		PoolID:    pool.ID,
	}, nil)
	mockStorage.On("DeleteVolume", ctx, "volume-uuid").Return(&datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "volume-uuid"}}, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock UpdateJob call to mark job as error
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(nil).Once()

		// Execute test
		_, _, err := _createVolume(ctx, mockStorage, mockTemporal, params)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

func TestRevertVolume_JobUpdateOnWorkflowFailure(t *testing.T) {
	t.Run("ShouldMarkJobAsErrorWhenWorkflowFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid"},
			Name:         "test-volume",
			AccountID:    1,
			Account:      account,
			Pool:         pool,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.RevertVolumeParams{
			AccountName: "test-account",
			VolumeID:    "volume-uuid",
			SnapshotID:  "snapshot-uuid",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		updateVolumeStatus = func(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume, state string, stateDetails string) (*datamodel.Volume, error) {
			volume.State = state
			volume.StateDetails = stateDetails
			return volume, nil
		}
		defer func() {
			getAccountWithName = _getAccountWithName
			updateVolumeStatus = _updateVolumeStatus
		}()

		mockStorage.On("GetVolumeWithAccountID", ctx, params.VolumeID, account.ID).Return(volume, nil)
		mockStorage.On("GetSnapshotByUUID", ctx, params.SnapshotID, volume.Account.ID, volume.ID).Return(snapshot, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock DeleteJob call to delete job on error
		mockStorage.On("DeleteJob", ctx, job.UUID, workflowErr.Error()).Return(nil)

		// Mock UpdateVolumeFields call to revert volume back to READY state
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateREADY,
			"state_details": models.LifeCycleStateAvailableDetails,
		}).Return(nil)

		// Execute test
		_, _, err := _revertVolume(ctx, mockStorage, mockTemporal, params)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

func TestDeleteVolume_JobUpdateOnWorkflowFailure(t *testing.T) {
	t.Run("ShouldMarkJobAsErrorWhenWorkflowFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
			Account:   account,
			Pool:      pool,
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		validateDeleteVolumeParams = func(ctx context.Context, se database.Storage, volume *datamodel.Volume) error {
			return nil
		}
		defer func() {
			validateDeleteVolumeParams = _validateDeleteVolumeParams
		}()

		mockStorage.On("GetVolume", ctx, "volume-uuid").Return(volume, nil)
		// Mock GetJobByResourceUUID to return nil (no existing job) since code checks for existing jobs
		mockStorage.On("GetJobByResourceUUID", ctx, "volume-uuid", string(models.JobTypeDeleteVolume)).Return(nil, errors.New("Job not found"))
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateDeleting,
			"state_details": models.LifeCycleStateDeletingDetails,
		}).Return(nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock UpdateJob call to mark job as error
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(nil)

		// Mock UpdateVolumeFields call to mark volume as error
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateError,
			"state_details": models.LifeCycleStateDeletionErrorDetails,
		}).Return(nil)

		// Execute test
		_, _, err := _deleteVolume(ctx, mockStorage, mockTemporal, "volume-uuid")

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdateVolume_JobUpdateOnWorkflowFailure(t *testing.T) {
	t.Run("ShouldMarkJobAsErrorWhenWorkflowFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				Name:             "test-pool",
				AccountID:        1,
				Account:          account,
				SizeInBytes:      1000000000000,
				AllowAutoTiering: false,
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-west1-a",
				},
			},
			QuotaInBytes: 0,
		}

		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{UUID: "volume-uuid"},
			Name:             "test-volume",
			AccountID:        1,
			Account:          account,
			State:            models.LifeCycleStateREADY,
			SizeInBytes:      100000000000,
			VolumeAttributes: &datamodel.VolumeAttributes{},
			Pool:             database.ConvertPoolViewToPool(pool),
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.UpdateVolumeParams{
			VolumeId:     "volume-uuid",
			PoolID:       "pool-uuid",
			QuotaInBytes: 200000000000,
		}

		updateVolumeStatus = func(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume, state string, stateDetails string) (*datamodel.Volume, error) {
			volume.State = state
			volume.StateDetails = stateDetails
			return volume, nil
		}
		defer func() {
			updateVolumeStatus = _updateVolumeStatus
		}()

		mockStorage.On("GetVolume", ctx, params.VolumeId).Return(volume, nil)
		mockStorage.On("GetPool", ctx, "pool-uuid", volume.AccountID).Return(pool, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		mockTemporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowErr)

		// Mock UpdateJob call to mark job as error
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(nil)

		// Mock UpdateVolumeFields call to mark volume as error
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateError,
			"state_details": models.LifeCycleStateUpdateErrorDetails,
		}).Return(nil)

		// Execute test
		_, _, err := _updateVolume(ctx, mockStorage, mockTemporal, params, false)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})
}

// Test for line 253: Failed to update volume state to DELETED during createVolume
func TestCreateVolume_FailedVolumeDeleteOnError(t *testing.T) {
	t.Run("ShouldLogErrorWhenVolumeDeleteFailsDuringErrorHandling", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:   datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				Name:        "test-pool",
				AccountID:   1,
				Account:     account,
				SizeInBytes: 1000000000000,
				Network:     "test-network",
				State:       models.LifeCycleStateREADY,
				VendorID:    "/projects/test-project/locations/us-west1/pools/test-pool",
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-west1-a",
				},
				APIAccessMode: common.DEFAULTMode,
			},
			QuotaInBytes: 0,
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "svm-uuid"},
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			AccountName:  "test-account",
			Zone:         "us-west1-a",
			VendorID:     "/projects/test-project/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			PoolID:       "pool-uuid",
			QuotaInBytes: 100000000000,
			Network:      "test-network",
			Protocols:    []string{"iscsi"},
		}

		// Mock successful setup calls
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		mockStorage.On("GetPool", ctx, params.PoolID, account.ID).Return(pool, nil)
		mockStorage.On("GetVolumeByNameAccountIDAndZone", ctx, params.Name, account.ID, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.IsRegionalHA).Return(nil, gorm.ErrRecordNotFound)
		mockStorage.On("GetSvmForPoolID", ctx, pool.ID).Return(svm, nil)
	mockStorage.On("CreateVolume", ctx, mock.AnythingOfType("*datamodel.Volume")).Return(&datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Account:   account,
		AccountID: account.ID,
		Pool:      &pool.Pool,
		PoolID:    pool.ID,
	}, nil)
	mockStorage.On("DeleteVolume", ctx, "volume-uuid").Return(&datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "volume-uuid"}}, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock UpdateJob call to mark job as error
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(nil)

		// Execute test
		_, _, err := _createVolume(ctx, mockStorage, mockTemporal, params)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

// Test for line 284: Failed to update job state to ERROR during createVolume
func TestCreateVolume_FailedJobUpdateOnError(t *testing.T) {
	t.Run("ShouldLogErrorWhenJobUpdateFailsDuringErrorHandling", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:   datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				Name:        "test-pool",
				AccountID:   1,
				Account:     account,
				SizeInBytes: 1000000000000,
				Network:     "test-network",
				State:       models.LifeCycleStateREADY,
				VendorID:    "/projects/test-project/locations/us-west1/pools/test-pool",
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-west1-a",
				},
				APIAccessMode: common.DEFAULTMode,
			},
			QuotaInBytes: 0,
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "svm-uuid"},
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			AccountName:  "test-account",
			Zone:         "us-west1-a",
			VendorID:     "/projects/test-project/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			PoolID:       "pool-uuid",
			QuotaInBytes: 100000000000,
			Network:      "test-network",
			Protocols:    []string{"iscsi"},
		}

		// Mock successful setup calls
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		mockStorage.On("GetPool", ctx, params.PoolID, account.ID).Return(pool, nil)
		mockStorage.On("GetVolumeByNameAccountIDAndZone", ctx, params.Name, account.ID, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.IsRegionalHA).Return(nil, gorm.ErrRecordNotFound)
		mockStorage.On("GetSvmForPoolID", ctx, pool.ID).Return(svm, nil)
	mockStorage.On("CreateVolume", ctx, mock.AnythingOfType("*datamodel.Volume")).Return(&datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Account:   account,
		AccountID: account.ID,
		Pool:      &pool.Pool,
		PoolID:    pool.ID,
	}, nil)
	mockStorage.On("DeleteVolume", ctx, "volume-uuid").Return(&datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "volume-uuid"}}, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock UpdateJob call to fail - this is what triggers line 480
		jobUpdateErr := errors.New("failed to update job")
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(jobUpdateErr)

		// Execute test
		_, _, err := _createVolume(ctx, mockStorage, mockTemporal, params)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

// Test for line 376: Failed to update job state to ERROR during revertVolume
func TestRevertVolume_FailedJobUpdateOnError_Line376(t *testing.T) {
	t.Run("ShouldLogErrorWhenJobUpdateFailsDuringErrorHandling", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid"},
			Name:         "test-volume",
			AccountID:    1,
			Account:      account,
			Pool:         pool,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.RevertVolumeParams{
			AccountName: "test-account",
			VolumeID:    "volume-uuid",
			SnapshotID:  "snapshot-uuid",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		updateVolumeStatus = func(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume, state string, stateDetails string) (*datamodel.Volume, error) {
			volume.State = state
			volume.StateDetails = stateDetails
			return volume, nil
		}
		defer func() {
			getAccountWithName = _getAccountWithName
			updateVolumeStatus = _updateVolumeStatus
		}()

		mockStorage.On("GetVolumeWithAccountID", ctx, params.VolumeID, account.ID).Return(volume, nil)
		mockStorage.On("GetSnapshotByUUID", ctx, params.SnapshotID, volume.Account.ID, volume.ID).Return(snapshot, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock DeleteJob call to fail - this tests error handling when DeleteJob fails
		jobDeleteErr := errors.New("failed to delete job")
		mockStorage.On("DeleteJob", ctx, job.UUID, workflowErr.Error()).Return(jobDeleteErr)

		// Mock UpdateVolumeFields call to succeed
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateREADY,
			"state_details": models.LifeCycleStateAvailableDetails,
		}).Return(nil)

		// Execute test
		_, _, err := _revertVolume(ctx, mockStorage, mockTemporal, params)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

// Test for line 396: Failed to update volume state back to READY during revertVolume
func TestRevertVolume_FailedVolumeUpdateBackToReady(t *testing.T) {
	t.Run("ShouldLogErrorWhenVolumeUpdateBackToReadyFailsDuringErrorHandling", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid"},
			Name:         "test-volume",
			AccountID:    1,
			Account:      account,
			Pool:         pool,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.RevertVolumeParams{
			AccountName: "test-account",
			VolumeID:    "volume-uuid",
			SnapshotID:  "snapshot-uuid",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		updateVolumeStatus = func(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume, state string, stateDetails string) (*datamodel.Volume, error) {
			volume.State = state
			volume.StateDetails = stateDetails
			return volume, nil
		}
		defer func() {
			getAccountWithName = _getAccountWithName
			updateVolumeStatus = _updateVolumeStatus
		}()

		mockStorage.On("GetVolumeWithAccountID", ctx, params.VolumeID, account.ID).Return(volume, nil)
		mockStorage.On("GetSnapshotByUUID", ctx, params.SnapshotID, volume.Account.ID, volume.ID).Return(snapshot, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock DeleteJob call to succeed
		mockStorage.On("DeleteJob", ctx, job.UUID, workflowErr.Error()).Return(nil)

		// Mock UpdateVolumeFields call to fail - this tests error handling when volume update fails
		volumeUpdateErr := errors.New("failed to update volume fields")
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateREADY,
			"state_details": models.LifeCycleStateAvailableDetails,
		}).Return(volumeUpdateErr)

		// Execute test
		_, _, err := _revertVolume(ctx, mockStorage, mockTemporal, params)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

// Test for line 985: Failed to update job state to ERROR during deleteVolume
func TestDeleteVolume_FailedJobUpdateOnError_Line985(t *testing.T) {
	t.Run("ShouldLogErrorWhenJobUpdateFailsDuringErrorHandling", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool",
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
			Account:   account,
			Pool:      pool,
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		validateDeleteVolumeParams = func(ctx context.Context, se database.Storage, volume *datamodel.Volume) error {
			return nil
		}
		defer func() {
			validateDeleteVolumeParams = _validateDeleteVolumeParams
		}()

		mockStorage.On("GetVolume", ctx, "volume-uuid").Return(volume, nil)
		mockStorage.On("GetJobByResourceUUID", ctx, "volume-uuid", mock.Anything).Return(nil, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateDeleting,
			"state_details": models.LifeCycleStateDeletingDetails,
		}).Return(nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock UpdateJob call to fail - this is what triggers line 985
		jobUpdateErr := errors.New("failed to update job")
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(jobUpdateErr)

		// Mock UpdateVolumeFields call to succeed for error state
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateError,
			"state_details": models.LifeCycleStateDeletionErrorDetails,
		}).Return(nil)

		// Execute test
		_, _, err := _deleteVolume(ctx, mockStorage, mockTemporal, "volume-uuid")

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

// Test for line 1014: Failed to update volume state to ERROR during deleteVolume
func TestDeleteVolume_FailedVolumeUpdateOnError_Line1014(t *testing.T) {
	t.Run("ShouldLogErrorWhenVolumeUpdateFailsDuringErrorHandling", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
			Account:   account,
			Pool:      pool,
			State:     models.LifeCycleStateREADY,
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		validateDeleteVolumeParams = func(ctx context.Context, se database.Storage, volume *datamodel.Volume) error {
			return nil
		}
		defer func() {
			validateDeleteVolumeParams = _validateDeleteVolumeParams
		}()

		mockStorage.On("GetVolume", ctx, "volume-uuid").Return(volume, nil)
		// Mock GetJobByResourceUUID for DELETE_VOLUME (called for non-transitional states)
		mockStorage.On("GetJobByResourceUUID", ctx, volume.UUID, string(models.JobTypeDeleteVolume)).Return(nil, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateDeleting,
			"state_details": models.LifeCycleStateDeletingDetails,
		}).Return(nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return workflowErr
		}
		defer func() {
			workflows.ExecuteWorkflowSeq = workflows.ExecuteWorkflowSequentially
		}()

		// Mock UpdateJob call to succeed
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(nil)

		// Mock UpdateVolumeFields call to fail - this is what triggers line 1014
		volumeUpdateErr := errors.New("failed to update volume fields")
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateError,
			"state_details": models.LifeCycleStateDeletionErrorDetails,
		}).Return(volumeUpdateErr)

		// Execute test
		_, _, err := _deleteVolume(ctx, mockStorage, mockTemporal, "volume-uuid")

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

// Test for line 1163: Failed to update job state to ERROR during updateVolume
func TestUpdateVolume_FailedJobUpdateOnError_Line1163(t *testing.T) {
	t.Run("ShouldLogErrorWhenJobUpdateFailsDuringErrorHandling", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				Name:             "test-pool",
				AccountID:        1,
				Account:          account,
				SizeInBytes:      1000000000000,
				AllowAutoTiering: false,
			},
			QuotaInBytes: 0,
		}

		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{UUID: "volume-uuid"},
			Name:             "test-volume",
			AccountID:        1,
			Account:          account,
			State:            models.LifeCycleStateREADY,
			SizeInBytes:      100000000000,
			VolumeAttributes: &datamodel.VolumeAttributes{},
			Pool:             database.ConvertPoolViewToPool(pool),
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.UpdateVolumeParams{
			VolumeId:     "volume-uuid",
			PoolID:       "pool-uuid",
			QuotaInBytes: 200000000000,
		}

		updateVolumeStatus = func(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume, state string, stateDetails string) (*datamodel.Volume, error) {
			volume.State = state
			volume.StateDetails = stateDetails
			return volume, nil
		}
		defer func() {
			updateVolumeStatus = _updateVolumeStatus
		}()

		mockStorage.On("GetVolume", ctx, params.VolumeId).Return(volume, nil)
		mockStorage.On("GetPool", ctx, params.PoolID, volume.AccountID).Return(pool, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		mockTemporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowErr)

		// Mock UpdateJob call to fail - this is what triggers line 1163
		jobUpdateErr := errors.New("failed to update job")
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(jobUpdateErr)

		// Mock UpdateVolumeFields call to succeed
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateError,
			"state_details": models.LifeCycleStateUpdateErrorDetails,
		}).Return(nil)

		// Execute test
		_, _, err := _updateVolume(ctx, mockStorage, mockTemporal, params, false)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})
}

// Test for line 1185: Failed to update volume state to ERROR during updateVolume
func TestUpdateVolume_FailedVolumeUpdateOnError_Line1185(t *testing.T) {
	t.Run("ShouldLogErrorWhenVolumeUpdateFailsDuringErrorHandling", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
				Name:             "test-pool",
				AccountID:        1,
				Account:          account,
				SizeInBytes:      1000000000000,
				AllowAutoTiering: false,
			},
			QuotaInBytes: 0,
		}

		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{UUID: "volume-uuid"},
			Name:             "test-volume",
			AccountID:        1,
			Account:          account,
			State:            models.LifeCycleStateREADY,
			SizeInBytes:      100000000000,
			VolumeAttributes: &datamodel.VolumeAttributes{},
			Pool:             database.ConvertPoolViewToPool(pool),
		}

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "job-uuid",
		}

		params := &common.UpdateVolumeParams{
			VolumeId:     "volume-uuid",
			PoolID:       "pool-uuid",
			QuotaInBytes: 200000000000,
		}

		updateVolumeStatus = func(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume, state string, stateDetails string) (*datamodel.Volume, error) {
			volume.State = state
			volume.StateDetails = stateDetails
			return volume, nil
		}
		defer func() {
			updateVolumeStatus = _updateVolumeStatus
		}()

		mockStorage.On("GetVolume", ctx, params.VolumeId).Return(volume, nil)
		mockStorage.On("GetPool", ctx, params.PoolID, volume.AccountID).Return(pool, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

		// Mock workflow failure
		workflowErr := errors.New("workflow execution failed")
		mockTemporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowErr)

		// Mock UpdateJob call to succeed
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, workflowErr.Error()).Return(nil)

		// Mock UpdateVolumeFields call to fail - this is what triggers line 1185
		volumeUpdateErr := errors.New("failed to update volume fields")
		mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateError,
			"state_details": models.LifeCycleStateUpdateErrorDetails,
		}).Return(volumeUpdateErr)

		// Execute test
		_, _, err := _updateVolume(ctx, mockStorage, mockTemporal, params, false)

		// Assertions
		assert.Error(tt, err)
		assert.Equal(tt, workflowErr.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})
}
