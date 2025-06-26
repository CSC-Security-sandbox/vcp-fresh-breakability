package adminbackgroundjobs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"gorm.io/gorm"
)

func TestLoadJobSpecs(t *testing.T) {
	t.Run("WhenDataHasValidJSON", func(tt *testing.T) {
		data = []byte(`{"SYNC_VSA_CLUSTER_STATUS": {"JobType": "SYNC_VSA_CLUSTER_STATUS", "CronExpression": "*/5 * * * *", "State": "CREATING"}}`)
		err := LoadJobSpecs()
		assert.NoError(tt, err)
		assert.Len(tt, adminJobSpecs, 1)
		adminJobSpecs = make(map[string]*datamodel.AdminJobSpec) // Reset for next test
	})
	t.Run("WhenDataHasEmptyJSON", func(tt *testing.T) {
		data = []byte(`{}`)
		err := LoadJobSpecs()
		assert.NoError(tt, err)
		assert.Len(tt, adminJobSpecs, 0)
		adminJobSpecs = make(map[string]*datamodel.AdminJobSpec)
	})
	t.Run("WhenDataHasInvalidJSON", func(tt *testing.T) {
		data = []byte(`{Name: John Doe, Age: 30}`)
		err := LoadJobSpecs()
		assert.Error(tt, err)
		assert.Len(tt, adminJobSpecs, 0)
		adminJobSpecs = make(map[string]*datamodel.AdminJobSpec)
	})
}

func TestIsJobSpecRefreshNeeded(t *testing.T) {
	ctx := context.TODO()
	logger := util.GetLogger(ctx)

	t.Run("WhenJobSpecsShouldBeRefreshed", func(tt *testing.T) {
		adminJobSpecs = map[string]*datamodel.AdminJobSpec{
			"ADMIN_JOB_1": {
				JobType:        "ADMIN_JOB_1",
				CronExpression: "0 */5 * * * *",
				State:          "CREATING",
			},
			"ADMIN_JOB_2": {
				JobType:        "ADMIN_JOB_2",
				CronExpression: "0 */10 * * * *",
				State:          "UPDATING",
			},
			"ADMIN_JOB_3": {
				JobType:        "ADMIN_JOB_3",
				CronExpression: "0 */15 * * * *",
				State:          "DELETING",
			},
		}

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("GetAdminJobSpecByJobType", ctx, "ADMIN_JOB_1").Return(nil, gorm.ErrRecordNotFound)
		mockStorage.On("GetAdminJobSpecByJobType", ctx, "ADMIN_JOB_2").Return(&datamodel.AdminJobSpec{CronExpression: "0 */5 * * * *"}, nil)
		mockStorage.On("GetAdminJobSpecByJobType", ctx, "ADMIN_JOB_3").Return(&datamodel.AdminJobSpec{State: scheduler.JobStatusScheduled}, nil)

		mockStorage.On("CreateAdminJobSpec", ctx, mock.Anything).Return(&datamodel.AdminJobSpec{JobType: "ADMIN_JOB_1", CronExpression: "0 */5 * * * *"}, nil)
		mockStorage.On("UpdateAdminJobSpec", ctx, mock.Anything).Return(nil)

		shouldRefreshAdminJobSpecs, err := IsJobSpecRefreshNeeded(ctx, mockStorage, logger)
		if err != nil {
			tt.Fatalf("Expected no error, got %v", err)
		}

		assert.True(tt, shouldRefreshAdminJobSpecs)
	})
	t.Run("WhenFetchingExistingJobSpecFails", func(tt *testing.T) {
		adminJobSpecs = map[string]*datamodel.AdminJobSpec{
			"ADMIN_JOB_1": {
				JobType:        "ADMIN_JOB_1",
				CronExpression: "0 */5 * * * *",
				State:          "CREATING",
			},
		}

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("GetAdminJobSpecByJobType", ctx, "ADMIN_JOB_1").Return(nil, gorm.ErrInvalidDB)

		shouldRefreshAdminJobSpecs, err := IsJobSpecRefreshNeeded(ctx, mockStorage, logger)
		if err != nil {
			tt.Fatalf("Expected no error, got %v", err)
		}

		assert.False(tt, shouldRefreshAdminJobSpecs)
	})
	t.Run("WhenAdminJobSpecCreationFailsWithUniqueConstraintViolation", func(tt *testing.T) {
		adminJobSpecs = map[string]*datamodel.AdminJobSpec{
			"ADMIN_JOB_1": {
				JobType:        "ADMIN_JOB_1",
				CronExpression: "0 */5 * * * *",
				State:          "CREATING",
			},
			"ADMIN_JOB_2": {
				JobType:        "ADMIN_JOB_2",
				CronExpression: "0 */10 * * * *",
				State:          "UPDATING",
			},
		}

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("GetAdminJobSpecByJobType", ctx, "ADMIN_JOB_1").Return(nil, gorm.ErrRecordNotFound)
		mockStorage.On("GetAdminJobSpecByJobType", ctx, "ADMIN_JOB_2").Return(
			&datamodel.AdminJobSpec{CronExpression: "0 */5 * * * *"}, nil)
		mockStorage.On("CreateAdminJobSpec", ctx, mock.Anything).Return(
			nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, gorm.ErrDuplicatedKey))
		mockStorage.On("UpdateAdminJobSpec", ctx, mock.Anything).Return(nil)

		shouldRefreshAdminJobSpecs, err := IsJobSpecRefreshNeeded(ctx, mockStorage, logger)
		if err != nil {
			tt.Fatalf("Expected no error, got %v", err)
		}

		assert.True(tt, shouldRefreshAdminJobSpecs)
	})
	t.Run("WhenAdminJobSpecUpdateFails", func(tt *testing.T) {
		adminJobSpecs = map[string]*datamodel.AdminJobSpec{
			"ADMIN_JOB_1": {
				JobType:        "ADMIN_JOB_1",
				CronExpression: "0 */10 * * * *",
				State:          "UPDATING",
			},
		}

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("GetAdminJobSpecByJobType", ctx, "ADMIN_JOB_1").Return(
			&datamodel.AdminJobSpec{CronExpression: "0 */5 * * * *"}, nil)
		mockStorage.On("UpdateAdminJobSpec", ctx, mock.Anything).Return(gorm.ErrInvalidValue)

		shouldRefreshAdminJobSpecs, err := IsJobSpecRefreshNeeded(ctx, mockStorage, logger)
		if err != nil {
			tt.Fatalf("Expected no error, got %v", err)
		}

		assert.False(tt, shouldRefreshAdminJobSpecs)
	})
	t.Run("WhenAdminJobSpecDeletionFails", func(tt *testing.T) {
		adminJobSpecs = map[string]*datamodel.AdminJobSpec{
			"ADMIN_JOB_1": {
				JobType:        "ADMIN_JOB_1",
				CronExpression: "0 */10 * * * *",
				State:          "DELETING",
			},
		}

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("GetAdminJobSpecByJobType", ctx, "ADMIN_JOB_1").Return(
			&datamodel.AdminJobSpec{State: scheduler.JobStatusScheduled}, nil)
		mockStorage.On("UpdateAdminJobSpec", ctx, mock.Anything).Return(gorm.ErrInvalidValue)

		shouldRefreshAdminJobSpecs, err := IsJobSpecRefreshNeeded(ctx, mockStorage, logger)
		if err != nil {
			tt.Fatalf("Expected no error, got %v", err)
		}

		assert.False(tt, shouldRefreshAdminJobSpecs)
	})
}

func TestLaunchJobManagerWorkflow(t *testing.T) {
	ctx := context.TODO()
	logger := util.GetLogger(ctx)

	t.Run("WhenJobManagerWorkflowLaunchesSuccessfully", func(tt *testing.T) {
		client := workflow_engine.NewMockTemporalTestClient(t)
		client.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		err := LaunchJobManagerWorkflow(context.TODO(), client, logger)
		assert.NoError(tt, err)
	})
	t.Run("WhenJobManagerWorkflowLaunchFails", func(tt *testing.T) {
		client := workflow_engine.NewMockTemporalTestClient(t)
		client.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil, temporal.ErrScheduleAlreadyRunning)

		err := LaunchJobManagerWorkflow(context.TODO(), client, logger)
		assert.Error(tt, err)
	})
}
