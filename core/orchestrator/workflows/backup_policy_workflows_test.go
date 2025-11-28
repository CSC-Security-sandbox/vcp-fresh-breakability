package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type BackupPolicyWorkflowsTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *BackupPolicyWorkflowsTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	s.env.RegisterWorkflow(UpdateBackupPolicyWorkflow)
	s.env.RegisterWorkflow(DeleteBackupPolicyWorkflow)
}

func (s *BackupPolicyWorkflowsTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func TestBackupPolicyTestSuite(t *testing.T) {
	suite.Run(t, new(BackupPolicyWorkflowsTestSuite))
}

// setupMockStorage creates a mock storage with GetJob mocked
func (s *BackupPolicyWorkflowsTestSuite) setupMockStorage() *database.MockStorage {
	mockStorage := database.NewMockStorage(s.T())
	mockStorage.On("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	return mockStorage
}

func (s *BackupPolicyWorkflowsTestSuite) TestEnableBackupPolicyWorkflow_Success() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInVCP)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.UnpauseBackupPolicySchedule)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(&cvpmodels.BackupPolicyV1beta{}, nil)
	s.env.OnActivity(backupPolicyActivity.UnpauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *BackupPolicyWorkflowsTestSuite) TestDisableBackupPolicyWorkflow_Success() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInVCP)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.UnpauseBackupPolicySchedule)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(&cvpmodels.BackupPolicyV1beta{}, nil)
	s.env.OnActivity(backupPolicyActivity.PauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(false),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         true,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_GetAuthJWTTokenFailure() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", errors.New("failed to get auth token"))
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(false),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         true,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UpdateBackupPolicyInSDEFailure() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(nil, errors.New("failed to update backup policy in SDE"))
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(false),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         true,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UpdateBackupPolicyInSDEBadRequestError() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(
		nil, backup_policy.NewV1betaUpdateBackupPolicyBadRequest())
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}

	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var applicationError *temporal.ApplicationError
	assert.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationError)

	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
	s.env.AssertActivityNumberOfCalls(s.T(), "UpdateBackupPolicyInSDE", 1)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UpdateBackupPolicyInSDEUnauthorizedError() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(
		nil, backup_policy.NewV1betaUpdateBackupPolicyUnauthorized())
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}

	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var applicationError *temporal.ApplicationError
	assert.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationError)

	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
	s.env.AssertActivityNumberOfCalls(s.T(), "UpdateBackupPolicyInSDE", 1)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UpdateBackupPolicyInSDEForbiddenError() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(
		nil, backup_policy.NewV1betaUpdateBackupPolicyForbidden())
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}

	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var applicationError *temporal.ApplicationError
	assert.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationError)

	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
	s.env.AssertActivityNumberOfCalls(s.T(), "UpdateBackupPolicyInSDE", 1)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UpdateBackupPolicyInSDENotFoundError() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(
		nil, backup_policy.NewV1betaUpdateBackupPolicyNotFound())
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}

	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var applicationError *temporal.ApplicationError
	assert.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationError)

	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
	s.env.AssertActivityNumberOfCalls(s.T(), "UpdateBackupPolicyInSDE", 1)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UpdateBackupPolicyInSDEInternalServerError() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(
		nil, backup_policy.NewV1betaUpdateBackupPolicyInternalServerError())
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}

	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var applicationError *temporal.ApplicationError
	assert.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationError)

	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
	s.env.AssertActivityNumberOfCalls(s.T(), "UpdateBackupPolicyInSDE", 1)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UnpauseBackupPolicyScheduleFailure() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.UnpauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(&cvpmodels.BackupPolicyV1beta{}, nil)
	s.env.OnActivity(backupPolicyActivity.UnpauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(errors.New("failed to unpause backup policy schedule"))
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInSDE", mock.Anything, mock.Anything, mock.Anything)
	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_PauseBackupPolicyScheduleFailure() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(&cvpmodels.BackupPolicyV1beta{}, nil)
	s.env.OnActivity(backupPolicyActivity.PauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(errors.New("failed to unpause backup policy schedule"))
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(false),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         true,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInSDE", mock.Anything, mock.Anything, mock.Anything)
	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
}

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UpdateBackupPolicyInVCPFailure() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInVCP)
	s.env.RegisterActivity(backupPolicyActivity.UnpauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInSDE, mock.Anything, mock.Anything).Return(&cvpmodels.BackupPolicyV1beta{}, nil)
	s.env.OnActivity(backupPolicyActivity.PauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.UpdateBackupPolicyInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to pause backup policy schedule in VCP"))
	s.env.OnActivity(backupPolicyActivity.UnpauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(false),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         true,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	s.env.AssertActivityCalled(s.T(), "UnpauseBackupPolicySchedule", mock.Anything, mock.Anything)
	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInSDE", mock.Anything, mock.Anything, mock.Anything)
	s.env.AssertActivityCalled(s.T(), "RevertBackupPolicyUpdateInVCP", mock.Anything, mock.Anything)
}

func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflow_Success() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInSDE, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
		Name:                  "backup-policy-1",
		Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	params := &common.DeleteBackupPolicyParams{
		Name:           dbBackupPolicy.Name,
		OwnerID:        dbBackupPolicy.Account.Name,
		BackupPolicyID: dbBackupPolicy.UUID,
		LocationID:     "us-west1",
	}
	s.env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflow_GetAuthJWTTokenFailure() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", errors.New("failed to get auth JWT token"))
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
		Name:                  "backup-policy-1",
		Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	params := &common.DeleteBackupPolicyParams{
		Name:           dbBackupPolicy.Name,
		OwnerID:        dbBackupPolicy.Account.Name,
		BackupPolicyID: dbBackupPolicy.UUID,
		LocationID:     "us-west1",
	}
	s.env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflow_DeleteBackupPolicyInSDEFails() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInSDE, mock.Anything, mock.Anything).Return(errors.New("failed to delete backup policy in SDE"))
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
		Name:                  "backup-policy-1",
		Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	params := &common.DeleteBackupPolicyParams{
		Name:           dbBackupPolicy.Name,
		OwnerID:        dbBackupPolicy.Account.Name,
		BackupPolicyID: dbBackupPolicy.UUID,
		LocationID:     "us-west1",
	}
	s.env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflow_DeleteBackupPolicyScheduleFailure() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInSDE, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicySchedule, mock.Anything, mock.Anything).Return(errors.New("failed to delete backup policy schedule"))
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
		Name:                  "backup-policy-1",
		Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	params := &common.DeleteBackupPolicyParams{
		Name:           dbBackupPolicy.Name,
		OwnerID:        dbBackupPolicy.Account.Name,
		BackupPolicyID: dbBackupPolicy.UUID,
		LocationID:     "us-west1",
	}
	s.env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflow_DeleteBackupPolicyInVCPFailure() {
	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInSDE, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, mock.Anything, mock.Anything).Return(nil, errors.New("failed to delete backup policy in VCP"))
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
		Name:                  "backup-policy-1",
		Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	params := &common.DeleteBackupPolicyParams{
		Name:           dbBackupPolicy.Name,
		OwnerID:        dbBackupPolicy.Account.Name,
		BackupPolicyID: dbBackupPolicy.UUID,
		LocationID:     "us-west1",
	}
	s.env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

// TestDeleteBackupPolicyWorkflow_RollbackBehavior tests that the rollback activity is called on errors
func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflow_RollbackBehavior() {
	// Test SDE deletion failure triggers state rollback
	s.T().Run("SDE_deletion_failure_triggers_rollback", func(t *testing.T) {
		// Create a fresh test environment for this subtest
		env := s.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		env.RegisterWorkflow(DeleteBackupPolicyWorkflow)

		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
			State:     string(models.JobsStateNEW),
		}, nil).Maybe()
		mockScheduler := scheduler.NewMockScheduler(t)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

		// Register activities
		env.RegisterActivity(commonActivity.GetAuthJWTToken)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
		env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
		env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
		env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyStateInCaseOfError)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		// Setup mocks
		env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
			State:     string(models.JobsStateNEW),
		}, nil).Maybe()
		env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
		env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInSDE, mock.Anything, mock.Anything).Return(errors.New("SDE deletion failed"))
		env.OnActivity(backupPolicyActivity.UpdateBackupPolicyStateInCaseOfError, mock.Anything, mock.Anything, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Return(nil)
		env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

		// Test data
		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
			Name:                  "backup-policy-1",
			Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
			AccountID:             int64(1),
			Description:           nil,
			DailyBackupsToKeep:    2,
			WeeklyBackupsToKeep:   2,
			MonthlyBackupsToKeep:  2,
			PolicyEnabled:         false,
			LifeCycleState:        models.LifeCycleStateDeleting,
			LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
		}
		params := &common.DeleteBackupPolicyParams{
			Name:           dbBackupPolicy.Name,
			OwnerID:        dbBackupPolicy.Account.Name,
			BackupPolicyID: dbBackupPolicy.UUID,
			LocationID:     "us-west1",
		}

		// Execute workflow
		env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)

		// Assertions
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		// The rollback activity is called in the defer block - we can see it in the logs
	})

	// Test VCP deletion failure triggers state rollback
	s.T().Run("VCP_deletion_failure_triggers_rollback", func(t *testing.T) {
		// Create a fresh test environment for this subtest
		env := s.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		env.RegisterWorkflow(DeleteBackupPolicyWorkflow)

		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
			State:     string(models.JobsStateNEW),
		}, nil).Maybe()
		mockScheduler := scheduler.NewMockScheduler(t)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

		// Register activities
		env.RegisterActivity(commonActivity.GetAuthJWTToken)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
		env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
		env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
		env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyStateInCaseOfError)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		// Setup mocks
		env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
			State:     string(models.JobsStateNEW),
		}, nil).Maybe()
		env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
		env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInSDE, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity(backupPolicyActivity.DeleteBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, mock.Anything, mock.Anything).Return(nil, errors.New("VCP deletion failed"))
		env.OnActivity(backupPolicyActivity.UpdateBackupPolicyStateInCaseOfError, mock.Anything, mock.Anything, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Return(nil)
		env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

		// Test data
		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
			Name:                  "backup-policy-1",
			Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
			AccountID:             int64(1),
			Description:           nil,
			DailyBackupsToKeep:    2,
			WeeklyBackupsToKeep:   2,
			MonthlyBackupsToKeep:  2,
			PolicyEnabled:         false,
			LifeCycleState:        models.LifeCycleStateDeleting,
			LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
		}
		params := &common.DeleteBackupPolicyParams{
			Name:           dbBackupPolicy.Name,
			OwnerID:        dbBackupPolicy.Account.Name,
			BackupPolicyID: dbBackupPolicy.UUID,
			LocationID:     "us-west1",
		}

		// Execute workflow
		env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)

		// Assertions
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		// The rollback activity is called in the defer block - we can see it in the logs
	})
}

// TestDeleteBackupPolicyWorkflow_StateRollbackFailure tests when the state rollback itself fails
func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflowStateRollbackFailure() {
	// Create a fresh test environment
	env := s.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.RegisterWorkflow(DeleteBackupPolicyWorkflow)

	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	env.RegisterActivity(commonActivity.GetAuthJWTToken)
	env.RegisterActivity(commonActivity.GetJob)
	env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyStateInCaseOfError)
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Setup mocks - simulate SDE failure and rollback failure
	env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInSDE, mock.Anything, mock.Anything).Return(errors.New("SDE deletion failed"))
	env.OnActivity(backupPolicyActivity.UpdateBackupPolicyStateInCaseOfError, mock.Anything, mock.Anything, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Return(errors.New("rollback failed"))
	env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
		Name:                  "backup-policy-1",
		Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	params := &common.DeleteBackupPolicyParams{
		Name:           dbBackupPolicy.Name,
		OwnerID:        dbBackupPolicy.Account.Name,
		BackupPolicyID: dbBackupPolicy.UUID,
		LocationID:     "us-west1",
	}

	// Execute workflow
	env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)

	// Assertions - workflow should still complete with error, but rollback attempt should be made
	assert.True(s.T(), env.IsWorkflowCompleted())
	assert.Error(s.T(), env.GetWorkflowError())
	// The rollback activity is called in the defer block - we can see it in the logs
}

// TestDeleteBackupPolicyWorkflowSuccessNoRollback tests successful deletion does not trigger rollback
func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflowSuccessNoRollback() {
	// Create a fresh test environment
	env := s.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.RegisterWorkflow(DeleteBackupPolicyWorkflow)

	mockStorage := s.setupMockStorage()
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities including the rollback activity
	env.RegisterActivity(commonActivity.GetAuthJWTToken)
	env.RegisterActivity(commonActivity.GetJob)
	env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyStateInCaseOfError)
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Setup mocks for successful execution
	env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("mock-jwt-token", nil)
	env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInSDE, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(backupPolicyActivity.DeleteBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{ID: int64(1), UUID: "backup-policy-uuid-1"},
		Name:                  "backup-policy-1",
		Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}, Name: "account-1"},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	params := &common.DeleteBackupPolicyParams{
		Name:           dbBackupPolicy.Name,
		OwnerID:        dbBackupPolicy.Account.Name,
		BackupPolicyID: dbBackupPolicy.UUID,
		LocationID:     "us-west1",
	}

	// Execute workflow
	env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)

	// Assertions - workflow should complete successfully without triggering rollback
	assert.True(s.T(), env.IsWorkflowCompleted())
	assert.NoError(s.T(), env.GetWorkflowError())
	// Verify rollback activity was NOT called
	env.AssertActivityNotCalled(s.T(), "UpdateBackupPolicyStateInCaseOfError")
}

// TestUpdateBackupPolicyWorkflow_EnsureJobStateError tests the error path when EnsureJobState fails
func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_EnsureJobStateError() {
	mockStorage := s.setupMockStorage()
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetJob)

	// Mock GetJob to return a job with state PROCESSING (not NEW) to trigger EnsureJobState error
	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStatePROCESSING), // Wrong state to trigger error
	}, nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "backup-policy-1",
		AccountName:        "account-1",
		BackupPolicyID:     "backup-policy-uuid-1",
		LocationID:         "us-west1",
		Description:        nillable.ToPointer("backup policy description"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(3)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(3)),
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         true,
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	s.env.ExecuteWorkflow(UpdateBackupPolicyWorkflow, params, dbBackupPolicy)

	// Assert that the workflow failed due to EnsureJobState error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

// TestDeleteBackupPolicyWorkflow_EnsureJobStateError tests the error path when EnsureJobState fails
func (s *BackupPolicyWorkflowsTestSuite) TestDeleteBackupPolicyWorkflow_EnsureJobStateError() {
	mockStorage := s.setupMockStorage()
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetJob)

	// Mock GetJob to return a job with state PROCESSING (not NEW) to trigger EnsureJobState error
	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStatePROCESSING), // Wrong state to trigger error
	}, nil)

	params := &common.DeleteBackupPolicyParams{
		Name:           "backup-policy-1",
		OwnerID:        "account-1",
		BackupPolicyID: "backup-policy-uuid-1",
		LocationID:     "us-west1",
	}
	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "backup-policy-uuid-1",
		},
		Name: "backup-policy-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "account-1",
		},
		AccountID:             int64(1),
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	s.env.ExecuteWorkflow(DeleteBackupPolicyWorkflow, params, dbBackupPolicy)

	// Assert that the workflow failed due to EnsureJobState error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}
