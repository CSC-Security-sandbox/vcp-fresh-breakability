package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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

func (s *BackupPolicyWorkflowsTestSuite) TestEnableBackupPolicyWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInVCP)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.UnpauseBackupPolicySchedule)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInVCP)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.UnpauseBackupPolicySchedule)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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

func (s *BackupPolicyWorkflowsTestSuite) TestUpdateBackupPolicyWorkflow_UnpauseBackupPolicyScheduleFailure() {
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.UnpauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.UpdateBackupPolicyInVCP)
	s.env.RegisterActivity(backupPolicyActivity.UnpauseBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP)
	s.env.RegisterActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
	mockStorage := database.NewMockStorage(s.T())
	mockScheduler := scheduler.NewMockScheduler(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Register activities
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInSDE)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicySchedule)
	s.env.RegisterActivity(backupPolicyActivity.DeleteBackupPolicyInVCP)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

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
