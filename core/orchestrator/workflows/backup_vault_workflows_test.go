package workflows

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultUpdateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(UpdateBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_UpdateSDEError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultUpdateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, errors.New("failed to update backup vault in SDE"))
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultStateInCaseOfError, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(UpdateBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_UpdateVCPError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultUpdateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to update backup vault in VCP"))
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultStateInCaseOfError, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(UpdateBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_SignedTokenError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultUpdateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get signed JWT token"))
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultStateInCaseOfError, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(UpdateBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(DeleteBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_DeleteSDEError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP)
	s.env.RegisterActivity(backupvaultDeleteActivity.UpdateBackupVaultStateInCaseOfError)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, errors.New("failed to update backup vault in SDE"))
	s.env.OnActivity(backupvaultDeleteActivity.UpdateBackupVaultStateInCaseOfError, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(DeleteBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_DeleteVCPError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP, mock.Anything, mock.Anything).Return(nil, errors.New("failed to update backup vault in VCP"))
	s.env.OnActivity(backupvaultDeleteActivity.UpdateBackupVaultStateInCaseOfError, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(DeleteBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_SignedTokenError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get signed JWT token"))
	s.env.OnActivity(backupvaultDeleteActivity.UpdateBackupVaultStateInCaseOfError, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(DeleteBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_DeleteBucketsError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel:   datamodel.BaseModel{ID: int64(1)},
		Name:        "backup_vault_test",
		Description: &des,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets)
	s.env.RegisterActivity(backupvaultDeleteActivity.UpdateBackupVaultStateInCaseOfError)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets, mock.Anything, mock.Anything).Return(errors.New("failed to delete backup vault buckets"))
	s.env.OnActivity(backupvaultDeleteActivity.UpdateBackupVaultStateInCaseOfError, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(DeleteBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}
