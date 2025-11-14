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

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_CrossRegion_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "cross-region backup vault"
	mrd := int64(30)
	backupRegion := "us-west1"
	sourceRegion := "us-east1"
	bv := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region",
		Description:      &des,
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &backupRegion,
		SourceRegionName: &sourceRegion,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	remoteBackupVault := &datamodel.BackupVault{
		BaseModel:       datamodel.BaseModel{ID: int64(2)},
		Name:            "backup_vault_remote",
		BackupVaultType: "CROSS_REGION",
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP)
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteRemoteBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP, mock.Anything, mock.Anything).Return(bv, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteRemoteBackupVaultInVCP, mock.Anything, mock.Anything).Return(remoteBackupVault, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(DeleteBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_CrossRegion_DeleteRemoteError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "cross-region backup vault"
	mrd := int64(30)
	backupRegion := "us-west1"
	sourceRegion := "us-east1"
	bv := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region",
		Description:      &des,
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &backupRegion,
		SourceRegionName: &sourceRegion,
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
	s.env.RegisterActivity(backupvaultDeleteActivity.DeleteRemoteBackupVaultInVCP)
	s.env.RegisterActivity(backupvaultDeleteActivity.UpdateBackupVaultStateInCaseOfError)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP, mock.Anything, mock.Anything).Return(bv, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteRemoteBackupVaultInVCP, mock.Anything, mock.Anything).Return(nil, errors.New("failed to delete remote backup vault"))
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

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_CrossRegion_EmptyBackupRegionName() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "cross-region backup vault with empty region"
	mrd := int64(30)
	emptyRegion := ""
	bv := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region",
		Description:      &des,
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &emptyRegion, // Empty string, should skip remote deletion
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
	// NOTE: DeleteRemoteBackupVaultInVCP should NOT be called due to empty BackupRegionName

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP, mock.Anything, mock.Anything).Return(bv, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(DeleteBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully (remote deletion skipped)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_DeleteBackupVaultWorkflow_NonCrossRegion_SkipRemoteDeletion() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultDeleteActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "non-cross-region backup vault"
	mrd := int64(30)
	backupRegion := "us-west1"
	bv := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_in_region",
		Description:      &des,
		BackupVaultType:  "IN_REGION", // Not CROSS_REGION
		BackupRegionName: &backupRegion,
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
	// NOTE: DeleteRemoteBackupVaultInVCP should NOT be called for non-cross-region type

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultBuckets, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupvaultDeleteActivity.DeleteBackupVaultInVCP, mock.Anything, mock.Anything).Return(bv, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(DeleteBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully (remote deletion skipped for non-cross-region)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_CrossRegion_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultUpdateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "cross-region backup vault"
	mrd := int64(30)
	backupRegion := "us-west1"
	sourceRegion := "us-east1"
	bv := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region",
		Description:      &des,
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &backupRegion,
		SourceRegionName: &sourceRegion,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	sdeBackupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region_sde",
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &backupRegion,
	}

	dbBackupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region_vcp",
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &backupRegion,
	}

	remoteBackupVault := &datamodel.BackupVault{
		BaseModel:       datamodel.BaseModel{ID: int64(2)},
		Name:            "backup_vault_remote",
		BackupVaultType: "CROSS_REGION",
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateRemoteBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE, mock.Anything, mock.Anything).Return(sdeBackupVault, nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything).Return(dbBackupVault, nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateRemoteBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything).Return(remoteBackupVault, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_CrossRegion_UpdateRemoteError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultUpdateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "cross-region backup vault"
	mrd := int64(30)
	backupRegion := "us-west1"
	sourceRegion := "us-east1"
	bv := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region",
		Description:      &des,
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &backupRegion,
		SourceRegionName: &sourceRegion,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	sdeBackupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region_sde",
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &backupRegion,
	}

	dbBackupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region_vcp",
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &backupRegion,
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateRemoteBackupVaultInVCP)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultStateInCaseOfError)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE, mock.Anything, mock.Anything).Return(sdeBackupVault, nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything).Return(dbBackupVault, nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateRemoteBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to update remote backup vault"))
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

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_CrossRegion_EmptyBackupRegionName() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultUpdateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "cross-region backup vault with empty region"
	mrd := int64(30)
	emptyRegion := ""
	bv := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region",
		Description:      &des,
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &emptyRegion, // Empty string, should skip remote update
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	sdeBackupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region_sde",
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &emptyRegion,
	}

	dbBackupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_cross_region_vcp",
		BackupVaultType:  "CROSS_REGION",
		BackupRegionName: &emptyRegion,
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP)
	// NOTE: UpdateRemoteBackupVaultInVCP should NOT be called due to empty BackupRegionName

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE, mock.Anything, mock.Anything).Return(sdeBackupVault, nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything).Return(dbBackupVault, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully (remote update skipped)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_NonCrossRegion_SkipRemoteUpdate() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultUpdateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "non-cross-region backup vault"
	mrd := int64(30)
	backupRegion := "us-west1"
	bv := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_in_region",
		Description:      &des,
		BackupVaultType:  "IN_REGION", // Not CROSS_REGION
		BackupRegionName: &backupRegion,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                true,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 true,
			BackupMinimumEnforcedRetentionDuration: &mrd,
		},
	}

	sdeBackupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_in_region_sde",
		BackupVaultType:  "IN_REGION",
		BackupRegionName: &backupRegion,
	}

	dbBackupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{ID: int64(1)},
		Name:             "backup_vault_in_region_vcp",
		BackupVaultType:  "IN_REGION",
		BackupRegionName: &backupRegion,
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP)
	// NOTE: UpdateRemoteBackupVaultInVCP should NOT be called for non-cross-region type

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInSDE, mock.Anything, mock.Anything).Return(sdeBackupVault, nil)
	s.env.OnActivity(backupvaultUpdateActivity.UpdateBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything).Return(dbBackupVault, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully (remote update skipped for non-cross-region)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}
