package workflows

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
)

func (s *UnitTestSuite) Test_UpdateBackupVaultWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultCreateActivity := activities.BackupVaultActivity{SE: mockStorage}
	des := "description of backup vault"
	mrd := int64(30)
	getSignedJwtToken = func(projectNumber string) (string, error) {
		return "test-jwt-token", nil
	}
	defer func() {
		getSignedJwtToken = auth.GetSignedJwtToken
	}()
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
	s.env.RegisterActivity(backupvaultCreateActivity.UpdateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultCreateActivity.UpdateBackupVaultInVCP)

	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(backupvaultCreateActivity.UpdateBackupVaultInSDE, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultCreateActivity.UpdateBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(UpdateBackupVaultWorkflow, &common.BackupVaultParams{}, bv)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}
