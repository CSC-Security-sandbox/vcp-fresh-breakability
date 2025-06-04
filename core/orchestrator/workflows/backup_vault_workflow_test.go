package workflows

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func (s *UnitTestSuite) Test_CreateBackupVaultWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupvaultCreateActivity := activities.BackupVaultActivity{SE: mockStorage}
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

	paramz := gcpgenserver.V1betaCreateBackupVaultParams{
		LocationId:     "us-central1",
		ProjectNumber:  "123456789",
		XCorrelationID: gcpgenserver.NewOptString("correlation"), // Ensure valid initialization
	}
	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(backupvaultCreateActivity.CreateBackupVaultInSDE)
	s.env.RegisterActivity(backupvaultCreateActivity.CheckBackupVaultExistsInVCP)
	s.env.RegisterActivity(backupvaultCreateActivity.CreateBackupVaultInVCP)

	s.env.OnActivity(backupvaultCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultCreateActivity.CreateBackupVaultInSDE, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(backupvaultCreateActivity.CreateBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	// Execute workflow
	s.env.ExecuteWorkflow(CreateBackupVault, &common.BackupVaultParams{}, bv, paramz)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}
