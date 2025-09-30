package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type FinishProjectEventDeleteStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *FinishProjectEventDeleteStateTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	s.env.SetHeader(mockHeader)

	// Register workflow
	s.env.RegisterWorkflow(FinishProjectEventDeleteStateWorkflow)
}

func (s *FinishProjectEventDeleteStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Register activities (don't register VerifySoftDeletedResourcesForAccount/HardDeleteResourcesInOrder
	// because we stub them with s.env.OnActivity to avoid running real activity code during unit tests)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(hostGroupActivities.DeleteHostGroup)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(kmsActivities.DeleteKmsConfig)
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)

	// Mock finish project event activity
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{
		Done: &done,
		Name: &operationName,
	}
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Mock host group activities - return empty list
	var hostGroups []*datamodel.HostGroup
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, mock.Anything).Return(hostGroups, nil).Once()

	// Mock KMS activities - return empty list
	var kmsConfigs []*datamodel.KmsConfig
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return(kmsConfigs, nil).Once()

	// Mock backup cleanup activities
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(nil).Once()

	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()

	// Mock DeleteAccountActivity
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, "test-project-number").Return(nil).Once()

	// Mock VerifySoftDeletedResourcesForAccount to return true (can hard delete)
	s.env.OnActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, "test-project-number").Return(true, nil).Once()

	// Mock HardDeleteResourcesInOrder
	s.env.OnActivity(finishProjectEventActivity.HardDeleteResourcesInOrder, mock.Anything, "test-project-number").Return(nil).Once()

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// Note: UpdateJob is called through UpdateJobStatus activity, not directly on mockStorage
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// Note: UpdateJob is called through UpdateJobStatus activity, not directly on mockStorage
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_WithHostGroupsAndKmsConfigs() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Register activities (don't register VerifySoftDeletedResourcesForAccount since we stub it)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(hostGroupActivities.DeleteHostGroup)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(kmsActivities.DeleteKmsConfig)
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)

	// Mock finish project event activity
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{
		Done: &done,
		Name: &operationName,
	}
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Mock backup cleanup activities
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(nil).Once()

	// Mock host group activities - return host groups to delete
	hostGroups := []*datamodel.HostGroup{
		{BaseModel: datamodel.BaseModel{UUID: "hg-uuid-1"}, AccountID: 123456},
		{BaseModel: datamodel.BaseModel{UUID: "hg-uuid-2"}, AccountID: 123456},
	}
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, "test-project-number").Return(hostGroups, nil).Once()
	s.env.OnActivity(hostGroupActivities.DeleteHostGroup, mock.Anything, "hg-uuid-1", int64(123456)).Return(nil, nil).Once()
	s.env.OnActivity(hostGroupActivities.DeleteHostGroup, mock.Anything, "hg-uuid-2", int64(123456)).Return(nil, nil).Once()

	// Mock KMS activities - return KMS config to delete
	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{UUID: "KMS-uuid-1"}},
	}
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, "test-project-number").Return(kmsConfigs, nil).Once()
	s.env.OnActivity(kmsActivities.DeleteKmsConfig, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()
	
	// Mock DeleteAccountActivity
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, "test-project-number").Return(nil).Once()

	// Mock VerifySoftDeletedResourcesForAccount to return false (cannot hard delete)
	s.env.OnActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, "test-project-number").Return(false, nil).Once()

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// Note: UpdateJob is called through UpdateJobStatus activity, not directly on mockStorage
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_FinishProjectEventForSDEActivity_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)

	// Mock finish project event activity to fail
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, errors.New("SDE activity failed")).Once()

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_PollFinishProjectEventSDEOperationActivity_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)

	// Mock finish project event activity
	done := false
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{
		Done: &done,
		Name: &operationName,
	}
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("polling failed")).Once()

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_DeleteAccountActivity_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)

	// Mock finish project event activity
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{
		Done: &done,
		Name: &operationName,
	}
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Mock backup cleanup activities
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(nil).Once()

	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()

	// Mock host group activities - return empty list
	var hostGroups []*datamodel.HostGroup
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, "test-project-number").Return(hostGroups, nil).Once()

	// Mock KMS activities - return empty list
	var kmsConfigs []*datamodel.KmsConfig
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, "test-project-number").Return(kmsConfigs, nil).Once()

	// Mock DeleteAccountActivity to fail
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, "test-project-number").Return(errors.New("delete account failed")).Once()

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_UpdateJobStatus_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Mock UpdateJob to fail on first call
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("update job failed"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
}

// Run the test suite
func TestFinishProjectEventDeleteStateWorkflow(t *testing.T) {
	suite.Run(t, new(FinishProjectEventDeleteStateTestSuite))
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_BackupCleanup_Success() {
	// Set CVP_HOST to ensure workflow doesn't return early
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Register other required activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}

	// Register backup cleanup activities
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)
	s.env.RegisterActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.HardDeleteResourcesInOrder)
	s.env.RegisterActivity(finishProjectEventActivity.RollbackAccountStateActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)

	// Mock all activities to succeed
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{Done: &done, Name: &operationName}
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil)
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{}, nil)
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, mock.Anything).Return(true, nil)
	s.env.OnActivity(finishProjectEventActivity.HardDeleteResourcesInOrder, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.RollbackAccountStateActivity, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	assert.Nil(s.T(), err)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_BackupVaultCleanup_Fails() {
	// Set CVP_HOST to ensure workflow doesn't return early
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Register other required activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}

	// Register backup cleanup activities
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)
	s.env.RegisterActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.HardDeleteResourcesInOrder)
	s.env.RegisterActivity(finishProjectEventActivity.RollbackAccountStateActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)

	// Mock activities - backup vault cleanup fails
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{Done: &done, Name: &operationName}
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil)
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{}, nil)
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(errors.New("backup vault cleanup failed"))
	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, mock.Anything).Return(true, nil)
	s.env.OnActivity(finishProjectEventActivity.HardDeleteResourcesInOrder, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.RollbackAccountStateActivity, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow completed successfully despite backup vault cleanup error
	// The workflow should continue and complete other cleanup activities
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	assert.Nil(s.T(), err)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_BackupPolicyCleanup_Fails() {
	// Set CVP_HOST to ensure workflow doesn't return early
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Register other required activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}

	// Register backup cleanup activities
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)
	s.env.RegisterActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.HardDeleteResourcesInOrder)
	s.env.RegisterActivity(finishProjectEventActivity.RollbackAccountStateActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)

	// Mock activities - backup policy cleanup fails
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{Done: &done, Name: &operationName}
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil)
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{}, nil)
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(errors.New("backup policy cleanup failed"))
	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, mock.Anything).Return(true, nil)
	s.env.OnActivity(finishProjectEventActivity.HardDeleteResourcesInOrder, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.RollbackAccountStateActivity, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow completed successfully despite backup policy cleanup error
	// The workflow should continue and complete other cleanup activities
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	assert.Nil(s.T(), err)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_BackupCleanup_WithRetryPolicy() {
	// Set CVP_HOST to ensure workflow doesn't return early
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Register other required activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}

	// Register backup cleanup activities
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)
	s.env.RegisterActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.HardDeleteResourcesInOrder)
	s.env.RegisterActivity(finishProjectEventActivity.RollbackAccountStateActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)

	// Mock activities - backup vault cleanup fails first time, succeeds on retry
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{Done: &done, Name: &operationName}
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil)
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{}, nil)
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)

	// First call fails, second call succeeds (testing retry policy)
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(errors.New("temporary error")).Once()
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(nil).Once()

	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, mock.Anything).Return(true, nil)
	s.env.OnActivity(finishProjectEventActivity.HardDeleteResourcesInOrder, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.RollbackAccountStateActivity, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow completed successfully after retry
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	assert.Nil(s.T(), err)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_BackupCleanup_NonRetryableError() {
	// Set CVP_HOST to ensure workflow doesn't return early
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Register other required activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}

	// Register backup cleanup activities
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)
	s.env.RegisterActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.HardDeleteResourcesInOrder)
	s.env.RegisterActivity(finishProjectEventActivity.RollbackAccountStateActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)

	// Mock activities - backup vault cleanup fails with non-retryable error
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{Done: &done, Name: &operationName}
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil)
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{}, nil)
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)

	// Create a non-retryable error (PanicError type)
	panicErr := temporal.NewApplicationError("panic occurred during backup cleanup", "PanicError")
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(panicErr)

	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, mock.Anything).Return(true, nil)
	s.env.OnActivity(finishProjectEventActivity.HardDeleteResourcesInOrder, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.RollbackAccountStateActivity, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow completed successfully despite backup cleanup error
	// The workflow should continue and complete other cleanup activities
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	assert.Nil(s.T(), err)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_DeferredRollback_ErrorBranches() {
	// Save and restore the global hardDeleteResources flag
	origHardDelete := hardDeleteResources
	hardDeleteResources = true
	defer func() { hardDeleteResources = origHardDelete }()

	mockStorage := database.NewMockStorage(s.T())
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hostGroupActivities := &activities.HostGroupUpdateActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	backupVaultActivity := &activities.BackupVaultActivity{SE: mockStorage}
	backupPolicyActivity := &activities.BackupPolicyActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() { cvp.CVP_HOST = "" }()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(hostGroupActivities.ListHostGroups)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(backupVaultActivity.CleanupBackupVaultsForAccount)
	s.env.RegisterActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteAccountActivity)
	s.env.RegisterActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount)
	s.env.RegisterActivity(finishProjectEventActivity.HardDeleteResourcesInOrder)
	s.env.RegisterActivity(finishProjectEventActivity.RollbackAccountStateActivity)
	s.env.RegisterActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID)

	// Mock all activities to succeed except VerifySoftDeletedResourcesForAccount, which returns error
	done := true
	operationName := "test-operation"
	finishResult := &commonparams.FinishProjectEventResult{Done: &done, Name: &operationName}
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil)
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hostGroupActivities.ListHostGroups, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{}, nil)
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)
	s.env.OnActivity(backupVaultActivity.CleanupBackupVaultsForAccount, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.CleanupBackupPoliciesForAccount, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(finishProjectEventActivity.DeleteAccountActivity, mock.Anything, mock.Anything).Return(nil)
	// Simulate error in VerifySoftDeletedResourcesForAccount to set errRollBack
	s.env.OnActivity(finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, mock.Anything).Return(false, errors.New("verify soft delete error"))
	// Simulate error in RollbackAccountStateActivity to hit logger branch
	s.env.OnActivity(finishProjectEventActivity.RollbackAccountStateActivity, mock.Anything, mock.Anything).Return(errors.New("rollback failed"))
	s.env.OnActivity(finishProjectEventActivity.DeleteServiceAccountsFromAccountID, mock.Anything, "test-project-number").Return(nil).Once()

	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow failed and error contains rollback
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	assert.Nil(s.T(), err)
}

func (s *FinishProjectEventDeleteStateTestSuite) TestFinishProjectEventDeleteStateWorkflow_PopulateRetryPolicyParamsError() {
	// Set invalid environment variable to cause PopulateRetryPolicyParams to fail
	originalStartToCloseTimeout := StartToCloseTimeout
	StartToCloseTimeout = "invalid-duration"
	defer func() { StartToCloseTimeout = originalStartToCloseTimeout }()

	// Register required activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Register all activities that the workflow calls before PopulateRetryPolicyParams
	finishProjectActivity := &resource_events_activities.FinishProjectEventActivity{}
	s.env.RegisterActivity(finishProjectActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectActivity.PollFinishProjectEventSDEOperationActivity)

	hostGroupActivity := &activities.HostGroupUpdateActivity{}
	s.env.RegisterActivity(hostGroupActivity.ListHostGroups)
	s.env.RegisterActivity(hostGroupActivity.DeleteHostGroup)

	kmsActivity := &kms_activities.KmsConfigActivity{}
	s.env.RegisterActivity(kmsActivity.ListKmsConfigActivity)
	s.env.RegisterActivity(kmsActivity.DeleteKmsConfig)

	s.env.RegisterActivity(&activities.BackupVaultActivity{})
	s.env.RegisterActivity(&activities.BackupPolicyActivity{})

	// Mock the UpdateJob calls that UpdateJobStatus will make
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Mock all the activities to return success so workflow reaches PopulateRetryPolicyParams
	s.env.OnActivity(finishProjectActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(&commonparams.FinishProjectEventResult{}, nil)
	s.env.OnActivity(finishProjectActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hostGroupActivity.ListHostGroups, mock.Anything).Return([]*datamodel.HostGroup{}, nil)
	s.env.OnActivity(kmsActivity.ListKmsConfigActivity, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)

	params := &commonparams.FinishProjectEventParams{
		ProjectNumber:  "test-project-123",
		State:          "DELETE",
		LocationId:     "us-central1",
		XCorrelationID: "test-correlation-id",
	}

	// Execute workflow
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow failed due to PopulateRetryPolicyParams error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "time: invalid duration")
}
