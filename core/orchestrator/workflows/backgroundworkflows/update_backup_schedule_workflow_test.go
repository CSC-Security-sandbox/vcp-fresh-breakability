package backgroundworkflows

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	databaseUtils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type UpdateBackupScheduleTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *UpdateBackupScheduleTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	s.env.RegisterWorkflow(UpdateBackupScheduleWorkflow)
}

func (s *UpdateBackupScheduleTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func TestUpdateBackupScheduleTestSuite(t *testing.T) {
	suite.Run(t, new(UpdateBackupScheduleTestSuite))
}

// Helper function to register all activities needed for UpdateBackupScheduleWorkflow tests
func (s *UpdateBackupScheduleTestSuite) registerUpdateBackupScheduleActivities(updateBackupScheduleActivity *backgroundactivities.UpdateBackupScheduleActivity) {
	s.env.RegisterActivity(updateBackupScheduleActivity.GetBackupPolicies)
	s.env.RegisterActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue)
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_Success_NoPolicies() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Mock GetBackupPolicies to return empty slice
	pagination := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination).
		Return([]*datamodel.BackupPolicy{}, nil)

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_Success_SingleBatch() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Create mock backup policies
	backupPolicies := []*datamodel.BackupPolicy{
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-2"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-3"}},
	}

	// Mock GetBackupPolicies to return policies (less than batch size)
	pagination1 := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination1).
		Return(backupPolicies, nil)

	// Mock UpdateBackupScheduleTaskQueue for each policy
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-1").
		Return(nil)
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-2").
		Return(nil)
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-3").
		Return(nil)

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_Success_MultipleBatches() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Create first batch (exactly batch size)
	firstBatch := make([]*datamodel.BackupPolicy, backgroundactivities.DefaultBackupPolicyBatchSize)
	for i := 0; i < backgroundactivities.DefaultBackupPolicyBatchSize; i++ {
		firstBatch[i] = &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("policy-uuid-%d", i+1)},
		}
	}

	// Create second batch (less than batch size, indicating end)
	secondBatch := []*datamodel.BackupPolicy{
		{BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("policy-uuid-%d", backgroundactivities.DefaultBackupPolicyBatchSize+1)}},
		{BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("policy-uuid-%d", backgroundactivities.DefaultBackupPolicyBatchSize+2)}},
	}

	// Mock GetBackupPolicies for first batch
	pagination1 := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination1).
		Return(firstBatch, nil)

	// Mock GetBackupPolicies for second batch
	pagination2 := &databaseUtils.Pagination{
		Offset: backgroundactivities.DefaultBackupPolicyBatchSize,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination2).
		Return(secondBatch, nil)

	// Mock UpdateBackupScheduleTaskQueue for all policies in first batch
	for i := 0; i < backgroundactivities.DefaultBackupPolicyBatchSize; i++ {
		s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, fmt.Sprintf("policy-uuid-%d", i+1)).
			Return(nil)
	}

	// Mock UpdateBackupScheduleTaskQueue for policies in second batch
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, fmt.Sprintf("policy-uuid-%d", backgroundactivities.DefaultBackupPolicyBatchSize+1)).
		Return(nil)
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, fmt.Sprintf("policy-uuid-%d", backgroundactivities.DefaultBackupPolicyBatchSize+2)).
		Return(nil)

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_Success_EmptyBatchIndicatesCompletion() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	originalDefaultBackupPolicyBatchSize := backgroundactivities.DefaultBackupPolicyBatchSize
	defer func() {
		backgroundactivities.DefaultBackupPolicyBatchSize = originalDefaultBackupPolicyBatchSize
	}()
	backgroundactivities.DefaultBackupPolicyBatchSize = 2

	// Create first batch
	firstBatch := []*datamodel.BackupPolicy{
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-2"}},
	}

	// Mock GetBackupPolicies for first batch
	pagination1 := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination1).
		Return(firstBatch, nil)

	// Mock GetBackupPolicies for second batch (empty, indicating completion)
	pagination2 := &databaseUtils.Pagination{
		Offset: backgroundactivities.DefaultBackupPolicyBatchSize,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination2).
		Return([]*datamodel.BackupPolicy{}, nil)

	// Mock UpdateBackupScheduleTaskQueue for policies in first batch
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-1").
		Return(nil)
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-2").
		Return(nil)

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_GetBackupPoliciesFailure() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Mock GetBackupPolicies to return error
	pagination := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination).
		Return(nil, errors.New("failed to fetch backup policies"))

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Contains(s.T(), activityError.Unwrap().Error(), "failed to fetch backup policies")
	} else {
		assert.Fail(s.T(), fmt.Sprintf("Expected ActivityError but got: %v", s.env.GetWorkflowError()))
	}
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_UpdateTaskQueueFailure_ContinuesProcessing() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Create backup policies
	backupPolicies := []*datamodel.BackupPolicy{
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-2"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-3"}},
	}

	// Mock GetBackupPolicies
	pagination := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination).
		Return(backupPolicies, nil)

	// Mock UpdateBackupScheduleTaskQueue - first fails, others succeed
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-1").
		Return(errors.New("failed to update task queue for policy-uuid-1"))
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-2").
		Return(nil)
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-3").
		Return(nil)

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	// Workflow should complete successfully even if some updates fail
	// because the workflow continues processing other policies
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_UpdateTaskQueueFailure_AllFail() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Create backup policies
	backupPolicies := []*datamodel.BackupPolicy{
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-2"}},
	}

	// Mock GetBackupPolicies
	pagination := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination).
		Return(backupPolicies, nil)

	// Mock UpdateBackupScheduleTaskQueue - all fail
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-1").
		Return(errors.New("failed to update task queue for policy-uuid-1"))
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-2").
		Return(errors.New("failed to update task queue for policy-uuid-2"))

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	// Workflow should complete successfully even if all updates fail
	// because failures are logged but don't stop the workflow
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_Success_ExactlyBatchSize() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Create exactly batch size policies
	backupPolicies := make([]*datamodel.BackupPolicy, backgroundactivities.DefaultBackupPolicyBatchSize)
	for i := 0; i < backgroundactivities.DefaultBackupPolicyBatchSize; i++ {
		backupPolicies[i] = &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("policy-uuid-%d", i+1)},
		}
	}

	// Mock GetBackupPolicies
	pagination1 := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination1).
		Return(backupPolicies, nil)

	// Mock GetBackupPolicies for second batch (empty, should not be called but workflow should handle it)
	pagination2 := &databaseUtils.Pagination{
		Offset: backgroundactivities.DefaultBackupPolicyBatchSize,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination2).
		Return([]*datamodel.BackupPolicy{}, nil)

	// Mock UpdateBackupScheduleTaskQueue for all policies
	for i := 0; i < backgroundactivities.DefaultBackupPolicyBatchSize; i++ {
		s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, fmt.Sprintf("policy-uuid-%d", i+1)).
			Return(nil)
	}

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_GetBackupPoliciesFailure_SecondBatch() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Create first batch
	firstBatch := make([]*datamodel.BackupPolicy, backgroundactivities.DefaultBackupPolicyBatchSize)
	for i := 0; i < backgroundactivities.DefaultBackupPolicyBatchSize; i++ {
		firstBatch[i] = &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("policy-uuid-%d", i+1)},
		}
	}

	// Mock GetBackupPolicies for first batch
	pagination1 := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination1).
		Return(firstBatch, nil)

	// Mock GetBackupPolicies for second batch to fail
	pagination2 := &databaseUtils.Pagination{
		Offset: backgroundactivities.DefaultBackupPolicyBatchSize,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination2).
		Return(nil, errors.New("failed to fetch backup policies for second batch"))

	// Mock UpdateBackupScheduleTaskQueue for all policies in first batch
	for i := 0; i < backgroundactivities.DefaultBackupPolicyBatchSize; i++ {
		s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, fmt.Sprintf("policy-uuid-%d", i+1)).
			Return(nil)
	}

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Contains(s.T(), activityError.Unwrap().Error(), "failed to fetch backup policies for second batch")
	} else {
		assert.Fail(s.T(), fmt.Sprintf("Expected ActivityError but got: %v", s.env.GetWorkflowError()))
	}
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_LargeNumberOfPolicies() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Simulate processing 450 policies (2 full batches + 50 in third batch)
	numPolicies := 450
	numBatches := (numPolicies + backgroundactivities.DefaultBackupPolicyBatchSize - 1) / backgroundactivities.DefaultBackupPolicyBatchSize

	// Mock GetBackupPolicies for each batch
	for batch := 0; batch < numBatches; batch++ {
		offset := batch * backgroundactivities.DefaultBackupPolicyBatchSize
		remaining := numPolicies - offset
		batchSize := backgroundactivities.DefaultBackupPolicyBatchSize
		if remaining < backgroundactivities.DefaultBackupPolicyBatchSize {
			batchSize = remaining
		}

		batchPolicies := make([]*datamodel.BackupPolicy, batchSize)
		for i := 0; i < batchSize; i++ {
			batchPolicies[i] = &datamodel.BackupPolicy{
				BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("policy-uuid-%d", offset+i+1)},
			}
		}

		pagination := &databaseUtils.Pagination{
			Offset: offset,
			Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
		}
		s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination).
			Return(batchPolicies, nil)

		// Mock UpdateBackupScheduleTaskQueue for each policy in the batch
		for i := 0; i < batchSize; i++ {
			policyUUID := fmt.Sprintf("policy-uuid-%d", offset+i+1)
			s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, policyUUID).
				Return(nil)
		}
	}

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}

func (s *UpdateBackupScheduleTestSuite) TestUpdateBackupScheduleWorkflow_MixedSuccessAndFailure() {
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	s.registerUpdateBackupScheduleActivities(updateBackupScheduleActivity)

	// Create backup policies
	backupPolicies := []*datamodel.BackupPolicy{
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-2"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-3"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-4"}},
		{BaseModel: datamodel.BaseModel{UUID: "policy-uuid-5"}},
	}

	// Mock GetBackupPolicies
	pagination := &databaseUtils.Pagination{
		Offset: 0,
		Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
	}
	s.env.OnActivity(updateBackupScheduleActivity.GetBackupPolicies, mock.Anything, pagination).
		Return(backupPolicies, nil)

	// Mock UpdateBackupScheduleTaskQueue - mix of success and failure
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-1").
		Return(nil) // Success
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-2").
		Return(errors.New("failed to update task queue for policy-uuid-2")) // Failure
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-3").
		Return(nil) // Success
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-4").
		Return(errors.New("failed to update task queue for policy-uuid-4")) // Failure
	s.env.OnActivity(updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, mock.Anything, "policy-uuid-5").
		Return(nil) // Success

	s.env.ExecuteWorkflow(UpdateBackupScheduleWorkflow)

	// Workflow should complete successfully even with mixed results
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	s.env.AssertExpectations(s.T())
}
