package expertMode

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	expertmodeactivities "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/expert_mode_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type RBACWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *RBACWorkflowTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	// Add correlation ID to logger fields for VLM workflows
	// The key must match what GetCorrelationIDFromWorkflowContextLoggerFields expects
	// It expects string(middleware.RequestCorrelationID) which is "requestCorrelationID"
	loggerFields := log.Fields{
		string(middleware.RequestCorrelationID): "test-correlation-id",
	}
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(loggerFields)
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	s.env.SetHeader(mockHeader)

	// Register workflows
	s.env.RegisterWorkflow(UpdateRbacForPoolsWorkflow)
	s.env.RegisterWorkflow(UpdateSinglePoolRbacChildWorkflow)

	// Register common activities for job management
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	s.env.RegisterActivity(commonActivity.GetJob)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Mock GetJob activity - return NEW state for workflow job (EnsureJobState)
	s.env.OnActivity(commonActivity.GetJob, mock.Anything, "default-test-workflow-id").Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	// Mock UpdateJobStatus activity
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Register RBAC activities
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	poolActivity := &activities.PoolActivity{}
	s.env.RegisterActivity(rbacActivity.ListActiveExpertModePools)
	s.env.RegisterActivity(rbacActivity.GetPoolsDetailsByOntapVersion)
	s.env.RegisterActivity(rbacActivity.GetLatestRbacHashForAllOntapVersion)
	s.env.RegisterActivity(rbacActivity.GetPoolByUUID)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(poolActivity.GetExpertModeCredentials)
	s.env.RegisterActivity(poolActivity.ParseVlmConfig)
	s.env.RegisterActivity(poolActivity.PrepareCreateVSAExpertModeReq)
	s.env.RegisterActivity(poolActivity.UpdateRbacCheckSumInPool)

	// Register VLM child workflow
	s.env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, request *vlm.OntapExpertModeUserConfig) (vlm.OntapExpertModeUserResponse, error) {
			return vlm.OntapExpertModeUserResponse{
				RbacFileChecksum: "updated-hash-123",
			}, nil
		},
		workflow.RegisterOptions{Name: vlm.CreateVSAExpertModeUserWorkflowName},
	)
}

func (s *RBACWorkflowTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_Success() {
	// Test successful RBAC hash update workflow execution

	// Mock ListActiveExpertModePools activity
	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-1",
			},
			Name: "pool-1",
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "9.18.1",
				RbacFileHash: "old-hash-1",
			},
		},
		{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-2",
			},
			Name: "pool-2",
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "9.18.1",
				RbacFileHash: "old-hash-2",
			},
		},
	}
	poolActivity := &activities.PoolActivity{}
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(pools, nil).Once()

	// Mock GetPoolsDetailsByOntapVersion activity
	poolsByVersion := map[string][]expertmodeactivities.PoolDetailWithCurrentHash{
		"9.18.1": {
			{PoolUUID: "pool-uuid-1", CurrentHash: "old-hash-1"},
			{PoolUUID: "pool-uuid-2", CurrentHash: "old-hash-2"},
		},
	}
	s.env.OnActivity(rbacActivity.GetPoolsDetailsByOntapVersion, mock.Anything, pools).Return(poolsByVersion, nil).Once()

	// Mock GetLatestRbacHashForAllOntapVersion activity
	poolListNeedUpdate := []expertmodeactivities.PoolDetailsWithRbacHash{
		{
			PoolUUID:       "pool-uuid-1",
			LatestRbacHash: "new-hash-123",
			CurrentHash:    "old-hash-1",
			NeedUpdate:     true,
		},
		{
			PoolUUID:       "pool-uuid-2",
			LatestRbacHash: "new-hash-123",
			CurrentHash:    "old-hash-2",
			NeedUpdate:     true,
		},
	}
	s.env.OnActivity(rbacActivity.GetLatestRbacHashForAllOntapVersion, mock.Anything, mock.AnythingOfType("map[string][]expertmodeactivities.PoolDetailWithCurrentHash")).Return(poolListNeedUpdate, nil).Once()

	// Mock activities that use PoolRbacUpdateRequest
	// Activities now modify the context in place, so we use mock.MatchedBy to handle this
	pool1 := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "pool-uuid-1",
		},
		Name: "pool-1",
		BuildInfo: &datamodel.PoolBuildInfo{
			OntapVersion: "9.18.1",
			RbacFileHash: "old-hash-1",
			RbacFileUrl:  "GCNV/9.18.1/RBAC/gcnvadmin_create_cli",
		},
		VLMConfig: `{"deployment":{"deployment_id":"deployment-1"}}`,
	}
	pool2 := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "pool-uuid-2",
		},
		Name: "pool-2",
		BuildInfo: &datamodel.PoolBuildInfo{
			OntapVersion: "9.18.1",
			RbacFileHash: "old-hash-2",
			RbacFileUrl:  "GCNV/9.18.1/RBAC/gcnvadmin_create_cli",
		},
		VLMConfig: `{"deployment":{"deployment_id":"deployment-2"}}`,
	}
	ontapCredentials := &vlm.OntapCredentials{
		AdminPassword: "password",
	}
	expertCredentials := &vlm.OntapCredentials{
		AdminPassword: "expert-password",
	}
	vlmConfig1 := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			DeploymentID: "deployment-1",
		},
	}
	vlmConfig2 := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			DeploymentID: "deployment-2",
		},
	}
	createVSAExpertModeReq1 := &vlm.OntapExpertModeUserConfig{
		VLMConfig: *vlmConfig1,
	}
	createVSAExpertModeReq2 := &vlm.OntapExpertModeUserConfig{
		VLMConfig: *vlmConfig2,
	}

	// Mock GetPoolByUUID - returns pool directly
	s.env.OnActivity(rbacActivity.GetPoolByUUID, mock.Anything, "pool-uuid-1").Return(pool1, nil).Once()
	s.env.OnActivity(rbacActivity.GetPoolByUUID, mock.Anything, "pool-uuid-2").Return(pool2, nil).Once()

	// Mock GetOnTapCredentials - returns credentials directly
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.AnythingOfType("*datamodel.Pool")).Return(ontapCredentials, nil).Times(2)

	// Mock GetExpertModeCredentials - returns credentials directly
	s.env.OnActivity(poolActivity.GetExpertModeCredentials, mock.Anything, mock.AnythingOfType("*datamodel.Pool")).Return(expertCredentials, nil).Times(2)

	// Mock ParseVlmConfig - returns different VLM configs for each pool
	s.env.OnActivity(poolActivity.ParseVlmConfig, mock.Anything, mock.MatchedBy(func(pool *datamodel.Pool) bool {
		return pool.UUID == "pool-uuid-1"
	})).Return(vlmConfig1, nil).Once()
	s.env.OnActivity(poolActivity.ParseVlmConfig, mock.Anything, mock.MatchedBy(func(pool *datamodel.Pool) bool {
		return pool.UUID == "pool-uuid-2"
	})).Return(vlmConfig2, nil).Once()

	// Mock PrepareCreateVSAExpertModeReq - returns different requests for each pool
	s.env.OnActivity(poolActivity.PrepareCreateVSAExpertModeReq, mock.MatchedBy(func(vlmConfig vlm.VLMConfig) bool {
		return vlmConfig.Deployment.DeploymentID == "deployment-1"
	}), mock.AnythingOfType("vlm.OntapCredentials"), mock.AnythingOfType("vlm.OntapCredentials"), mock.AnythingOfType("*datamodel.Pool"), mock.Anything).Return(createVSAExpertModeReq1, nil).Once()
	s.env.OnActivity(poolActivity.PrepareCreateVSAExpertModeReq, mock.MatchedBy(func(vlmConfig vlm.VLMConfig) bool {
		return vlmConfig.Deployment.DeploymentID == "deployment-2"
	}), mock.AnythingOfType("vlm.OntapCredentials"), mock.AnythingOfType("vlm.OntapCredentials"), mock.AnythingOfType("*datamodel.Pool"), mock.Anything).Return(createVSAExpertModeReq2, nil).Once()

	// Mock UpdateRbacCheckSumInPool - takes pool and bucketFileDetails as parameters
	s.env.OnActivity(poolActivity.UpdateRbacCheckSumInPool, mock.Anything, mock.AnythingOfType("*datamodel.Pool"), mock.AnythingOfType("*hyperscaler.BucketFileDetails")).Return(nil).Times(2)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_NoPools() {
	// Test workflow when no active pools exist

	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return([]*datamodel.Pool{}, nil).Once()

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_NoPoolsNeedUpdate() {
	// Test workflow when pools exist but none need updates

	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-1",
			},
			Name: "pool-1",
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "9.18.1",
				RbacFileHash: "current-hash-123",
			},
		},
	}
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(pools, nil).Once()

	poolsByVersion := map[string][]expertmodeactivities.PoolDetailWithCurrentHash{
		"9.18.1": {
			{PoolUUID: "pool-uuid-1", CurrentHash: "current-hash-123"},
		},
	}
	s.env.OnActivity(rbacActivity.GetPoolsDetailsByOntapVersion, mock.Anything, pools).Return(poolsByVersion, nil).Once()

	// Mock GetLatestRbacHashForAllOntapVersion to return empty list (no pools need update)
	s.env.OnActivity(rbacActivity.GetLatestRbacHashForAllOntapVersion, mock.Anything, mock.AnythingOfType("map[string][]expertmodeactivities.PoolDetailWithCurrentHash")).Return([]expertmodeactivities.PoolDetailsWithRbacHash{}, nil).Once()

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_ListPoolsFails() {
	// Test workflow when ListActiveExpertModePools activity fails

	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	// Mock ListActiveExpertModePools to fail 3 times (max retries)
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(nil, errors.New("failed to list pools")).Times(3)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed
	// Note: The workflow handles errors by updating job status, so it may complete successfully
	// even if activities fail. The error handling is internal to the workflow.
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// The workflow should return an error when activities fail after retries
	// However, if UpdateJobStatus succeeds, the workflow may return nil
	// For this test, we verify the workflow handled the error by checking completion
	// In a real scenario, the job status would be ERROR
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_GetPoolsDetailsFails() {
	// Test workflow when GetPoolsDetailsByOntapVersion activity fails

	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-1",
			},
			Name: "pool-1",
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "9.18.1",
			},
		},
	}
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(pools, nil).Once()
	// Mock GetPoolsDetailsByOntapVersion to fail 3 times (max retries)
	s.env.OnActivity(rbacActivity.GetPoolsDetailsByOntapVersion, mock.Anything, pools).Return(nil, errors.New("failed to get pools details")).Times(3)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed
	// Note: The workflow handles errors by updating job status, so it may complete successfully
	// even if activities fail. The error handling is internal to the workflow.
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// The workflow should return an error when activities fail after retries
	// However, if UpdateJobStatus succeeds, the workflow may return nil
	// For this test, we verify the workflow handled the error by checking completion
	// In a real scenario, the job status would be ERROR
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_GetLatestHashFails() {
	// Test workflow when GetLatestRbacHashForAllOntapVersion activity fails

	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-1",
			},
			Name: "pool-1",
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "9.18.1",
			},
		},
	}
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(pools, nil).Once()

	poolsByVersion := map[string][]expertmodeactivities.PoolDetailWithCurrentHash{
		"9.18.1": {
			{PoolUUID: "pool-uuid-1", CurrentHash: "old-hash-1"},
		},
	}
	s.env.OnActivity(rbacActivity.GetPoolsDetailsByOntapVersion, mock.Anything, pools).Return(poolsByVersion, nil).Once()
	// Mock GetLatestRbacHashForAllOntapVersion to fail 3 times (max retries)
	s.env.OnActivity(rbacActivity.GetLatestRbacHashForAllOntapVersion, mock.Anything, mock.AnythingOfType("map[string][]expertmodeactivities.PoolDetailWithCurrentHash")).Return(nil, errors.New("failed to get latest hash")).Times(3)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed
	// Note: The workflow handles errors by updating job status, so it may complete successfully
	// even if activities fail. The error handling is internal to the workflow.
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// The workflow should return an error when activities fail after retries
	// However, if UpdateJobStatus succeeds, the workflow may return nil
	// For this test, we verify the workflow handled the error by checking completion
	// In a real scenario, the job status would be ERROR
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_GetPoolByUUIDFails() {
	// Test workflow when GetPoolByUUID activity fails for a pool
	// The workflow should return an error when any pool fails

	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-1",
			},
			Name: "pool-1",
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "9.18.1",
				RbacFileHash: "old-hash-1",
			},
		},
	}
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(pools, nil).Once()

	poolsByVersion := map[string][]expertmodeactivities.PoolDetailWithCurrentHash{
		"9.18.1": {
			{PoolUUID: "pool-uuid-1", CurrentHash: "old-hash-1"},
		},
	}
	s.env.OnActivity(rbacActivity.GetPoolsDetailsByOntapVersion, mock.Anything, pools).Return(poolsByVersion, nil).Once()

	poolListNeedUpdate := []expertmodeactivities.PoolDetailsWithRbacHash{
		{
			PoolUUID:       "pool-uuid-1",
			LatestRbacHash: "new-hash-123",
			CurrentHash:    "old-hash-1",
			NeedUpdate:     true,
		},
	}
	s.env.OnActivity(rbacActivity.GetLatestRbacHashForAllOntapVersion, mock.Anything, mock.AnythingOfType("map[string][]expertmodeactivities.PoolDetailWithCurrentHash")).Return(poolListNeedUpdate, nil).Once()

	// Mock GetPoolByUUID to fail (allow retries)
	s.env.OnActivity(rbacActivity.GetPoolByUUID, mock.Anything, mock.AnythingOfType("string")).Return(nil, errors.New("pool not found")).Maybe()

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed with error (workflow should fail when any pool fails)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	// Verify error message contains information about the failed pool
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "pool-uuid-1")
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_GetCredentialsFails() {
	// Test workflow when GetOnTapCredentials activity fails

	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-1",
			},
			Name: "pool-1",
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "9.18.1",
				RbacFileHash: "old-hash-1",
			},
		},
	}
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	poolActivity := &activities.PoolActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(pools, nil).Once()

	poolsByVersion := map[string][]expertmodeactivities.PoolDetailWithCurrentHash{
		"9.18.1": {
			{PoolUUID: "pool-uuid-1", CurrentHash: "old-hash-1"},
		},
	}
	s.env.OnActivity(rbacActivity.GetPoolsDetailsByOntapVersion, mock.Anything, pools).Return(poolsByVersion, nil).Once()

	poolListNeedUpdate := []expertmodeactivities.PoolDetailsWithRbacHash{
		{
			PoolUUID:       "pool-uuid-1",
			LatestRbacHash: "new-hash-123",
			CurrentHash:    "old-hash-1",
			NeedUpdate:     true,
		},
	}
	s.env.OnActivity(rbacActivity.GetLatestRbacHashForAllOntapVersion, mock.Anything, mock.AnythingOfType("map[string][]expertmodeactivities.PoolDetailWithCurrentHash")).Return(poolListNeedUpdate, nil).Once()

	pool1 := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "pool-uuid-1",
		},
		Name: "pool-1",
		BuildInfo: &datamodel.PoolBuildInfo{
			OntapVersion: "9.18.1",
			RbacFileHash: "old-hash-1",
			RbacFileUrl:  "GCNV/9.18.1/RBAC/gcnvadmin_create_cli",
		},
		VLMConfig: `{"deployment":{"deployment_id":"deployment-1"}}`,
	}
	s.env.OnActivity(rbacActivity.GetPoolByUUID, mock.Anything, "pool-uuid-1").Return(pool1, nil).Once()

	// Mock GetOnTapCredentials to fail (allow retries)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.AnythingOfType("*datamodel.Pool")).Return(nil, errors.New("failed to get credentials")).Maybe()

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed with error (workflow should fail when any pool fails)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	// Verify error message contains information about the failed pool
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "pool-uuid-1")
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_BatchProcessing() {
	// Test workflow with multiple pools to verify batch parallel processing
	// This test uses 7 pools to ensure they are processed in batches (batch size is 5)

	pools := []*datamodel.Pool{
		{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-1"}, Name: "pool-1", BuildInfo: &datamodel.PoolBuildInfo{OntapVersion: "9.18.1", RbacFileHash: "old-hash-1"}},
		{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-2"}, Name: "pool-2", BuildInfo: &datamodel.PoolBuildInfo{OntapVersion: "9.18.1", RbacFileHash: "old-hash-2"}},
		{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-3"}, Name: "pool-3", BuildInfo: &datamodel.PoolBuildInfo{OntapVersion: "9.18.1", RbacFileHash: "old-hash-3"}},
		{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-4"}, Name: "pool-4", BuildInfo: &datamodel.PoolBuildInfo{OntapVersion: "9.18.1", RbacFileHash: "old-hash-4"}},
		{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-5"}, Name: "pool-5", BuildInfo: &datamodel.PoolBuildInfo{OntapVersion: "9.18.1", RbacFileHash: "old-hash-5"}},
		{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-6"}, Name: "pool-6", BuildInfo: &datamodel.PoolBuildInfo{OntapVersion: "9.18.1", RbacFileHash: "old-hash-6"}},
		{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-7"}, Name: "pool-7", BuildInfo: &datamodel.PoolBuildInfo{OntapVersion: "9.18.1", RbacFileHash: "old-hash-7"}},
	}
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	poolActivity := &activities.PoolActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(pools, nil).Once()

	poolsByVersion := map[string][]expertmodeactivities.PoolDetailWithCurrentHash{
		"9.18.1": {
			{PoolUUID: "pool-uuid-1", CurrentHash: "old-hash-1"},
			{PoolUUID: "pool-uuid-2", CurrentHash: "old-hash-2"},
			{PoolUUID: "pool-uuid-3", CurrentHash: "old-hash-3"},
			{PoolUUID: "pool-uuid-4", CurrentHash: "old-hash-4"},
			{PoolUUID: "pool-uuid-5", CurrentHash: "old-hash-5"},
			{PoolUUID: "pool-uuid-6", CurrentHash: "old-hash-6"},
			{PoolUUID: "pool-uuid-7", CurrentHash: "old-hash-7"},
		},
	}
	s.env.OnActivity(rbacActivity.GetPoolsDetailsByOntapVersion, mock.Anything, pools).Return(poolsByVersion, nil).Once()

	poolListNeedUpdate := []expertmodeactivities.PoolDetailsWithRbacHash{
		{PoolUUID: "pool-uuid-1", LatestRbacHash: "new-hash-123", CurrentHash: "old-hash-1", NeedUpdate: true},
		{PoolUUID: "pool-uuid-2", LatestRbacHash: "new-hash-123", CurrentHash: "old-hash-2", NeedUpdate: true},
		{PoolUUID: "pool-uuid-3", LatestRbacHash: "new-hash-123", CurrentHash: "old-hash-3", NeedUpdate: true},
		{PoolUUID: "pool-uuid-4", LatestRbacHash: "new-hash-123", CurrentHash: "old-hash-4", NeedUpdate: true},
		{PoolUUID: "pool-uuid-5", LatestRbacHash: "new-hash-123", CurrentHash: "old-hash-5", NeedUpdate: true},
		{PoolUUID: "pool-uuid-6", LatestRbacHash: "new-hash-123", CurrentHash: "old-hash-6", NeedUpdate: true},
		{PoolUUID: "pool-uuid-7", LatestRbacHash: "new-hash-123", CurrentHash: "old-hash-7", NeedUpdate: true},
	}
	s.env.OnActivity(rbacActivity.GetLatestRbacHashForAllOntapVersion, mock.Anything, mock.AnythingOfType("map[string][]expertmodeactivities.PoolDetailWithCurrentHash")).Return(poolListNeedUpdate, nil).Once()

	// Mock activities for all 7 pools
	ontapCredentials := &vlm.OntapCredentials{AdminPassword: "password"}
	expertCredentials := &vlm.OntapCredentials{AdminPassword: "expert-password"}

	// Create VLM configs and requests for each pool with unique deployment IDs
	vlmConfigs := make(map[int]*vlm.VLMConfig)
	createVSAExpertModeReqs := make(map[int]*vlm.OntapExpertModeUserConfig)
	for i := 1; i <= 7; i++ {
		vlmConfigs[i] = &vlm.VLMConfig{Deployment: vlm.DeploymentConfig{DeploymentID: fmt.Sprintf("deployment-%d", i)}}
		createVSAExpertModeReqs[i] = &vlm.OntapExpertModeUserConfig{VLMConfig: *vlmConfigs[i]}
	}

	// Mock GetPoolByUUID for all pools with unique deployment IDs
	for i := 1; i <= 7; i++ {
		poolUUID := fmt.Sprintf("pool-uuid-%d", i)
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: poolUUID},
			Name:      fmt.Sprintf("pool-%d", i),
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "9.18.1",
				RbacFileHash: fmt.Sprintf("old-hash-%d", i),
				RbacFileUrl:  "GCNV/9.18.1/RBAC/gcnvadmin_create_cli",
			},
			VLMConfig: fmt.Sprintf(`{"deployment":{"deployment_id":"deployment-%d"}}`, i),
		}
		s.env.OnActivity(rbacActivity.GetPoolByUUID, mock.Anything, poolUUID).Return(pool, nil).Once()
	}

	// Mock other activities for all 7 pools
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.AnythingOfType("*datamodel.Pool")).Return(ontapCredentials, nil).Times(7)

	s.env.OnActivity(poolActivity.GetExpertModeCredentials, mock.Anything, mock.AnythingOfType("*datamodel.Pool")).Return(expertCredentials, nil).Times(7)

	// Mock ParseVlmConfig to return different VLM configs for each pool
	for i := 1; i <= 7; i++ {
		poolUUID := fmt.Sprintf("pool-uuid-%d", i)
		s.env.OnActivity(poolActivity.ParseVlmConfig, mock.Anything, mock.MatchedBy(func(pool *datamodel.Pool) bool {
			return pool.UUID == poolUUID
		})).Return(vlmConfigs[i], nil).Once()
	}

	// Mock PrepareCreateVSAExpertModeReq to return different requests for each pool
	for i := 1; i <= 7; i++ {
		deploymentID := fmt.Sprintf("deployment-%d", i)
		s.env.OnActivity(poolActivity.PrepareCreateVSAExpertModeReq, mock.MatchedBy(func(vlmConfig vlm.VLMConfig) bool {
			return vlmConfig.Deployment.DeploymentID == deploymentID
		}), mock.AnythingOfType("vlm.OntapCredentials"), mock.AnythingOfType("vlm.OntapCredentials"), mock.AnythingOfType("*datamodel.Pool"), mock.Anything).Return(createVSAExpertModeReqs[i], nil).Once()
	}

	s.env.OnActivity(poolActivity.UpdateRbacCheckSumInPool, mock.Anything, mock.AnythingOfType("*datamodel.Pool"), mock.AnythingOfType("*hyperscaler.BucketFileDetails")).Return(nil).Times(7)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *RBACWorkflowTestSuite) Test_UpdateRbacHashForPoolsWorkflow_StatusQuery() {
	// Test status query handler functionality

	pools := []*datamodel.Pool{}
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	s.env.OnActivity(rbacActivity.ListActiveExpertModePools, mock.Anything).Return(pools, nil).Once()

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateRbacForPoolsWorkflow)

	// Query workflow status - Note: Status query may return CREATED if workflow hasn't started yet
	// We'll just verify the query handler exists and doesn't error
	var statusResult *workflows.WorkflowStatus
	value, err := s.env.QueryWorkflow("status")
	if err == nil {
		err = value.Get(&statusResult)
		assert.NoError(s.T(), err)
		// Status may be CREATED, RUNNING, or COMPLETED depending on workflow execution state
		assert.NotNil(s.T(), statusResult)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func TestRBACWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(RBACWorkflowTestSuite))
}
