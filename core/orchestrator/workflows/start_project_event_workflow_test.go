package workflows

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type StartProjectEventOffStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *StartProjectEventOffStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(StartProjectEventOffStateWorkflow)
}

func (s *StartProjectEventOffStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Create empty filter result for empty pool list
	emptyFilterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: []*datamodel.PoolView{}, // Empty pools
		VSAError:      false,                   // No transient states
	}

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(emptyFilterResult, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Create empty filter result for empty pool list
	emptyFilterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: []*datamodel.PoolView{}, // Empty pools
		VSAError:      false,                   // No transient states
	}

	// Mock ListPoolsForAccount to return empty pool list
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(emptyFilterResult, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities
	result := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_FirstUpdateJobFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	// Execute workflow

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 10)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_StartProjectEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, errors.New("failed to start SDE Activity"))
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_PollStartProjectEventSDEOperationActivity() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities
	result := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to Poll SDE Activity"))
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_TransientStateFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Create test pools for listing
	allPoolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				State:     datamodel.LifeCycleStateCreating, // Transient state
			},
		},
	}

	// Create filter result indicating transient states detected
	filterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: []*datamodel.PoolView{}, // No pools pass filter
		VSAError:      true,                    // Transient states detected
	}

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateDisabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(allPoolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(filterResult, nil)
	// Account state should be reverted to enabled on failure
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to transient states
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "transient states")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_FilterFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Create test pools for listing
	allPoolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				State:     datamodel.LifeCycleStateREADY,
			},
		},
	}

	// Mock activities - filter operation fails
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateDisabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(allPoolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("database error during filtering"))
	// Account state should be reverted to enabled on failure
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to filter failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_TransientStateWithPartialSuccess() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations for successful pool operations
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil)

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Create test pools - some in transient states, some valid
	allPoolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "valid-pool",
				State:     datamodel.LifeCycleStateREADY,
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-2", ID: 2},
				Name:      "transient-pool",
				State:     datamodel.LifeCycleStateCreating, // Transient state
			},
		},
	}

	// Filter result: some pools pass, but transient states detected
	filteredPools := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "valid-pool",
				State:     datamodel.LifeCycleStateREADY,
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
	}
	filterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: filteredPools, // Some pools pass filter
		VSAError:      true,          // But transient states detected
	}

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateDisabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(allPoolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(filterResult, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	// Account state should be reverted to enabled on failure due to transient states
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed despite successful pool operations due to transient states
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "transient states")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_WithVLMOperations_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations for processClusterForOFFState
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil)

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Create test pools
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-2", ID: 2},
				Name:      "test-pool-2",
				VLMConfig: `{"deployment":{"deployment_id":"dep-2"}}`,
			},
		},
	}

	// Register activities - including UpdatePoolState and FilterPoolsForClusterOperations
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Create filter result - no transient states detected
	filterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: poolList, // All pools pass filter
		VSAError:      false,    // No transient states detected
	}

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(filterResult, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_WithVLMOperations_PoolListFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities - list pools fails
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to list pools"))

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_WithVLMOperations_EmptyPoolList() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Mock activities - empty pool list
	emptyPoolFilterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: []*datamodel.PoolView{},
		VSAError:      false,
	}
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(emptyPoolFilterResult, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_ValidateClusterHealth_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations - health check fails
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(errors.New("cluster health check failed"))
	mockVSAClientWorkflowManager.AssertNotCalled(s.T(), "ClusterPowerOp") // Power off should not be called

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to health check failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_ClusterPowerOff_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations - health check succeeds, power off fails
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(errors.New("cluster power off failed"))

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
	}

	// Create filter result for valid pools
	poolFilterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: poolList,
		VSAError:      false,
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(poolFilterResult, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to power off failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_InvalidVLMConfig() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools with invalid VLM config JSON
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"invalid": json config"malformed}`, // Malformed JSON
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to invalid VLM config
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_MixedPoolResults() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations - first health check succeeds, second fails for OFF state
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(nil).Once()
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(errors.New("cluster health check failed for dep-2")).Once()
	// Power off operations - first succeeds, second should not be called due to health check failure
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil).Once()

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools - mix of valid configs
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-2", ID: 2},
				Name:      "test-pool-2",
				VLMConfig: `{"deployment":{"deployment_id":"dep-2"}}`,
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to mixed results (at least one failure)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_AccountStateReversion_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations - health check fails for OFF state
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(errors.New("cluster health check failed"))

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Create filter result - no transient states detected
	filterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: poolList, // All pools pass filter
		VSAError:      false,    // No transient states detected
	}

	// Mock activities - account state reversion fails
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateDisabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(filterResult, nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(errors.New("failed to revert account state"))
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed - should handle reversion failure appropriately
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

// Test coverage for lines 218-241: SDE polling logic after VSA operations complete
func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_SDEPolling_AlreadyCompleted() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock SDE result that is already completed (Done = true)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true), // SDE operation already completed
		Name: nillable.GetStringPtr("sde-operation-on-123"),
	}

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	// PollStartProjectEventSDEOperationActivity should NOT be called since SDE is already done
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Times(0)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + DONE
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_SDEPolling_NeedsPolling_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock SDE result that needs polling (Done = false)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false), // SDE operation needs polling
		Name: nillable.GetStringPtr("sde-operation-on-456"),
	}

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil) // Polling succeeds
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + DONE
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_SDEPolling_NeedsPolling_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock SDE result that needs polling (Done = false)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false), // SDE operation needs polling
		Name: nillable.GetStringPtr("sde-operation-on-789"),
	}

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("SDE polling failed: operation timeout"))
	// Account state should be reverted to disabled on SDE failure in ON state workflow
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateHyperscalerDisabled).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow failed due to SDE polling failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + ERROR
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_SDEPolling_WithLimitedRemainingTime() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set a very short VSA timeout to test the remaining time calculation logic
	originalTimeout := VSAOperationTimeout
	VSAOperationTimeout = 5 * time.Second // Very short timeout
	defer func() {
		VSAOperationTimeout = originalTimeout
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)

	// Mock SDE result that needs polling (Done = false)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false), // SDE operation needs polling
		Name: nillable.GetStringPtr("sde-operation-short-timeout"),
	}

	// Mock activities - no pools so VSA operations complete immediately
	emptyPoolFilterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: []*datamodel.PoolView{},
		VSAError:      false,
	}
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateDisabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(emptyPoolFilterResult, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)

	// The SDE polling should be called with MaximumAttempts = 1 due to limited remaining time
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateHyperscalerDisabled).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOff,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	// Assert workflow completed successfully despite limited time
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + DONE
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_NoSDEOperation_WhenCVPHostEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Ensure CVP_HOST is empty to skip SDE operations
	cvp.CVP_HOST = ""

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)

	// Mock activities - no pools so VSA operations complete immediately
	emptyPoolFilterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: []*datamodel.PoolView{},
		VSAError:      false,
	}
	// First call: set account to DISABLING state
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateDisabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(emptyPoolFilterResult, nil)
	// Second call: set account to HYPERSCALER_DISABLED state since no SDE operations
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateHyperscalerDisabled).Return(nil)

	// SDE activities should NOT be called when CVP_HOST is empty
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Times(0)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Times(0)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOff,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	// Assert workflow completed successfully without SDE operations
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + DONE
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_SDEStart_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateDisabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	// SDE start operation fails
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, errors.New("Failed to start SDE operation"))
	// Revert account state due to failure
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	// PollStartProjectEventSDEOperationActivity should NOT be called since SDE start failed
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Times(0)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOff,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	// Assert workflow completed with error since SDE start failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + ERROR
}

func TestStartProjectEventOffStateWorkflow(t *testing.T) {
	suite.Run(t, new(StartProjectEventOffStateTestSuite))
}

type StartProjectEventOnStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *StartProjectEventOnStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(StartProjectEventOnStateWorkflow)
}

func (s *StartProjectEventOnStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true), // Set to true so polling is skipped
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Mock ListPoolsForAccount to return empty pool list
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	result := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateHyperscalerDisabled).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_FirstUpdateJobFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	// Execute workflow

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 10)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_StartProjectEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, errors.New("failed to start SDE Activity"))
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_PollStartProjectEventSDEOperationActivity() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities
	result := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to Poll SDE Activity"))
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_WithVLMOperations_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations for processClusterForONState
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil)

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Create test pools
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
	}

	// Register activities - including UpdatePoolState
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true), // Set to true so polling is skipped
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_WithVLMOperations_CredentialsFail() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
	}

	// Register activities - including UpdatePoolState
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get credentials"))
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true), // Set to true so polling is skipped
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to credentials failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_WithVLMOperations_PoolListFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock ListPoolsForAccount to fail
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to list pools"))
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow failed due to pool list failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_WithVLMOperations_EmptyPoolList() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)

	// Mock activities - empty pool list
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true), // Set to true so polling is skipped
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_ClusterPowerOn_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations - power on fails
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(errors.New("cluster power on failed"))

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow failed due to power on failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_InvalidVLMConfig() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Mock storage operations - expect failure due to invalid VLM config
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools with invalid VLM config
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"invalid": json config"malformed}`, // Malformed JSON
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true), // Set to true so polling is skipped
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to invalid VLM config
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_MixedPoolResults() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations - first call succeeds, second fails
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil).Once()
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(errors.New("cluster power on failed for dep-2")).Once()

	// Mock storage operations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools - mix of valid configs
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-2", ID: 2},
				Name:      "test-pool-2",
				VLMConfig: `{"deployment":{"deployment_id":"dep-2"}}`,
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed due to mixed results (at least one failure)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_AccountStateReversion_Fails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations - power on fails (health check passes, but power op fails)
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(errors.New("cluster power on failed"))

	// Mock storage operations - first call succeeds, second call (reverting) succeeds, third call fails
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Create test pools
	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-1", ID: 1},
				Name:      "test-pool-1",
				VLMConfig: `{"deployment":{"deployment_id":"dep-1"}}`,
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Mock activities - account state reversion fails after VSA operations fail
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	// First call should succeed (ENABLING), second call should fail (revert to HYPERSCALERDISABLED)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to revert account state")).Once()
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed - should handle reversion failure appropriately
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

// Test coverage for lines 218-241: SDE polling logic after VSA operations complete
func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_SDEPolling_AlreadyCompleted() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock SDE result that is already completed (Done = true)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true), // SDE operation already completed
		Name: nillable.GetStringPtr("sde-operation-on-123"),
	}

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	// PollStartProjectEventSDEOperationActivity should NOT be called since SDE is already done
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Times(0)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + DONE
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_SDEPolling_NeedsPolling_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock SDE result that needs polling (Done = false)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false), // SDE operation needs polling
		Name: nillable.GetStringPtr("sde-operation-on-456"),
	}

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil) // Polling succeeds
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + DONE
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_SDEPolling_NeedsPolling_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock SDE result that needs polling (Done = false)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false), // SDE operation needs polling
		Name: nillable.GetStringPtr("sde-operation-on-789"),
	}

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("SDE polling failed: operation timeout"))
	// Account state should be reverted to disabled on SDE failure in ON state workflow
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateHyperscalerDisabled).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow failed due to SDE polling failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + ERROR
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_SDEPolling_WithLimitedRemainingTime() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set a very short VSA timeout to test the remaining time calculation logic
	originalTimeout := VSAOperationTimeout
	VSAOperationTimeout = 5 * time.Second // Very short timeout
	defer func() {
		VSAOperationTimeout = originalTimeout
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock SDE result that needs polling (Done = false)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false), // SDE operation needs polling
		Name: nillable.GetStringPtr("sde-operation-short-timeout"),
	}

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)

	// The SDE polling should be called with MaximumAttempts = 1 due to limited remaining time
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow completed successfully despite limited time
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + DONE
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_NoSDEOperation_WhenCVPHostEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Ensure CVP_HOST is empty to skip SDE operations
	cvp.CVP_HOST = ""

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)

	// SDE activities should NOT be called when CVP_HOST is empty
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Times(0)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Times(0)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow completed successfully without SDE operations
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + DONE
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_SDEStart_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}

	// Set CVP_HOST to trigger SDE operations
	cvp.CVP_HOST = "test-sde-host"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)

	// Mock activities - no pools so VSA operations complete immediately
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabling).Return(nil)
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	// SDE start operation fails
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, errors.New("Failed to start SDE operation"))
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateEnabled).Return(nil)
	// Add mock for reversion state when SDE fails
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, datamodel.AccountStateHyperscalerDisabled).Return(nil)

	// PollStartProjectEventSDEOperationActivity should NOT be called since SDE start failed
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Times(0)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationId:     "test-location",
		ProjectNumber:  "123456789",
		State:          datamodel.StateOn,
		XCorrelationID: "test-correlation-id",
	}

	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	// Assert workflow completed with error due to SDE failure
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2) // PROCESSING + ERROR
}

// Test_StartProjectEventOffStateWorkflow_WithHarvestFarmPollerDeregistration tests that
// UnRegisterNodeFromHarvestFarmWorkflow is called when pool state is updated to DISABLED
func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_WithHarvestFarmPollerDeregistration() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations for processClusterForOFFState
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil)

	// Enable metrics to trigger harvest farm poller registration
	oldEnableMetrics := enableMetrics
	enableMetrics = true
	defer func() { enableMetrics = oldEnableMetrics }()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid-1",
				},
				Name:           "Pool1",
				AccountID:      1,
				DeploymentName: "deployment-1",
				VLMConfig:      `{"deployment":{"deployment_id":"dep-1"}}`,
				ClusterDetails: datamodel.ClusterDetails{
					RegionalTenantProject: "tenant-project-1",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					IsRegionalHA: false,
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Register child workflows
	s.env.RegisterWorkflow(UnRegisterNodeFromHarvestFarmWorkflow)

	// Create filter result
	filterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: poolList,
		VSAError:      false,
	}

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(filterResult, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock child workflow for unregister
	s.env.OnWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, mock.Anything, mock.MatchedBy(func(params *unRegisterNodeFromHarvestFarmParams) bool {
		return params.PoolID == 1 && params.CustomerProjectID == "ProjectNumber" && params.TenantProjectID == "tenant-project-1"
	})).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

// Test_StartProjectEventOnStateWorkflow_WithHarvestFarmPollerRegistration tests that
// RegisterNodeToHarvestFarmWorkflow is called when pool state is updated to READY
func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_WithHarvestFarmPollerRegistration() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations for processClusterForONState (only ClusterPowerOp, no ValidateClusterHealth)
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil)

	// Enable metrics to trigger harvest farm poller registration
	oldEnableMetrics := enableMetrics
	enableMetrics = true
	defer func() { enableMetrics = oldEnableMetrics }()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid-1",
				},
				Name:           "Pool1",
				AccountID:      1,
				DeploymentName: "deployment-1",
				VLMConfig:      `{"deployment":{"deployment_id":"dep-1"}}`,
				ClusterDetails: datamodel.ClusterDetails{
					RegionalTenantProject: "tenant-project-1",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					IsRegionalHA: false,
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Register child workflows
	s.env.RegisterWorkflow(RegisterNodeToHarvestFarmWorkflow)

	// Create filter result
	filterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: poolList,
		VSAError:      false,
	}

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(filterResult, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sdeResult := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(true),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(sdeResult, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock child workflow for register
	s.env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, mock.MatchedBy(func(input RegisterNodeToHarvestFarmWorkflowInput) bool {
		return input.PoolID == 1 && input.CustomerProjectID == "ProjectNumber" && input.TenantProjectID == "tenant-project-1" && input.PoolUUID == "pool-uuid-1"
	})).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

// Test_StartProjectEventOffStateWorkflow_WithHarvestFarmPollerDeregistration_ChildWorkflowFails tests that
// when UnRegisterNodeFromHarvestFarmWorkflow fails, it logs a warning but doesn't fail the parent workflow
func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_WithHarvestFarmPollerDeregistration_ChildWorkflowFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations for processClusterForOFFState
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil)

	// Enable metrics to trigger harvest farm poller registration
	oldEnableMetrics := enableMetrics
	enableMetrics = true
	defer func() { enableMetrics = oldEnableMetrics }()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid-1",
				},
				Name:           "Pool1",
				AccountID:      1,
				DeploymentName: "deployment-1",
				VLMConfig:      `{"deployment":{"deployment_id":"dep-1"}}`,
				ClusterDetails: datamodel.ClusterDetails{
					RegionalTenantProject: "tenant-project-1",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					IsRegionalHA: false,
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Register child workflows
	s.env.RegisterWorkflow(UnRegisterNodeFromHarvestFarmWorkflow)

	// Create filter result
	filterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: poolList,
		VSAError:      false,
	}

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(filterResult, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock child workflow to fail
	s.env.OnWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, mock.Anything, mock.Anything).Return(errors.New("unregister workflow failed"))

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully even though child workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

// Test_StartProjectEventOffStateWorkflow_WithHarvestFarmPollerDeregistration_MetricsDisabled tests that
// when enableMetrics is false, no harvest farm workflows are called
func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_WithHarvestFarmPollerDeregistration_MetricsDisabled() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	poolActivity := &activities.PoolActivity{SE: mockStorage}

	// Set up VLM mocking using dependency injection
	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	originalGetNewVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}
	defer func() {
		GetNewVSAClientWorkflowManager = originalGetNewVSAClientWorkflowManager
	}()

	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Set up VLM mock expectations for processClusterForOFFState
	mockVSAClientWorkflowManager.On("ValidateClusterHealth", mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("ClusterPowerOp", mock.Anything, mock.Anything).Return(nil)

	// Disable metrics - should not trigger harvest farm poller registration
	oldEnableMetrics := enableMetrics
	enableMetrics = false
	defer func() { enableMetrics = oldEnableMetrics }()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	poolList := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid-1",
				},
				Name:           "Pool1",
				AccountID:      1,
				DeploymentName: "deployment-1",
				VLMConfig:      `{"deployment":{"deployment_id":"dep-1"}}`,
				ClusterDetails: datamodel.ClusterDetails{
					RegionalTenantProject: "tenant-project-1",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					IsRegionalHA: false,
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)
	s.env.RegisterActivity(startProjectEventActivity.ListPoolsForAccount)
	s.env.RegisterActivity(startProjectEventActivity.FilterPoolsForClusterOperations)
	s.env.RegisterActivity(poolActivity.GetOnTapCredentials)
	s.env.RegisterActivity(startProjectEventActivity.UpdateAccountStateForHandleResource)
	s.env.RegisterActivity(poolActivity.UpdatePoolState)

	// Create filter result
	filterResult := &resource_events_activities.PoolFilterResult{
		FilteredPools: poolList,
		VSAError:      false,
	}

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.ListPoolsForAccount, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolList, nil)
	s.env.OnActivity(startProjectEventActivity.FilterPoolsForClusterOperations, mock.Anything, mock.Anything, mock.Anything).Return(filterResult, nil)
	s.env.OnActivity(poolActivity.GetOnTapCredentials, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.UpdateAccountStateForHandleResource, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(poolActivity.UpdatePoolState, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	params := &commonparams.StartProjectEventParams{
		LocationId:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         datamodel.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
	// Note: No child workflow should be called when enableMetrics is false
}

func TestStartProjectEventOnStateWorkflow(t *testing.T) {
	suite.Run(t, new(StartProjectEventOnStateTestSuite))
}
