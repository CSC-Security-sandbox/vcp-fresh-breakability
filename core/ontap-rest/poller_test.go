package ontap_rest

import (
	"context"
	"errors"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestPollOntapJob_Workflow_Success(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	go func() {
		defer mcs.MockClientServiceDone()
		env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, &cluster.JobGetOK{Payload: &models.Job{
		State: nillable.ToPointer("success"),
	}}, nil)
	mcs.AssertMockClientServiceDone()

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestPollOntapJob_Workflow_Error(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	go func() {
		defer mcs.MockClientServiceDone()
		env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, &cluster.JobGetOK{Payload: &models.Job{
		State: nillable.ToPointer("failed"),
	}}, errors.New("mock error"))
	mcs.AssertMockClientServiceDone()

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPollOntapJob_Workflow_Timeout(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)

	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	go func() {
		defer mcs.MockClientServiceDone()
		env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, &cluster.JobGetOK{Payload: &models.Job{
		State: nillable.ToPointer("in-progress"),
	}}, nil)

	// Check the workflow as not completed,
	require.False(t, env.IsWorkflowCompleted())
}

func TestPollOntapJob_Workflow_NewOntapRestClient_Error(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return nil, errors.New("client creation failed")
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "failed to create ontap-rest client")
}

func TestPollOntapJob_Workflow_JobFail(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	go func() {
		defer mcs.MockClientServiceDone()
		env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, &cluster.JobGetOK{Payload: &models.Job{
		State: nillable.ToPointer("failure"),
	}}, nil)
	mcs.AssertMockClientServiceDone()

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPollOntapJob_Workflow_OverrideTimeout(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)

	// Override the timeout variable for this test
	origTimeout := timeout
	timeout = 100 * time.Millisecond
	defer func() { timeout = origTimeout }()

	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	go func() {
		defer mcs.MockClientServiceDone()
		env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)
	}()

	// Simulate job always in-progress to trigger timeout
	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, &cluster.JobGetOK{Payload: &models.Job{
		State: nillable.ToPointer("in-progress"),
	}}, nil)
	mcs.AssertMockClientServiceDone()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("timeout", nil)
	}, timeout)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "timed out")
}

func TestPollOntapJob_Workflow_WorkflowSleepError(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)

	// Override workflowSleep to return an error
	origWorkflowSleep := workflowSleep
	workflowSleep = func(ctx workflow.Context, d time.Duration) error {
		return errors.New("workflow sleep error")
	}
	defer func() { workflowSleep = origWorkflowSleep }()

	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	go func() {
		defer mcs.MockClientServiceDone()
		env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, &cluster.JobGetOK{Payload: &models.Job{
		State: nillable.ToPointer("in-progress"),
	}}, nil)
	mcs.AssertMockClientServiceDone()

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "workflow sleep error")
}

// workflowTest calls the Poll function from within a Temporal workflow.
func workflowTest(ctx workflow.Context, clientParams RESTClientParams, uuid string) error {
	// Execute the activityTest function within the workflow context.
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	return workflow.ExecuteActivity(ctx, activityTest, clientParams, uuid).Get(ctx, nil)
}

// activityTest calls the Poll function from within a Temporal activity.
func activityTest(ctx context.Context, clientParams RESTClientParams, uuid string) error {
	clientParams.Ctx = ctx
	p := &poller{
		clientParams: clientParams,
		logger:       log.NewLogger(),
	}
	return p.Poll(uuid)
}

type mockFuture struct {
	error bool
}

func (m *mockFuture) GetID() string {
	return ""
}
func (m *mockFuture) GetRunID() string {
	return ""
}
func (m *mockFuture) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	return nil
}
func (m *mockFuture) Get(ctx context.Context, result interface{}) error {
	if m != nil && m.error {
		return errors.New("mock future error")
	}
	return nil
}

func TestPoll_PollOntapJobWorkflow_Success(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	// Patch fetchTemporalClient to return mockOntap
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
	}()

	var mockFut *mockFuture
	mockTemp.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockFut, nil)

	// Register the workflow
	env.RegisterActivity(activityTest)
	env.ExecuteWorkflow(workflowTest, RESTClientParams{}, "job-uuid")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestPoll_PollOntapJobWorkflow_Error(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	// Patch fetchTemporalClient to return mockOntap
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
	}()

	mockTemp.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test error"))

	// Register the workflow
	env.RegisterActivity(activityTest)
	env.ExecuteWorkflow(workflowTest, RESTClientParams{}, "job-uuid")

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPoll_PollOntapJobWorkflow_FutureError(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	// Patch fetchTemporalClient to return mockOntap
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
	}()

	mockFut := &mockFuture{
		error: true,
	}
	mockTemp.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockFut, nil)

	// Register the workflow
	env.RegisterActivity(activityTest)
	env.ExecuteWorkflow(workflowTest, RESTClientParams{}, "job-uuid")

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPoll_PollOntapJobWorkflow_NotActivityContextError(t *testing.T) {
	err := activityTest(context.Background(), RESTClientParams{}, "job-uuid")
	require.Error(t, err)
	require.Equal(t, "Context is not an activity context, cannot poll job in non-blocking way", err.Error())
}
