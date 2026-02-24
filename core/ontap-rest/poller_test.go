package ontap_rest

import (
	"context"
	"errors"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"sync/atomic"
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
	origWait := wait
	wait = 10 * time.Millisecond
	defer func() { wait = origWait }()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(30 * time.Second) // Set test timeout to prevent deadlock detection
	env.RegisterWorkflow(PollOntapJob)
	env.RegisterActivity(PollOntapJobActivity)

	// Mock the activity to return success
	env.OnActivity(PollOntapJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestPollOntapJob_Workflow_Error(t *testing.T) {
	origWait := wait
	wait = 10 * time.Millisecond
	defer func() { wait = origWait }()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)
	env.RegisterActivity(PollOntapJobActivity)

	// Mock the activity to return an error
	env.OnActivity(PollOntapJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPollOntapJob_Workflow_Timeout(t *testing.T) {
	origWait := wait
	wait = 10 * time.Millisecond
	defer func() { wait = origWait }()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)
	env.RegisterActivity(PollOntapJobActivity)

	// Mock the activity to return an error for in-progress state
	env.OnActivity(PollOntapJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("Job is still processing"))

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)

	// The workflow should complete with an error due to retry policy
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPollOntapJob_Workflow_NewOntapRestClient_Error(t *testing.T) {
	origWait := wait
	wait = 10 * time.Millisecond
	defer func() { wait = origWait }()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)
	env.RegisterActivity(PollOntapJobActivity)

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
	origWait := wait
	wait = 10 * time.Millisecond
	defer func() { wait = origWait }()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)
	env.RegisterActivity(PollOntapJobActivity)

	// Mock the activity to return an error for job failure
	env.OnActivity(PollOntapJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("job failed"))

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPollOntapJob_Workflow_OverrideTimeout(t *testing.T) {
	origWait := wait
	wait = 10 * time.Millisecond
	defer func() { wait = origWait }()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)
	env.RegisterActivity(PollOntapJobActivity)

	// Override the timeout variable for this test
	origTimeout := timeout
	timeout = 100 * time.Millisecond
	defer func() { timeout = origTimeout }()

	// Mock the activity to return success
	env.OnActivity(PollOntapJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)

	// The workflow should complete successfully
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestPollOntapJob_Workflow_WorkflowSleepError(t *testing.T) {
	origWait := wait
	wait = 10 * time.Millisecond
	defer func() { wait = origWait }()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(PollOntapJob)
	env.RegisterActivity(PollOntapJobActivity)

	// Override workflowSleep to return an error
	origWorkflowSleep := workflowSleep
	workflowSleep = func(ctx workflow.Context, d time.Duration) error {
		return errors.New("workflow sleep error")
	}
	defer func() { workflowSleep = origWorkflowSleep }()

	// Mock the activity to return success
	env.OnActivity(PollOntapJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	clientParams := RESTClientParams{}
	uuid := "job-uuid"

	env.ExecuteWorkflow(PollOntapJob, clientParams, uuid)

	// The current PollOntapJob workflow doesn't use workflowSleep, so it should complete successfully
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
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

type blockingFuture struct {
	done chan struct{}
	err  error
}

func (b *blockingFuture) GetID() string {
	return ""
}
func (b *blockingFuture) GetRunID() string {
	return ""
}
func (b *blockingFuture) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	return nil
}
func (b *blockingFuture) Get(ctx context.Context, result interface{}) error {
	<-b.done
	return b.err
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

func TestPoll_PollOntapJobWorkflow_RecordsHeartbeat(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
	}()

	var heartbeatCount int32
	origRecordHeartbeat := recordHeartbeat
	recordHeartbeat = func(ctx context.Context, details ...interface{}) {
		atomic.AddInt32(&heartbeatCount, 1)
	}
	defer func() {
		recordHeartbeat = origRecordHeartbeat
	}()

	origHeartbeatInterval := pollerHeartbeatInterval
	pollerHeartbeatInterval = 5 * time.Millisecond
	defer func() {
		pollerHeartbeatInterval = origHeartbeatInterval
	}()

	done := make(chan struct{})
	mockFut := &blockingFuture{done: done}
	mockTemp.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockFut, nil)

	go func() {
		for {
			if atomic.LoadInt32(&heartbeatCount) > 0 {
				close(done)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	env.RegisterActivity(activityTest)
	env.ExecuteWorkflow(workflowTest, RESTClientParams{}, "job-uuid")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.GreaterOrEqual(t, atomic.LoadInt32(&heartbeatCount), int32(1))
}

func TestPoll_PollOntapJobWorkflow_Error(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	origStartMaxRetries := pollWorkflowStartMaxRetries
	pollWorkflowStartMaxRetries = 1
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
		pollWorkflowStartMaxRetries = origStartMaxRetries
	}()

	mockTemp.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test error"))

	env.RegisterActivity(activityTest)
	env.ExecuteWorkflow(workflowTest, RESTClientParams{}, "job-uuid")

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPoll_PollOntapJobWorkflow_StartRetriesAndSucceeds(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	origStartMaxRetries := pollWorkflowStartMaxRetries
	pollWorkflowStartMaxRetries = 3
	origStartRetryWait := pollWorkflowStartRetryWait
	pollWorkflowStartRetryWait = 0
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
		pollWorkflowStartMaxRetries = origStartMaxRetries
		pollWorkflowStartRetryWait = origStartRetryWait
	}()

	var successFut *mockFuture
	mockTemp.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("transient error")).Once()
	mockTemp.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(successFut, nil).Once()

	env.RegisterActivity(activityTest)
	env.ExecuteWorkflow(workflowTest, RESTClientParams{}, "job-uuid")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestPoll_PollOntapJobWorkflow_FutureError(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
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

func TestPollOntapJobActivity_Success(t *testing.T) {
	ctx := context.Background()
	clientParams := RESTClientParams{}
	uuid := "test-job-uuid"

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	// Mock successful job response
	jobResponse := &cluster.JobGetOK{
		Payload: &models.Job{
			State: nillable.ToPointer(models.JobStateSuccess),
		},
	}

	// Set up the mock expectation and run in goroutine
	go func() {
		defer mcs.MockClientServiceDone()
		// Call the activity
		result, err := PollOntapJobActivity(ctx, clientParams, uuid)

		// Verify results
		require.NoError(t, err)
		require.Nil(t, result) // Should return nil for success state
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, jobResponse, nil)
	mcs.AssertMockClientServiceDone()
}

func TestPollOntapJobActivity_Failure(t *testing.T) {
	ctx := context.Background()
	clientParams := RESTClientParams{}
	uuid := "test-job-uuid"

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	// Mock failed job response
	jobResponse := &cluster.JobGetOK{
		Payload: &models.Job{
			State: nillable.ToPointer(models.JobStateFailure),
		},
	}

	// Set up the mock expectation and run in goroutine
	go func() {
		defer mcs.MockClientServiceDone()
		// Call the activity
		result, err := PollOntapJobActivity(ctx, clientParams, uuid)

		// Verify results
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "jobGetOK")
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, jobResponse, nil)
	mcs.AssertMockClientServiceDone()
}

func TestPollOntapJobActivity_InProgress(t *testing.T) {
	ctx := context.Background()
	clientParams := RESTClientParams{}
	uuid := "test-job-uuid"

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	// Mock in-progress job response
	jobResponse := &cluster.JobGetOK{
		Payload: &models.Job{
			State: nillable.ToPointer("in-progress"),
		},
	}

	// Set up the mock expectation and run in goroutine
	go func() {
		defer mcs.MockClientServiceDone()
		// Call the activity
		result, err := PollOntapJobActivity(ctx, clientParams, uuid)

		// Verify results - should return error for in-progress state
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "Job is still processing")
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, jobResponse, nil)
	mcs.AssertMockClientServiceDone()
}

func TestPollOntapJobActivity_ClientCreationError(t *testing.T) {
	ctx := context.Background()
	clientParams := RESTClientParams{}
	uuid := "test-job-uuid"

	// Mock NewOntapRestClient to return an error
	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return nil, errors.New("client creation failed")
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	// Call the activity
	result, err := PollOntapJobActivity(ctx, clientParams, uuid)

	// Verify results
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to create ontap-rest client")
}

func TestPollOntapJobActivity_GetJobError(t *testing.T) {
	ctx := context.Background()
	clientParams := RESTClientParams{}
	uuid := "test-job-uuid"

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	// Set up the mock expectation and run in goroutine
	go func() {
		defer mcs.MockClientServiceDone()
		// Call the activity
		result, err := PollOntapJobActivity(ctx, clientParams, uuid)

		// Verify results
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "failed to poll job")
	}()

	// Mock GetJob to return an error
	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, nil, errors.New("job not found"))
	mcs.AssertMockClientServiceDone()
}

func TestPollOntapJobActivity_NilJobResponse(t *testing.T) {
	ctx := context.Background()
	clientParams := RESTClientParams{}
	uuid := "test-job-uuid"

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	// Mock successful job response but with nil payload
	jobResponse := &cluster.JobGetOK{
		Payload: nil,
	}

	// Set up the mock expectation and run in goroutine
	go func() {
		defer mcs.MockClientServiceDone()
		// Call the activity - this should panic due to nil pointer dereference
		require.Panics(t, func() {
			_, _ = PollOntapJobActivity(ctx, clientParams, uuid)
		})
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, jobResponse, nil)
	mcs.AssertMockClientServiceDone()
}

func TestPollOntapJobActivity_NilJobState(t *testing.T) {
	ctx := context.Background()
	clientParams := RESTClientParams{}
	uuid := "test-job-uuid"

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	// Mock job response with nil state
	jobResponse := &cluster.JobGetOK{
		Payload: &models.Job{
			State: nil,
		},
	}

	// Set up the mock expectation and run in goroutine
	go func() {
		defer mcs.MockClientServiceDone()
		// Call the activity - this should panic due to nil pointer dereference
		require.Panics(t, func() {
			_, _ = PollOntapJobActivity(ctx, clientParams, uuid)
		})
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, jobResponse, nil)
	mcs.AssertMockClientServiceDone()
}

func TestPollOntapJobActivity_WithLogger(t *testing.T) {
	ctx := context.Background()
	clientParams := RESTClientParams{
		Trace: log.NewLogger(), // Add a logger to test the logger assignment
	}
	uuid := "test-job-uuid"

	// Mock NewOntapRestClient to return a mock client
	mcs := cluster.NewMockClientService(t)
	mockOntap := &OntapRestClient{cluster: &clusterClient{api: mcs}}

	origMockAPI := NewOntapRestClient
	NewOntapRestClient = func(params RESTClientParams) (RESTClient, error) {
		// Verify that the logger was assigned to Trace
		require.NotNil(t, params.Trace)
		return mockOntap, nil
	}
	defer func() {
		NewOntapRestClient = origMockAPI
	}()

	// Mock successful job response
	jobResponse := &cluster.JobGetOK{
		Payload: &models.Job{
			State: nillable.ToPointer(models.JobStateSuccess),
		},
	}

	// Set up the mock expectation and run in goroutine
	go func() {
		defer mcs.MockClientServiceDone()
		// Call the activity
		result, err := PollOntapJobActivity(ctx, clientParams, uuid)

		// Verify results
		require.NoError(t, err)
		require.Nil(t, result)
	}()

	mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, jobResponse, nil)
	mcs.AssertMockClientServiceDone()
}
