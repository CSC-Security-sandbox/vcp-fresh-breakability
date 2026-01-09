package activities

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

// mockWorkflowRun is a simple mock for client.WorkflowRun
type mockWorkflowRun struct {
	mock.Mock
}

// createMockWorkflowExecutionResponse creates a mock DescribeWorkflowExecutionResponse with the given status
func createMockWorkflowExecutionResponse(status enums.WorkflowExecutionStatus) *workflowservice.DescribeWorkflowExecutionResponse {
	mockDesc := &workflowservice.DescribeWorkflowExecutionResponse{}
	// Get the WorkflowExecutionInfo field and set its Status using reflection
	info := mockDesc.GetWorkflowExecutionInfo()
	if info == nil {
		// Create a new WorkflowExecutionInfo using reflection
		infoType := reflect.TypeOf(mockDesc).Elem()
		infoField, _ := infoType.FieldByName("WorkflowExecutionInfo")
		infoValue := reflect.New(infoField.Type.Elem())
		reflect.ValueOf(mockDesc).Elem().FieldByName("WorkflowExecutionInfo").Set(infoValue)
		info = mockDesc.GetWorkflowExecutionInfo()
	}
	// Set Status using reflection
	statusField := reflect.ValueOf(info).Elem().FieldByName("Status")
	if statusField.IsValid() && statusField.CanSet() {
		statusField.Set(reflect.ValueOf(status))
	}
	return mockDesc
}

func (m *mockWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	args := m.Called(ctx, valuePtr)
	return args.Error(0)
}

func (m *mockWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	args := m.Called(ctx, valuePtr, options)
	return args.Error(0)
}

func (m *mockWorkflowRun) GetID() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockWorkflowRun) GetRunID() string {
	args := m.Called()
	return args.String(0)
}

func TestNewCancellationActivity_WithTemporalClient(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	assert.NotNil(t, activity)
	assert.Equal(t, mockClient, activity.TemporalClient)
}

func TestNewCancellationActivity_WithoutTemporalClient(t *testing.T) {
	activity := NewCancellationActivity(nil)

	assert.NotNil(t, activity)
	assert.Nil(t, activity.TemporalClient)
}

func TestCancellationActivity_getTemporalClient_WithClientSet(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := &CancellationActivity{
		TemporalClient: mockClient,
	}

	ctx := context.Background()
	result := activity.getTemporalClient(ctx)

	assert.Equal(t, mockClient, result)
}

func TestCancellationActivity_getTemporalClient_FromActivityContext(t *testing.T) {
	// This test requires a proper activity context which would panic in unit tests
	// We'll test this scenario by ensuring the function handles the case when
	// TemporalClient is set (which is the normal case)
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := &CancellationActivity{
		TemporalClient: mockClient,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	// When TemporalClient is set, it should return that client
	result := activity.getTemporalClient(ctx)

	assert.Equal(t, mockClient, result)
}

// TestCancellationActivity_getTemporalClient_WhenTemporalClientIsNil tests the case when
// TemporalClient is nil and the function falls back to activity.GetClient(ctx)
// This covers line 30 in cancellation_activities.go
// We test this by creating a test activity that calls getTemporalClient in an activity context
func TestCancellationActivity_getTemporalClient_WhenTemporalClientIsNil(t *testing.T) {
	// Use Temporal test activity environment to get a proper activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()

	activity := &CancellationActivity{
		TemporalClient: nil, // Explicitly set to nil to test fallback to activity.GetClient(ctx)
	}

	// Define a test activity that calls getTemporalClient to verify it works
	// We return a boolean instead of the client since client.Client is an interface
	// and cannot be serialized by Temporal's test framework
	testActivity := func(ctx context.Context) (bool, error) {
		// This will call getTemporalClient which should use activity.GetClient(ctx) on line 30
		// since TemporalClient is nil
		client := activity.getTemporalClient(ctx)
		if client == nil {
			return false, errors.New("failed to get client from activity context")
		}
		return true, nil
	}

	// Register the test activity
	testEnv.RegisterActivity(testActivity)

	// Execute the activity - this will trigger getTemporalClient with nil TemporalClient
	// The function should fall back to activity.GetClient(ctx) which returns the test client
	result, err := testEnv.ExecuteActivity(testActivity)

	// The function should not panic and should return a client (from activity context)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify that the result indicates success (client was retrieved)
	var success bool
	err = result.Get(&success)
	assert.NoError(t, err)
	assert.True(t, success)
}

func TestCancellationActivity_IsWorkflowRunningActivity_Success(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"

	// Create a mock response with WorkflowExecutionInfo showing running status
	mockDesc := createMockWorkflowExecutionResponse(enums.WORKFLOW_EXECUTION_STATUS_RUNNING)
	mockClient.EXPECT().DescribeWorkflowExecution(ctx, workflowID, "").Return(mockDesc, nil)

	result, err := activity.IsWorkflowRunningActivity(ctx, workflowID)

	assert.NoError(t, err)
	assert.True(t, result)
	mockClient.AssertExpectations(t)
}

func TestCancellationActivity_IsWorkflowRunningActivity_NotRunning(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"

	// Create a mock response with WorkflowExecutionInfo showing completed status
	mockDesc := createMockWorkflowExecutionResponse(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	mockClient.EXPECT().DescribeWorkflowExecution(ctx, workflowID, "").Return(mockDesc, nil)

	result, err := activity.IsWorkflowRunningActivity(ctx, workflowID)

	assert.NoError(t, err)
	assert.False(t, result)
	mockClient.AssertExpectations(t)
}

func TestCancellationActivity_IsWorkflowRunningActivity_Error(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"

	mockClient.EXPECT().DescribeWorkflowExecution(ctx, workflowID, "").Return(nil, errors.New("workflow not found"))

	result, err := activity.IsWorkflowRunningActivity(ctx, workflowID)

	assert.Error(t, err)
	assert.False(t, result)
	mockClient.AssertExpectations(t)
}

func TestCancellationActivity_SendCancelSignalActivity_Success(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"
	signalName := "cancel-signal"
	signalData := "cancel data"

	mockClient.EXPECT().SignalWorkflow(ctx, workflowID, "", signalName, signalData).Return(nil)

	err := activity.SendCancelSignalActivity(ctx, workflowID, signalName, signalData)

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestCancellationActivity_SendCancelSignalActivity_Error(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"
	signalName := "cancel-signal"
	signalData := "cancel data"

	mockClient.EXPECT().SignalWorkflow(ctx, workflowID, "", signalName, signalData).Return(errors.New("failed to send signal"))

	err := activity.SendCancelSignalActivity(ctx, workflowID, signalName, signalData)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to send signal")
	mockClient.AssertExpectations(t)
}

func TestCancellationActivity_WaitForWorkflowCancellationAckActivity_Success(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"
	timeout := 5 * time.Minute

	mockRun := &mockWorkflowRun{}
	mockClient.EXPECT().GetWorkflow(ctx, workflowID, "").Return(mockRun)
	mockRun.On("Get", mock.Anything, nil).Return(nil)

	result, err := activity.WaitForWorkflowCancellationAckActivity(ctx, workflowID, timeout)

	assert.NoError(t, err)
	assert.True(t, result)
	mockClient.AssertExpectations(t)
	mockRun.AssertExpectations(t)
}

func TestCancellationActivity_WaitForWorkflowCancellationAckActivity_Timeout(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"
	timeout := 1 * time.Millisecond // Very short timeout

	mockRun := &mockWorkflowRun{}
	mockClient.EXPECT().GetWorkflow(ctx, workflowID, "").Return(mockRun)
	// The implementation creates a context with timeout and checks ctxWithTimeout.Err()
	// We need to make the mock's Get() method wait long enough for the context to timeout
	// Use Run to access the actual context parameter
	mockRun.On("Get", mock.Anything, nil).Run(func(args mock.Arguments) {
		ctxParam := args.Get(0).(context.Context)
		// Wait longer than the timeout to ensure the context expires
		time.Sleep(5 * time.Millisecond)
		// The context should have timed out by now
		_ = ctxParam
	}).Return(context.DeadlineExceeded)

	result, err := activity.WaitForWorkflowCancellationAckActivity(ctx, workflowID, timeout)

	// When timeout occurs, the function returns false, nil (no error, but timeout occurred)
	assert.NoError(t, err)
	assert.False(t, result)
	mockClient.AssertExpectations(t)
	mockRun.AssertExpectations(t)
}

func TestCancellationActivity_ForceCancelWorkflowActivity_Success(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"

	mockClient.EXPECT().TerminateWorkflow(ctx, workflowID, "", "Force cancelled due to delete request", nil).Return(nil)

	err := activity.ForceCancelWorkflowActivity(ctx, workflowID)

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestCancellationActivity_ForceCancelWorkflowActivity_Error(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	activity := NewCancellationActivity(mockClient)

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"

	mockClient.EXPECT().TerminateWorkflow(ctx, workflowID, "", "Force cancelled due to delete request", nil).Return(errors.New("failed to terminate"))

	err := activity.ForceCancelWorkflowActivity(ctx, workflowID)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to terminate")
	mockClient.AssertExpectations(t)
}
