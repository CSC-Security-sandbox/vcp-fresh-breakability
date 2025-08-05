package workflows

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type mockEncodedValue struct {
	err bool
}

type mockWorkflowContext struct {
	workflow.Context
	base context.Context
}

func (m *mockWorkflowContext) Value(key interface{}) interface{} {
	return m.base.Value(key)
}

type mockFuture struct{ mock.Mock }

func (m *mockFuture) Get(ctx workflow.Context, valuePtr interface{}) error {
	args := m.Called(ctx, valuePtr)
	return args.Error(0)
}
func (m *mockFuture) IsReady() bool { return true }

func (m mockEncodedValue) Get(valuePtr interface{}) error {
	if m.err {
		return fmt.Errorf("encoding error for value: %+v", valuePtr)
	}
	return nil
}

func (m mockEncodedValue) HasValue() bool {
	return true
}

func TestQueryWorkflowStatus_Success(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	expectedStatus := &WorkflowStatus{}

	mockClient.On("QueryWorkflow", mock.Anything, "wf-1", "run-1", StatusQueryName).
		Return(mockEncodedValue{err: false}, nil)

	status, err := QueryWorkflowStatus(context.Background(), mockClient, "wf-1", "run-1")
	assert.NoError(t, err)
	assert.Equal(t, expectedStatus, status)
}

func TestQueryWorkflowStatus_EncodeError(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)

	mockClient.On("QueryWorkflow", mock.Anything, "wf-1", "run-1", StatusQueryName).
		Return(mockEncodedValue{err: true}, nil)

	status, err := QueryWorkflowStatus(context.Background(), mockClient, "wf-1", "run-1")
	assert.Error(t, err)
	assert.Nil(t, status)
}

func TestQueryWorkflowStatus_QueryError(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	mockClient.On("QueryWorkflow", mock.Anything, "wf-2", "run-2", StatusQueryName).
		Return(nil, errors.New("query error"))

	status, err := QueryWorkflowStatus(context.Background(), mockClient, "wf-2", "run-2")
	assert.Error(t, err)
	assert.Nil(t, status)
}

func TestPopulateRetryPolicyParams(t *testing.T) {
	origStartToCloseTimeout := StartToCloseTimeout
	origRetryInterval := RetryInterval
	origRetryMaxAttempts := RetryMaxAttempts
	origRetryMaxInterval := RetryMaxInterval
	origRetryBackoff := RetryBackoff

	defer func() {
		StartToCloseTimeout = origStartToCloseTimeout
		RetryInterval = origRetryInterval
		RetryMaxAttempts = origRetryMaxAttempts
		RetryMaxInterval = origRetryMaxInterval
		RetryBackoff = origRetryBackoff
	}()

	t.Run("success", func(t *testing.T) {
		StartToCloseTimeout = "10m"
		RetryInterval = "1s"
		RetryMaxAttempts = 2
		RetryMaxInterval = "2m"
		RetryBackoff = "1.5"
		policy, err := PopulateRetryPolicyParams()
		assert.NoError(t, err)
		assert.NotNil(t, policy)
	})

	t.Run("invalid StartToCloseTimeout", func(t *testing.T) {
		StartToCloseTimeout = "invalid"
		_, err := PopulateRetryPolicyParams()
		assert.Error(t, err)
	})

	t.Run("invalid RetryInterval", func(t *testing.T) {
		StartToCloseTimeout = "10m"
		RetryInterval = "invalid"
		_, err := PopulateRetryPolicyParams()
		assert.Error(t, err)
	})

	t.Run("invalid RetryMaxInterval", func(t *testing.T) {
		StartToCloseTimeout = "10m"
		RetryInterval = "1s"
		RetryMaxInterval = "invalid"
		_, err := PopulateRetryPolicyParams()
		assert.Error(t, err)
	})

	t.Run("invalid RetryBackoff", func(t *testing.T) {
		StartToCloseTimeout = "10m"
		RetryInterval = "1s"
		RetryMaxInterval = "2m"
		RetryBackoff = "invalid"
		_, err := PopulateRetryPolicyParams()
		assert.Error(t, err)
	})
}

func TestUpdateJobStatusWithCustomError(t *testing.T) {
	ctx := context.TODO()
	wfCtx := &mockWorkflowContext{base: ctx}
	origExecuteActivity := executeActivity

	defer func() { executeActivity = origExecuteActivity }()
	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	bw := &BaseWorkflow{ID: "test-id"}
	jobError := temporal.NewApplicationError("test error", "CustomError", 123, "original error details")
	err := bw.UpdateJobStatus(wfCtx, models.JobStateFailure, jobError)

	assert.NoError(t, err)
}

func TestUpdateJobStatusWithCustomErrorMissingDetails(t *testing.T) {
	ctx := context.TODO()
	wfCtx := &mockWorkflowContext{base: ctx}
	origExecuteActivity := executeActivity

	defer func() { executeActivity = origExecuteActivity }()
	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	bw := &BaseWorkflow{Logger: util.GetLogger(ctx), ID: "test-id"}
	jobError := temporal.NewApplicationError("test error", "CustomError", nil)
	err := bw.UpdateJobStatus(wfCtx, models.JobStateFailure, jobError)

	assert.NoError(t, err)
}

func TestUpdateJobStatusWithGenericError(t *testing.T) {
	ctx := context.TODO()
	wfCtx := &mockWorkflowContext{base: ctx}
	origExecuteActivity := executeActivity

	defer func() { executeActivity = origExecuteActivity }()
	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	bw := &BaseWorkflow{Logger: util.GetLogger(ctx), ID: "test-id"}
	jobError := errors.New("test error")
	err := bw.UpdateJobStatus(wfCtx, models.JobStateFailure, jobError)

	assert.NoError(t, err)
}

func TestUpdateJobStatusWithEmptyID(t *testing.T) {
	ctx := context.TODO()
	wfCtx := &mockWorkflowContext{base: ctx}
	origExecuteActivity := executeActivity

	defer func() { executeActivity = origExecuteActivity }()
	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	bw := &BaseWorkflow{ID: ""}
	err := bw.UpdateJobStatus(wfCtx, models.JobStateFailure, errors.New("test error"))

	assert.Error(t, err)
	assert.ErrorContains(t, err, "job uuid cannot be empty")
}

func TestGetSnapshotPolicyName(t *testing.T) {
	t.Run("ReturnsPolicyName", func(t *testing.T) {
		volume := &datamodel.Volume{
			SnapshotPolicy: &datamodel.SnapshotPolicy{
				Name: "policy1",
			},
		}
		result := getSnapshotPolicyName(volume)
		assert.Equal(t, "policy1", result)
	})

	t.Run("ReturnsEmptyString_WhenVolumeIsNil", func(t *testing.T) {
		var volume *datamodel.Volume
		result := getSnapshotPolicyName(volume)
		assert.Equal(t, "", result)
	})

	t.Run("ReturnsEmptyString_WhenSnapshotPolicyIsNil", func(t *testing.T) {
		volume := &datamodel.Volume{}
		result := getSnapshotPolicyName(volume)
		assert.Equal(t, "", result)
	})

	t.Run("ReturnsEmptyString_WhenPolicyNameIsEmpty", func(t *testing.T) {
		volume := &datamodel.Volume{
			SnapshotPolicy: &datamodel.SnapshotPolicy{
				Name: "",
			},
		}
		result := getSnapshotPolicyName(volume)
		assert.Equal(t, "", result)
	})
}

func WfTest(ctx workflow.Context, jobUUID string, timeout time.Duration) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
	})
	err := PollOnDBJob(ctx, jobUUID, timeout)
	if err != nil {
		return fmt.Errorf("workflow test failed: %w", err)
	}
	return nil
}
func TestWaitForDBJob_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	commonActivity := activities.CommonActivities{}
	jobUUID := "job-uuid"
	job := &datamodel.Job{
		State: "DONE",
	}

	env.OnActivity(commonActivity.GetJob, mock.Anything, jobUUID).Return(job, nil)

	env.RegisterActivity(commonActivity.GetJob)
	env.ExecuteWorkflow(WfTest, jobUUID, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestWaitForDBJob_JobWithErrorDetails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	commonActivity := activities.CommonActivities{}

	jobUUID := "job-uuid"
	job := &datamodel.Job{
		State:        "DONE",
		ErrorDetails: "some error",
	}

	env.OnActivity(commonActivity.GetJob, mock.Anything, jobUUID).Return(job, nil)

	env.RegisterActivity(commonActivity.GetJob)
	env.ExecuteWorkflow(WfTest, jobUUID, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job completed with error")
}

func TestWaitForDBJob_Timeout(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	commonActivity := activities.CommonActivities{}

	jobUUID := "job-uuid"
	job := &datamodel.Job{
		State: "PENDING",
	}

	env.OnActivity(commonActivity.GetJob, mock.Anything, jobUUID).Return(job, nil)

	env.RegisterActivity(commonActivity.GetJob)
	env.ExecuteWorkflow(WfTest, jobUUID, 1*time.Millisecond)

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestWaitForDBJob_GetJobFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	commonActivity := activities.CommonActivities{}

	jobUUID := "job-uuid"

	env.OnActivity(commonActivity.GetJob, mock.Anything, jobUUID).Return(nil, assert.AnError)

	env.RegisterActivity(commonActivity.GetJob)
	env.ExecuteWorkflow(WfTest, jobUUID, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get db job status")
}

func TestCreateNodeForProviderWithPool_CERT(t *testing.T) {
	dbNodes := []*datamodel.Node{
		{EndpointAddress: "1.1.1.1", HostDNSName: "host1"},
		{EndpointAddress: "2.2.2.2", HostDNSName: "host2"},
	}
	pool := &datamodel.Pool{
		DeploymentName: "cluster1",
		PoolCredentials: &datamodel.PoolCredentials{
			CertificateID: "cert-123",
			AuthType:      env.USER_CERTIFICATE,
		},
	}
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: pool.PoolCredentials.Password, SecretID: pool.PoolCredentials.SecretID, DeploymentName: pool.DeploymentName, CertificateID: pool.PoolCredentials.CertificateID, AuthType: pool.PoolCredentials.AuthType})
	assert.Equal(t, map[string]string{"1.1.1.1": "host1", "2.2.2.2": "host2"}, node.EndpointAddressesToHostNameMap)
	assert.Equal(t, "cluster1", node.DeploymentName)
	assert.Equal(t, "cert-123", node.CertificateID)
}

func TestCreateNodeForProviderWithPool_NonCERT(t *testing.T) {
	dbNodes := []*datamodel.Node{
		{EndpointAddress: "1.1.1.1"},
		{EndpointAddress: "2.2.2.2"},
	}
	pool := &datamodel.Pool{
		DeploymentName: "cluster2",
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "secret",
			AuthType: env.USERNAME_PWD,
		},
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: pool.PoolCredentials.Password, SecretID: pool.PoolCredentials.SecretID, DeploymentName: pool.DeploymentName, CertificateID: pool.PoolCredentials.CertificateID, AuthType: pool.PoolCredentials.AuthType})
	assert.Equal(t, map[string]string{"1.1.1.1": "1.1.1.1", "2.2.2.2": "2.2.2.2"}, node.EndpointAddressesToHostNameMap)
	assert.Equal(t, "secret", node.Password)
	assert.Equal(t, "cluster2", node.DeploymentName)
}
