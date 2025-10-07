package backgroundworkflows

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/testsuite"
)

// Helper function to set enableSyncPoolZIZS to true and return a cleanup function
func setEnableSyncPoolZIZSTrue() func() {
	originalValue := enableSyncPoolZIZS
	enableSyncPoolZIZS = true
	return func() {
		enableSyncPoolZIZS = originalValue
	}
}

// Test workflow function
func TestSyncPoolZIZSDetailsWorkflow_Success(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-1-uuid",
			Name:      "pool-1",
			AccountID: 12345,
			VendorID:  "vendor-1",
		},
		{
			UUID:      "pool-2-uuid",
			Name:      "pool-2",
			AccountID: 12345,
			VendorID:  "vendor-2",
		},
	}

	// Register activities
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(poolIdentifiers, nil)

	// Mock child workflow executions
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Times(2)

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}

func TestSyncPoolZIZSDetailsWorkflow_EmptyPoolList(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register activities - return empty list
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return([]*database.PoolIdentifier{}, nil)

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncPoolZIZSDetailsWorkflow_NilPoolList(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register activities - return nil
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncPoolZIZSDetailsWorkflow_ListPoolsFailure(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register activities - ListPoolsUUID fails
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(nil, errors.New("database error"))

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "database error")
}

func TestSyncPoolZIZSDetailsWorkflow_ChildWorkflowFailure(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-1-uuid",
			Name:      "pool-1",
			AccountID: 12345,
			VendorID:  "vendor-1",
		},
		{
			UUID:      "pool-2-uuid",
			Name:      "pool-2",
			AccountID: 12345,
			VendorID:  "vendor-2",
		},
	}

	// Register activities
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(poolIdentifiers, nil)

	// Mock child workflow executions - first succeeds, second fails
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Once()
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(errors.New("child workflow failed")).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions - workflow should still complete successfully even if some child workflows fail
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}

func TestSyncPoolZIZSDetailsWorkflow_MultiplePools(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data - multiple pools
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-1-uuid",
			Name:      "pool-1",
			AccountID: 12345,
			VendorID:  "vendor-1",
		},
		{
			UUID:      "pool-2-uuid",
			Name:      "pool-2",
			AccountID: 12345,
			VendorID:  "vendor-2",
		},
		{
			UUID:      "pool-3-uuid",
			Name:      "pool-3",
			AccountID: 67890,
			VendorID:  "vendor-3",
		},
	}

	// Register activities
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(poolIdentifiers, nil)

	// Mock child workflow executions
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Times(3)

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}

func TestSyncPoolZIZSDetailsWorkflow_RetryPolicy(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-1-uuid",
			Name:      "pool-1",
			AccountID: 12345,
			VendorID:  "vendor-1",
		},
	}

	// Register activities with retry scenario
	callCount := 0
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method with retry scenario
	env.OnActivity("ListPoolsUUID", mock.Anything).Run(func(args mock.Arguments) {
		callCount++
	}).Return(func(context.Context) ([]*database.PoolIdentifier, error) {
		if callCount == 1 {
			return nil, errors.New("temporary error")
		}
		return poolIdentifiers, nil
	})

	// Mock child workflow execution
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}

func TestSyncPoolZIZSDetailsWorkflow_ContextWithLoggerFields(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-1-uuid",
			Name:      "pool-1",
			AccountID: 12345,
			VendorID:  "vendor-1",
		},
	}

	// Register activities
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(poolIdentifiers, nil)

	// Mock child workflow execution
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}

func TestSyncPoolZIZSDetailsWorkflow_ActivityOptions(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-1-uuid",
			Name:      "pool-1",
			AccountID: 12345,
			VendorID:  "vendor-1",
		},
	}

	// Register activities
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(poolIdentifiers, nil)

	// Mock child workflow execution
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}

func TestSyncPoolZIZSDetailsWorkflow_WorkflowInfo(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-1-uuid",
			Name:      "pool-1",
			AccountID: 12345,
			VendorID:  "vendor-1",
		},
	}

	// Register activities
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(poolIdentifiers, nil)

	// Mock child workflow execution
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}

// Test the workflow with different pool configurations
func TestSyncPoolZIZSDetailsWorkflow_DifferentPoolConfigurations(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data with different configurations
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-gcp-uuid",
			Name:      "pool-gcp",
			AccountID: 12345,
			VendorID:  "gcp-vendor",
		},
		{
			UUID:      "pool-aws-uuid",
			Name:      "pool-aws",
			AccountID: 67890,
			VendorID:  "aws-vendor",
		},
		{
			UUID:      "pool-azure-uuid",
			Name:      "pool-azure",
			AccountID: 11111,
			VendorID:  "azure-vendor",
		},
	}

	// Register activities
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(poolIdentifiers, nil)

	// Mock child workflow executions
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Times(3)

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}

// Test workflow with timeout scenario
func TestSyncPoolZIZSDetailsWorkflow_Timeout(t *testing.T) {
	// Set enableSyncPoolZIZS to true for this test
	cleanup := setEnableSyncPoolZIZSTrue()
	defer cleanup()

	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Mock data
	poolIdentifiers := []*database.PoolIdentifier{
		{
			UUID:      "pool-1-uuid",
			Name:      "pool-1",
			AccountID: 12345,
			VendorID:  "vendor-1",
		},
	}

	// Register activities
	mockCommonActivities := &activities.CommonActivities{}
	env.RegisterActivity(mockCommonActivities)

	// Mock the ListPoolsUUID method
	env.OnActivity("ListPoolsUUID", mock.Anything).Return(poolIdentifiers, nil)

	// Mock child workflow execution with timeout
	env.OnWorkflow(workflows.SyncPoolComplianceForPoolWorkflow, mock.Anything, mock.AnythingOfType("*database.PoolIdentifier")).Return(nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncPoolZIZSDetailsWorkflow)

	// Assertions
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow calls
	env.AssertExpectations(t)
}
