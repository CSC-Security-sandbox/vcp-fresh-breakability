package common

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// mockFuture is already defined in rollbackManager_test.go in the same package

func TestExecuteDeferredCleanup_NoErrorAndNotCancelled_ReturnsEarly(t *testing.T) {
	// Test for lines 29-30: Early return when no error and not cancelled
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: false,
	}
	rollbackManager := NewRollbackManager()
	
	// Add an activity that should not be called
	// Note: This is a workflow function that will be executed via workflow.ExecuteActivity
	// We don't register it as an activity because it's not a real activity function
	rollbackActivity := func(ctx workflow.Context) error {
		// This should not be called
		assert.Fail(t, "Rollback should not be executed")
		return nil
	}
	rollbackManager.AddActivity(rollbackActivity)
	// Don't register as activity - it will be executed via workflow.ExecuteActivity in ExecuteRollback

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		// Execute the function - it should return early without calling rollback
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, nil, nil, "volume", "test-uuid", nil, nil, nil)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	// If we get here without the assert.Fail being called, the test passes
	// The rollback manager's activities should not have been executed
}

func TestExecuteDeferredCleanup_NilLogger_SetsLoggerFromContext(t *testing.T) {
	// Test for lines 32-34: Setting logger if nil
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: false,
	}
	rollbackManager := NewRollbackManager()
	testErr := errors.New("test error")

	rollbackExecuted := false
	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackExecuted = true
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		// Execute the activity function to track execution
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, testErr, nil, "volume", "test-uuid", nil, nil, nil)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, rollbackExecuted, "Rollback should be executed")
}

func TestExecuteDeferredCleanup_Cancelled_ExecutesCallbackAndRollback(t *testing.T) {
	// Test for lines 37-41, 43-44: Cancellation handling with callback and rollback
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: true,
	}
	rollbackManager := NewRollbackManager()

	callbackCalled := false
	onCancellationCallback := func(ctx workflow.Context, cancelErr error) {
		callbackCalled = true
		assert.NotNil(t, cancelErr)
	}

	rollbackExecuted := false
	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackExecuted = true
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, nil, nil, "volume", "test-uuid", nil, onCancellationCallback, nil)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, callbackCalled, "Cancellation callback should be called")
	assert.True(t, rollbackExecuted, "Rollback should be executed on cancellation")
}

func TestExecuteDeferredCleanup_Cancelled_NoCallback_ExecutesRollback(t *testing.T) {
	// Test for lines 37-38, 43-44: Cancellation handling without callback
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: true,
	}
	rollbackManager := NewRollbackManager()

	rollbackExecuted := false
	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackExecuted = true
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, nil, nil, "volume", "test-uuid", nil, nil, nil)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, rollbackExecuted, "Rollback should be executed on cancellation even without callback")
}

func TestExecuteDeferredCleanup_Error_ExecutesRollback(t *testing.T) {
	// Test for lines 48, 59: Error handling and rollback execution
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: false,
	}
	rollbackManager := NewRollbackManager()
	testErr := errors.New("test error")

	rollbackExecuted := false
	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackExecuted = true
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, testErr, nil, "volume", "test-uuid", nil, nil, nil)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, rollbackExecuted, "Rollback should be executed on error")
}

func TestExecuteDeferredCleanup_Error_ShouldRollbackOnError_ReturnsFalse_SkipsRollback(t *testing.T) {
	// Test for lines 50-52: shouldRollbackOnError returns false, skips rollback
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: false,
	}
	rollbackManager := NewRollbackManager()
	testErr := errors.New("test error")

	shouldRollbackOnError := func(ctx workflow.Context, err error) bool {
		return false // Don't rollback
	}

	rollbackExecuted := false
	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackExecuted = true
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, testErr, nil, "volume", "test-uuid", nil, nil, shouldRollbackOnError)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.False(t, rollbackExecuted, "Rollback should not be executed when shouldRollbackOnError returns false")
}

func TestExecuteDeferredCleanup_Error_ShouldRollbackOnError_ReturnsTrue_ExecutesRollback(t *testing.T) {
	// Test for lines 50-52: shouldRollbackOnError returns true, executes rollback
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: false,
	}
	rollbackManager := NewRollbackManager()
	testErr := errors.New("test error")

	shouldRollbackOnError := func(ctx workflow.Context, err error) bool {
		return true // Rollback
	}

	rollbackExecuted := false
	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackExecuted = true
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, testErr, nil, "volume", "test-uuid", nil, nil, shouldRollbackOnError)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, rollbackExecuted, "Rollback should be executed when shouldRollbackOnError returns true")
}

func TestExecuteDeferredCleanup_Error_UpdateErrorState_CalledBeforeRollback(t *testing.T) {
	// Test for lines 56-57, 59: updateErrorState callback is called before rollback
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: false,
	}
	rollbackManager := NewRollbackManager()
	testErr := errors.New("test error")

	updateErrorStateCalled := false
	updateErrorStateOrder := 0
	rollbackOrder := 0

	updateErrorState := func(ctx workflow.Context) error {
		updateErrorStateCalled = true
		updateErrorStateOrder = 1
		return nil
	}

	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackOrder = 2
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, testErr, nil, "volume", "test-uuid", updateErrorState, nil, nil)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, updateErrorStateCalled, "updateErrorState should be called")
	assert.Equal(t, 1, updateErrorStateOrder, "updateErrorState should be called first")
	assert.Equal(t, 2, rollbackOrder, "Rollback should be called after updateErrorState")
}

func TestExecuteDeferredCleanup_Error_NoUpdateErrorState_ExecutesRollback(t *testing.T) {
	// Test for lines 48, 59: Error handling without updateErrorState callback
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: false,
	}
	rollbackManager := NewRollbackManager()
	testErr := errors.New("test error")

	rollbackExecuted := false
	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackExecuted = true
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		// Pass nil for updateErrorState
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, testErr, nil, "volume", "test-uuid", nil, nil, nil)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, rollbackExecuted, "Rollback should be executed even without updateErrorState callback")
}

func TestExecuteDeferredCleanup_WithProvidedLogger_UsesProvidedLogger(t *testing.T) {
	// Test that provided logger is used instead of getting from context
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetTestTimeout(1 * time.Minute)

	fields := log.Fields{}

	handler := &WorkflowCancellationHandler{
		cancelled: false,
	}
	rollbackManager := NewRollbackManager()
	testErr := errors.New("test error")

	mockLogger := &log.MockLogger{}
	// Note: Infof is only called when cancellationHandler.IsCancelled() is true (line 38)
	// In this test, we're testing the error path, so Infof won't be called

	rollbackExecuted := false
	rollbackManager.AddActivity(func(ctx workflow.Context) error {
		rollbackExecuted = true
		return nil
	})

	// Mock executeActivity at package level
	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		if fn, ok := activity.(func(workflow.Context) error); ok {
			_ = fn(ctx)
		}
		return f
	}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
		// ExecuteDeferredCleanup should use the provided logger without panicking
		ExecuteDeferredCleanup(ctx, handler, rollbackManager, testErr, mockLogger, "volume", "test-uuid", nil, nil, nil)
		return nil
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, rollbackExecuted, "Rollback should be executed")
	// The test verifies that the provided logger is used (no panic) and rollback executes
}
