package common

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/workflow"
)

// --- Mocks ---
type mockWorkflowContext struct {
	workflow.Context
	base context.Context
}

func (m *mockWorkflowContext) Value(key interface{}) interface{} {
	return m.base.Value(key)
}

type mockChildWorkflowFuture struct {
	mockFuture
}

func (m *mockChildWorkflowFuture) GetChildWorkflowExecution() workflow.Future {
	return new(mockFuture)
}

func (m *mockChildWorkflowFuture) SignalChildWorkflow(ctx workflow.Context, signalName string, data interface{}) workflow.Future {
	return new(mockFuture)
}

type mockFuture struct{ mock.Mock }

func (m *mockFuture) Get(ctx workflow.Context, valuePtr interface{}) error {
	args := m.Called(ctx, valuePtr)
	return args.Error(0)
}
func (m *mockFuture) IsReady() bool { return true }

// --- Tests ---

func TestRollbackManager_ExecuteRollback(t *testing.T) {
	t.Run("should execute all rollback activities without error", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{"test": "value"})
		wfCtx := &mockWorkflowContext{base: ctx}

		origExecuteActivity := executeActivity
		defer func() { executeActivity = origExecuteActivity }()

		rm := NewRollbackManager()
		rm.AddActivity(nil, 1)
		rm.AddActivity(nil, 2)

		executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
			f := new(mockFuture)
			f.On("Get", ctx, mock.Anything).Return(nil)
			return f
		}

		rm.ExecuteRollback(wfCtx, errors.New("fail"))
	})
}

func TestRollbackManager_Add(t *testing.T) {
	t.Run("should add activity to rollback manager", func(t *testing.T) {
		rm := NewRollbackManager()
		rm.AddActivity("activity1", 1, 2)
		assert.Equal(t, 1, len(rm.rollbacks))
		assert.Equal(t, "activity1", rm.rollbacks[0].fn)
		assert.Equal(t, []interface{}{1, 2}, rm.rollbacks[0].args)
	})
}

func TestRollbackManager_ExecuteRollback_LIFO(t *testing.T) {
	t.Run("should execute rollback activities in LIFO order", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{"test": "value"})
		wfCtx := &mockWorkflowContext{base: ctx}

		origExecuteActivity := executeActivity
		defer func() { executeActivity = origExecuteActivity }()

		activityOrder := []string{}
		rm := NewRollbackManager()
		rm.AddActivity(func(ctx workflow.Context, arg int) error {
			activityOrder = append(activityOrder, "first")
			return nil
		}, 1)
		rm.AddActivity(func(ctx workflow.Context, arg int) error {
			activityOrder = append(activityOrder, "second")
			return nil
		}, 2)

		executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
			if fn, ok := activity.(func(workflow.Context, int) error); ok {
				_ = fn(ctx, args[0].(int))
			}
			f := new(mockFuture)
			f.On("Get", ctx, mock.Anything).Return(nil)
			return f
		}

		rm.ExecuteRollback(wfCtx, errors.New("fail"))
		assert.Equal(t, []string{"second", "first"}, activityOrder)
	})
}

func TestRollbackManager_AddWorkflow_And_ExecuteRollback(t *testing.T) {
	t.Run("should execute rollback workflows in LIFO order with task queue", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{"test": "value"})
		wfCtx := &mockWorkflowContext{base: ctx}

		origExecuteChildWorkflow := executeChildWorkflow
		defer func() { executeChildWorkflow = origExecuteChildWorkflow }()

		workflowOrder := []string{}
		rm := NewRollbackManager()
		rm.AddWorkflow("test-queue", func(ctx workflow.Context, arg int) error {
			workflowOrder = append(workflowOrder, "first-workflow")
			return nil
		}, 1)
		rm.AddWorkflow("test-queue", func(ctx workflow.Context, arg int) error {
			workflowOrder = append(workflowOrder, "second-workflow")
			return nil
		}, 2)

		executeChildWorkflow = func(ctx workflow.Context, workflowFn interface{}, args ...interface{}) workflow.ChildWorkflowFuture {
			if fn, ok := workflowFn.(func(workflow.Context, int) error); ok {
				_ = fn(ctx, args[0].(int))
			}
			f := new(mockChildWorkflowFuture)
			f.On("Get", ctx, mock.Anything).Return(nil)
			return f
		}

		rm.ExecuteRollback(wfCtx, errors.New("fail"))
		assert.Equal(t, []string{"second-workflow", "first-workflow"}, workflowOrder)
	})
}

func TestRollbackManager_ExecuteRollback_WorkflowError(t *testing.T) {
	t.Run("should log error if workflow rollback fails", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{"test": "value"})
		wfCtx := &mockWorkflowContext{base: ctx}

		origExecuteChildWorkflow := executeChildWorkflow
		defer func() { executeChildWorkflow = origExecuteChildWorkflow }()

		rm := NewRollbackManager()
		rm.AddWorkflow("test-queue", func(ctx workflow.Context, arg int) error {
			return errors.New("should not be called directly")
		}, 1)

		executeChildWorkflow = func(ctx workflow.Context, workflowFn interface{}, args ...interface{}) workflow.ChildWorkflowFuture {
			f := new(mockChildWorkflowFuture)
			f.On("Get", ctx, mock.Anything).Return(errors.New("workflow rollback failed"))
			return f
		}

		rm.ExecuteRollback(wfCtx, errors.New("fail"))
	})
}
