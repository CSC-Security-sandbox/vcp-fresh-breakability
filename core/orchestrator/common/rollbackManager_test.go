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

type mockFuture struct{ mock.Mock }

func (m *mockFuture) Get(ctx workflow.Context, valuePtr interface{}) error {
	args := m.Called(ctx, valuePtr)
	return args.Error(0)
}
func (m *mockFuture) IsReady() bool { return true }

// --- Tests ---

func TestRollbackManager_ExecuteRollback(t *testing.T) {
	// Patch util.GetLogger
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{"test": "value"})
	wfCtx := &mockWorkflowContext{base: ctx}

	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	rm := NewRollbackManager()
	rm.Add(nil, 1)
	rm.Add(nil, 2)

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	rm.ExecuteRollback(wfCtx, errors.New("fail"))
}

func TestRollbackManager_Add(t *testing.T) {
	rm := NewRollbackManager()
	rm.Add("activity1", 1, 2)
	assert.Equal(t, 1, len(rm.rollbacks))
	assert.Equal(t, "activity1", rm.rollbacks[0].activity)
	assert.Equal(t, []interface{}{1, 2}, rm.rollbacks[0].args)
}

func TestRollbackManager_ExecuteRollback_LIFO(t *testing.T) {
	// Patch util.GetLogger
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{"test": "value"})
	wfCtx := &mockWorkflowContext{base: ctx}

	origExecuteActivity := executeActivity
	defer func() { executeActivity = origExecuteActivity }()

	activityOrder := []string{}
	rm := NewRollbackManager()
	rm.Add(func(ctx workflow.Context, arg int) error {
		activityOrder = append(activityOrder, "first")
		return nil
	}, 1)
	rm.Add(func(ctx workflow.Context, arg int) error {
		activityOrder = append(activityOrder, "second")
		return nil
	}, 2)

	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		// Call the activity to record order
		if fn, ok := activity.(func(workflow.Context, int) error); ok {
			_ = fn(ctx, args[0].(int))
		}
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	rm.ExecuteRollback(wfCtx, errors.New("fail"))
	assert.Equal(t, []string{"second", "first"}, activityOrder)
}
