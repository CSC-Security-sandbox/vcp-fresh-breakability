package inmemotasksprocessor

import "time"

type UnitResult struct {
	UnitID     string // Format: taskId + "unit" + unitId (e.g., "task_1unit1")
	Result     interface{}
	Err        error
	RetryCount int // Number of retries performed (0 means no retries)
}

type TaskCtx struct {
	Timeout time.Duration
	TaskID  *string
}

type TaskResult struct {
	TaskID      string
	UnitResults []UnitResult
	Err         error
}

type TaskFunc func(imtpCtx interface{}, inputs ...interface{})

func NewTaskCtx(timeout time.Duration) TaskCtx {
	return TaskCtx{Timeout: timeout}
}

func NewTaskCtxWithID(timeout time.Duration, taskID string) TaskCtx {
	return TaskCtx{Timeout: timeout, TaskID: &taskID}
}
