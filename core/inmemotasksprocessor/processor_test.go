package inmemotasksprocessor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNonRetryableError(t *testing.T) {
	t.Run("Error method returns underlying error message", func(t *testing.T) {
		originalErr := errors.New("test error")
		nonRetryErr := NonRetryableError{Err: originalErr}

		assert.Equal(t, "test error", nonRetryErr.Error())
	})

	t.Run("Unwrap returns underlying error", func(t *testing.T) {
		originalErr := errors.New("test error")
		nonRetryErr := NonRetryableError{Err: originalErr}

		assert.Equal(t, originalErr, nonRetryErr.Unwrap())
	})
}

func TestUnitOptions_applyDefaults(t *testing.T) {
	t.Run("sets default timeout and retries", func(t *testing.T) {
		options := UnitOptions{}
		options.applyDefaults()

		assert.Equal(t, time.Second*30, options.Timeout)
		assert.Equal(t, 0, options.Retries)
	})

	t.Run("overwrites existing values with defaults", func(t *testing.T) {
		options := UnitOptions{
			Timeout: time.Minute * 2,
			Retries: 3,
		}
		options.applyDefaults()

		// applyDefaults overwrites all values regardless of current values
		assert.Equal(t, time.Second*30, options.Timeout)
		assert.Equal(t, 0, options.Retries)
	})
}

func TestIMTPContext_RunUnit(t *testing.T) {
	t.Run("successful unit execution", func(t *testing.T) {
		ctx := context.Background()
		imtpCtx := &IMTPContext{
			ctx:         ctx,
			taskID:      "test_task",
			unitResults: []UnitResult{},
			nextUnitID:  1,
		}

		unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
			return "success", nil
		}

		options := UnitOptions{Retries: 0, Timeout: time.Second * 5}
		result := imtpCtx.RunUnit(unitFunc, options, "input1", "input2")

		assert.Equal(t, "test_taskunit1", result.UnitID)
		assert.Equal(t, "success", result.Result)
		assert.NoError(t, result.Err)
		assert.Equal(t, 0, result.RetryCount)
		assert.Len(t, imtpCtx.unitResults, 1)
		assert.Equal(t, 2, imtpCtx.nextUnitID)
	})

	t.Run("unit execution with error - no retries due to applyDefaults", func(t *testing.T) {
		ctx := context.Background()
		imtpCtx := &IMTPContext{
			ctx:         ctx,
			taskID:      "test_task",
			unitResults: []UnitResult{},
			nextUnitID:  1,
		}

		callCount := 0
		unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
			callCount++
			return nil, errors.New("temporary error")
		}

		// Note: applyDefaults() will overwrite Retries to 0
		options := UnitOptions{Retries: 3, Timeout: time.Second * 10}
		result := imtpCtx.RunUnit(unitFunc, options, "input1")

		assert.Equal(t, "test_taskunit1", result.UnitID)
		assert.Nil(t, result.Result)
		assert.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "temporary error")
		// Due to applyDefaults overwriting Retries to 0
		assert.Equal(t, 0, result.RetryCount)
		assert.Equal(t, 1, callCount) // Only called once, no retries
	})

	t.Run("unit execution with non-retryable error", func(t *testing.T) {
		ctx := context.Background()
		imtpCtx := &IMTPContext{
			ctx:         ctx,
			taskID:      "test_task",
			unitResults: []UnitResult{},
			nextUnitID:  1,
		}

		callCount := 0
		unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
			callCount++
			return nil, &NonRetryableError{Err: errors.New("non-retryable error")}
		}

		options := UnitOptions{Retries: 3, Timeout: time.Second * 5}
		result := imtpCtx.RunUnit(unitFunc, options, "input1")

		assert.Equal(t, "test_taskunit1", result.UnitID)
		assert.Nil(t, result.Result)
		assert.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "non-retryable error")
		assert.Equal(t, 0, result.RetryCount)
		assert.Equal(t, 1, callCount) // Should not retry
	})

	t.Run("unit execution with default timeout - does not timeout", func(t *testing.T) {
		ctx := context.Background()
		imtpCtx := &IMTPContext{
			ctx:         ctx,
			taskID:      "test_task",
			unitResults: []UnitResult{},
			nextUnitID:  1,
		}

		unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
			// Quick return without sleep to avoid timing sensitivity
			return "completed", nil
		}

		// applyDefaults() will overwrite Timeout to 30 seconds
		options := UnitOptions{Retries: 0}
		result := imtpCtx.RunUnit(unitFunc, options)

		assert.Equal(t, "test_taskunit1", result.UnitID)
		// Should complete successfully because default timeout is 30 seconds
		assert.Equal(t, "completed", result.Result)
		assert.NoError(t, result.Err)
		assert.Equal(t, 0, result.RetryCount)
	})

	t.Run("parent context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		imtpCtx := &IMTPContext{
			ctx:         ctx,
			taskID:      "test_task",
			unitResults: []UnitResult{},
			nextUnitID:  1,
		}

		// Cancel context immediately to test cancellation behavior
		cancel()

		unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
			// Function should return immediately due to cancelled context
			return nil, ctx.Err()
		}

		options := UnitOptions{Retries: 0, Timeout: time.Second * 5}
		result := imtpCtx.RunUnit(unitFunc, options)

		assert.Equal(t, "test_taskunit1", result.UnitID)
		assert.Nil(t, result.Result)
		assert.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "context canceled")
	})

	t.Run("multiple units increment ID correctly", func(t *testing.T) {
		ctx := context.Background()
		imtpCtx := &IMTPContext{
			ctx:         ctx,
			taskID:      "test_task",
			unitResults: []UnitResult{},
			nextUnitID:  1,
		}

		unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
			return "success", nil
		}

		options := UnitOptions{Retries: 0, Timeout: time.Second * 5}

		result1 := imtpCtx.RunUnit(unitFunc, options)
		result2 := imtpCtx.RunUnit(unitFunc, options)
		result3 := imtpCtx.RunUnit(unitFunc, options)

		assert.Equal(t, "test_taskunit1", result1.UnitID)
		assert.Equal(t, "test_taskunit2", result2.UnitID)
		assert.Equal(t, "test_taskunit3", result3.UnitID)
		assert.Len(t, imtpCtx.unitResults, 3)
		assert.Equal(t, 4, imtpCtx.nextUnitID)
	})

	t.Run("concurrent unit execution with mutex protection", func(t *testing.T) {
		ctx := context.Background()
		imtpCtx := &IMTPContext{
			ctx:         ctx,
			taskID:      "test_task",
			unitResults: []UnitResult{},
			nextUnitID:  1,
		}

		unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
			return "success", nil
		}

		options := UnitOptions{Retries: 0, Timeout: time.Second * 5}

		// Run multiple units concurrently
		numGoroutines := 10
		results := make([]UnitResult, numGoroutines)
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				results[index] = imtpCtx.RunUnit(unitFunc, options)
			}(i)
		}

		wg.Wait()

		// Verify all units have unique IDs and all executed successfully
		unitIDs := make(map[string]bool)
		successCount := 0
		for _, result := range results {
			if result.Err == nil {
				successCount++
				assert.Equal(t, "success", result.Result)
				assert.False(t, unitIDs[result.UnitID], "Duplicate unit ID: %s", result.UnitID)
				unitIDs[result.UnitID] = true
			}
		}

		// All should succeed
		assert.Equal(t, numGoroutines, successCount)
		assert.Len(t, unitIDs, numGoroutines)
		// The unitResults slice should contain all results
		assert.Equal(t, numGoroutines, len(imtpCtx.unitResults))
		assert.Equal(t, numGoroutines+1, imtpCtx.nextUnitID)
	})
}

func TestNewInMemoTasksProcessor(t *testing.T) {
	t.Run("valid parameters", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(5, 10)

		require.NoError(t, err)
		assert.NotNil(t, processor)
		assert.Equal(t, 5, processor.numWorkers)
		assert.Equal(t, 1, processor.nextTaskID)
		assert.NotNil(t, processor.tasks)
		assert.NotNil(t, processor.results)
	})

	t.Run("minimum valid values", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(MinWorkers, MinBufferSize)

		require.NoError(t, err)
		assert.NotNil(t, processor)
		assert.Equal(t, MinWorkers, processor.numWorkers)
	})

	t.Run("maximum valid workers", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(MaxWorkers, MinBufferSize)

		require.NoError(t, err)
		assert.NotNil(t, processor)
		assert.Equal(t, MaxWorkers, processor.numWorkers)
	})

	t.Run("numWorkers too small", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(0, 10)

		assert.Error(t, err)
		assert.Nil(t, processor)
		assert.Contains(t, err.Error(), "numWorkers must be at least 1")
	})

	t.Run("numWorkers too large", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(MaxWorkers+1, 10)

		assert.Error(t, err)
		assert.Nil(t, processor)
		assert.Contains(t, err.Error(), "numWorkers exceeds maximum limit")
	})

	t.Run("bufferSize too small", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(5, 0)

		assert.Error(t, err)
		assert.Nil(t, processor)
		assert.Contains(t, err.Error(), "bufferSize must be at least 1")
	})
}

func TestInMemoTasksProcessor_Add(t *testing.T) {
	t.Run("add task with provided task ID", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(2, 10)
		require.NoError(t, err)

		taskID := "custom_task_id"
		taskCtx := NewTaskCtxWithID(time.Second*5, taskID)

		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {
			// Simple task function
		}

		processor.Add(taskFunc, taskCtx, "input1", "input2")

		// Verify task was added (we can't easily check the channel contents directly)
		assert.Equal(t, 1, processor.nextTaskID) // Should not increment when task ID is provided
	})

	t.Run("add task without task ID", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(2, 10)
		require.NoError(t, err)

		taskCtx := NewTaskCtx(time.Second * 5)

		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {
			// Simple task function
		}

		processor.Add(taskFunc, taskCtx, "input1")

		// Verify task ID was auto-generated and counter incremented
		assert.Equal(t, 2, processor.nextTaskID)
	})

	t.Run("add multiple tasks", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(2, 10)
		require.NoError(t, err)

		taskCtx := NewTaskCtx(time.Second * 5)
		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {}

		processor.Add(taskFunc, taskCtx, "input1")
		processor.Add(taskFunc, taskCtx, "input2")
		processor.Add(taskFunc, taskCtx, "input3")

		assert.Equal(t, 4, processor.nextTaskID)
	})
}

func TestInMemoTasksProcessor_Run(t *testing.T) {
	t.Run("run single task successfully", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(2, 10)
		require.NoError(t, err)

		executed := false
		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {
			executed = true
			ctx := imtpCtx.(*IMTPContext)

			unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
				return "unit result", nil
			}

			ctx.RunUnit(unitFunc, UnitOptions{}, inputs...)
		}

		taskCtx := NewTaskCtx(time.Second * 5)
		processor.Add(taskFunc, taskCtx, "test input")

		results := processor.Run()

		assert.True(t, executed)
		assert.Len(t, results, 1)
		assert.Equal(t, "task_1", results[0].TaskID)
		assert.NoError(t, results[0].Err)
		assert.Len(t, results[0].UnitResults, 1)
		assert.Equal(t, "unit result", results[0].UnitResults[0].Result)
	})

	t.Run("run multiple tasks concurrently", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(3, 10)
		require.NoError(t, err)

		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {
			ctx := imtpCtx.(*IMTPContext)

			unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
				// Remove timing dependency by doing quick work
				return inputs[0], nil
			}

			result := ctx.RunUnit(unitFunc, UnitOptions{}, inputs...)
			_ = result
		}

		taskCtx := NewTaskCtx(time.Second * 5)

		// Add multiple tasks
		for i := 0; i < 5; i++ {
			processor.Add(taskFunc, taskCtx, fmt.Sprintf("input_%d", i))
		}

		results := processor.Run()

		assert.Len(t, results, 5)

		// Verify all tasks completed successfully
		for _, result := range results {
			assert.NoError(t, result.Err)
			assert.Len(t, result.UnitResults, 1)
			assert.NoError(t, result.UnitResults[0].Err)
		}
	})

	t.Run("task with timeout", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(1, 10)
		require.NoError(t, err)

		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {
			ctx := imtpCtx.(*IMTPContext)

			unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
				// Check task context timeout instead of sleeping
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Millisecond * 100):
					return "should not complete", nil
				}
			}

			ctx.RunUnit(unitFunc, UnitOptions{Timeout: time.Second * 5}, inputs...)
		}

		// Set a short task timeout that will be exceeded
		taskCtx := NewTaskCtx(time.Millisecond * 20)
		processor.Add(taskFunc, taskCtx)

		results := processor.Run()

		assert.Len(t, results, 1)
		assert.Equal(t, "task_1", results[0].TaskID)
		assert.Error(t, results[0].Err)
		assert.Contains(t, results[0].Err.Error(), "task timeout exceeded")
	})

	t.Run("no tasks added", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(2, 10)
		require.NoError(t, err)

		results := processor.Run()

		assert.Len(t, results, 0)
	})
}

func TestInMemoTasksProcessor_processTask(t *testing.T) {
	t.Run("successful task processing", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(1, 10)
		require.NoError(t, err)

		executed := false
		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {
			executed = true
			ctx := imtpCtx.(*IMTPContext)

			unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
				return "processed", nil
			}

			ctx.RunUnit(unitFunc, UnitOptions{}, inputs...)
		}

		task := taskWrapper{
			id:       "test_task",
			taskFunc: taskFunc,
			ctx:      TaskCtx{Timeout: time.Second * 5},
			inputs:   []interface{}{"test_input"},
		}

		unitResults, err := processor.processTask(task)

		assert.True(t, executed)
		assert.NoError(t, err)
		assert.Len(t, unitResults, 1)
		assert.Equal(t, "test_taskunit1", unitResults[0].UnitID)
		assert.Equal(t, "processed", unitResults[0].Result)
	})

	t.Run("task processing with timeout", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(1, 10)
		require.NoError(t, err)

		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {
			ctx := imtpCtx.(*IMTPContext)

			unitFunc := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
				// Check if context is already cancelled
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
					// Sleep for much longer than task timeout to ensure timeout
					time.Sleep(time.Second * 2)
					return "completed", nil
				}
			}

			ctx.RunUnit(unitFunc, UnitOptions{}, inputs...)
		}

		task := taskWrapper{
			id:       "test_task",
			taskFunc: taskFunc,
			ctx:      TaskCtx{Timeout: time.Millisecond * 50},
			inputs:   []interface{}{"test_input"},
		}

		unitResults, err := processor.processTask(task)

		assert.Error(t, err)
		if err != nil {
			assert.Contains(t, err.Error(), "task timeout exceeded")
		}
		assert.NotNil(t, unitResults) // Should return partial results
	})
}

// Integration test combining multiple features
func TestInMemoTasksProcessor_Integration(t *testing.T) {
	t.Run("complex workflow with retries and multiple units", func(t *testing.T) {
		processor, err := NewInMemoTasksProcessor(2, 10)
		require.NoError(t, err)

		var callCount2 int // Counter needs to be accessible in closure
		taskFunc := func(imtpCtx interface{}, inputs ...interface{}) {
			ctx := imtpCtx.(*IMTPContext)

			// Unit 1: Successful unit
			unitFunc1 := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
				return "unit1_success", nil
			}
			ctx.RunUnit(unitFunc1, UnitOptions{Retries: 0}, "input1")

			// Unit 2: Unit with error (retries gets overwritten to 0 by applyDefaults)
			unitFunc2 := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
				callCount2++
				return nil, errors.New("permanent failure due to no retries")
			}
			ctx.RunUnit(unitFunc2, UnitOptions{Retries: 3}, "input2")

			// Unit 3: Non-retryable error
			unitFunc3 := func(ctx context.Context, inputs ...interface{}) (interface{}, error) {
				return nil, &NonRetryableError{Err: errors.New("permanent failure")}
			}
			ctx.RunUnit(unitFunc3, UnitOptions{Retries: 2}, "input3")
		}

		taskCtx := NewTaskCtx(time.Second * 10)
		processor.Add(taskFunc, taskCtx)

		results := processor.Run()

		require.Len(t, results, 1)
		result := results[0]

		assert.NoError(t, result.Err)
		assert.Len(t, result.UnitResults, 3)

		// Verify Unit 1
		assert.Equal(t, "task_1unit1", result.UnitResults[0].UnitID)
		assert.Equal(t, "unit1_success", result.UnitResults[0].Result)
		assert.NoError(t, result.UnitResults[0].Err)
		assert.Equal(t, 0, result.UnitResults[0].RetryCount)

		// Verify Unit 2  - fails because retries get set to 0 by applyDefaults
		assert.Equal(t, "task_1unit2", result.UnitResults[1].UnitID)
		assert.Nil(t, result.UnitResults[1].Result)
		assert.Error(t, result.UnitResults[1].Err)
		assert.Contains(t, result.UnitResults[1].Err.Error(), "permanent failure due to no retries")
		assert.Equal(t, 0, result.UnitResults[1].RetryCount)

		// Verify Unit 3
		assert.Equal(t, "task_1unit3", result.UnitResults[2].UnitID)
		assert.Nil(t, result.UnitResults[2].Result)
		assert.Error(t, result.UnitResults[2].Err)
		assert.Contains(t, result.UnitResults[2].Err.Error(), "permanent failure")
		assert.Equal(t, 0, result.UnitResults[2].RetryCount)
	})
}
