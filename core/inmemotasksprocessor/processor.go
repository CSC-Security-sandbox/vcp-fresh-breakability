package inmemotasksprocessor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	MaxWorkers    = 10000
	MinWorkers    = 1
	MinBufferSize = 1
)

type NonRetryableError struct {
	Err error
}

func (e NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e NonRetryableError) Unwrap() error {
	return e.Err
}

// UnitOptions configures retry and timeout behavior for unit execution
type UnitOptions struct {
	Retries int           // Number of retry attempts (default: 0 - no retries)
	Timeout time.Duration // Timeout for unit execution (default: 0 - no timeout)
}

// applyDefaults sets default values for any zero/empty fields
func (uo *UnitOptions) applyDefaults() {
	uo.Timeout = time.Second * 30 // Default timeout of 30 seconds
	uo.Retries = 0
}

type IMTPContext struct {
	ctx         context.Context
	taskID      string
	unitResults []UnitResult
	nextUnitID  int
	mutex       sync.Mutex
}

// RunUnit executes a unit function with configurable options
func (ic *IMTPContext) RunUnit(unitFunc func(ctx context.Context, inputs ...interface{}) (interface{}, error), options UnitOptions, inputs ...interface{}) UnitResult {
	options.applyDefaults()

	ic.mutex.Lock()
	unitNum := ic.nextUnitID
	ic.nextUnitID++
	ic.mutex.Unlock()

	// Create UnitID in format: taskIdunitId (e.g., "task_1unit1")
	unitID := fmt.Sprintf("%sunit%d", ic.taskID, unitNum)

	ctx, cancel := context.WithTimeout(ic.ctx, options.Timeout)
	defer cancel()

	done := make(chan struct {
		result     interface{}
		err        error
		retryCount int
	})

	go func() {
		var result interface{}
		var err error
		retryCount := 0

		for attempt := 0; attempt <= options.Retries; attempt++ {
			select {
			case <-ctx.Done():
				done <- struct {
					result     interface{}
					err        error
					retryCount int
				}{nil, ctx.Err(), retryCount}
				return
			default:
				result, err = unitFunc(ctx, inputs...)
				if err == nil {
					done <- struct {
						result     interface{}
						err        error
						retryCount int
					}{result, nil, retryCount}
					return
				}
				var nonRetryErr *NonRetryableError
				if errors.As(err, &nonRetryErr) {
					done <- struct {
						result     interface{}
						err        error
						retryCount int
					}{nil, err, retryCount}
					return
				}
				if attempt < options.Retries {
					retryCount++
					time.Sleep(time.Second)
				}
			}
		}

		done <- struct {
			result     interface{}
			err        error
			retryCount int
		}{nil, err, retryCount}
	}()

	select {
	case <-ctx.Done():
		unitResult := UnitResult{
			UnitID:     unitID,
			Result:     nil,
			Err:        ctx.Err(),
			RetryCount: 0,
		}
		ic.mutex.Lock()
		ic.unitResults = append(ic.unitResults, unitResult)
		ic.mutex.Unlock()
		return unitResult
	case res := <-done:
		unitResult := UnitResult{
			UnitID:     unitID,
			Result:     res.result,
			Err:        res.err,
			RetryCount: res.retryCount,
		}
		ic.mutex.Lock()
		ic.unitResults = append(ic.unitResults, unitResult)
		ic.mutex.Unlock()
		return unitResult
	}
}

type taskWrapper struct {
	id       string
	taskFunc TaskFunc
	ctx      TaskCtx
	inputs   []interface{}
}

type InMemoTasksProcessor struct {
	tasks      chan taskWrapper
	results    chan TaskResult
	wg         sync.WaitGroup
	numWorkers int
	nextTaskID int
	mu         sync.Mutex
}

func NewInMemoTasksProcessor(numWorkers, bufferSize int) (*InMemoTasksProcessor, error) {
	if numWorkers < MinWorkers {
		return nil, fmt.Errorf("numWorkers must be at least %d, got %d", MinWorkers, numWorkers)
	}

	if numWorkers > MaxWorkers {
		return nil, fmt.Errorf("numWorkers exceeds maximum limit of %d, got %d", MaxWorkers, numWorkers)
	}

	if bufferSize < MinBufferSize {
		return nil, fmt.Errorf("bufferSize must be at least %d, got %d", MinBufferSize, bufferSize)
	}

	return &InMemoTasksProcessor{
		tasks:      make(chan taskWrapper, bufferSize),
		results:    make(chan TaskResult, bufferSize),
		numWorkers: numWorkers,
		nextTaskID: 1,
	}, nil
}

func (imtp *InMemoTasksProcessor) Add(taskFuc TaskFunc, ctx TaskCtx, inputs ...interface{}) {
	imtp.mu.Lock()
	var taskID string
	if ctx.TaskID != nil {
		taskID = *ctx.TaskID
	} else {
		taskID = fmt.Sprintf("task_%d", imtp.nextTaskID)
		imtp.nextTaskID++
	}
	imtp.mu.Unlock()

	imtp.tasks <- taskWrapper{
		id:       taskID,
		taskFunc: taskFuc,
		ctx:      ctx,
		inputs:   inputs,
	}
}

func (imtp *InMemoTasksProcessor) Run() []TaskResult {
	imtp.wg.Add(imtp.numWorkers)
	for i := 0; i < imtp.numWorkers; i++ {
		go imtp.worker()
	}

	close(imtp.tasks)

	go func() {
		imtp.wg.Wait()
		close(imtp.results)
	}()

	var allResults []TaskResult
	for res := range imtp.results {
		allResults = append(allResults, res)
	}

	return allResults
}

func (imtp *InMemoTasksProcessor) worker() {
	defer imtp.wg.Done()
	for task := range imtp.tasks {
		result, err := imtp.processTask(task)
		imtp.results <- TaskResult{TaskID: task.id, UnitResults: result, Err: err}
	}
}

func (imtp *InMemoTasksProcessor) processTask(task taskWrapper) ([]UnitResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), task.ctx.Timeout)
	defer cancel()

	imtpCtx := &IMTPContext{
		ctx:         ctx,
		taskID:      task.id,
		unitResults: []UnitResult{},
		nextUnitID:  1,
	}

	done := make(chan struct {
		unitResults []UnitResult
		err         error
	})

	go func() {
		task.taskFunc(imtpCtx, task.inputs...)
		imtpCtx.mutex.Lock()
		results := make([]UnitResult, len(imtpCtx.unitResults))
		copy(results, imtpCtx.unitResults)
		imtpCtx.mutex.Unlock()
		done <- struct {
			unitResults []UnitResult
			err         error
		}{results, nil}
	}()

	select {
	case <-ctx.Done():
		imtpCtx.mutex.Lock()
		results := make([]UnitResult, len(imtpCtx.unitResults))
		copy(results, imtpCtx.unitResults)
		imtpCtx.mutex.Unlock()
		return results, errors.New("task timeout exceeded")
	case res := <-done:
		return res.unitResults, res.err
	}
}
