package scheduler

import (
	"context"
	"go.temporal.io/sdk/client"

	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// TemporalScheduler manages schedules using Temporal as the backend.
// It implements the Scheduler interface.
type TemporalScheduler struct {
	schedulerClient client.ScheduleClient
}

// TemporalCreateScheduleParams defines options for creating a new Temporal schedule.
//
// WorkflowID: Unique identifier for the workflow to be scheduled.
// Workflow: The workflow function or type to be scheduled.
// Spec: Schedule specification, such as intervals or cron expressions.
type TemporalCreateScheduleParams struct {
	WorkflowID string
	Workflow   interface{}
	Spec       client.ScheduleSpec
}

// TemporalUpdateScheduleParams defines options for updating an existing Temporal schedule.
//
// Spec: The updated schedule specification.
type TemporalUpdateScheduleParams struct {
	Spec client.ScheduleSpec
}

// TemporalDeleteScheduleParams defines options for deleting a Temporal schedule.
// Currently empty, reserved for future use.
type TemporalDeleteScheduleParams struct{}

// NewTemporalScheduler creates a new TemporalScheduler using the provided ScheduleClient.
//
// schedulerClient: The Temporal ScheduleClient used to manage schedules.
// Returns a pointer to a TemporalScheduler.
func NewTemporalScheduler(schedulerClient client.ScheduleClient) *TemporalScheduler {
	return &TemporalScheduler{schedulerClient: schedulerClient}
}

// Create creates a new schedule in Temporal using the provided parameters.
//
// ctx: Context for request-scoped values and cancellation.
// params: Parameters for schedule creation, including workflow and schedule spec.
// Returns a pointer to ScheduleResponse on success, or an error if creation fails.
func (temporalScheduler TemporalScheduler) Create(ctx context.Context, params CreateScheduleParams) (*ScheduleResponse, error) {
	logger := util.GetLogger(ctx)
	temporalArgs := params.TemporalScheduleOptions

	maxRetries := params.MaxRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}

	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		scheduleHandler, err := temporalScheduler.schedulerClient.Create(ctx, client.ScheduleOptions{
			ID:   params.ScheduleID,
			Spec: temporalArgs.Spec,
			Action: &client.ScheduleWorkflowAction{
				ID:        temporalArgs.WorkflowID,
				TaskQueue: workflowengine.BackgroundTaskQueue,
				Workflow:  temporalArgs.Workflow,
				Args:      params.Args,
			},
		})

		if err == nil {
			logger.Info("Schedule created successfully")
			return &ScheduleResponse{
				ID:     scheduleHandler.GetID(),
				Status: ScheduleStateActive,
			}, nil
		}

		lastErr = err
		logger.Error("Failed to create schedule", "error", err, "attempt", attempt)
	}

	logger.Error("ALERT: Failed to create schedule after multiple attempts", "error", lastErr)
	return nil, lastErr
}

// Update modifies an existing schedule in Temporal using the provided parameters.
//
// ctx: Context for request-scoped values and cancellation.
// params: Parameters for schedule update, including the new schedule spec.
// Returns a pointer to ScheduleResponse on success, or an error if update fails.
func (temporalScheduler TemporalScheduler) Update(ctx context.Context, params UpdateScheduleParams) (*ScheduleResponse, error) {
	logger := util.GetLogger(ctx)
	temporalArgs := params.TemporalScheduleOptions

	maxRetries := params.MaxRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		scheduleHandler := temporalScheduler.schedulerClient.GetHandle(ctx, params.ScheduleID)
		err := scheduleHandler.Update(ctx, client.ScheduleUpdateOptions{
			DoUpdate: func(schedule client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
				schedule.Description.Schedule.Spec = &temporalArgs.Spec
				return &client.ScheduleUpdate{
					Schedule: &schedule.Description.Schedule,
				}, nil
			},
		})

		if err == nil {
			logger.Info("Schedule updated successfully")
			return &ScheduleResponse{
				ID:     params.ScheduleID,
				Status: ScheduleStateActive,
			}, nil
		}

		lastErr = err
		logger.Error("Failed to update schedule", "error", err, "attempt", attempt)
	}

	logger.Error("ALERT: Failed to update schedule after multiple attempts", "error", lastErr)
	return nil, lastErr
}

// Delete removes an existing schedule in Temporal using the provided parameters.
//
// ctx: Context for request-scoped values and cancellation.
// params: Parameters for schedule deletion.
// Returns a pointer to ScheduleResponse on success, or an error if deletion fails.
func (temporalScheduler TemporalScheduler) Delete(ctx context.Context, params DeleteScheduleParams) (*ScheduleResponse, error) {
	logger := util.GetLogger(ctx)

	maxRetries := params.MaxRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		scheduleHandler := temporalScheduler.schedulerClient.GetHandle(ctx, params.ScheduleID)
		err := scheduleHandler.Delete(ctx)
		if err == nil {
			logger.Info("Schedule deleted successfully")
			return &ScheduleResponse{
				ID:     params.ScheduleID,
				Status: ScheduleStateDeleted,
			}, nil
		}

		lastErr = err
		logger.Error("Failed to delete schedule", "error", err, "attempt", attempt)
	}

	logger.Error("ALERT: Failed to delete schedule after multiple attempts", "error", lastErr)
	return nil, lastErr
}
