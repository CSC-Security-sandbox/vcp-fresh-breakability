package scheduler

import (
	"context"
)

// ScheduleState defines the possible states of a schedule.
type ScheduleState string

const (
	// ScheduleStateActive means the schedule is currently active.
	ScheduleStateActive ScheduleState = "ACTIVE"
	// ScheduleStateDeleted means the schedule has been deleted or is inactive.
	ScheduleStateDeleted ScheduleState = "DELETED"
	// ScheduleStatePaused means the schedule is currently paused.
	ScheduleStatePaused ScheduleState = "PAUSED"
)

const DefaultMaxRetries = 3

// ScheduleParams holds common parameters for schedule operations.
//
// Args is a generic argument list passed to background jobs. While currently used with Temporal,
// it is designed to be compatible with other background job systems (e.g., Airflow) in the future.
// Only parameters common to all background job types should be included here.
type ScheduleParams struct {
	ScheduleID string
	Args       []interface{}
	// MaxRetries specifies the maximum number of retries for schedule operations.
	MaxRetries int
}

// CreateScheduleParams contains parameters required to create a schedule.
type CreateScheduleParams struct {
	ScheduleParams
	TemporalScheduleOptions TemporalCreateScheduleParams
}

// UpdateScheduleParams contains parameters required to update a schedule.
type UpdateScheduleParams struct {
	ScheduleParams
	TemporalScheduleOptions TemporalUpdateScheduleParams
}

// DeleteScheduleParams contains parameters required to delete a schedule.
type DeleteScheduleParams struct {
	ScheduleParams
	TemporalScheduleOptions TemporalDeleteScheduleParams
}

// PauseScheduleParams contains parameters required to pause a schedule.
type PauseScheduleParams struct {
	ScheduleParams
	TemporalScheduleOptions TemporalPauseScheduleParams
}

// UnpauseScheduleParams contains parameters required to pause a schedule.
type UnpauseScheduleParams struct {
	ScheduleParams
	TemporalScheduleOptions TemporalUnpauseScheduleParams
}

// DescribeScheduleParams contains parameters required to describe a schedule.
type DescribeScheduleParams struct {
	ScheduleParams
}

// ScheduleResponse represents the result of a schedule operation.
type ScheduleResponse struct {
	ID     string
	Status ScheduleState
}

// ScheduleDescription represents detailed information about a schedule.
type ScheduleDescription struct {
	ID     string
	Paused bool
}

// Scheduler provides an interface for managing schedules.
type Scheduler interface {
	// Create creates a new schedule with the given parameters.
	Create(ctx context.Context, params CreateScheduleParams) (*ScheduleResponse, error)
	// Update modifies an existing schedule with the given parameters.
	Update(ctx context.Context, params UpdateScheduleParams) (*ScheduleResponse, error)
	// Delete removes an existing schedule with the given parameters.
	Delete(ctx context.Context, params DeleteScheduleParams) (*ScheduleResponse, error)
	// Pause pauses an existing schedule with the given parameters.
	Pause(ctx context.Context, params PauseScheduleParams) (*ScheduleResponse, error)
	// Unpause pauses an existing schedule with the given parameters.
	Unpause(ctx context.Context, params UnpauseScheduleParams) (*ScheduleResponse, error)
	// Describe retrieves the current state and information of an existing schedule.
	Describe(ctx context.Context, params DescribeScheduleParams) (*ScheduleDescription, error)
}
