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

// ScheduleResponse represents the result of a schedule operation.
type ScheduleResponse struct {
	ID     string
	Status ScheduleState
}

// Scheduler provides an interface for managing schedules.
type Scheduler interface {
	// Create creates a new schedule with the given parameters.
	Create(ctx context.Context, params CreateScheduleParams) (*ScheduleResponse, error)
	// Update modifies an existing schedule with the given parameters.
	Update(ctx context.Context, params UpdateScheduleParams) (*ScheduleResponse, error)
	// Delete removes an existing schedule with the given parameters.
	Delete(ctx context.Context, params DeleteScheduleParams) (*ScheduleResponse, error)
}
