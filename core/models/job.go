package models

import (
	"database/sql"
	"time"
)

type JobState string

const (
	JobsStateNEW        JobState = "NEW"
	JobsStatePROCESSING JobState = "PROCESSING"
	JobsStateERROR      JobState = "ERROR"
	JobsStateDONE       JobState = "DONE"
)

type JobType string

const (
	JobTypeCreatePool JobType = "CREATE_POOL"
)

// Job describes a job DB model
type Job struct {
	BaseModel
	CorrelationID string
	RequestID     string
	Type          JobType
	State         JobState
	StateDetails  string
	ErrorDetails  []byte
	AccountID     sql.NullInt64
	IsAdminJob    bool
	JobAttributes []byte
	WorkflowID    string
	ScheduledAt   time.Time
}
