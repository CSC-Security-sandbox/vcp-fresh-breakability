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
	JobTypeCreatePool        JobType = "CREATE_POOL"
	JobTypeDeletePool        JobType = "DELETE_POOL"
	JobTypeCreateVolume      JobType = "CREATE_VOLUME"
	JobTypeDeleteVolume      JobType = "DELETE_VOLUME"
	JobTypeCreateSnapshot    JobType = "CREATE_SNAPSHOT"
	JobTypeDeleteSnapshot    JobType = "DELETE_SNAPSHOT"
	JobTypeAcceptClusterPeer JobType = "ACCEPT_CLUSTER_PEER"
)

// Job describes a job DB model
type Job struct {
	BaseModel
	CorrelationID string
	RequestID     string
	Type          JobType
	State         JobState
	StateDetails  string
	TrackingID    int
	ErrorDetails  []byte
	AccountID     sql.NullInt64
	IsAdminJob    bool
	JobAttributes []byte
	WorkflowID    string
	ScheduledAt   time.Time
}
