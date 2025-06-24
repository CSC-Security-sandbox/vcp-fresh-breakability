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
	JobTypeCreatePool                       JobType = "CREATE_POOL"
	JobTypeUpdatePool                       JobType = "UPDATE_POOL"
	JobTypeDeletePool                       JobType = "DELETE_POOL"
	JobTypeCreateVolume                     JobType = "CREATE_VOLUME"
	JobTypeUpdateVolume                     JobType = "UPDATE_VOLUME"
	JobTypeDeleteVolume                     JobType = "DELETE_VOLUME"
	JobTypeCreateSnapshot                   JobType = "CREATE_SNAPSHOT"
	JobTypeUpdateSnapshot                   JobType = "UPDATE_SNAPSHOT"
	JobTypeDeleteSnapshot                   JobType = "DELETE_SNAPSHOT"
	JobTypeAcceptClusterPeer                JobType = "ACCEPT_CLUSTER_PEER"
	JobTypeUpdateKmsConfig                  JobType = "UPDATE_KMS_CONFIG"
	JobTypeCreateKmsConfig                  JobType = "CREATE_KMS_CONFIG"
	JobTypeCreateVolumeReplication          JobType = "CREATE_VOLUME_REPLICATION"
	JobTypeCreateVolumeReplicationInternal  JobType = "CREATE_VOLUME_REPLICATION_INTERNAL"
	JobTypeCreateBackupVault                JobType = "CREATE_BACKUP_VAULT"
	JobTypeDeleteVolumeReplication          JobType = "DELETE_VOLUME_REPLICATION"
	JobTypeUpdateVolumeReplication          JobType = "UPDATE_VOLUME_REPLICATION"
	JobTypeResumeVolumeReplication          JobType = "RESUME_VOLUME_REPLICATION"
	JobTypeReverseResumeVolumeReplication   JobType = "REVERSE_RESUME_VOLUME_REPLICATION"
	JobTypeStopVolumeReplication            JobType = "STOP_VOLUME_REPLICATION"
	JobTypeRefreshVolumeReplicationInternal JobType = "REFRESH_VOLUME_REPLICATION_INTERNAL"
	JobTypeCreateBackup                     JobType = "CREATE_BACKUP"
	JobTypeMountCheck                       JobType = "MOUNT_VOLUME_REPLICATION_INTERNAL"
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
	JobAttributes *JobAttributes
	WorkflowID    string
	ScheduledAt   time.Time
}
type JobAttributes struct {
	ResourceUUID string
	PoolUUID     string
}
