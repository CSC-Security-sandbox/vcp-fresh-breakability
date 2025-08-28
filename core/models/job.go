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
	JobTypeUpdatePool JobType = "UPDATE_POOL"
	JobTypeDeletePool JobType = "DELETE_POOL"

	// We will use a single workflow for FC volume creation and it will handle creating/completing these jobs.
	// These 3 jobs are used to keep consistency with PO workflow/expectations.
	JobTypeFlexCacheCreateVolume     JobType = "FLEXCACHE_CREATE_VOLUME"
	JobTypeFlexCacheEstablishPeering JobType = "FLEXCACHE_ESTABLISH_PEERING"
	JobTypeFlexCacheInternalPeering  JobType = "FLEXCACHE_INTERNAL_PEERING"

	JobTypeCreateVolume                      JobType = "CREATE_VOLUME"
	JobTypeUpdateVolume                      JobType = "UPDATE_VOLUME"
	JobTypeRevertVolume                      JobType = "REVERT_VOLUME"
	JobTypeDeleteVolume                      JobType = "DELETE_VOLUME"
	JobTypeCreateSnapshot                    JobType = "CREATE_SNAPSHOT"
	JobTypeUpdateSnapshot                    JobType = "UPDATE_SNAPSHOT"
	JobTypeDeleteSnapshot                    JobType = "DELETE_SNAPSHOT"
	JobTypeRestoreBackup                     JobType = "RESTORE_BACKUP"
	JobTypeAcceptClusterPeer                 JobType = "ACCEPT_CLUSTER_PEER"
	JobTypeUpdateKmsConfig                   JobType = "UPDATE_KMS_CONFIG"
	JobTypeCreateKmsConfig                   JobType = "CREATE_KMS_CONFIG"
	JobTypeDeleteKmsConfig                   JobType = "DELETE_KMS_CONFIG"
	JobTypeMigrateKmsConfig                  JobType = "MIGRATE_KMS_CONFIG"
	JobTypeRotateKmsConfig                   JobType = "ROTATE_KMS_CONFIG"
	JobTypeCreateVolumeReplication           JobType = "CREATE_VOLUME_REPLICATION"
	JobTypeCreateVolumeReplicationInternal   JobType = "CREATE_VOLUME_REPLICATION_INTERNAL"
	JobTypeDeleteVolumeReplicationInternal   JobType = "DELETE_VOLUME_REPLICATION_INTERNAL"
	JobTypeUpdateVolumeReplicationInternal   JobType = "UPDATE_VOLUME_REPLICATION_INTERNAL"
	JobTypeCreateBackupVault                 JobType = "CREATE_BACKUP_VAULT"
	JobTypeDeleteVolumeReplication           JobType = "DELETE_VOLUME_REPLICATION"
	JobTypeUpdateVolumeReplication           JobType = "UPDATE_VOLUME_REPLICATION"
	JobTypeResumeVolumeReplication           JobType = "RESUME_VOLUME_REPLICATION"
	JobTypeResumeVolumeReplicationInternal   JobType = "RESUME_VOLUME_REPLICATION_INTERNAL"
	JobTypeSyncVolumeReplication             JobType = "SYNC_VOLUME_REPLICATION"
	JobTypeReverseResumeVolumeReplication    JobType = "REVERSE_RESUME_VOLUME_REPLICATION"
	JobTypeStopVolumeReplication             JobType = "STOP_VOLUME_REPLICATION"
	JobTypeStopVolumeReplicationInternal     JobType = "STOP_VOLUME_REPLICATION_INTERNAL"
	JobTypeRefreshVolumeReplicationInternal  JobType = "REFRESH_VOLUME_REPLICATION_INTERNAL"
	JobTypeCreateBackup                      JobType = "CREATE_BACKUP"
	JobTypeDeleteBackup                      JobType = "DELETE_BACKUP"
	JobTypeUpdateHostGroup                   JobType = "UPDATE_HOSTGROUP"
	JobTypeMountCheck                        JobType = "MOUNT_VOLUME_REPLICATION_INTERNAL"
	JobTypeRefreshAdminJobSpecs              JobType = "REFRESH_ADMIN_JOB_SPECS"
	JobTypeStartProjectEventOffState         JobType = "START_PROJECT_EVENT_OFF_STATE"
	JobTypeStartProjectEventOnState          JobType = "START_PROJECT_EVENT_ON_STATE"
	JobTypeFinishProjectEventDeleteState     JobType = "FINISH_PROJECT_EVENT_DELETE_STATE"
	JobTypeReleaseVolumeReplicationInternal  JobType = "RELEASE_VOLUME_REPLICATION_INTERNAL"
	JobTypeUpdateBackupVault                 JobType = "UPDATE_BACKUP_VAULT"
	JobTypeDeleteSnapmirrorSnapshotsInternal JobType = "DELETE_SM_SNAPSHOTS_INTERNAL"
	JobTypeCreateSubnet                      JobType = "CREATE_SUBNET"
	JobTypeHandleResourceEvent               JobType = "HANDLE_RESOURCE_EVENT"
	JobTypeHandleResourceEventOffState       JobType = "HANDLE_RESOURCE_EVENT_OFF_STATE"
	JobTypeHandleResourceEventOnState        JobType = "HANDLE_RESOURCE_EVENT_ON_STATE"
	JobTypeDeleteBackupVault                 JobType = "DELETE_BACKUP_VAULT"
	JobTypeInitCreateScheduledBackup         JobType = "INIT_CREATE_SCHEDULED_BACKUP"
	JobTypeCreateScheduledBackup             JobType = "CREATE_SCHEDULED_BACKUP"
	JobTypeDeleteScheduledBackup             JobType = "DELETE_SCHEDULED_BACKUP"
	JobTypeRefreshVolumeFields               JobType = "REFRESH_VOLUME_FIELDS"
	JobTypeUpdateBackup                      JobType = "UPDATE_BACKUP"
	JobTypeUpdateBackupPolicy                JobType = "UPDATE_BACKUP_POLICY"
	JobTypeDeleteBackupPolicy                JobType = "DELETE_BACKUP_POLICY"
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
	ResourceName  string
}
type JobAttributes struct {
	ResourceUUID string
	PoolUUID     string
}
