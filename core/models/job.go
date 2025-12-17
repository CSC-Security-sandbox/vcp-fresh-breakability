package models

import (
	"database/sql"
	"time"
)

type JobState string

const (
	JobsStateNEW                    JobState = "NEW"
	JobsStatePROCESSING             JobState = "PROCESSING"
	JobsStateERROR                  JobState = "ERROR"
	JobsStateDONE                   JobState = "DONE"
	JobsStateWaitForTemporal        JobState = "WAIT_FOR_TEMPORAL"
	JobsStateCANCELLED              JobState = "CANCELLED"
	WaitForTemporalJobMaxRetryCount          = 5
)

type JobType string

// ResourceOperation represents the type of operation being performed on a resource
type ResourceOperation string

// ResourceType represents the type of resource being operated on
type ResourceType string

const (
	// Resource operation types
	ResourceOperationCreate ResourceOperation = "CREATE"
	ResourceOperationUpdate ResourceOperation = "UPDATE"
	ResourceOperationDelete ResourceOperation = "DELETE"
)

const (
	// Resource types
	ResourceTypePool         ResourceType = "POOL"
	ResourceTypeSubnet       ResourceType = "SUBNET"
	ResourceTypeStringBucket              = "BUCKET"
)

type PoolCategory string

const (
	// Pool categories for extensible classification
	PoolCategoryStandard      PoolCategory = "standardPool"      // Standard/regular pools
	PoolCategoryLargeCapacity PoolCategory = "largeCapacityPool" // Large capacity pools
	PoolCategoryDefault       PoolCategory = "default"           // Default fallback (maps to standard)
)

const (
	JobTypeCreatePool      JobType = "CREATE_POOL"
	JobTypeCreateLargePool JobType = "CREATE_LARGE_POOL"
	JobTypeUpdatePool      JobType = "UPDATE_POOL"
	JobTypeUpdateLargePool JobType = "UPDATE_LARGE_POOL"
	JobTypeDeletePool      JobType = "DELETE_POOL"
	JobTypeDeleteLargePool JobType = "DELETE_LARGE_POOL"

	// We will use a single workflow for FC volume creation and it will handle creating/completing these jobs.
	// These 3 jobs are used to keep consistency with PO workflow/expectations.
	JobTypeFlexCacheCreateVolume     JobType = "FLEXCACHE_CREATE_VOLUME"
	JobTypeFlexCacheEstablishPeering JobType = "FLEXCACHE_ESTABLISH_PEERING"
	JobTypeFlexCacheInternalPeering  JobType = "FLEXCACHE_INTERNAL_PEERING"
	JobTypeFlexCacheDeleteVolume     JobType = "FLEXCACHE_DELETE_VOLUME"
	JobTypeFlexCachePrePopulate      JobType = "FLEXCACHE_PREPOPULATE"

	JobTypeCreateVolume                             JobType = "CREATE_VOLUME"
	JobTypeCreateLargeVolume                        JobType = "CREATE_LARGE_VOLUME"
	JobTypeUpdateVolume                             JobType = "UPDATE_VOLUME"
	JobTypeUpdateVolumeInReplication                JobType = "UPDATE_VOLUME_IN_REPLICATION"
	JobTypeRevertVolume                             JobType = "REVERT_VOLUME"
	JobTypeDeleteVolume                             JobType = "DELETE_VOLUME"
	JobTypeDeleteLargeVolume                        JobType = "DELETE_LARGE_VOLUME"
	JobTypeCreateSnapshot                           JobType = "CREATE_SNAPSHOT"
	JobTypeUpdateSnapshot                           JobType = "UPDATE_SNAPSHOT"
	JobTypeDeleteSnapshot                           JobType = "DELETE_SNAPSHOT"
	JobTypeCreateQuotaRule                          JobType = "CREATE_QUOTA_RULE"
	JobTypeUpdateQuotaRule                          JobType = "UPDATE_QUOTA_RULE"
	JobTypeRestoreBackup                            JobType = "RESTORE_BACKUP"
	JobTypeRestoreFilesBackup                       JobType = "RESTORE_FILES_BACKUP"
	JobTypeAcceptClusterPeer                        JobType = "ACCEPT_CLUSTER_PEER"
	JobTypeUpdateKmsConfig                          JobType = "UPDATE_KMS_CONFIG"
	JobTypeCreateKmsConfig                          JobType = "CREATE_KMS_CONFIG"
	JobTypeDeleteKmsConfig                          JobType = "DELETE_KMS_CONFIG"
	JobTypeSdeKmsCreate                             JobType = "SDE_KMS_CREATE"
	JobTypeMigrateKmsConfig                         JobType = "MIGRATE_KMS_CONFIG"
	JobTypeRotateKmsConfig                          JobType = "ROTATE_KMS_CONFIG"
	JobTypeCreateVolumeReplication                  JobType = "CREATE_VOLUME_REPLICATION"
	JobTypeCreateVolumeReplicationInternal          JobType = "CREATE_VOLUME_REPLICATION_INTERNAL"
	JobTypeDeleteVolumeReplicationInternal          JobType = "DELETE_VOLUME_REPLICATION_INTERNAL"
	JobTypeUpdateVolumeReplicationInternal          JobType = "UPDATE_VOLUME_REPLICATION_INTERNAL"
	JobTypeCreateBackupVault                        JobType = "CREATE_BACKUP_VAULT"
	JobTypeDeleteVolumeReplication                  JobType = "DELETE_VOLUME_REPLICATION"
	JobTypeUpdateVolumeReplication                  JobType = "UPDATE_VOLUME_REPLICATION"
	JobTypeResumeVolumeReplication                  JobType = "RESUME_VOLUME_REPLICATION"
	JobTypeResumeVolumeReplicationInternal          JobType = "RESUME_VOLUME_REPLICATION_INTERNAL"
	JobTypeReverseVolumeReplicationInternal         JobType = "REVERSE_VOLUME_REPLICATION_INTERNAL"
	JobTypeSyncVolumeReplication                    JobType = "SYNC_VOLUME_REPLICATION"
	JobTypeReverseResumeVolumeReplication           JobType = "REVERSE_RESUME_VOLUME_REPLICATION"
	JobTypeUpdateVolumeReplicationAttributes        JobType = "UPDATE_VOLUME_REPLICATION_ATTRIBUTES"
	JobTypeStopVolumeReplication                    JobType = "STOP_VOLUME_REPLICATION"
	JobTypeStopVolumeReplicationInternal            JobType = "STOP_VOLUME_REPLICATION_INTERNAL"
	JobTypeRefreshVolumeReplicationInternal         JobType = "REFRESH_VOLUME_REPLICATION_INTERNAL"
	JobTypeCreateBackup                             JobType = "CREATE_BACKUP"
	JobTypeDeleteBackup                             JobType = "DELETE_BACKUP"
	JobTypeUpdateHostGroup                          JobType = "UPDATE_HOSTGROUP"
	JobTypeMountCheck                               JobType = "MOUNT_VOLUME_REPLICATION_INTERNAL"
	JobTypeRefreshAdminJobSpecs                     JobType = "REFRESH_ADMIN_JOB_SPECS"
	JobTypeStartProjectEventOffState                JobType = "START_PROJECT_EVENT_OFF_STATE"
	JobTypeStartProjectEventOnState                 JobType = "START_PROJECT_EVENT_ON_STATE"
	JobTypeFinishProjectEventDeleteState            JobType = "FINISH_PROJECT_EVENT_DELETE_STATE"
	JobTypeReleaseVolumeReplicationInternal         JobType = "RELEASE_VOLUME_REPLICATION_INTERNAL"
	JobTypeUpdateBackupVault                        JobType = "UPDATE_BACKUP_VAULT"
	JobTypeDeleteSnapmirrorSnapshotsInternal        JobType = "DELETE_SM_SNAPSHOTS_INTERNAL"
	JobTypeCreateSubnet                             JobType = "CREATE_SUBNET"
	JobTypeDeleteSubnet                             JobType = "DELETE_SUBNET"
	JobTypeCreateLargeSubnet                        JobType = "CREATE_LARGE_SUBNET"
	JobTypeHandleResourceEvent                      JobType = "HANDLE_RESOURCE_EVENT"
	JobTypeHandleResourceEventOffState              JobType = "HANDLE_RESOURCE_EVENT_OFF_STATE"
	JobTypeHandleResourceEventOnState               JobType = "HANDLE_RESOURCE_EVENT_ON_STATE"
	JobTypeHandleResourceEventDeleteState           JobType = "HANDLE_RESOURCE_EVENT_DELETE_STATE"
	JobTypeDeleteBackupVault                        JobType = "DELETE_BACKUP_VAULT"
	JobTypeInitCreateScheduledBackup                JobType = "INIT_CREATE_SCHEDULED_BACKUP"
	JobTypeCreateScheduledBackup                    JobType = "CREATE_SCHEDULED_BACKUP"
	JobTypeDeleteScheduledBackup                    JobType = "DELETE_SCHEDULED_BACKUP"
	JobTypeRefreshVolumeFields                      JobType = "REFRESH_VOLUME_FIELDS"
	JobTypeUpdateBackup                             JobType = "UPDATE_BACKUP"
	JobTypeUpdateBackupPolicy                       JobType = "UPDATE_BACKUP_POLICY"
	JobTypeDeleteBackupPolicy                       JobType = "DELETE_BACKUP_POLICY"
	JobTypeCreateActiveDirectory                    JobType = "CREATE_ACTIVE_DIRECTORY"
	JobTypeUpdateActiveDirectory                    JobType = "UPDATE_ACTIVE_DIRECTORY"
	JobTypeDeleteActiveDirectory                    JobType = "DELETE_ACTIVE_DIRECTORY"
	JobTypeSplitVolume                              JobType = "SPLIT_CLONE_VOLUME"
	JobTypeCreateHybridReplication                  JobType = "CREATE_HYBRID_REPLICATION"
	JobTypeHybridReplicationEstablishPeering        JobType = "HYBRID_REPLICATION_ESTABLISH_PEERING"
	JobTypeHybridReplicationInternalEstablish       JobType = "HYBRID_REPLICATION_INTERNAL_ESTABLISH"
	JobTypeReverseHybridReplicationInternal         JobType = "HYBRID_REPLICATION_INTERNAL_REVERSE"
	JobTypeReverseHybridReplicationFallbackInternal JobType = "HYBRID_REPLICATION_INTERNAL_REVERSE_FALLBACK"
	JobTypeCreateExpertModeVolume                   JobType = "RECONCILE_EXPERT_MODE_VOLUME_CREATE"
)

// GetResourceJobType returns the appropriate job type based on the resource type, operation, and pool category
func GetResourceJobType(resourceType ResourceType, operation ResourceOperation, poolCategory PoolCategory) JobType {
	// Handle default category by mapping to standard pool
	if poolCategory == PoolCategoryDefault {
		poolCategory = PoolCategoryStandard
	}

	// Define the job type mapping based on resource type, operation, and pool category
	// This extensible design allows adding new pool categories without breaking existing code
	jobTypeMap := map[ResourceType]map[ResourceOperation]map[PoolCategory]JobType{
		ResourceTypePool: {
			ResourceOperationCreate: {
				PoolCategoryStandard:      JobTypeCreatePool,      // Standard pool create
				PoolCategoryLargeCapacity: JobTypeCreateLargePool, // Large capacity pool create
			},
			ResourceOperationUpdate: {
				PoolCategoryStandard:      JobTypeUpdatePool,      // Standard pool update
				PoolCategoryLargeCapacity: JobTypeUpdateLargePool, // Large capacity pool update
			},
			ResourceOperationDelete: {
				PoolCategoryStandard:      JobTypeDeletePool,      // Standard pool delete
				PoolCategoryLargeCapacity: JobTypeDeleteLargePool, // Large capacity pool delete
			},
		},
		ResourceTypeSubnet: {
			ResourceOperationCreate: {
				PoolCategoryStandard:      JobTypeCreateSubnet,      // Standard subnet create
				PoolCategoryLargeCapacity: JobTypeCreateLargeSubnet, // Large capacity subnet create
			},
			ResourceOperationDelete: {
				PoolCategoryStandard: JobTypeDeleteSubnet, // Standard subnet delete
				// TODO: adding subnet delete support for Large volumes . PoolCategoryLargeCapacity: JobTypeDeleteLargeSubnet, // Large capacity subnet delete
			},
			// Note: Subnets only support CREATE operations currently
			// Future operations can be added here as needed
		},
	}

	// Get the job type from the mapping
	if resourceMap, exists := jobTypeMap[resourceType]; exists {
		if operationMap, exists := resourceMap[operation]; exists {
			if jobType, exists := operationMap[poolCategory]; exists {
				return jobType
			}
		}
	}

	// Default fallback (should not reach here with valid inputs)
	return JobTypeCreatePool
}

// GetPoolCategory is a concise helper function that maps boolean capacity to PoolCategory
func GetPoolCategory(isLargeCapacity bool) PoolCategory {
	if isLargeCapacity {
		return PoolCategoryLargeCapacity
	}
	return PoolCategoryStandard
}

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
	Location     string
}
