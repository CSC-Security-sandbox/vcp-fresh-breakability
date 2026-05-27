package models

import (
	"database/sql"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
)

type JobState = datamodel.JobState

const (
	JobsStateNEW                    JobState = datamodel.JobsStateNEW
	JobsStatePROCESSING             JobState = datamodel.JobsStatePROCESSING
	JobsStateERROR                  JobState = datamodel.JobsStateERROR
	JobsStateDONE                   JobState = datamodel.JobsStateDONE
	JobsStateWaitForTemporal        JobState = datamodel.JobsStateWaitForTemporal
	JobsStateCANCELLED              JobState = datamodel.JobsStateCANCELLED
	WaitForTemporalJobMaxRetryCount          = 5
)

type JobType = datamodel.JobType

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
	JobTypeCreatePool      JobType = datamodel.JobTypeCreatePool
	JobTypeCreateLargePool JobType = datamodel.JobTypeCreateLargePool
	JobTypeUpdatePool      JobType = datamodel.JobTypeUpdatePool
	JobTypeUpdateLargePool JobType = datamodel.JobTypeUpdateLargePool
	JobTypeDeletePool      JobType = datamodel.JobTypeDeletePool
	JobTypeDeleteLargePool JobType = datamodel.JobTypeDeleteLargePool
	JobTypeCreateSvm       JobType = datamodel.JobTypeCreateSvm
	JobTypeDeleteSvm       JobType = datamodel.JobTypeDeleteSvm

	// We will use a single workflow for FC volume creation and it will handle creating/completing these jobs.
	// These 3 jobs are used to keep consistency with PO workflow/expectations.
	JobTypeFlexCacheCreateVolume     JobType = datamodel.JobTypeFlexCacheCreateVolume
	JobTypeFlexCacheEstablishPeering JobType = datamodel.JobTypeFlexCacheEstablishPeering
	JobTypeFlexCacheInternalPeering  JobType = datamodel.JobTypeFlexCacheInternalPeering
	JobTypeFlexCacheDeleteVolume     JobType = datamodel.JobTypeFlexCacheDeleteVolume
	JobTypeFlexCachePrePopulate      JobType = datamodel.JobTypeFlexCachePrePopulate

	JobTypeCreateVolume                             JobType = datamodel.JobTypeCreateVolume
	JobTypeCreateLargeVolume                        JobType = datamodel.JobTypeCreateLargeVolume
	JobTypeUpdateVolume                             JobType = datamodel.JobTypeUpdateVolume
	JobTypeUpdateVolumeInReplication                JobType = datamodel.JobTypeUpdateVolumeInReplication
	JobTypeUpdateVolumePerformanceGroup             JobType = datamodel.JobTypeUpdateVolumePerformanceGroup
	JobTypeDeleteVolumePerformanceGroup             JobType = datamodel.JobTypeDeleteVolumePerformanceGroup
	JobTypeRevertVolume                             JobType = datamodel.JobTypeRevertVolume
	JobTypeDeleteVolume                             JobType = datamodel.JobTypeDeleteVolume
	JobTypeDeleteLargeVolume                        JobType = datamodel.JobTypeDeleteLargeVolume
	JobTypeCreateSnapshot                           JobType = datamodel.JobTypeCreateSnapshot
	JobTypeUpdateSnapshot                           JobType = datamodel.JobTypeUpdateSnapshot
	JobTypeDeleteSnapshot                           JobType = datamodel.JobTypeDeleteSnapshot
	JobTypeCreateQuotaRule                          JobType = datamodel.JobTypeCreateQuotaRule
	JobTypeUpdateQuotaRule                          JobType = datamodel.JobTypeUpdateQuotaRule
	JobTypeDeleteQuotaRule                          JobType = datamodel.JobTypeDeleteQuotaRule
	JobTypeRestoreBackup                            JobType = datamodel.JobTypeRestoreBackup
	JobTypeRestoreFilesBackup                       JobType = datamodel.JobTypeRestoreFilesBackup
	JobTypeRestoreOntapModeBackup                   JobType = datamodel.JobTypeRestoreOntapModeBackup
	JobTypeAcceptClusterPeer                        JobType = datamodel.JobTypeAcceptClusterPeer
	JobTypeUpdateKmsConfig                          JobType = datamodel.JobTypeUpdateKmsConfig
	JobTypeCreateKmsConfig                          JobType = datamodel.JobTypeCreateKmsConfig
	JobTypeDeleteKmsConfig                          JobType = datamodel.JobTypeDeleteKmsConfig
	JobTypeSdeKmsCreate                             JobType = datamodel.JobTypeSdeKmsCreate
	JobTypeMigrateKmsConfig                         JobType = datamodel.JobTypeMigrateKmsConfig
	JobTypeRotateKmsConfig                          JobType = datamodel.JobTypeRotateKmsConfig
	JobTypeCreateVolumeReplication                  JobType = datamodel.JobTypeCreateVolumeReplication
	JobTypeCreateVolumeReplicationInternal          JobType = datamodel.JobTypeCreateVolumeReplicationInternal
	JobTypeDeleteVolumeReplicationInternal          JobType = datamodel.JobTypeDeleteVolumeReplicationInternal
	JobTypeUpdateVolumeReplicationInternal          JobType = datamodel.JobTypeUpdateVolumeReplicationInternal
	JobTypeCreateBackupVault                        JobType = datamodel.JobTypeCreateBackupVault
	JobTypeDeleteVolumeReplication                  JobType = datamodel.JobTypeDeleteVolumeReplication
	JobTypeUpdateVolumeReplication                  JobType = datamodel.JobTypeUpdateVolumeReplication
	JobTypeResumeVolumeReplication                  JobType = datamodel.JobTypeResumeVolumeReplication
	JobTypeResumeVolumeReplicationInternal          JobType = datamodel.JobTypeResumeVolumeReplicationInternal
	JobTypeReverseVolumeReplicationInternal         JobType = datamodel.JobTypeReverseVolumeReplicationInternal
	JobTypeSyncVolumeReplication                    JobType = datamodel.JobTypeSyncVolumeReplication
	JobTypeReverseResumeVolumeReplication           JobType = datamodel.JobTypeReverseResumeVolumeReplication
	JobTypeUpdateVolumeReplicationAttributes        JobType = datamodel.JobTypeUpdateVolumeReplicationAttributes
	JobTypeStopVolumeReplication                    JobType = datamodel.JobTypeStopVolumeReplication
	JobTypeStopVolumeReplicationInternal            JobType = datamodel.JobTypeStopVolumeReplicationInternal
	JobTypeRefreshVolumeReplicationInternal         JobType = datamodel.JobTypeRefreshVolumeReplicationInternal
	JobTypeCreateBackup                             JobType = datamodel.JobTypeCreateBackup
	JobTypeDeleteBackup                             JobType = datamodel.JobTypeDeleteBackup
	JobTypeUpdateHostGroup                          JobType = datamodel.JobTypeUpdateHostGroup
	JobTypeMountCheck                               JobType = datamodel.JobTypeMountCheck
	JobTypeRefreshAdminJobSpecs                     JobType = datamodel.JobTypeRefreshAdminJobSpecs
	JobTypeStartProjectEventOffState                JobType = datamodel.JobTypeStartProjectEventOffState
	JobTypeStartProjectEventOnState                 JobType = datamodel.JobTypeStartProjectEventOnState
	JobTypeFinishProjectEventDeleteState            JobType = datamodel.JobTypeFinishProjectEventDeleteState
	JobTypeReleaseVolumeReplicationInternal         JobType = datamodel.JobTypeReleaseVolumeReplicationInternal
	JobTypeUpdateBackupVault                        JobType = datamodel.JobTypeUpdateBackupVault
	JobTypeDeleteSnapmirrorSnapshotsInternal        JobType = datamodel.JobTypeDeleteSnapmirrorSnapshotsInternal
	JobTypeCreateSubnet                             JobType = datamodel.JobTypeCreateSubnet
	JobTypeDeleteSubnet                             JobType = datamodel.JobTypeDeleteSubnet
	JobTypeCreateLargeSubnet                        JobType = datamodel.JobTypeCreateLargeSubnet
	JobTypeHandleResourceEvent                      JobType = datamodel.JobTypeHandleResourceEvent
	JobTypeHandleResourceEventOffState              JobType = datamodel.JobTypeHandleResourceEventOffState
	JobTypeHandleResourceEventOnState               JobType = datamodel.JobTypeHandleResourceEventOnState
	JobTypeHandleResourceEventDeleteState           JobType = datamodel.JobTypeHandleResourceEventDeleteState
	JobTypeDeleteBackupVault                        JobType = datamodel.JobTypeDeleteBackupVault
	JobTypeInitCreateScheduledBackup                JobType = datamodel.JobTypeInitCreateScheduledBackup
	JobTypeCreateScheduledBackup                    JobType = datamodel.JobTypeCreateScheduledBackup
	JobTypeDeleteScheduledBackup                    JobType = datamodel.JobTypeDeleteScheduledBackup
	JobTypeRefreshVolumeFields                      JobType = datamodel.JobTypeRefreshVolumeFields
	JobTypeUpdateBackup                             JobType = datamodel.JobTypeUpdateBackup
	JobTypeCreateBackupPolicy                       JobType = datamodel.JobTypeCreateBackupPolicy
	JobTypeUpdateBackupPolicy                       JobType = datamodel.JobTypeUpdateBackupPolicy
	JobTypeDeleteBackupPolicy                       JobType = datamodel.JobTypeDeleteBackupPolicy
	JobTypeCreateActiveDirectory                    JobType = datamodel.JobTypeCreateActiveDirectory
	JobTypeUpdateActiveDirectory                    JobType = datamodel.JobTypeUpdateActiveDirectory
	JobTypeDeleteActiveDirectory                    JobType = datamodel.JobTypeDeleteActiveDirectory
	JobTypeSplitVolume                              JobType = datamodel.JobTypeSplitVolume
	JobTypeCreateHybridReplication                  JobType = datamodel.JobTypeCreateHybridReplication
	JobTypeHybridReplicationDeleteVolume            JobType = datamodel.JobTypeHybridReplicationDeleteVolume
	JobTypeHybridReplicationEstablishPeering        JobType = datamodel.JobTypeHybridReplicationEstablishPeering
	JobTypeHybridReplicationInternalEstablish       JobType = datamodel.JobTypeHybridReplicationInternalEstablish
	JobTypeReverseHybridReplicationInternal         JobType = datamodel.JobTypeReverseHybridReplicationInternal
	JobTypeReverseHybridReplicationFallbackInternal JobType = datamodel.JobTypeReverseHybridReplicationFallbackInternal
	JobTypeCreateExpertModeVolume                   JobType = datamodel.JobTypeCreateExpertModeVolume
	JobTypeUpdateExpertModeVolume                   JobType = datamodel.JobTypeUpdateExpertModeVolume
	JobTypeDeleteExpertModeVolume                   JobType = datamodel.JobTypeDeleteExpertModeVolume
	JobTypeExpertModeRbacRefresh                    JobType = datamodel.JobTypeExpertModeRbacRefresh
	JobTypeExpertModeFlexCloneSplit                 JobType = datamodel.JobTypeExpertModeFlexCloneSplit
	JobTypeRotateCmekBackups                        JobType = datamodel.JobTypeRotateCmekBackups
	JobTypeManageBackupConfigExpertModeVolume       JobType = datamodel.JobTypeManageBackupConfigExpertModeVolume
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
