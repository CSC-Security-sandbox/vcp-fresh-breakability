package datamodel

// HybridReplicationStatus and ClusterPeeringStatus are typed string enums
// used in JSONB columns. They previously lived in core/models; moved here
// so database/datamodel is self-contained.
type HybridReplicationStatus string
type ClusterPeeringStatus string

type JobType string
type JobState string

// Lifecycle states for storage resources (pools, volumes, snapshots, etc.).
const (
	LifeCycleStateCreating        = "CREATING"
	LifeCycleStatePreparing       = "PREPARING"
	LifeCycleStateOngoing         = "ONGOING"
	LifeCycleStateReverting       = "REVERTING"
	LifeCycleStateUndeleting      = "UNDELETING"
	LifeCycleStateCompleted       = "COMPLETED"
	LifeCycleStateRestoring       = "RESTORING"
	LifeCycleStateSplitting       = "SPLITTING"
	LifeCycleStateAvailable       = "AVAILABLE"
	LifeCycleStateREADY           = "READY"
	LifeCycleStateInUse           = "IN_USE"
	LifeCycleStateDisabled        = "DISABLED"
	LifeCycleStateDisabling       = "DISABLING"
	LifeCycleStateEnabling        = "ENABLING"
	LifeCycleStateUpdating        = "UPDATING"
	LifeCycleStateDeleting        = "DELETING"
	LifeCycleStateDeleted         = "DELETED"
	LifeCycleStateError           = "ERROR"
	LifeCycleStateRetained        = "RETAINED"
	LifeCycleStateCreated         = "CREATED"
	LifeCycleStateKeyCheckPending = "KEY_CHECK_PENDING"
	LifeCycleStateMigrating       = "MIGRATING"
	LifeCycleStateDegraded        = "DEGRADED"
	LifeCycleStateUnknown         = "UNKNOWN"

	// "*Details" human-readable suffixes (used as StateDetails column values).
	LifeCycleStateCreatingDetails            = "Creation in progress"
	LifeCycleStateRevertingDetails           = "Revert in progress"
	LifeCycleStateUndeletingDetails          = "Undelete in progress"
	LifeCycleStateRestoringDetails           = "Restore in progress"
	LifeCycleStateAvailableDetails           = "Available for use"
	LifeCycleStateDisabledDetails            = "Disabled"
	LifeCycleStateUpdatingDetails            = "Update in progress"
	LifeCycleStateSyncDetails                = "Sync in progress"
	LifeCycleStateDeletingDetails            = "Deletion in progress"
	LifeCycleStateSplittingDetails           = "Splitting in progress"
	LifeCycleStateDeletedDetails             = "Deleted"
	LifeCycleStateCompletedDetails           = "Completed"
	LifeCycleStateRetainedDetails            = "Retained"
	LifeCycleStateOngoingDetails             = "Ongoing"
	LifeCycleStateCreationErrorDetails       = "Error in creating"
	LifeCycleStateUpdateErrorDetails         = "Error in updating"
	LifeCycleStateDeletionErrorDetails       = "Error in deleting"
	LifeCycleStateReadyDetails               = "Ready for use"
	LifeCycleStateCreatedDetails             = "Created successfully"
	LifeCycleStateUnknownDetails             = "Unknown state"
	LifeCycleStateInUseDetails               = "In use"
	LifeCycleStateMigratingDetails           = "Kms config is in migrating state"
	LifeCycleStateDegradedDetails            = "We're currently experiencing degraded performance for this resource, which may result in increased write latency. Some operations maybe restricted during this time."
	LifeCycleStateVolMigratingDetails        = "Volume encryption in progress"
	LifeCycleStateHyperscalerDisabledDetails = "Hyperscaler disabled"
)

// Typed ClusterPeering constants (canonical, after PR-2c alias).
const (
	CvpClusterPeeringStatusCREATING              ClusterPeeringStatus = "CREATING"
	CvpClusterPeeringStatusPENDINGCLUSTERPEERING ClusterPeeringStatus = "PENDING_CLUSTER_PEERING"
	CvpClusterPeeringStatusPEERED                ClusterPeeringStatus = "PEERED"
	CvpClusterPeeringStatusDELETED               ClusterPeeringStatus = "DELETED"
	CvpClusterPeeringStatusERROR                 ClusterPeeringStatus = "ERROR"
)

const (
	HybridReplicationStatusPendingClusterPeer  HybridReplicationStatus = "PENDING_CLUSTER_PEER"
	HybridReplicationStatusPendingSVMPeer      HybridReplicationStatus = "PENDING_SVM_PEER"
	HybridReplicationStatusSVMPeered           HybridReplicationStatus = "SVM_PEERED"
	HybridReplicationStatusPeered              HybridReplicationStatus = "PEERED"
	HybridReplicationStatusPendingRemoteResync HybridReplicationStatus = "PENDING_REMOTE_RESYNC"
	HybridReplicationStatusExternalManaged     HybridReplicationStatus = "EXTERNALLY_MANAGED_REPLICATION"
)

// ONTAP-side enums referenced by the DAO layer.
const (
	OntapUninitialized         = "uninitialized"
	OntapSnapmirrored          = "snapmirrored"
	SnapmirrorRelationshipIdle = "idle"
	StateOn                    = "on"
	StateOff                   = "off"
)

const (
	JobsStateNEW                    JobState = "NEW"
	JobsStatePROCESSING             JobState = "PROCESSING"
	JobsStateERROR                  JobState = "ERROR"
	JobsStateDONE                   JobState = "DONE"
	JobsStateWaitForTemporal        JobState = "WAIT_FOR_TEMPORAL"
	JobsStateCANCELLED              JobState = "CANCELLED"
	WaitForTemporalJobMaxRetryCount          = 5
)

const (
	JobTypeCreatePool      JobType = "CREATE_POOL"
	JobTypeCreateLargePool JobType = "CREATE_LARGE_POOL"
	JobTypeUpdatePool      JobType = "UPDATE_POOL"
	JobTypeUpdateLargePool JobType = "UPDATE_LARGE_POOL"
	JobTypeDeletePool      JobType = "DELETE_POOL"
	JobTypeDeleteLargePool JobType = "DELETE_LARGE_POOL"
	JobTypeCreateSvm       JobType = "CREATE_SVM"
	JobTypeDeleteSvm       JobType = "DELETE_SVM"

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
	JobTypeUpdateVolumePerformanceGroup             JobType = "UPDATE_VOLUME_PERFORMANCE_GROUP"
	JobTypeDeleteVolumePerformanceGroup             JobType = "DELETE_VOLUME_PERFORMANCE_GROUP"
	JobTypeRevertVolume                             JobType = "REVERT_VOLUME"
	JobTypeDeleteVolume                             JobType = "DELETE_VOLUME"
	JobTypeDeleteLargeVolume                        JobType = "DELETE_LARGE_VOLUME"
	JobTypeCreateSnapshot                           JobType = "CREATE_SNAPSHOT"
	JobTypeUpdateSnapshot                           JobType = "UPDATE_SNAPSHOT"
	JobTypeDeleteSnapshot                           JobType = "DELETE_SNAPSHOT"
	JobTypeCreateQuotaRule                          JobType = "CREATE_QUOTA_RULE"
	JobTypeUpdateQuotaRule                          JobType = "UPDATE_QUOTA_RULE"
	JobTypeDeleteQuotaRule                          JobType = "DELETE_QUOTA_RULE"
	JobTypeRestoreBackup                            JobType = "RESTORE_BACKUP"
	JobTypeRestoreFilesBackup                       JobType = "RESTORE_FILES_BACKUP"
	JobTypeRestoreOntapModeBackup                   JobType = "RESTORE_ONTAP_MODE_BACKUP"
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
	JobTypeCreateBackupPolicy                       JobType = "CREATE_BACKUP_POLICY"
	JobTypeUpdateBackupPolicy                       JobType = "UPDATE_BACKUP_POLICY"
	JobTypeDeleteBackupPolicy                       JobType = "DELETE_BACKUP_POLICY"
	JobTypeCreateActiveDirectory                    JobType = "CREATE_ACTIVE_DIRECTORY"
	JobTypeUpdateActiveDirectory                    JobType = "UPDATE_ACTIVE_DIRECTORY"
	JobTypeDeleteActiveDirectory                    JobType = "DELETE_ACTIVE_DIRECTORY"
	JobTypeSplitVolume                              JobType = "SPLIT_CLONE_VOLUME"
	JobTypeCreateHybridReplication                  JobType = "CREATE_HYBRID_REPLICATION"
	JobTypeHybridReplicationDeleteVolume            JobType = "HYBRID_REPLICATION_DELETE_VOLUME"
	JobTypeHybridReplicationEstablishPeering        JobType = "HYBRID_REPLICATION_ESTABLISH_PEERING"
	JobTypeHybridReplicationInternalEstablish       JobType = "HYBRID_REPLICATION_INTERNAL_ESTABLISH"
	JobTypeReverseHybridReplicationInternal         JobType = "HYBRID_REPLICATION_INTERNAL_REVERSE"
	JobTypeReverseHybridReplicationFallbackInternal JobType = "HYBRID_REPLICATION_INTERNAL_REVERSE_FALLBACK"
	JobTypeCreateExpertModeVolume                   JobType = "RECONCILE_EXPERT_MODE_VOLUME_CREATE"
	JobTypeUpdateExpertModeVolume                   JobType = "RECONCILE_EXPERT_MODE_VOLUME_UPDATE"
	JobTypeDeleteExpertModeVolume                   JobType = "RECONCILE_EXPERT_MODE_VOLUME_DELETE"
	JobTypeExpertModeRbacRefresh                    JobType = "EXPERT_MODE_RBAC_REFRESH"
	JobTypeExpertModeFlexCloneSplit                 JobType = "RECONCILE_EXPERT_MODE_VOLUME_FLEXCLONE_SPLIT"
	JobTypeRotateCmekBackups                        JobType = "ROTATE_CMEK_BACKUPS"
	JobTypeManageBackupConfigExpertModeVolume       JobType = "MANAGE_BACKUP_CONFIG_EXPERT_MODE_VOLUME"
)

// Account states (lifecycle for the Account entity in the multi-tenant model).
const (
	AccountStateDisabled            = "DISABLED"
	AccountStateEnabled             = "ENABLED"
	AccountStateDeleted             = "DELETED"
	AccountStateEnabling            = "ENABLING"
	AccountStateDisabling           = "DISABLING"
	AccountStateHyperscalerDisabled = "HYPERSCALERDISABLED"
)

// ServiceType for backup endpoint classification.
const (
	ServiceTypeGCNV         = "GCNV"
	ServiceTypeCrossProject = "CrossProject"
)
