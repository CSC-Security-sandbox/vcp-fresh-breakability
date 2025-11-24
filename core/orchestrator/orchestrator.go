package orchestrator

import (
	"context"

	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"go.temporal.io/sdk/client"
)

type OrchestratorFactory interface {
	CreatePool(ctx context.Context, params *commonparams.CreatePoolParams) (*models.Pool, string, error)
	UpdatePool(ctx context.Context, params *commonparams.UpdatePoolParams) (*models.Pool, string, error)
	DescribePool(ctx context.Context, poolId string, accountName string) (*models.Pool, error)
	GetExpertModePoolCreds(ctx context.Context, poolId string, accountName string, userName string) (*models.UserCredentials, error)
	DeletePool(ctx context.Context, params *commonparams.DeletePoolParams) (*models.Pool, string, error)
	GetMultiplePools(ctx context.Context, accountName string, poolUUIDs []string) ([]*models.Pool, error)
	GetPoolByVendorID(ctx context.Context, vendorID string, accountName string) (*models.Pool, error)
	GetPoolByName(ctx context.Context, poolName string, accountName string, queryDepth int) (*models.Pool, error)
	ListPools(ctx context.Context, accountName string, includeDeleted bool) ([]*models.Pool, error)
	ListAllPools(ctx context.Context) ([]*models.Pool, error)

	CreateHostGroup(ctx context.Context, params *commonparams.CreateHostGroupParams) (*models.HostGroup, error)
	GetHostGroup(ctx context.Context, hostGroupUUID string, accountID string) (*models.HostGroup, error)
	DeleteHostGroup(ctx context.Context, hostGroupUUID string, accountID string) (*models.HostGroup, error)
	UpdateHostGroup(ctx context.Context, params *commonparams.UpdateHostGroupParams) (*models.HostGroup, string, error)
	GetMultipleHostGroups(ctx context.Context, accountName string, hostGroupUUIDs []string) ([]*models.HostGroup, error)

	CreateVolume(ctx context.Context, params *commonparams.CreateVolumeParams) (*models.Volume, string, error)
	CreateFlexCacheVolume(ctx context.Context, params *commonparams.CreateVolumeParams) (*models.Volume, string, error)
	RevertVolume(ctx context.Context, params *commonparams.RevertVolumeParams) (*models.Volume, string, error)
	GetVolume(ctx context.Context, volumeId string, updateVolumeMetrics bool) (*models.Volume, error)
	UpdateVolume(ctx context.Context, param *commonparams.UpdateVolumeParams) (*models.Volume, string, error)
	UpdateVolumeV2(ctx context.Context, param *commonparams.UpdateVolumeParams) (*models.Volume, string, error)
	GetVolumeCount(ctx context.Context, projectNumber string) (int64, error)
	DeleteVolume(ctx context.Context, volumeId string) (*models.Volume, string, error)
	GetMultipleVolumes(ctx context.Context, volumeIds []string, accountName string) ([]*models.Volume, error)
	ListVolumes(ctx context.Context, accountName string) ([]*models.Volume, error)
	EstablishFlexCacheVolumePeering(ctx context.Context, params *commonparams.EstablishVolumePeeringParams) (*models.Volume, string, error)
	RestoreFilesFromBackup(ctx context.Context, params *commonparams.RestoreFilesFromBackupParams) (string, error)
	SplitCloneVolume(ctx context.Context, params *commonparams.SplitCloneVolumeParams) (*models.Volume, string, error)

	GetJob(ctx context.Context, operationId string) (*models.Job, error)
	GetReplicationJobs(ctx context.Context, projectName string, poolUUID string) ([]*models.Job, error)
	GetJobByResourceUUID(ctx context.Context, resourceUUID string, jobType string) (*models.Job, error)

	CreateSnapshot(ctx context.Context, params *commonparams.CreateSnapshotParams) (*models.Snapshot, string, error)
	GetSnapshot(ctx context.Context, params *commonparams.GetSnapshotParams) (*models.Snapshot, error)
	DeleteSnapshot(ctx context.Context, params *commonparams.DeleteSnapshotParams) (*models.Snapshot, string, error)
	ListSnapshots(ctx context.Context, params *commonparams.ListSnapshotsParams) ([]*models.Snapshot, error)
	UpdateSnapshot(ctx context.Context, params *commonparams.UpdateSnapshotParams) (*models.Snapshot, string, error)
	GetMultipleSnapshots(ctx context.Context, VolumeUuId string, accountName string, snapshotUUIDs []string) ([]*models.Snapshot, error)
	DeleteSnapmirrorSnapshots(ctx context.Context, params *commonparams.SnapshotsInternalDeleteParams) (string, error)

	CreateVolumeReplicationInternal(ctx context.Context, params *commonparams.CreateVolumeReplicationInternalParams) (*models.VolumeReplication, *datamodel.Job, error)
	GetReplicationCount(ctx context.Context, projectNumber string) (int64, error)
	CreateVolumeReplication(ctx context.Context, params *commonparams.CreateVolumeReplicationParams) (*models.VolumeReplication, string, error)
	UpdateVolumeReplicationInternal(ctx context.Context, params *commonparams.UpdateVolumeReplicationInternalParams) (*models.VolumeReplication, *datamodel.Job, error)
	UpdateVolumeReplicationAttributes(ctx context.Context, params models.UpdateVolumeReplicationAttributesParams) (*models.Job, error)
	UpdateVolumeReplicationState(ctx context.Context, params models.UpdateVolumeReplicationStateParams) (*models.VolumeReplication, error)
	GetMultipleReplicationsInternal(ctx context.Context, accountName string, replicationUUIDs []string) ([]*datamodel.VolumeReplication, error)
	GetMultipleReplications(ctx context.Context, params commonparams.GetMultipleReplicationsParams) ([]gcpserver.ReplicationV1beta, error)
	GetMultipleReplicationsByExternalUUID(ctx context.Context, params commonparams.GetMultipleReplicationsByExternalUUIDParams) ([]gcpserver.ReplicationV1beta, error)
	AcceptClusterPeer(ctx context.Context, params *commonparams.ClusterPeerParams, poolID string) (*commonparams.ClusterPeerParams, *datamodel.Job, error)
	PerformMountCheck(ctx context.Context, replicationUUID string, accountName string) (*models.Job, error)
	ResumeReplication(ctx context.Context, params *commonparams.ResumeReplicationParams) (*models.VolumeReplication, string, error)
	UpdateReplication(ctx context.Context, params *commonparams.UpdateReplicationParams) (*models.VolumeReplication, string, error)
	ResumeReplicationInternal(ctx context.Context, volumeReplicationId, accountName string, forceResume bool) (*models.VolumeReplication, *datamodel.Job, error)
	ReverseReplicationInternal(ctx context.Context, volumeReplicationId, accountName string) (*models.VolumeReplication, *datamodel.Job, error)
	GetReplication(ctx context.Context, volumeReplicationId string) (*models.VolumeReplication, error)
	ReleaseVolumeReplication(ctx context.Context, replicationUUID string) (*models.VolumeReplication, *datamodel.Job, error)
	DeleteReplicationInternal(ctx context.Context, volumeReplicationId string, cleanupAfterReverse bool) (*models.VolumeReplication, *datamodel.Job, error)
	StopReplicationInternal(ctx context.Context, replicationUUID string, accountName string, forceStop bool) (*models.VolumeReplication, *datamodel.Job, error)
	StopReplication(ctx context.Context, params *commonparams.StopReplicationParams) (*models.VolumeReplication, string, error)
	DeleteReplication(ctx context.Context, params *commonparams.DeleteReplicationParams, cleanupResourcesJobId string, isCleanUp bool) (*models.VolumeReplication, string, error)
	SyncReplication(ctx context.Context, params *commonparams.ResumeReplicationParams) (*models.VolumeReplication, string, error)
	ReverseAndResumeReplication(ctx context.Context, params *commonparams.ReverseAndResumeReplicationParams) (*models.VolumeReplication, *string, error)

	// KMS Config related methods
	KmsConfigInterface

	GetBackupVaultByNameAndOwnerID(ctx context.Context, bvName, ownerID string) (*models.BackupVaultV1beta, error)
	GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName, ownerID string) (*models.BackupPolicy, error)
	GetBackupPolicyByUUIDAndOwnerID(ctx context.Context, uuid string, ownerID string) (*models.BackupPolicy, error)
	UpdateBackupPolicy(ctx context.Context, params *commonparams.UpdateBackupPolicyParams) (*models.BackupPolicy, string, error)
	ListBackupPoliciesAndVolumeCount(ctx context.Context, ownerID string, backupPolicyUUIDs []string) (map[string]int64, map[string]*models.BackupPolicy, error)
	DeleteBackupPolicy(ctx context.Context, params *commonparams.DeleteBackupPolicyParams) (*models.BackupPolicy, string, error)
	GetBackupPolicyUUIDsFromBackupVaultUUID(ctx context.Context, backupVaultUUID string, ownerId string) ([]string, error)
	ListBackupVaults(ctx context.Context, accountName string) ([]*models.BackupVaultV1beta, error)
	GetBackupVaultByUUID(ctx context.Context, bvUUID string, ownerID string) (*models.BackupVaultV1beta, error)
	UpdateBackupVault(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error)
	GetMultipleBackupVaults(ctx context.Context, backupVaultUUIDList []string) ([]*models.BackupVaultV1beta, error)
	DeleteBackupVault(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error)
	DeleteBackupVaultInternal(ctx context.Context, params *commonparams.BackupVaultParams) (string, error)
	UpdateBackupVaultInternal(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error)
	IsBackupVaultAttachedToVolume(ctx context.Context, backupVaultUUID string) (bool, error)
	GetBackupVaultUUIDsFromBackupPolicyUUID(ctx context.Context, backupPolicyUUID string, accountName string) ([]string, error)

	CreateBackupVaultEntryInVCP(ctx context.Context, bv *datamodel.BackupVault) (*datamodel.BackupVault, error)
	GetBackupVaultByExternalUUIDAndOwnerID(ctx context.Context, externalUUID string, ownerID string) (*datamodel.BackupVault, error)

	GetAccount(ctx context.Context, accountName string) (*datamodel.Account, error)
	UpdateResourceState(ctx context.Context, params *commonparams.UpdateResourceStateParams) (string, error)
	CreateBackup(ctx context.Context, params *commonparams.CreateBackupParams) (*models.Backup, string, error)
	CreateBackupInternal(ctx context.Context, params *commonparams.CreateBackupParams) (*models.Backup, string, error)
	GetBackup(ctx context.Context, params *commonparams.GetBackupParams) (*datamodel.Backup, error)
	GetBackupByExternalUUID(ctx context.Context, backupVaultUUID string, externalUUID string, accountName string) (*datamodel.Backup, error)
	DeleteBackup(ctx context.Context, params *commonparams.DeleteBackupParams) (*models.BaseModel, string, error)
	DeleteBackupInternal(ctx context.Context, params *commonparams.DeleteBackupParams) (string, error)
	ListBackups(ctx context.Context, backupVaultID, ownerID string, filters [][]interface{}) ([]*datamodel.Backup, error)
	UpdateBackup(ctx context.Context, params *commonparams.UpdateBackupParams) (*models.Backup, string, error)
	UpdateBackupInternal(ctx context.Context, params *commonparams.UpdateBackupParams) (*models.Backup, string, error)
	GetBackupsUnderBackupVault(ctx context.Context, backupVaultID, ownerID string, backupUUIDs []string) ([]*datamodel.Backup, error)

	CreateOrGetStartProjectEventJob(ctx context.Context, params *commonparams.StartProjectEventParams) (string, error)
	CreateOrGetFinishProjectEventJob(ctx context.Context, params *commonparams.FinishProjectEventParams) (string, error)

	CreateActiveDirectory(ctx context.Context, params *commonparams.CreateActiveDirectoryParams) (*models.ActiveDirectory, string, error)
	UpdateActiveDirectory(ctx context.Context, params *commonparams.UpdateActiveDirectoryParams) (*models.ActiveDirectory, string, error)

	// Cluster upgrade methods
	UpgradeCluster(ctx context.Context, params *commonparams.UpgradeClusterParams) (*models.ClusterUpgradeResponse, string, error)
	GetClusterUpgradeStatus(ctx context.Context, jobUUID string) (*models.UpgradeProgress, error)
	ListAvailableVersions(ctx context.Context) (*models.ListAvailableVersionsResponse, error)
	CreateImageVersion(ctx context.Context, ontapVersion, vsaImagePath, vsaName, mediatorName string, isActive bool) (*datamodel.ImageVersion, error)
	DeleteImageVersion(ctx context.Context, ontapVersion string) error

	GetActiveDirectory(ctx context.Context, activeDirectoryUUID string) (*models.ActiveDirectory, error)
	ListActiveDirectories(ctx context.Context, accountName string) ([]*models.ActiveDirectory, error)
	GetMultipleActiveDirectories(ctx context.Context, uuids []string) ([]*models.ActiveDirectory, error)
	GetADConfig(ctx context.Context, params *commonparams.GetADParams) (*models.ActiveDirectory, error)
	GetSDEActiveDirectory(ctx context.Context, getADParams *commonparams.GetADParams) (*cvpmodels.ActiveDirectoryV1beta, error)
	DeleteActiveDirectory(ctx context.Context, params *commonparams.DeleteActiveDirectoryParams) (string, error)
}

type Orchestrator struct {
	storage  database.Storage
	temporal client.Client
}

func NewOrchestrator(storage database.Storage, temporalClient client.Client) *Orchestrator {
	return &Orchestrator{
		storage:  storage,
		temporal: temporalClient,
	}
}

func GetNewOrchestrator(storage database.Storage, temporalClient client.Client) OrchestratorFactory {
	return NewOrchestrator(storage, temporalClient)
}
