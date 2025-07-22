package orchestrator

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"go.temporal.io/sdk/client"
)

type OrchestratorFactory interface {
	CreatePool(ctx context.Context, params *commonparams.CreatePoolParams) (*models.Pool, string, error)
	UpdatePool(ctx context.Context, params *commonparams.UpdatePoolParams) (*models.Pool, string, error)
	DescribePool(ctx context.Context, poolId string, accountName string) (*models.Pool, error)
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
	GetVolume(ctx context.Context, volumeId string, updateVolumeMetrics bool) (*models.Volume, error)
	UpdateVolume(ctx context.Context, param *commonparams.UpdateVolumeParams) (*models.Volume, string, error)
	GetVolumeCount(ctx context.Context, projectNumber string) (int64, error)
	DeleteVolume(ctx context.Context, volumeId string) (*models.Volume, string, error)
	GetMultipleVolumes(ctx context.Context, volumeIds []string, accountName string) ([]*models.Volume, error)
	ListVolumes(ctx context.Context, accountName string) ([]*models.Volume, error)

	GetJob(ctx context.Context, operationId string) (*models.Job, error)
	GetReplicationJobs(ctx context.Context, projectName string, poolUUID string) ([]*models.Job, error)

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
	GetMultipleReplicationsInternal(ctx context.Context, accountName string, replicationUUIDs []string) ([]*datamodel.VolumeReplication, error)
	GetMultipleReplications(ctx context.Context, params commonparams.GetMultipleReplicationsParams) ([]gcpserver.ReplicationV1beta, error)
	AcceptClusterPeer(ctx context.Context, params *commonparams.ClusterPeerParams, poolID string) (*commonparams.ClusterPeerParams, *datamodel.Job, error)
	PerformMountCheck(ctx context.Context, replicationUUID string, accountName string) (*models.Job, error)
	ResumeReplication(ctx context.Context, params *commonparams.ResumeReplicationParams) (*models.VolumeReplication, string, error)
	ResumeReplicationInternal(ctx context.Context, volumeReplicationId, accountName string, forceResume bool) (*models.VolumeReplication, *datamodel.Job, error)
	GetReplication(ctx context.Context, volumeReplicationId string) (*models.VolumeReplication, error)
	ReleaseVolumeReplication(ctx context.Context, replicationUUID string) (*models.VolumeReplication, *datamodel.Job, error)
	DeleteReplicationInternal(ctx context.Context, volumeReplicationId string) (*models.VolumeReplication, *datamodel.Job, error)
	StopReplicationInternal(ctx context.Context, replicationUUID string, accountName string, forceStop bool) (*models.VolumeReplication, *datamodel.Job, error)
	StopReplication(ctx context.Context, params *commonparams.StopReplicationParams) (*models.VolumeReplication, string, error)
	DeleteReplication(ctx context.Context, params *commonparams.DeleteReplicationParams) (*models.VolumeReplication, string, error)

	// KMS Config related methods
	KmsConfigInterface

	GetBackupVaultByNameAndOwnerID(ctx context.Context, bvName, ownerID string) (*models.BackupVaultV1beta, error)
	GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName, ownerID string) (*models.BackupPolicy, error)
	ListBackupPoliciesAndVolumeCount(ctx context.Context, ownerID string, backupPolicyUUIDs []string) (map[string]int64, map[string]*models.BackupPolicy, error)
	ListBackupVaults(ctx context.Context, accountName string) ([]*models.BackupVaultV1beta, error)
	GetBackupVaultByUUID(ctx context.Context, bvUUID string, ownerID string) (*models.BackupVaultV1beta, error)
	UpdateBackupVault(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error)
	GetMultipleBackupVaults(ctx context.Context, backupVaultUUIDList []string) ([]*models.BackupVaultV1beta, error)
	DeleteBackupVault(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error)

	CreateBackup(ctx context.Context, params *commonparams.CreateBackupParams) (*models.Backup, string, error)
	GetBackup(ctx context.Context, params *commonparams.GetBackupParams) (*datamodel.Backup, error)
	DeleteBackup(ctx context.Context, params *commonparams.DeleteBackupParams) (*models.BaseModel, string, error)
	ListBackups(ctx context.Context, backupVaultID, ownerID string, filters [][]interface{}) ([]*datamodel.Backup, error)
	GetBackupsUnderBackupVault(ctx context.Context, backupVaultID, ownerID string, backupUUIDs []string) ([]*datamodel.Backup, error)
	CreateOrGetStartProjectEventJob(ctx context.Context, params *commonparams.StartProjectEventParams) (string, error)
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
