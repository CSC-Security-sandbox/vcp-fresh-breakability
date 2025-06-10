package orchestrator

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"go.temporal.io/sdk/client"
)

type OrchestratorFactory interface {
	CreatePool(ctx context.Context, params *commonparams.CreatePoolParams) (*models.Pool, string, error)
	GetPool(ctx context.Context, poolId string, accountName string) (*models.Pool, error)
	DeletePool(ctx context.Context, params *commonparams.DeletePoolParams) (*models.Pool, string, error)
	GetMultiplePools(ctx context.Context, accountName string, poolUUIDs []string) ([]*models.Pool, error)
	GetPoolByVendorID(ctx context.Context, vendorID string) (*models.Pool, error)
	GetPoolByName(ctx context.Context, poolName string, accountName string, queryDepth int) (*models.Pool, error)
	ListPools(ctx context.Context, accountName string) ([]*models.Pool, error)

	CreateHostGroup(ctx context.Context, params *CreateHostGroupParams) (*models.HostGroup, error)
	GetHostGroup(ctx context.Context, hostGroupUUID string, accountID string) (*models.HostGroup, error)
	DeleteHostGroup(ctx context.Context, hostGroupUUID string, accountID string) (*models.HostGroup, error)
	GetMultipleHostGroups(ctx context.Context, accountName string, hostGroupUUIDs []string) ([]*models.HostGroup, error)

	CreateVolume(ctx context.Context, params *commonparams.CreateVolumeParams) (*models.Volume, string, error)
	GetVolume(ctx context.Context, volumeId string) (*models.Volume, error)
	UpdateVolume(ctx context.Context, param *commonparams.UpdateVolumeParams) (*models.Volume, string, error)
	DeleteVolume(ctx context.Context, volumeId string) (*models.Volume, string, error)
	GetMultipleVolumes(ctx context.Context, volumeIds []string, accountName string) ([]*models.Volume, error)

	AcceptClusterPeer(ctx context.Context, params *commonparams.ClusterPeerParams, poolID string) (*commonparams.ClusterPeerParams, *datamodel.Job, error)

	GetJob(ctx context.Context, operationId string) (*models.Job, error)
	GetReplicationJobs(ctx context.Context, projectName string, poolUUID string) ([]*models.Job, error)

	CreateSnapshot(ctx context.Context, params *commonparams.CreateSnapshotParams) (*models.Snapshot, string, error)
	GetSnapshot(ctx context.Context, params *commonparams.GetSnapshotParams) (*models.Snapshot, error)
	DeleteSnapshot(ctx context.Context, params *commonparams.DeleteSnapshotParams) (*models.Snapshot, string, error)
	ListSnapshots(ctx context.Context, params *commonparams.ListSnapshotsParams) ([]*models.Snapshot, error)
	UpdateSnapshot(ctx context.Context, params *commonparams.UpdateSnapshotParams) (*models.Snapshot, string, error)

	CreateVolumeReplication(ctx context.Context, params *commonparams.CreateVolumeReplicationParams) (*models.VolumeReplication, *datamodel.Job, error)

	// KMS Config related methods
	KmsConfigInterface

	GetBackupVaultByNameAndOwnerID(ctx context.Context, bvName, ownerID string) (*models.BackupVaultV1beta, error)
	CreateBackupVault(ctx context.Context, params *commonparams.BackupVaultParams, paramz gcpserver.V1betaCreateBackupVaultParams) (*models.BackupVaultV1beta, string, error)
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
