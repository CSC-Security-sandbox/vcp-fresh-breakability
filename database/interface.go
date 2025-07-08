package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"gorm.io/gorm"
)

type (
	Storage interface {
		Connect(isAdmin bool) error
		Close() error
		HealthCheck() error
		WithTransaction(ctx context.Context, fn func(Transaction) error) error
		Migrate(ctx context.Context) error
		Rollback(ctx context.Context) error
		DB() *gorm.DB
		SetupDatabase(ctx context.Context) error

		DataStore
	}

	Transaction interface {
		GORM() *gorm.DB
		Commit() error
		Rollback() error
	}

	DbConfig struct {
		Type              string
		Host              string
		Port              string
		User              string
		Password          string
		Name              string
		SSLMode           string
		TimeZone          string
		MaxOpenConns      int
		MaxIdleConns      int
		ConnMaxLifetime   time.Duration
		ConnectionTimeout int
		AdminUser         string
		AdminPassword     string
		MigrationPath     string
	}
)

// DataStore defines all operations
type DataStore interface {
	CreatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error)
	CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error)
	UpdatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error)
	UpdatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error)
	DeletePool(ctx context.Context, pool *datamodel.Pool) error
	DeletingPool(ctx context.Context, pool *datamodel.Pool) error
	DescribePool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error)
	GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error)
	ListPools(ctx context.Context, conditions [][]interface{}) ([]*datamodel.PoolView, error)
	GetPoolByVendorID(ctx context.Context, vendorID string) (*datamodel.PoolView, error)
	GetPoolByName(ctx context.Context, conditions [][]interface{}) (*datamodel.PoolView, error)
	SavePoolWithVsaClusterDetails(ctx context.Context, pool *datamodel.Pool, cluster *datamodel.ClusterDetails) error
	UpdatePoolWithKmsConfigID(ctx context.Context, pool *datamodel.Pool, kmsConfigUUID string) (*datamodel.Pool, error)

	CreateVolume(ctx context.Context, volume *datamodel.Volume, params *commonparams.CreateVolumeParams) (*datamodel.Volume, error)
	GetVolume(ctx context.Context, id string) (*datamodel.Volume, error)
	GetVolumeWithAccountID(ctx context.Context, id string, accountID int64) (*datamodel.Volume, error)
	GetVolumeCount(ctx context.Context, accountName string) (int64, error)
	GetVolumeByName(ctx context.Context, name string) (*datamodel.Volume, error)
	UpdateVolume(ctx context.Context, volume *datamodel.Volume) error
	UpdateVolumeFields(ctx context.Context, volumeUUID string, updates map[string]interface{}) error
	DeleteVolume(ctx context.Context, id string) (*datamodel.Volume, error)
	UpdateVolumeState(ctx context.Context, id string, state string, stateDetails string) (*datamodel.Volume, error)
	ListVolumes(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Volume, error)
	GetVolumesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Volume, error)
	GetVolumeCountByPoolID(ctx context.Context, poolID int64) (int64, error)
	GetMultipleVolumes(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Volume, error)
	VerifyVolumeOwnership(ctx context.Context, volumeID string, accountName string) (*datamodel.Volume, error)
	GetAllVolumesForHG(ctx context.Context, hostGroupUUID string, accountID int64) ([]*datamodel.Volume, error)

	CreateVolumeReplication(ctx context.Context, volumeRep *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error)
	GetVolumeReplication(ctx context.Context, id string) (*datamodel.VolumeReplication, error)
	UpdateVolumeReplication(ctx context.Context, volumeRep *datamodel.VolumeReplication) error
	UpdateVolumeReplicationStates(ctx context.Context, volumeRep *datamodel.VolumeReplication) error
	UpdateVolumeReplicationTransferStats(ctx context.Context, volumeRep *datamodel.VolumeReplication) error
	DeleteVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error)
	GetVolumeReplicationByProjectId(ctx context.Context, accountId int64) ([]*datamodel.VolumeReplication, error)
	GetVolumeReplicationCount(ctx context.Context, accountName string) (int64, error)
	ListVolumeReplications(ctx context.Context, filter utils.Filter) ([]*datamodel.VolumeReplication, error)

	GetAccount(ctx context.Context, name string) (*datamodel.Account, error)
	CreateAccount(ctx context.Context, account *datamodel.Account) (*datamodel.Account, error)

	CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error)
	UpdateJob(ctx context.Context, jobID string, status string, trackingID int, errorDetails []byte) error
	GetJob(ctx context.Context, jobID string) (*datamodel.Job, error)
	GetJobsWithCondition(ctx context.Context, filter utils.Filter) ([]*datamodel.Job, error)

	GetSvmForPoolID(ctx context.Context, poolID int64) (*datamodel.Svm, error)
	GetNodesByPoolID(ctx context.Context, poolId int64) ([]*datamodel.Node, error)
	CreateNode(ctx context.Context, node *datamodel.Node) (*datamodel.Node, error)

	CreateSVM(ctx context.Context, svm *datamodel.Svm) (*datamodel.Svm, error)
	GetSvmsByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Svm, error)
	UpdateSvmWithKmsConfigIDs(ctx context.Context, svm *datamodel.Svm, gcpKmsConfigUUID, externalGcpKmsConfigUUID string) (*datamodel.Svm, error)

	CreateLif(ctx context.Context, lif *datamodel.Lif) (*datamodel.Lif, error)
	GetLifForNode(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error)

	CreateHostGroup(ctx context.Context, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error)
	GetHostGroup(ctx context.Context, id string, accountID int64) (*datamodel.HostGroup, error)
	GetMultipleHostGroups(ctx context.Context, ids []string, accountID int64) ([]*datamodel.HostGroup, error)
	DeleteHostGroup(ctx context.Context, hostGroupUUID string, accountID int64) (*datamodel.HostGroup, error)
	UpdateHostGroupsState(ctx context.Context, hostGroupUUID []string, accountID int64, state string, stateDetails string) error
	UpdateHostGroup(ctx context.Context, hostGroupUUID string, accountID int64, description *string, Hosts *[]string) (*datamodel.HostGroup, error)

	GetLifByNodeID(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error)
	DeleteLif(ctx context.Context, lif *datamodel.Lif) error
	DeleteNode(ctx context.Context, node *datamodel.Node) error
	DeletingNode(ctx context.Context, node *datamodel.Node) error
	DeleteSVM(ctx context.Context, svm *datamodel.Svm) error
	DeletingSVM(ctx context.Context, svm *datamodel.Svm) error
	ErroredNode(ctx context.Context, node *datamodel.Node, errMsg string) error
	ErroredSVM(ctx context.Context, svm *datamodel.Svm, errMsg string) error

	CreatingSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) (*datamodel.Snapshot, error)
	UpdateSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) (*datamodel.Snapshot, error)
	GetSnapshotByUUID(ctx context.Context, uuid string, accountID int64, isParentSnapshot bool) (*datamodel.Snapshot, error)
	GetSnapshotsByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.Snapshot, error)
	GetWronglyDeletedSnapshot(ctx context.Context, snapshotExternalUUID string) (*datamodel.Snapshot, error)
	UnDeleteSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error
	GetSnapshotsByVolumeIDs(ctx context.Context, volumeID []int64) ([]*datamodel.Snapshot, error)
	GetSnapshotsWithCondition(ctx context.Context, filter utils.Filter) ([]*datamodel.Snapshot, error)
	GetAppConsistentSnapshotsForVolume(ctx context.Context, accountID, volumeID int64) ([]*datamodel.Snapshot, error)
	DeleteSnapshot(ctx context.Context, id string) (*datamodel.Snapshot, error)
	DeletingSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error
	BatchDeleteSnapshots(ctx context.Context, snapshotIDs []int64) ([]*datamodel.Snapshot, error)

	GetMultipleKmsConfigs(ctx context.Context, conditions [][]interface{}) ([]*datamodel.KmsConfig, error)
	GetKmsConfig(ctx context.Context, kmsConfigUUID string) (*datamodel.KmsConfig, error)
	UpdateKmsConfigState(ctx context.Context, kmsConfigUUID string, state string, stateDetails string) (*datamodel.KmsConfig, error)
	DeleteKmsConfig(ctx context.Context, kmsConfigUUID string) (*datamodel.KmsConfig, error)
	GetSvmsByKmsConfigID(ctx context.Context, kmsConfigID int64) ([]*datamodel.Svm, error)
	ListOngoingPoolJobsWithKmsConfigId(ctx context.Context, kmsId, accountId int64) ([]*datamodel.Job, error)

	CreateKmsConfig(ctx context.Context, kmsConfig *datamodel.KmsConfig) (*datamodel.KmsConfig, error)
	GetKmsConfigByUUID(ctx context.Context, uuid string) (*datamodel.KmsConfig, error)
	UpdateKmsConfigAttributes(ctx context.Context, uuid string, attributes *datamodel.KmsAttributes) (*datamodel.KmsConfig, error)
	GetJobByResourceUUID(ctx context.Context, kmsConfigUUID string) (*datamodel.Job, error)
	UpdateKmsConfigDetails(ctx context.Context, uuid string, fullKeyPath string, resourceID string) (*datamodel.KmsConfig, error)
	GetKmsConfigByKeyFullPath(ctx context.Context, keyFullPath string) (*datamodel.KmsConfig, error)
	UpdateKmsConfig(ctx context.Context, kmsUUID string, updates map[string]interface{}) error

	CreateKmsServiceAccount(ctx context.Context, serviceAccount *datamodel.ServiceAccount) (*datamodel.ServiceAccount, error)
	UpdateServiceAccountEmailAndKey(ctx context.Context, uuid string, email string, key string) (*datamodel.ServiceAccount, error)
	UpdateServiceAccountState(ctx context.Context, uuid string, state string, stateDetails string) (*datamodel.ServiceAccount, error)
	GetServiceAccountFromEmail(ctx context.Context, email string) (*datamodel.ServiceAccount, error)

	GetBackupVaultByNameAndOwnerID(ctx context.Context, backupVaultName, ownerID string) (*datamodel.BackupVault, error)
	CreatingBackupVault(ctx context.Context, bv *datamodel.BackupVault) (*datamodel.BackupVault, error)
	ListBackupVaults(ctx context.Context, accountID int64) ([]*datamodel.BackupVault, error)
	GetBackupVaultByUUIDndOwnerID(ctx context.Context, backupVaultUUID string, accountID int64) (*datamodel.BackupVault, error)
	GetBackupByNameAndBackupVaultID(ctx context.Context, backupName string, backupVaultID int64) (*datamodel.Backup, error)
	CreateBackupVaultEntryInVCP(ctx context.Context, bv *datamodel.BackupVault) (*datamodel.BackupVault, error)
	UpdateBackupVault(ctx context.Context, backupVault *datamodel.BackupVault) error
	GetBackupVault(ctx context.Context, backupVaultId string) (*datamodel.BackupVault, error)
	UpdateBackupVaultState(ctx context.Context, bv *datamodel.BackupVault, state, stateDetails string) (*datamodel.BackupVault, error)
	UpdateBackupVaultInVCP(ctx context.Context, vault *datamodel.BackupVault, vcpVault *datamodel.BackupVault) (*datamodel.BackupVault, error)

	GetBackupPolicyByUUIDAndOwnerID(ctx context.Context, backupPolicyUUID string, accountID int64) (*datamodel.BackupPolicy, error)
	GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName string, accountID int64) (*datamodel.BackupPolicy, error)
	ListBackupPolicyVolumeCount(ctx context.Context, conditions [][]interface{}) (map[string]int64, error)
	CreateBackupPolicyEntryInVCP(ctx context.Context, backupPolicy *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error)

	CreateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error)
	GetBackup(ctx context.Context, backupVaultUUID string, backupUUID string, accountName string) (*datamodel.Backup, error)
	DeleteBackup(ctx context.Context, backupUUID string) (*datamodel.Backup, error)
	UpdateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error)
	FinishBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error)
	UpdateBackupState(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error)
	IsBackupInCreatingorDeletingStateByVolume(ctx context.Context, volumeUUID string) (bool, error)
	IsLatestBackup(ctx context.Context, backupUUID, volumeUUID string) (bool, error)
	BackupCountByVolumeID(ctx context.Context, volumeUUID string) (int64, error)

	CreateAdminJobSpec(ctx context.Context, jobSpec *datamodel.AdminJobSpec) (*datamodel.AdminJobSpec, error)
	GetAdminJobSpecByJobType(ctx context.Context, jobType string) (*datamodel.AdminJobSpec, error)
	UpdateAdminJobSpec(ctx context.Context, jobSpec *datamodel.AdminJobSpec) error
	GetAdminJobSpecsByState(ctx context.Context, state string) ([]*datamodel.AdminJobSpec, error)

	ErroredResource(ctx context.Context, resource interface{}, errorMessage string) (interface{}, error)
	GetBackupsByBackupVaultOwnerIDAndFilter(ctx context.Context, backupVaultUUID string, accountID int64, filters [][]interface{}) ([]*datamodel.Backup, error)
}
