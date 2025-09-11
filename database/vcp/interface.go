package database

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormWrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"gorm.io/gorm"
)

type DataStoreRepository struct {
	db *gormWrapper.Wrapper
}

func NewDataStoreRepository(db *gormWrapper.Wrapper) *DataStoreRepository {
	return &DataStoreRepository{db: db}
}

type (
	Storage interface {
		Connect(isAdmin bool) error
		Close() error
		HealthCheck() error
		WithTransaction(ctx context.Context, fn func(dbutils.Transaction) error) error
		Migrate(ctx context.Context) error
		Rollback(ctx context.Context) error
		DB() *gorm.DB
		SetupDatabase(ctx context.Context) error

		// Embed DataStore interface
		DataStore
	}

	// DataStore defines all operations
	DataStore interface {
		CreatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error)
		CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error)
		UpdatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error)
		UpdatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error)
		UpdatePoolSubnetNames(ctx context.Context, poolUUID, snHostProject string, subnetNames []string) error
		UpdatePoolState(ctx context.Context, pool *datamodel.Pool, state string, stateDetails string) (*datamodel.Pool, error)
		UpdatePoolFields(ctx context.Context, poolUUID string, updates map[string]interface{}) error
		DeletePool(ctx context.Context, pool *datamodel.Pool) error
		DeletingPool(ctx context.Context, pool *datamodel.Pool) error
		DescribePool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error)
		GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error)
		ListPools(ctx context.Context, filter *dbutils.Filter) ([]*datamodel.PoolView, error)
		ListPoolUUIDs(ctx context.Context, filter *dbutils.Filter) ([]*PoolIdentifier, error)
		GetPoolByVendorID(ctx context.Context, vendorID string, accountID int64) (*datamodel.PoolView, error)
		GetPoolByName(ctx context.Context, conditions [][]interface{}) (*datamodel.PoolView, error)
		SavePoolWithVsaDetails(ctx context.Context, pool *datamodel.Pool, cluster *datamodel.ClusterDetails) error
		UpdatePoolWithKmsConfigID(ctx context.Context, pool *datamodel.Pool, kmsConfigUUID string) (*datamodel.Pool, error)
		GetPoolsByAccountName(ctx context.Context, accountName string) ([]*datamodel.Pool, error)
		GetNextSerialNumberInRegion(ctx context.Context, region string) (string, error)
		ListTpProjects(ctx context.Context) ([]string, error)

		CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error)
		GetVolume(ctx context.Context, id string) (*datamodel.Volume, error)
		DescribeVolume(ctx context.Context, id string) (*datamodel.Volume, error)
		GetVolumeWithAccountID(ctx context.Context, id string, accountID int64) (*datamodel.Volume, error)
		GetVolumeByNameAndAccountID(ctx context.Context, name string, accountID int64) (*datamodel.Volume, error)
		GetVolumeByNameAccountIDAndZone(ctx context.Context, name string, accountID int64, primaryZone string) (*datamodel.Volume, error)
		GetVolumeCount(ctx context.Context, accountName string) (int64, error)
		GetVolumeByName(ctx context.Context, name string) (*datamodel.Volume, error)
		UpdateVolume(ctx context.Context, volume *datamodel.Volume) error
		RevertedVolume(ctx context.Context, volume *datamodel.Volume, snapshot *datamodel.Snapshot) ([]*datamodel.Snapshot, error)
		UpdateVolumeFields(ctx context.Context, volumeUUID string, updates map[string]interface{}) error
		DeleteVolume(ctx context.Context, id string) (*datamodel.Volume, error)
		DeleteVolumeAndChildResources(ctx context.Context, volumeUUID string) (*datamodel.Volume, error)
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
		UpdateVolumeReplicationFields(ctx context.Context, volumeRepUUID string, updates map[string]interface{}) error
		UpdateVolumeReplicationStates(ctx context.Context, volumeRep *datamodel.VolumeReplication) error
		UpdateVolumeReplicationTransferStats(ctx context.Context, volumeRep *datamodel.VolumeReplication) error
		DeleteVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error)
		GetVolumeReplicationByProjectId(ctx context.Context, accountId int64) ([]*datamodel.VolumeReplication, error)
		GetVolumeReplicationCount(ctx context.Context, accountName string) (int64, error)
		GetVolumeReplicationCountByVolumeID(ctx context.Context, volumeID int64) (int64, error)
		ListVolumeReplications(ctx context.Context, filter dbutils.Filter) ([]*datamodel.VolumeReplication, error)

		GetAccount(ctx context.Context, name string) (*datamodel.Account, error)
		CreateAccount(ctx context.Context, account *datamodel.Account) (*datamodel.Account, error)
		GetAccountByUUID(ctx context.Context, uuid string) (*datamodel.Account, error)

		CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error)
		DeleteJob(ctx context.Context, id, errorDetails string) error
		UpdateJob(ctx context.Context, jobID string, status string, trackingID int, errorDetails string) error
		GetJob(ctx context.Context, jobID string) (*datamodel.Job, error)
		GetJobsWithCondition(ctx context.Context, filter dbutils.Filter) ([]*datamodel.Job, error)
		GetOngoingMigrateKmsConfigJob(ctx context.Context, accountId int64) (*datamodel.Job, error)

		GetSvmForPoolID(ctx context.Context, poolID int64) (*datamodel.Svm, error)
		GetNodesByPoolID(ctx context.Context, poolId int64) ([]*datamodel.Node, error)
		CreateNode(ctx context.Context, node *datamodel.Node) (*datamodel.Node, error)

		CreateSVM(ctx context.Context, svm *datamodel.Svm) (*datamodel.Svm, error)
		GetSvmsByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Svm, error)
		GetNextSVMIndexByPoolID(ctx context.Context, poolID int64) (int64, error)
		UpdateSvmWithKmsConfigIDs(ctx context.Context, svm *datamodel.Svm, gcpKmsConfigUUID, externalGcpKmsConfigUUID string) (*datamodel.Svm, error)

		CreateLif(ctx context.Context, lif *datamodel.Lif) (*datamodel.Lif, error)
		GetLifForNode(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error)

		CreateHostGroup(ctx context.Context, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error)
		GetHostGroup(ctx context.Context, id string, accountID int64) (*datamodel.HostGroup, error)
		GetMultipleHostGroups(ctx context.Context, ids []string, accountID int64) ([]*datamodel.HostGroup, error)
		DeleteHostGroup(ctx context.Context, hostGroupUUID string, accountID int64) (*datamodel.HostGroup, error)
		UpdateHostGroupsState(ctx context.Context, hostGroupUUID []string, accountID int64, state string, stateDetails string) error
		UpdateHostGroup(ctx context.Context, hostGroupUUID string, accountID int64, description *string, Hosts *[]string) (*datamodel.HostGroup, error)
		ListHostGroupsByAccountID(ctx context.Context, accountID int64) ([]*datamodel.HostGroup, error)

		GetLifsForNodesWithProtocol(ctx context.Context, nodeIDs []int64, accountID int64, protocol string) ([]*datamodel.Lif, error)
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
		UpdateSnapshotForHandleResource(ctx context.Context, snapshot *datamodel.Snapshot) (*datamodel.Snapshot, error)
		GetSnapshotByUUID(ctx context.Context, uuid string, accountID int64, volumeID int64) (*datamodel.Snapshot, error)
		GetSnapshotByNameAndVolumeId(ctx context.Context, snapshotName string, accountID int64, volumeID int64) (*datamodel.Snapshot, error)
		GetSnapshotByPoolID(ctx context.Context, SnapshotUUID string, accountID int64, poolID int64, isParentSnapshot bool) (*datamodel.Snapshot, error)
		GetSnapshotsByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.Snapshot, error)
		GetWronglyDeletedSnapshot(ctx context.Context, snapshotExternalUUID string) (*datamodel.Snapshot, error)
		UnDeleteSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error
		GetSnapshotsByVolumeIDs(ctx context.Context, volumeID []int64) ([]*datamodel.Snapshot, error)
		GetReplicationSnapshotsByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.Snapshot, error)
		GetSnapshotsWithCondition(ctx context.Context, filter dbutils.Filter) ([]*datamodel.Snapshot, error)
		GetAppConsistentSnapshotsForVolume(ctx context.Context, accountID, volumeID int64) ([]*datamodel.Snapshot, error)
		GetSnapshotsByTypeAndVolumeID(ctx context.Context, snapshotType string, volumeID int64) ([]*datamodel.Snapshot, error)
		DeleteSnapshot(ctx context.Context, id string) (*datamodel.Snapshot, error)
		DeletingSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error
		BatchDeleteSnapshots(ctx context.Context, snapshotIDs []int64) ([]*datamodel.Snapshot, error)
		BatchCreateSnapshots(ctx context.Context, newSnapshots []*datamodel.Snapshot, returnCreatedSnapshotUUIDs bool) ([]string, error)
		BatchUpdateSnapshots(ctx context.Context, snapshots []*datamodel.Snapshot) error
		BatchUnDeleteSnapshots(ctx context.Context, snapshots []*datamodel.Snapshot) error
		BatchGetSnapshotsByUUIDs(ctx context.Context, snapshotUUIDs []string) ([]*datamodel.Snapshot, error)
		BatchGetWronglyDeletedSnapshots(ctx context.Context, snapshotExternalUUIDs []string) ([]*datamodel.Snapshot, error)

		GetMultipleKmsConfigs(ctx context.Context, conditions [][]interface{}) ([]*datamodel.KmsConfig, error)
		GetKmsConfig(ctx context.Context, kmsConfigUUID string) (*datamodel.KmsConfig, error)
		UpdateKmsConfigState(ctx context.Context, kmsConfigUUID string, state string, stateDetails string) (*datamodel.KmsConfig, error)
		DeleteKmsConfig(ctx context.Context, kmsConfigUUID, state, stateDetails string) (*datamodel.KmsConfig, error)
		GetSvmsByKmsConfigID(ctx context.Context, kmsConfigID int64) ([]*datamodel.Svm, error)
		ListOngoingPoolJobsWithKmsConfigId(ctx context.Context, kmsId, accountId int64) ([]*datamodel.Job, error)
		UpdateKmsConfigStateForHandleResource(ctx context.Context, kmsConfigUUID string, stateDetails string, event string) (*datamodel.KmsConfig, error)

		CreateKmsConfig(ctx context.Context, kmsConfig *datamodel.KmsConfig) (*datamodel.KmsConfig, error)
		GetKmsConfigByUUID(ctx context.Context, uuid string) (*datamodel.KmsConfig, error)
		UpdateKmsConfigAttributes(ctx context.Context, uuid string, attributes *datamodel.KmsAttributes) (*datamodel.KmsConfig, error)
		GetJobByResourceUUID(ctx context.Context, resourceUUID string, jobType string) (*datamodel.Job, error)
		UpdateKmsConfigDetails(ctx context.Context, uuid string, fullKeyPath string, resourceID string) (*datamodel.KmsConfig, error)
		GetKmsConfigByKeyFullPath(ctx context.Context, keyFullPath string, accountID int64) (*datamodel.KmsConfig, error)
		UpdateKmsConfig(ctx context.Context, kmsUUID string, updates map[string]interface{}) error
		IsKmsConfigInUse(ctx context.Context, kmsConfigUUID string) (bool, error)
		ListKmsConfigByAccountID(ctx context.Context, accountID int64) ([]*datamodel.KmsConfig, error)

		CreateKmsServiceAccount(ctx context.Context, serviceAccount *datamodel.ServiceAccount) (*datamodel.ServiceAccount, error)
		UpdateServiceAccountEmailAndKey(ctx context.Context, uuid string, email string, key string) (*datamodel.ServiceAccount, error)
		UpdateServiceAccountState(ctx context.Context, uuid string, state string, stateDetails string) (*datamodel.ServiceAccount, error)
		GetServiceAccountFromEmail(ctx context.Context, email string) (*datamodel.ServiceAccount, error)
		ListKmsServiceAccounts(ctx context.Context, filter *dbutils.Filter) ([]*datamodel.ServiceAccount, error)

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
		GetMultipleBackupVaults(ctx context.Context, conditions [][]interface{}) ([]*datamodel.BackupVault, error)
		DeleteBackupVaultInVCP(ctx context.Context, backupVaultId string) (*datamodel.BackupVault, error)
		GetVolumeCountByBackupVaultID(ctx context.Context, backupVaultUUID string) (int64, error)
		GetBackupCountByBackupVaultID(ctx context.Context, backupVaultID int64) (int64, error)

		GetBackupPolicyByUUIDAndOwnerID(ctx context.Context, backupPolicyUUID string, accountID int64) (*datamodel.BackupPolicy, error)
		GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName string, accountID int64) (*datamodel.BackupPolicy, error)
		GetVolumeCountByBackupPolicyID(ctx context.Context, backupPolicyUUID string) (int64, error)
		ListBackupPolicyVolumeCount(ctx context.Context, conditions [][]interface{}) (map[string]int64, error)
		ListBackupPolicies(ctx context.Context, conditions [][]interface{}) ([]*datamodel.BackupPolicy, error)
		CreateBackupPolicyEntryInVCP(ctx context.Context, backupPolicy *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error)
		UpdateBackupPolicy(ctx context.Context, uuid string, updates map[string]interface{}) (*datamodel.BackupPolicy, error)
		DeleteBackupPolicy(ctx context.Context, backupPolicyUUID string) (*datamodel.BackupPolicy, error)

		CreateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error)
		GetBackup(ctx context.Context, backupVaultUUID string, backupUUID string, accountName string) (*datamodel.Backup, error)
		DeleteBackup(ctx context.Context, backupUUID string) (*datamodel.Backup, error)
		UpdateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error)
		FinishBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error)
		UpdateBackupState(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error)
		IsBackupInCreatingorDeletingStateByVolume(ctx context.Context, volumeUUID string) (bool, error)
		IsLatestBackup(ctx context.Context, backupUUID, volumeUUID string) (bool, error)
		BackupCountByVolumeID(ctx context.Context, volumeUUID string) (int64, error)
		FetchScheduledBackupsForDeletion(ctx context.Context, volume *datamodel.Volume, backupPolicy *datamodel.BackupPolicy) ([]*datamodel.Backup, error)
		IsBackupShared(ctx context.Context, backup *datamodel.Backup) (bool, error)
		GetBackupCountByVolumeUUIDs(ctx context.Context, volumeUUIDs []string, conditions [][]interface{}) (map[string]int64, error)
		GetBackupsByVolumeUUID(ctx context.Context, volumeUUID string) ([]*datamodel.Backup, error)
		UpdateBackupLatestLogicalBackupSizeByVolume(ctx context.Context, volumeUUID, excludeBackupUUID string) error

		CreateAdminJobSpec(ctx context.Context, jobSpec *datamodel.AdminJobSpec) (*datamodel.AdminJobSpec, error)
		GetAdminJobSpecByJobType(ctx context.Context, jobType string) (*datamodel.AdminJobSpec, error)
		UpdateAdminJobSpec(ctx context.Context, jobSpec *datamodel.AdminJobSpec) error
		GetAdminJobSpecsByState(ctx context.Context, state string) ([]*datamodel.AdminJobSpec, error)

		CreateNodeNodeGroupMap(ctx context.Context, mapping *datamodel.NodeNodeGroupMap) (*datamodel.NodeNodeGroupMap, error)
		GetNodeNodeGroupMap(ctx context.Context, id int64) (*datamodel.NodeNodeGroupMap, error)
		UpdateNodeNodeGroupMap(ctx context.Context, mapping *datamodel.NodeNodeGroupMap) (*datamodel.NodeNodeGroupMap, error)
		DeleteNodeNodeGroupMap(ctx context.Context, id int64) error
		// Below one performs soft delete on nodenodegroupmap table
		DeleteNodeGroupMap(ctx context.Context, nodeGroupMap *datamodel.NodeNodeGroupMap) error
		GetNodeGroupMapNodeCount(ctx context.Context, nodeGroupID int64) (int64, error)
		GetNodeNodeGroupMapByNodeID(ctx context.Context, nodeID int64) (*datamodel.NodeNodeGroupMap, error)

		CreateNodeGroup(ctx context.Context, group *datamodel.NodeGroup) (*datamodel.NodeGroup, error)
		GetNodeGroup(ctx context.Context, id int64) (*datamodel.NodeGroup, error)
		UpdateNodeGroup(ctx context.Context, group *datamodel.NodeGroup) (*datamodel.NodeGroup, error)
		DeleteNodeGroup(ctx context.Context, id int64) error

		ErroredResource(ctx context.Context, resource interface{}, errorMessage string) (interface{}, error)
		GetBackupsByBackupVaultOwnerIDAndFilter(ctx context.Context, backupVaultUUID string, accountID int64, filters [][]interface{}) ([]*datamodel.Backup, error)

		// AssignTwoNodesToTwoGroups assigns two nodes to two different node groups, ensuring no group exceeds maxNodesPerGroup nodes
		// Assumes that node1 and node2 are precreated and have valid IDs
		AssignTwoNodesToTwoGroups(ctx context.Context, params datamodel.NodeGroupAssignmentParams) ([]*datamodel.NodeNodeGroupMap, error)
	}
)
