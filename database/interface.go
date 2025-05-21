package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
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
	GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.Pool, error)
	UpdatePool(ctx context.Context, pool *datamodel.Pool) error
	DeletePool(ctx context.Context, pool *datamodel.Pool) error
	DeletingPool(ctx context.Context, pool *datamodel.Pool) error
	ListPools(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Pool, error)
	GetPoolByVendorID(ctx context.Context, vendorID string) (*datamodel.Pool, error)
	SavePoolWithVsaClusterDetails(ctx context.Context, pool *datamodel.Pool, cluster *datamodel.ClusterDetails) error

	CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error)
	GetVolume(ctx context.Context, id string) (*datamodel.Volume, error)
	UpdateVolume(ctx context.Context, volume *datamodel.Volume) error
	DeleteVolume(ctx context.Context, id string) (*datamodel.Volume, error)
	UpdateVolumeState(ctx context.Context, id string, state string, stateDetails string) (*datamodel.Volume, error)
	ListVolumes(ctx context.Context) ([]*datamodel.Volume, error)
	GetVolumesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Volume, error)
	GetVolumeCountByPoolID(ctx context.Context, poolID int64) (int64, error)
	GetMultipleVolumes(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Volume, error)

	CreateVolumeReplication(ctx context.Context, volumeRep *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error)
	GetVolumeReplication(ctx context.Context, id string) (*datamodel.VolumeReplication, error)
	UpdateVolumeReplicationStates(ctx context.Context, volumeRep *datamodel.VolumeReplication) error
	UpdateVolumeReplicationTransferStats(ctx context.Context, volumeRep *datamodel.VolumeReplication) error
	DeleteVolumeReplication(ctx context.Context, volumeReplicationID string) (*datamodel.VolumeReplication, error)

	GetAccount(ctx context.Context, name string) (*datamodel.Account, error)
	CreateAccount(ctx context.Context, account *datamodel.Account) (*datamodel.Account, error)

	CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error)
	UpdateJob(ctx context.Context, jobID string, status string) error
	GetJob(ctx context.Context, jobID string) (*datamodel.Job, error)

	GetSvmForPoolID(ctx context.Context, poolID int64) (*datamodel.Svm, error)

	GetNodesByPoolID(ctx context.Context, poolId int64) ([]*datamodel.Node, error)
	CreateNode(ctx context.Context, node *datamodel.Node) (*datamodel.Node, error)

	CreateSVM(ctx context.Context, svm *datamodel.Svm) (*datamodel.Svm, error)
	GetSvmsByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Svm, error)

	CreateLif(ctx context.Context, lif *datamodel.Lif) (*datamodel.Lif, error)
	GetLifForNode(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error)

	CreateHostGroup(ctx context.Context, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error)
	GetHostGroup(ctx context.Context, id string, accountID int64) (*datamodel.HostGroup, error)
	GetMultipleHostGroups(ctx context.Context, ids []string, accountID int64) ([]*datamodel.HostGroup, error)
	GetLifByNodeID(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error)
	DeleteLif(ctx context.Context, lif *datamodel.Lif) error
	DeleteNode(ctx context.Context, node *datamodel.Node) error
	DeletingNode(ctx context.Context, node *datamodel.Node) error
	DeleteSVM(ctx context.Context, svm *datamodel.Svm) error
	DeletingSVM(ctx context.Context, svm *datamodel.Svm) error
}
