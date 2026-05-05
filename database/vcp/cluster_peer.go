package database

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

// GetClusterPeerByAccountIDExternalClusterAndPoolID retrieves a cluster peer by account ID, external cluster name, and pool ID
func (d *DataStoreRepository) GetClusterPeerByAccountIDExternalClusterAndPoolID(ctx context.Context, accountID int64, onPrempCluster string, poolID int64) (*datamodel.ClusterPeerings, error) {
	return getClusterPeerByAccountIDExternalClusterAndPoolID(d.db.GORM().WithContext(ctx), accountID, onPrempCluster, poolID)
}

// GetClusterPeeringRowByID retrieves a cluster peering row by primary key (cluster_peerings.id).
func (d *DataStoreRepository) GetClusterPeeringRowByID(ctx context.Context, clusterPeerID int64) (*datamodel.ClusterPeerings, error) {
	return getClusterPeeringRowByID(d.db.GORM().WithContext(ctx), clusterPeerID)
}

// CreateClusterPeeringRow creates a new cluster peering row in the database
func (d *DataStoreRepository) CreateClusterPeeringRow(ctx context.Context, clusterPeeringRow *datamodel.ClusterPeerings) (*datamodel.ClusterPeerings, error) {
	return createClusterPeeringRow(d.db.GORM().WithContext(ctx), clusterPeeringRow)
}

// UpdateClusterPeeringRow updates an existing cluster peering row in the database
func (d *DataStoreRepository) UpdateClusterPeeringRow(ctx context.Context, clusterPeeringRow *datamodel.ClusterPeerings) error {
	return updateClusterPeeringRow(d.db.GORM().WithContext(ctx), clusterPeeringRow)
}

// ListClusterPeeringRowsByAccountID retrieves all cluster peering rows for a given account ID
func (d *DataStoreRepository) ListClusterPeeringRowsByAccountID(ctx context.Context, accountID int64) ([]*datamodel.ClusterPeerings, error) {
	return listClusterPeeringRowsByAccountID(d.db.GORM().WithContext(ctx), accountID)
}

func (d *DataStoreRepository) DeleteClusterPeeringRow(ctx context.Context, clusterPeeringRow *datamodel.ClusterPeerings) error {
	return deleteClusterPeeringRow(d.db.GORM().WithContext(ctx), clusterPeeringRow)
}

// ListClusterPeeringRowsByPoolID retrieves all cluster peering rows for a given pool ID
func (d *DataStoreRepository) ListClusterPeeringRowsByPoolID(ctx context.Context, poolID int64) ([]*datamodel.ClusterPeerings, error) {
	return listClusterPeeringRowsByPoolID(d.db.GORM().WithContext(ctx), poolID)
}

// getClusterPeerByAccountIDExternalClusterAndPoolID retrieves a cluster peer by account ID, external cluster name, and pool ID
func getClusterPeerByAccountIDExternalClusterAndPoolID(db *gorm.DB, accountID int64, onPrempCluster string, poolID int64) (*datamodel.ClusterPeerings, error) {
	clusterPeeringRow := &datamodel.ClusterPeerings{}
	err := db.Where("account_id = ? AND onpremp_cluster = ? AND pool_id = ?", accountID, onPrempCluster, poolID).First(clusterPeeringRow).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			contextMap := map[string]interface{}{
				"account_id":      accountID,
				"onpremp_cluster": onPrempCluster,
				"pool_id":         poolID,
			}
			contextJSON, _ := json.Marshal(contextMap)
			contextStr := string(contextJSON)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrClusterPeerNotFound, customerrors.NewNotFoundErr("cluster peer", &contextStr))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return clusterPeeringRow, nil
}

func getClusterPeeringRowByID(db *gorm.DB, clusterPeerID int64) (*datamodel.ClusterPeerings, error) {
	clusterPeeringRow := &datamodel.ClusterPeerings{}
	err := db.First(clusterPeeringRow, clusterPeerID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			contextMap := map[string]interface{}{
				"cluster_peer_id": clusterPeerID,
			}
			contextJSON, _ := json.Marshal(contextMap)
			contextStr := string(contextJSON)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrClusterPeerNotFound, customerrors.NewNotFoundErr("cluster peer", &contextStr))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return clusterPeeringRow, nil
}

// createClusterPeeringRow creates a new cluster peering row in the database
func createClusterPeeringRow(db *gorm.DB, clusterPeeringRow *datamodel.ClusterPeerings) (*datamodel.ClusterPeerings, error) {
	err := db.Create(clusterPeeringRow).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}
	return clusterPeeringRow, nil
}

// updateClusterPeeringRow updates an existing cluster peering row in the database
func updateClusterPeeringRow(db *gorm.DB, clusterPeeringRow *datamodel.ClusterPeerings) error {
	err := db.Save(clusterPeeringRow).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}
	return nil
}

// listClusterPeeringRowsByAccountID retrieves all cluster peering rows for a given account ID
func listClusterPeeringRowsByAccountID(db *gorm.DB, accountID int64) ([]*datamodel.ClusterPeerings, error) {
	var clusterPeeringRows []*datamodel.ClusterPeerings
	err := db.Where("account_id = ?", accountID).Find(&clusterPeeringRows).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "cluster peering rows", nil))
	}
	return clusterPeeringRows, nil
}

// deleteClusterPeeringRow doing soft-delete of a cluster peering row in the database
// keeping the record by setting deleted_at and updated_at fields
func deleteClusterPeeringRow(db *gorm.DB, clusterPeeringRow *datamodel.ClusterPeerings) error {
	res := db.Delete(clusterPeeringRow)
	if res.Error != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, res.Error)
	}
	if res.RowsAffected == 0 {
		return vsaerrors.NewVCPError(
			vsaerrors.ErrClusterPeerNotFound,
			customerrors.NewNotFoundErr("cluster peer", nil),
		)
	}
	return nil
}

// listClusterPeeringRowsByPoolID retrieves all cluster peering rows for a given pool ID
func listClusterPeeringRowsByPoolID(db *gorm.DB, poolID int64) ([]*datamodel.ClusterPeerings, error) {
	var clusterPeeringRows []*datamodel.ClusterPeerings
	err := db.Where("pool_id = ?", poolID).Find(&clusterPeeringRows).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "cluster peering rows", nil))
	}
	return clusterPeeringRows, nil
}
