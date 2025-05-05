package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

var (
	getLifWithDetails = _getLifWithDetails
)

func (d *DataStoreRepository) CreateLif(ctx context.Context, lif *datamodel.Lif) (*datamodel.Lif, error) {
	var dbLif datamodel.Lif
	err := d.db.GORM().WithContext(ctx).Where("name = ? and node_id = ? and account_id = ?", lif.Name, lif.NodeID, lif.AccountID).First(&dbLif).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		lif.UUID = utils.RandomUUID()
		lif.CreatedAt = time.Now()
		lif.UpdatedAt = lif.CreatedAt
		err = d.db.GORM().WithContext(ctx).Create(lif).Error
		if err != nil {
			return nil, err
		}
		err = d.db.GORM().WithContext(ctx).Where("name = ? and account_id = ?", lif.Name, lif.AccountID).First(&dbLif).Error
		if err != nil {
			return nil, err
		}
		return &dbLif, nil
	}
	return nil, errors.New("lif already exists")
}

func (d *DataStoreRepository) GetLifForNode(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error) {
	lif, err := getLifWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Lif{NodeID: nodeID, AccountID: accountID})
	if err != nil {
		return nil, err
	}
	return lif, nil
}

func _getLifWithDetails(db *gorm.DB, query *datamodel.Lif) (*datamodel.Lif, error) {
	lif := &datamodel.Lif{}
	err := db.First(&lif, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "lif", nil)
	}
	return lif, nil
}
