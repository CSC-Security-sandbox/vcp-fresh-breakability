package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"gorm.io/gorm"
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
