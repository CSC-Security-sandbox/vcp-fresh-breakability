package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

var (
	getLifWithDetails = _getLifWithDetails
)

// GetLifByNodeID retrieves a LIF by its corresponding Node ID
func (d *DataStoreRepository) GetLifByNodeID(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error) {
	lif, err := getLifWithDetails(d.db.GORM().Unscoped().WithContext(ctx), &datamodel.Lif{NodeID: nodeID, AccountID: accountID})
	if err != nil {
		return nil, err
	}
	return lif, nil
}

// CreateLif creates a new LIF in the database
func (d *DataStoreRepository) CreateLif(ctx context.Context, lif *datamodel.Lif) (*datamodel.Lif, error) {
	var dbLif datamodel.Lif
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	// Fixme: The logger should be fetched from ctx
	logger := slogger.NewLogger()
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	err1 := tx.Where("name = ? and node_id = ? and account_id = ?", lif.Name, lif.NodeID, lif.AccountID).First(&dbLif).Error
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		lif.UUID = utils.RandomUUID()
		lif.CreatedAt = time.Now()
		lif.UpdatedAt = lif.CreatedAt
		err = tx.Create(lif).Error
		if err != nil {
			return nil, err
		}
		err = tx.Where("name = ? and account_id = ?", lif.Name, lif.AccountID).First(&dbLif).Error
		if err != nil {
			return nil, err
		}
		return &dbLif, nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if lif exists: %v", err1)
		return nil, err1
	}
	return nil, customerrors.NewConflictErr("lif already exists")
}

// DeleteLif deletes a LIF from the database
func (d *DataStoreRepository) DeleteLif(ctx context.Context, lif *datamodel.Lif) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	// Fixme: The logger should be fetched from ctx
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	lif.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	err = tx.Updates(lif).Error
	if err != nil {
		return err
	}
	return nil
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
