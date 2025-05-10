package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

// GetNodesByPoolID retrieves nodes by their corresponding pool ID
func (d *DataStoreRepository) GetNodesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Node, error) {
	return getNodesByPoolID(d.db.GORM().Unscoped().WithContext(ctx), poolID)
}

func getNodesByPoolID(db *gorm.DB, poolID int64) ([]*datamodel.Node, error) {
	var nodes []*datamodel.Node
	err := db.Where("pool_id = ?", poolID).Find(&nodes).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "node", nil)
	}
	return nodes, nil
}

// CreateNode creates a new Node entry in the database
func (d *DataStoreRepository) CreateNode(ctx context.Context, node *datamodel.Node) (*datamodel.Node, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	// Fixme: The logger should be fetched from ctx
	logger := slogger.NewLogger()
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	var dbNode datamodel.Node
	err1 := tx.Where("name = ? and account_id = ?", node.Name, node.AccountID).First(&dbNode).Error
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		node.UUID = utils.RandomUUID()
		node.State = models.LifeCycleStateREADY
		node.StateDetails = models.LifeCycleStateAvailableDetails
		node.CreatedAt = time.Now()
		node.UpdatedAt = node.CreatedAt
		err = tx.Create(node).Error
		if err != nil {
			return nil, err
		}
		err = tx.Where("name = ? and account_id = ?", node.Name, node.AccountID).First(&dbNode).Error
		if err != nil {
			return nil, err
		}
		return &dbNode, nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if node exists: %v", err1)
		return nil, err1
	}

	return nil, customerrors.NewConflictErr("node already exists")
}

// DeleteNode deletes a Node from the database
func (d *DataStoreRepository) DeleteNode(ctx context.Context, node *datamodel.Node) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	// Fixme: The logger should be fetched from ctx
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	node.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	node.State = models.LifeCycleStateDeleted
	node.StateDetails = models.LifeCycleStateDeletedDetails
	err = tx.Updates(node).Error
	if err != nil {
		return err
	}
	return nil
}

// DeletingNode updates the node entry to deleting state
func (d *DataStoreRepository) DeletingNode(ctx context.Context, node *datamodel.Node) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	// Fixme: The logger should be fetched from ctx
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	node.State = models.LifeCycleStateDeleting
	node.StateDetails = models.LifeCycleStateDeletingDetails
	err = tx.Updates(node).Error
	if err != nil {
		return err
	}
	return nil
}
