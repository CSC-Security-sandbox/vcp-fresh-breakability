package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

// GetNodesByPoolID retrieves nodes by their corresponding pool ID
func (d *DataStoreRepository) GetNodesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Node, error) {
	return getNodesByPoolID(d.db.GORM().Unscoped().WithContext(ctx), poolID)
}

// GetNodeByID retrieves a node by ID. Uses Unscoped to include soft-deleted nodes so callers can check State.
func (d *DataStoreRepository) GetNodeByID(ctx context.Context, nodeID int64) (*datamodel.Node, error) {
	var node datamodel.Node
	err := d.db.GORM().Unscoped().WithContext(ctx).First(&node, nodeID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "node", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return &node, nil
}

func getNodesByPoolID(db *gorm.DB, poolID int64) ([]*datamodel.Node, error) {
	var nodes []*datamodel.Node
	err := db.Where("pool_id = ?", poolID).Order("id ASC").Find(&nodes).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "node", nil))
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
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	var dbNode datamodel.Node
	err1 := tx.Where("name = ? and account_id = ?", node.Name, node.AccountID).First(&dbNode).Error
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		node.UUID = utils.RandomUUID()
		node.State = datamodel.LifeCycleStateREADY
		node.StateDetails = datamodel.LifeCycleStateAvailableDetails
		node.CreatedAt = time.Now()
		node.UpdatedAt = node.CreatedAt
		err = tx.Create(node).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
		}
		err = tx.Where("name = ? and account_id = ?", node.Name, node.AccountID).First(&dbNode).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
		return &dbNode, nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if node exists: %v", err1)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err1)
	}

	return nil, vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, customerrors.NewConflictErr("node already exists"))
}

// DeleteNode deletes a Node from the database
func (d *DataStoreRepository) DeleteNode(ctx context.Context, node *datamodel.Node) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	node.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	node.State = datamodel.LifeCycleStateDeleted
	node.StateDetails = datamodel.LifeCycleStateDeletedDetails
	err = tx.Updates(node).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

// ErroredNode marks a Node state to error in the database
func (d *DataStoreRepository) ErroredNode(ctx context.Context, node *datamodel.Node, errMsg string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	node.UpdatedAt = time.Now()
	node.State = datamodel.LifeCycleStateError
	node.StateDetails = errMsg
	err = tx.Updates(node).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
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
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	node.State = datamodel.LifeCycleStateDeleting
	node.StateDetails = datamodel.LifeCycleStateDeletingDetails
	err = tx.Updates(node).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

// UpdateNodesInstanceType updates the instance type for all nodes in a pool
func (d *DataStoreRepository) UpdateNodesInstanceType(ctx context.Context, poolID int64, newInstanceType string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	// Get all nodes for the pool
	nodes, err := getNodesByPoolID(tx, poolID)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		logger.Warnf("No nodes found for pool ID %d", poolID)
		return nil
	}

	// Update instance type for each node
	for _, node := range nodes {
		if node.NodeAttributes == nil {
			node.NodeAttributes = &datamodel.NodeDetails{}
		}

		// Update instance type
		node.NodeAttributes.InstanceType = newInstanceType
		node.UpdatedAt = time.Now()

		err = tx.Model(&datamodel.Node{}).Where("id = ?", node.ID).Updates(map[string]interface{}{
			"node_attributes": node.NodeAttributes,
			"updated_at":      node.UpdatedAt,
		}).Error
		if err != nil {
			logger.Errorf("Failed to update instance type for node %s: %v", node.UUID, err)
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
		}
	}

	logger.Infof("Successfully updated instance type to %s for %d nodes in pool %d", newInstanceType, len(nodes), poolID)
	return nil
}

func (d *DataStoreRepository) UpdateNodesSizeAndInstanceType(ctx context.Context, poolID int64, updatesByNodeName map[string]datamodel.NodeDetails) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	nodes, err := getNodesByPoolID(tx, poolID)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		logger.Warnf("No nodes found for pool ID %d", poolID)
		return nil
	}

	updatedCount := 0
	for _, node := range nodes {
		update, ok := updatesByNodeName[node.Name]
		if !ok {
			continue
		}
		if node.NodeAttributes == nil {
			node.NodeAttributes = &datamodel.NodeDetails{}
		}
		node.NodeAttributes.InstanceType = update.InstanceType
		node.NodeAttributes.SizeInGiB = update.SizeInGiB
		node.UpdatedAt = time.Now()

		err = tx.Model(&datamodel.Node{}).Where("id = ?", node.ID).Updates(map[string]interface{}{
			"node_attributes": node.NodeAttributes,
			"updated_at":      node.UpdatedAt,
		}).Error
		if err != nil {
			logger.Errorf("Failed to update size and instance type for node %s: %v", node.UUID, err)
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
		}
		updatedCount++
	}

	logger.Infof("Successfully updated size and instance type for %d nodes in pool %d", updatedCount, poolID)
	return nil
}
