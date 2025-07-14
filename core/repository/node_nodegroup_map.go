package repository

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var generateRandomNodeGroup = _generateRandomNodeGroup

// CreateNodeNodeGroupMap creates a new node to nodegroup mapping
func (d *DataStoreRepository) CreateNodeNodeGroupMap(ctx context.Context, mapping *datamodel.NodeNodeGroupMap) (*datamodel.NodeNodeGroupMap, error) {
	tx := d.db.GORM().WithContext(ctx)
	mapping.CreatedAt = time.Now()
	mapping.UpdatedAt = mapping.CreatedAt
	err := tx.Create(mapping).Error
	if err != nil {
		// Check if this is a duplicate key error for node_id
		if strings.Contains(err.Error(), "idx_node_node_group_maps_node_id_unique") ||
			strings.Contains(err.Error(), "duplicate key") ||
			strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, errors.New("node is already assigned to a group"))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}
	return mapping, nil
}

// GetNodeNodeGroupMap retrieves a node to nodegroup mapping by ID
func (d *DataStoreRepository) GetNodeNodeGroupMap(ctx context.Context, id int64) (*datamodel.NodeNodeGroupMap, error) {
	var mapping datamodel.NodeNodeGroupMap
	err := d.db.GORM().WithContext(ctx).First(&mapping, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return &mapping, nil
}

// UpdateNodeNodeGroupMap updates an existing node to nodegroup mapping
func (d *DataStoreRepository) UpdateNodeNodeGroupMap(ctx context.Context, mapping *datamodel.NodeNodeGroupMap) (*datamodel.NodeNodeGroupMap, error) {
	tx := d.db.GORM().WithContext(ctx)
	mapping.UpdatedAt = time.Now()
	err := tx.Save(mapping).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return mapping, nil
}

// DeleteNodeNodeGroupMap deletes a node to nodegroup mapping by ID
func (d *DataStoreRepository) DeleteNodeNodeGroupMap(ctx context.Context, id int64) error {
	tx := d.db.GORM().WithContext(ctx)
	// Soft delete: update DeletedAt field instead of hard delete
	var mapping datamodel.NodeNodeGroupMap
	err := tx.First(&mapping, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, err)
		}
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}
	mapping.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	err = tx.Save(&mapping).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}
	return nil
}

// AssignTwoNodesToTwoGroups assigns two nodes to two different node groups, ensuring no group exceeds maxNodesPerGroup nodes
// Assumes that node1 and node2 are pre-created and have valid IDs
// This function is idempotent - if nodes are already assigned to groups, it returns the existing mappings
func (d *DataStoreRepository) AssignTwoNodesToTwoGroups(ctx context.Context, node1, node2 *datamodel.Node, maxNodesPerGroup int) ([]*datamodel.NodeNodeGroupMap, error) {
	logger := util.GetLogger(ctx)
	if node1 == nil || node2 == nil {
		logger.Errorf("AssignTwoNodesToTwoGroups: node1 or node2 is nil")
		return nil, errors.New("node1 or node2 is nil")
	}
	if node1.ID == node2.ID {
		logger.Errorf("AssignTwoNodesToTwoGroups: node1 and node2 must be different nodes (node1.ID=%d)", node1.ID)
		return nil, errors.New("node1 and node2 must be different nodes")
	}
	if maxNodesPerGroup <= 0 {
		logger.Errorf("AssignTwoNodesToTwoGroups: maxNodesPerGroup must be greater than zero (got %d)", maxNodesPerGroup)
		return nil, errors.New("maxNodesPerGroup must be greater than zero")
	}
	tx := d.db.GORM().WithContext(ctx)
	logger.Debugf("AssignTwoNodesToTwoGroups: node1.ID=%d, node2.ID=%d, maxNodesPerGroup=%d", node1.ID, node2.ID, maxNodesPerGroup)
	ctxWithTx := utils.WithTx(ctx, tx)

	// Check if nodes are already assigned to groups
	logger.Debugf("Checking existing mappings for node1.ID=%d and node2.ID=%d", node1.ID, node2.ID)
	var existingMapping1, existingMapping2 datamodel.NodeNodeGroupMap
	err1 := tx.Where("node_id = ?", node1.ID).First(&existingMapping1).Error
	err2 := tx.Where("node_id = ?", node2.ID).First(&existingMapping2).Error

	// If both nodes already have mappings, return them (idempotent behavior)
	if err1 == nil && err2 == nil {
		logger.Infof("Both nodes are already assigned: node1 (ID: %d) -> group %d, node2 (ID: %d) -> group %d",
			node1.ID, existingMapping1.NodeGroupID, node2.ID, existingMapping2.NodeGroupID)
		return []*datamodel.NodeNodeGroupMap{&existingMapping1, &existingMapping2}, nil
	}

	var group1, group2 datamodel.NodeGroup

	// Handle node1 assignment
	if err1 == nil {
		// Node1 already has a mapping, get its group
		logger.Infof("Node1 (ID: %d) is already assigned to group %d", node1.ID, existingMapping1.NodeGroupID)
		err := tx.Where("id = ?", existingMapping1.NodeGroupID).First(&group1).Error
		if err != nil {
			logger.Errorf("Failed to fetch group for node1.ID=%d, groupID=%d: %v", node1.ID, existingMapping1.NodeGroupID, err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
	} else if errors.Is(err1, gorm.ErrRecordNotFound) {
		// Node1 needs a new assignment, find an available group
		logger.Debugf("Node1 (ID: %d) not assigned, searching for available group (maxNodesPerGroup=%d)", node1.ID, maxNodesPerGroup)
		subquery := tx.Model(&datamodel.NodeNodeGroupMap{}).
			Select("node_group_id").
			Group("node_group_id").
			Having("COUNT(node_id) < ?", maxNodesPerGroup)

		err := tx.Model(&datamodel.NodeGroup{}).
			Where("id IN (?)", subquery).
			Limit(1).
			Find(&group1).Error
		if err != nil {
			logger.Errorf("Error searching for available group for node1.ID=%d: %v", node1.ID, err)
		}
		if group1.ID == 0 {
			logger.Infof("No available group found for node1.ID=%d, creating new group", node1.ID)
			group1Ptr, err := generateRandomNodeGroup(ctxWithTx, d, group1)
			if err != nil {
				logger.Errorf("Failed to create new group for node1.ID=%d: %v", node1.ID, err)
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
			}
			group1 = *group1Ptr
		}
	} else {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err1)
	}

	// Handle node2 assignment
	if err2 == nil {
		// Node2 already has a mapping, get its group
		logger.Infof("Node2 (ID: %d) is already assigned to group %d", node2.ID, existingMapping2.NodeGroupID)
		err := tx.Where("id = ?", existingMapping2.NodeGroupID).First(&group2).Error
		if err != nil {
			logger.Errorf("Failed to fetch group for node2.ID=%d, groupID=%d: %v", node2.ID, existingMapping2.NodeGroupID, err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
	} else if errors.Is(err2, gorm.ErrRecordNotFound) {
		// Node2 needs a new assignment, find an available group (different from group1)
		logger.Debugf("Node2 (ID: %d) not assigned, searching for available group (maxNodesPerGroup=%d, exclude group1.ID=%d)", node2.ID, maxNodesPerGroup, group1.ID)
		subquery := tx.Model(&datamodel.NodeNodeGroupMap{}).
			Select("node_group_id").
			Group("node_group_id").
			Having("COUNT(node_id) < ?", maxNodesPerGroup)

		err := tx.Model(&datamodel.NodeGroup{}).
			Where("id IN (?)", subquery).
			Where("id != ?", group1.ID).
			Limit(1).
			Find(&group2).Error
		if err != nil {
			logger.Errorf("Error searching for available group for node2.ID=%d: %v", node2.ID, err)
		}
		if group2.ID == 0 {
			logger.Infof("No available group found for node2.ID=%d, creating new group", node2.ID)
			group2Ptr, err := generateRandomNodeGroup(ctxWithTx, d, group2)
			if err != nil {
				logger.Errorf("Failed to create new group for node2.ID=%d: %v", node2.ID, err)
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
			}
			group2 = *group2Ptr
		}
	} else {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err2)
	}

	var mappings []*datamodel.NodeNodeGroupMap

	// Create mapping for node1 if it doesn't exist
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		logger.Infof("Creating new mapping for node1.ID=%d to group1.ID=%d", node1.ID, group1.ID)
		mapping1 := &datamodel.NodeNodeGroupMap{
			BaseModel:     datamodel.BaseModel{UUID: uuid.New().String()},
			NodeID:        node1.ID,
			NodeGroupID:   group1.ID,
			HarvestConfig: renderHarvestConfig(*node1),
		}
		if err := tx.Create(mapping1).Error; err != nil {
			logger.Errorf("Failed to create mapping for node1.ID=%d: %v", node1.ID, err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
		}
		mappings = append(mappings, mapping1)
	} else {
		logger.Debugf("Mapping for node1.ID=%d already exists", node1.ID)
		mappings = append(mappings, &existingMapping1)
	}

	// Create mapping for node2 if it doesn't exist
	if errors.Is(err2, gorm.ErrRecordNotFound) {
		logger.Infof("Creating new mapping for node2.ID=%d to group2.ID=%d", node2.ID, group2.ID)
		mapping2 := &datamodel.NodeNodeGroupMap{
			BaseModel:     datamodel.BaseModel{UUID: uuid.New().String()},
			NodeID:        node2.ID,
			NodeGroupID:   group2.ID,
			HarvestConfig: renderHarvestConfig(*node2),
		}
		if err := tx.Create(mapping2).Error; err != nil {
			logger.Errorf("Failed to create mapping for node2.ID=%d: %v", node2.ID, err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
		}
		mappings = append(mappings, mapping2)
	} else {
		logger.Debugf("Mapping for node2.ID=%d already exists", node2.ID)
		mappings = append(mappings, &existingMapping2)
	}

	return mappings, nil
}

func renderHarvestConfig(node datamodel.Node) *datamodel.HarvestConfig {
	return &datamodel.HarvestConfig{
		PORT:                "443",
		SERVICE_CONTROL_URL: "http://service-control-url",
		SERVICE_NAME:        "test",
		POLLER_NAME:         "cluster" + strconv.FormatInt(node.PoolID, 10) + "-" + node.Name,
		DATACENTER:          "us-west-1",
		NODE_IP:             node.EndpointAddress,
		AUTH_STYLE:          "basic",
		USERNAME:            "admin",
		PASSWORD:            "test-password",
		PROJECT:             "test-project",
	}
}

func _generateRandomNodeGroup(ctx context.Context, d *DataStoreRepository, group1 datamodel.NodeGroup) (*datamodel.NodeGroup, error) {
	group1 = datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{UUID: uuid.New().String()},
		Name:      "lease-" + utils.RandomHex10(),
	}
	group1Ptr, err := d.CreateNodeGroup(ctx, &group1)
	return group1Ptr, err
}
