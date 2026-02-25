package orchestrator

import (
	"context"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// CreateVolumePerformanceGroup creates a new volume performance group
func (o *Orchestrator) CreateVolumePerformanceGroup(ctx context.Context, params *commonparams.CreateVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, error) {
	return nil, errors.New("volume performance group creation is not implemented")
}

// ListVolumePerformanceGroups lists all volume performance groups for a pool
func (o *Orchestrator) ListVolumePerformanceGroups(ctx context.Context, params *commonparams.ListVolumePerformanceGroupsParams) ([]*models.VolumePerformanceGroup, error) {
	logger := util.GetLogger(ctx)
	se := o.storage

	// Validate account exists
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to fetch account for the given projectNumber", "error", err)
		return nil, err
	}

	// Validate pool exists and belongs to account
	pool, err := se.DescribePool(ctx, params.PoolID, account.ID)
	if err != nil {
		logger.Error("Failed to fetch pool for the given pool ID and account", "error", err)
		return nil, err
	}

	// List VPGs from database for the pool
	vpgs, err := se.ListVolumePerformanceGroupsByPoolID(ctx, pool.ID)
	if err != nil {
		logger.Error("Failed to list volume performance groups", "error", err)
		return nil, err
	}

	// Filter out automatically generated VPGs
	filteredVpgs := make([]*datamodel.VolumePerformanceGroup, 0)
	for _, vpg := range vpgs {
		if !vpg.IsAutoGen {
			filteredVpgs = append(filteredVpgs, vpg)
		}
	}

	// Convert datamodel to models
	result := make([]*models.VolumePerformanceGroup, 0, len(filteredVpgs))
	for _, vpg := range filteredVpgs {
		result = append(result, convertDatastoreVPGToModel(vpg))
	}

	return result, nil
}

// GetVolumePerformanceGroup describes a specific volume performance group
func (o *Orchestrator) GetVolumePerformanceGroup(ctx context.Context, params *commonparams.GetVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, error) {
	logger := util.GetLogger(ctx)
	se := o.storage

	// Validate account exists
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to fetch account for the given projectNumber", "error", err)
		return nil, err
	}

	// Validate pool exists and belongs to account
	pool, err := se.DescribePool(ctx, params.PoolID, account.ID)
	if err != nil {
		logger.Error("Failed to fetch pool for the given pool ID and account", "error", err)
		return nil, err
	}

	// Get VPG from database
	vpg, err := se.GetVolumePerformanceGroupByUUID(ctx, params.VolumePerformanceGroupID)
	if err != nil {
		logger.Error("Failed to fetch volume performance group", "error", err)
		return nil, err
	}

	// Validate VPG belongs to the specified pool
	if vpg.PoolID != pool.ID {
		logger.Error("Volume performance group does not belong to the specified pool", "vpgPoolID", vpg.PoolID, "requestedPoolID", pool.ID)
		return nil, customerrors.NewUserInputValidationErr("volume performance group does not belong to the specified pool")
	}

	// Convert datamodel to models
	return convertDatastoreVPGToModel(vpg), nil
}

// convertDatastoreVPGToModel converts datamodel.VolumePerformanceGroup to models.VolumePerformanceGroup
func convertDatastoreVPGToModel(vpg *datamodel.VolumePerformanceGroup) *models.VolumePerformanceGroup {
	if vpg == nil {
		return nil
	}

	return &models.VolumePerformanceGroup{
		BaseModel: models.BaseModel{
			ID:        vpg.ID,
			UUID:      vpg.UUID,
			CreatedAt: vpg.CreatedAt,
			UpdatedAt: vpg.UpdatedAt,
		},
		Name:            vpg.Name,
		ThroughputMibps: vpg.ThroughputMibps,
		Iops:            vpg.Iops,
		IsShared:        vpg.IsShared,
	}
}

// UpdateVolumePerformanceGroup updates a volume performance group
func (o *Orchestrator) UpdateVolumePerformanceGroup(ctx context.Context, params *commonparams.UpdateVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, error) {
	return nil, errors.New("updating volume performance group is not implemented")
}

// DeleteVolumePerformanceGroup deletes a volume performance group
func (o *Orchestrator) DeleteVolumePerformanceGroup(ctx context.Context, params *commonparams.DeleteVolumePerformanceGroupParams) error {
	return errors.New("deleting volume performance group is not implemented")
}
