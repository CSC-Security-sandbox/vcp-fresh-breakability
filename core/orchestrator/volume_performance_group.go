package orchestrator

import (
	"context"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
)

// CreateVolumePerformanceGroup creates a new volume performance group
func (o *Orchestrator) CreateVolumePerformanceGroup(ctx context.Context, params *commonparams.CreateVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, error) {
	return nil, errors.New("volume performance group creation is not implemented")
}

// ListVolumePerformanceGroups lists all volume performance groups for a pool
func (o *Orchestrator) ListVolumePerformanceGroups(ctx context.Context, params *commonparams.ListVolumePerformanceGroupsParams) ([]*models.VolumePerformanceGroup, error) {
	return nil, errors.New("listing volume performance groups is not implemented")
}

// GetVolumePerformanceGroup describes a specific volume performance group
func (o *Orchestrator) GetVolumePerformanceGroup(ctx context.Context, params *commonparams.GetVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, error) {
	return nil, errors.New("get volume performance group is not implemented")
}

// UpdateVolumePerformanceGroup updates a volume performance group
func (o *Orchestrator) UpdateVolumePerformanceGroup(ctx context.Context, params *commonparams.UpdateVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, error) {
	return nil, errors.New("updating volume performance group is not implemented")
}

// DeleteVolumePerformanceGroup deletes a volume performance group
func (o *Orchestrator) DeleteVolumePerformanceGroup(ctx context.Context, params *commonparams.DeleteVolumePerformanceGroupParams) error {
	return errors.New("deleting volume performance group is not implemented")
}
