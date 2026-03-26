package gcp

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// CreateVolumePerformanceGroup creates a new volume performance group
func (o *GCPOrchestrator) CreateVolumePerformanceGroup(ctx context.Context, params *commonparams.CreateVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, error) {
	logger := util.GetLogger(ctx)
	se := o.storage

	// Validate account exists
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to fetch account for the given projectNumber", "error", err)
		return nil, err
	}

	// Validate pool exists and belongs to account
	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
	if err != nil {
		logger.Error("Failed to fetch pool for the given pool ID and account", "error", err)
		return nil, err
	}
	if pool.State != models.LifeCycleStateREADY {
		return nil, customerrors.NewUserInputValidationErr("pool is not in a ready state")
	}
	if pool.APIAccessMode == commonparams.ONTAPMode {
		return nil, customerrors.NewUserInputValidationErr("Cannot create Volume Performance Groups in ONTAP mode pool using GCNV API")
	}
	if pool.QosType != utils.QosTypeManual {
		return nil, customerrors.NewUserInputValidationErr("VPGs can only be created in pools with manual QoS type")
	}

	vpg, err := se.CreateVolumePerformanceGroup(ctx, &datamodel.VolumePerformanceGroup{
		Name:            params.Name,
		PoolID:          pool.ID,
		ThroughputMibps: params.ThroughputMibps,
		Iops:            params.Iops,
		IsShared:        params.IsShared,
		IsAutoGen:       false,
	})
	if err != nil {
		logger.Error("Failed to create volume performance group", "error", err)
		return nil, err
	}

	defer func() {
		if err != nil {
			deleteErr := se.DeleteVolumePerformanceGroup(ctx, vpg)
			if deleteErr != nil {
				logger.Error("Failed to delete volume performance group", "volume_performance_group_name", vpg.Name, "error", deleteErr)
			}
		}
	}()

	workflowID := fmt.Sprintf("vpg-create-%s", vpg.UUID)
	_, err = o.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             workflowengine.CustomerTaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
	}, workflows.CreateVolumePerformanceGroupWorkflow, vpg.UUID)
	if err != nil {
		logger.Error("Failed to start create volume performance group workflow", "error", err, "vpg_uuid", vpg.UUID)
		return nil, err
	}

	return convertDatastoreVPGToModel(vpg), nil
}

// ListVolumePerformanceGroups lists all volume performance groups for a pool
func (o *GCPOrchestrator) ListVolumePerformanceGroups(ctx context.Context, params *commonparams.ListVolumePerformanceGroupsParams) ([]*models.VolumePerformanceGroup, error) {
	logger := util.GetLogger(ctx)
	se := o.storage

	// Validate account exists
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to fetch account for the given projectNumber", "error", err)
		return nil, err
	}

	// Validate pool exists and belongs to account
	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
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
func (o *GCPOrchestrator) GetVolumePerformanceGroup(ctx context.Context, params *commonparams.GetVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, error) {
	logger := util.GetLogger(ctx)
	se := o.storage

	// Validate account exists
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to fetch account for the given projectNumber", "error", err)
		return nil, err
	}

	// Validate pool exists and belongs to account
	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
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
		PoolID:          strconv.FormatInt(vpg.PoolID, 10),
		ThroughputMibps: vpg.ThroughputMibps,
		Iops:            vpg.Iops,
		IsShared:        vpg.IsShared,
	}
}

// validatePoolCapacityForVPGUpdate checks that updating the VPG's throughput/IOPS would not exceed the pool's total capacity.
// It accounts for the VPG's isShared state and the number of volumes assigned to this VPG: when isShared is true,
// the VPG contributes vpg.throughput/vpg.iops once; when isShared is false, each volume gets its own allocation so
// pool total is vpg.throughput * numVolumes (pre) and newThroughput * numVolumes (post).
func validatePoolCapacityForVPGUpdate(ctx context.Context, se database.Storage, pool *datamodel.PoolView, vpg *datamodel.VolumePerformanceGroup, newThroughputMibps *int64, newIops *int64) error {
	if pool == nil || pool.PoolAttributes == nil || pool.PoolAttributes.ThroughputMibps == 0 {
		return nil
	}
	numVolumes, err := se.GetVolumeCountByVolumePerformanceGroupID(ctx, vpg.ID)
	if err != nil {
		return err
	}
	// When no volumes are assigned, the VPG consumes no pool capacity regardless of isShared.
	volumesPresent := int64(0)
	if numVolumes > 0 {
		volumesPresent = 1
	}

	totalPoolThroughput := pool.PoolAttributes.ThroughputMibps
	totalPoolIops := pool.PoolAttributes.Iops

	// Pre-update: this VPG's current contribution to pool configured throughput/IOPS.
	// When isShared is true, the VPG contributes once (or zero if no volumes). When false, each volume gets its own, so pool consumption = vpg * numVolumes.
	newTput := vpg.ThroughputMibps
	if newThroughputMibps != nil {
		newTput = *newThroughputMibps
	}
	newIopsVal := vpg.Iops
	if newIops != nil {
		newIopsVal = *newIops
	}

	var preThroughput, preIops, postThroughput, postIops int64
	if vpg.IsShared {
		preThroughput = vpg.ThroughputMibps * volumesPresent
		preIops = vpg.Iops * volumesPresent
		postThroughput = newTput * volumesPresent
		postIops = newIopsVal * volumesPresent
	} else {
		preThroughput = vpg.ThroughputMibps * numVolumes
		preIops = vpg.Iops * numVolumes
		postThroughput = newTput * numVolumes
		postIops = newIopsVal * numVolumes
	}

	// Total configured for pool after update = (current pool configured) - (this VPG's pre contribution) + (this VPG's post contribution)
	totalConfiguredThroughput := int64(pool.Throughput) - preThroughput + postThroughput
	totalConfiguredIops := pool.Iops - preIops + postIops

	if totalConfiguredThroughput > totalPoolThroughput {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf(
			"Sum of configured throughput (%d MiBps) would exceed pool's total throughput (%d MiBps)",
			totalConfiguredThroughput, totalPoolThroughput))
	}
	if totalConfiguredIops > totalPoolIops {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf(
			"Sum of configured IOPS (%d) would exceed pool's total IOPS (%d)",
			totalConfiguredIops, totalPoolIops))
	}
	return nil
}

// UpdateVolumePerformanceGroup starts an async update of a volume performance group (name, throughput, IOPS) and returns the VPG model and job UUID for 202 response.
// Validation order mirrors Create flow: account → pool → pool manual QoS → fetch resource → resource belongs to pool → resource allowed (e.g. not autogenerated) → capacity.
func (o *GCPOrchestrator) UpdateVolumePerformanceGroup(ctx context.Context, params *commonparams.UpdateVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage

	// Validate account exists
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to fetch account for VPG update", "error", err)
		return nil, "", err
	}

	// Validate pool exists and belongs to account
	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
	if err != nil {
		logger.Error("Failed to fetch pool for VPG update", "error", err)
		return nil, "", err
	}

	// Pool must have manual QoS to update a VPG
	if pool.QosType != "manual" {
		return nil, "", customerrors.NewUserInputValidationErr("pool must have manual QoS to update volume performance group")
	}

	// Fetch VPG and ensure it belongs to the specified pool
	vpg, err := se.GetVolumePerformanceGroupByUUID(ctx, params.VolumePerformanceGroupID)
	if err != nil {
		logger.Error("Failed to fetch VPG for update", "error", err)
		return nil, "", err
	}
	if vpg.PoolID != pool.ID {
		return nil, "", customerrors.NewUserInputValidationErr("volume performance group does not belong to the specified pool")
	}
	// Only manually created VPGs can be updated; autogenerated VPGs are managed by the system
	if vpg.IsAutoGen {
		return nil, "", customerrors.NewUserInputValidationErr("only manually created volume performance groups can be updated; autogenerated VPGs cannot be updated")
	}

	// Validate pool capacity would not be exceeded after update
	if err := validatePoolCapacityForVPGUpdate(ctx, se, pool, vpg, params.ThroughputMibps, params.Iops); err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeUpdateVolumePerformanceGroup),
		State:        string(models.JobsStateNEW),
		ResourceName: vpg.Name,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: vpg.UUID,
		},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create VPG update job", "error", err)
		return nil, "", err
	}
	defer func() {
		if err != nil && createdJob != nil {
			_ = se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error())
		}
	}()

	workflowExecutor := workflows.NewWorkflowExecutor(o.temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.UpdateVolumePerformanceGroupWorkflow,
		nil,
		params,
		vpg,
	)
	if err != nil {
		logger.Error("Failed to start VPG update workflow", "error", err)
		return nil, "", err
	}

	return convertDatastoreVPGToModel(vpg), createdJob.UUID, nil
}

// DeleteVolumePerformanceGroup deletes a volume performance group from ONTAP and the VCP database.
// Deletion is only allowed when the VPG is not attached to any volumes.
// Returns the deleted VPG model, the job UUID (for operation polling), or an error.
func (o *GCPOrchestrator) DeleteVolumePerformanceGroup(ctx context.Context, params *commonparams.DeleteVolumePerformanceGroupParams) (*models.VolumePerformanceGroup, string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to fetch account for VPG delete", "error", err)
		return nil, "", err
	}

	poolView, err := se.GetPool(ctx, params.PoolID, account.ID)
	if err != nil {
		logger.Error("Failed to fetch pool for VPG delete", "error", err)
		return nil, "", err
	}

	vpg, err := se.GetVolumePerformanceGroupByUUID(ctx, params.VolumePerformanceGroupID)
	if err != nil {
		logger.Error("Failed to fetch volume performance group", "error", err)
		return nil, "", err
	}

	if vpg.PoolID != poolView.Pool.ID {
		logger.Error("Volume performance group does not belong to the specified pool", "vpgPoolID", vpg.PoolID, "requestedPoolID", poolView.Pool.ID)
		return nil, "", customerrors.NewUserInputValidationErr("volume performance group does not belong to the specified pool")
	}

	count, err := se.GetVolumeCountByVolumePerformanceGroupID(ctx, vpg.ID)
	if err != nil {
		logger.Error("Failed to get volume count for VPG", "vpg_id", vpg.UUID, "error", err)
		return nil, "", err
	}
	if count > 0 {
		logger.Error("Cannot delete volume performance group: it is attached to one or more volumes", "vpg_id", vpg.UUID, "volume_count", count)
		return nil, "", customerrors.NewConflictErr("volume performance group cannot be deleted because it is attached to one or more volumes")
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeDeleteVolumePerformanceGroup),
		State:        string(models.JobsStateNEW),
		ResourceName: vpg.Name,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: vpg.UUID,
		},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create VPG delete job", "error", err)
		return nil, "", err
	}
	defer func() {
		if err != nil && createdJob != nil {
			_ = se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error())
		}
	}()

	// Build response model before the workflow hard-deletes the row.
	deletedModel := convertDatastoreVPGToModel(vpg)

	workflowExecutor := workflows.NewWorkflowExecutor(o.temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.DeleteVolumePerformanceGroupWorkflow,
		nil,
		&workflows.DeleteVolumePerformanceGroupWorkflowParams{VPG: vpg, AccountName: params.AccountName},
	)
	if err != nil {
		logger.Error("Failed to start VPG delete workflow", "error", err)
		return nil, "", err
	}

	return deletedModel, createdJob.UUID, nil
}
