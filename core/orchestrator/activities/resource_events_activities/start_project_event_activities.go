package resource_events_activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type StartProjectEventActivity struct {
	SE database.Storage
}

func (j *StartProjectEventActivity) StartProjectEventForSDEActivity(ctx context.Context, params *common.StartProjectEventParams) (*common.StartProjectEventResult, error) {
	body := &cvpmodels.ProjectStateUpdateV1beta{StateUpdateV1beta: cvpmodels.StateUpdateV1beta{State: params.State}}
	reqParams := &resource_events.V1betaStartProjectEventParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID,
		Body:           body,
	}

	jwtToken, err := getSignedToken(params.ProjectNumber)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err))
	}

	logger := util.GetLogger(ctx)
	cvpClient := createClient(logger, jwtToken)
	created, accepted, _, err := cvpClient.ResourceEvents.V1betaStartProjectEvent(reqParams)
	if err != nil {
		logger.Errorf("Error turning %s SDE data path: %v", params.State, err)
		switch e := err.(type) {
		case *resource_events.V1betaResourceStateUpdateBadRequest:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorBadRequest,
					fmt.Errorf("Bad request for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateUnauthorized:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorUnauthorized,
					fmt.Errorf("Unauthorized for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateForbidden:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorForbidden,
					fmt.Errorf("Forbidden for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateNotFound:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorNotFound,
					fmt.Errorf("Project not found for state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateConflict:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorConflict,
					fmt.Errorf("Conflict for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateInternalServerError:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorInternalServerError,
					fmt.Errorf("Internal server error for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateNotImplemented:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorNotImplemented,
					fmt.Errorf("Not implemented for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateTooManyRequests:
			return nil, vsaerrors.WrapAsTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorTooManyRequests,
					fmt.Errorf("Too many requests for project state update: %s", e.Error())),
			)

		default:
			logger.Warnf("Unknown error type for project state update: %T - %s", err, err.Error())
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrCVPClientStartProjectEventError, err),
			)
		}
	}

	if created != nil {
		pl := created.GetPayload()
		return &common.StartProjectEventResult{
			Done: pl.Done,
			Name: &pl.Name,
		}, nil
	}

	if accepted != nil {
		pl := accepted.GetPayload()
		return &common.StartProjectEventResult{
			Done: pl.Done,
			Name: &pl.Name,
		}, nil
	}

	return nil, nil
}

func (j *StartProjectEventActivity) PollStartProjectEventSDEOperationActivity(ctx context.Context, params *common.StartProjectEventParams, result *common.StartProjectEventResult) error {
	if result == nil || (result.Done != nil && *result.Done) {
		return nil
	}

	if result.Name == nil {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrInvalidOperationName, errors.New("operation name is nil")))
	}

	logger := util.GetLogger(ctx)
	jwtToken, err := getSignedToken(params.ProjectNumber)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err))
	}
	cvpClient := createClient(logger, jwtToken)

	// Extract the operation UUID
	operationUUID := utils.GetOperationUUID(*result.Name)
	operationParams := async.NewV1betaDescribeOperationParams()
	operationParams.OperationID = operationUUID
	operationParams.ProjectNumber = params.ProjectNumber
	operationParams.LocationID = params.LocationId
	res, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrCVPClientHandleResourceEventError, err),
		)
	}

	if res.Done != nil && *res.Done {
		if res.Error != nil {
			switch int(res.Error.Code) {
			case common.HTTPStatusBadRequest:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorBadRequest,
						fmt.Errorf("Bad request while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusUnauthorized:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorUnauthorized,
						fmt.Errorf("Unauthorized while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusForbidden:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorForbidden,
						fmt.Errorf("Forbidden while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusNotFound:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorNotFound,
						fmt.Errorf("Operation %s not found while polling: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusInternalServerError:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorInternalServerError,
						fmt.Errorf("Internal server error while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusTooManyRequests:
				return vsaerrors.WrapAsTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorTooManyRequests,
						fmt.Errorf("Too many requests while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			default:
				logger.Warnf("Unknown error code while polling operation %s: %d - %s", operationUUID, int(res.Error.Code), res.Error.Message)
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrCVPClientStartProjectEventError,
						fmt.Errorf("SDE polling failed for operation %s: %s", operationUUID, res.Error.Message)),
				)
			}
		}
		return nil
	}
	return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSDEJobNotFinished, errors.New("job not finished")))
}

func (j *StartProjectEventActivity) ListPoolsForAccount(ctx context.Context, projectNumber string, state string, isZone bool) ([]*datamodel.PoolView, error) {
	logger := util.GetLogger(ctx)

	// Skip pool listing if this is a zone-specific operation
	if isZone {
		logger.Infof("Zone-specific operation, skipping pool listing for VSA operations")
		return []*datamodel.PoolView{}, nil
	}

	account, err := j.SE.GetAccount(ctx, projectNumber)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseListPoolsForAccount, err))
	}

	var filter *dbutils.Filter
	switch state {
	case string(gcpserver.ResourceStateUpdateV1betaStateOFF):
		filter = dbutils.CreateFilterWithConditions(dbutils.NewFilterCondition("account_id", "=", account.ID))
	case string(gcpserver.ResourceStateUpdateV1betaStateON):
		jobTransitioningStates := []string{string(gcpserver.PoolV1betaStoragePoolStateDISABLED)}
		filter = dbutils.CreateFilterWithConditions(dbutils.NewFilterCondition("account_id", "=", account.ID),
			dbutils.NewFilterCondition("state", "in", jobTransitioningStates))
	default:
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrInvalidOperationName, fmt.Errorf("invalid resource state: %s", state)))
	}

	pools, err := j.SE.ListPools(ctx, filter)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseListPoolsForAccount, err))
	}
	return pools, nil
}

func (j *StartProjectEventActivity) UpdateAccountStateForHandleResource(ctx context.Context, projectNumber string, newState string) error {
	account, err := j.SE.GetAccount(ctx, projectNumber)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseListPoolsForAccount, err))
	}

	err = j.SE.UpdateAccountStateForHandleResource(ctx, account.UUID, newState)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseUpdateAccountState, err))
	}
	return nil
}

// PoolFilterResult represents the result of filtering pools
type PoolFilterResult struct {
	FilteredPools []*datamodel.PoolView
	VSAError      bool
}

// FilterPoolsForClusterOperations filters pools based on transient states and associated resources
func (j *StartProjectEventActivity) FilterPoolsForClusterOperations(ctx context.Context, allPools []*datamodel.PoolView, isZone bool) (*PoolFilterResult, error) {
	logger := util.GetLogger(ctx)

	// Skip pool filtering if this is a zone-specific operation
	if isZone {
		logger.Infof("Zone-specific operation, skipping pool filtering for VSA operations")
		return &PoolFilterResult{
			FilteredPools: []*datamodel.PoolView{},
			VSAError:      false,
		}, nil
	}

	se := j.SE

	var filteredPools []*datamodel.PoolView
	var vsaError bool

	for _, pool := range allPools {
		if pool.State == models.LifeCycleStateDisabled {
			logger.Infof("Skipping pool %s (%s) - in DISABLED state", pool.Name, pool.UUID)
			continue
		}

		// Skip pools in transient states (Creating, Updating, Deleting)
		if isPoolInTransientState(pool.State) {
			logger.Warnf("Skipping pool %s (%s) - in transient state: %s", pool.Name, pool.UUID, pool.State)
			vsaError = true
			continue
		}

		// Only process READY or ERROR pools for cluster health check and operations
		if pool.State != models.LifeCycleStateREADY && pool.State != models.LifeCycleStateError {
			logger.Infof("Skipping pool %s (%s) - not in READY or ERROR state: %s", pool.Name, pool.UUID, pool.State)
			vsaError = true
			continue
		}

		// Check for volumes and snapshots in transient states
		volumes, err := se.GetVolumesByPoolID(ctx, pool.Pool.ID)
		if err != nil {
			logger.Errorf("Failed to get volumes for pool %s: %v", pool.Name, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}

		hasTransientResource := false
		for _, volume := range volumes {
			// Check volume transient state
			if isVolumeInTransientState(volume.State) {
				logger.Warnf("Skipping pool %s (%s) - volume %s in transient state: %s", pool.Name, pool.UUID, volume.Name, volume.State)
				hasTransientResource = true
				vsaError = true
				break
			}

			// Check snapshots transient states for this volume
			snapshots, err := se.GetSnapshotsByVolumeID(ctx, volume.ID)
			if err != nil {
				logger.Errorf("Failed to get snapshots for volume %s: %v", volume.Name, err)
				return nil, vsaerrors.WrapAsTemporalApplicationError(err)
			}

			for _, snapshot := range snapshots {
				if isSnapshotInTransientState(snapshot.State) {
					logger.Warnf("Skipping pool %s (%s) - snapshot %s in transient state: %s", pool.Name, pool.UUID, snapshot.Name, snapshot.State)
					hasTransientResource = true
					vsaError = true
					break
				}
			}

			if hasTransientResource {
				break
			}
		}

		if hasTransientResource {
			continue
		}

		// Pool and all its resources are in valid states, include it
		filteredPools = append(filteredPools, pool)
		logger.Infof("Including pool %s (%s) for cluster operations - state: %s", pool.Name, pool.UUID, pool.State)
	}

	logger.Infof("Pool filtering complete: %d total pools, %d filtered for operations", len(allPools), len(filteredPools))
	return &PoolFilterResult{
		FilteredPools: filteredPools,
		VSAError:      vsaError,
	}, nil
}

// isPoolInTransientState checks if pool is in a transient state
func isPoolInTransientState(state string) bool {
	transientStates := []string{
		models.LifeCycleStateCreating,
		models.LifeCycleStateUpdating,
		models.LifeCycleStateDeleting,
	}
	for _, transientState := range transientStates {
		if state == transientState {
			return true
		}
	}
	return false
}

// isVolumeInTransientState checks if volume is in a transient state
func isVolumeInTransientState(state string) bool {
	transientStates := []string{
		models.LifeCycleStateCreating,
		models.LifeCycleStateUpdating,
		models.LifeCycleStateDeleting,
		models.LifeCycleStateRestoring,
	}
	for _, transientState := range transientStates {
		if state == transientState {
			return true
		}
	}
	return false
}

// isSnapshotInTransientState checks if snapshot is in a transient state
func isSnapshotInTransientState(state string) bool {
	transientStates := []string{
		models.LifeCycleStateCreating,
		models.LifeCycleStateDeleting,
	}
	for _, transientState := range transientStates {
		if state == transientState {
			return true
		}
	}
	return false
}
