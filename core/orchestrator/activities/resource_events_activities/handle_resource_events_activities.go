package resource_events_activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

const (
	ErrTypeResourceNotFound = "NotFoundErr"
)

var (
	PollCvpOperationForWorkflow = pollCvpOperationForWorkflow
)

type ResourceEventsActivity struct {
	SE        database.Storage
	Scheduler scheduler.Scheduler
}

func (a *ResourceEventsActivity) HandleResourceEventCheckForVCPActivity(ctx context.Context, params *common.HandleResourceEventParams) (bool, error) {
	switch params.ResourceType {
	case common.ResourceStateV1ResourceTypeKmsConfig:
		return a.checkKmsConfigExistence(ctx, params)
	case common.ResourceStateV1ResourceTypeStoragePool:
		return a.checkStoragePoolExistence(ctx, params)
	case common.ResourceStateV1ResourceTypeSnapshot:
		return a.checkSnapshotExistence(ctx, params)
	case common.ResourceStateV1ResourceTypeVolume:
		return a.checkVolumeExistence(ctx, params)
	case common.ResourceStateV1ResourceTypeBackupPolicy:
		return a.checkBackupPolicyExistence(ctx, params)
	default:
		return false, errors.New("unsupported resource type")
	}
}

func (a *ResourceEventsActivity) checkKmsConfigExistence(ctx context.Context, params *common.HandleResourceEventParams) (bool, error) {
	_, err := a.SE.GetKmsConfig(ctx, params.ResourceId)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) checkStoragePoolExistence(ctx context.Context, params *common.HandleResourceEventParams) (bool, error) {
	account, err := a.SE.GetAccount(ctx, params.ProjectNumber)
	if err != nil {
		return false, err
	}
	_, err = a.SE.GetPool(ctx, params.ResourceId, account.ID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) checkSnapshotExistence(ctx context.Context, params *common.HandleResourceEventParams) (bool, error) {
	account, err := a.SE.GetAccount(ctx, params.ProjectNumber)
	if err != nil {
		return false, err
	}
	volume, err := a.SE.GetVolumeWithAccountID(ctx, params.ParentResourceID, account.ID)
	if err != nil {
		return false, err
	}

	_, err = a.SE.GetSnapshotByUUID(ctx, params.ResourceId, account.ID, volume.ID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) checkVolumeExistence(ctx context.Context, params *common.HandleResourceEventParams) (bool, error) {
	_, err := a.SE.GetVolume(ctx, params.ResourceId)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) checkBackupPolicyExistence(ctx context.Context, params *common.HandleResourceEventParams) (bool, error) {
	account, err := a.SE.GetAccount(ctx, params.ProjectNumber)
	if err != nil {
		return false, err
	}

	_, err = a.SE.GetBackupPolicyByUUIDAndOwnerID(ctx, params.ResourceId, account.ID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) HandleResourceEventsOFFForVCPActivity(ctx context.Context, params *common.HandleResourceEventParams) (bool, error) {
	switch params.ResourceType {
	case common.ResourceStateV1ResourceTypeKmsConfig:
		return a.handleKmsConfig(ctx, params, coremodels.LifeCycleStateDisabledDetails)
	case common.ResourceStateV1ResourceTypeStoragePool:
		return a.handleStoragePool(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
	case common.ResourceStateV1ResourceTypeSnapshot:
		return a.handleSnapshot(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
	case common.ResourceStateV1ResourceTypeVolume:
		return a.handleVolume(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
	case common.ResourceStateV1ResourceTypeAD:
		return false, nil
	case common.ResourceStateV1ResourceTypeBackupPolicy:
		return a.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
	default:
		return false, errors.New("unsupported resource type")
	}
}

func (a *ResourceEventsActivity) HandleResourceEventsONForVCPActivity(ctx context.Context, params *common.HandleResourceEventParams) (bool, error) {
	switch params.ResourceType {
	case common.ResourceStateV1ResourceTypeKmsConfig:
		return a.handleKmsConfig(ctx, params, common.ResourceLifeCycleStateEnabledDetails)
	case common.ResourceStateV1ResourceTypeStoragePool:
		return a.handleStoragePool(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
	case common.ResourceStateV1ResourceTypeSnapshot:
		return a.handleSnapshot(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
	case common.ResourceStateV1ResourceTypeVolume:
		return a.handleVolume(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
	case common.ResourceStateV1ResourceTypeAD:
		return false, nil
	case common.ResourceStateV1ResourceTypeBackupPolicy:
		return a.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
	default:
		return false, errors.New("unsupported resource type")
	}
}

func (a *ResourceEventsActivity) HandleResourceEventsForSDEActivity(ctx context.Context, params *common.HandleResourceEventParams) (*common.HandleResourceEventResult, error) {
	body := &models.ResourceStateUpdateV1beta{
		StateUpdateV1beta: models.StateUpdateV1beta{
			State: params.State,
		},
		ResourceType: params.ResourceType,
		ResourceID:   params.ResourceId,
	}

	reqParams := &resource_events.V1betaResourceStateUpdateParams{
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
	created, accepted, _, err := cvpClient.ResourceEvents.V1betaResourceStateUpdate(reqParams)
	if err != nil {
		logger.Errorf("Error turning %s Resource: %v", params.State, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCVPClientHandleResourceEventError, err))
	}

	if created != nil {
		pl := created.GetPayload()
		return &common.HandleResourceEventResult{
			Done: pl.Done,
			Name: &pl.Name,
		}, nil
	}

	if accepted != nil {
		pl := accepted.GetPayload()
		return &common.HandleResourceEventResult{
			Done: pl.Done,
			Name: &pl.Name,
		}, nil
	}

	return nil, nil
}

func (j *ResourceEventsActivity) PollHandleResourceEventSDEOperationActivity(ctx context.Context, params *common.HandleResourceEventParams, result *common.HandleResourceEventResult) error {
	if result.Done != nil && *result.Done {
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
	res, err := PollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		logger.Errorf("Error while polling SDE handleResourceEvent operation: %s", operationUUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if res.Done != nil && *res.Done {
		if res.Error != nil {
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCVPClientHandleResourceEventError, errors.New(res.Error.Message)))
		}
		return nil
	}
	return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSDEJobNotFinished, errors.New("job not finished")))
}

func (a *ResourceEventsActivity) handleKmsConfig(ctx context.Context, params *common.HandleResourceEventParams, stateDetails string) (bool, error) {
	_, err := a.SE.UpdateKmsConfigStateForHandleResource(ctx, params.ResourceId, stateDetails, params.State)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) handleStoragePool(ctx context.Context, params *common.HandleResourceEventParams, state string, stateDetails string) (bool, error) {
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: params.ResourceId,
		},
		State:        state,
		StateDetails: stateDetails,
	}
	_, err := a.SE.UpdatePoolState(ctx, pool, state, stateDetails)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) handleSnapshot(ctx context.Context, params *common.HandleResourceEventParams, state string, stateDetails string) (bool, error) {
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			UUID: params.ResourceId,
		},
		State:        state,
		StateDetails: stateDetails,
	}
	_, err := a.SE.UpdateSnapshotForHandleResource(ctx, snapshot)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) handleVolume(ctx context.Context, params *common.HandleResourceEventParams, state string, stateDetails string) (bool, error) {
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: params.ResourceId,
		},
		State:        state,
		StateDetails: stateDetails,
	}
	err := a.SE.UpdateVolume(ctx, volume)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}
	return true, nil
}

func (a *ResourceEventsActivity) handleBackupPolicy(ctx context.Context, params *common.HandleResourceEventParams, state string, stateDetails string) (bool, error) {
	logger := util.GetLogger(ctx)

	account, err := a.SE.GetAccount(ctx, params.ProjectNumber)
	if err != nil {
		logger.Errorf("Failed to get account for project number %s: %v", params.ProjectNumber, err)
		return false, err
	}

	backupPolicy, err := a.SE.GetBackupPolicyByUUIDAndOwnerID(ctx, params.ResourceId, account.ID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		}
		return false, err
	}

	backupPolicyActivity := &activities.BackupPolicyActivity{
		SE:        a.SE,
		Scheduler: a.Scheduler,
	}

	switch state {
	case coremodels.LifeCycleStateDisabled:
		// For OFF events: check if the policy is enabled in DB, then pause
		if backupPolicy.PolicyEnabled {
			logger.Infof("Processing pause request for backup policy schedule: %s", params.ResourceId)
			err = backupPolicyActivity.PauseBackupPolicySchedule(ctx, backupPolicy)
			if err != nil {
				logger.Errorf("Failed to pause backup policy schedule: %v", err)
				return false, err
			}
			logger.Infof("Successfully processed pause request for backup policy schedule: %s", params.ResourceId)
		} else {
			logger.Infof("Backup policy is already disabled in database, skipping pause: %s", params.ResourceId)
		}

	case coremodels.LifeCycleStateREADY:
		if backupPolicy.PolicyEnabled {
			logger.Infof("Processing unpause request for backup policy schedule: %s", params.ResourceId)
			err = backupPolicyActivity.UnpauseBackupPolicySchedule(ctx, backupPolicy)
			if err != nil {
				logger.Errorf("Failed to unpause backup policy schedule: %v", err)
				return false, err
			}
			logger.Infof("Successfully processed unpause request for backup policy schedule: %s", params.ResourceId)
		} else {
			logger.Infof("Backup policy is disabled in database, skipping unpause: %s", params.ResourceId)
		}

	default:
		logger.Warnf("Unknown state for backup policy resource event: %s", state)
		return false, errors.New("unknown state for backup policy resource event")
	}

	return true, nil
}

func (a *ResourceEventsActivity) DeleteVolumeForPool(ctx context.Context, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	_, err := se.DeleteVolumeAndChildResources(ctx, volume.UUID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debugf("Volume:%s marked deleted successfully in the db", volume.Name)

	return nil
}

func (a *ResourceEventsActivity) DeleteReplicationsForVolume(ctx context.Context, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("account_id", "=", volume.AccountID),
		dbutils.NewFilterCondition("volume_id", "=", volume.ID))
	replications, err := se.ListVolumeReplications(ctx, *filter)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, replication := range replications {
		_, err = se.DeleteVolumeReplication(ctx, replication)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		logger.Debugf("Replication:%s marked deleted successfully in the db", replication.Name)
	}

	return nil
}
