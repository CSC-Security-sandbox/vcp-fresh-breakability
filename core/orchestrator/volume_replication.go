package orchestrator

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/google/uuid"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/replicationWorkflows"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	logger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

var (
	// Internal functions for orchestrator package
	createVolumeReplicationInternal = _createVolumeReplicationInternal
	updateVolumeReplicationInternal = _updateVolumeReplicationInternal
	stopReplicationInternal         = _stopReplicationInternal
	resumeReplicationInternal       = _resumeReplicationInternal
	deleteReplicationInternal       = _deleteReplicationInternal
	reverseReplicationInternal      = _reverseReplicationInternal

	createVolumeReplication     = _createVolumeReplication
	stopReplication             = _stopReplication
	resumeReplication           = _resumeReplication
	deleteReplication           = _deleteReplication
	releaseVolumeReplication    = _releaseVolumeReplication
	syncReplication             = _syncReplication
	reverseAndResumeReplication = _reverseAndResumeReplication
	updateReplication           = _updateReplication
	getActiveReplicationJobs    = _getActiveReplicationJobs

	validateCreateReplicationParams = replication.ValidateCreateReplicationParams
	validateReplicationParams       = replication.ValidateReplicationParams
	verifyDstReplicationResume      = replication.VerifyDstReplicationResume
	verifyDstReplicationStop        = replication.VerifyDstReplicationStop
	VerifyDstReplicationDelete      = replication.VerifyDstReplication
	verifyDstReplicationSync        = replication.VerifyDstReplicationSync
	validateReplicationUpdate       = replication.ValidateReplicationUpdate
	verifyDstReplicationReverse     = replication.VerifyDstReplicationReverse

	convertCreateReplicationParamsToEventParam = _convertCreateReplicationParamsToEventParam
	getReplicationObjects                      = _getReplicationObjects
	googleProxyInternalGetMultipleReplications = _googleProxyInternalGetMultipleReplications
	GetProjectNumberForRegion                  = _getProjectNumberForRegion

	utilParseRegionAndZone            = utils.ParseRegionAndZone
	utilParseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
	utilsGetPairedRegionUri           = utils.GetPairedRegionURI
	utilsParseProjectNumberFromURI    = utils.ParseProjectNumberFromURI
	authGetSignedJwtToken             = auth.GetSignedJwtToken
)

const (
	volumeReplicationCVPV1betaLifeCycleStateAvailableForUse = "Available for use"
	volumeReplicationCVP1betaLifeCycleStateCreation         = "Create in progress"
	volumeReplicationCVP1betaLifeCycleStateStopping         = "Stop in progress"
	volumeReplicationCVP1betaLifeCycleStateResuming         = "Resume in progress"
	volumeReplicationCVP1betaLifeCycleStateSync             = "Sync in progress"
	volumeReplicationCVP1betaLifeCycleStateReversing        = "Reverse in progress"
	volumeReplicationCVP1betaLifeCycleStateUpdating         = "Update in progress"
	volumeReplicationCVP1betaLifeCycleStateDeleting         = "Delete in progress"
)

func (o *Orchestrator) CreateVolumeReplicationInternal(ctx context.Context, params *commonparams.CreateVolumeReplicationInternalParams) (*models.VolumeReplication, *datamodel.Job, error) {
	return createVolumeReplicationInternal(ctx, o.storage, o.temporal, params)
}

func _createVolumeReplicationInternal(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.CreateVolumeReplicationInternalParams) (*models.VolumeReplication, *datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, params.VolumeReplication.Account.Name)
	if err != nil {
		logger.Error("Failed to get or create account", "error", err)
		return nil, nil, err
	}

	volume, err := se.GetVolume(ctx, params.VolumeReplication.ReplicationAttributes.DestinationVolumeUUID)
	if err != nil {
		logger.Error("Failed to get volume", "error", err)
		return nil, nil, err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateVolumeReplicationInternal),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.VolumeReplication.Uri,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, nil, err
	}

	replicationDb, err := se.CreateVolumeReplication(ctx, prepareReplicationDataModel(params, account, volume))
	if err != nil {
		logger.Error("Failed to create volume replication in database", "error", err)
		return nil, nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.CreateInternalVolumeReplicationWorkflow,
		params,
		replicationDb,
	)

	if err != nil {
		logger.Error("Failed to execute workflow for volume replication creation", "error", err)
		return nil, nil, err
	}

	return convertDataStoreReplicationToModel(replicationDb), createdJob, nil
}

func (o *Orchestrator) UpdateVolumeReplicationInternal(ctx context.Context, params *commonparams.UpdateVolumeReplicationInternalParams) (*models.VolumeReplication, *datamodel.Job, error) {
	return updateVolumeReplicationInternal(ctx, o.storage, o.temporal, params)
}

func _updateVolumeReplicationInternal(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.UpdateVolumeReplicationInternalParams) (*models.VolumeReplication, *datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to get account", "error", err)
		return nil, nil, err
	}

	replicationDb, err := se.GetVolumeReplication(ctx, params.VolumeReplicationUuid)
	if err != nil {
		logger.Error("Failed to get volume replication from database", "error", err)
		return nil, nil, err
	}

	replicationDb.State = models.LifeCycleStateUpdating
	replicationDb.StateDetails = models.LifeCycleStateUpdatingDetails
	err = se.UpdateVolumeReplicationStates(ctx, replicationDb)
	if err != nil {
		return nil, nil, err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateVolumeReplicationInternal),
		State:         string(models.JobsStateNEW),
		ResourceName:  replicationDb.Uri,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: replicationDb.UUID,
			PoolUUID:     replicationDb.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.UpdateInternalVolumeReplicationWorkflow,
		params,
		replicationDb,
	)

	if err != nil {
		logger.Error("Failed to execute workflow for volume replication update", "error", err)
		return nil, nil, err
	}

	return convertDataStoreReplicationToModel(replicationDb), createdJob, nil
}

func (o *Orchestrator) StopReplicationInternal(ctx context.Context, replicationUUID string, accountName string, forceStop bool) (*models.VolumeReplication, *datamodel.Job, error) {
	return stopReplicationInternal(ctx, o.storage, o.temporal, replicationUUID, accountName, forceStop)
}

func _stopReplicationInternal(ctx context.Context, se database.Storage, temporal client.Client, volumeReplicationId, accountName string, forceStop bool) (*models.VolumeReplication, *datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		logger.Error("Failed to get account", "error", err)
		return nil, nil, err
	}

	replicationDb, err := se.GetVolumeReplication(ctx, volumeReplicationId)
	if err != nil {
		return nil, nil, err
	}

	replicationDb.State = models.LifeCycleStateUpdating
	replicationDb.StateDetails = models.LifeCycleStateUpdatingDetails

	err = se.UpdateVolumeReplicationStates(ctx, replicationDb)
	if err != nil {
		logger.Error("Failed to update volume replication states in database", "error", err)
		return nil, nil, err
	}

	replicationDb.Account = &datamodel.Account{
		Name: accountName,
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeStopVolumeReplicationInternal),
		State:        string(models.JobsStateNEW),
		ResourceName: replicationDb.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: replicationDb.UUID,
			PoolUUID:     replicationDb.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.StopInternalVolumeReplicationWorkflow,
		replicationDb,
		forceStop,
	)
	if err != nil {
		logger.Error("Failed to execute workflow for resuming volume replication", "error", err)
		return nil, nil, err
	}

	return convertDataStoreReplicationToModel(replicationDb), createdJob, nil
}

func (o *Orchestrator) StopReplication(ctx context.Context, params *commonparams.StopReplicationParams) (*models.VolumeReplication, string, error) {
	return stopReplication(ctx, o.storage, o.temporal, params)
}

func _stopReplication(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.StopReplicationParams) (*models.VolumeReplication, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	event := replication.StopReplicationEvent{
		CommonReplicationEventParams: replication.CommonReplicationEventParams{
			VolumeResourceID:      params.VolumeResourceId,
			ReplicationResourceID: params.ReplicationResourceId,
			AccountName:           params.AccountName,
			XCorrelationID:        &params.CorrelationId,
			Location:              params.Region,
			Zone:                  params.Zone,
		},
		ForceStop: params.ForceStop,
	}

	if params.Zone != "" {
		event.CommonReplicationEventParams.Location = params.Zone
	}

	err = validateReplicationParams(ctx, &event.CommonReplicationEventParams, account.ID, se, false)
	if err != nil {
		return nil, "", err
	}
	dstReplication, err := verifyDstReplicationStop(ctx, &event)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeStopVolumeReplication),
		State:        string(models.JobsStateNEW),
		ResourceName: event.ReplicationModel.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: event.ReplicationModel.UUID,
			PoolUUID:     event.ReplicationModel.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.StopReplicationWorkflow,
		params,
		&event,
	)
	if err != nil {
		logger.Error("Failed to execute workflow", "error", err)
		return nil, "", err
	}
	dstReplication.State = models.LifeCycleStateUpdating
	dstReplication.StateDetails = models.LifeCycleStateUpdatingDetails
	dstReplication.ReplicationAttributes.EndpointType = event.ReplicationModel.ReplicationAttributes.EndpointType
	return dstReplication, createdJob.UUID, nil
}

func convertDataStoreReplicationToModel(replication *datamodel.VolumeReplication) *models.VolumeReplication {
	return &models.VolumeReplication{
		BaseModel: models.BaseModel{
			UUID:      replication.UUID,
			CreatedAt: replication.CreatedAt,
			UpdatedAt: replication.UpdatedAt,
			DeletedAt: DeletedAtOrNil(replication.DeletedAt),
		},
		Name:         replication.Name,
		Description:  replication.Description,
		State:        replication.State,
		StateDetails: replication.StateDetails,
		Uri:          replication.Uri,
		RemoteUri:    replication.RemoteUri,
		ReplicationAttributes: &models.ReplicationDetails{
			EndpointType:               replication.ReplicationAttributes.EndpointType,
			ReplicationType:            replication.ReplicationAttributes.ReplicationType,
			ReplicationSchedule:        replication.ReplicationAttributes.ReplicationSchedule,
			SourcePoolUUID:             replication.ReplicationAttributes.SourcePoolUUID,
			SourceVolumeUUID:           replication.ReplicationAttributes.SourceVolumeUUID,
			SourceRegion:               replication.ReplicationAttributes.SourceLocation,
			SourceHostName:             replication.ReplicationAttributes.SourceHostName,
			SourceReplicationUUID:      replication.ReplicationAttributes.SourceReplicationUUID,
			SourceSvmName:              replication.ReplicationAttributes.SourceSvmName,
			SourceVolumeName:           replication.ReplicationAttributes.SourceVolumeName,
			DestinationPoolUUID:        replication.ReplicationAttributes.DestinationPoolUUID,
			DestinationVolumeUUID:      replication.ReplicationAttributes.DestinationVolumeUUID,
			DestinationRegion:          replication.ReplicationAttributes.DestinationLocation,
			DestinationHostName:        replication.ReplicationAttributes.DestinationHostName,
			DestinationReplicationUUID: replication.ReplicationAttributes.DestinationReplicationUUID,
			DestinationSvmName:         replication.ReplicationAttributes.DestinationSvmName,
			DestinationVolumeName:      replication.ReplicationAttributes.DestinationVolumeName,
		},
		MirrorState:           replication.MirrorState,
		RelationshipStatus:    replication.RelationshipStatus,
		TotalProgress:         replication.TotalProgress,
		TotalTransferBytes:    replication.TotalTransferBytes,
		TotalTransferTimeSecs: replication.TotalTransferTimeSecs,
		LastTransferSize:      replication.LastTransferSize,
		LastTransferError:     replication.LastTransferError,
		LastTransferDuration:  replication.LastTransferDuration,
		LastTransferEndTime:   replication.LastTransferEndTime,
		ProgressLastUpdated:   replication.ProgressLastUpdated,
		LagTime:               replication.LagTime,
		AccountID:             replication.AccountID,
		VolumeID:              replication.VolumeID,
	}
}

func prepareReplicationDataModel(params *commonparams.CreateVolumeReplicationInternalParams, account *datamodel.Account, volume *datamodel.Volume) *datamodel.VolumeReplication {
	return &datamodel.VolumeReplication{
		Name:        params.VolumeReplication.Name,
		Description: params.VolumeReplication.Description,
		Uri:         params.VolumeReplication.RemoteUri,
		RemoteUri:   params.VolumeReplication.Uri,
		ReplicationAttributes: &datamodel.ReplicationDetails{
			EndpointType:               params.VolumeReplication.ReplicationAttributes.EndpointType,
			ReplicationType:            params.VolumeReplication.ReplicationAttributes.ReplicationType,
			ReplicationSchedule:        params.VolumeReplication.ReplicationAttributes.ReplicationSchedule,
			SourcePoolUUID:             params.VolumeReplication.ReplicationAttributes.SourcePoolUUID,
			SourceVolumeUUID:           params.VolumeReplication.ReplicationAttributes.SourceVolumeUUID,
			SourceLocation:             params.VolumeReplication.ReplicationAttributes.SourceRegion,
			SourceHostName:             params.VolumeReplication.ReplicationAttributes.SourceHostName,
			SourceReplicationUUID:      params.VolumeReplication.ReplicationAttributes.SourceReplicationUUID,
			SourceSvmName:              params.VolumeReplication.ReplicationAttributes.SourceSvmName,
			SourceVolumeName:           params.VolumeReplication.ReplicationAttributes.SourceVolumeName,
			DestinationPoolUUID:        params.VolumeReplication.ReplicationAttributes.DestinationPoolUUID,
			DestinationVolumeUUID:      params.VolumeReplication.ReplicationAttributes.DestinationVolumeUUID,
			DestinationLocation:        params.VolumeReplication.ReplicationAttributes.DestinationRegion,
			DestinationHostName:        params.VolumeReplication.ReplicationAttributes.DestinationHostName,
			DestinationReplicationUUID: params.VolumeReplication.ReplicationAttributes.DestinationReplicationUUID,
			DestinationSvmName:         params.VolumeReplication.ReplicationAttributes.DestinationSvmName,
			DestinationVolumeName:      params.VolumeReplication.ReplicationAttributes.DestinationVolumeName,
		},
		MirrorState:           params.VolumeReplication.MirrorState,
		RelationshipStatus:    params.VolumeReplication.RelationshipStatus,
		TotalProgress:         params.VolumeReplication.TotalProgress,
		TotalTransferBytes:    params.VolumeReplication.TotalTransferBytes,
		TotalTransferTimeSecs: params.VolumeReplication.TotalTransferTimeSecs,
		LastTransferSize:      params.VolumeReplication.LastTransferSize,
		LastTransferError:     params.VolumeReplication.LastTransferError,
		LastTransferDuration:  params.VolumeReplication.LastTransferDuration,
		LastTransferEndTime:   params.VolumeReplication.LastTransferEndTime,
		ProgressLastUpdated:   params.VolumeReplication.ProgressLastUpdated,
		LagTime:               params.VolumeReplication.LagTime,
		AccountID:             account.ID,
		Account:               account,
		VolumeID:              params.VolumeReplication.VolumeID,
		Volume:                volume,
	}
}

func (o *Orchestrator) GetReplicationCount(ctx context.Context, projectNumber string) (int64, error) {
	// Get the count of volume replications for the specified account
	count, err := o.storage.GetVolumeReplicationCount(ctx, projectNumber)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CreateVolume creates the specified volume and adds it to the list of volume belonging to the specified owner
func (o *Orchestrator) CreateVolumeReplication(ctx context.Context, params *commonparams.CreateVolumeReplicationParams) (*models.VolumeReplication, string, error) {
	return createVolumeReplication(ctx, o.storage, o.temporal, params)
}

func _createVolumeReplication(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.CreateVolumeReplicationParams) (*models.VolumeReplication, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	srcVolume, err := se.GetVolumeByName(ctx, params.SourceVolumeName)
	if err != nil {
		return nil, "", err
	}

	baseEvent := replication.ReplicationEventBase{}
	baseEvent.AddEvent(commonparams.NewEvent(commonparams.EventCreated, time.Now(), commonparams.String("created_by", "CreateReplication")))

	event := replication.CreateReplicationEvent{
		ReplicationEventBase: baseEvent,
		SourceVolume:         *srcVolume,
		SourcePool:           *srcVolume.Pool,
		XCorrelationID:       &params.CorrelationId,
	}

	err = convertCreateReplicationParamsToEventParam(params, &event)
	if err != nil {
		return nil, "", err
	}

	dbRepl, err := validateCreateReplicationParams(ctx, &event, se)
	if err != nil {
		return nil, "", err
	}

	dbRepl.AccountID = account.ID
	dbRepl.VolumeID = srcVolume.ID
	volumeRep, err := se.CreateVolumeReplication(ctx, dbRepl)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeCreateVolumeReplication),
		State:        string(models.JobsStateNEW),
		ResourceName: dbRepl.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: dbRepl.UUID,
			PoolUUID:     srcVolume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	// Set the workflow ID for the job
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.CreateVolumeReplicationWorkflow,
		params,
		volumeRep,
		&event,
	)

	if err != nil {
		logger.Error("Failed to start create volume replication workflow: ", "error", err)
		return nil, "", err
	}

	return convertDataStoreReplicationToModel(volumeRep), createdJob.UUID, nil
}

func _convertCreateReplicationParamsToEventParam(in *commonparams.CreateVolumeReplicationParams, out *replication.CreateReplicationEvent) error {
	bytes, err := replication.JsonMarshal(in)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrorFailedToMarshalModel, err)
	}

	err = replication.JsonUnMarshal(bytes, out)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrorFailedToUnmarshal, err)
	}

	uri := strings.Split(*out.CreateReplicationParams.DestinationVolumeParameters.StoragePool, "/")

	if len(uri) >= 4 {
		// Grab the pool name from the uri of the destination pool
		out.DestinationPoolName = uri[len(uri)-1]
		// Grab the location from the uri of the destination pool
		out.DestinationLocationID = uri[3]
		// Grab the project number from the uri of the destination pool to check for cross project replication
		out.DestinationProjectNumber = uri[1]
	}
	out.SourceProjectNumber = in.AccountName
	out.LocationID = in.LocationId
	out.SourceRegion = in.Region
	out.VolumeResourceID = in.SourceVolumeName
	out.XCorrelationID = &in.CorrelationId

	return nil
}

func (o *Orchestrator) GetMultipleReplications(ctx context.Context, params commonparams.GetMultipleReplicationsParams) ([]gcpgenserver.ReplicationV1beta, error) {
	return _getMultipleReplications(ctx, o.storage, params)
}

func _getMultipleReplications(ctx context.Context, se database.Storage, params commonparams.GetMultipleReplicationsParams) ([]gcpgenserver.ReplicationV1beta, error) {
	logger := util.GetLogger(ctx)
	resp := []gcpgenserver.ReplicationV1beta{}
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to get account", "error", err)
		return nil, err
	}

	// Check if replication exists in the database
	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", account.ID),
		utils2.NewFilterCondition("uri", "in", params.ReplicationURIs))
	replications, err := se.ListVolumeReplications(ctx, *filter)
	if err != nil {
		logger.Errorf("Failed to list replications for account %s: %v", params.AccountName, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	if len(replications) == 0 {
		logger.Warnf("No replications found for account %s with URIs %v", params.AccountName, params.ReplicationURIs)
		return resp, nil
	}

	// Create a region - replication UUID map
	currentRegion, currentZone, parsingErr := utilParseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		logger.Error("Failed to parse current region", "error", parsingErr)
		custErr := vsaerrors.CustomError{
			TrackingID: 0,
			Message:    parsingErr.Message,
			Retriable:  false,
			HttpCode:   nillable.GetIntPtr(500),
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrRegionZoneParsingErrorCurrentRegion, &custErr)
	}

	currentLocation := currentRegion
	if currentZone != "" {
		currentLocation = currentZone
	}

	regionReplicationMap := make(map[string][]*datamodel.VolumeReplication)
	emptyUUID := uuid.UUID{}

	// Add destination regions with their replications and source regions for job fetching
	for _, replication := range replications {
		// Add destination region with replication
		if replication.ReplicationAttributes.DestinationReplicationUUID != emptyUUID.String() && !nillable.IsNilOrEmpty(&replication.ReplicationAttributes.DestinationLocation) {
			destRegion, _, err := utilParseRegionAndZone(replication.ReplicationAttributes.DestinationLocation)
			if err != nil {
				logger.Error("Failed to parse destination region", "error", err)
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrRegionZoneParsingErrorDestinationRegion, err)
			}
			regionReplicationMap[destRegion] = append(regionReplicationMap[destRegion], replication)
		}

		// Add source region to map (without replications) so we can get active jobs from both regions
		if replication.ReplicationAttributes.SourceReplicationUUID != emptyUUID.String() && !nillable.IsNilOrEmpty(&replication.ReplicationAttributes.SourceLocation) {
			srcRegion, _, err := utilParseRegionAndZone(replication.ReplicationAttributes.SourceLocation)
			if err != nil {
				logger.Error("Failed to parse source region", "error", err)
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrRegionZoneParsingErrorSourceRegion, err)
			}

			// Add source region to map if not already present
			if _, exists := regionReplicationMap[srcRegion]; !exists {
				regionReplicationMap[srcRegion] = []*datamodel.VolumeReplication{}
			}

			// If destination location is empty, add the replication to the source region
			if nillable.IsNilOrEmpty(&replication.ReplicationAttributes.DestinationLocation) {
				regionReplicationMap[srcRegion] = append(regionReplicationMap[srcRegion], replication)
			}
		}
	}

	// Add current region to map if it is missing
	if _, ok := regionReplicationMap[currentRegion]; !ok {
		regionReplicationMap[currentRegion] = []*datamodel.VolumeReplication{}
	}

	// Fetch the replications from the respective regions via internal API calls
	list, jobsList, err := getReplicationObjects(ctx, regionReplicationMap, logger, params)
	if err != nil {
		logger.Error("Failed to get replication objects", "error", err)
		return nil, err
	}

	// Convert the internal replications to the response format
	for _, repl := range list {
		resp = append(resp, convertInternalReplicationToCCFEModel(*repl, currentLocation, &jobsList))
	}

	return resp, nil
}

func _getReplicationObjects(ctx context.Context, regionReplicationMap map[string][]*datamodel.VolumeReplication, logger logger.Logger, params commonparams.GetMultipleReplicationsParams) ([]*googleproxyclient.VolumeReplicationInternalV1beta, []googleproxyclient.InternalJobV1beta, error) {
	type ReplicationsForProject struct {
		replicationUUIDs []string
		token            string
	}
	replicationList := make([]*googleproxyclient.VolumeReplicationInternalV1beta, 0)
	jobsList := make([]googleproxyclient.InternalJobV1beta, 0)

	for region, replicationsInRegion := range regionReplicationMap {
		basePath, err := utilsGetPairedRegionUri(region)
		if err != nil {
			logger.Error("Failed to get paired region URI", "region", region, "error", err)
			return nil, nil, vsaerrors.NewVCPError(vsaerrors.ErrRegionZoneParsingErrorPairedRegionURI, err)
		}

		emptyUUID := uuid.UUID{}

		if len(replicationsInRegion) == 0 {
			// No replications found in this region, get all the jobs for the region
			token, err := authGetSignedJwtToken(params.AccountName)
			if err != nil {
				logger.Error("Failed to get signed JWT token", "error", err)
				return nil, nil, vsaerrors.NewVCPError(vsaerrors.ErrFailedToGenerateAccessToken, err)
			}

			jobs, err := getActiveReplicationJobs(ctx, basePath, token, region, params.AccountName, &params.XCorrelationID)
			if err != nil {
				logger.Error("Failed to get active replication jobs", "error", err, "region", region)
				return nil, nil, err
			}
			jobsList = append(jobsList, jobs...)
			continue
		}

		// Build a map with a list of replication UUIDs for each project
		// Because the replications could use different projects we need to get the token for each project
		replicationsForProjects := make(map[string]ReplicationsForProject)
		for _, replication := range replicationsInRegion {
			projectNumber, err := GetProjectNumberForRegion(replication, region)
			if err != nil {
				return nil, nil, vsaerrors.NewVCPError(vsaerrors.ErrProjectParsingError, err)
			}
			var replicationUUID string
			if replication.ReplicationAttributes.DestinationReplicationUUID != emptyUUID.String() {
				replicationUUID = replication.ReplicationAttributes.DestinationReplicationUUID
			} else if replication.ReplicationAttributes.SourceReplicationUUID != emptyUUID.String() {
				replicationUUID = replication.ReplicationAttributes.SourceReplicationUUID
			}

			found, ok := replicationsForProjects[projectNumber]
			if !ok {
				token, err := authGetSignedJwtToken(projectNumber)
				if err != nil {
					return nil, nil, vsaerrors.NewVCPError(vsaerrors.ErrFailedToGenerateAccessToken, err)
				}
				replicationsForProjects[projectNumber] = ReplicationsForProject{token: token, replicationUUIDs: []string{replicationUUID}}
			} else {
				// Project already exists in the map so we don't need to get a new token and just append to the UUID list.
				found.replicationUUIDs = append(replicationsForProjects[projectNumber].replicationUUIDs, replicationUUID)
			}
		}

		// Now we have a map of project numbers to replication UUIDs, we can make the API calls
		for projectNumber, replicationsForProject := range replicationsForProjects {
			internalGetReplicationBody := googleproxyclient.ReplicationIDListV1beta{
				ReplicationUUIDs: replicationsForProject.replicationUUIDs,
			}
			list, err := googleProxyInternalGetMultipleReplications(ctx, basePath, projectNumber, region, replicationsForProject.token, internalGetReplicationBody, logger, params)
			if err != nil {
				if err.(*vsaerrors.CustomError).TrackingID != vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsNotFound {
					logger.Error("Failed to get multiple replications from Google Proxy", "error", err, "projectNumber", projectNumber, "region", region)
					return nil, nil, err
				}
			}
			if len(list) == 0 {
				logger.Warn("No replications found for project", "projectNumber", projectNumber, "region", region)
				continue
			}

			// Append the replications to the final list
			for _, replication := range list {
				replicationList = append(replicationList, &replication)
			}
		}

		// Get active replication jobs
		for projectNumber, replicationsForProject := range replicationsForProjects {
			// Get all replication jobs for the project
			jobs, err := getActiveReplicationJobs(ctx, basePath, replicationsForProject.token, region, projectNumber, &params.XCorrelationID)
			if err != nil {
				logger.Error("Failed to get active replication jobs", "error", err, "projectNumber", projectNumber, "region", region)
				return nil, nil, err
			}
			jobsList = append(jobsList, jobs...)
		}
	}
	return replicationList, jobsList, nil
}

func _getActiveReplicationJobs(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string) ([]googleproxyclient.InternalJobV1beta, error) {
	logger := util.GetLogger(ctx)

	logger.Debug(
		"cvp geActiveReplicationJobs",
		commonparams.String("destBasePath", basePath),
		commonparams.String("projectNumber", projectNumber),
		commonparams.String("locationID", locationID),
	)

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, token, logger)
	params := googleproxyclient.V1betaInternalGetReplicationJobsParams{}
	params.ProjectNumber = projectNumber
	params.LocationId = locationID
	if xCorrelationID != nil {
		params.XCorrelationID = googleproxyclient.OptString{Value: *xCorrelationID, Set: true}
	} else {
		params.XCorrelationID = googleproxyclient.OptString{Value: "", Set: false}
	}

	getReplicationJobsResponse, err := googleProxyClient.Invoker.V1betaInternalGetReplicationJobs(ctx, params)
	if err != nil {
		return nil, err
	}

	switch r := getReplicationJobsResponse.(type) {
	case *googleproxyclient.V1betaInternalGetReplicationJobsOK:
		return r.Jobs, nil
	case *googleproxyclient.V1betaInternalGetReplicationJobsBadRequest:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsBadRequest, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalGetReplicationJobsInternalServerError:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsInternalServerError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalGetReplicationJobsUnauthorized:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsUnauthorized, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalGetReplicationJobsForbidden:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsForbidden, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalGetReplicationJobsNotFound:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsNotFound, errors.New(r.Message))
	default:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsUnknown, errors.New("unknown response type"))
	}
}

func _googleProxyInternalGetMultipleReplications(ctx context.Context, basePath, projectNumber, location, token string, body googleproxyclient.ReplicationIDListV1beta, logger logger.Logger, paramz commonparams.GetMultipleReplicationsParams) ([]googleproxyclient.VolumeReplicationInternalV1beta, error) {
	cli := googleproxyclient.GetGProxyClient(basePath, token, logger)
	params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
		ProjectNumber:  projectNumber,
		LocationId:     location,
		XCorrelationID: googleproxyclient.NewOptString(paramz.XCorrelationID),
	}

	res, err := cli.Invoker.V1betaGetMultipleReplicationsInternal(ctx, &body, params)
	if err != nil {
		logger.Error("Failed to get multiple replications from Google Proxy", "error", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalOK:
		return r.GetReplications(), nil
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsBadRequest, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsInternalServerError, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsUnauthorized, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsForbidden, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsNotFound, errors.New(r.Message))
	default:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGoogleProxyInternalGetMultipleReplicationsUnknown, errors.New("unknown response type"))
	}
}

func _getProjectNumberForRegion(replication *datamodel.VolumeReplication, region string) (string, error) {
	if strings.Contains(replication.Uri, region) {
		return utilsParseProjectNumberFromURI(replication.Uri)
	}
	return utilsParseProjectNumberFromURI(replication.RemoteUri)
}

func (o *Orchestrator) ReleaseVolumeReplication(ctx context.Context, replicationUUID string) (*models.VolumeReplication, *datamodel.Job, error) {
	return releaseVolumeReplication(ctx, o.storage, o.temporal, replicationUUID)
}

func _releaseVolumeReplication(ctx context.Context, se database.Storage, temporal client.Client, replicationUUID string) (*models.VolumeReplication, *datamodel.Job, error) {
	logger := util.GetLogger(ctx)

	dbVolumeReplication, err := se.GetVolumeReplication(ctx, replicationUUID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Replication not found", "uuid", replicationUUID)
			return nil, nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("VolumeReplication", nil))
		} else {
			logger.Error("Failed to check existing replication", "error", err.Error())
			return nil, nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
	}
	if dbVolumeReplication.State == models.LifeCycleStateCreating ||
		dbVolumeReplication.State == models.LifeCycleStateUpdating ||
		dbVolumeReplication.State == models.LifeCycleStateDeleting {
		return nil, nil, errors.New("Error releasing volume Replication - Volume replication is already transitioning between states")
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeReleaseVolumeReplicationInternal),
		State:        string(models.JobsStateNEW),
		ResourceName: dbVolumeReplication.Uri,
		AccountID:    sql.NullInt64{Int64: dbVolumeReplication.AccountID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: dbVolumeReplication.UUID,
			PoolUUID:     dbVolumeReplication.Volume.Pool.UUID,
		},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, nil, err
	}

	dbVolumeReplication.State = models.LifeCycleStateDeleting
	dbVolumeReplication.StateDetails = models.LifeCycleStateDeletingDetails

	if err = se.UpdateVolumeReplicationStates(ctx, dbVolumeReplication); err != nil {
		return nil, nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.ReleaseVolumeReplicationInternalWorkflow,
		dbVolumeReplication,
	)

	if err != nil {
		logger.Error("Failed to execute workflow for release volume replication ", "error", err)
		return nil, nil, err
	}
	return convertDataStoreReplicationToModel(dbVolumeReplication), createdJob, nil
}

func convertInternalReplicationToCCFEModel(in googleproxyclient.VolumeReplicationInternalV1beta, currentLocation string, jobsList *[]googleproxyclient.InternalJobV1beta) gcpgenserver.ReplicationV1beta {
	sourceReplication := gcpgenserver.ReplicationVolumeInformationV1beta{
		VolumeName: gcpgenserver.NewOptString(in.SourceVolumeName),
		VolumeId:   gcpgenserver.NewOptString(in.SourceVolumeUuid.Value),
	}

	destinationReplication := gcpgenserver.ReplicationVolumeInformationV1beta{
		VolumeName: gcpgenserver.NewOptString(in.DestinationVolumeName),
		VolumeId:   gcpgenserver.NewOptString(in.DestinationVolumeUuid.Value),
	}

	transferStats := gcpgenserver.TransferStatsV1beta{
		TotalTransferBytes:    gcpgenserver.NewOptFloat64(float64(in.TotalTransferBytes.Value)),
		TotalTransferTimeSecs: gcpgenserver.NewOptFloat64(float64(in.TotalTransferTimeSecs.Value)),
		LastTransferSize:      gcpgenserver.NewOptFloat64(float64(in.LastTransferSize.Value)),
		LastTransferError:     gcpgenserver.NewOptString(in.LastTransferError.Value),
		LastTransferDuration:  gcpgenserver.NewOptFloat64(float64(in.LastTransferDuration.Value)),
		LastTransferEndTime:   gcpgenserver.NewOptDateTime(in.LastTransferEndTime.Value),
		TotalProgress:         gcpgenserver.NewOptFloat64(float64(in.TotalProgress.Value)),
		ProgressLastUpdated:   gcpgenserver.NewOptDateTime(in.ProgressLastUpdated.Value),
		LagTime:               gcpgenserver.NewOptFloat64(float64(in.LagTime.Value)),
	}

	out := gcpgenserver.ReplicationV1beta{
		ReplicationId:       gcpgenserver.NewOptString(in.VolumeReplicationUuid.Value),
		ResourceId:          gcpgenserver.NewOptString(in.Name.Value),
		Description:         gcpgenserver.NewOptString(in.Description.Value),
		Source:              gcpgenserver.NewOptReplicationVolumeInformationV1beta(sourceReplication),
		Destination:         gcpgenserver.NewOptReplicationVolumeInformationV1beta(destinationReplication),
		State:               gcpgenserver.NewOptReplicationV1betaState(mapInternalReplicationStateToCCFEState(in.LifeCycleState.Value)),
		StateDetails:        gcpgenserver.NewOptString(in.LifeCycleStateDetails.Value),
		StateDetailsCode:    gcpgenserver.NewOptInt32(0), // Fixme: add state codes mapping when hybrid replication support is added
		ReplicationSchedule: gcpgenserver.NewOptReplicationV1betaReplicationSchedule(mapInternalReplicationScheduleToCCFEReschedule(in.ReplicationSchedule.Value)),
		MirrorState:         gcpgenserver.NewOptReplicationV1betaMirrorState(mapInternalReplicationMirrorStateToCCFEMirrorState(in.MirrorState.Value)),
		Healthy:             gcpgenserver.NewOptBool(in.Healthy.Value),
		TransferStats:       gcpgenserver.NewOptTransferStatsV1beta(transferStats),
		Created:             gcpgenserver.NewOptDateTime(in.CreatedAt.Value),
		Labels:              gcpgenserver.OptReplicationV1betaLabels{},
		// Fixme: add remaining fields when hybrid replication support is added
		ClusterLocation:               gcpgenserver.OptString{},
		HybridReplicationType:         gcpgenserver.OptReplicationV1betaHybridReplicationType{},
		HybridPeeringDetails:          gcpgenserver.OptHybridPeeringV1beta{},
		HybridReplicationUserCommands: gcpgenserver.OptHybridReplicationUserCommandsV1beta{},
	}

	if in.RemoteRegion == currentLocation {
		out.Role = gcpgenserver.NewOptReplicationV1betaRole(gcpgenserver.ReplicationV1betaRoleDESTINATION)
	} else {
		out.Role = gcpgenserver.NewOptReplicationV1betaRole(gcpgenserver.ReplicationV1betaRoleSOURCE)
	}

	// Check active jobs and override state and mirror state if needed
	replicationJobType, hasJob := replicationHasJob(in, jobsList)
	if hasJob {
		switch replicationJobType {
		case string(models.JobTypeDeleteVolumeReplication):
			out.State = gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaStateDELETING)
			out.StateDetails = gcpgenserver.NewOptString(volumeReplicationCVP1betaLifeCycleStateDeleting)
			out.StateDetailsCode = gcpgenserver.NewOptInt32(0)

		case string(models.JobTypeCreateVolumeReplication):
			out.MirrorState = gcpgenserver.NewOptReplicationV1betaMirrorState(gcpgenserver.ReplicationV1betaMirrorStatePREPARING)
			out.State = gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaStateCREATING)
			out.StateDetails = gcpgenserver.NewOptString(volumeReplicationCVP1betaLifeCycleStateCreation)
			out.StateDetailsCode = gcpgenserver.NewOptInt32(0)

		case string(models.JobTypeStopVolumeReplication):
			out.State = gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaStateUPDATING)
			out.StateDetails = gcpgenserver.NewOptString(volumeReplicationCVP1betaLifeCycleStateStopping)
			out.StateDetailsCode = gcpgenserver.NewOptInt32(0)

		case string(models.JobTypeReverseResumeVolumeReplication):
			out.MirrorState = gcpgenserver.NewOptReplicationV1betaMirrorState(gcpgenserver.ReplicationV1betaMirrorStatePREPARING)
			out.State = gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaStateUPDATING)
			out.StateDetails = gcpgenserver.NewOptString(volumeReplicationCVP1betaLifeCycleStateReversing)
			out.StateDetailsCode = gcpgenserver.NewOptInt32(0)

		case string(models.JobTypeResumeVolumeReplication):
			out.MirrorState = gcpgenserver.NewOptReplicationV1betaMirrorState(gcpgenserver.ReplicationV1betaMirrorStatePREPARING)
			out.State = gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaStateUPDATING)
			out.StateDetails = gcpgenserver.NewOptString(volumeReplicationCVP1betaLifeCycleStateResuming)
			out.StateDetailsCode = gcpgenserver.NewOptInt32(0)

		case string(models.JobTypeUpdateVolumeReplication):
			out.State = gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaStateUPDATING)
			out.StateDetails = gcpgenserver.NewOptString(volumeReplicationCVP1betaLifeCycleStateUpdating)
			out.StateDetailsCode = gcpgenserver.NewOptInt32(0)

		default:
			out.MirrorState = gcpgenserver.NewOptReplicationV1betaMirrorState(gcpgenserver.ReplicationV1betaMirrorStatePREPARING)
			out.State = gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaStateUPDATING)
			out.StateDetailsCode = gcpgenserver.NewOptInt32(0)
		}
	}

	return out
}

func replicationHasJob(in googleproxyclient.VolumeReplicationInternalV1beta, jobsList *[]googleproxyclient.InternalJobV1beta) (string, bool) {
	if jobsList == nil {
		return "", false
	}
	for _, job := range *jobsList {
		if in.CcfeUri.Value == job.ResourceName.Value || in.CcfeRemoteUri.Value == job.ResourceName.Value {
			return job.JobType.Value, true
		}
	}
	return "", false
}

func mapInternalReplicationStateToCCFEState(state googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState) gcpgenserver.ReplicationV1betaState {
	// TODO: Add cluster peer states when hybrid replication support is added
	switch state {
	case googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateAvailable:
		return gcpgenserver.ReplicationV1betaStateREADY
	case googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateCreating:
		return gcpgenserver.ReplicationV1betaStateCREATING
	case googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDeleting:
		return gcpgenserver.ReplicationV1betaStateDELETING
	case googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateUpdating:
		return gcpgenserver.ReplicationV1betaStateUPDATING
	case googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError:
		return gcpgenserver.ReplicationV1betaStateERROR
	case googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDisabled:
		return gcpgenserver.ReplicationV1betaStateDISABLED
	default:
		return gcpgenserver.ReplicationV1betaStateSTATEUNSPECIFIED
	}
}

func mapInternalReplicationScheduleToCCFEReschedule(schedule googleproxyclient.VolumeReplicationInternalV1betaReplicationSchedule) gcpgenserver.ReplicationV1betaReplicationSchedule {
	switch schedule {
	case googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleHourly:
		return gcpgenserver.ReplicationV1betaReplicationScheduleHOURLY
	case googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleDaily:
		return gcpgenserver.ReplicationV1betaReplicationScheduleDAILY
	case googleproxyclient.VolumeReplicationInternalV1betaReplicationSchedule10minutely:
		return gcpgenserver.ReplicationV1betaReplicationScheduleEVERY10MINUTES
	default:
		return gcpgenserver.ReplicationV1betaReplicationScheduleREPLICATIONSCHEDULEUNSPECIFIED
	}
}

func mapInternalReplicationMirrorStateToCCFEMirrorState(mirrorState googleproxyclient.VolumeReplicationInternalV1betaMirrorState) gcpgenserver.ReplicationV1betaMirrorState {
	switch mirrorState {
	case googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED:
		return gcpgenserver.ReplicationV1betaMirrorStateMIRRORED
	case googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED:
		return gcpgenserver.ReplicationV1betaMirrorStateUNINITIALIZED
	case googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED:
		return gcpgenserver.ReplicationV1betaMirrorStateSTOPPED
	case googleproxyclient.VolumeReplicationInternalV1betaMirrorStateBASELINETRANSFERRING:
		return gcpgenserver.ReplicationV1betaMirrorStateBASELINETRANSFERRING
	case googleproxyclient.VolumeReplicationInternalV1betaMirrorStateABORTED:
		return gcpgenserver.ReplicationV1betaMirrorStateABORTED
	case googleproxyclient.VolumeReplicationInternalV1betaMirrorStatePREPARING:
		return gcpgenserver.ReplicationV1betaMirrorStatePREPARING
	case googleproxyclient.VolumeReplicationInternalV1betaMirrorStateEXTERNALLYMANAGED:
		return gcpgenserver.ReplicationV1betaMirrorStateEXTERNALLYMANAGED
	case googleproxyclient.VolumeReplicationInternalV1betaMirrorStateTRANSFERRING:
		return gcpgenserver.ReplicationV1betaMirrorStateTRANSFERRING
	default:
		return gcpgenserver.ReplicationV1betaMirrorStateMIRRORSTATEUNSPECIFIED
	}
}

func (o *Orchestrator) ResumeReplication(ctx context.Context, params *commonparams.ResumeReplicationParams) (*models.VolumeReplication, string, error) {
	return resumeReplication(ctx, o.storage, o.temporal, params)
}

func _resumeReplication(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.ResumeReplicationParams) (*models.VolumeReplication, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	event := replication.ResumeReplicationEvent{
		CommonReplicationEventParams: replication.CommonReplicationEventParams{
			VolumeResourceID:      params.VolumeResourceId,
			ReplicationResourceID: params.ReplicationResourceId,
			AccountName:           params.AccountName,
			XCorrelationID:        &params.CorrelationId,
			Location:              params.Region,
			Zone:                  params.Zone,
		},
	}

	if params.Zone != "" {
		event.CommonReplicationEventParams.Location = params.Zone
	}

	err = validateReplicationParams(ctx, &event.CommonReplicationEventParams, account.ID, se, false)
	if err != nil {
		return nil, "", err
	}

	dstReplication, err := verifyDstReplicationResume(ctx, &event)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeResumeVolumeReplication),
		State:        string(models.JobsStateNEW),
		ResourceName: event.ReplicationModel.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: event.ReplicationModel.UUID,
			PoolUUID:     event.ReplicationModel.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.ResumeReplicationWorkflow,
		params,
		&event,
	)
	if err != nil {
		logger.Error("Failed to execute workflow", "error", err)
		return nil, "", err
	}

	dstReplication.State = models.LifeCycleStateUpdating
	dstReplication.StateDetails = models.LifeCycleStateUpdatingDetails
	dstReplication.ReplicationAttributes.EndpointType = event.ReplicationModel.ReplicationAttributes.EndpointType

	return dstReplication, createdJob.UUID, nil
}

func (o *Orchestrator) UpdateReplication(ctx context.Context, params *commonparams.UpdateReplicationParams) (*models.VolumeReplication, string, error) {
	return updateReplication(ctx, o.storage, o.temporal, params)
}

func _updateReplication(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.UpdateReplicationParams) (*models.VolumeReplication, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	event := replication.UpdateReplicationEvent{
		CommonReplicationEventParams: replication.CommonReplicationEventParams{
			VolumeResourceID:      params.VolumeResourceId,
			ReplicationResourceID: params.ReplicationResourceId,
			AccountName:           params.AccountName,
			XCorrelationID:        &params.CorrelationId,
			Location:              params.Region,
			Zone:                  params.Zone,
		},
		ReplicationSchedule: params.ReplicationSchedule,
		Description:         params.Description,
	}

	if params.Zone != "" {
		event.CommonReplicationEventParams.Location = params.Zone
	}

	err = validateReplicationParams(ctx, &event.CommonReplicationEventParams, account.ID, se, false)
	if err != nil {
		return nil, "", err
	}

	dstReplication, err := validateReplicationUpdate(ctx, &event)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeUpdateVolumeReplication),
		State:        string(models.JobsStateNEW),
		ResourceName: event.ReplicationModel.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: event.ReplicationModel.UUID,
			PoolUUID:     event.ReplicationModel.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		replicationWorkflows.UpdateVolumeReplicationWorkflow,
		params,
		&event,
	)
	if err != nil {
		logger.Error("Failed to execute workflow", "error", err)
		return nil, "", err
	}

	dstReplication.State = models.LifeCycleStateUpdating
	dstReplication.StateDetails = models.LifeCycleStateUpdatingDetails
	dstReplication.ReplicationAttributes.EndpointType = event.ReplicationModel.ReplicationAttributes.EndpointType

	return dstReplication, createdJob.UUID, nil
}

func (o *Orchestrator) ResumeReplicationInternal(ctx context.Context, volumeReplicationId, accountName string, forceResume bool) (*models.VolumeReplication, *datamodel.Job, error) {
	return resumeReplicationInternal(ctx, o.storage, o.temporal, volumeReplicationId, accountName, forceResume)
}

func _resumeReplicationInternal(ctx context.Context, se database.Storage, temporal client.Client, volumeReplicationId, accountName string, forceResume bool) (*models.VolumeReplication, *datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		logger.Error("Failed to get or create account", "error", err)
		return nil, nil, err
	}

	replicationDb, err := se.GetVolumeReplication(ctx, volumeReplicationId)
	if err != nil {
		return nil, nil, err
	}

	replicationDb.State = models.LifeCycleStateUpdating
	replicationDb.StateDetails = models.LifeCycleStateUpdatingDetails

	err = se.UpdateVolumeReplicationStates(ctx, replicationDb)
	if err != nil {
		logger.Error("Failed to update volume replication states in database", "error", err)
		return nil, nil, err
	}

	replicationDb.Account = &datamodel.Account{
		Name: accountName,
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeResumeVolumeReplicationInternal),
		State:        string(models.JobsStateNEW),
		ResourceName: replicationDb.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: replicationDb.UUID,
			PoolUUID:     replicationDb.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.ResumeInternalVolumeReplicationWorkflow,
		replicationDb,
		forceResume,
	)
	if err != nil {
		logger.Error("Failed to execute workflow for resuming volume replication", "error", err)
		return nil, nil, err
	}

	return convertDataStoreReplicationToModel(replicationDb), createdJob, nil
}

// GetReplication gets the specified replication
func (o *Orchestrator) GetReplication(ctx context.Context, volumeReplicationId string) (*models.VolumeReplication, error) {
	se := o.storage

	replication, err := se.GetVolumeReplication(ctx, volumeReplicationId)
	if err != nil {
		return nil, err
	}

	return convertDataStoreReplicationToModel(replication), nil
}

func (o *Orchestrator) DeleteReplicationInternal(ctx context.Context, volumeReplicationId string, cleanupAfterReverse bool) (*models.VolumeReplication, *datamodel.Job, error) {
	return deleteReplicationInternal(ctx, o.storage, o.temporal, volumeReplicationId, cleanupAfterReverse)
}

func _deleteReplicationInternal(ctx context.Context, se database.Storage, temporal client.Client, volumeReplicationId string, cleanupAfterReverse bool) (*models.VolumeReplication, *datamodel.Job, error) {
	logger := util.GetLogger(ctx)

	dbVolumeReplication, err := se.GetVolumeReplication(ctx, volumeReplicationId)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Warn("Volume replication not found", "volumeReplicationId", volumeReplicationId)
			return nil, nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("replication", nil))
		}
		return nil, nil, err
	}

	if dbVolumeReplication.State == models.LifeCycleStateCreating ||
		dbVolumeReplication.State == models.LifeCycleStateUpdating ||
		dbVolumeReplication.State == models.LifeCycleStateDeleting {
		return nil, nil, errors.New("Error deleting volume Replication - Volume replication is already transitioning between states")
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeDeleteVolumeReplicationInternal),
		State:        string(models.JobsStateNEW),
		ResourceName: dbVolumeReplication.Uri,
		AccountID:    sql.NullInt64{Int64: dbVolumeReplication.AccountID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: dbVolumeReplication.UUID,
			PoolUUID:     dbVolumeReplication.Volume.Pool.UUID,
		},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, nil, err
	}

	if !cleanupAfterReverse {
		dbVolumeReplication.State = models.LifeCycleStateDeleting
		dbVolumeReplication.StateDetails = models.LifeCycleStateDeletingDetails

		if err = se.UpdateVolumeReplicationStates(ctx, dbVolumeReplication); err != nil {
			return nil, nil, err
		}
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.DeleteInternalVolumeReplicationWorkflow,
		dbVolumeReplication,
		cleanupAfterReverse,
	)

	if err != nil {
		logger.Error("Failed to execute workflow for volume replication deletion", "error", err)
		return nil, nil, err
	}

	return convertDataStoreReplicationToModel(dbVolumeReplication), createdJob, nil
}

func (o *Orchestrator) DeleteReplication(ctx context.Context, params *commonparams.DeleteReplicationParams, isCleanUp bool) (*models.VolumeReplication, string, error) {
	return deleteReplication(ctx, o.storage, o.temporal, params, isCleanUp)
}

func _deleteReplication(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.DeleteReplicationParams, isCleanUp bool) (*models.VolumeReplication, string, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to get or create account", "error", err)

		return nil, "", err
	}

	event := replication.DeleteReplicationEvent{
		CommonReplicationEventParams: replication.CommonReplicationEventParams{
			VolumeResourceID:      params.VolumeResourceId,
			ReplicationResourceID: params.ReplicationResourceId,
			AccountName:           params.AccountName,
			XCorrelationID:        &params.CorrelationId,
			Location:              params.Region,
			Zone:                  params.Zone,
		},
	}

	if params.Zone != "" {
		event.CommonReplicationEventParams.Location = params.Zone
	}

	err = validateReplicationParams(ctx, &event.CommonReplicationEventParams, account.ID, se, isCleanUp)
	if err != nil {
		return nil, "", err
	}
	dstReplication := &models.VolumeReplication{
		ReplicationAttributes: &models.ReplicationDetails{
			EndpointType: event.ReplicationModel.ReplicationAttributes.EndpointType,
		},
	}

	if !isCleanUp {
		dstReplication, err = VerifyDstReplicationDelete(ctx, &event)
		if err != nil {
			return nil, "", err
		}
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeDeleteVolumeReplication),
		State:        string(models.JobsStateNEW),
		ResourceName: event.ReplicationModel.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: event.ReplicationModel.UUID,
			PoolUUID:     event.ReplicationModel.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	if isCleanUp {
		_, err = temporal.ExecuteWorkflow(ctx,
			client.StartWorkflowOptions{
				TaskQueue:             workflowengine.CustomerTaskQueue,
				ID:                    createdJob.WorkflowID,
				WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
			},
			replicationWorkflows.ReplicationCleanupWorkflow,
			params,
			&event,
		)
	} else {
		_, err = temporal.ExecuteWorkflow(ctx,
			client.StartWorkflowOptions{
				TaskQueue:             workflowengine.CustomerTaskQueue,
				ID:                    createdJob.WorkflowID,
				WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
			},
			replicationWorkflows.ReplicationDeleteWorkflow,
			params,
			&event,
		)
	}

	if err != nil {
		logger.Error("Failed to execute workflow", "error", err)
		return nil, "", err
	}
	dstReplication.State = models.LifeCycleStateDeleting
	dstReplication.StateDetails = models.LifeCycleStateDeletingDetails

	return dstReplication, createdJob.UUID, nil
}

func (o *Orchestrator) SyncReplication(ctx context.Context, params *commonparams.ResumeReplicationParams) (*models.VolumeReplication, string, error) {
	return syncReplication(ctx, o.storage, o.temporal, params)
}

func _syncReplication(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.ResumeReplicationParams) (*models.VolumeReplication, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	event := replication.ResumeReplicationEvent{
		CommonReplicationEventParams: replication.CommonReplicationEventParams{
			VolumeResourceID:      params.VolumeResourceId,
			ReplicationResourceID: params.ReplicationResourceId,
			AccountName:           params.AccountName,
			XCorrelationID:        &params.CorrelationId,
			Location:              params.Region,
			Zone:                  params.Zone,
		},
	}

	if params.Zone != "" {
		event.CommonReplicationEventParams.Location = params.Zone
	}

	err = validateReplicationParams(ctx, &event.CommonReplicationEventParams, account.ID, se, false)
	if err != nil {
		return nil, "", err
	}

	dstReplication, err := verifyDstReplicationSync(ctx, &event)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeSyncVolumeReplication),
		State:        string(models.JobsStateNEW),
		ResourceName: event.ReplicationModel.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: event.ReplicationModel.UUID,
			PoolUUID:     event.ReplicationModel.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.ResumeReplicationWorkflow,
		params,
		&event,
	)
	if err != nil {
		logger.Error("Failed to execute workflow", "error", err)
		return nil, "", err
	}

	dstReplication.State = models.LifeCycleStateUpdating
	dstReplication.StateDetails = models.LifeCycleStateSyncDetails
	dstReplication.ReplicationAttributes.EndpointType = event.ReplicationModel.ReplicationAttributes.EndpointType

	return dstReplication, createdJob.UUID, nil
}

func (o *Orchestrator) ReverseReplicationInternal(ctx context.Context, volumeReplicationId, accountName string) (*models.VolumeReplication, *datamodel.Job, error) {
	return reverseReplicationInternal(ctx, o.storage, o.temporal, volumeReplicationId, accountName)
}

func _reverseReplicationInternal(ctx context.Context, se database.Storage, temporal client.Client, volumeReplicationId, accountName string) (*models.VolumeReplication, *datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		logger.Error("Failed to get or create account", "error", err)
		return nil, nil, err
	}

	replicationDb, err := se.GetVolumeReplication(ctx, volumeReplicationId)
	if err != nil {
		return nil, nil, err
	}

	replicationDb.State = models.LifeCycleStateUpdating
	replicationDb.StateDetails = models.LifeCycleStateUpdatingDetails

	err = se.UpdateVolumeReplicationStates(ctx, replicationDb)
	if err != nil {
		logger.Error("Failed to update volume replication states in database", "error", err)
		return nil, nil, err
	}

	replicationDb.Account = &datamodel.Account{
		Name: accountName,
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeReverseVolumeReplicationInternal),
		State:        string(models.JobsStateNEW),
		ResourceName: replicationDb.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: replicationDb.UUID,
			PoolUUID:     replicationDb.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.ReverseInternalVolumeReplicationWorkflow,
		replicationDb,
	)
	if err != nil {
		logger.Error("Failed to execute workflow for reversing volume replication", "error", err)
		return nil, nil, err
	}

	return convertDataStoreReplicationToModel(replicationDb), createdJob, nil
}

func (o *Orchestrator) ReverseAndResumeReplication(ctx context.Context, params *commonparams.ReverseAndResumeReplicationParams) (*models.VolumeReplication, *string, error) {
	return reverseAndResumeReplication(ctx, o.storage, o.temporal, params)
}

func _reverseAndResumeReplication(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.ReverseAndResumeReplicationParams) (*models.VolumeReplication, *string, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to get or create account", "error", err)
		return nil, nil, err
	}

	event := replication.ReverseReplicationEvent{
		CommonReplicationEventParams: replication.CommonReplicationEventParams{
			VolumeResourceID:      params.VolumeResourceId,
			ReplicationResourceID: params.ReplicationResourceId,
			AccountName:           params.AccountName,
			XCorrelationID:        &params.CorrelationId,
			Location:              params.Region,
			Zone:                  params.Zone,
		},
	}

	if params.Zone != "" {
		event.CommonReplicationEventParams.Location = params.Zone
	}

	err = validateReplicationParams(ctx, &event.CommonReplicationEventParams, account.ID, se, false)
	if err != nil {
		return nil, nil, err
	}

	replicationDb, err := verifyDstReplicationReverse(ctx, &event)
	if err != nil {
		return nil, nil, err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeReverseResumeVolumeReplication),
		State:        string(models.JobsStateNEW),
		ResourceName: event.ReplicationModel.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: event.ReplicationModel.UUID,
			PoolUUID:     event.ReplicationModel.Volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.ReverseAndResumeVolumeReplicationWorkflow,
		params,
		&event,
	)

	if err != nil {
		logger.Error("Failed to execute workflow", "error", err)
		return nil, nil, err
	}

	replicationDb.State = models.LifeCycleStateUpdating
	replicationDb.StateDetails = models.LifeCycleStateUpdatingDetails
	replicationDb.ReplicationAttributes.EndpointType = event.ReplicationModel.ReplicationAttributes.EndpointType

	return replicationDb, &createdJob.UUID, nil
}
