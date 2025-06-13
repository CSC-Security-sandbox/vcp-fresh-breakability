package orchestrator

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/replicationWorkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

var (
	createVolumeReplicationInternal            = _createVolumeReplicationInternal
	createVolumeReplication                    = _createVolumeReplication
	convertCreateReplicationParamsToEventParam = _convertCreateReplicationParamsToEventParam
	validateCreateReplicationParams            = replication.ValidateCreateReplicationParams
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
		ResourceName:  params.VolumeReplication.Name,
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

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
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
	}

	err = convertCreateReplicationParamsToEventParam(params, &event)
	if err != nil {
		return nil, "", err
	}

	dbRepl, err := validateCreateReplicationParams(ctx, &event, se)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeCreateVolumeReplication),
		State:        string(models.JobsStateNEW),
		ResourceName: dbRepl.Uri,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	dbRepl.AccountID = account.ID
	dbRepl.VolumeID = srcVolume.ID
	volumeRep, err := se.CreateVolumeReplication(ctx, dbRepl)
	if err != nil {
		return nil, "", err
	}
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
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
		return errors.NewVCPError(errors.ErrorFailedToMarshalModel, err)
	}

	err = replication.JsonUnMarshal(bytes, out)
	if err != nil {
		return errors.NewVCPError(errors.ErrorFailedToUnmarshal, err)
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
	out.LocationID = in.Region
	out.VolumeResourceID = in.SourceVolumeName
	out.XCorrelationID = &in.CorrelationId

	return nil
}
