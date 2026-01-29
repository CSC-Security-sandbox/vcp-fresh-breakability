package activities

import (
	"context"
	"database/sql"
	"strings"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
	InternalParseRegionAndZone     = utils.ParseRegionAndZone
)

type UpdateVolumeInReplicationActivity struct {
	SE database.Storage
}

func (a *UpdateVolumeInReplicationActivity) GetReplicationFromDBVolume(ctx context.Context, dbVolume *datamodel.Volume, event *common.VolumeUpdateEventParams, params *common.UpdateVolumeParams) (*common.VolumeUpdateEventParams, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("volume_id", "=", dbVolume.ID))

	dbReplication, err := se.ListVolumeReplications(ctx, *filter, database.QueryDepthZero)
	if err != nil {
		logger.Error("Failed to list volume replications", "error", err)
		return nil, err
	}
	if dbReplication != nil && len(dbReplication) == 0 {
		logger.Error("No replication found for the volume", "volumeID", dbVolume.ID)
		return nil, utilErrors.NewNonRetryableErr("no replication found for the volume")
	}

	repldb := dbReplication[0]
	remoteProject, err := utilsParseProjectNumberFromURI(repldb.RemoteUri)
	if err != nil {
		logger.Error("Parse Remote URI Error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrProjectParsingError, err)
	}

	event.URI = repldb.Uri
	localRegion, _, parseError := InternalParseRegionAndZone(repldb.ReplicationAttributes.SourceLocation)
	if parseError != nil {
		logger.Error("Parse Source Location Error")
		return nil, errors.NewVCPError(errors.ErrParseSourceLocation, errors.New(parseError.Error()))
	}

	remoteRegion, _, parseError := InternalParseRegionAndZone(repldb.ReplicationAttributes.DestinationLocation)
	if parseError != nil {
		logger.Error("Parse Destination Location Error")
		return nil, errors.NewVCPError(errors.ErrParseDestinationLocation, errors.New(parseError.Error()))
	}

	if repldb.ReplicationAttributes.EndpointType == coreModels.SrcEndpoint {
		event.Local.ProjectNumber = params.AccountName
		event.Remote.ProjectNumber = remoteProject
		event.Local.Region = localRegion
		event.Local.Location = repldb.ReplicationAttributes.SourceLocation
		event.Remote.Region = remoteRegion
		event.Remote.Location = repldb.ReplicationAttributes.DestinationLocation
		event.Remote.VolumeUUID = repldb.ReplicationAttributes.DestinationVolumeUUID
		event.Remote.PoolUUID = repldb.ReplicationAttributes.DestinationPoolUUID
	} else {
		event.Local.ProjectNumber = remoteProject
		event.Local.Region = remoteRegion
		event.Local.Location = repldb.ReplicationAttributes.DestinationLocation
		event.Remote.ProjectNumber = params.AccountName
		event.Remote.Region = localRegion
		event.Remote.Location = repldb.ReplicationAttributes.SourceLocation
		event.Remote.VolumeUUID = repldb.ReplicationAttributes.SourceVolumeUUID
		event.Remote.PoolUUID = repldb.ReplicationAttributes.SourcePoolUUID
	}

	return event, nil
}

func (a *UpdateVolumeInReplicationActivity) GetLocalBasePathVolume(ctx context.Context, event *common.VolumeUpdateEventParams) (*common.VolumeUpdateEventParams, error) {
	localBasePath, err := GetBasePath(ctx, event.Local.Region)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	event.Local.BasePath = *localBasePath
	return event, nil
}

func (a *UpdateVolumeInReplicationActivity) GetRemoteBasePathVolume(ctx context.Context, event *common.VolumeUpdateEventParams) (*common.VolumeUpdateEventParams, error) {
	remoteBasePath, err := GetBasePath(ctx, event.Remote.Region)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	event.Remote.BasePath = *remoteBasePath
	return event, nil
}

func (a *UpdateVolumeInReplicationActivity) GetSignedLocalTokenVolume(ctx context.Context, event *common.VolumeUpdateEventParams) (*common.VolumeUpdateEventParams, error) {
	localJwt, err := GetSignedToken(ctx, event.Local.ProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	event.Local.JwtToken = *localJwt
	return event, nil
}

func (a *UpdateVolumeInReplicationActivity) GetSignedRemoteTokenVolume(ctx context.Context, event *common.VolumeUpdateEventParams) (*common.VolumeUpdateEventParams, error) {
	if event.Local.ProjectNumber == event.Remote.ProjectNumber {
		event.Remote.JwtToken = event.Local.JwtToken
		return event, nil
	}
	remoteJwt, err := GetSignedToken(ctx, event.Remote.ProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	event.Remote.JwtToken = *remoteJwt
	return event, nil
}

func (a *UpdateVolumeInReplicationActivity) CreateJobForChildWorkflow(ctx context.Context, volume *datamodel.Volume) (*datamodel.Job, error) {
	se := a.SE

	job := &datamodel.Job{
		Type:          string(coreModels.JobTypeUpdateVolume),
		State:         string(coreModels.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: volume.AccountID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: volume.UUID},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		return nil, err
	}

	return createdJob, nil
}

func (a *UpdateVolumeInReplicationActivity) GetReplicationMirrorState(ctx context.Context, event *common.VolumeUpdateEventParams, dbVolume *datamodel.Volume) (*string, error) {
	logger := util.GetLogger(ctx)

	// Get replication from database to access destination replication UUID
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("volume_id", "=", dbVolume.ID))

	dbReplications, err := a.SE.ListVolumeReplications(ctx, *filter, database.QueryDepthZero)
	if err != nil {
		logger.Error("Failed to list volume replications", "error", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
	}
	if dbReplications == nil || len(dbReplications) == 0 {
		logger.Error("No replication found for the volume", "volumeID", dbVolume.ID)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataNotFoundError, utilErrors.NewNotFoundErr("replication", nil))
	}

	dbReplication := dbReplications[0]
	destinationReplicationUUID := dbReplication.ReplicationAttributes.DestinationReplicationUUID
	if destinationReplicationUUID == "" {
		logger.Error("Destination replication UUID is empty", "replicationUUID", dbReplication.UUID)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataNotFoundError, utilErrors.NewNotFoundErr("destination replication UUID", nil))
	}

	projectNumber := event.Local.ProjectNumber
	token := event.Local.JwtToken
	basePath := event.Local.BasePath
	if dbReplication.ReplicationAttributes.EndpointType == "src" {
		projectNumber = event.Remote.ProjectNumber
		token = event.Remote.JwtToken
		basePath = event.Remote.BasePath
	}
	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, token, logger)
	getMultiReplicationParams := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
		ProjectNumber:  projectNumber,
		LocationId:     dbReplication.ReplicationAttributes.DestinationLocation,
		XCorrelationID: googleproxyclient.NewOptString(event.CorrelationID),
	}
	req := &googleproxyclient.ReplicationIDListV1beta{
		ReplicationUUIDs: []string{destinationReplicationUUID},
	}
	res, err := googleProxyClient.Invoker.V1betaGetMultipleReplicationsInternal(ctx, req, getMultiReplicationParams)
	if err != nil || res == nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}
	switch r := res.(type) {
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalOK:
		if len(r.Replications) == 0 {
			return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, errors.New("no replications returned"))
		}
		if !r.Replications[0].MirrorState.IsSet() {
			return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, errors.New("mirror state not set in response"))
		}
		mirrorState := string(r.Replications[0].MirrorState.Value)
		return &mirrorState, nil
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsBadRequest, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsUnauthorized, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForbidden, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsNotFound, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsInternalServerError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsUnknown, errors.New("unexpected response type from Google Proxy"))
	}
}

func (a *UpdateVolumeInReplicationActivity) GetRemotePoolDetailsVolume(ctx context.Context, event *common.VolumeUpdateEventParams) (*googleproxyclient.PoolV1beta, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(event.Remote.BasePath, event.Remote.JwtToken, logger)
	getPoolParams := &googleproxyclient.V1betaDescribePoolParams{
		ProjectNumber:  event.Remote.ProjectNumber,
		LocationId:     event.Remote.Location,
		PoolId:         event.Remote.PoolUUID,
		XCorrelationID: googleproxyclient.NewOptString(event.CorrelationID),
	}
	res, err := googleProxyClient.Invoker.V1betaDescribePool(ctx, *getPoolParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDescribePool, err)
	}
	switch r := res.(type) {
	case *googleproxyclient.PoolV1beta:
		return r, nil
	case *googleproxyclient.V1betaDescribePoolBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDescribePool, errors.New(r.Message))
	case *googleproxyclient.V1betaDescribePoolUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDescribePool, errors.New(r.Message))
	case *googleproxyclient.V1betaDescribePoolForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDescribePool, errors.New(r.Message))
	case *googleproxyclient.V1betaDescribePoolNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDescribePool, errors.New(r.Message))
	case *googleproxyclient.V1betaDescribePoolInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDescribePool, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDescribePool, errors.New("unexpected response type from Google Proxy"))
	}
}

func (a *UpdateVolumeInReplicationActivity) ValidateRemoteVolumeUpdate(ctx context.Context, pool *googleproxyclient.PoolV1beta, params *common.UpdateVolumeParams, dbVolume *datamodel.Volume) (bool, error) {
	// check for sync attributes only otherwise skip remote volume update
	if params.QuotaInBytes <= dbVolume.SizeInBytes {
		return false, nil
	}

	var allocatedBytes float64
	allocatedBytes = 0
	if pool.AllocatedBytes.Set {
		allocatedBytes = pool.AllocatedBytes.Value
	}

	if (pool.SizeInBytes - allocatedBytes) < float64(params.QuotaInBytes-dbVolume.SizeInBytes) {
		return false, errors.NewVCPError(errors.ErrDestPoolSize, errors.New("Volume exceeds destination pool size"))
	}

	return true, nil
}

func (a *UpdateVolumeInReplicationActivity) UpdateRemoteVolume(ctx context.Context, params *common.UpdateVolumeParams, event *common.VolumeUpdateEventParams) (*string, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(event.Remote.BasePath, event.Remote.JwtToken, logger)
	updateVolumeParams := &googleproxyclient.V1betaInternalUpdateVolumeParams{
		ProjectNumber:  event.Remote.ProjectNumber,
		LocationId:     event.Remote.Location,
		VolumeId:       event.Remote.VolumeUUID,
		XCorrelationID: googleproxyclient.NewOptString(event.CorrelationID),
	}
	requestBody := &googleproxyclient.VolumeUpdateV1beta{
		QuotaInBytes: googleproxyclient.NewOptNilFloat64(float64(params.QuotaInBytes)),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalUpdateVolume(ctx, requestBody, *updateVolumeParams)
	if err != nil {
		return nil, err
	}
	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		jobId := ""
		parts := strings.Split(r.Name.Value, "/")
		if len(parts) > 0 {
			jobId = parts[len(parts)-1]
			return &jobId, nil
		}
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New("invalid operation name received from Google Proxy"))
	case *googleproxyclient.V1betaInternalUpdateVolumeBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateVolumeUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateVolumeForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateVolumeNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateVolumeConflict:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateVolumeInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New("unexpected response type from Google Proxy"))
	}
}

func (a *UpdateVolumeInReplicationActivity) DescribeRemoteJobVolumeUpdate(ctx context.Context, event *common.VolumeUpdateEventParams, jobId string) error {
	err := DescribeJob(ctx, &jobId, &event.Remote.BasePath, &event.Remote.JwtToken, &event.Remote.ProjectNumber, &event.Remote.Location, &event.CorrelationID)
	if err != nil {
		return err
	}
	return nil
}

func GetBasePath(ctx context.Context, region string) (*string, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("getBasePath")
	basePath, err := replication.InternalUtilGetPairedRegionURI(region)
	if err != nil {
		return nil, err
	}
	return &basePath, nil
}

func GetSignedToken(ctx context.Context, projectNumber string) (*string, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("getSignedToken")
	jwt, err := replication.InternalUtilGetSignedToken(projectNumber)
	if err != nil {
		return nil, err
	}
	return &jwt, nil
}
