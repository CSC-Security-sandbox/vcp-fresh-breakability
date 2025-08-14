package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/pools"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/replications"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	ValidateReplicationParams       = _validateReplicationParams
	ValidateCreateReplicationParams = _validateCreateReplicationParams
	validateReplicationResourceId   = _validateReplicationResourceId
	validateLabels                  = _validateLabels
	internalUtilGetCCFEURI          = GetCCFEURI
	utilsParseProjectNumberFromURI  = utils.ParseProjectNumberFromURI

	validateStoragePoolUri      = _validateStoragePoolUri
	getDestinationPool          = _getDestinationPool
	getVolume                   = _getVolume
	describeVolume              = _describeVolume
	createReplicationObjects    = _createReplicationObjects
	replicationJobInProcess     = _replicationJobInProcess
	internalGetReplicationCount = _internalGetReplicationCount
	internalGetVolumeCount      = _internalGetVolumeCount
	getReplicationJobs          = _getReplicationJobs
	getReplication              = _getReplication
	VerifyDstReplicationResume  = _verifyDstReplicationResume
	ValidateReplicationUpdate   = _validateReplicationUpdate
	VerifyDstReplicationStop    = _verifyDstReplicationStop
	VerifyDstVolume             = _verifyDstVolume
	VerifyDstReplication        = _verifyDstReplication
	VerifyDstReplicationSync    = _verifyDstReplicationSync

	InternalUtilGetCallbackToken   = auth.GetSignedAccessToken
	InternalUtilGetSignedToken     = auth.GetSignedJwtToken
	InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
	internalParseRegionAndZone     = utils.ParseRegionAndZone

	regexpCompile    = regexp.Compile
	JsonMarshal      = json.Marshal
	JsonUnMarshal    = json.Unmarshal
	hydrationEnabled = env.GetBool("GCP_HYDRATE_ENABLED", true)
	getQuotaLimit    = common.GetQuotaLimit
)

type QuotaType string
type ResourceType string

const (
	storageUriRegex = "^projects\\/([^\\/]+)\\/locations\\/([^\\/]+)\\/storagePools|pools\\/([^\\/]+)$"
	maxRuneCount    = 63
	maxByteCount    = 128
)

func _validateCreateReplicationParams(ctx context.Context, event *CreateReplicationEvent, se database.Storage) (*datamodel.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("Starting validateCreateReplicationParams")

	if *event.CreateReplicationParams.ReplicationSchedule == models.ReplicationV1betaReplicationScheduleREPLICATIONSCHEDULEUNSPECIFIED {
		typeErr := errors.NewVCPError(errors.ErrWorkflowConfigurationError, errors.New("replicationSchedule is UNSPECIFIED"))
		logger.Error("replicationSchedule is UNSPECIFIED", common.Error(typeErr))
		return nil, typeErr
	}

	if event.CreateReplicationParams.Labels != nil {
		err := validateLabels(event.CreateReplicationParams.Labels)
		if err != nil {
			logger.Error("validateLabels error", common.Error(err))
			return nil, err
		}
	}

	token, err := InternalUtilGetSignedToken(event.SourceProjectNumber)
	if err != nil {
		logger.Error("Get Signed Token Error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}

	dstToken := token
	if event.DestinationProjectNumber != event.SourceProjectNumber {
		// if remoteProject is not the same as the projectNumber, we need to get a new token for the remote project
		dstToken, err = InternalUtilGetSignedToken(event.DestinationProjectNumber)
		if err != nil {
			logger.Error("Get Signed Token Error For Remote Project", common.Error(err))
			return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
		}
	}

	sourceRegion, _, parseError := internalParseRegionAndZone(event.LocationID)
	if parseError != nil {
		logger.Error("Parse Source Location Error")
		return nil, errors.NewVCPError(errors.ErrParseSourceLocation, errors.New(parseError.Error()))
	}
	srcBasePath, err := InternalUtilGetPairedRegionURI(sourceRegion)
	if err != nil {
		logger.Error("Get Paired Source Region Uri error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}

	destRegion, _, parseError := internalParseRegionAndZone(event.DestinationLocationID)
	if parseError != nil {
		logger.Error("Parse Destination Location Error", common.Error(errors.New(parseError.Error())))
		return nil, errors.NewVCPError(errors.ErrParseDestinationLocation, errors.New(parseError.Error()))
	}
	destBasePath, err := InternalUtilGetPairedRegionURI(destRegion)
	if err != nil {
		logger.Error("Get Paired Destination Region Uri error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}

	event.CCFEUri = internalUtilGetCCFEURI(event.SourceProjectNumber, event.LocationID, event.VolumeResourceID, *event.CreateReplicationParams.ResourceID)

	err = validateReplicationResourceId(ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, se)
	if err != nil {
		logger.Error("Replication resourceId error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrValidateCreateResourceIdInUse, err)
	}

	if event.SourceVolume.VolumeAttributes.IsDataProtection {
		logger.Error("sourceVolume already in replication")
		return nil, errors.NewVCPError(errors.ErrValidateCreateSourceVolumeInReplicationGroup, errors.New("sourceVolume already in replication"))
	}

	if event.SourceVolume.State != string(googleproxyclient.VolumeV1betaVolumeStateREADY) {
		logger.Error("sourceVolume is not in a READY state")
		return nil, errors.NewVCPError(errors.ErrValidateCreateSourceVolumeNotReady, errors.New("sourceVolume is not in a READY state"))
	}

	err = validateStoragePoolUri(*event.CreateReplicationParams.DestinationVolumeParameters.StoragePool)
	if err != nil {
		logger.Error("validateStoragePoolUri error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrValidateStoragePoolUri, err)
	}

	destPool, err := getDestinationPool(ctx, destBasePath, dstToken, event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName)
	if err != nil {
		logger.Error("getDestinationPool error", common.Error(err))
		return nil, err
	}

	if isPoolInTransitionState(destPool) {
		typeErr := errors.NewVCPError(errors.ErrValidateDestinationPoolTransitioning, errors.New("Destination pool is in transition state"))
		logger.Error("Destination pool is in transition state, Please try after some time", common.Error(typeErr))
		return nil, typeErr
	}
	if !isPoolHealthy(destPool) {
		typeErr := errors.NewVCPError(
			errors.ErrValidateDestinationStoragePoolState, errors.New("Destination pool is in unhealthy state, Please try after some time"))
		logger.Error("Destination pool is in unhealthy state, Please try after some time", common.Error(typeErr))
		return nil, typeErr
	}

	bytesNeeded := float64(event.SourceVolume.SizeInBytes) + destPool.AllocatedBytes.Value
	if bytesNeeded > destPool.SizeInBytes {
		typeErr := errors.NewVCPError(errors.ErrDestPoolSize, errors.New("Volume exceeds destination pool size"))
		logger.Error("Volume exceeds destination pool size", common.Error(typeErr))
		return nil, typeErr
	}

	if event.SourceVolume.Pool.ServiceLevel != string(destPool.ServiceLevel) {
		typeErr := errors.NewVCPError(errors.ErrServiceLevelMismatch, errors.New("Service level on source volume and destination pool do not match"))
		logger.Error("Service level on source volume and destination pool do not match", common.Error(typeErr))
		return nil, typeErr
	}

	err = replicationJobInProcess(ctx, event.SourceProjectNumber, event.DestinationProjectNumber, srcBasePath, destBasePath, event.LocationID, event.DestinationLocationID, token, dstToken, event.CCFEUri, "", event.SourcePool.UUID, destPool.PoolId.Value, event.XCorrelationID)
	if err != nil {
		return nil, err
	}

	if hydrationEnabled {
		storageClass := models.StorageClassV1betaSOFTWARE
		serviceLevel := event.SourceVolume.Pool.ServiceLevel
		callbackToken, err := InternalUtilGetCallbackToken()
		if err != nil {
			logger.Error("Get callback token error", common.Error(err))
			return nil, errors.NewVCPError(errors.ErrGetSignedCallbackToken, err)
		}

		replicationQuotaLimit, err := getQuotaLimit(ctx, logger, event.LocationID, event.SourceProjectNumber, callbackToken, common.ResourceTypeReplication)
		if err != nil {
			println(err.Error())
			logger.Error("Get replication quota limit error", common.Error(err))
			return nil, errors.NewVCPError(errors.ErrGetReplicationQuotaLimitInternal, err)
		}
		destReplicationCount, err := internalGetReplicationCount(ctx, destBasePath, event.DestinationProjectNumber, event.DestinationLocationID, "", dstToken, string(storageClass), string(serviceLevel))
		if err != nil {
			return nil, errors.NewVCPError(errors.ErrValidateCreateReplicationCvpInternalGetReplicationCount, err)
		}
		if replicationQuotaLimit <= destReplicationCount {
			return nil, errors.NewVCPError(errors.ErrReplicationQuotaLimitExceeded, errors.New("Quota limit 'ReplicatedVolumesPerRegion' has been exceeded."))
		}

		destVolumeQuotaLimit, err := getQuotaLimit(ctx, logger, event.DestinationLocationID, event.DestinationProjectNumber, callbackToken, common.ResourceTypeVolume)
		if err != nil {
			logger.Error("Get volume quota limit error", common.Error(err))
			return nil, errors.NewVCPError(errors.ErrGetVolumeQuotaLimitInternal, err)
		}
		destVolumeCount, err := internalGetVolumeCount(ctx, destBasePath, event.DestinationProjectNumber, event.DestinationLocationID, "", dstToken, string(storageClass), string(serviceLevel))
		if err != nil {
			return nil, errors.NewVCPError(errors.ErrValidateCreateReplicationCvpInternalGetVolumeCount, err)
		}
		if destVolumeQuotaLimit <= destVolumeCount {
			return nil, errors.NewVCPError(errors.ErrVolumeQuotaLimitExceeded, errors.New("Quota limit 'VolumesPerRegion' on destination region has been exceeded."))
		}
	}

	destShareName := event.CreateReplicationParams.DestinationVolumeParameters.ShareName
	if destShareName == "" && event.SourceVolume.VolumeAttributes.CreationToken != "" {
		destShareName = event.SourceVolume.VolumeAttributes.CreationToken
	}
	destVolumeID := event.CreateReplicationParams.DestinationVolumeParameters.VolumeID
	if destVolumeID == "" {
		destVolumeID = event.SourceVolume.Name
	}
	destVolume, err := getVolume(ctx, destBasePath, dstToken, event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, destVolumeID)
	if err != nil {
		if err.Error() != "volume not found" {
			logger.Error("getDestinationVolume error", common.Error(err))
			return nil, errors.NewVCPError(errors.ErrValidateGetVolumeReplicationCreation, err)
		}
	}
	if destVolume.ResourceId != "" {
		if destVolume.CreationToken.Value == destShareName {
			typeErr := errors.NewVCPError(errors.ErrGetVolumeCreateTokenInUseRemoteShareName, errors.New("RemoteShareName already Exists"))
			logger.Error("RemoteShareName already Exists", common.Error(typeErr))
			return nil, typeErr
		}

		typeErr := errors.NewVCPError(errors.ErrGetVolumeCreateTokenInUseRemoteResourceID, errors.New("RemoteResourceID already Exists"))
		logger.Error("RemoteResourceID already Exists", common.Error(typeErr))
		return nil, typeErr
	}

	expectedDbReplication, err := createReplicationObjects(event, event.DestinationLocationID, sourceRegion, destRegion)
	if err != nil {
		logger.Error("create dummy replication error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrValidateCreateDummyReplication, err)
	}

	logger.Debug("Finished validateCreateReplicationParams")

	return expectedDbReplication, nil
}

func isPoolHealthy(dstPool *googleproxyclient.PoolV1beta) bool {
	if dstPool.StoragePoolState.Value == googleproxyclient.PoolV1betaStoragePoolStateERROR || dstPool.StoragePoolState.Value == googleproxyclient.PoolV1betaStoragePoolStateDISABLED {
		return false
	}
	return true
}

func isPoolInTransitionState(dstPool *googleproxyclient.PoolV1beta) bool {
	if dstPool.StoragePoolState.Value == googleproxyclient.PoolV1betaStoragePoolStateDELETING || dstPool.StoragePoolState.Value == googleproxyclient.PoolV1betaStoragePoolStateCREATING || dstPool.StoragePoolState.Value == googleproxyclient.PoolV1betaStoragePoolStateUPDATING {
		return true
	}
	return false
}

func _getVolume(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeResourceId string) (googleproxyclient.VolumeV1beta, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, token, logger)
	params := googleproxyclient.V1betaListVolumesParams{}
	params.LocationId = locationID
	params.ProjectNumber = projectNumber
	params.XCorrelationID = googleproxyclient.OptString{Value: *xCorrelationID, Set: true}

	response, err := googleProxyClient.Invoker.V1betaListVolumes(ctx, params)
	if err != nil {
		return googleproxyclient.VolumeV1beta{}, errors.NewVCPError(errors.ErrListVolumes, err)
	}

	for _, vol := range response.(*googleproxyclient.V1betaListVolumesOK).Volumes {
		if volumeResourceId == vol.ResourceId {
			return vol, nil
		}
	}
	return googleproxyclient.VolumeV1beta{}, errors.New("volume not found")
}

func _internalGetReplicationCount(ctx context.Context, basePath string, projectNumber string, locationID string, poolID string, jwt string, storageClass, serviceLevel string) (int, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwt, logger)
	params := googleproxyclient.V1betaGetReplicationCountParams{}
	params.LocationId = locationID
	params.ProjectNumber = projectNumber
	params.PoolID = googleproxyclient.NewOptString(poolID)
	params.StorageClass = googleproxyclient.OptStorageClassQueryParameter{Value: googleproxyclient.StorageClassQueryParameterSoftware, Set: true}
	params.ServiceLevel = []googleproxyclient.ServiceLevelQueryParameterItem{googleproxyclient.ServiceLevelQueryParameterItemFlex}
	response, err := googleProxyClient.Invoker.V1betaGetReplicationCount(ctx, params)
	if err != nil {
		return 0, nil
	}
	return response.(*googleproxyclient.V1betaGetReplicationCountOK).ReplicationCount, nil
}

func _internalGetVolumeCount(ctx context.Context, basePath string, projectNumber string, locationID string, poolID string, jwt string, storageClass, serviceLevel string) (int, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwt, logger)
	params := googleproxyclient.V1betaGetVolumeCountParams{}
	params.LocationId = locationID
	params.ProjectNumber = projectNumber
	params.PoolID = googleproxyclient.NewOptString(poolID)
	params.StorageClass = googleproxyclient.NewOptStorageClassQueryParameter(googleproxyclient.StorageClassQueryParameterSoftware)
	params.ServiceLevel = []googleproxyclient.ServiceLevelQueryParameterItem{googleproxyclient.ServiceLevelQueryParameterItemFlex}
	response, err := googleProxyClient.Invoker.V1betaGetVolumeCount(ctx, params)
	if err != nil {
		return 0, nil
	}
	return response.(*googleproxyclient.V1betaGetVolumeCountOK).VolumeCount, nil
}

func _getDestinationPool(ctx context.Context, destBasePath string, token string, remoteLocationID string, projectNumber string, xCorrelationID *string, name string) (*googleproxyclient.PoolV1beta, error) {
	logger := util.GetLogger(ctx)

	logger.Debug(
		"cvp getDestinationPool",
		common.String("destBasePath", destBasePath),
		common.String("projectNumber", projectNumber),
		common.String("remoteLocationID", remoteLocationID),
		common.String("name", name),
	)

	googleProxyClient := googleproxyclient.GetGProxyClient(destBasePath, token, logger)
	params := googleproxyclient.V1betaListPoolsParams{}
	params.ProjectNumber = projectNumber
	params.LocationId = remoteLocationID
	params.XCorrelationID = googleproxyclient.OptString{Value: *xCorrelationID, Set: true}
	params.IncludeDeleted = googleproxyclient.OptBool{Value: false, Set: true}

	payloadError := &models.Error{Code: float64(404), Message: fmt.Sprintf("Error fetching pool - Pool %s not found", name)}

	poolsOk, err := googleProxyClient.Invoker.V1betaListPools(ctx, params)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrListPools, err)
	}

	poolsResponse := poolsOk.(*googleproxyclient.V1betaListPoolsOK)

	if poolsResponse != nil && len(poolsResponse.Pools) < 1 {
		return nil, errors.NewVCPError(errors.ErrGetPoolNotFound, &pools.V1betaListPoolsNotFound{Payload: payloadError})
	}

	for _, pool := range poolsResponse.Pools {
		if name == pool.ResourceId {
			return &pool, nil
		}
	}

	return nil, errors.NewVCPError(errors.ErrGetPoolNotFound, &pools.V1betaListPoolsNotFound{Payload: payloadError})
}

func _getReplicationJobs(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, poolId string) ([]googleproxyclient.InternalJobV1beta, error) {
	logger := util.GetLogger(ctx)

	logger.Debug(
		"cvp getReplicationJobs",
		common.String("destBasePath", basePath),
		common.String("projectNumber", projectNumber),
		common.String("locationID", locationID),
		common.String("poolId", poolId),
	)

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, token, logger)
	params := googleproxyclient.V1betaInternalGetReplicationJobsParams{}
	params.ProjectNumber = projectNumber
	params.LocationId = locationID
	params.PoolUUID = googleproxyclient.NewOptString(poolId)
	params.XCorrelationID = googleproxyclient.OptString{Value: *xCorrelationID, Set: true}

	getReplicationJobsResponse, err := googleProxyClient.Invoker.V1betaInternalGetReplicationJobs(ctx, params)
	if err != nil {
		return nil, err
	}

	jobs := getReplicationJobsResponse.(*googleproxyclient.V1betaInternalGetReplicationJobsOK).Jobs

	return jobs, nil
}

// Validates that account does not already have a replication with same resourceId
func _validateReplicationResourceId(ctx context.Context, projectNumber string, paramReplicationResourceId string, paramsVolumeResourceId string, se database.Storage) error {
	account, err := se.GetAccount(ctx, projectNumber)
	if err != nil {
		return err
	}
	replications, err := se.GetVolumeReplicationByProjectId(ctx, account.ID)
	if err != nil {
		return err
	}

	for _, replication := range replications {
		ccfeReplicationSplit := strings.Split(replication.Uri, "/")
		replicationResourceID := ccfeReplicationSplit[len(ccfeReplicationSplit)-1]
		volumeName := ccfeReplicationSplit[5]
		if replicationResourceID == paramReplicationResourceId && volumeName == paramsVolumeResourceId {
			return fmt.Errorf("replication resourceId already in use")
		}
	}

	return nil
}

// _createReplicationObjects return a dummy replication objects for expectedResponseCreateReplication endpoint to return
func _createReplicationObjects(event *CreateReplicationEvent, remotelocation, region, remoteRegion string) (*datamodel.VolumeReplication, error) {
	// projects/netapp-prod-prs-14/locations/northAmerica-northeast1/volumes/vol-1/replications/replication-1
	ccfeReplicationUri := fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s", event.SourceProjectNumber, event.LocationID, event.VolumeResourceID, *event.CreateReplicationParams.ResourceID)

	CcfeRemoteUri := fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s", event.DestinationProjectNumber, remotelocation, event.CreateReplicationParams.DestinationVolumeParameters.VolumeID, *event.CreateReplicationParams.ResourceID)

	sourceVolumeUUID, err := uuid.Parse(event.SourceVolume.UUID)
	if err != nil {
		return nil, err
	}

	expectedDbReplication := &datamodel.VolumeReplication{
		Uri:       ccfeReplicationUri,
		RemoteUri: CcfeRemoteUri,
		BaseModel: datamodel.BaseModel{
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Name:        *event.CreateReplicationParams.ResourceID,
		Description: nillable.GetString(event.CreateReplicationParams.Description, ""),
	}
	replicationAttributes := datamodel.ReplicationDetails{
		SourceVolumeUUID:    sourceVolumeUUID.String(),
		SourceVolumeName:    event.SourceVolume.Name,
		SourceLocation:      region,
		DestinationLocation: remoteRegion,
		EndpointType:        models.VolumeReplicationCVPV1betaEndpointTypeSrc,
		ReplicationSchedule: *event.CreateReplicationParams.ReplicationSchedule,
		SourcePoolUUID:      event.SourcePool.UUID,
	}
	expectedDbReplication.ReplicationAttributes = &replicationAttributes

	return expectedDbReplication, nil
}

// _validateLabels will loop through the label map and validate labels according to Google requirements
func _validateLabels(labels map[string]string) error {
	_, err := json.Marshal(labels)
	if err != nil {
		return errors.NewVCPError(errors.ErrLabelsMarshalFailure, fmt.Errorf("unable to marshal labels"))
	}

	if len(labels) > 64 {
		return errors.NewVCPError(errors.ErrLabelsCountExceeded, fmt.Errorf("invalid label count"))
	}

	for k, v := range labels {
		if len(k) == 0 {
			return errors.NewVCPError(errors.ErrLabelsKeyRequired, fmt.Errorf("key is required in label"))
		}
		if len(strings.Split(k, "")) > maxRuneCount {
			return errors.NewVCPError(errors.ErrLabelsKeyTooLongCharacters, fmt.Errorf("label key '%s' is too long (length can't exceed %d characters)", k, maxRuneCount))
		}
		if len(k) > maxByteCount {
			return errors.NewVCPError(errors.ErrLabelsKeyTooLongBytes, fmt.Errorf("label key '%s' is too long (encoded length can't exceed %d bytes)", k, maxByteCount))
		}
		if len(strings.Split(v, "")) > maxRuneCount {
			return errors.NewVCPError(errors.ErrLabelsValueTooLongCharacters, fmt.Errorf("label value '%s' is too long (length can't exceed %d characters)", v, maxRuneCount))
		}
		if len(v) > maxByteCount {
			return errors.NewVCPError(errors.ErrLabelsValueTooLongBytes, fmt.Errorf("label value '%s' is too long (encoded length can't exceed %d bytes)", v, maxByteCount))
		}
	}
	return nil
}

func _replicationJobInProcess(ctx context.Context, srcProjectNumber string, destProjectNumber string, srcBasePath string, destBasePath string, srcLocationID string, destLocationId, srcToken string, destToken string, ccfeUri string, remoteCcfeUri string, srcPoolId, dstPoolId string, correlationId *string) error {
	logger := util.GetLogger(ctx)
	if srcBasePath != "" {
		srcJobs, err := getReplicationJobs(ctx, srcBasePath, srcToken, srcLocationID, srcProjectNumber, correlationId, srcPoolId)
		if err != nil {
			logger.Error("ListCvpReplicationJobsInProcessing source error", common.Error(err))
			return err
		}
		if len(srcJobs) > 0 {
			for _, j := range srcJobs {
				if j.ResourceName.Value == ccfeUri || j.ResourceName.Value == remoteCcfeUri {
					return errors.NewVCPError(errors.ErrorCvpReplicationJobAlreadyInProcess, errors.New("Another operation against this replication is in progress. Please wait until the operation has finished and try again later."))
				}
			}
		}
	}

	if destBasePath != "" {
		destJobs, err := getReplicationJobs(ctx, destBasePath, destToken, destLocationId, destProjectNumber, correlationId, dstPoolId)
		if err != nil {
			logger.Error("ListCvpReplicationJobsInProcessing destination error", common.Error(err))
			return errors.NewVCPError(errors.ErrGetRemoteReplicationJobs, err)
		}
		if len(destJobs) > 0 {
			for _, j := range destJobs {
				// edge case during reverse resume when replicationEventBase.CCFEUri == ccfeUri while job is still in progress.
				if j.ResourceName.Value == remoteCcfeUri || j.ResourceName.Value == ccfeUri {
					return errors.NewVCPError(errors.ErrorCvpReplicationJobAlreadyInProcess, errors.New("Another operation against this replication is in progress. Please wait until the operation has finished and try again later."))
				}
			}
		}
	}
	return nil
}

// URI example: projects/458122799691/locations/us-central1/pools/pool-name
func _validateStoragePoolUri(uri string) error {
	uriList := strings.Split(uri, "/")
	if len(uriList) < 5 {
		return fmt.Errorf("storagePool should match %s", storageUriRegex)
	}

	reg, err := regexpCompile(storageUriRegex)
	if err != nil {
		return fmt.Errorf("storagePool should match %s", storageUriRegex)
	}

	valid := reg.MatchString(uri)
	if !valid {
		return fmt.Errorf("storagePool should match %s", storageUriRegex)
	}

	return nil
}

// GetCCFEURI returns the CCFE URI for a replication
func GetCCFEURI(projectNumber, location, volumeName, replicationName string) string {
	out := fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s", projectNumber, location, volumeName, replicationName)
	return out
}

func _validateReplicationParams(ctx context.Context, event *CommonReplicationEventParams, accountID int64, se database.Storage) error {
	logger := util.GetLogger(ctx)
	ccfeURI := internalUtilGetCCFEURI(event.AccountName, event.Location, event.VolumeResourceID, event.ReplicationResourceID)
	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", accountID),
		utils2.NewFilterCondition("uri", "=", ccfeURI))
	replicationDb, err := se.ListVolumeReplications(ctx, *filter)
	if err != nil {
		return errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
	}
	if len(replicationDb) == 0 {
		logger.Error("Replication not found in database", common.String("ccfeURI", ccfeURI))
		return utilErrors.NewUserInputValidationErr("No replication found for the given URI")
	}
	replication := replicationDb[0]

	remoteProject, err := utilsParseProjectNumberFromURI(replication.RemoteUri)
	if err != nil {
		logger.Error("Parse Remote URI Error", common.Error(err))
		return errors.NewVCPError(errors.ErrProjectParsingError, err)
	}

	event.SourceProjectNumber, event.DestinationProjectNumber = event.AccountName, remoteProject
	if replication.ReplicationAttributes.EndpointType == coreModels.DstEndpoint {
		event.SourceProjectNumber, event.DestinationProjectNumber = remoteProject, event.AccountName
	}

	srcToken, err := InternalUtilGetSignedToken(event.SourceProjectNumber)
	if err != nil {
		logger.Error("Get Signed Token Error", common.Error(err))
		return errors.NewVCPError(errors.ErrGetSignedToken, err)
	}

	dstToken := srcToken
	if event.DestinationProjectNumber != event.SourceProjectNumber {
		// if remoteProject is not the same as the projectNumber, we need to get a new token for the remote project
		dstToken, err = InternalUtilGetSignedToken(event.DestinationProjectNumber)
		if err != nil {
			logger.Error("Get Signed Token Error For Remote Project", common.Error(err))
			return errors.NewVCPError(errors.ErrGetSignedToken, err)
		}
	}

	srcBasePath, err := InternalUtilGetPairedRegionURI(replication.ReplicationAttributes.SourceLocation)
	if err != nil {
		logger.Error("Get Paired Source Region Uri error", common.Error(err))
		return errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}

	dstBasePath, err := InternalUtilGetPairedRegionURI(replication.ReplicationAttributes.DestinationLocation)
	if err != nil {
		logger.Error("Get Paired Destination Region Uri error", common.Error(err))
		return errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}

	// Check if replication job is in process
	err = replicationJobInProcess(ctx, event.SourceProjectNumber, event.DestinationProjectNumber, srcBasePath, dstBasePath, replication.ReplicationAttributes.SourceLocation, replication.ReplicationAttributes.DestinationLocation, srcToken, dstToken, replication.Uri, replication.RemoteUri, replication.ReplicationAttributes.SourcePoolUUID, replication.ReplicationAttributes.DestinationPoolUUID, event.XCorrelationID)
	if err != nil {
		return err
	}
	event.SrcBasePath = srcBasePath
	event.DstBasePath = dstBasePath
	event.SrcToken = srcToken
	event.DstToken = dstToken
	event.ReplicationModel = replication
	return nil
}

func _verifyDstReplicationResume(ctx context.Context, event *ResumeReplicationEvent) (*coreModels.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	dstReplication, err := getReplication(ctx, event.DstBasePath, event.DestinationProjectNumber, event.ReplicationModel.ReplicationAttributes.DestinationLocation, event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID, event.DstToken)
	if err != nil || dstReplication == nil {
		logger.Error("getReplication error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}

	if *dstReplication.MirrorState == models.ReplicationV1betaMirrorStateMIRRORED {
		return nil, utilErrors.NewUserInputValidationErr(fmt.Sprintf("Replication mirror state should be %s", models.ReplicationV1betaMirrorStateSTOPPED))
	}

	if *dstReplication.MirrorState == models.ReplicationV1betaMirrorStateUNINITIALIZED && *dstReplication.RelationshipStatus == coreModels.SnapmirrorRelationshipTransferring {
		return nil, utilErrors.NewUserInputValidationErr(fmt.Sprintf("Replication relationship status should be %s", models.VolumeReplicationCVPV1betaRelationshipStatusIdle))
	}

	return dstReplication, nil
}

func _verifyDstReplicationStop(ctx context.Context, event *StopReplicationEvent) (*coreModels.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	dstReplication, err := getReplication(ctx, event.DstBasePath, event.DestinationProjectNumber, event.ReplicationModel.ReplicationAttributes.DestinationLocation, event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID, event.DstToken)
	if err != nil || dstReplication == nil {
		logger.Error("getReplication error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}

	if *dstReplication.MirrorState == models.ReplicationV1betaMirrorStateSTOPPED || *dstReplication.MirrorState == models.ReplicationV1betaMirrorStateABORTED {
		return nil, utilErrors.NewUserInputValidationErr(fmt.Sprintf("Replication is already in %s state", *dstReplication.MirrorState))
	}

	if *dstReplication.MirrorState == models.ReplicationV1betaMirrorStateUNINITIALIZED {
		if *dstReplication.RelationshipStatus == strings.ToLower(models.ReplicationV1betaMirrorStateTRANSFERRING) && !event.ForceStop {
			return nil, utilErrors.NewUserInputValidationErr("Replication in preparing state. Please try again later")
		}
	}

	if *dstReplication.MirrorState == models.ReplicationV1betaMirrorStateMIRRORED && *dstReplication.RelationshipStatus == strings.ToLower(models.ReplicationV1betaMirrorStateTRANSFERRING) && !event.ForceStop {
		return nil, utilErrors.NewUserInputValidationErr(fmt.Sprintf("Replication relationship status is in %s state", strings.ToLower(models.ReplicationV1betaMirrorStateTRANSFERRING)))
	}

	return dstReplication, nil
}

func _validateReplicationUpdate(ctx context.Context, event *UpdateReplicationEvent) (*coreModels.VolumeReplication, error) {
	logger := util.GetLogger(ctx)

	if event.ReplicationSchedule == nil && event.Description == nil {
		logger.Error("empty replication update payload")
		return nil, errors.NewVCPError(errors.ErrorEmptyUpdateReplicationPayload, errors.New("empty replication update payload"))
	}

	if event.ReplicationSchedule != nil && *event.ReplicationSchedule == models.ReplicationV1betaReplicationScheduleREPLICATIONSCHEDULEUNSPECIFIED {
		logger.Error("replicationSchedule is UNSPECIFIED for update replication")
		return nil, errors.NewVCPError(errors.ErrorReplicationScheduleUnspecified, errors.New("Invalid replication schedule provided."))
	}

	dstReplication, err := getReplication(ctx, event.DstBasePath, event.DestinationProjectNumber, event.ReplicationModel.ReplicationAttributes.DestinationLocation, event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID, event.DstToken)
	if err != nil || dstReplication == nil {
		logger.Error("getReplication error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}

	return dstReplication, nil
}

func _verifyDstVolume(ctx context.Context, event *ResumeReplicationEvent, srcBasePath string, destBasePath string, srcToken string, dstToken string) (googleproxyclient.VolumeV1beta, googleproxyclient.VolumeV1beta, error) {
	logger := util.GetLogger(ctx)
	srcVolume, err := describeVolume(ctx, srcBasePath, srcToken, event.ReplicationModel.ReplicationAttributes.SourceLocation, event.SourceProjectNumber, event.XCorrelationID, event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID)
	if err != nil {
		if err.Error() != "volume not found" {
			logger.Error("getSourceVolume error", common.Error(err))
			return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, errors.NewVCPError(errors.ErrValidateGetVolumeReplicationCreation, err)
		}
		return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, errors.New("volume not found")
	}

	dstVolume, err := describeVolume(ctx, destBasePath, dstToken, event.ReplicationModel.ReplicationAttributes.DestinationLocation, event.DestinationProjectNumber, event.XCorrelationID, event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID)
	if err != nil {
		if err.Error() != "volume not found" {
			logger.Error("getDestinationVolume error", common.Error(err))
			return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, errors.NewVCPError(errors.ErrValidateGetVolumeReplicationCreation, err)
		}
		return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, errors.New("volume not found")
	}

	if (srcVolume.VolumeState.Set && srcVolume.VolumeState.Value == vsa.VolumeStateOffline) && (dstVolume.VolumeState.Set && dstVolume.VolumeState.Value == vsa.VolumeStateOffline) {
		return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, utilErrors.NewWithTrackingID("Volume is offline", utilErrors.VolumeInOfflineState)
	}

	var srcQuotaInBytes float64
	var dstQuotaInBytes float64
	var dstUsedBytes float64

	if srcVolume.QuotaInBytes.Set {
		srcQuotaInBytes = srcVolume.QuotaInBytes.Value
	}
	if dstVolume.QuotaInBytes.Set {
		dstQuotaInBytes = dstVolume.QuotaInBytes.Value
	}
	if dstVolume.UsedBytes.Set {
		dstUsedBytes = dstVolume.UsedBytes.Value
	}
	if srcQuotaInBytes != dstQuotaInBytes {
		if dstUsedBytes > srcQuotaInBytes {
			return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, utilErrors.NewBadRequestErr("Destination volume used size is greater than source volume available quota")
		}
	}
	return srcVolume, dstVolume, nil
}

func _describeVolume(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, token, logger)
	params := googleproxyclient.V1betaDescribeVolumeParams{}
	params.LocationId = locationID
	params.ProjectNumber = projectNumber
	params.XCorrelationID = googleproxyclient.OptString{Value: *xCorrelationID, Set: true}
	params.VolumeId = volumeId

	response, err := googleProxyClient.Invoker.V1betaDescribeVolume(ctx, params)
	if err != nil {
		return googleproxyclient.VolumeV1beta{}, errors.NewVCPError(errors.ErrListVolumes, err)
	}
	volumeResponse := response.(*googleproxyclient.VolumeV1beta)
	if volumeResponse == nil {
		return googleproxyclient.VolumeV1beta{}, errors.NewVCPError(errors.ErrVolumeNotFound, nil)
	}

	return *volumeResponse, nil
}

func _getReplication(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
	logger := util.GetLogger(ctx)

	logger.Debug(
		"get destination replication",
		common.String("basePath", basePath),
		common.String("projectNumber", projectNumber),
		common.String("locationID", locationID),
		common.String("volumeReplicationID", volumeReplicationID),
	)
	payloadError := &models.Error{Code: float64(404), Message: fmt.Sprintf("Error fetching replication - Replication %s not found", volumeReplicationID)}
	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwt, logger)
	params := &googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
		ProjectNumber: projectNumber,
		LocationId:    locationID,
	}
	body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{volumeReplicationID}}
	response, err := googleProxyClient.Invoker.V1betaGetMultipleReplicationsInternal(ctx, &body, *params)
	if err != nil {
		return nil, err
	}
	replicationResponse := response.(*googleproxyclient.V1betaGetMultipleReplicationsInternalOK)

	if replicationResponse != nil && len(replicationResponse.Replications) < 1 {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsNotFound, &replications.V1betaGetMultipleReplicationsNotFound{Payload: payloadError})
	}

	return convertReplicationResponseToModels(replicationResponse), nil
}

func convertReplicationResponseToModels(response *googleproxyclient.V1betaGetMultipleReplicationsInternalOK) *coreModels.VolumeReplication {
	var replication coreModels.VolumeReplication
	if response.Replications != nil && len(response.Replications) < 1 {
		return nil
	}
	var mirrorState, relationshipStatus string
	if response.Replications[0].MirrorState.Set {
		mirrorState = string(response.Replications[0].MirrorState.Value)
		replication.MirrorState = &mirrorState
	}
	if response.Replications[0].RelationshipStatus.Set {
		relationshipStatus = string(response.Replications[0].RelationshipStatus.Value)
		replication.RelationshipStatus = &relationshipStatus
	}
	replication.Name = response.Replications[0].Name.Value
	replication.UUID = response.Replications[0].VolumeReplicationUuid.Value
	replication.Description = response.Replications[0].Description.Value
	replication.ReplicationAttributes = &coreModels.ReplicationDetails{
		SourceVolumeUUID:      response.Replications[0].SourceVolumeUuid.Value,
		SourceVolumeName:      response.Replications[0].SourceVolumeName,
		DestinationVolumeUUID: response.Replications[0].DestinationVolumeUuid.Value,
		DestinationVolumeName: response.Replications[0].DestinationVolumeName,
		ReplicationSchedule:   string(response.Replications[0].ReplicationSchedule.Value),
		EndpointType:          string(response.Replications[0].EndpointType),
	}
	replication.TotalTransferBytes = response.Replications[0].TotalTransferBytes.Value
	replication.TotalTransferTimeSecs = response.Replications[0].TotalTransferTimeSecs.Value
	replication.LastTransferSize = response.Replications[0].LastTransferSize.Value
	replication.LastTransferError = response.Replications[0].LastTransferError.Value
	replication.LastTransferDuration = response.Replications[0].LastTransferDuration.Value
	replication.TotalProgress = response.Replications[0].TotalProgress.Value
	replication.LagTime = response.Replications[0].LagTime.Value
	if response.Replications[0].LastTransferEndTime.Set {
		replication.LastTransferEndTime = &response.Replications[0].LastTransferEndTime.Value
	}
	if response.Replications[0].ProgressLastUpdated.Set {
		replication.ProgressLastUpdated = &response.Replications[0].ProgressLastUpdated.Value
	}
	replication.CreatedAt = response.Replications[0].CreatedAt.Value
	return &replication
}

func _verifyDstReplication(ctx context.Context, event *DeleteReplicationEvent) (*coreModels.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	dstReplication, err := getReplication(ctx, event.DstBasePath, event.DestinationProjectNumber, event.ReplicationModel.ReplicationAttributes.DestinationLocation, event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID, event.DstToken)
	if err != nil || dstReplication == nil {
		logger.Error("getReplication error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}

	if *dstReplication.MirrorState == models.ReplicationV1betaMirrorStateMIRRORED {
		logger.Error("Replication in mirrored state", common.Error(err))
		return nil, utilErrors.NewUserInputValidationErr(fmt.Sprintf("Destination replication is in mirror_state: %v expected_mirror_state: %v", models.ReplicationV1betaMirrorStateMIRRORED, models.ReplicationV1betaMirrorStateSTOPPED))
	}

	// Check if replication is in valid state
	if *dstReplication.MirrorState != models.ReplicationV1betaMirrorStateSTOPPED && *dstReplication.MirrorState != models.ReplicationV1betaMirrorStateUNINITIALIZED {
		logger.Error("Replication should be in PREPARING or STOPPED state before deleting", common.Error(err))
		return nil, utilErrors.NewUserInputValidationErr(fmt.Sprintf("Expected mirror state: %v or %v", models.ReplicationV1betaMirrorStatePREPARING, models.ReplicationV1betaMirrorStateSTOPPED))
	}

	// Edge Case where mirrorState is uninitialized but data is being transferred and state is PENDING_SVM_PEERING.
	if *dstReplication.MirrorState == models.ReplicationV1betaMirrorStateUNINITIALIZED && *dstReplication.RelationshipStatus == coreModels.SnapmirrorRelationshipTransferring {
		logger.Error("Replication needs to be in stopped state", common.Error(err))
		return nil, utilErrors.NewUserInputValidationErr(fmt.Sprintf("Replication relationship status should be %s", models.VolumeReplicationCVPV1betaRelationshipStatusIdle))
	}

	return dstReplication, nil
}

func _verifyDstReplicationSync(ctx context.Context, event *ResumeReplicationEvent) (*coreModels.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	dstReplication, err := getReplication(ctx, event.DstBasePath, event.DestinationProjectNumber, event.ReplicationModel.ReplicationAttributes.DestinationLocation, event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID, event.DstToken)
	if err != nil || dstReplication == nil {
		logger.Error("getReplication error", common.Error(err))
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}
	if *dstReplication.MirrorState != models.ReplicationV1betaMirrorStateMIRRORED {
		return nil, utilErrors.NewUserInputValidationErr(fmt.Sprintf("Replication mirror state should be %s", models.ReplicationV1betaMirrorStateMIRRORED))
	}

	return dstReplication, nil
}
