package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/pools"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	regionalPoolEnabled                    = env.GetBool("REGIONAL_SUPPORT_ENABLED", false)
	minCustomThroughput                    = utils.MinCustomThroughput
	getAndSyncKmsConfigForPool             = _getAndSyncKmsConfigForPool
	enableLdap                             = env.GetBool("ENABLE_LDAP", false)
	blockUpdatePooltoATPool                = env.GetBool("BLOCK_UPDATE_POOL_TO_AT_POOL", true)
	enableMqos                             = env.GetBool("ENABLE_MQOS", false)
	enableVolumePerformanceGroupAssignment = env.GetBool("ENABLE_VOLUME_PERFORMANCE_GROUP_ASSIGNMENT", false)
)

const (
	HTTP_BAD_REQUEST_CODE = 400
	maxRuneCount          = 63
	maxByteCount          = 128
)

func resolvePerformanceParams(reqThroughput gcpgenserver.OptNilFloat64, reqIops gcpgenserver.OptNilFloat64) (throughput int64, iops *int64) {
	// Set default throughput if not provided
	if reqThroughput.IsSet() {
		throughput = int64(reqThroughput.Value)
	} else {
		throughput = int64(minCustomThroughput)
	}

	// Set IOPS based on throughput if not provided, otherwise use provided value
	if reqIops.IsSet() {
		value := int64(reqIops.Value)
		iops = &value
	} else {
		// Leave IOPS as nil - orchestrator will calculate from throughput if needed
		iops = nil
	}

	return throughput, iops
}

// V1betaDescribePool handles the request to describe a pool.
func (h Handler) V1betaDescribePool(ctx context.Context, params gcpgenserver.V1betaDescribePoolParams) (gcpgenserver.V1betaDescribePoolRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDescribePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	pool, err := h.Orchestrator.DescribePool(ctx, params.PoolId, params.ProjectNumber)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDescribePoolNotFound{
				Code:    404,
				Message: "Pool not found",
			}, nil
		}
		logger.Error("Failed to describe pool", "error", err.Error())
		return &gcpgenserver.V1betaDescribePoolInternalServerError{}, err
	}
	return convertToPoolV1BetaWithConsumption(pool), nil
}

// V1betaCreatePool handles the request to create a pool.
func (h Handler) V1betaCreatePool(ctx context.Context, req *gcpgenserver.PoolV1beta, params gcpgenserver.V1betaCreatePoolParams) (gcpgenserver.V1betaCreatePoolRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreatePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	isRegionalHA := zone == ""

	validateErr := validateCreatePoolParams(req, zone)
	if validateErr != nil {
		switch validateErr.Code {
		case http.StatusBadRequest:
			return &gcpgenserver.V1betaCreatePoolBadRequest{
				Code:    validateErr.Code,
				Message: validateErr.Message,
			}, nil
		default:
			return &gcpgenserver.V1betaCreatePoolInternalServerError{
				Code:    validateErr.Code,
				Message: validateErr.Message,
			}, nil
		}
	}

	vendorId := fmt.Sprintf("/projects/%v/locations/%v/pools/%s", params.ProjectNumber, params.LocationId, req.ResourceId)
	// Check if the pool already exists
	existingPool, err := h.Orchestrator.GetPoolByVendorID(ctx, vendorId, params.ProjectNumber)
	if err == nil {
		logger.Info("Pool already exists", "vendorId", vendorId)
		res, err2 := handleExistingPool(ctx, req, params, existingPool, h.Orchestrator)
		return res, err2
	} else if !errors.IsNotFoundErr(err) {
		logger.Error("Failed to check existing pool", "error", err.Error())
		return &gcpgenserver.V1betaCreatePoolInternalServerError{}, err
	}

	primaryZone := ""
	if !nillable.IsNilOrEmpty(&zone) {
		primaryZone = zone
	} else {
		primaryZone = req.Zone.Value
	}

	secondaryZone := ""
	if req.SecondaryZone.IsSet() {
		secondaryZone = req.SecondaryZone.Value
	}

	// Resolve performance parameters with defaults and auto-calculation
	totalThroughput, totalIops := resolvePerformanceParams(req.TotalThroughputMibps, req.TotalIops)

	hotTierSizeInBytes := uint64(req.SizeInBytes)
	if req.AllowAutoTiering.IsSet() && req.AllowAutoTiering.Value {
		hotTierSizeInBytes = uint64(req.HotTierSizeInBytes.Value)
	}
	createPoolParams := &commonparams.CreatePoolParams{
		AccountName:             params.ProjectNumber,
		Region:                  region,
		PrimaryZone:             primaryZone,
		SecondaryZone:           secondaryZone,
		IsRegionalHA:            isRegionalHA,
		Name:                    req.ResourceId,
		Description:             req.Description.Value,
		VendorID:                vendorId,
		VendorSubNetID:          req.Network,
		ServiceLevel:            string(req.ServiceLevel),
		SizeInBytes:             uint64(req.SizeInBytes),
		QosType:                 req.QosType.Value,
		AllowAutoTiering:        req.AllowAutoTiering.Value,
		HotTierSizeInBytes:      hotTierSizeInBytes,
		EnableHotTierAutoResize: req.EnableHotTierAutoResize.Value,
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{ThroughputMibps: totalThroughput, Enabled: req.CustomPerformanceEnabled.Value, Iops: totalIops},
		LargeCapacity:           req.LargeCapacity.Value,
	}

	if string(req.Mode.Value) == string(gcpgenserver.PoolV1betaModeMODEUNSPECIFIED) || string(req.Mode.Value) == string(gcpgenserver.PoolV1betaModeDEFAULT) {
		createPoolParams.Mode = commonparams.DEFAULTMode
	} else {
		createPoolParams.Mode = commonparams.ONTAPMode
	}

	// Set AD related params
	adConfig, adErrResp := getAndSyncAdConfigForPool(ctx, req, createPoolParams, h.Orchestrator)
	if adErrResp != nil {
		return adErrResp, nil
	}
	if adConfig != nil {
		createPoolParams.ActiveDirectoryId = adConfig.UUID
		createPoolParams.ActiveDirectory = adConfig
	}

	// Set LDAP enabled param
	if req.LdapEnabled.IsSet() {
		createPoolParams.LdapEnabled = req.LdapEnabled.Value
	} else {
		createPoolParams.LdapEnabled = false
	}

	// Set kms config related params if kms config is provided
	kmsConfig, errResp := getAndSyncKmsConfigForPool(ctx, req, createPoolParams, h.Orchestrator)
	if errResp != nil {
		return errResp, nil
	}
	if kmsConfig != nil {
		createPoolParams.KmsConfigId = kmsConfig.UUID
		createPoolParams.KmsConfigResourceID = kmsConfig.ResourceID
		createPoolParams.KmsConfig = kmsConfig
	}

	if req.Labels.IsSet() {
		jsonbLabels, err := validateLabels(req.Labels.Value)
		if err != nil {
			return &gcpgenserver.V1betaCreatePoolBadRequest{
				Code:    HTTP_BAD_REQUEST_CODE,
				Message: err.Error(),
			}, nil
		}
		createPoolParams.Labels = jsonbLabels
	}
	created, operationID, err := h.Orchestrator.CreatePool(ctx, createPoolParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaCreatePoolBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		return &gcpgenserver.V1betaCreatePoolInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, nil
	}
	resp, err := encodePoolV1(convertToPoolV1Beta(created))
	if err != nil {
		return nil, err
	}
	if operationID != "" {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{}, nil
}

func handleExistingPool(ctx context.Context, req *gcpgenserver.PoolV1beta, params gcpgenserver.V1betaCreatePoolParams, existingPool *models.Pool, orchestrator orchestrator.OrchestratorFactory) (gcpgenserver.V1betaCreatePoolRes, error) {
	logger := util.GetLogger(ctx)
	if existingPool.State != models.LifeCycleStateCreating {
		// Pool exists and is not in creating state, return 409 Conflict
		return &gcpgenserver.V1betaCreatePoolConflict{
			Code:    409,
			Message: fmt.Sprintf("Pool with resource_id '%s' already exists", req.ResourceId),
		}, nil
	} else {
		resp, err := encodePoolV1(convertToPoolV1Beta(existingPool))
		if err != nil {
			logger.Error("Failed to encode existing pool response", "error", err.Error())
			return &gcpgenserver.V1betaCreatePoolInternalServerError{}, err
		}
		// Pool is in creating state, find the existing job and return same operation
		poolCategory := models.GetPoolCategory(common.GetBoolOrDefault(req.LargeCapacity, false))
		jobType := string(models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationCreate, poolCategory))
		job, jobErr := orchestrator.GetJobByResourceUUID(ctx, existingPool.UUID, jobType)
		if jobErr != nil {
			logger.Error("Failed to find job for creating pool", "poolUUID", existingPool.UUID, "error", jobErr.Error())
			// Return the pool response even if job lookup fails
			return &gcpgenserver.OperationV1beta{
				Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, uuid.UUID{}.String())), // Dummy operation ID
				Response: resp,
				Done:     gcpgenserver.NewOptBool(true), // Mark as done since we can't track the job
			}, nil
		}
		operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, job.UUID)
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(operationID),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(job.State == models.JobsStateDONE || job.State == models.JobsStateERROR), // Done if job is in DONE or ERROR state
		}, nil
	}
}

// V1betaDeletePool handles the request to delete a pool.
func (h Handler) V1betaDeletePool(ctx context.Context, params gcpgenserver.V1betaDeletePoolParams) (gcpgenserver.V1betaDeletePoolRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeletePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	// Check if the pool exists
	existingPool, err := h.Orchestrator.DescribePool(ctx, params.PoolId, params.ProjectNumber)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDeletePoolNotFound{
				Code:    404,
				Message: "Pool not found",
			}, nil
		} else {
			logger.Error("Failed to check existing pool", "error", err.Error())
			return &gcpgenserver.V1betaDeletePoolInternalServerError{}, err
		}
	}

	dummyOperationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, uuid.UUID{}.String())
	if existingPool != nil {
		switch existingPool.State {
		case models.LifeCycleStateDeleting:
			log := util.GetLogger(ctx)
			poolCategory := models.GetPoolCategory(existingPool.LargeCapacity)
			job, jobErr := h.Orchestrator.GetJobByResourceUUID(ctx, existingPool.UUID, string(models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationDelete, poolCategory)))
			if jobErr != nil {
				log.Error("Failed to find job for deleting pool", "poolUUID", existingPool.UUID, "error", jobErr.Error())
				// Return the pool response even if job lookup fails
				return &gcpgenserver.OperationV1beta{
					Name: gcpgenserver.NewOptString(dummyOperationID), // Dummy operation ID
					Done: gcpgenserver.NewOptBool(true),               // Mark as done since we can't find the job
				}, nil
			}
			operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, job.UUID)
			return &gcpgenserver.OperationV1beta{
				Name: gcpgenserver.NewOptString(operationID),
				Done: gcpgenserver.NewOptBool(job.State == models.JobsStateDONE || job.State == models.JobsStateERROR), // Done if job is in DONE or ERROR state
			}, nil
		case models.LifeCycleStateCreating:
			if params.XCorrelationID.IsSet() && params.XCorrelationID.Value != "" {
				log := util.GetLogger(ctx)
				poolCategory := models.GetPoolCategory(existingPool.LargeCapacity)
				deleteJobType := string(models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationDelete, poolCategory))
				job, jobErr := h.Orchestrator.GetJobByResourceUUID(ctx, existingPool.UUID, deleteJobType)
				if jobErr == nil && job != nil {
					// Checking if correlation ID matches - return existing job for idempotency
					if job.CorrelationID == params.XCorrelationID.Value {
						log.Infof("Found existing delete job %s for pool %s in CREATING state with matching correlation ID %s (cleanup case), returning existing job UUID",
							job.UUID, existingPool.UUID, params.XCorrelationID.Value)
						operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, job.UUID)
						return &gcpgenserver.OperationV1beta{
							Name: gcpgenserver.NewOptString(operationID),
							Done: gcpgenserver.NewOptBool(job.State == models.JobsStateDONE || job.State == models.JobsStateERROR),
						}, nil
					}
				}
			}
		case models.LifeCycleStateUpdating:
			msg := "Error deleting pool - Pool is already transitioning between states"
			return &gcpgenserver.V1betaDeletePoolConflict{
				Code:    409,
				Message: msg,
			}, nil
		}
	}

	if existingPool != nil && existingPool.DeletedAt != nil {
		return &gcpgenserver.OperationV1beta{
			Name: gcpgenserver.NewOptString(dummyOperationID),
			Done: gcpgenserver.NewOptBool(true),
		}, nil
	}
	deletePoolParams := &commonparams.DeletePoolParams{
		AccountName: params.ProjectNumber,
		PoolID:      params.PoolId,
	}
	// Delete the pool
	deleted, operationID, err := h.Orchestrator.DeletePool(ctx, deletePoolParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "uuid", params.PoolId)
			return &gcpgenserver.OperationV1beta{
				Name: gcpgenserver.NewOptString(dummyOperationID),
				Done: gcpgenserver.NewOptBool(true),
			}, nil
		}
		if errors.IsBadRequestErr(err) {
			logger.Info("Pool has volume", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDeletePoolConflict{
				Code:    400,
				Message: "Pool has active volumes",
			}, nil
		}
		if errors.IsConflictErr(err) {
			logger.Info("Pool is in transition state", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDeletePoolConflict{
				Code:    409,
				Message: "Error deleting pool - Pool is already transitioning between states",
			}, nil
		}
		logger.Error("Failed to delete pool", "error", err.Error())
		return &gcpgenserver.V1betaDeletePoolInternalServerError{}, err
	}
	resp, err := encodePoolV1(convertToPoolV1Beta(deleted))
	if err != nil {
		return nil, err
	}
	if deleted.State == models.LifeCycleStateDeleting || deleted.State == models.LifeCycleStateCreating {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}

	logger.Info("Pool deleted successfully", "PoolID", params.PoolId)
	return &gcpgenserver.V1betaDeletePoolNoContent{}, nil
}

// V1betaGetMultiplePools handles the request to get multiple pools.
func (h Handler) V1betaGetMultiplePools(ctx context.Context, req *gcpgenserver.PoolIdListV1beta, params gcpgenserver.V1betaGetMultiplePoolsParams) (gcpgenserver.V1betaGetMultiplePoolsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	// Validate the location first
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaGetMultiplePoolsBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	if req.PoolUuids == nil {
		return &gcpgenserver.V1betaGetMultiplePoolsBadRequest{
			Code:    400,
			Message: "PoolUUIDs is required",
		}, nil
	}

	if len(req.PoolUuids) > 1000 {
		return &gcpgenserver.V1betaGetMultiplePoolsBadRequest{
			Code:    float64(400),
			Message: "poolUUIDs in body should have at most 1000 items",
		}, nil
	}

	// Query VCP first
	poolsModelVCP, err := h.Orchestrator.GetMultiplePools(ctx, params.ProjectNumber, req.PoolUuids)
	if err != nil {
		logger.Error("Failed to get multiple pools", "error", err.Error())
		return &gcpgenserver.V1betaGetMultiplePoolsInternalServerError{
			Code:    500,
			Message: "Internal server error while getting pools",
		}, nil
	}

	poolsVCP := make([]gcpgenserver.PoolV1beta, 0, len(req.PoolUuids))
	foundPoolUUIDs := make(map[string]struct{}, len(poolsModelVCP))
	for _, pool := range poolsModelVCP {
		response := convertToPoolV1BetaWithConsumption(pool)
		poolsVCP = append(poolsVCP, *response)
		foundPoolUUIDs[pool.UUID] = struct{}{}
	}

	// If all pools are found in VCP, just return them.
	if len(req.PoolUuids) == len(poolsVCP) {
		logger.Info("All pools found in VCP", "pools", poolsVCP)
		return &gcpgenserver.V1betaGetMultiplePoolsOK{
			Pools: poolsVCP,
		}, nil
	}

	// Only call CVP if CVP_HOST is set.
	// logger.Info("DEBUG: CVP_HOST value in handler", "cvpHost", cvpHost, "os.Getenv", os.Getenv("CVP_HOST"))
	if cvp.CVP_HOST == "" {
		logger.Info("CVP_HOST environment variable is not set, skipping CVP call", "foundPools", len(poolsVCP), "requestedPools", len(req.PoolUuids))
		return &gcpgenserver.V1betaGetMultiplePoolsOK{
			Pools: poolsVCP,
		}, nil
	}

	// Figure out which pools are missing and need to be fetched from CVP
	missingPoolUUIDs := helper.FindMissingUUIDs(req.PoolUuids, foundPoolUUIDs)

	// If no pools are missing (e.g. due to duplicates in request), we don't need to call CVP
	if len(missingPoolUUIDs) == 0 {
		return &gcpgenserver.V1betaGetMultiplePoolsOK{
			Pools: poolsVCP,
		}, nil
	}

	logger.Debug("Some pools not found in VCP, fetching from CVP", "missingPools", missingPoolUUIDs)
	return getMultiplePoolsFromCVP(ctx, missingPoolUUIDs, params, poolsVCP)
}

func getMultiplePoolsFromCVP(ctx context.Context, missingPoolUUIDs []string, params gcpgenserver.V1betaGetMultiplePoolsParams, vcpPools []gcpgenserver.PoolV1beta) (gcpgenserver.V1betaGetMultiplePoolsRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	getMultiplePoolsParams := &pools.V1betaGetMultiplePoolsParams{
		LocationID:    params.LocationId,
		ProjectNumber: params.ProjectNumber,
		Body: &cvpmodels.PoolIDListV1beta{
			PoolUUIDs: missingPoolUUIDs,
		},
	}
	resp, err := cvpClient.Pools.V1betaGetMultiplePools(getMultiplePoolsParams)
	if err != nil {
		switch e := err.(type) {
		case *pools.V1betaGetMultiplePoolsBadRequest:
			return &gcpgenserver.V1betaGetMultiplePoolsBadRequest{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *pools.V1betaGetMultiplePoolsUnauthorized:
			return &gcpgenserver.V1betaGetMultiplePoolsUnauthorized{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *pools.V1betaGetMultiplePoolsForbidden:
			return &gcpgenserver.V1betaGetMultiplePoolsForbidden{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *pools.V1betaGetMultiplePoolsNotFound:
			return &gcpgenserver.V1betaGetMultiplePoolsNotFound{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *pools.V1betaGetMultiplePoolsInternalServerError:
			return &gcpgenserver.V1betaGetMultiplePoolsInternalServerError{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		}
	}

	var cvpPools []gcpgenserver.PoolV1beta
	if resp != nil && resp.Payload != nil && resp.Payload.Pools != nil {
		cvpPools = append(cvpPools, convertToPoolsV1beta(resp.Payload.Pools)...)
	}

	// Combine VCP and CVP pools
	allPools := append(vcpPools, cvpPools...)
	return &gcpgenserver.V1betaGetMultiplePoolsOK{
		Pools: allPools,
	}, nil
}

// V1betaListPools handles the request to list pools.
func (h Handler) V1betaListPools(ctx context.Context, params gcpgenserver.V1betaListPoolsParams) (gcpgenserver.V1betaListPoolsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaListPoolsBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	includeDeleted := false
	if params.IncludeDeleted.IsSet() {
		includeDeleted = params.IncludeDeleted.Value
	}

	poolList, err := h.Orchestrator.ListPools(ctx, params.ProjectNumber, includeDeleted)
	if err != nil {
		return &gcpgenserver.V1betaListPoolsInternalServerError{}, err
	}

	logger.Info("Pools found", "pools", poolList)
	return &gcpgenserver.V1betaListPoolsOK{
		Pools: convertToPoolsV1Beta(poolList),
	}, nil
}

// V1betaUpdatePool handles the request to update a pool.
func (h Handler) V1betaUpdatePool(ctx context.Context, req *gcpgenserver.PoolUpdateV1beta, params gcpgenserver.V1betaUpdatePoolParams) (gcpgenserver.V1betaUpdatePoolRes, error) {
	logger := util.GetLogger(ctx)

	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	existingPool, err := h.Orchestrator.DescribePool(ctx, params.PoolId, params.ProjectNumber)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "uuid", params.PoolId)
			return &gcpgenserver.V1betaUpdatePoolNotFound{
				Code:    404,
				Message: "Pool not found",
			}, nil
		}
		logger.Error("Failed to describe pool", "error", err.Error())
		return &gcpgenserver.V1betaUpdatePoolInternalServerError{}, err
	}

	validateErr := validateUpdatePoolParams(req, existingPool)
	if validateErr != nil {
		return validateErr, nil
	}

	param := &commonparams.UpdatePoolParams{
		AccountName: params.ProjectNumber,
		Region:      region,
		CurrentZone: zone,
		PoolId:      params.PoolId,
	}

	if req.LargeCapacity.IsSet() {
		param.LargeCapacity = nillable.GetBoolPtr(req.LargeCapacity.Or(false))
	}

	// IOPS Handling: When only throughput is changed (IOPS not provided in request):
	// - If current IOPS > (new throughput * 16): Keep current IOPS unchanged
	// - If current IOPS < (new throughput * 16): Increase IOPS to minimum (throughput * 16)
	//
	// This ensures that customers only changing throughput don't lose their higher IOPS
	// while maintaining the minimum IOPS requirement for the new throughput level.
	// Always validate and calculate IOPS - handles all cases including validation
	calculatedIops := calculateIopsForUpdate(ctx, req.TotalThroughputMibps, req.TotalIops, existingPool)

	// Validate if user is updating throughput/qos (either TotalThroughputMibps or TotalIops is set)
	// This blocks QoS reductions below what's utilized by child volumes
	if existingPool.QosType == utils.QosTypeManual && (req.TotalThroughputMibps.IsSet() || req.TotalIops.IsSet()) {
		// Use requested throughput if set, otherwise use existing pool throughput
		var throughputToValidate float64
		if req.TotalThroughputMibps.IsSet() {
			throughputToValidate = req.TotalThroughputMibps.Value
		} else {
			if existingPool.CustomPerformanceParams != nil {
				throughputToValidate = float64(existingPool.CustomPerformanceParams.Throughput)
			} else {
				throughputToValidate = float64(existingPool.TotalThroughputMibps)
			}
		}

		validateErr2 := validateUpdateThroughputAndIopsAboveUtilized(
			ctx,
			throughputToValidate,
			float64(calculatedIops),
			existingPool)
		if validateErr2 != nil {
			return &gcpgenserver.V1betaUpdatePoolBadRequest{
				Code:    http.StatusBadRequest,
				Message: validateErr2.Error(),
			}, nil
		}
	}
	param.TotalIops = &calculatedIops

	if req.Description.IsSet() {
		param.Description = req.Description.Value
	} else {
		param.Description = existingPool.Description
	}

	if req.QosType.IsSet() {
		param.QosType = req.QosType.Value
	} else {
		param.QosType = existingPool.QosType
	}

	if req.SizeInBytes.IsSet() {
		param.SizeInBytes = uint64(req.SizeInBytes.Value)
	} else {
		param.SizeInBytes = existingPool.SizeInBytes
	}

	if req.TotalThroughputMibps.IsSet() {
		param.TotalThroughputMibps = int64(req.TotalThroughputMibps.Value)
	} else {
		param.TotalThroughputMibps = int64(existingPool.CustomPerformanceParams.Throughput)
	}

	if req.Labels.IsSet() {
		jsonbLabels, err := validateLabels(req.Labels.Value)
		if err != nil {
			return &gcpgenserver.V1betaUpdatePoolBadRequest{
				Code:    HTTP_BAD_REQUEST_CODE,
				Message: err.Error(),
			}, nil
		}
		param.Labels = jsonbLabels
	}

	// AutoTiering parameter handling
	if req.AllowAutoTiering.IsSet() {
		param.AllowAutoTiering = req.AllowAutoTiering.Value
	} else {
		param.AllowAutoTiering = existingPool.AllowAutoTiering
	}

	if req.HotTierSizeInBytes.IsSet() {
		param.HotTierSizeInBytes = uint64(req.HotTierSizeInBytes.Value)
	} else if existingPool.AutoTieringConfig != nil {
		param.HotTierSizeInBytes = uint64(existingPool.AutoTieringConfig.HotTierSizeInBytes)
	}

	if req.EnableHotTierAutoResize.IsSet() {
		param.EnableHotTierAutoResize = req.EnableHotTierAutoResize.Value
	} else if existingPool.AutoTieringConfig != nil {
		param.EnableHotTierAutoResize = existingPool.AutoTieringConfig.EnableHotTierAutoResize
	}

	if req.ActiveDirectoryConfigId.IsSet() {
		param.ActiveDirectoryConfigId = req.ActiveDirectoryConfigId.Value
	}

	updatedPool, operationID, err := h.Orchestrator.UpdatePool(ctx, param)
	if err != nil {
		logger.Error("Failed to update pool", "error", err.Error())
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaUpdatePoolBadRequest{
				Code:    HTTP_BAD_REQUEST_CODE,
				Message: err.Error(),
			}, nil
		}
		if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaUpdatePoolConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}

		return &gcpgenserver.V1betaUpdatePoolInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	resp, err := encodePoolV1(convertToPoolV1Beta(updatedPool))
	if err != nil {
		return nil, err
	}
	if operationID != "" {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{}, nil
}

func convertToPoolsV1Beta(pools []*models.Pool) []gcpgenserver.PoolV1beta {
	poolsV1Beta := make([]gcpgenserver.PoolV1beta, len(pools))
	for i, pool := range pools {
		poolsV1Beta[i] = *convertToPoolV1BetaWithConsumption(pool)
	}
	return poolsV1Beta
}

// getLdapEnabled safely extracts LdapEnabled from PoolAttributes
func getLdapEnabled(pool *models.Pool) bool {
	if pool.PoolAttributes != nil {
		return pool.PoolAttributes.LdapEnabled
	}
	return false
}

func convertToPoolV1Beta(pool *models.Pool) *gcpgenserver.PoolV1beta {
	var deletedAt gcpgenserver.OptNilDateTime
	if pool.DeletedAt != nil {
		deletedAt = gcpgenserver.NewOptNilDateTime(*pool.DeletedAt)
	}

	var throughputValue float64
	var iops int64
	customPerformanceEnabled := false
	if (pool.CustomPerformanceParams != nil) && (pool.CustomPerformanceParams.Enabled) {
		customPerformanceEnabled = pool.CustomPerformanceParams.Enabled
		throughputValue = pool.CustomPerformanceParams.Throughput
		iops = pool.CustomPerformanceParams.Iops
	} else {
		throughputValue = pool.TotalThroughputMibps
		iops = pool.TotalIops
	}

	labels := gcpgenserver.PoolV1betaLabels{}
	if pool.PoolAttributes.Labels != nil {
		for key, value := range pool.PoolAttributes.Labels {
			labels[key] = value
		}
	}
	secondaryZone := ""
	if pool.PoolAttributes.IsRegionalHA {
		secondaryZone = pool.PoolAttributes.SecondaryZone
	}

	poolV1beta := &gcpgenserver.PoolV1beta{
		PoolId:                   gcpgenserver.NewOptString(pool.UUID),
		CreatedAt:                gcpgenserver.NewOptDateTime(pool.CreatedAt),
		UpdatedAt:                gcpgenserver.NewOptDateTime(pool.UpdatedAt),
		DeletedAt:                deletedAt,
		ResourceId:               pool.Name,
		Description:              gcpgenserver.NewOptNilString(pool.Description),
		Network:                  pool.VendorSubNetID,
		SizeInBytes:              float64(pool.SizeInBytes),
		TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(throughputValue),
		AvailableThroughputMibps: gcpgenserver.NewOptNilFloat64(throughputValue - pool.UtilizedThroughputMibps),
		TotalIops:                gcpgenserver.NewOptNilFloat64(float64(iops)),
		AvailableIops:            gcpgenserver.NewOptNilFloat64(float64(iops) - float64(pool.UtilizedIops)),
		StoragePoolState:         gcpgenserver.NewOptPoolV1betaStoragePoolState(gcpgenserver.PoolV1betaStoragePoolState(pool.State)),
		StoragePoolStateDetails:  gcpgenserver.NewOptString(pool.StateDetails),
		ServiceLevel:             gcpgenserver.PoolV1betaServiceLevel(pool.ServiceLevel),
		QosType:                  gcpgenserver.NewOptNilString(pool.QosType),
		CustomPerformanceEnabled: gcpgenserver.NewOptBool(customPerformanceEnabled),
		// Unified Pool is set true & StorageClass is to software for VSA pools
		Type:             gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
		UnifiedPool:      gcpgenserver.NewOptBool(true),
		Unified:          gcpgenserver.NewOptBool(true),
		StorageClass:     gcpgenserver.NewOptStorageClassV1beta("SOFTWARE"),
		AllowAutoTiering: gcpgenserver.NewOptNilBool(pool.AllowAutoTiering),
		AllocatedBytes:   gcpgenserver.NewOptNilFloat64(pool.PoolAttributes.AllocatedBytes),
		NumberOfVolumes:  gcpgenserver.NewOptNilInt32(int32(pool.PoolAttributes.NumberOfVolumes)),
		Zone:             gcpgenserver.NewOptString(pool.PoolAttributes.PrimaryZone),
		SecondaryZone:    gcpgenserver.NewOptString(secondaryZone),
		Labels:           gcpgenserver.NewOptPoolV1betaLabels(labels),
		LargeCapacity:    gcpgenserver.NewOptBool(pool.LargeCapacity),
		SatisfiesPzs:     gcpgenserver.NewOptNilBool(pool.SatisfiesPzs),
		SatisfiesPzi:     gcpgenserver.NewOptNilBool(pool.SatisfiesPzi),
		Mode:             gcpgenserver.NewOptPoolV1betaMode(gcpgenserver.PoolV1betaMode(pool.APIAccessMode)),
		LdapEnabled:      gcpgenserver.NewOptNilBool(getLdapEnabled(pool)),
	}

	if pool.ActiveDirectoryConfigId != "" {
		region, _, err := utils.ParseRegionAndZone(pool.PoolAttributes.PrimaryZone)
		if err == nil {
			poolV1beta.ActiveDirectoryConfigId = gcpgenserver.NewOptNilString(pool.ActiveDirectoryConfigId)
			poolV1beta.ActiveDirectoryResourceId = gcpgenserver.NewOptString(fmt.Sprintf(
				"projects/%s/locations/%s/activeDirectories/%s", pool.AccountName, region, pool.ActiveDirectoryResourceId))
		}
	}

	kmsConfigId := ""
	if pool.KmsConfig != nil {
		poolV1beta.KmsConfigId = gcpgenserver.NewOptNilString(pool.KmsConfig.UUID)
		poolV1beta.KmsConfigResourceId = gcpgenserver.NewOptString(utils.ParsedKeyFullPathResource{ProjectID: pool.KmsConfig.KeyProjectID,
			KeyRing: pool.KmsConfig.KeyRing, Location: pool.KmsConfig.KeyRingLocation, CryptoKey: pool.KmsConfig.KeyName}.String())
		kmsConfigId = pool.KmsConfig.UUID
	}
	poolV1beta.EncryptionType = gcpgenserver.NewOptPoolV1betaEncryptionType(gcpgenserver.PoolV1betaEncryptionType(utils.GetEncryptionType(&kmsConfigId)))
	var assetLocationMetadata gcpgenserver.PoolV1betaAssetLocationMetadata
	if pool.AssetMetadata != nil {
		var assets []gcpgenserver.ChildAsset
		inChildAssets := pool.AssetMetadata.ChildAssets
		for _, asset := range inChildAssets {
			var childAsset gcpgenserver.ChildAsset
			childAsset.AssetType = gcpgenserver.NewOptString(asset.AssetType)
			childAsset.AssetNames = asset.AssetNames
			assets = append(assets, childAsset)
		}
		assetLocationMetadata = gcpgenserver.PoolV1betaAssetLocationMetadata{
			ChildAssets: gcpgenserver.OptNilChildAssetArray{Value: assets, Set: true},
		}
		poolV1beta.AssetLocationMetadata = gcpgenserver.NewOptNilPoolV1betaAssetLocationMetadata(assetLocationMetadata)
	}

	// Only include auto tiering fields if auto tiering is enabled
	if pool.AllowAutoTiering {
		poolV1beta.HotTierSizeInBytes = gcpgenserver.NewOptNilFloat64(getHotTierSizeInBytes(pool.AutoTieringConfig))
		poolV1beta.EnableHotTierAutoResize = gcpgenserver.NewOptNilBool(getEnableHotTierAutoResize(pool.AutoTieringConfig))
		poolV1beta.HotTierConsumption = getHotTierConsumptionOpt(pool.AutoTieringConfig)
		poolV1beta.ColdTierConsumption = getColdTierConsumptionOpt(pool.AutoTieringConfig)
	}

	return poolV1beta
}

// encodePoolV1 encodes a PoolV1 struct to JSON.
func encodePoolV1(pool *gcpgenserver.PoolV1beta) (jx.Raw, error) {
	data, err := json.Marshal(pool)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func convertToPoolsV1beta(pools []*cvpmodels.PoolV1beta) []gcpgenserver.PoolV1beta {
	poolsV1Beta := make([]gcpgenserver.PoolV1beta, len(pools))
	for i, pool := range pools {
		poolsV1Beta[i] = *convertToPoolV1beta(pool)
	}
	return poolsV1Beta
}

func convertToPoolV1beta(pool *cvpmodels.PoolV1beta) *gcpgenserver.PoolV1beta {
	var assetLocationMetadata gcpgenserver.PoolV1betaAssetLocationMetadata
	if pool.AssetLocationMetadata != nil {
		var assets []gcpgenserver.ChildAsset
		inChildAssets := pool.AssetLocationMetadata.ChildAssets
		for _, asset := range inChildAssets {
			var cvpAsset gcpgenserver.ChildAsset
			cvpAsset.AssetType = gcpgenserver.NewOptString(asset.AssetType)
			cvpAsset.AssetNames = asset.AssetNames
			assets = append(assets, cvpAsset)
		}
		assetLocationMetadata = gcpgenserver.PoolV1betaAssetLocationMetadata{
			ChildAssets: gcpgenserver.OptNilChildAssetArray{Value: assets},
		}
	}
	return &gcpgenserver.PoolV1beta{
		PoolId:                    gcpgenserver.NewOptString(pool.PoolID),
		CreatedAt:                 gcpgenserver.NewOptDateTime(time.Time(pool.CreatedAt)),
		UpdatedAt:                 gcpgenserver.NewOptDateTime(time.Time(pool.UpdatedAt)),
		DeletedAt:                 utils.SafeTime(pool.DeletedAt),
		ResourceId:                *pool.ResourceID,
		Network:                   *pool.Network,
		AllocatedBytes:            utils.SafeFloat64(pool.AllocatedBytes),
		SizeInBytes:               *pool.SizeInBytes,
		TotalThroughputMibps:      utils.SafeFloat64(pool.TotalThroughputMibps),
		AvailableThroughputMibps:  utils.SafeFloat64(pool.AvailableThroughputMibps),
		ServiceLevel:              gcpgenserver.PoolV1betaServiceLevel(*pool.ServiceLevel),
		TotalIops:                 utils.SafeFloat64(pool.TotalIops),
		CustomPerformanceEnabled:  gcpgenserver.NewOptBool(pool.CustomPerformanceEnabled),
		Zone:                      gcpgenserver.NewOptString(pool.Zone),
		StorageClass:              gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1beta(pool.StorageClass)),
		StoragePoolState:          gcpgenserver.NewOptPoolV1betaStoragePoolState(gcpgenserver.PoolV1betaStoragePoolState(pool.StoragePoolState)),
		NumberOfVolumes:           utils.SafeInt64ToInt32(pool.NumberOfVolumes),
		StoragePoolStateDetails:   gcpgenserver.NewOptString(pool.StateDetails),
		Description:               utils.SafeString(pool.Description),
		AllowAutoTiering:          utils.SafeBool(pool.AllowAutoTiering),
		HotTierSizeInBytes:        utils.SafeFloat64(pool.HotTierSizeInBytes),
		EnableHotTierAutoResize:   utils.SafeBool(pool.EnableHotTierAutoResize),
		KmsConfigId:               utils.SafeString(pool.KmsConfigID),
		KmsConfigResourceId:       gcpgenserver.NewOptString(pool.KmsConfigResourceID),
		ActiveDirectoryConfigId:   utils.SafeString(pool.ActiveDirectoryConfigID),
		ActiveDirectoryResourceId: gcpgenserver.NewOptString(pool.ActiveDirectoryResourceID),
		LdapEnabled:               utils.SafeBool(pool.LdapEnabled),
		EncryptionType:            gcpgenserver.NewOptPoolV1betaEncryptionType(gcpgenserver.PoolV1betaEncryptionType(pool.EncryptionType)),
		GlobalAccessAllowed:       utils.SafeBool(pool.GlobalAccessAllowed),
		Labels:                    gcpgenserver.NewOptPoolV1betaLabels(pool.Labels),
		SecondaryZone:             gcpgenserver.NewOptString(pool.SecondaryZone),
		QosType:                   utils.SafeString(pool.QosType),
		SatisfiesPzi:              utils.SafeBool(pool.SatisfiesPzi),
		SatisfiesPzs:              utils.SafeBool(pool.SatisfiesPzs),
		AssetLocationMetadata:     gcpgenserver.NewOptNilPoolV1betaAssetLocationMetadata(assetLocationMetadata),
		// Unified Pool is set false for SDE pools
		Type:                gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeFILE),
		UnifiedPool:         gcpgenserver.NewOptBool(false),
		Unified:             gcpgenserver.NewOptBool(false),
		HotTierConsumption:  utils.SafeInt64(pool.HotTierConsumption),
		ColdTierConsumption: utils.SafeInt64(pool.ColdTierConsumption),
	}
}

// Helper functions for AutoTieringConfig field access
func getHotTierSizeInBytes(config *models.AutoTieringConfig) float64 {
	if config == nil {
		return 0
	}
	return float64(config.HotTierSizeInBytes)
}

func getEnableHotTierAutoResize(config *models.AutoTieringConfig) bool {
	if config == nil {
		return false
	}
	return config.EnableHotTierAutoResize
}

func convertToPoolV1BetaWithConsumption(pool *models.Pool) *gcpgenserver.PoolV1beta {
	result := convertToPoolV1Beta(pool)
	// Add consumption fields only if auto tiering is enabled
	if pool.AllowAutoTiering {
		result.HotTierConsumption = getHotTierConsumptionOpt(pool.AutoTieringConfig)
		result.ColdTierConsumption = getColdTierConsumptionOpt(pool.AutoTieringConfig)
	}
	return result
}

func getHotTierConsumptionOpt(config *models.AutoTieringConfig) gcpgenserver.OptNilInt64 {
	if config == nil {
		return gcpgenserver.OptNilInt64{}
	}
	return gcpgenserver.NewOptNilInt64(config.HotTierConsumption)
}

func getColdTierConsumptionOpt(config *models.AutoTieringConfig) gcpgenserver.OptNilInt64 {
	if config == nil {
		return gcpgenserver.OptNilInt64{}
	}
	return gcpgenserver.NewOptNilInt64(config.ColdTierConsumption)
}

// validateCreatePoolParams validates the parameters for creating a pool.
// It ensures that the provided parameters meet the requirements for a Unified Flex Storage Pool.
func validateCreatePoolParams(req *gcpgenserver.PoolV1beta, zone string) *gcpgenserver.Error {
	// Check the new Type field first, then fall back to unified/unifiedPool fields for backward compatibility
	isUnified := false

	// Check the new Type field
	if req.Type.IsSet() {
		switch req.Type.Value {
		case gcpgenserver.PoolV1betaTypeUNIFIED:
			isUnified = true
		case gcpgenserver.PoolV1betaTypeFILE:
			isUnified = false
		case gcpgenserver.PoolV1betaTypeSTORAGEPOOLTYPEUNSPECIFIED:
			// Default value, should not be used
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "type field cannot be STORAGE_POOL_TYPE_UNSPECIFIED",
			}
		}
	} else {
		// Fall back to legacy fields for backward compatibility
		if req.Unified.IsSet() {
			isUnified = req.Unified.Value
		} else if req.UnifiedPool.IsSet() {
			isUnified = req.UnifiedPool.Value
		}
	}

	if !isUnified {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "type must be set to UNIFIED, or unified/unifiedPool must be set to true (for backward compatibility)",
		}
	}

	if req.Mode.Value == gcpgenserver.PoolV1betaModeONTAP && req.ActiveDirectoryResourceId.Value != "" {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Active directory cannot be assigned to ONTAP Mode Pool",
		}
	}

	if req.QosType.IsSet() && req.QosType.Value == utils.QosTypeManual {
		if !enableMqos {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Manual QosType is not supported",
			}
		}
		if req.Mode.Value == gcpgenserver.PoolV1betaModeONTAP {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Manual QosType cannot be assigned to ONTAP Mode Pool",
			}
		}
	}

	if req.LdapEnabled.IsSet() && req.LdapEnabled.Value && req.ActiveDirectoryConfigId.Value == "" {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Active Directory configuration is required when LDAP is enabled",
		}
	}

	if req.LdapEnabled.IsSet() && req.LdapEnabled.Value && !enableLdap {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "LDAP is not currently supported for Unified Flex Storage Pool",
		}
	}

	if nillable.IsNilOrEmpty(&zone) {
		if !regionalPoolEnabled {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Regional Pool Support is not enabled",
			}
		}

		if !req.Zone.IsSet() || req.Zone.Value == "" {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Zone cannot be empty for regional pool.",
			}
		}

		if !req.SecondaryZone.IsSet() || req.SecondaryZone.Value == "" {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Secondary Zone cannot be empty for regional pool.",
			}
		}
		if req.SecondaryZone.Value == req.Zone.Value {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Secondary Zone cannot be same as Primary Zone",
			}
		}
	} else {
		if req.Zone.IsSet() && req.Zone.Value != "" && req.Zone.Value != zone {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Multiple Zone values cannot be passed for Zonal Pool Creation",
			}
		}
		if req.SecondaryZone.IsSet() && req.SecondaryZone.Value != "" && req.SecondaryZone.Value == zone {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Secondary Zone cannot be same as Primary Zone",
			}
		}
	}

	// Validate auto-tiering parameters
	if !autoTieringEnabled && ((req.AllowAutoTiering.IsSet() && req.AllowAutoTiering.Value) || (req.HotTierSizeInBytes.IsSet() && req.HotTierSizeInBytes.Value > 0)) {
		return &gcpgenserver.Error{
			Code:    HTTP_BAD_REQUEST_CODE,
			Message: "Auto-Tiering feature is currently not enabled.",
		}
	}

	if req.AllowAutoTiering.IsSet() && req.AllowAutoTiering.Value {
		// 1. HotTierSizeInBytes is required when auto-tiering is enabled (existence check only)
		if !req.HotTierSizeInBytes.IsSet() || req.HotTierSizeInBytes.Value == 0 {
			return &gcpgenserver.Error{
				Code:    HTTP_BAD_REQUEST_CODE,
				Message: "HotTierSizeInBytes is a required field to enable auto-tiering",
			}
		}
		// Note: All numerical validations (size comparisons, min/max checks) moved to orchestrator
	}

	// Auto-tiering params cannot be set without enabling AllowAutoTiering
	allowAutoTieringValue := false
	if req.AllowAutoTiering.IsSet() {
		allowAutoTieringValue = req.AllowAutoTiering.Value
	}
	if !allowAutoTieringValue && ((req.HotTierSizeInBytes.IsSet() && req.HotTierSizeInBytes.Value > 0) || (req.EnableHotTierAutoResize.IsSet() && req.EnableHotTierAutoResize.Value)) {
		return &gcpgenserver.Error{
			Code:    HTTP_BAD_REQUEST_CODE,
			Message: "HotTierSizeInBytes and EnableHotTierAutoResize cannot be set without enabling AllowAutoTiering",
		}
	}
	return nil
}

// validateUpdatePoolParams validates the parameters for updating a pool.
// We currently only allow updating the description, size, total throughput, and total IOPS.
func validateUpdatePoolParams(req *gcpgenserver.PoolUpdateV1beta, existingPool *models.Pool) gcpgenserver.V1betaUpdatePoolRes {
	if existingPool.State == models.LifeCycleStateUpdating {
		return &gcpgenserver.V1betaUpdatePoolConflict{
			Code:    http.StatusConflict,
			Message: "An update operation is already in progress for this pool",
		}
	}

	if existingPool.State == models.LifeCycleStateDegraded {
		return &gcpgenserver.V1betaUpdatePoolConflict{
			Code:    http.StatusConflict,
			Message: "Update operation is not allowed when the pool is in degraded state",
		}
	}

	if req.Zone.IsSet() && req.Zone.Value != existingPool.PoolAttributes.PrimaryZone {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    http.StatusBadRequest,
			Message: "Migrating to a different Zone is currently not supported",
		}
	}

	if req.GlobalAccessAllowed.IsSet() {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    http.StatusBadRequest,
			Message: "Updating Global access is currently not supported",
		}
	}

	// Feature flag validation
	if !autoTieringEnabled && (req.AllowAutoTiering.IsSet() ||
		(req.HotTierSizeInBytes.IsSet() && req.HotTierSizeInBytes.Value > 0) ||
		(req.EnableHotTierAutoResize.IsSet())) {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    http.StatusBadRequest,
			Message: "Auto-Tiering feature is currently not enabled",
		}
	}

	// HotTierSizeInBytes is required when enabling auto-tiering
	if req.AllowAutoTiering.IsSet() && req.AllowAutoTiering.Value {
		// Validate enabling Auto-Tiering env variable if blockUpdatePooltoATPool is true
		if !existingPool.AllowAutoTiering && blockUpdatePooltoATPool {
			return &gcpgenserver.V1betaUpdatePoolBadRequest{
				Code:    http.StatusBadRequest,
				Message: "Enabling Auto-Tiering on a non-AT pool is not supported currently",
			}
		}
		if !req.HotTierSizeInBytes.IsSet() || req.HotTierSizeInBytes.Value == 0 {
			return &gcpgenserver.V1betaUpdatePoolBadRequest{
				Code:    http.StatusBadRequest,
				Message: "HotTierSizeInBytes is required when enabling auto-tiering",
			}
		}
	}

	// AutoTiering parameter validation - HotTierSizeInBytes and EnableHotTierAutoResize cannot be set without enabling AllowAutoTiering
	// However, if the pool already has auto-tiering enabled, these parameters can be updated directly
	allowAutoTieringValue := false
	if req.AllowAutoTiering.IsSet() {
		allowAutoTieringValue = req.AllowAutoTiering.Value
	}

	// Check if pool already has auto-tiering enabled
	poolHasAutoTiering := existingPool.AllowAutoTiering

	// Only validate if auto-tiering is not already enabled on the pool
	if !poolHasAutoTiering && !allowAutoTieringValue && ((req.HotTierSizeInBytes.IsSet() && req.HotTierSizeInBytes.Value > 0) || req.EnableHotTierAutoResize.IsSet()) {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    http.StatusBadRequest,
			Message: "HotTierSizeInBytes and EnableHotTierAutoResize cannot be set without enabling AllowAutoTiering",
		}
	}

	// We do not allow pool size to be reduced.
	if req.SizeInBytes.IsSet() && req.SizeInBytes.Value < float64(existingPool.SizeInBytes) {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    http.StatusBadRequest,
			Message: "Pool size cannot be reduced",
		}
	}

	if req.QosType.IsSet() && req.QosType.Value != existingPool.QosType {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    http.StatusBadRequest,
			Message: "Updating QosType is currently not supported",
		}
	}

	if req.CustomPerformanceEnabled.IsSet() {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    http.StatusBadRequest,
			Message: "Updating CustomerPerformance is currently not supported",
		}
	}

	return nil
}

// calculateIopsForUpdate calculates IOPS for pool updates
// It ensures IOPS meets minimum requirements for both new and existing throughput
func calculateIopsForUpdate(ctx context.Context, throughput gcpgenserver.OptNilFloat64, iops gcpgenserver.OptNilFloat64, existingPool *models.Pool) int64 {
	// Case 1: IOPS explicitly provided - validate against throughput requirements
	if iops.IsSet() {
		// Return as it is since the calculation is done in the orchestrator
		return int64(iops.Value)
	} else if throughput.IsSet() {
		// Case 2: Only throughput is provided - smart IOPS calculation
		var currentIops int64
		if existingPool.CustomPerformanceParams != nil {
			currentIops = existingPool.CustomPerformanceParams.Iops
		} else {
			currentIops = existingPool.TotalIops
		}
		newThroughput := int64(throughput.Value)
		minimumIopsInt := newThroughput * 16

		logger := util.GetLogger(ctx)
		logger.Info("IOPS calculation",
			"newThroughput", newThroughput,
			"currentIops", currentIops,
			"minimumIops", minimumIopsInt)

		if currentIops > minimumIopsInt {
			// Current IOPS is already above minimum, keep it as is
			logger.Info("Keeping current IOPS (above minimum)", "finalIops", currentIops)
			return currentIops
		} else {
			// Current IOPS is below minimum, increase to minimum
			logger.Info("Increasing IOPS to minimum", "finalIops", minimumIopsInt)
			return minimumIopsInt
		}
	} else {
		// Case 3: Neither throughput nor IOPS provided - use existing IOPS
		if existingPool.CustomPerformanceParams != nil {
			return existingPool.CustomPerformanceParams.Iops
		}
		return existingPool.TotalIops
	}
}

// validateUpdateThroughputAndIopsAboveUtilized validates that the requested throughput and IOPS are at least the existing pool's usage (utilized by volumes in the pool)
func validateUpdateThroughputAndIopsAboveUtilized(ctx context.Context, throughput float64, iops float64, existingPool *models.Pool) error {
	if throughput < existingPool.UtilizedThroughputMibps {
		return errors.NewUserInputValidationErr(fmt.Sprintf(
			"Requested throughput (%.0f MiBps) must be >= current pool utilization (%.0f MiBps).",
			throughput, existingPool.UtilizedThroughputMibps,
		))
	}
	if iops < float64(existingPool.UtilizedIops) {
		return errors.NewUserInputValidationErr(fmt.Sprintf(
			"Requested IOPS (%.0f) must be >= current pool utilization (%.0f IOPS).",
			iops, float64(existingPool.UtilizedIops),
		))
	}
	return nil
}

// validateLabels will loop through the label map and validate labels according to Google requirements
func validateLabels(labels map[string]string) (*datamodel.JSONB, error) {
	_, err := json.Marshal(labels)
	if err != nil {
		return nil, errors.NewUserInputValidationErr("unable to marshal labels")
	}

	if len(labels) > 64 {
		return nil, errors.NewUserInputValidationErr("invalid label count")
	}

	jsonbLabels := make(datamodel.JSONB)
	for k, v := range labels {
		if len(k) == 0 {
			return nil, errors.NewUserInputValidationErr("key is required in label")
		}
		if len(strings.Split(k, "")) > maxRuneCount {
			return nil, errors.NewUserInputValidationErr(fmt.Sprintf("label key '%s' is too long (length can't exceed %d characters)", k, maxRuneCount))
		}
		if len(k) > maxByteCount {
			return nil, errors.NewUserInputValidationErr(fmt.Sprintf("label key '%s' is too long (encoded length can't exceed %d bytes)", k, maxByteCount))
		}
		if len(strings.Split(v, "")) > maxRuneCount {
			return nil, errors.NewUserInputValidationErr(fmt.Sprintf("label value '%s' is too long (length can't exceed %d characters)", v, maxRuneCount))
		}
		if len(v) > maxByteCount {
			return nil, errors.NewUserInputValidationErr(fmt.Sprintf("label value '%s' is too long (encoded length can't exceed %d bytes)", v, maxByteCount))
		}
		jsonbLabels[k] = v
	}
	return &jsonbLabels, nil
}

func _getAndSyncKmsConfigForPool(ctx context.Context, req *gcpgenserver.PoolV1beta, params *commonparams.CreatePoolParams, orchestratorInterface orchestrator.OrchestratorFactory) (*models.KmsConfig, gcpgenserver.V1betaCreatePoolRes) {
	if req.KmsConfigId.Value == "" {
		return nil, nil
	}
	getKmsConfigParams := &commonparams.GetKmsConfigParams{
		UUID:          req.KmsConfigId.Value,
		AccountName:   params.AccountName,
		LocationID:    params.Region,
		ProjectNumber: params.AccountName,
	}
	kmsConfig, err := orchestratorInterface.GetKmsConfig(ctx, getKmsConfigParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			// try to find the kms config in SDE, it's possible that the user created the KMS config in SDE
			sdeKmsConfig, err := orchestratorInterface.GetSDEKmsConfiguration(ctx, getKmsConfigParams)
			if err != nil {
				if errors.IsNotFoundErr(err) {
					return nil, &gcpgenserver.V1betaCreatePoolBadRequest{
						Code:    http.StatusBadRequest,
						Message: fmt.Sprintf("KMS Config with ID %s not found", req.KmsConfigId.Value),
					}
				}
				return nil, &gcpgenserver.V1betaCreatePoolInternalServerError{
					Code:    http.StatusInternalServerError,
					Message: err.Error(),
				}
			}
			// create and sync the KMS configuration with the SDE KMS configuration in VCP
			createKmsConfigParams := kms_activities.ConvertToCreateKmsConfigParams(sdeKmsConfig, params)
			kmsConfig, err := orchestratorInterface.CreateAndSyncKmsConfig(ctx, createKmsConfigParams)
			if err != nil {
				return nil, &gcpgenserver.V1betaCreatePoolInternalServerError{
					Code:    http.StatusInternalServerError,
					Message: err.Error(),
				}
			}
			return kmsConfig, nil
		}
		return nil, &gcpgenserver.V1betaCreatePoolInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}
	}
	return kmsConfig, nil
}

func getAndSyncAdConfigForPool(ctx context.Context, req *gcpgenserver.PoolV1beta, params *commonparams.CreatePoolParams, orchestrator orchestrator.OrchestratorFactory) (*models.ActiveDirectory, gcpgenserver.V1betaCreatePoolRes) {
	log := util.GetLogger(ctx)
	if req.ActiveDirectoryConfigId.Value == "" {
		return nil, nil
	}

	getADParams := &commonparams.GetADParams{
		UUID:          req.ActiveDirectoryConfigId.Value,
		AccountName:   params.AccountName,
		LocationID:    params.Region,
		ProjectNumber: params.AccountName,
	}
	adConfig, err := orchestrator.GetADConfig(ctx, getADParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			log.Debugf("Active Directory config with ID %s not found in VCP, trying SDE", req.ActiveDirectoryConfigId.Value)

			// ToDo: implement SDE AD config fetch and sync logic here

			return nil, &gcpgenserver.V1betaCreatePoolBadRequest{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("Active Directory Config with ID %s not found", req.ActiveDirectoryConfigId.Value),
			}
		}
		return nil, &gcpgenserver.V1betaCreatePoolInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}
	}
	return adConfig, nil
}
