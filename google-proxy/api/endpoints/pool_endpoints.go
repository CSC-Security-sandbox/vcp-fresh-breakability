package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/pools"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	regionalPoolEnabled = env.GetBool("REGIONAL_SUPPORT_ENABLED", false)
)

const (
	HTTP_BAD_REQUEST_CODE = 400
	DEFAULT_IOPS          = 1024
	maxRuneCount          = 63
	maxByteCount          = 128
)

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
	return convertToPoolV1Beta(pool), nil
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
		resp, err := encodePoolV1(convertToPoolV1Beta(existingPool))
		if err != nil {
			return nil, err
		}
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.OptString{Value: "operation-id"},
			Response: resp,
		}, nil
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

	totalIops := 0
	if !req.TotalIops.IsSet() {
		totalIops = DEFAULT_IOPS // Default to 1024 IOPS if not provided
	} else {
		totalIops = int(req.TotalIops.Value)
	}

	param := &common.CreatePoolParams{
		AccountName:             params.ProjectNumber,
		Region:                  region,
		PrimaryZone:             primaryZone,
		SecondaryZone:           secondaryZone,
		Name:                    req.ResourceId,
		Description:             req.Description.Value,
		VendorID:                vendorId,
		VendorSubNetID:          req.Network,
		ServiceLevel:            string(req.ServiceLevel),
		SizeInBytes:             uint64(req.SizeInBytes),
		QosType:                 req.QosType.Value,
		AllowAutoTiering:        req.AllowAutoTiering.Value,
		HotTierSizeInBytes:      uint64(req.HotTierSizeInBytes.Value),
		EnableHotTierAutoResize: req.EnableHotTierAutoResize.Value,
		CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: int64(req.TotalThroughputMibps.Value), Enabled: req.CustomPerformanceEnabled.Value, Iops: int64(totalIops)},
		KmsConfigId:             req.KmsConfigId.Value,
		KmsConfigResourceID:     req.KmsConfigResourceId.Value,
	}
	if req.Labels.IsSet() {
		jsonbLabels, err := validateLabels(req.Labels.Value)
		if err != nil {
			return &gcpgenserver.V1betaCreatePoolBadRequest{
				Code:    HTTP_BAD_REQUEST_CODE,
				Message: err.Error(),
			}, nil
		}
		param.Labels = jsonbLabels
	}
	created, operationID, err := h.Orchestrator.CreatePool(ctx, param)
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
	deletePoolParams := &common.DeletePoolParams{
		AccountName: params.ProjectNumber,
		PoolID:      existingPool.UUID,
	}
	// Delete the pool
	deleted, operationID, err := h.Orchestrator.DeletePool(ctx, deletePoolParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDeletePoolNotFound{
				Code:    404,
				Message: "Pool not found",
			}, nil
		}
		if errors.IsConflictErr(err) {
			logger.Info("Pool has volume", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDeletePoolConflict{
				Code:    409,
				Message: "Pool has active volumes",
			}, nil
		}
		logger.Error("Failed to delete pool", "error", err.Error())
		return &gcpgenserver.V1betaDeletePoolInternalServerError{}, err
	}
	resp, err := encodePoolV1(convertToPoolV1Beta(deleted))
	if err != nil {
		return nil, err
	}
	if deleted.State == models.LifeCycleStateDeleting {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}

	logger.Info("Pool deleted successfully", "PoolID", existingPool.UUID)
	return &gcpgenserver.V1betaDeletePoolNoContent{}, nil
}

// V1betaGetMultiplePools handles the request to get multiple pools.
func (h Handler) V1betaGetMultiplePools(ctx context.Context, req *gcpgenserver.PoolIdListV1beta, params gcpgenserver.V1betaGetMultiplePoolsParams) (gcpgenserver.V1betaGetMultiplePoolsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	getMultiplePoolsParams := &pools.V1betaGetMultiplePoolsParams{
		LocationID:    params.LocationId,
		ProjectNumber: params.ProjectNumber,
		Body: &cvpmodels.PoolIDListV1beta{
			PoolUUIDs: req.PoolUuids,
		},
	}
	resp, err := cvpClient.Pools.V1betaGetMultiplePools(getMultiplePoolsParams)
	if err != nil {
		switch e := err.(type) {
		case *pools.V1betaGetMultiplePoolsBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultiplePoolsBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *pools.V1betaGetMultiplePoolsUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultiplePoolsUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *pools.V1betaGetMultiplePoolsForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultiplePoolsForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *pools.V1betaGetMultiplePoolsNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultiplePoolsNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *pools.V1betaGetMultiplePoolsInternalServerError:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultiplePoolsInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}

	var cvpPools []gcpgenserver.PoolV1beta
	if resp != nil && resp.Payload != nil && resp.Payload.Pools != nil {
		cvpPools = append(cvpPools, convertToPoolsV1beta(resp.Payload.Pools)...)
	}

	// Validate the location
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

	poolList, err := h.Orchestrator.GetMultiplePools(ctx, params.ProjectNumber, req.PoolUuids)
	if err != nil {
		return &gcpgenserver.V1betaGetMultiplePoolsInternalServerError{}, err
	}

	vsaPools := convertToPoolsV1Beta(poolList)
	vsaPools = append(vsaPools, cvpPools...)
	logger.Info("Pools found", "pools", poolList)
	return &gcpgenserver.V1betaGetMultiplePoolsOK{
		Pools: vsaPools,
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

func (h Handler) V1betaUpdatePool(ctx context.Context, req *gcpgenserver.PoolUpdateV1beta, params gcpgenserver.V1betaUpdatePoolParams) (gcpgenserver.V1betaUpdatePoolRes, error) {
	logger := util.GetLogger(ctx)

	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdatePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	vendorId := fmt.Sprintf("/projects/%v/locations/%v/pools/%s", params.ProjectNumber, params.LocationId, params.PoolId)
	existingPool, err := h.Orchestrator.GetPoolByVendorID(ctx, vendorId, params.ProjectNumber)
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
		if validateErr.Code == http.StatusBadRequest {
			return &gcpgenserver.V1betaUpdatePoolBadRequest{
				Code:    validateErr.Code,
				Message: validateErr.Message,
			}, nil
		} else {
			return &gcpgenserver.V1betaUpdatePoolInternalServerError{
				Code:    validateErr.Code,
				Message: validateErr.Message,
			}, nil
		}
	}

	param := &common.UpdatePoolParams{
		AccountName: params.ProjectNumber,
		Region:      region,
		CurrentZone: zone,
		PoolId:      params.PoolId,
	}

	// -------------------------------------------------------------------------
	// Update params only if needed.
	// -------------------------------------------------------------------------
	if req.Description.IsSet() {
		param.Description = req.Description.Value
	} else {
		param.Description = existingPool.Description
	}

	if req.SizeInBytes.IsSet() {
		param.SizeInBytes = uint64(req.SizeInBytes.Value)
	} else {
		param.SizeInBytes = existingPool.SizeInBytes
	}

	if req.TotalThroughputMibps.IsSet() {
		param.TotalThroughputMibps = req.TotalThroughputMibps.Value
	} else {
		param.TotalThroughputMibps = existingPool.CustomPerformanceParams.Throughput
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

	if req.TotalIops.IsSet() {
		param.TotalIops = req.TotalIops.Value
	} else {
		param.TotalIops = float64(existingPool.CustomPerformanceParams.Iops)
	}
	// -------------------------------------------------------------------------
	// -------------------------------------------------------------------------

	updatedPool, operationID, err := h.Orchestrator.UpdatePool(ctx, param)
	if err != nil {
		logger.Error("Failed to update pool", "error", err.Error())
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
		poolsV1Beta[i] = *convertToPoolV1Beta(pool)
	}
	return poolsV1Beta
}

func convertToPoolV1Beta(pool *models.Pool) *gcpgenserver.PoolV1beta {
	var deletedAt time.Time
	if pool.DeletedAt != nil {
		deletedAt = *pool.DeletedAt
	}

	var throughputValue float64
	customPerformanceEnabled := false
	var iops int64 = 0
	if (pool.CustomPerformanceParams != nil) && (pool.CustomPerformanceParams.Enabled) {
		customPerformanceEnabled = pool.CustomPerformanceParams.Enabled
		throughputValue = pool.CustomPerformanceParams.Throughput
		iops = pool.CustomPerformanceParams.Iops
	} else {
		throughputValue = pool.TotalThroughputMibps
	}

	labels := gcpgenserver.PoolV1betaLabels{}
	if pool.PoolAttributes.Labels != nil {
		for key, value := range pool.PoolAttributes.Labels {
			labels[key] = value
		}
	}

	return &gcpgenserver.PoolV1beta{
		PoolId:                   gcpgenserver.NewOptString(pool.UUID),
		CreatedAt:                gcpgenserver.NewOptDateTime(pool.CreatedAt),
		UpdatedAt:                gcpgenserver.NewOptDateTime(pool.UpdatedAt),
		DeletedAt:                gcpgenserver.NewOptNilDateTime(deletedAt),
		ResourceId:               pool.Name,
		Description:              gcpgenserver.NewOptNilString(pool.Description),
		Network:                  pool.VendorSubNetID,
		SizeInBytes:              float64(pool.SizeInBytes),
		TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(throughputValue),
		StoragePoolState:         gcpgenserver.NewOptPoolV1betaStoragePoolState(gcpgenserver.PoolV1betaStoragePoolState(pool.State)),
		StoragePoolStateDetails:  gcpgenserver.NewOptString(pool.StateDetails),
		ServiceLevel:             gcpgenserver.PoolV1betaServiceLevel(pool.ServiceLevel),
		TotalIops:                gcpgenserver.NewOptNilFloat64(float64(iops)),
		QosType:                  gcpgenserver.NewOptNilString(pool.QosType),
		CustomPerformanceEnabled: gcpgenserver.NewOptBool(customPerformanceEnabled),
		// Unified Pool is set true & StorageClass is to software for VSA pools
		UnifiedPool:             gcpgenserver.NewOptBool(true),
		Unified:                 gcpgenserver.NewOptBool(true),
		StorageClass:            gcpgenserver.NewOptStorageClassV1beta("SOFTWARE"),
		AllowAutoTiering:        gcpgenserver.NewOptNilBool(pool.AllowAutoTiering),
		HotTierSizeInBytes:      gcpgenserver.NewOptNilFloat64(getHotTierSizeInBytes(pool.AutoTieringConfig)),
		EnableHotTierAutoResize: gcpgenserver.NewOptNilBool(getEnableHotTierAutoResize(pool.AutoTieringConfig)),
		AllocatedBytes:          gcpgenserver.NewOptNilFloat64(pool.PoolAttributes.AllocatedBytes),
		NumberOfVolumes:         gcpgenserver.NewOptNilInt32(int32(pool.PoolAttributes.NumberOfVolumes)),
		EncryptionType:          gcpgenserver.NewOptPoolV1betaEncryptionType(gcpgenserver.PoolV1betaEncryptionType(utils.GetEncryptionType(nil))), // pass pool.KmsConfigID
		Zone:                    gcpgenserver.NewOptString(pool.PoolAttributes.PrimaryZone),
		SecondaryZone:           gcpgenserver.NewOptString(pool.PoolAttributes.SecondaryZone),
		Labels:                  gcpgenserver.OptPoolV1betaLabels{Value: labels},
	}
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
	var deletedAt time.Time
	if pool.DeletedAt != nil {
		deletedAt = time.Time(*pool.DeletedAt)
	}

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
		DeletedAt:                 gcpgenserver.NewOptNilDateTime(deletedAt),
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
		UnifiedPool: gcpgenserver.NewOptBool(false),
		Unified:     gcpgenserver.NewOptBool(false),
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

// validateCreatePoolParams validates the parameters for creating a pool.
// It ensures that the provided parameters meet the requirements for a Unified Flex Storage Pool.
func validateCreatePoolParams(req *gcpgenserver.PoolV1beta, zone string) *gcpgenserver.Error {
	// Check both unified and unifiedPool fields (for backward compatibility)
	unifiedValue := false
	if req.Unified.IsSet() {
		unifiedValue = req.Unified.Value
	} else if req.UnifiedPool.IsSet() {
		unifiedValue = req.UnifiedPool.Value
	}
	if !unifiedValue {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "unified (or unifiedPool) must be set to true",
		}
	}

	if req.ActiveDirectoryResourceId.Value != "" {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Active directory cannot be assigned to a Unified Flex Storage Pool",
		}
	}

	if req.LdapEnabled.Value {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Ldap can not enabled on a Unified Flex Storage Pool",
		}
	}

	if nillable.IsNilOrEmpty(&zone) {
		if !regionalPoolEnabled {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Regional Pool Support is not enabled",
			}
		}

		if !req.Zone.IsSet() {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Zone cannot be empty for regional pool.",
			}
		}

		if !req.SecondaryZone.IsSet() {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Secondary Zone cannot be empty for regional pool.",
			}
		}
	} else {
		if req.Zone.IsSet() && req.Zone.Value != "" && req.Zone.Value != zone {
			return &gcpgenserver.Error{
				Code:    http.StatusBadRequest,
				Message: "Multiple Zone values cannot be passed for Zonal Pool Creation",
			}
		}
	}
	return nil
}

// validateUpdatePoolParams validates the parameters for updating a pool.
// We currently only allow updating the description, size, total throughput, and total IOPS.
func validateUpdatePoolParams(req *gcpgenserver.PoolUpdateV1beta, existingPool *models.Pool) *gcpgenserver.Error {
	if req.Zone.IsSet() && req.Zone.Value != existingPool.PoolAttributes.PrimaryZone {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Migrating to a different Zone is currently not supported",
		}
	}

	if req.GlobalAccessAllowed.IsSet() {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Updating Global access is currently not supported",
		}
	}

	if req.ActiveDirectoryConfigId.IsSet() {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Updating Active Directory is currently not supported",
		}
	}

	if req.AllowAutoTiering.IsSet() {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Updating Auto tiering is currently not supported",
		}
	}

	if req.HotTierSizeInBytes.IsSet() {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Updating HotTierSize is currently not supported",
		}
	}

	if req.EnableHotTierAutoResize.IsSet() {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Updating HotTier auto resize is currently not supported",
		}
	}

	if req.QosType.IsSet() {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Updating QosType is currently not supported",
		}
	}

	if req.CustomPerformanceEnabled.IsSet() && !req.CustomPerformanceEnabled.Value {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "CustomerPerformance must be enabled for Unified Flex Storage Pool",
		}
	}

	// We do not allow pool size to be reduced.
	if req.SizeInBytes.IsSet() && req.SizeInBytes.Value < float64(existingPool.SizeInBytes) {
		return &gcpgenserver.Error{
			Code:    http.StatusBadRequest,
			Message: "Pool size cannot be reduced",
		}
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
