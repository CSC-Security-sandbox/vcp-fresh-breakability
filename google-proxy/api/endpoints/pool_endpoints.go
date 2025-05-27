package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/pools"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
)

// V1betaDescribePool handles the request to describe a pool.
func (h Handler) V1betaDescribePool(ctx context.Context, params gcpgenserver.V1betaDescribePoolParams) (gcpgenserver.V1betaDescribePoolRes, error) {
	logger := util.GetLogger(ctx)
	log.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDescribePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	pool, err := h.Orchestrator.GetPool(ctx, params.PoolId, params.ProjectNumber)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDescribePoolBadRequest{
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
	log.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
	if !req.UnifiedPool.Value {
		logger.Error("UnifiedPool is not set to true")
		return &gcpgenserver.V1betaCreatePoolBadRequest{
			Code:    400,
			Message: "UnifiedPool must be set to true",
		}, nil
	}
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreatePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	if req.ActiveDirectoryResourceId.Value != "" {
		return &gcpgenserver.V1betaCreatePoolBadRequest{
			Code:    400,
			Message: "Active directory cannot be assigned to a Storage Pool of type unified",
		}, nil
	}

	if req.LdapEnabled.Value {
		return &gcpgenserver.V1betaCreatePoolBadRequest{
			Code:    400,
			Message: "Ldap can not enabled to a Storage Pool of type unified",
		}, nil
	}

	vendorId := fmt.Sprintf("/projects/%v/locations/%v/pools/%s", params.ProjectNumber, params.LocationId, req.ResourceId)
	// Check if the pool already exists
	existingPool, err := h.Orchestrator.GetPoolByVendorID(ctx, vendorId)
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

	param := &common.CreatePoolParams{
		AccountName:    params.ProjectNumber,
		Region:         region,
		CurrentZone:    zone,
		Name:           req.ResourceId,
		VendorID:       vendorId,
		VendorSubNetID: req.Network,
		ServiceLevel:   string(req.ServiceLevel),
		SizeInBytes:    uint64(req.SizeInBytes),
		QosType:        string(req.QosType.Value),
	}
	created, operationID, err := h.Orchestrator.CreatePool(ctx, param)
	if err != nil {
		logger.Error("Failed to create pool", "error", err.Error())
		return &gcpgenserver.V1betaCreatePoolInternalServerError{}, err
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
	log.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeletePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	// Check if the pool exists
	existingPool, err := h.Orchestrator.GetPool(ctx, params.PoolId, params.ProjectNumber)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDeletePoolBadRequest{
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
			return &gcpgenserver.V1betaDeletePoolBadRequest{
				Code:    404,
				Message: "Pool not found",
			}, nil
		}
		if errors.IsConflictErr(err) {
			logger.Info("Pool has volume", "uuid", params.PoolId)
			return &gcpgenserver.V1betaDeletePoolBadRequest{
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
	log.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
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

	pools, err := h.Orchestrator.GetMultiplePools(ctx, params.ProjectNumber, req.PoolUuids)
	if err != nil {
		return &gcpgenserver.V1betaGetMultiplePoolsInternalServerError{}, err
	}

	vsaPools := convertToPoolsV1Beta(pools)
	vsaPools = append(vsaPools, cvpPools...)
	logger.Info("Pools found", "pools", pools)
	return &gcpgenserver.V1betaGetMultiplePoolsOK{
		Pools: vsaPools,
	}, nil
}

// V1betaListPools handles the request to list pools.
func (h Handler) V1betaListPools(ctx context.Context, params gcpgenserver.V1betaListPoolsParams) (gcpgenserver.V1betaListPoolsRes, error) {
	logger := util.GetLogger(ctx)
	log.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaListPoolsBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	// TODO: Check if the include deleted flag is true

	pools, err := h.Orchestrator.ListPools(ctx, params.ProjectNumber)
	if err != nil {
		return &gcpgenserver.V1betaListPoolsInternalServerError{}, err
	}

	logger.Info("Pools found", "pools", pools)
	return &gcpgenserver.V1betaListPoolsOK{
		Pools: convertToPoolsV1Beta(pools),
	}, nil
}

func (h Handler) V1betaUpdatePool(ctx context.Context, req *gcpgenserver.PoolUpdateV1beta, params gcpgenserver.V1betaUpdatePoolParams) (gcpgenserver.V1betaUpdatePoolRes, error) {
	// TODO implement me
	panic("implement me")
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

	return &gcpgenserver.PoolV1beta{
		PoolId:                   gcpgenserver.NewOptString(pool.UUID),
		CreatedAt:                gcpgenserver.NewOptDateTime(pool.CreatedAt),
		UpdatedAt:                gcpgenserver.NewOptDateTime(pool.UpdatedAt),
		DeletedAt:                gcpgenserver.NewOptNilDateTime(deletedAt),
		ResourceId:               pool.Name,
		Network:                  pool.VendorSubNetID,
		SizeInBytes:              float64(pool.SizeInBytes),
		TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(throughputValue),
		StoragePoolState:         gcpgenserver.NewOptPoolV1betaStoragePoolState(gcpgenserver.PoolV1betaStoragePoolState(pool.State)),
		StoragePoolStateDetails:  gcpgenserver.NewOptString(pool.StateDetails),
		ServiceLevel:             gcpgenserver.PoolV1betaServiceLevel(pool.ServiceLevel),
		TotalIops:                gcpgenserver.NewOptNilFloat64(float64(iops)),
		QosType:                  gcpgenserver.NewOptNilString(pool.QosType),
		CustomPerformanceEnabled: gcpgenserver.NewOptBool(customPerformanceEnabled),
		// Unified Pool is set true for VSA pools
		UnifiedPool: gcpgenserver.NewOptBool(true),
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
		StorageClass:              gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1beta(*pool.StorageClass)),
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
	}
}
