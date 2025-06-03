package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	errs "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	VolumeCreate                   = _hydrateVolumeCreate
	VolumeDelete                   = _hydrateVolumeDelete
	HydrateReplicationState        = _hydrateReplicationState
	HydrateReplicationStateAndType = _hydrateReplicationStateAndType
	ReplicationDelete              = _hydrateReplicationDelete
	ReplicationCreate              = _hydrateReplicationCreate
	GetQuotaLimit                  = _getQuotaLimit
	createHydrateCreateObject      = _createHydrateCreateObject
	hydrateToCffe                  = _hydrateToCffe
	doHydrateToCffe                = _doHydrateToCffe
	getQuotaLimitsForResource      = _getQuotaLimitsForResource
	HydrateRetryErrors             = []int{409, 429, 500, 503, 504}
	baseUri                        = env.GetString("GCP_HYDRATE_BASE_URL", "")
	quotaLimitExceededRegex        = regexp.MustCompile(`^Quota limit`)
	ApiHydrateMaxRetries           = max(1, env.GetInt("API_HYDRATE_MAX_RETRIES", 10))
	ApiHydrateRetryDelay           = time.Duration(env.GetInt("API_HYDRATE_RETRY_DELAY", 5)) * time.Second
	jsonMarshal                    = json.Marshal
	httpNewRequest                 = http.NewRequest
	httpClient                     *http.Client
	httpClientDo                   = httpClient.Do
	ioReadAll                      = io.ReadAll
	jsonUnmarshal                  = json.Unmarshal
	apiIdleTimeout                 = env.GetUint("API_HYDRATE_IDLE_TIMEOUT", 8)
	stringConvAtoi                 = strconv.Atoi
)

type ContextKey int
type ResourceType string
type QuotaType string

const (
	Create                                   = "batchCreate"
	Delete                                   = "batchDelete"
	ResourceTypeVolume          ResourceType = "VOLUME"
	ResourceTypeReplication     ResourceType = "REPLICATION"
	FlexVolumesPerRegion        QuotaType    = "FLEX_VOLUMES_PER_REGION"
	FlexReplicationVolumesLimit QuotaType    = "FLEX_REPLICATED_VOLUMES_PER_REGION"
	ResourceQuotaTypeEmpty      QuotaType    = ""
	CorrelationContextKey       ContextKey   = iota
	CorrelationIDName           string       = log.RequestCorrelationID
)

func init() {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	httpTransportClone := http.DefaultTransport.(*http.Transport).Clone()
	if apiIdleTimeout > 0 {
		httpTransportClone.IdleConnTimeout = time.Second * ((time.Duration)(apiIdleTimeout))
	} else {
		httpTransportClone.DisableKeepAlives = true
	}
	httpClient := &http.Client{}
	loggingRoundTripper := httphelpers.GetLoggingRoundTripper("HYDRATE", logger, httpTransportClone)
	httpClient.Transport = loggingRoundTripper
	httpClientDo = httpClient.Do
}

type ccfeSuccessResponseObject struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type ccfeErrorResponseObject struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

func _hydrateVolumeCreate(ctx context.Context, logger log.Logger, volume models.VolumeHydrateObject, location string, projectId string, token string) error {
	request := models.Request{
		Volume: &volume,
	}
	hydrateVolume := createHydrateCreateObject(request)
	url := fmt.Sprintf("%s/v1internal/projects/%s/locations/%s/resources:%s", baseUri, projectId, location, Create)
	logger.Infof("Hydrating volume create to callbackApi, volume: %+v", volume)
	err := hydrateToCffe(ctx, logger, hydrateVolume, url, http.MethodPost, token)
	return err
}

func _hydrateVolumeDelete(ctx context.Context, logger log.Logger, volumeResourceID string, region string, projectId string, token string) error {
	nameArray := make([]string, 1)
	nameArray[0] = "volumes/" + volumeResourceID
	url := fmt.Sprintf("%s/v1internal/projects/%s/locations/%s/resources:%s", baseUri, projectId, region, Delete)
	logger.Infof("Hydrating volume delete to callbackApi, volumeId: %s", volumeResourceID)
	err := hydrateToCffe(ctx, logger, models.GcpHydrateDelete{Names: nameArray}, url, http.MethodPost, token)
	return err
}

func _hydrateReplicationState(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, token string) error {
	request := &models.VolumeReplicationUpdateMaskRequest{
		State: state,
	}
	url := fmt.Sprintf("%s/v1internal/projects/%s/locations/%s/volumes/%s/replications/%s?update_mask=state", baseUri, projectId, region, volumeResourceID, replicationId)
	logger.Infof("Hydrating replication state to callbackApi, replicationId:: %+v", replicationId)
	err := hydrateToCffe(ctx, logger, request, url, http.MethodPatch, token)
	return err
}

func _hydrateReplicationStateAndType(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, hybridReplicationType models.HybridReplicationHydrateType, token string) error {
	request := &models.VolumeReplicationUpdateMaskRequest{
		State:                 state,
		HybridReplicationType: hybridReplicationType,
	}
	url := fmt.Sprintf("%s/v1internal/projects/%s/locations/%s/volumes/%s/replications/%s?update_mask=state,hybrid_replication_type", baseUri, projectId, region, volumeResourceID, replicationId)
	logger.Infof("Hydrating replication state & type to callbackApi, replicationId:: %+v", replicationId)
	err := hydrateToCffe(ctx, logger, request, url, http.MethodPatch, token)
	return err
}
func _hydrateReplicationDelete(ctx context.Context, logger log.Logger, replicationResourceId string, volumeResourceID string, region string, projectId string, token string) error {
	nameArray := make([]string, 1)
	nameArray[0] = "replications/" + replicationResourceId
	url := fmt.Sprintf("%s/v1internal/projects/%s/locations/%s/volumes/%s/resources:%s", baseUri, projectId, region, volumeResourceID, Delete)
	logger.Infof("Hydrating replication delete to callbackApi, replicationId:: %+v", replicationResourceId)
	err := hydrateToCffe(ctx, logger, models.GcpHydrateDelete{Names: nameArray}, url, http.MethodPost, token)
	return err
}

func _createHydrateCreateObject(request models.Request) models.GcpHydrateCreate {
	requestArr := make([]models.Request, 1)
	requestArr[0] = request
	requestBody := models.GcpHydrateCreate{Requests: requestArr}
	return requestBody
}

func _hydrateToCffe(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
	var err error
	utils.RetrierOnCodes(logger, func() (bool, error) {
		err = doHydrateToCffe(ctx, logger, v, url, method, token)
		if err != nil {
			_, httpcode := err.(*errs.CustomError).GetHttpCode()
			if httpcode == 429 {
				quotaLimitExceededMatch := quotaLimitExceededRegex.FindStringSubmatch(err.(*errs.CustomError).GetMessage())
				if quotaLimitExceededMatch != nil {
					return true, err
				}
			}
		}
		return false, err
	}, HydrateRetryErrors, ApiHydrateMaxRetries, ApiHydrateRetryDelay)
	return nil
}

func _doHydrateToCffe(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
	bodyBytes, err := jsonMarshal(v)
	if err != nil {
		return errs.NewVCPError(errs.ErrFailedToMarshalJson, err)
	}

	req, err := httpNewRequest(method, url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return errs.NewVCPError(errs.ErrFailedToCreateHTTP, err)
	}

	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Content-Type", "application/json")

	if ctxCorrId, ok := ctx.Value(CorrelationContextKey).(string); ok {
		req.Header.Set(CorrelationIDName, ctxCorrId)
	} else {
		logger.Warn("x-correlation-id not present in context for CCFE hydration request")
	}

	res, err := httpClientDo(req)
	if err != nil {
		return errs.NewVCPError(errs.ErrFailedToExecuteHTTP, err)
	}
	defer func(res *http.Response) {
		err := res.Body.Close()
		if err != nil {
			logger.Error("error in response body close: ", err)
		}
	}(res)

	if res.StatusCode != 200 {
		responseBody, err := ioReadAll(res.Body)
		if err != nil {
			return errs.NewVCPError(errs.ErrFailedToReadResponse, err)
		}
		var ccfeRespStruct models.CcfeErrorResponseObject
		err = jsonUnmarshal(responseBody, &ccfeRespStruct)
		if err != nil || ccfeRespStruct.Error == nil {
			return errs.NewVCPError(errs.ErrFailedToUnmarshalCCFE, err)
		}
		return &errs.CustomError{
			HttpCode: &ccfeRespStruct.Error.Code,
			Message:  ccfeRespStruct.Error.Message,
		}
	}
	logger.Info("Hydration successfully completed")
	return nil
}

func _hydrateReplicationCreate(ctx context.Context, logger log.Logger, replication models.ReplicationHydrateObject, region string, projectId string, volumeResourceID string, token string) error {
	request := models.Request{
		Replication: &replication,
	}
	hydrateReplication := createHydrateCreateObject(request)
	url := fmt.Sprintf("%s/v1internal/projects/%s/locations/%s/volumes/%s/resources:%s", baseUri, projectId, region, volumeResourceID, Create)
	logger.Infof("Hydrating replication create to callbackApi, replication: %+v", replication)
	err := hydrateToCffe(ctx, logger, hydrateReplication, url, http.MethodPost, token)
	return err
}

func _getQuotaLimit(ctx context.Context, logger log.Logger, region string, projectId string, token string, resourceType ResourceType) (int, error) {
	quotaType := getResourceQuotaType(resourceType)
	logger.Infof("Calling get Quota type ,quotaType : %+v", quotaType)
	destVolumeQuota, err := getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, logger)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return 0, err
	}
	return destVolumeQuota, nil
}

// GetQuotaLimitsForResource calls google callback API and returns the limit
func _getQuotaLimitsForResource(ctx context.Context, projectId string, region string, quotaType QuotaType, token string, logger log.Logger) (int, error) {
	url := fmt.Sprintf("%s/v1internal/projects/%s/locations/%s:getQuotaLimit?type=%s", baseUri, projectId, region, quotaType)

	req, err := httpNewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, errs.NewVCPError(errs.ErrFailedToExecuteHTTP, err)
	}

	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Content-Type", "application/json")

	if ctxCorrId, ok := ctx.Value(CorrelationContextKey).(string); ok {
		req.Header.Set(CorrelationIDName, ctxCorrId)
	} else {
		logger.Warn("x-correlation-id not present in context for CCFE hydration request")
	}

	res, err := httpClientDo(req)
	if err != nil {
		return 0, errs.NewVCPError(errs.ErrFailedToExecuteHTTP, err)
	}
	defer func(res *http.Response) {
		err := res.Body.Close()
		if err != nil {
			logger.Error("error in response body close: ", err)
		}
	}(res)

	responseBody, err := ioReadAll(res.Body)
	if err != nil {
		return 0, errs.NewVCPError(errs.ErrFailedToReadResponse, err)
	}
	if res.StatusCode == 200 {
		var quotaObject ccfeSuccessResponseObject
		err = jsonUnmarshal(responseBody, &quotaObject)
		if err != nil {
			return 0, errs.NewVCPError(errs.ErrFailedToUnmarshalCCFE, err)
		}
		quota, err := stringConvAtoi(quotaObject.Value)
		if err != nil {
			return 0, errs.NewVCPError(errs.ErrFailedToReadQuota, err)
		}
		return quota, nil
	} else {
		var errorObject ccfeErrorResponseObject
		err = jsonUnmarshal(responseBody, &errorObject)
		if err != nil {
			return 0, errs.NewVCPError(errs.ErrFailedToUnmarshalCCFE, err)
		}
		return 0, &errs.CustomError{
			OriginalErr: errs.WrapAsTemporalApplicationError(err),
			Message:     errorObject.Message,
			HttpCode:    &errorObject.Code,
		}
	}
}

func getResourceQuotaType(resourceType ResourceType) QuotaType {
	switch resourceType {
	case ResourceTypeVolume:
		return FlexVolumesPerRegion
	case ResourceTypeReplication:
		return FlexReplicationVolumesLimit
	}
	return ResourceQuotaTypeEmpty
}
