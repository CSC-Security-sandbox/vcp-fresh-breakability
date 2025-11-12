package api

import (
	// Standard library imports
	"context"
	"encoding/json"
	"net/http"
	"time"

	// Third-party and local imports
	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vcpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	createClient              = cvp.CreateClient
	getActiveDirectoryFromVCP = _getActiveDirectoryFromVCP
	// Define the state hierarchy once, in priority order (highest to lowest)
	activeDirectoryStateHierarchy = []gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState{
		gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateUPDATING,
		gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateERROR,
		gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateINUSE,
		gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY,
		// Add more states here in priority order as needed
	}
)

// PasswordMask defines the mask used when logging out a password
const (
	PasswordMask = "******************"
	UsernameMask = "******************"
)

func (h Handler) V1betaCreateActiveDirectory(
	ctx context.Context,
	req *gcpgenserver.ActiveDirectoryV1beta,
	params gcpgenserver.V1betaCreateActiveDirectoryParams,
) (gcpgenserver.V1betaCreateActiveDirectoryRes, error) {
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	param := common.CreateActiveDirectoryParams{
		AccountId:                  params.ProjectNumber,
		LocationId:                 params.LocationId,
		XCorrelationId:             params.XCorrelationID.Value,
		Username:                   req.Username,
		ResourceId:                 req.ResourceId,
		Description:                req.Description.Value,
		Password:                   req.Password,
		Domain:                     req.Domain,
		DNS:                        req.DNS,
		NetBIOS:                    req.NetBIOS,
		OrganizationalUnit:         req.OrganizationalUnit.Value,
		Site:                       req.Site.Value,
		KdcIP:                      req.KdcIP.Value,
		KdcHostname:                req.KdcHostname.Value,
		LdapSigning:                req.LdapSigning.Value,
		AllowLocalNFSUsersWithLdap: req.AllowLocalNFSUsersWithLdap.Value,
		EncryptDCConnections:       req.EncryptDCConnections.Value,
		SecurityOperators:          req.SecurityOperators,
		BackupOperators:            req.BackupOperators,
		Administrators:             req.Administrators,
		AesEncryption:              req.AesEncryption.Value,
	}

	ad, jobUUID, err := h.Orchestrator.CreateActiveDirectory(ctx, &param)
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaCreateActiveDirectoryBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		return &gcpgenserver.V1betaCreateActiveDirectoryInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}, nil
	}

	resp, err := encodeActiveDirectoryV1(convertToActiveDirectoryV1Beta(ad))
	if err != nil {
		return &gcpgenserver.V1betaCreateActiveDirectoryInternalServerError{}, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber +
		"/locations/" + params.LocationId +
		"/operations/" + jobUUID

	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationID),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(false),
	}, nil
}

func convertToActiveDirectoryV1Beta(
	ad *vcpModels.ActiveDirectory,
) *gcpgenserver.ActiveDirectoryV1beta {
	return &gcpgenserver.ActiveDirectoryV1beta{
		ActiveDirectoryId:           gcpgenserver.NewOptString(ad.UUID),
		ResourceId:                  ad.AdName,
		Username:                    UsernameMask,
		Password:                    PasswordMask,
		Description:                 gcpgenserver.NewOptString(ad.ActiveDirectoryAttributes.Description),
		Domain:                      ad.Domain,
		DNS:                         ad.DNS,
		NetBIOS:                     ad.NetBIOS,
		ActiveDirectoryState:        gcpgenserver.NewOptActiveDirectoryV1betaActiveDirectoryState(mapActiveDirectoryState(ad.State)),
		ActiveDirectoryStateDetails: gcpgenserver.NewOptString(ad.StateDetails),
		CreatedAt:                   gcpgenserver.NewOptDateTime(ad.CreatedAt),
		UpdatedAt:                   gcpgenserver.NewOptDateTime(ad.UpdatedAt),
		SecurityOperators:           ad.ActiveDirectoryAttributes.SecurityOperators,
		BackupOperators:             ad.ActiveDirectoryAttributes.BackupOperators,
		Administrators:              ad.ActiveDirectoryAttributes.Administrators,
		AesEncryption:               gcpgenserver.NewOptBool(ad.ActiveDirectoryAttributes.AesEncryption),
		AllowLocalNFSUsersWithLdap:  gcpgenserver.NewOptBool(ad.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap),
		EncryptDCConnections:        gcpgenserver.NewOptBool(ad.ActiveDirectoryAttributes.EncryptDCConnections),
		LdapSigning:                 gcpgenserver.NewOptBool(ad.ActiveDirectoryAttributes.LdapSigning),
		OrganizationalUnit:          gcpgenserver.NewOptString(ad.ActiveDirectoryAttributes.OrganizationalUnit),
		Site:                        gcpgenserver.NewOptString(ad.ActiveDirectoryAttributes.Site),
		KdcIP:                       gcpgenserver.NewOptString(ad.ActiveDirectoryAttributes.KdcIP),
		KdcHostname:                 gcpgenserver.NewOptString(ad.ActiveDirectoryAttributes.KdcHostname),
	}
}

// convertOrchestratorActiveDirectoryToV1Beta converts orchestrator's *models.ActiveDirectory to gcpgenserver.ActiveDirectoryV1beta
func convertOrchestratorActiveDirectoryToV1Beta(ad *vcpModels.ActiveDirectory) gcpgenserver.ActiveDirectoryV1beta {
	// Convert state from VCP format to CVS format
	state := gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY // Default state
	details := ad.StateDetails
	if ad.State != "" {
		switch ad.State {
		case "CREATING":
			state = gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateCREATING
		case "READY":
			state = gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY
		case "UPDATING":
			state = gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateUPDATING
		case "IN_USE":
			state = gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateINUSE
		case "DELETING":
			state = gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateDELETING
		case "ERROR":
			state = gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateERROR
		default:
			state = gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY
		}
	}
	if details == "" {
		details = "Active Directory is ready"
	}

	// Initialize empty slices if nil
	backupOperators := make([]string, 0)
	securityOperators := make([]string, 0)
	administrators := make([]string, 0)

	// Extract attributes if available
	if ad.ActiveDirectoryAttributes != nil {
		if ad.ActiveDirectoryAttributes.BackupOperators != nil {
			backupOperators = ad.ActiveDirectoryAttributes.BackupOperators
		}
		if ad.ActiveDirectoryAttributes.SecurityOperators != nil {
			securityOperators = ad.ActiveDirectoryAttributes.SecurityOperators
		}
		if ad.ActiveDirectoryAttributes.Administrators != nil {
			administrators = ad.ActiveDirectoryAttributes.Administrators
		}
	}

	adResponse := gcpgenserver.ActiveDirectoryV1beta{
		ActiveDirectoryId:           gcpgenserver.NewOptString(ad.UUID),
		ResourceId:                  ad.AdName,
		Username:                    log.Secret(ad.Username).String(),
		Password:                    log.Secret(ad.Password).String(),
		Domain:                      ad.Domain,
		DNS:                         ad.DNS,
		NetBIOS:                     ad.NetBIOS,
		ActiveDirectoryState:        gcpgenserver.NewOptActiveDirectoryV1betaActiveDirectoryState(state),
		ActiveDirectoryStateDetails: gcpgenserver.NewOptString(details),
		CreatedAt:                   gcpgenserver.NewOptDateTime(ad.CreatedAt),
		UpdatedAt:                   gcpgenserver.NewOptDateTime(ad.UpdatedAt),
		SecurityOperators:           securityOperators,
		BackupOperators:             backupOperators,
		Administrators:              administrators,
	}
	if ad.DeletedAt != nil {
		adResponse.DeletedAt = gcpgenserver.NewOptDateTime(*ad.DeletedAt)
	}
	return adResponse
}

// encodeActiveDirectoryV1 encodes an ActiveDirectoryV1beta struct to JSON.
func encodeActiveDirectoryV1(
	pool *gcpgenserver.ActiveDirectoryV1beta,
) (jx.Raw, error) {
	data, err := json.Marshal(pool)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (h Handler) V1betaDeleteActiveDirectory(ctx context.Context, params gcpgenserver.V1betaDeleteActiveDirectoryParams) (r gcpgenserver.V1betaDeleteActiveDirectoryRes, _ error) {
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	deleteParams := common.DeleteActiveDirectoryParams{
		ProjectNumber:       params.ProjectNumber,
		ActiveDirectoryUUID: params.ActiveDirectoryId,
	}

	jobUUID, err := h.Orchestrator.DeleteActiveDirectory(ctx, &deleteParams)
	if err != nil {
		if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaDeleteActiveDirectoryConflict{
				Code:    http.StatusConflict,
				Message: err.Error(),
			}, nil
		}
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaDeleteActiveDirectoryBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		return &gcpgenserver.V1betaDeleteActiveDirectoryInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}, nil
	}

	// If jobUUID is empty, it means the Active Directory is already deleted
	// Return a done operation with a dummy operation ID
	if jobUUID == "" {
		dummyOperationID := "/v1beta/projects/" + params.ProjectNumber +
			"/locations/" + params.LocationId +
			"/operations/" + uuid.UUID{}.String()
		return &gcpgenserver.OperationV1beta{
			Name: gcpgenserver.NewOptString(dummyOperationID),
			Done: gcpgenserver.NewOptBool(true),
		}, nil
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber +
		"/locations/" + params.LocationId +
		"/operations/" + jobUUID

	return &gcpgenserver.OperationV1beta{
		Name: gcpgenserver.NewOptString(operationID),
		Done: gcpgenserver.NewOptBool(false),
	}, nil
}

func (h Handler) V1betaDescribeActiveDirectory(ctx context.Context, params gcpgenserver.V1betaDescribeActiveDirectoryParams) (r gcpgenserver.V1betaDescribeActiveDirectoryRes, err error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	var adV1BetaModel *gcpgenserver.ActiveDirectoryV1beta
	if cvp.CVP_HOST == "" {
		adV1BetaModel, err = getActiveDirectoryFromVCP(ctx, h, params.ActiveDirectoryId)
		if err != nil {
			if errors.IsNotFoundErr(err) {
				return &gcpgenserver.V1betaDescribeActiveDirectoryNotFound{
					Code:    http.StatusNotFound,
					Message: err.Error(),
				}, nil
			}
			logger.Errorf("Error getting active directory from VCP when cvp is not present with error: %v", err)
			return &gcpgenserver.V1betaDescribeActiveDirectoryInternalServerError{
				Code:    http.StatusInternalServerError,
				Message: "internal error during the describe active directory",
			}, nil
		}
	} else {
		pathParams := &active_directories.V1betaDescribeActiveDirectoryParams{
			LocationID:        params.LocationId,
			ProjectNumber:     params.ProjectNumber,
			XCorrelationID:    &params.XCorrelationID.Value,
			ActiveDirectoryID: params.ActiveDirectoryId,
		}
		jwtToken := utils.GetJWTTokenFromContext(ctx)
		cvpClient := createClient(logger, jwtToken)
		resp, err := cvpClient.ActiveDirectories.V1betaDescribeActiveDirectory(pathParams)
		if err != nil {
			switch e := err.(type) {
			case *active_directories.V1betaDescribeActiveDirectoryUnprocessableEntity:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeActiveDirectoryUnprocessableEntity{
					Code:    code,
					Message: msg,
				}, nil
			case *active_directories.V1betaDescribeActiveDirectoryNotFound:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeActiveDirectoryNotFound{
					Code:    code,
					Message: msg,
				}, nil
			case *active_directories.V1betaDescribeActiveDirectoryBadRequest:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeActiveDirectoryBadRequest{
					Code:    code,
					Message: msg,
				}, nil
			case *active_directories.V1betaDescribeActiveDirectoryUnauthorized:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeActiveDirectoryUnauthorized{
					Code:    code,
					Message: msg,
				}, nil

			case *active_directories.V1betaDescribeActiveDirectoryForbidden:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeActiveDirectoryForbidden{
					Code:    code,
					Message: msg,
				}, nil

			case *active_directories.V1betaDescribeActiveDirectoryTooManyRequests:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeActiveDirectoryTooManyRequests{
					Code:    code,
					Message: msg,
				}, nil
			case *active_directories.V1betaDescribeActiveDirectoryDefault:
				return &gcpgenserver.V1betaDescribeActiveDirectoryInternalServerError{
					Code:    500,
					Message: err.Error(),
				}, nil
			}
		}
		if resp == nil || resp.Payload == nil {
			return &gcpgenserver.V1betaDescribeActiveDirectoryInternalServerError{
				Code:    500,
				Message: "unknown error during the describe active directory",
			}, nil
		}
		// Converting CVP model to gcpgenserver.ActiveDirectoryV1beta
		cvpADV1BetaModel := convertToADV1Beta(resp.Payload)
		adV1BetaModel = &cvpADV1BetaModel
		// Compare AD state hierarchy
		vcpAd, vcpErr := getActiveDirectoryFromVCP(ctx, h, params.ActiveDirectoryId)
		if vcpErr != nil {
			// If the AD is not found in VCP, return the AD from CVP.
			if errors.IsNotFoundErr(vcpErr) {
				logger.Infof("AD %s not found in VCP, returning AD from CVP", params.ActiveDirectoryId)
				return adV1BetaModel, nil
			}
			logger.Errorf("Error getting active directory from VCP when cvp is present with error: %v", vcpErr)
			return &gcpgenserver.V1betaDescribeActiveDirectoryInternalServerError{
				Code:    500,
				Message: "internal error during the describe active directory",
			}, nil
		}
		compareADStateHierarchy(adV1BetaModel, vcpAd)
	}
	return adV1BetaModel, nil
}

func (h Handler) V1betaGetMultipleActiveDirectories(ctx context.Context, req *gcpgenserver.ActiveDirectoryIdListV1beta, params gcpgenserver.V1betaGetMultipleActiveDirectoriesParams) (r gcpgenserver.V1betaGetMultipleActiveDirectoriesRes, _ error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	var adResponse gcpgenserver.V1betaGetMultipleActiveDirectoriesOK

	if cvp.CVP_HOST == "" {
		// VCP Path: Use orchestrator to get from VCP database
		ads, err := h.Orchestrator.GetMultipleActiveDirectories(ctx, req.ActiveDirectoryUuids)
		if err != nil {
			return &gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}

		// Convert VCP models to CVS response format
		adResponse = gcpgenserver.V1betaGetMultipleActiveDirectoriesOK{
			ActiveDirectories: []gcpgenserver.ActiveDirectoryV1beta{},
		}
		for _, ad := range ads {
			adResponse.ActiveDirectories = append(adResponse.ActiveDirectories, convertOrchestratorActiveDirectoryToV1Beta(ad))
		}
	} else {
		// CVS Path: Original CVS client call logic
		body := &models.ActiveDirectoryIDListV1beta{
			ActiveDirectoryUUIDs: req.ActiveDirectoryUuids,
		}
		reqPrams := &active_directories.V1betaGetMultipleActiveDirectoriesParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body:           body,
		}
		jwtToken := utils.GetJWTTokenFromContext(ctx)
		cvpClient := createClient(logger, jwtToken)
		resp, err := cvpClient.ActiveDirectories.V1betaGetMultipleActiveDirectories(reqPrams)
		if err != nil {
			switch e := err.(type) {
			case *active_directories.V1betaGetMultipleActiveDirectoriesNotFound:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleActiveDirectoriesNotFound{
					Code:    code,
					Message: msg,
				}, nil
			case *active_directories.V1betaGetMultipleActiveDirectoriesBadRequest:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleActiveDirectoriesBadRequest{
					Code:    code,
					Message: msg,
				}, nil
			case *active_directories.V1betaGetMultipleActiveDirectoriesUnauthorized:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleActiveDirectoriesUnauthorized{
					Code:    code,
					Message: msg,
				}, nil

			case *active_directories.V1betaGetMultipleActiveDirectoriesForbidden:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleActiveDirectoriesForbidden{
					Code:    code,
					Message: msg,
				}, nil

			case *active_directories.V1betaGetMultipleActiveDirectoriesTooManyRequests:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleActiveDirectoriesTooManyRequests{
					Code:    code,
					Message: msg,
				}, nil
			case *active_directories.V1betaGetMultipleActiveDirectoriesDefault:
				return &gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError{
					Code:    500,
					Message: err.Error(),
				}, nil
			}
		}
		if resp == nil || resp.Payload == nil {
			return &gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError{
				Code:    500,
				Message: "unknown error during the get multiple active directories",
			}, nil
		}
		// Converting CVP model to gcpgenserver.ActiveDirectoryV1beta
		adResponse = gcpgenserver.V1betaGetMultipleActiveDirectoriesOK{
			ActiveDirectories: []gcpgenserver.ActiveDirectoryV1beta{},
		}

		var vcpADMap map[string]*vcpModels.ActiveDirectory
		if len(req.ActiveDirectoryUuids) > 0 {
			ads, vcpErr := h.Orchestrator.GetMultipleActiveDirectories(ctx, req.ActiveDirectoryUuids)
			if vcpErr != nil {
				logger.Errorf("Error getting active directories from VCP when cvp is present in getMultipleActiveDirectories with error: %v", vcpErr)
				return &gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError{
					Code:    500,
					Message: "internal error during the get multiple active directory",
				}, nil
			} else {
				vcpADMap = make(map[string]*vcpModels.ActiveDirectory, len(ads))
				for _, ad := range ads {
					vcpADMap[ad.UUID] = ad
				}
			}
		}

		adResponse.ActiveDirectories = append(adResponse.ActiveDirectories, mergeActiveDirectoryResponses(resp.Payload.ActiveDirectories, vcpADMap)...)
	}

	return &adResponse, nil
}

func (h Handler) V1betaUpdateActiveDirectory(ctx context.Context, req *gcpgenserver.ActiveDirectoryUpdateV1beta, params gcpgenserver.V1betaUpdateActiveDirectoryParams) (r gcpgenserver.V1betaUpdateActiveDirectoryRes, _ error) {
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	param := convertToUpdateParamsForValidation(req, params)

	ad, jobUUID, err := h.Orchestrator.UpdateActiveDirectory(ctx, param)

	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaUpdateActiveDirectoryBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		return &gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}, nil
	}

	resp, err := encodeActiveDirectoryV1(convertToActiveDirectoryV1Beta(ad))
	if err != nil {
		return &gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError{}, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber +
		"/locations/" + params.LocationId +
		"/operations/" + jobUUID

	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationID),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(false),
	}, nil
}

func (h Handler) V1betaListActiveDirectories(ctx context.Context, params gcpgenserver.V1betaListActiveDirectoriesParams) (gcpgenserver.V1betaListActiveDirectoriesRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	var adResponse gcpgenserver.V1betaListActiveDirectoriesOK

	if cvp.CVP_HOST == "" {
		ads, err := h.Orchestrator.ListActiveDirectories(ctx, params.ProjectNumber)
		if err != nil {
			return &gcpgenserver.V1betaListActiveDirectoriesInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}

		// Convert VCP models to CVS response format
		adResponse = gcpgenserver.V1betaListActiveDirectoriesOK{
			ActiveDirectories: []gcpgenserver.ActiveDirectoryV1beta{},
		}
		for _, ad := range ads {
			adResponse.ActiveDirectories = append(adResponse.ActiveDirectories, convertOrchestratorActiveDirectoryToV1Beta(ad))
		}
	} else {
		// CVS Path: Original CVS client call logic
		pathParams := &active_directories.V1betaListActiveDirectoriesParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
		}
		jwtToken := utils.GetJWTTokenFromContext(ctx)
		cvpClient := createClient(logger, jwtToken)
		resp, err := cvpClient.ActiveDirectories.V1betaListActiveDirectories(pathParams)
		if err != nil {
			return nil, err
		}
		// Converting CVP model to gcpgenserver.ActiveDirectoryV1beta
		adResponse = gcpgenserver.V1betaListActiveDirectoriesOK{
			ActiveDirectories: []gcpgenserver.ActiveDirectoryV1beta{},
		}

		ads, vcpErr := h.Orchestrator.ListActiveDirectories(ctx, params.ProjectNumber)
		var vcpADMap map[string]*vcpModels.ActiveDirectory
		if vcpErr != nil {
			logger.Errorf("Error getting active directories from VCP when cvp is present in listActiveDirectories with error: %v", vcpErr)
			return &gcpgenserver.V1betaListActiveDirectoriesInternalServerError{
				Code:    500,
				Message: "internal error during the list multiple active directory",
			}, nil
		} else {
			vcpADMap = make(map[string]*vcpModels.ActiveDirectory, len(ads))
			for _, ad := range ads {
				vcpADMap[ad.UUID] = ad
			}
		}

		adResponse.ActiveDirectories = append(adResponse.ActiveDirectories, mergeActiveDirectoryResponses(resp.Payload.ActiveDirectories, vcpADMap)...)
	}

	return &adResponse, nil
}

func mergeActiveDirectoryResponses(cvpAds []*models.ActiveDirectoryV1beta, vcpADMap map[string]*vcpModels.ActiveDirectory) []gcpgenserver.ActiveDirectoryV1beta {
	if len(cvpAds) == 0 {
		return []gcpgenserver.ActiveDirectoryV1beta{}
	}

	merged := make([]gcpgenserver.ActiveDirectoryV1beta, 0, len(cvpAds))
	for _, ad := range cvpAds {
		cvpAD := convertToADV1Beta(ad)
		if vcpADMap != nil && cvpAD.ActiveDirectoryId.Set && len(cvpAD.ActiveDirectoryId.Value) > 0 {
			if vcpAd, ok := vcpADMap[cvpAD.ActiveDirectoryId.Value]; ok {
				convertedVCP := convertOrchestratorActiveDirectoryToV1Beta(vcpAd)
				compareADStateHierarchy(&cvpAD, &convertedVCP)
			}
		}
		merged = append(merged, cvpAD)
	}

	return merged
}

func convertToADV1Beta(ad *models.ActiveDirectoryV1beta) gcpgenserver.ActiveDirectoryV1beta {
	adResponse := gcpgenserver.ActiveDirectoryV1beta{
		ActiveDirectoryId:           gcpgenserver.NewOptString(ad.ActiveDirectoryID),
		ResourceId:                  *ad.ResourceID,
		Username:                    *ad.Username,
		Password:                    *ad.Password,
		Domain:                      *ad.Domain,
		DNS:                         *ad.DNS,
		NetBIOS:                     *ad.NetBIOS,
		ActiveDirectoryState:        gcpgenserver.NewOptActiveDirectoryV1betaActiveDirectoryState(gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState(ad.ActiveDirectoryState)),
		ActiveDirectoryStateDetails: gcpgenserver.NewOptString(ad.ActiveDirectoryStateDetails),
		CreatedAt:                   gcpgenserver.NewOptDateTime(time.Time(ad.CreatedAt)),
		UpdatedAt:                   gcpgenserver.NewOptDateTime(time.Time(ad.UpdatedAt)),
		SecurityOperators:           ad.SecurityOperators,
		BackupOperators:             ad.BackupOperators,
		Administrators:              ad.Administrators,
	}
	if ad.Description != nil {
		adResponse.Description = gcpgenserver.NewOptString(*ad.Description)
	}
	if ad.AllowLocalNFSUsersWithLdap != nil {
		adResponse.AllowLocalNFSUsersWithLdap = gcpgenserver.NewOptBool(*ad.AllowLocalNFSUsersWithLdap)
	}
	if ad.EncryptDCConnections != nil {
		adResponse.EncryptDCConnections = gcpgenserver.NewOptBool(*ad.EncryptDCConnections)
	}
	if ad.AesEncryption != nil {
		adResponse.AesEncryption = gcpgenserver.NewOptBool(*ad.AesEncryption)
	}
	if ad.LdapSigning != nil {
		adResponse.LdapSigning = gcpgenserver.NewOptBool(*ad.LdapSigning)
	}
	if ad.OrganizationalUnit != nil {
		adResponse.OrganizationalUnit = gcpgenserver.NewOptString(*ad.OrganizationalUnit)
	}
	if ad.Site != nil {
		adResponse.Site = gcpgenserver.NewOptString(*ad.Site)
	}
	if ad.KdcIP != "" {
		adResponse.KdcIP = gcpgenserver.NewOptString(ad.KdcIP)
	}
	if ad.KdcHostname != "" {
		adResponse.KdcHostname = gcpgenserver.NewOptString(ad.KdcHostname)
	}
	return adResponse
}

func mapActiveDirectoryState(state string) gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState {
	switch state {
	case "CREATING":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateCREATING
	case "READY":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY
	case "DELETING":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateDELETING
	case "ERROR":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateERROR
	case "UPDATING":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateUPDATING
	default:
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateSTATEUNSPECIFIED
	}
}

// _getActiveDirectoryFromVCP fetches an active directory by its ID using the orchestrator and converts it to V1Beta format.
func _getActiveDirectoryFromVCP(ctx context.Context, h Handler, activeDirectoryId string) (*gcpgenserver.ActiveDirectoryV1beta, error) {
	logger := util.GetLogger(ctx)
	ad, err := h.Orchestrator.GetActiveDirectory(ctx, activeDirectoryId)
	if err != nil {
		logger.Errorf("Error getting active directory from orchestrator for vcp with error: %v", err)
		return nil, err
	}
	vcpAd := convertOrchestratorActiveDirectoryToV1Beta(ad)
	return &vcpAd, nil
}

// compareADStateHierarchy evaluates and updates the primary Active Directory state based on the hierarchy of two input AD states.
// It prioritizes states according to activeDirectoryStateHierarchy (e.g., "UPDATING" > "ERROR" > "INUSE").
func compareADStateHierarchy(sdeAD, vcpAD *gcpgenserver.ActiveDirectoryV1beta) {
	sdeState := sdeAD.ActiveDirectoryState.Value
	vcpState := vcpAD.ActiveDirectoryState.Value

	sdePriority := getStatePriority(sdeState)
	vcpPriority := getStatePriority(vcpState)

	// Select the state with higher priority (lower index)
	var selectedState gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState

	// If both states are not in hierarchy, keep the original sdeAD state
	if sdePriority == -1 && vcpPriority == -1 {
		return
	}

	// If one state is not in hierarchy, use the other
	if sdePriority == -1 {
		selectedState = vcpState
	} else if vcpPriority == -1 {
		selectedState = sdeState
	} else if sdePriority <= vcpPriority {
		selectedState = sdeState
	} else {
		selectedState = vcpState
	}

	sdeAD.ActiveDirectoryState = gcpgenserver.NewOptActiveDirectoryV1betaActiveDirectoryState(selectedState)
}

// getStatePriority returns the priority index of a state (lower index = higher priority)
// Returns -1 if state is not in the hierarchy
func getStatePriority(state gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState) int {
	for i, hierarchyState := range activeDirectoryStateHierarchy {
		if state == hierarchyState {
			return i
		}
	}
	return -1 // State not found in hierarchy
}

// convertToUpdateParamsForValidation converts the request and params to UpdateActiveDirectoryParams for validation purpose
func convertToUpdateParamsForValidation(req *gcpgenserver.ActiveDirectoryUpdateV1beta, params gcpgenserver.V1betaUpdateActiveDirectoryParams) *common.UpdateActiveDirectoryParams {
	param := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: params.ActiveDirectoryId,
		AccountId:         params.ProjectNumber,
		LocationId:        params.LocationId,
		XCorrelationId:    params.XCorrelationID.Value,
		SecurityOperators: req.SecurityOperators,
		BackupOperators:   req.BackupOperators,
		Administrators:    req.Administrators,
	}

	// Set optional string fields
	setIfPresent := func(opt gcpgenserver.OptString, target **string) {
		if opt.IsSet() {
			val := opt.Value
			*target = &val
		}
	}
	setIfPresent(req.Username, &param.Username)
	setIfPresent(req.Description, &param.Description)
	setIfPresent(req.Password, &param.Password)
	setIfPresent(req.Domain, &param.Domain)
	setIfPresent(req.DNS, &param.DNS)
	setIfPresent(req.NetBIOS, &param.NetBIOS)
	setIfPresent(req.OrganizationalUnit, &param.OrganizationalUnit)
	setIfPresent(req.Site, &param.Site)
	setIfPresent(req.KdcIP, &param.KdcIP)
	setIfPresent(req.KdcHostname, &param.KdcHostname)

	// Set optional bool fields
	setIfPresentBool := func(opt gcpgenserver.OptBool, target **bool) {
		if opt.IsSet() {
			val := opt.Value
			*target = &val
		}
	}
	setIfPresentBool(req.LdapSigning, &param.LdapSigning)
	setIfPresentBool(req.AllowLocalNFSUsersWithLdap, &param.AllowLocalNFSUsersWithLdap)
	setIfPresentBool(req.EncryptDCConnections, &param.EncryptDCConnections)
	setIfPresentBool(req.AesEncryption, &param.AesEncryption)

	return param
}
