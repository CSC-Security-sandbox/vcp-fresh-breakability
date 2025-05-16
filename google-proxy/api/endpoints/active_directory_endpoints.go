package api

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var createClient = cvp.CreateClient

func (h Handler) V1betaCreateActiveDirectory(ctx context.Context, req *gcpgenserver.ActiveDirectoryV1beta, params gcpgenserver.V1betaCreateActiveDirectoryParams) (r gcpgenserver.V1betaCreateActiveDirectoryRes, _ error) {
	logger := util.GetLogger(ctx)
	body := &models.ActiveDirectoryV1beta{
		DNS:                        &req.DNS,
		Domain:                     &req.Domain,
		NetBIOS:                    &req.NetBIOS,
		Username:                   &req.Username,
		Password:                   &req.Password,
		ResourceID:                 &req.ResourceId,
		Administrators:             req.Administrators,
		SecurityOperators:          req.SecurityOperators,
		AesEncryption:              &req.AesEncryption.Value,
		AllowLocalNFSUsersWithLdap: &req.AllowLocalNFSUsersWithLdap.Value,
		BackupOperators:            req.BackupOperators,
		Description:                &req.Description.Value,
		EncryptDCConnections:       &req.EncryptDCConnections.Value,
		KdcIP:                      req.KdcIP.Value,
		KdcHostname:                req.KdcHostname.Value,
		Site:                       &req.Site.Value,
		LdapSigning:                &req.LdapSigning.Value,
		OrganizationalUnit:         &req.OrganizationalUnit.Value,
	}
	createParams := &active_directories.V1betaCreateActiveDirectoryParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		Body:           body,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	created, err := cvpClient.ActiveDirectories.V1betaCreateActiveDirectory(createParams)
	if err != nil {
		switch e := err.(type) {
		case *active_directories.V1betaCreateActiveDirectoryUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateActiveDirectoryUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaCreateActiveDirectoryConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateActiveDirectoryConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaCreateActiveDirectoryBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateActiveDirectoryBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaCreateActiveDirectoryUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateActiveDirectoryUnauthorized{
				Code:    code,
				Message: msg,
			}, nil

		case *active_directories.V1betaCreateActiveDirectoryForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateActiveDirectoryForbidden{
				Code:    code,
				Message: msg,
			}, nil

		case *active_directories.V1betaCreateActiveDirectoryTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateActiveDirectoryTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaCreateActiveDirectoryDefault:
			return &gcpgenserver.V1betaCreateActiveDirectoryInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	if created == nil || created.Payload == nil {
		return &gcpgenserver.V1betaCreateActiveDirectoryInternalServerError{
			Code:    500,
			Message: "unknown error during the create active directory",
		}, nil
	}
	response := convertOperationToOperationV1Beta(created.Payload)
	return response, nil
}

func (h Handler) V1betaDeleteActiveDirectory(ctx context.Context, params gcpgenserver.V1betaDeleteActiveDirectoryParams) (r gcpgenserver.V1betaDeleteActiveDirectoryRes, _ error) {
	logger := util.GetLogger(ctx)
	pathParams := &active_directories.V1betaDeleteActiveDirectoryParams{
		LocationID:        params.LocationId,
		ProjectNumber:     params.ProjectNumber,
		XCorrelationID:    &params.XCorrelationID.Value,
		ActiveDirectoryID: params.ActiveDirectoryId,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	deleted, err := cvpClient.ActiveDirectories.V1betaDeleteActiveDirectory(pathParams)
	if err != nil {
		switch e := err.(type) {
		case *active_directories.V1betaDeleteActiveDirectoryUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteActiveDirectoryUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaDeleteActiveDirectoryConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteActiveDirectoryConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaDeleteActiveDirectoryBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteActiveDirectoryBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaDeleteActiveDirectoryUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteActiveDirectoryUnauthorized{
				Code:    code,
				Message: msg,
			}, nil

		case *active_directories.V1betaDeleteActiveDirectoryForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteActiveDirectoryForbidden{
				Code:    code,
				Message: msg,
			}, nil

		case *active_directories.V1betaDeleteActiveDirectoryTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteActiveDirectoryTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaDeleteActiveDirectoryDefault:
			return &gcpgenserver.V1betaDeleteActiveDirectoryInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	response := convertOperationToOperationV1Beta(deleted.Payload)
	return response, nil
}

func (h Handler) V1betaDescribeActiveDirectory(ctx context.Context, params gcpgenserver.V1betaDescribeActiveDirectoryParams) (r gcpgenserver.V1betaDescribeActiveDirectoryRes, err error) {
	logger := util.GetLogger(ctx)
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
	adV1BetaModel := convertToADV1Beta(resp.Payload)
	return &adV1BetaModel, nil
}

func (h Handler) V1betaGetMultipleActiveDirectories(ctx context.Context, req *gcpgenserver.ActiveDirectoryIdListV1beta, params gcpgenserver.V1betaGetMultipleActiveDirectoriesParams) (r gcpgenserver.V1betaGetMultipleActiveDirectoriesRes, _ error) {
	logger := util.GetLogger(ctx)
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
	adResponse := gcpgenserver.V1betaGetMultipleActiveDirectoriesOK{
		ActiveDirectories: []gcpgenserver.ActiveDirectoryV1beta{},
	}

	for _, ad := range resp.Payload.ActiveDirectories {
		adResponse.ActiveDirectories = append(adResponse.ActiveDirectories, convertToADV1Beta(ad))
	}
	return &adResponse, nil
}

func (h Handler) V1betaUpdateActiveDirectory(ctx context.Context, req *gcpgenserver.ActiveDirectoryUpdateV1beta, params gcpgenserver.V1betaUpdateActiveDirectoryParams) (r gcpgenserver.V1betaUpdateActiveDirectoryRes, _ error) {
	logger := util.GetLogger(ctx)
	body := &models.ActiveDirectoryUpdateV1beta{
		DNS:                        req.DNS.Value,
		Domain:                     req.Domain.Value,
		NetBIOS:                    req.NetBIOS.Value,
		Username:                   req.Username.Value,
		Password:                   req.Password.Value,
		Administrators:             req.Administrators,
		SecurityOperators:          req.SecurityOperators,
		AesEncryption:              &req.AesEncryption.Value,
		AllowLocalNFSUsersWithLdap: &req.AllowLocalNFSUsersWithLdap.Value,
		BackupOperators:            req.BackupOperators,
		Description:                &req.Description.Value,
		EncryptDCConnections:       &req.EncryptDCConnections.Value,
		KdcIP:                      req.KdcIP.Value,
		KdcHostname:                req.KdcHostname.Value,
		Site:                       &req.Site.Value,
		LdapSigning:                &req.LdapSigning.Value,
		OrganizationalUnit:         &req.OrganizationalUnit.Value,
	}
	reqPrams := &active_directories.V1betaUpdateActiveDirectoryParams{
		LocationID:        params.LocationId,
		ProjectNumber:     params.ProjectNumber,
		XCorrelationID:    &params.XCorrelationID.Value,
		ActiveDirectoryID: params.ActiveDirectoryId,
		Body:              body,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	updated, err := cvpClient.ActiveDirectories.V1betaUpdateActiveDirectory(reqPrams)
	if err != nil {
		switch e := err.(type) {
		case *active_directories.V1betaUpdateActiveDirectoryUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateActiveDirectoryUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaUpdateActiveDirectoryNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateActiveDirectoryNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaUpdateActiveDirectoryBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateActiveDirectoryBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaUpdateActiveDirectoryUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateActiveDirectoryUnauthorized{
				Code:    code,
				Message: msg,
			}, nil

		case *active_directories.V1betaUpdateActiveDirectoryForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateActiveDirectoryForbidden{
				Code:    code,
				Message: msg,
			}, nil

		case *active_directories.V1betaUpdateActiveDirectoryTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateActiveDirectoryTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaUpdateActiveDirectoryConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateActiveDirectoryConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *active_directories.V1betaUpdateActiveDirectoryDefault:
			return &gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	if updated == nil || updated.Payload == nil {
		return &gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError{
			Code:    500,
			Message: "unknown error during the update active directory",
		}, nil
	}
	response := convertOperationToOperationV1Beta(updated.Payload)
	return response, nil
}

func (h Handler) V1betaListActiveDirectories(ctx context.Context, params gcpgenserver.V1betaListActiveDirectoriesParams) (gcpgenserver.V1betaListActiveDirectoriesRes, error) {
	logger := util.GetLogger(ctx)
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
	adResponse := gcpgenserver.V1betaListActiveDirectoriesOK{
		ActiveDirectories: []gcpgenserver.ActiveDirectoryV1beta{},
	}
	for _, ad := range resp.Payload.ActiveDirectories {
		adResponse.ActiveDirectories = append(adResponse.ActiveDirectories, convertToADV1Beta(ad))
	}
	return &adResponse, nil
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
