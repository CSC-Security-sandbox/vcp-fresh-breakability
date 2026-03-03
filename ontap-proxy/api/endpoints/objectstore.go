package endpoints

import (
	"context"
	"fmt"
	"net/http"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	ontapproxyutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// V1GetDestinationEndpointInfo implements v1_getDestinationEndpointInfo (GET .../object-stores/{objectStoreId}/endpoints/{destinationEndpointId}).
func (h Handler) V1GetDestinationEndpointInfo(ctx context.Context, params oasgenserver.V1GetDestinationEndpointInfoParams) (oasgenserver.V1GetDestinationEndpointInfoRes, error) {
	logger := util.GetLogger(ctx)
	if !smcOperationEnabled {
		logger.Debug("V1GetDestinationEndpointInfo: SMC operation is disabled")
		return &oasgenserver.V1GetDestinationEndpointInfoBadRequest{Code: 400, Message: "Operation is disabled"}, nil
	}
	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1GetDestinationEndpointInfoInternalServerError{Code: 500, Message: fmt.Sprintf("failed to setup credentials: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1GetDestinationEndpointInfoInternalServerError{Code: 500, Message: fmt.Sprintf("failed to setup certificate/password: %s", err.Error())}, nil
	}
	client, clientErr := newOntapClientFromContext(ctx)
	if clientErr != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", clientErr)
		return &oasgenserver.V1GetDestinationEndpointInfoInternalServerError{Code: 500, Message: fmt.Sprintf("failed to connect to ONTAP: %s", clientErr.Error())}, nil
	}
	path := objectStoreEndpointPath(params.ObjectStoreId.String(), params.DestinationEndpointId.String())
	respBody, statusCode, err := client.ExecuteAPI(ctx, http.MethodGet, path, nil)
	if err != nil {
		logger.ErrorContext(ctx, "ONTAP request failed", "error", err)
		return &oasgenserver.V1GetDestinationEndpointInfoInternalServerError{Code: 500, Message: fmt.Sprintf("ONTAP request failed: %s", err.Error())}, nil
	}
	if statusCode == http.StatusOK {
		var info oasgenserver.ObjectStoreEndpointInfo
		if err := info.UnmarshalJSON(respBody); err != nil {
			logger.ErrorContext(ctx, "Failed to parse ONTAP response", "error", err)
			return &oasgenserver.V1GetDestinationEndpointInfoInternalServerError{Code: 500, Message: fmt.Sprintf("invalid ONTAP response: %s", err.Error())}, nil
		}
		return &info, nil
	}
	errCode, message := ontapproxyutils.ParseOntapErrorBody(respBody)
	if message == "" {
		message = fmt.Sprintf("ONTAP returned status %d", statusCode)
	}
	if errCode == 0 {
		errCode = statusCode
	}
	logger.InfoContext(ctx, fmt.Sprintf("Returning ONTAP error to customer: statusCode=%d message=%s", statusCode, message))
	return nil, &oasgenserver.ErrorStatusCode{StatusCode: statusCode, Response: oasgenserver.Error{Code: errCode, Message: message}}
}

// V1DeleteDestinationEndpoint implements v1_deleteDestinationEndpoint (DELETE .../object-stores/{objectStoreId}/endpoints/{destinationEndpointId}).
func (h Handler) V1DeleteDestinationEndpoint(ctx context.Context, params oasgenserver.V1DeleteDestinationEndpointParams) (oasgenserver.V1DeleteDestinationEndpointRes, error) {
	logger := util.GetLogger(ctx)
	if !smcOperationEnabled {
		logger.Debug("V1DeleteDestinationEndpoint: SMC operation is disabled")
		return &oasgenserver.V1DeleteDestinationEndpointBadRequest{Code: 400, Message: "Operation is disabled"}, nil
	}
	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1DeleteDestinationEndpointInternalServerError{Code: 500, Message: fmt.Sprintf("failed to setup credentials: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1DeleteDestinationEndpointInternalServerError{Code: 500, Message: fmt.Sprintf("failed to setup certificate/password: %s", err.Error())}, nil
	}
	client, clientErr := newOntapClientFromContext(ctx)
	if clientErr != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", clientErr)
		return &oasgenserver.V1DeleteDestinationEndpointInternalServerError{Code: 500, Message: fmt.Sprintf("failed to connect to ONTAP: %s", clientErr.Error())}, nil
	}
	path := objectStoreEndpointPath(params.ObjectStoreId.String(), params.DestinationEndpointId.String())
	respBody, statusCode, err := client.ExecuteAPI(ctx, http.MethodDelete, path, nil)
	if err != nil {
		logger.ErrorContext(ctx, "ONTAP request failed", "error", err)
		return &oasgenserver.V1DeleteDestinationEndpointInternalServerError{Code: 500, Message: fmt.Sprintf("ONTAP request failed: %s", err.Error())}, nil
	}
	if statusCode == http.StatusOK || statusCode == http.StatusAccepted {
		var jobResp oasgenserver.ObjectStoreEndpointInfoJobLinkResponse
		if err := jobResp.UnmarshalJSON(respBody); err != nil {
			logger.ErrorContext(ctx, "Failed to parse ONTAP response", "error", err)
			return &oasgenserver.V1DeleteDestinationEndpointInternalServerError{Code: 500, Message: fmt.Sprintf("invalid ONTAP response: %s", err.Error())}, nil
		}
		if statusCode == http.StatusAccepted {
			return (*oasgenserver.V1DeleteDestinationEndpointAccepted)(&jobResp), nil
		}
		return (*oasgenserver.V1DeleteDestinationEndpointOK)(&jobResp), nil
	}
	errCode, message := ontapproxyutils.ParseOntapErrorBody(respBody)
	if message == "" {
		message = fmt.Sprintf("ONTAP returned status %d", statusCode)
	}
	if errCode == 0 {
		errCode = statusCode
	}
	logger.InfoContext(ctx, fmt.Sprintf("Returning ONTAP error to customer: statusCode=%d message=%s", statusCode, message))
	return nil, &oasgenserver.ErrorStatusCode{StatusCode: statusCode, Response: oasgenserver.Error{Code: errCode, Message: message}}
}

// V1GetSnapshots implements v1_getSnapshots (GET .../object-stores/.../endpoints/.../snapshots).
func (h Handler) V1GetSnapshots(ctx context.Context, params oasgenserver.V1GetSnapshotsParams) (oasgenserver.V1GetSnapshotsRes, error) {
	logger := util.GetLogger(ctx)
	if !smcOperationEnabled {
		logger.Debug("V1GetSnapshots: SMC operation is disabled")
		return &oasgenserver.V1GetSnapshotsBadRequest{Code: 400, Message: "Operation is disabled"}, nil
	}
	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1GetSnapshotsInternalServerError{Code: 500, Message: fmt.Sprintf("failed to setup credentials: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1GetSnapshotsInternalServerError{Code: 500, Message: fmt.Sprintf("failed to setup certificate/password: %s", err.Error())}, nil
	}
	client, clientErr := newOntapClientFromContext(ctx)
	if clientErr != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", clientErr)
		return &oasgenserver.V1GetSnapshotsInternalServerError{Code: 500, Message: fmt.Sprintf("failed to connect to ONTAP: %s", clientErr.Error())}, nil
	}
	path := objectStoreSnapshotPath(params.ObjectStoreId.String(), params.DestinationEndpointId.String(), "")
	respBody, statusCode, err := client.ExecuteAPI(ctx, http.MethodGet, path, nil)
	if err != nil {
		logger.ErrorContext(ctx, "ONTAP request failed", "error", err)
		return &oasgenserver.V1GetSnapshotsInternalServerError{Code: 500, Message: fmt.Sprintf("ONTAP request failed: %s", err.Error())}, nil
	}
	if statusCode == http.StatusOK {
		var snapResp oasgenserver.SnapmirrorObjectStoreEndpointSnapshotResponse
		if err := snapResp.UnmarshalJSON(respBody); err != nil {
			logger.ErrorContext(ctx, "Failed to parse ONTAP response", "error", err)
			return &oasgenserver.V1GetSnapshotsInternalServerError{Code: 500, Message: fmt.Sprintf("invalid ONTAP response: %s", err.Error())}, nil
		}
		return &snapResp, nil
	}
	errCode, message := ontapproxyutils.ParseOntapErrorBody(respBody)
	if message == "" {
		message = fmt.Sprintf("ONTAP returned status %d", statusCode)
	}
	if errCode == 0 {
		errCode = statusCode
	}
	logger.InfoContext(ctx, fmt.Sprintf("Returning ONTAP error to customer: statusCode=%d message=%s", statusCode, message))
	return nil, &oasgenserver.ErrorStatusCode{StatusCode: statusCode, Response: oasgenserver.Error{Code: errCode, Message: message}}
}

// V1DeleteSnapshot implements v1_deleteSnapshot (DELETE .../object-stores/.../endpoints/.../snapshots/{snapshotId}).
func (h Handler) V1DeleteSnapshot(ctx context.Context, params oasgenserver.V1DeleteSnapshotParams) (oasgenserver.V1DeleteSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	if !smcOperationEnabled {
		logger.Debug("V1DeleteSnapshot: SMC operation is disabled")
		return &oasgenserver.V1DeleteSnapshotBadRequest{Code: 400, Message: "Operation is disabled"}, nil
	}
	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1DeleteSnapshotInternalServerError{Code: 500, Message: fmt.Sprintf("failed to setup credentials: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1DeleteSnapshotInternalServerError{Code: 500, Message: fmt.Sprintf("failed to setup certificate/password: %s", err.Error())}, nil
	}
	client, clientErr := newOntapClientFromContext(ctx)
	if clientErr != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", clientErr)
		return &oasgenserver.V1DeleteSnapshotInternalServerError{Code: 500, Message: fmt.Sprintf("failed to connect to ONTAP: %s", clientErr.Error())}, nil
	}
	path := objectStoreSnapshotPath(params.ObjectStoreId.String(), params.DestinationEndpointId.String(), params.SnapshotId.String())
	respBody, statusCode, err := client.ExecuteAPI(ctx, http.MethodDelete, path, nil)
	if err != nil {
		logger.ErrorContext(ctx, "ONTAP request failed", "error", err)
		return &oasgenserver.V1DeleteSnapshotInternalServerError{Code: 500, Message: fmt.Sprintf("ONTAP request failed: %s", err.Error())}, nil
	}
	if statusCode == http.StatusOK || statusCode == http.StatusAccepted {
		var jobResp oasgenserver.SnapmirrorObjectStoreEndpointSnapshotJobLinkResponse
		if err := jobResp.UnmarshalJSON(respBody); err != nil {
			logger.ErrorContext(ctx, "Failed to parse ONTAP response", "error", err)
			return &oasgenserver.V1DeleteSnapshotInternalServerError{Code: 500, Message: fmt.Sprintf("invalid ONTAP response: %s", err.Error())}, nil
		}
		if statusCode == http.StatusAccepted {
			return (*oasgenserver.V1DeleteSnapshotAccepted)(&jobResp), nil
		}
		return (*oasgenserver.V1DeleteSnapshotOK)(&jobResp), nil
	}
	errCode, message := ontapproxyutils.ParseOntapErrorBody(respBody)
	if message == "" {
		message = fmt.Sprintf("ONTAP returned status %d", statusCode)
	}
	if errCode == 0 {
		errCode = statusCode
	}
	logger.InfoContext(ctx, fmt.Sprintf("Returning ONTAP error to customer: statusCode=%d message=%s", statusCode, message))
	return nil, &oasgenserver.ErrorStatusCode{StatusCode: statusCode, Response: oasgenserver.Error{Code: errCode, Message: message}}
}

func objectStoreEndpointPath(objectStoreId, destinationEndpointId string) string {
	return fmt.Sprintf("/api/snapmirror/object-stores/%s/endpoints/%s", objectStoreId, destinationEndpointId)
}

func objectStoreSnapshotPath(objectStoreId, destinationEndpointId, snapshotId string) string {
	if snapshotId != "" {
		return fmt.Sprintf("/api/snapmirror/object-stores/%s/endpoints/%s/snapshots/%s", objectStoreId, destinationEndpointId, snapshotId)
	}
	return fmt.Sprintf("/api/snapmirror/object-stores/%s/endpoints/%s/snapshots", objectStoreId, destinationEndpointId)
}
