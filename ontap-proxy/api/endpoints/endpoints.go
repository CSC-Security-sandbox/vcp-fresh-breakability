package endpoints

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/reverseproxy"
	ontapproxyutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	snapLockOperationEnabled   = env.GetBool("SNAPLOCK_OPERATION_ENABLED", false)
	privateCliOperationEnabled = env.GetBool("PRIVATE_CLI_OPERATION_ENABLED", false)
	smcOperationEnabled        = env.GetBool("SMC_OPERATION_ENABLED", false)

	setupCredentialsForHandler  = middleware.SetupCredentialsForHandler
	ensureCertificateOrPassword = middleware.EnsureCertificateOrPassword
	newOntapClientFromContext   = handlers.NewOntapClientFromContext
)

type Handler struct {
	oasgenserver.UnimplementedHandler
}

func (h Handler) GetHealth(ctx context.Context) (oasgenserver.GetHealthRes, error) {
	return &oasgenserver.Health{}, nil
}

func (h Handler) GetCacheStatus(ctx context.Context) (oasgenserver.GetCacheStatusRes, error) {
	// Get auth data cache entries
	authCacheEntries := cache.GetAuthDataCacheStatus()

	// Get client cache entries
	clientCacheEntries := reverseproxy.GetGlobalClientCacheStatus()

	// Combine both caches into the response
	// Auth cache keys: "projectNumber:poolID:userName"
	// Client cache keys: "accountName:poolID:authType:username[:certID]"
	totalEntries := len(authCacheEntries) + len(clientCacheEntries)
	entries := make([]oasgenserver.CacheEntry, 0, totalEntries)

	// Add auth cache entries
	for _, entry := range authCacheEntries {
		entries = append(entries, oasgenserver.CacheEntry{
			CacheKey:  oasgenserver.NewOptString("auth:" + entry.CacheKey),
			CachedAt:  oasgenserver.NewOptDateTime(entry.CachedAt),
			ExpiresAt: oasgenserver.NewOptDateTime(entry.ExpiresAt),
		})
	}

	// Add client cache entries
	for _, entry := range clientCacheEntries {
		entries = append(entries, oasgenserver.CacheEntry{
			CacheKey:  oasgenserver.NewOptString("client:" + entry.CacheKey),
			CachedAt:  oasgenserver.NewOptDateTime(entry.CachedAt),
			ExpiresAt: oasgenserver.NewOptDateTime(entry.ExpiresAt),
		})
	}

	return &oasgenserver.CacheStatus{
		Entries:      entries,
		TotalEntries: oasgenserver.NewOptInt(len(entries)),
	}, nil
}

// SnaplockFileDelete implements the snaplockFileDelete operation.
// DELETE /v1beta/projects/{projectNumber}/locations/{locationId}/pools/{poolId}/ontap/api/storage/snaplock/file/{volumeUuid}/{filePath}
//
// This handler:
// 1. Sets up admin credentials using params from ogen
// 2. Ensures certificate/password is fetched from secret manager
// 3. Gets ONTAP client using cached auth data
// 4. Retrieves volume info to get volume name and SVM name
// 5. Executes CLI command for privileged delete
// 6. Returns typed response
func (h Handler) SnaplockFileDelete(
	ctx context.Context,
	params oasgenserver.SnaplockFileDeleteParams,
) (oasgenserver.SnaplockFileDeleteRes, error) {
	logger := util.GetLogger(ctx)

	if !snapLockOperationEnabled {
		logger.Debug("SnaplockFileDelete: operation is disabled")
		return &oasgenserver.SnaplockFileDeleteBadRequest{
			Code:    400,
			Message: "Snaplock file delete operation is disabled",
		}, nil
	}

	logger.InfoContext(ctx, "Processing snaplock file delete request",
		"projectNumber", params.ProjectNumber,
		"poolId", params.PoolId.String(),
		"volumeUuid", params.VolumeUuid.String(),
		"filePath", params.FilePath,
	)

	// 1. Setup credentials (admin for snaplock operations)
	ctx, err := middleware.SetupCredentialsForHandler(
		ctx,
		params.ProjectNumber,
		params.PoolId.String(),
		middleware.CredentialTypeAdmin,
	)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.SnaplockFileDeleteUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	// 2. Ensure certificate/password is fetched
	if err := middleware.EnsureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.SnaplockFileDeleteUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	// 3. Get ONTAP client (uses auth data from context)
	ontapClient, err := handlers.NewOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.SnaplockFileDeleteInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	// 4. Get volume info (need volume name and SVM name for CLI command)
	volumeInfo, err := ontapClient.GetVolume(ctx, params.VolumeUuid.String())
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get volume info",
			"volumeUuid", params.VolumeUuid.String(),
			"error", err,
		)
		return &oasgenserver.SnaplockFileDeleteNotFound{
			Code:    404,
			Message: fmt.Sprintf("volume not found: %s", err.Error()),
		}, nil
	}

	if volumeInfo.Name == "" || volumeInfo.SVM.Name == "" {
		logger.ErrorContext(ctx, "Volume info incomplete",
			"volumeUuid", params.VolumeUuid.String(),
			"volumeName", volumeInfo.Name,
			"svmName", volumeInfo.SVM.Name,
		)
		return &oasgenserver.SnaplockFileDeleteInternalServerError{
			Code:    500,
			Message: "volume information is incomplete",
		}, nil
	}

	// 5. Build and execute CLI command
	filePath := strings.TrimPrefix(params.FilePath, "/")
	fullFilePath := fmt.Sprintf("/vol/%s/%s", volumeInfo.Name, filePath)
	cliCommand := handlers.BuildSnaplockDeleteCommand(fullFilePath, volumeInfo.SVM.Name)

	logger.InfoContext(ctx, "Executing snaplock privileged delete",
		"volumeUuid", params.VolumeUuid.String(),
		"volumeName", volumeInfo.Name,
		"svmName", volumeInfo.SVM.Name,
		"filePath", fullFilePath,
	)

	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed",
			"volumeUuid", params.VolumeUuid.String(),
			"error", err,
		)

		// Check if it's an ONTAP CLI error with structured response
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			return &oasgenserver.SnaplockFileDeleteBadRequest{
				Code:    handlers.OntapCodeToInt(cliErr.Code),
				Message: cliErr.Message,
			}, nil
		}
		return &oasgenserver.SnaplockFileDeleteInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error()),
		}, nil
	}

	// 6. Check CLI success
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Snaplock delete operation failed",
			"volumeUuid", params.VolumeUuid.String(),
			"filePath", fullFilePath,
			"cliOutput", cliResponse.Output,
		)
		return &oasgenserver.SnaplockFileDeleteBadRequest{
			Code:    400,
			Message: message,
		}, nil
	}

	// 7. Return typed response
	logger.InfoContext(ctx, "Snaplock file delete completed successfully",
		"volumeUuid", params.VolumeUuid.String(),
		"volumeName", volumeInfo.Name,
		"filePath", fullFilePath,
	)

	return &oasgenserver.SnaplockFileRetentionJobLinkResponse{
		NumRecords: oasgenserver.NewOptInt(1),
		Records: []oasgenserver.SnaplockFileRetention{
			{
				File: oasgenserver.NewOptFileInfo(oasgenserver.FileInfo{
					Path: oasgenserver.NewOptString("/" + filePath),
				}),
				Volume: oasgenserver.NewOptVolumeRef(oasgenserver.VolumeRef{
					UUID: oasgenserver.NewOptString(params.VolumeUuid.String()),
					Name: oasgenserver.NewOptString(volumeInfo.Name),
				}),
			},
		},
	}, nil
}

// V1ClusterLicensingAccessTokensCreate implements v1_clusterLicensingAccessTokensCreate (POST /api/cluster/licensing/access_tokens).
// Uses admin credentials and forwards the request to ONTAP SM-C access_tokens.
func (h Handler) V1ClusterLicensingAccessTokensCreate(
	ctx context.Context,
	req *oasgenserver.AccessTokenRequest,
	params oasgenserver.V1ClusterLicensingAccessTokensCreateParams,
) (oasgenserver.V1ClusterLicensingAccessTokensCreateRes, error) {
	logger := util.GetLogger(ctx)

	logger.InfoContext(ctx, "Processing cluster licensing access token request",
		"projectNumber", params.ProjectNumber,
		"poolId", params.PoolId.String(),
	)

	if !smcOperationEnabled {
		logger.Debug("V1ClusterLicensingAccessTokensCreate: SMC operation is disabled")
		return &oasgenserver.V1ClusterLicensingAccessTokensCreateBadRequest{
			Code:    400,
			Message: "Operation is disabled",
		}, nil
	}

	ctx, err := setupCredentialsForHandler(
		ctx,
		params.ProjectNumber,
		params.PoolId.String(),
		middleware.CredentialTypeAdmin,
	)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to setup credentials: %s", err.Error()),
		}, nil
	}

	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to setup certificate/password: %s", err.Error()),
		}, nil
	}

	bodyBytes, err := json.Marshal(req)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to marshal request", "error", err)
		return &oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError{
			Code:    500,
			Message: "Failed to serialize request",
		}, nil
	}

	ontapClient, clientErr := newOntapClientFromContext(ctx)
	if clientErr != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", clientErr)
		return &oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", clientErr.Error()),
		}, nil
	}
	respBody, statusCode, err := ontapClient.ExecuteAPI(ctx, http.MethodPost, "/api/cluster/licensing/access_tokens", bodyBytes)
	if err != nil {
		logger.ErrorContext(ctx, "ONTAP request failed", "error", err)
		return &oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("ONTAP request failed: %s", err.Error()),
		}, nil
	}

	if statusCode == http.StatusOK {
		var accessResp oasgenserver.AccessTokenInfo
		if err := json.Unmarshal(respBody, &accessResp); err != nil {
			logger.ErrorContext(ctx, "Failed to parse ONTAP response", "error", err)
			return &oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError{
				Code:    500,
				Message: fmt.Sprintf("invalid ONTAP response: %s", err.Error()),
			}, nil
		}
		return &accessResp, nil
	}

	errCode, message := ontapproxyutils.ParseOntapErrorBody(respBody)
	if message == "" {
		message = fmt.Sprintf("ONTAP returned status %d", statusCode)
	}
	if errCode == 0 {
		errCode = statusCode
	}
	logger.InfoContext(ctx, fmt.Sprintf("Returning ONTAP error to customer: statusCode=%d message=%s", statusCode, message))
	return nil, &oasgenserver.ErrorStatusCode{
		StatusCode: statusCode,
		Response:   oasgenserver.Error{Code: errCode, Message: message},
	}
}
