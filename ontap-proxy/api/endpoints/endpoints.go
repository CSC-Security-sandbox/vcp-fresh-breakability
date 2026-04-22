package endpoints

import (
	"context"
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

const snaplockIAMRoleRequiredMessage = "user is not authorized for this operation"

var (
	snapLockOperationEnabled     = env.GetBool("SNAPLOCK_OPERATION_ENABLED", false)
	privateCliOperationEnabled   = env.GetBool("PRIVATE_CLI_OPERATION_ENABLED", false)
	advancedModeAllowlistEnabled = env.GetBool("ADVANCED_MODE_ALLOWLIST_ENABLED", false)

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

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.PrivilegedDeleteRole) {
		return &oasgenserver.SnaplockFileDeleteForbidden{
			Code:    http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	logger.InfoContext(ctx, "Processing snaplock file delete request",
		"projectNumber", params.ProjectNumber,
		"poolId", params.PoolId.String(),
		"volumeUuid", params.VolumeUuid.String(),
		"filePath", params.FilePath,
	)

	// 1. Setup credentials (admin for snaplock operations)
	ctx, err := setupCredentialsForHandler(
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
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.SnaplockFileDeleteUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	// Add backend metrics context so OntapClient records to ontap_proxy_backend_* (same as passthrough)
	ctx = middleware.AddBackendMetricsToContext(ctx, params.ProjectNumber, params.PoolId.String(), "/api/storage/snaplock/file")

	// 3. Get ONTAP client (uses auth data from context)
	ontapClient, err := newOntapClientFromContext(ctx)
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
		return &oasgenserver.SnaplockFileDeleteBadRequest{
			Code:    400,
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

// V1SnaplockLitigationBegin implements snaplockLitigationBegin. Uses ONTAP CLI `snaplock legal-hold begin`.
func (h Handler) V1SnaplockLitigationBegin(
	ctx context.Context,
	req *oasgenserver.SnaplockLitigationBeginRequest,
	params oasgenserver.V1SnaplockLitigationBeginParams,
) (oasgenserver.V1SnaplockLitigationBeginRes, error) {
	logger := util.GetLogger(ctx)

	if !snapLockOperationEnabled {
		logger.Debug("V1SnaplockLitigationBegin: operation is disabled")
		return &oasgenserver.V1SnaplockLitigationBeginBadRequest{
			Code:    400,
			Message: "Snaplock litigation operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1SnaplockLitigationBeginForbidden{
			Code:    http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	if req == nil || strings.TrimSpace(req.LitigationName) == "" || strings.TrimSpace(req.Path) == "" {
		return &oasgenserver.V1SnaplockLitigationBeginBadRequest{
			Code:    400,
			Message: "litigation_name, path, and volume (name or uuid) are required",
		}, nil
	}
	var volumeName string
	var volumeUUID string
	vol := req.Volume
	if vol.Name.IsSet() && strings.TrimSpace(vol.Name.Value) != "" {
		volumeName = strings.TrimSpace(vol.Name.Value)
	}
	if vol.UUID.IsSet() {
		volumeUUID = vol.UUID.Value.String()
	}
	if volumeName == "" && volumeUUID == "" {
		return &oasgenserver.V1SnaplockLitigationBeginBadRequest{
			Code:    400,
			Message: "volume (name or uuid) is required",
		}, nil
	}

	logger.InfoContext(ctx, "Processing snaplock litigation begin",
		"projectNumber", params.ProjectNumber,
		"poolId", params.PoolId.String(),
		"litigationName", req.LitigationName,
		"path", req.Path,
		"volumeName", volumeName,
		"volumeUuid", volumeUUID,
	)

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1SnaplockLitigationBeginUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1SnaplockLitigationBeginUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1SnaplockLitigationBeginInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	// Resolve to volume info: by UUID (GetVolume) or by name (ListVolumesWithSvm), same pattern as EBROperationCreate.
	var volumeInfo *handlers.VolumeInfo
	if volumeUUID != "" {
		info, err := ontapClient.GetVolume(ctx, volumeUUID)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get volume info", "volumeUuid", volumeUUID, "error", err)
			return &oasgenserver.V1SnaplockLitigationBeginNotFound{
				Code:    404,
				Message: fmt.Sprintf("volume not found: %s", err.Error()),
			}, nil
		}
		volumeInfo = info
	} else {
		volumes, err := ontapClient.ListVolumesWithSvm(ctx, 1000)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to list volumes", "error", err)
			return &oasgenserver.V1SnaplockLitigationBeginInternalServerError{
				Code:    500,
				Message: fmt.Sprintf("failed to list volumes: %s", err.Error()),
			}, nil
		}
		for i := range volumes {
			if volumes[i].Name == volumeName {
				volumeInfo = &volumes[i]
				break
			}
		}
		if volumeInfo == nil {
			return &oasgenserver.V1SnaplockLitigationBeginNotFound{
				Code:    404,
				Message: fmt.Sprintf("volume not found: %s", volumeName),
			}, nil
		}
	}
	if volumeInfo.Name == "" || volumeInfo.SVM.Name == "" {
		return &oasgenserver.V1SnaplockLitigationBeginBadRequest{
			Code:    400,
			Message: "volume information is incomplete",
		}, nil
	}
	volumeUUID = volumeInfo.UUID

	cliCommand := handlers.BuildSnaplockLegalHoldBeginCommand(req.LitigationName, volumeInfo.Name, req.Path, volumeInfo.SVM.Name)
	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			ontapCode := 400
			if _, _ = fmt.Sscanf(cliErr.Code, "%d", &ontapCode); ontapCode == 0 {
				ontapCode = 400
			}
			return &oasgenserver.V1SnaplockLitigationBeginBadRequest{
				Code:    ontapCode,
				Message: cliErr.Message,
			}, nil
		}
		return &oasgenserver.V1SnaplockLitigationBeginInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error()),
		}, nil
	}

	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Snaplock Litigation Begin Failed",
			"volumeUuid", volumeInfo.UUID,
			"cliResponse", cliResponse.Output)
		return &oasgenserver.V1SnaplockLitigationBeginBadRequest{
			Code:    400,
			Message: message,
		}, nil
	}

	return &oasgenserver.SnaplockLitigationResponse{
		ID:   oasgenserver.NewOptString(volumeUUID + ":" + req.LitigationName),
		Name: oasgenserver.NewOptString(req.LitigationName),
		Path: oasgenserver.NewOptString(req.Path),
	}, nil
}

// V1SnaplockLitigationCollectionGet implements snaplockLitigationCollectionGet. Uses ONTAP CLI `snaplock legal-hold show` per volume.
func (h Handler) V1SnaplockLitigationCollectionGet(
	ctx context.Context,
	params oasgenserver.V1SnaplockLitigationCollectionGetParams,
) (oasgenserver.V1SnaplockLitigationCollectionGetRes, error) {
	logger := util.GetLogger(ctx)

	if !snapLockOperationEnabled {
		logger.Debug("V1SnaplockLitigationCollectionGet: operation is disabled")
		return &oasgenserver.V1SnaplockLitigationCollectionGetBadRequest{
			Code:    400,
			Message: "Snaplock litigation operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1SnaplockLitigationCollectionGetForbidden{
			Code:    http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1SnaplockLitigationCollectionGetUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1SnaplockLitigationCollectionGetUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1SnaplockLitigationCollectionGetInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	volumes, err := ontapClient.ListVolumesWithSvm(ctx, 1000)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to list volumes for litigation discovery", "error", err)
		return &oasgenserver.V1SnaplockLitigationCollectionGetInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to list volumes: %s", err.Error()),
		}, nil
	}

	// One record per operation block (matches CLI "snaplock legal-hold show" table).
	var listRecords []oasgenserver.SnaplockLitigationListRecord
	for _, vol := range volumes {
		if vol.SVM.Name == "" || vol.Name == "" {
			continue
		}
		cliCommand := handlers.BuildSnaplockLegalHoldShowCommand(vol.SVM.Name, vol.Name)
		cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
		if err != nil {
			logger.WarnContext(ctx, "CLI legal-hold show failed for volume, skipping", "volume", vol.Name, "error", err)
			continue
		}
		if !handlers.IsCLISuccess(cliResponse.Output) {
			continue
		}
		ops := ontapproxyutils.ParseSnaplockLegalHoldShowInstanceOutputToOperations(cliResponse.Output)
		for _, op := range ops {
			litName := op.LitigationName
			if litName == "" {
				continue
			}
			id := vol.UUID + ":" + litName
			path := op.Path
			if path == "" {
				path = "/"
			}
			opType := oasgenserver.SnaplockLitigationListRecordOperationBegin
			if op.OperationType == "end" {
				opType = oasgenserver.SnaplockLitigationListRecordOperationEnd
			}
			stateStr := ontapproxyutils.MapOperationStatusToState(op.Status)
			status := oasgenserver.SnaplockLitigationListRecordStatusInProgress
			switch stateStr {
			case "completed":
				status = oasgenserver.SnaplockLitigationListRecordStatusCompleted
			case "failed":
				status = oasgenserver.SnaplockLitigationListRecordStatusFailed
			case "aborting":
				status = oasgenserver.SnaplockLitigationListRecordStatusAborting
			case "in_progress":
				status = oasgenserver.SnaplockLitigationListRecordStatusInProgress
			}
			listRecords = append(listRecords, oasgenserver.SnaplockLitigationListRecord{
				ID:          oasgenserver.NewOptString(id),
				Name:        oasgenserver.NewOptString(litName),
				Path:        oasgenserver.NewOptString(path),
				Operation:   oasgenserver.NewOptSnaplockLitigationListRecordOperation(opType),
				OperationID: oasgenserver.NewOptInt(op.OperationID),
				Vserver:     oasgenserver.NewOptString(vol.SVM.Name),
				Volume:      oasgenserver.NewOptString(vol.Name),
				Status:      oasgenserver.NewOptSnaplockLitigationListRecordStatus(status),
			})
		}
	}

	out := &oasgenserver.SnaplockLitigationListResponse{
		Records:    listRecords,
		NumRecords: oasgenserver.NewOptInt(len(listRecords)),
	}
	return out, nil
}

// V1SnaplockLitigationEnd implements snaplockLitigationEnd. Uses ONTAP CLI `snaplock legal-hold end`.
func (h Handler) V1SnaplockLitigationEnd(
	ctx context.Context,
	params oasgenserver.V1SnaplockLitigationEndParams,
) (oasgenserver.V1SnaplockLitigationEndRes, error) {
	logger := util.GetLogger(ctx)

	if !snapLockOperationEnabled {
		logger.Debug("V1SnaplockLitigationEnd: operation is disabled")
		return &oasgenserver.V1SnaplockLitigationEndBadRequest{
			Code:    400,
			Message: "Snaplock litigation operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1SnaplockLitigationEndForbidden{
			Code:    http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	parts := strings.SplitN(params.LitigationId, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return &oasgenserver.V1SnaplockLitigationEndBadRequest{
			Code:    400,
			Message: "litigation_id must be volumeUuid:litigationName",
		}, nil
	}
	volumeUUID, litigationName := parts[0], parts[1]

	logger.InfoContext(ctx, "Processing snaplock litigation end",
		"projectNumber", params.ProjectNumber,
		"poolId", params.PoolId.String(),
		"litigationId", params.LitigationId,
	)

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1SnaplockLitigationEndUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1SnaplockLitigationEndUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1SnaplockLitigationEndInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	volumeInfo, err := ontapClient.GetVolume(ctx, volumeUUID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get volume info", "volumeUuid", volumeUUID, "error", err)
		return &oasgenserver.V1SnaplockLitigationEndNotFound{
			Code:    404,
			Message: fmt.Sprintf("volume not found: %s", err.Error()),
		}, nil
	}
	if volumeInfo.Name == "" || volumeInfo.SVM.Name == "" {
		return &oasgenserver.V1SnaplockLitigationEndBadRequest{
			Code:    400,
			Message: "volume information is incomplete",
		}, nil
	}

	cliCommand := handlers.BuildSnaplockLegalHoldEndPathCommand(litigationName, volumeInfo.Name, "/", volumeInfo.SVM.Name)
	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			ontapCode := 400
			if _, _ = fmt.Sscanf(cliErr.Code, "%d", &ontapCode); ontapCode == 0 {
				ontapCode = 400
			}
			return &oasgenserver.V1SnaplockLitigationEndBadRequest{
				Code:    ontapCode,
				Message: cliErr.Message,
			}, nil
		}
		return &oasgenserver.V1SnaplockLitigationEndInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error()),
		}, nil
	}

	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Snaplock Litigation End Failed",
			"volumeUuid", volumeUUID,
			"cliResponse", cliResponse.Output)
		return &oasgenserver.V1SnaplockLitigationEndBadRequest{
			Code:    400,
			Message: message,
		}, nil
	}

	return &oasgenserver.V1SnaplockLitigationEndOK{}, nil
}

// V1SnaplockLitigationGet implements snaplockLitigationGet. Uses ONTAP CLI `snaplock legal-hold show` with -litigation-name.
func (h Handler) V1SnaplockLitigationGet(
	ctx context.Context,
	params oasgenserver.V1SnaplockLitigationGetParams,
) (oasgenserver.V1SnaplockLitigationGetRes, error) {
	logger := util.GetLogger(ctx)

	if !snapLockOperationEnabled {
		logger.Debug("V1SnaplockLitigationGet: operation is disabled")
		return &oasgenserver.V1SnaplockLitigationGetBadRequest{
			Code:    400,
			Message: "Snaplock litigation operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1SnaplockLitigationGetForbidden{
			Code:    http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	parts := strings.SplitN(params.LitigationId, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return &oasgenserver.V1SnaplockLitigationGetBadRequest{
			Code:    400,
			Message: "litigation_id must be volumeUuid:litigationName",
		}, nil
	}
	volumeUUID, litigationName := parts[0], parts[1]

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1SnaplockLitigationGetUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1SnaplockLitigationGetUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1SnaplockLitigationGetInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	vol, err := ontapClient.GetVolume(ctx, volumeUUID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get volume info", "volumeUuid", volumeUUID, "error", err)
		return &oasgenserver.V1SnaplockLitigationGetNotFound{
			Code:    404,
			Message: fmt.Sprintf("volume lookup failed: %s", err.Error()),
		}, nil
	}
	if vol.Name == "" || vol.SVM.Name == "" {
		return &oasgenserver.V1SnaplockLitigationGetBadRequest{
			Code:    400,
			Message: "volume information is incomplete",
		}, nil
	}

	cmd := handlers.BuildSnaplockLegalHoldShowForLitigationCommand(vol.SVM.Name, vol.Name, litigationName)
	cliResp, err := ontapClient.ExecuteCLI(ctx, cmd, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "Litigation get CLI failed", "error", err)
		return &oasgenserver.V1SnaplockLitigationGetInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("litigation get CLI failed: %s", err.Error()),
		}, nil
	}
	if !handlers.IsCLISuccess(cliResp.Output) {
		msg := handlers.ParseCLIError(cliResp.Output)
		logger.WarnContext(ctx, "Litigation not found or CLI error", "cliOutput", cliResp.Output)
		return &oasgenserver.V1SnaplockLitigationGetNotFound{
			Code:    404,
			Message: msg,
		}, nil
	}
	records, err := ontapproxyutils.ParseSnaplockLegalHoldShowInstanceOutput(cliResp.Output)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to parse litigation CLI output", "error", err)
		return &oasgenserver.V1SnaplockLitigationGetInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to parse CLI output: %s", err.Error()),
		}, nil
	}
	if len(records) == 0 {
		return &oasgenserver.V1SnaplockLitigationGetNotFound{
			Code:    404,
			Message: fmt.Sprintf("litigation not found: %s", litigationName),
		}, nil
	}
	path := records[0].Path
	if path == "" {
		path = "/"
	}
	litID := volumeUUID + ":" + litigationName

	// Parse operation details (Operation ID, Status, Path, Type, NumFilesProcessed, etc.) from same CLI output.
	opRecords := ontapproxyutils.ParseSnaplockLegalHoldShowInstanceOutputToOperations(cliResp.Output)
	oasOps := make([]oasgenserver.SnaplockLegalHoldOperationResponse, 0, len(opRecords))
	for _, rec := range opRecords {
		opPath := rec.Path
		if opPath == "" {
			opPath = "/"
		}
		opType := rec.OperationType
		if opType == "" {
			opType = "begin"
		}
		stateStr := ontapproxyutils.MapOperationStatusToState(rec.Status)
		opState := oasgenserver.SnaplockLegalHoldOperationResponseState(stateStr)
		apiType := oasgenserver.SnaplockLegalHoldOperationResponseTypeBegin
		if opType == "end" {
			apiType = oasgenserver.SnaplockLegalHoldOperationResponseTypeEnd
		}
		op := oasgenserver.SnaplockLegalHoldOperationResponse{}
		op.SetID(oasgenserver.NewOptInt(rec.OperationID))
		op.SetState(oasgenserver.NewOptSnaplockLegalHoldOperationResponseState(opState))
		op.SetPath(oasgenserver.NewOptString(opPath))
		op.SetType(oasgenserver.NewOptSnaplockLegalHoldOperationResponseType(apiType))
		if rec.NumFilesProcessed != "" {
			op.SetNumFilesProcessed(oasgenserver.NewOptString(rec.NumFilesProcessed))
		}
		if rec.NumFilesFailed != "" {
			op.SetNumFilesFailed(oasgenserver.NewOptString(rec.NumFilesFailed))
		}
		if rec.NumFilesSkipped != "" {
			op.SetNumFilesSkipped(oasgenserver.NewOptString(rec.NumFilesSkipped))
		}
		if rec.NumInodesIgnored != "" {
			op.SetNumInodesIgnored(oasgenserver.NewOptString(rec.NumInodesIgnored))
		}
		oasOps = append(oasOps, op)
	}

	lit := &oasgenserver.SnaplockLitigationResponse{}
	lit.SetID(oasgenserver.NewOptString(litID))
	lit.SetName(oasgenserver.NewOptString(litigationName))
	lit.SetPath(oasgenserver.NewOptString(path))
	lit.SetSvm(oasgenserver.NewOptSnaplockLitigationResponseSvm(oasgenserver.SnaplockLitigationResponseSvm{
		Name: oasgenserver.NewOptString(vol.SVM.Name),
		UUID: oasgenserver.NewOptString(vol.SVM.UUID),
	}))
	lit.SetVolume(oasgenserver.NewOptSnaplockLitigationResponseVolume(oasgenserver.SnaplockLitigationResponseVolume{
		Name: oasgenserver.NewOptString(vol.Name),
		UUID: oasgenserver.NewOptString(vol.UUID),
	}))
	lit.SetOperations(oasOps)
	return lit, nil
}

// V1SnaplockLitigationOperationCreate implements snaplockLitigationOperationCreate. Uses ONTAP CLI `snaplock legal-hold begin` or `snaplock legal-hold end`.
func (h Handler) V1SnaplockLitigationOperationCreate(
	ctx context.Context,
	req *oasgenserver.SnaplockLegalHoldOperationRequest,
	params oasgenserver.V1SnaplockLitigationOperationCreateParams,
) (oasgenserver.V1SnaplockLitigationOperationCreateRes, error) {
	logger := util.GetLogger(ctx)

	if !snapLockOperationEnabled {
		logger.Debug("V1SnaplockLitigationOperationCreate: operation is disabled")
		return &oasgenserver.V1SnaplockLitigationOperationCreateBadRequest{
			Code:    400,
			Message: "Snaplock litigation operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1SnaplockLitigationOperationCreateForbidden{
			Code:    http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	if req == nil || strings.TrimSpace(req.Path) == "" {
		return &oasgenserver.V1SnaplockLitigationOperationCreateBadRequest{
			Code:    400,
			Message: "request body with type and path is required",
		}, nil
	}

	parts := strings.SplitN(params.LitigationId, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return &oasgenserver.V1SnaplockLitigationOperationCreateBadRequest{
			Code:    400,
			Message: "litigation_id must be volumeUuid:litigationName",
		}, nil
	}
	volumeUUID, litigationName := parts[0], parts[1]

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationCreateUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationCreateUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationCreateInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	volumeInfo, err := ontapClient.GetVolume(ctx, volumeUUID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get volume info", "volumeUuid", volumeUUID, "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationCreateNotFound{
			Code:    404,
			Message: fmt.Sprintf("volume lookup failed: %s", err.Error()),
		}, nil
	}
	if volumeInfo.Name == "" || volumeInfo.SVM.Name == "" {
		return &oasgenserver.V1SnaplockLitigationOperationCreateBadRequest{
			Code:    400,
			Message: "volume information is incomplete",
		}, nil
	}

	var cliCommand string
	switch req.Type {
	case oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin:
		cliCommand = handlers.BuildSnaplockLegalHoldBeginCommand(litigationName, volumeInfo.Name, req.Path, volumeInfo.SVM.Name)
	case oasgenserver.SnaplockLegalHoldOperationRequestTypeEnd:
		cliCommand = handlers.BuildSnaplockLegalHoldEndPathCommand(litigationName, volumeInfo.Name, req.Path, volumeInfo.SVM.Name)
	default:
		return &oasgenserver.V1SnaplockLitigationOperationCreateBadRequest{
			Code:    400,
			Message: fmt.Sprintf("invalid type %q", req.Type),
		}, nil
	}

	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			ontapCode := 400
			if _, _ = fmt.Sscanf(cliErr.Code, "%d", &ontapCode); ontapCode == 0 {
				ontapCode = 400
			}
			return &oasgenserver.V1SnaplockLitigationOperationCreateBadRequest{
				Code:    ontapCode,
				Message: cliErr.Message,
			}, nil
		}
		return &oasgenserver.V1SnaplockLitigationOperationCreateInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error()),
		}, nil
	}

	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)

		return &oasgenserver.V1SnaplockLitigationOperationCreateBadRequest{
			Code:    400,
			Message: message,
		}, nil
	}

	resp := oasgenserver.SnaplockLegalHoldOperationResponse{
		Path: oasgenserver.NewOptString(req.Path),
		Type: oasgenserver.NewOptSnaplockLegalHoldOperationResponseType(oasgenserver.SnaplockLegalHoldOperationResponseType(req.Type)),
	}
	if opID, ok := ontapproxyutils.ParseOperationIDFromBeginEndOutput(cliResponse.Output); ok {
		resp.ID = oasgenserver.NewOptInt(opID)
	}
	return &oasgenserver.SnaplockLegalHoldOperationResponseHeaders{
		Response: resp,
	}, nil
}

// V1SnaplockLitigationOperationGet implements snaplockLitigationOperationGet. Uses ONTAP CLI `snaplock legal-hold show -operation-id -instance`.
func (h Handler) V1SnaplockLitigationOperationGet(
	ctx context.Context,
	params oasgenserver.V1SnaplockLitigationOperationGetParams,
) (oasgenserver.V1SnaplockLitigationOperationGetRes, error) {
	logger := util.GetLogger(ctx)

	if !snapLockOperationEnabled {
		logger.Debug("V1SnaplockLitigationOperationGet: operation is disabled")
		return &oasgenserver.V1SnaplockLitigationOperationGetBadRequest{
			Code:    400,
			Message: "Snaplock litigation operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1SnaplockLitigationOperationGetForbidden{
			Code:    http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationGetUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationGetUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationGetInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	cmd := handlers.BuildSnaplockLegalHoldShowOperationCommand(params.OperationId)
	cliResp, err := ontapClient.ExecuteCLI(ctx, cmd, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "ONTAP litigation operation get CLI failed", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationGetInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("CLI request failed: %s", err.Error()),
		}, nil
	}
	if !handlers.IsCLISuccess(cliResp.Output) {
		msg := handlers.ParseCLIError(cliResp.Output)
		logger.ErrorContext(ctx, "ONTAP litigation operation get failed", "cliOutput", cliResp.Output)
		return &oasgenserver.V1SnaplockLitigationOperationGetNotFound{
			Code:    404,
			Message: msg,
		}, nil
	}

	rec, err := ontapproxyutils.ParseSnaplockLegalHoldShowOperationOutput(cliResp.Output)
	if err != nil || rec == nil {
		msg := "operation not found"
		if err != nil {
			msg = err.Error()
		}
		return &oasgenserver.V1SnaplockLitigationOperationGetNotFound{
			Code:    404,
			Message: msg,
		}, nil
	}

	stateStr := ontapproxyutils.MapOperationStatusToState(rec.Status)
	opType := rec.OperationType
	if opType == "" {
		opType = "begin"
	}

	op := oasgenserver.SnaplockLegalHoldOperationResponse{
		ID:                oasgenserver.NewOptInt(rec.OperationID),
		State:             oasgenserver.NewOptSnaplockLegalHoldOperationResponseState(oasgenserver.SnaplockLegalHoldOperationResponseState(stateStr)),
		Path:              oasgenserver.NewOptString(rec.Path),
		Type:              oasgenserver.NewOptSnaplockLegalHoldOperationResponseType(oasgenserver.SnaplockLegalHoldOperationResponseType(opType)),
		NumFilesProcessed: oasgenserver.NewOptString(rec.NumFilesProcessed),
		NumFilesFailed:    oasgenserver.NewOptString(rec.NumFilesFailed),
		NumFilesSkipped:   oasgenserver.NewOptString(rec.NumFilesSkipped),
		NumInodesIgnored:  oasgenserver.NewOptString(rec.NumInodesIgnored),
		StatusDetails:     oasgenserver.NewOptString(rec.StatusDetails),
	}
	return &op, nil
}

// V1SnaplockLitigationOperationAbort implements snaplockLitigationOperationAbort. Uses ONTAP CLI `snaplock legal-hold abort`.
func (h Handler) V1SnaplockLitigationOperationAbort(
	ctx context.Context,
	params oasgenserver.V1SnaplockLitigationOperationAbortParams,
) (oasgenserver.V1SnaplockLitigationOperationAbortRes, error) {
	logger := util.GetLogger(ctx)

	if !snapLockOperationEnabled {
		logger.Debug("V1SnaplockLitigationOperationAbort: operation is disabled")
		return &oasgenserver.V1SnaplockLitigationOperationAbortBadRequest{
			Code:    400,
			Message: "Snaplock litigation operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1SnaplockLitigationOperationAbortForbidden{
			Code:    http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	parts := strings.SplitN(params.LitigationId, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return &oasgenserver.V1SnaplockLitigationOperationAbortBadRequest{
			Code:    400,
			Message: "litigation_id must be volumeUuid:litigationName",
		}, nil
	}
	volumeUUID, _ := parts[0], parts[1]

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationAbortUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationAbortUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationAbortInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	volumeInfo, err := ontapClient.GetVolume(ctx, volumeUUID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get volume info", "volumeUuid", volumeUUID, "error", err)
		return &oasgenserver.V1SnaplockLitigationOperationAbortNotFound{
			Code:    404,
			Message: fmt.Sprintf("volume lookup failed: %s", err.Error()),
		}, nil
	}
	if volumeInfo.Name == "" || volumeInfo.SVM.Name == "" {
		return &oasgenserver.V1SnaplockLitigationOperationAbortBadRequest{
			Code:    400,
			Message: "volume information is incomplete",
		}, nil
	}

	cliCommand := handlers.BuildSnaplockLegalHoldAbortCommand(params.OperationId, volumeInfo.SVM.Name)
	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			msg := handlers.ParseSnaplockAbortError(cliErr.Message)
			// "Operation is complete" and similar are bad request (400); not found stays 404
			if strings.Contains(strings.ToLower(cliErr.Message), "operation is complete") {
				return &oasgenserver.V1SnaplockLitigationOperationAbortBadRequest{
					Code:    400,
					Message: msg,
				}, nil
			}
			if strings.Contains(strings.ToLower(cliErr.Message), "not found") {
				return &oasgenserver.V1SnaplockLitigationOperationAbortNotFound{
					Code:    404,
					Message: msg,
				}, nil
			}
			return &oasgenserver.V1SnaplockLitigationOperationAbortBadRequest{
				Code:    400,
				Message: msg,
			}, nil
		}
		return &oasgenserver.V1SnaplockLitigationOperationAbortInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error()),
		}, nil
	}

	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseSnaplockAbortError(cliResponse.Output)
		if strings.Contains(strings.ToLower(cliResponse.Output), "operation is complete") {
			return &oasgenserver.V1SnaplockLitigationOperationAbortBadRequest{
				Code:    400,
				Message: message,
			}, nil
		}
		if strings.Contains(strings.ToLower(cliResponse.Output), "not found") {
			return &oasgenserver.V1SnaplockLitigationOperationAbortNotFound{
				Code:    404,
				Message: message,
			}, nil
		}
		return &oasgenserver.V1SnaplockLitigationOperationAbortBadRequest{
			Code:    400,
			Message: message,
		}, nil
	}

	return &oasgenserver.V1SnaplockLitigationOperationAbortOK{}, nil
}
