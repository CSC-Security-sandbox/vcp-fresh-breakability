package endpoints

import (
	"context"
	"net/http"
	"errors"
	"fmt"
	"strings"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// V1ListEventRetentionOperations returns a list of EBR operations via CLI "snaplock event-retention show".
func (h Handler) V1ListEventRetentionOperations(
	ctx context.Context,
	params oasgenserver.V1ListEventRetentionOperationsParams,
) (oasgenserver.V1ListEventRetentionOperationsRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing list event retention operations request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String())

	if !snapLockOperationEnabled {
		logger.Debug("V1ListEventRetentionOperations: operation is disabled")
		return &oasgenserver.V1ListEventRetentionOperationsBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1ListEventRetentionOperationsForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1ListEventRetentionOperationsUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1ListEventRetentionOperationsUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1ListEventRetentionOperationsInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}
	cliCommand := handlers.BuildEventRetentionOperationShowCommand(0)
	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "error", err)
		return &oasgenserver.V1ListEventRetentionOperationsInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}
	logger.InfoContext(ctx, "Event retention operations CLI output", "operation", "list", "cliOutput", cliResponse.Output)
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Event retention operations show failed", "cliOutput", cliResponse.Output)
		return &oasgenserver.V1ListEventRetentionOperationsInternalServerError{Code: http.StatusInternalServerError, Message: message}, nil
	}
	rows, err := handlers.ParseEventRetentionOperationShowOutput(cliResponse.Output)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to parse CLI output", "error", err)
		return &oasgenserver.V1ListEventRetentionOperationsInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to parse EBR operations: %s", err.Error())}, nil
	}
	records := make([]oasgenserver.EBROperation, 0, len(rows))
	for _, row := range rows {
		records = append(records, rowToEBROperation(row))
	}
	return &oasgenserver.EBROperationResponse{Records: records, NumRecords: oasgenserver.NewOptInt(len(records))}, nil
}

// V1CreateEventRetentionOperation starts an EBR apply operation via CLI "snaplock event-retention apply".
func (h Handler) V1CreateEventRetentionOperation(
	ctx context.Context,
	req *oasgenserver.EBROperationCreate,
	params oasgenserver.V1CreateEventRetentionOperationParams,
) (oasgenserver.V1CreateEventRetentionOperationRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing create event retention operation request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String())

	if !snapLockOperationEnabled {
		logger.Debug("V1CreateEventRetentionOperation: operation is disabled")
		return &oasgenserver.V1CreateEventRetentionOperationBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1CreateEventRetentionOperationForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	if req == nil || strings.TrimSpace(req.Path) == "" {
		return &oasgenserver.V1CreateEventRetentionOperationBadRequest{Code: http.StatusBadRequest, Message: "path is required"}, nil
	}
	policyName := strings.TrimSpace(req.Policy.Name)
	if policyName == "" {
		return &oasgenserver.V1CreateEventRetentionOperationBadRequest{Code: http.StatusBadRequest, Message: "policy.name is required"}, nil
	}
	var volumeName string
	var volumeUUID string
	if req.Volume.IsSet() {
		vol := req.Volume.Value
		if vol.Name.IsSet() && vol.Name.Value != "" {
			volumeName = strings.TrimSpace(vol.Name.Value)
		}
		if vol.UUID.IsSet() {
			volumeUUID = vol.UUID.Value.String()
		}
	}
	if volumeName == "" && volumeUUID == "" {
		return &oasgenserver.V1CreateEventRetentionOperationBadRequest{Code: http.StatusBadRequest, Message: "volume (name or uuid) is required"}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1CreateEventRetentionOperationUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1CreateEventRetentionOperationUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1CreateEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}

	// When volume UUID is passed, resolve to volume name via GetVolume (same as SnaplockFileDelete).
	if volumeName == "" && volumeUUID != "" {
		volumeInfo, err := ontapClient.GetVolume(ctx, volumeUUID)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get volume info", "volumeUuid", volumeUUID, "error", err)
			return &oasgenserver.V1CreateEventRetentionOperationNotFound{Code: http.StatusNotFound, Message: fmt.Sprintf("volume not found: %s", err.Error())}, nil
		}
		if volumeInfo.Name == "" {
			logger.ErrorContext(ctx, "Volume info incomplete", "volumeUuid", volumeUUID, "volumeName", volumeInfo.Name)
			return &oasgenserver.V1CreateEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: "volume information is incomplete"}, nil
		}
		volumeName = volumeInfo.Name
	}

	cliCommand := handlers.BuildEventRetentionOperationApplyCommand(volumeName, policyName, req.Path)
	logger.InfoContext(ctx, "Executing snaplock event-retention apply", "policyName", policyName, "path", req.Path, "volumeName", volumeName)

	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "error", err)
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			return &oasgenserver.V1CreateEventRetentionOperationBadRequest{Code: handlers.OntapCodeToInt(cliErr.Code), Message: cliErr.Message}, nil
		}
		return &oasgenserver.V1CreateEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}
	logger.InfoContext(ctx, "Event retention apply CLI output", "cliOutput", cliResponse.Output)
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Event retention apply failed", "cliOutput", cliResponse.Output)
		if strings.Contains(strings.ToLower(message), "not found") || strings.Contains(strings.ToLower(message), "does not exist") {
			return &oasgenserver.V1CreateEventRetentionOperationNotFound{Code: http.StatusNotFound, Message: message}, nil
		}
		return &oasgenserver.V1CreateEventRetentionOperationBadRequest{Code: http.StatusBadRequest, Message: message}, nil
	}
	logger.InfoContext(ctx, "Event retention operation apply completed successfully", "policyName", policyName, "path", req.Path)
	return &oasgenserver.EBROperation{
		State:  oasgenserver.NewOptString("in_progress"),
		Path:   oasgenserver.NewOptString(req.Path),
		Policy: oasgenserver.NewOptEBRPolicyRef(oasgenserver.EBRPolicyRef{Name: oasgenserver.NewOptString(policyName)}),
		Volume: oasgenserver.NewOptEBROperationVolume(oasgenserver.EBROperationVolume{Name: oasgenserver.NewOptString(volumeName)}),
	}, nil
}

// V1GetEventRetentionOperation returns one EBR operation by ID via CLI "snaplock event-retention show -operation-id <id>".
func (h Handler) V1GetEventRetentionOperation(
	ctx context.Context,
	params oasgenserver.V1GetEventRetentionOperationParams,
) (oasgenserver.V1GetEventRetentionOperationRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing get event retention operation request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String(), "id", params.ID)

	if !snapLockOperationEnabled {
		logger.Debug("V1GetEventRetentionOperation: operation is disabled")
		return &oasgenserver.V1GetEventRetentionOperationBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1GetEventRetentionOperationForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1GetEventRetentionOperationUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1GetEventRetentionOperationUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1GetEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}
	cliCommand := handlers.BuildEventRetentionOperationShowCommand(params.ID)
	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "error", err)
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) && (cliErr.Code == "4" || strings.Contains(strings.ToLower(cliErr.Message), "not found") || strings.Contains(strings.ToLower(cliErr.Message), "does not exist")) {
			return &oasgenserver.V1GetEventRetentionOperationNotFound{Code: http.StatusNotFound, Message: cliErr.Message}, nil
		}
		return &oasgenserver.V1GetEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}
	logger.InfoContext(ctx, "Event retention operation show output", "id", params.ID, "cliOutput", cliResponse.Output)
	cliSuccess := handlers.IsCLISuccess(cliResponse.Output)
	logger.InfoContext(ctx, "Event retention operation CLI success check", "id", params.ID, "isCLISuccess", cliSuccess)
	if !cliSuccess {
		message := handlers.ParseCLIError(cliResponse.Output)
		if strings.Contains(strings.ToLower(message), "not found") || strings.Contains(strings.ToLower(message), "does not exist") {
			return &oasgenserver.V1GetEventRetentionOperationNotFound{Code: http.StatusNotFound, Message: message}, nil
		}
		return &oasgenserver.V1GetEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: message}, nil
	}
	rows, err := handlers.ParseEventRetentionOperationShowOutput(cliResponse.Output)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to parse CLI output", "error", err, "cliOutput", cliResponse.Output)
		return &oasgenserver.V1GetEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to parse EBR operation: %s", err.Error())}, nil
	}
	if len(rows) == 0 {
		return &oasgenserver.V1GetEventRetentionOperationNotFound{Code: http.StatusNotFound, Message: "EBR operation not found"}, nil
	}
	res := rowToEBROperation(rows[0])
	return &res, nil
}

// V1AbortEventRetentionOperation aborts an EBR operation via CLI "snaplock event-retention abort -operation-id <id>".
func (h Handler) V1AbortEventRetentionOperation(
	ctx context.Context,
	params oasgenserver.V1AbortEventRetentionOperationParams,
) (oasgenserver.V1AbortEventRetentionOperationRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing abort event retention operation request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String(), "id", params.ID)

	if !snapLockOperationEnabled {
		logger.Debug("V1AbortEventRetentionOperation: operation is disabled")
		return &oasgenserver.V1AbortEventRetentionOperationBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1AbortEventRetentionOperationForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1AbortEventRetentionOperationUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1AbortEventRetentionOperationUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1AbortEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}
	cliCommand := handlers.BuildEventRetentionOperationAbortCommand(params.ID)
	logger.InfoContext(ctx, "Executing snaplock event-retention abort", "id", params.ID)

	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "error", err)
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			codeInt := handlers.OntapCodeToInt(cliErr.Code)
			if codeInt == 404 || strings.Contains(strings.ToLower(cliErr.Message), "not found") || strings.Contains(strings.ToLower(cliErr.Message), "does not exist") {
				return &oasgenserver.V1AbortEventRetentionOperationNotFound{Code: http.StatusNotFound, Message: cliErr.Message}, nil
			}
			return &oasgenserver.V1AbortEventRetentionOperationBadRequest{Code: codeInt, Message: cliErr.Message}, nil
		}
		return &oasgenserver.V1AbortEventRetentionOperationInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}
	logger.InfoContext(ctx, "Event retention abort CLI output", "id", params.ID, "cliOutput", cliResponse.Output)
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		if strings.Contains(strings.ToLower(message), "not found") || strings.Contains(strings.ToLower(message), "does not exist") {
			return &oasgenserver.V1AbortEventRetentionOperationNotFound{Code: http.StatusNotFound, Message: message}, nil
		}
		return &oasgenserver.V1AbortEventRetentionOperationBadRequest{Code: http.StatusBadRequest, Message: message}, nil
	}
	logger.InfoContext(ctx, "Event retention operation aborted successfully", "id", params.ID)
	return &oasgenserver.V1AbortEventRetentionOperationOK{}, nil
}

// rowToEBROperation converts a parsed CLI row to the API EBROperation type.
func rowToEBROperation(row handlers.EventRetentionOperationRow) oasgenserver.EBROperation {
	op := oasgenserver.EBROperation{
		ID:                oasgenserver.NewOptInt64(row.OperationID),
		State:             oasgenserver.NewOptString(row.State),
		Path:              oasgenserver.NewOptString(row.Path),
		NumFilesProcessed: oasgenserver.NewOptInt64(row.NumFilesProcessed),
		NumFilesFailed:    oasgenserver.NewOptInt64(row.NumFilesFailed),
		NumFilesSkipped:   oasgenserver.NewOptInt64(row.NumFilesSkipped),
		NumInodesIgnored:  oasgenserver.NewOptInt64(row.NumInodesIgnored),
	}
	if row.PolicyName != "" {
		op.Policy = oasgenserver.NewOptEBRPolicyRef(oasgenserver.EBRPolicyRef{Name: oasgenserver.NewOptString(row.PolicyName)})
	}
	if row.VolumeName != "" {
		op.Volume = oasgenserver.NewOptEBROperationVolume(oasgenserver.EBROperationVolume{Name: oasgenserver.NewOptString(row.VolumeName)})
	}
	if row.Vserver != "" {
		op.Svm = oasgenserver.NewOptSvmRef(oasgenserver.SvmRef{Name: oasgenserver.NewOptString(row.Vserver)})
	}
	return op
}
