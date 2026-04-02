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

// V1ListEventRetentionPolicies: GET list via CLI; parses "snaplock event-retention policy show" output into EBRPolicyResponse.
func (h Handler) V1ListEventRetentionPolicies(
	ctx context.Context,
	params oasgenserver.V1ListEventRetentionPoliciesParams,
) (oasgenserver.V1ListEventRetentionPoliciesRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing list event retention policies request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String())

	if !snapLockOperationEnabled {
		logger.Debug("V1ListEventRetentionPolicies: operation is disabled")
		return &oasgenserver.V1ListEventRetentionPoliciesBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention policy operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1ListEventRetentionPoliciesForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1ListEventRetentionPoliciesUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1ListEventRetentionPoliciesUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1ListEventRetentionPoliciesInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}
	cliCommand := handlers.BuildEventRetentionPolicyShowCommand("")
	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "error", err)
		return &oasgenserver.V1ListEventRetentionPoliciesInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}

	logger.InfoContext(ctx, "Event retention CLI output", "operation", "list", "cliOutput", cliResponse.Output)
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Event retention policy show failed", "cliOutput", cliResponse.Output)
		return &oasgenserver.V1ListEventRetentionPoliciesInternalServerError{Code: http.StatusInternalServerError, Message: message}, nil
	}
	rows, err := handlers.ParseEventRetentionPolicyShowOutput(cliResponse.Output)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to parse CLI output", "error", err)
		return &oasgenserver.V1ListEventRetentionPoliciesInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to parse event retention policies: %s", err.Error())}, nil
	}

	records := make([]oasgenserver.EBRPolicy, 0, len(rows))
	for _, row := range rows {
		rec := oasgenserver.EBRPolicy{
			Name:            row.Name,
			RetentionPeriod: row.RetentionPeriod,
		}
		if row.Vserver != "" {
			rec.Svm = oasgenserver.NewOptSvmRef(oasgenserver.SvmRef{Name: oasgenserver.NewOptString(row.Vserver)})
		}
		records = append(records, rec)
	}
	return &oasgenserver.EBRPolicyResponse{Records: records, NumRecords: oasgenserver.NewOptInt(len(records))}, nil
}

// V1CreateEventRetentionPolicy: CLI create; API retention period (e.g. P7Y) is converted to CLI format (e.g. "7 years").
func (h Handler) V1CreateEventRetentionPolicy(
	ctx context.Context,
	req *oasgenserver.EBRPolicy,
	params oasgenserver.V1CreateEventRetentionPolicyParams,
) (oasgenserver.V1CreateEventRetentionPolicyRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing create event retention policy request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String(), "policyName", req.Name)

	if !snapLockOperationEnabled {
		logger.Debug("V1CreateEventRetentionPolicy: operation is disabled")
		return &oasgenserver.V1CreateEventRetentionPolicyBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention policy operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1CreateEventRetentionPolicyForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	if req.Name == "" {
		return &oasgenserver.V1CreateEventRetentionPolicyBadRequest{Code: http.StatusBadRequest, Message: "policy name is required"}, nil
	}
	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1CreateEventRetentionPolicyUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1CreateEventRetentionPolicyUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1CreateEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}
	retentionCLI := handlers.RetentionPeriodAPIToCLI(req.RetentionPeriod)
	cliCommand := handlers.BuildEventRetentionPolicyCreateCommand(req.Name, retentionCLI)
	logger.InfoContext(ctx, "Executing snaplock event-retention policy create", "policyName", req.Name)

	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "policyName", req.Name, "error", err)
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			return &oasgenserver.V1CreateEventRetentionPolicyBadRequest{Code: handlers.OntapCodeToInt(cliErr.Code), Message: cliErr.Message}, nil
		}
		return &oasgenserver.V1CreateEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}
	logger.InfoContext(ctx, "Event retention CLI output", "operation", "create", "policyName", req.Name, "cliOutput", cliResponse.Output)
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Event retention policy create failed", "policyName", req.Name, "cliOutput", cliResponse.Output)
		return &oasgenserver.V1CreateEventRetentionPolicyBadRequest{Code: http.StatusBadRequest, Message: message}, nil
	}
	logger.InfoContext(ctx, "Event retention policy create completed successfully", "policyName", req.Name)
	return req, nil
}

// V1GetEventRetentionPolicy: GET one via CLI; runs "snaplock event-retention policy show -name <policyName>" and parses output.
func (h Handler) V1GetEventRetentionPolicy(
	ctx context.Context,
	params oasgenserver.V1GetEventRetentionPolicyParams,
) (oasgenserver.V1GetEventRetentionPolicyRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing get event retention policy request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String(), "policyName", params.PolicyName)

	if !snapLockOperationEnabled {
		logger.Debug("V1GetEventRetentionPolicy: operation is disabled")
		return &oasgenserver.V1GetEventRetentionPolicyBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention policy operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1GetEventRetentionPolicyForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1GetEventRetentionPolicyUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1GetEventRetentionPolicyUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1GetEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}
	cliCommand := handlers.BuildEventRetentionPolicyShowCommand(params.PolicyName)
	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "error", err)
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) && (cliErr.Code == "4" || strings.Contains(strings.ToLower(cliErr.Message), "doesn't exist") || strings.Contains(strings.ToLower(cliErr.Message), "does not exist")) {
			return &oasgenserver.V1GetEventRetentionPolicyNotFound{Code: http.StatusNotFound, Message: cliErr.Message}, nil
		}
		return &oasgenserver.V1GetEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}
	logger.InfoContext(ctx, "Event retention CLI output", "operation", "get", "policyName", params.PolicyName, "cliOutput", cliResponse.Output)
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		if strings.Contains(strings.ToLower(message), "not found") || strings.Contains(strings.ToLower(message), "does not exist") {
			return &oasgenserver.V1GetEventRetentionPolicyNotFound{Code: http.StatusNotFound, Message: message}, nil
		}
		return &oasgenserver.V1GetEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: message}, nil
	}
	rows, err := handlers.ParseEventRetentionPolicyShowOutput(cliResponse.Output)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to parse CLI output", "error", err)
		return &oasgenserver.V1GetEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to parse event retention policy: %s", err.Error())}, nil
	}
	if len(rows) == 0 {
		return &oasgenserver.V1GetEventRetentionPolicyNotFound{Code: http.StatusNotFound, Message: "event retention policy not found"}, nil
	}
	row := rows[0]
	res := &oasgenserver.EBRPolicy{
		Name:            row.Name,
		RetentionPeriod: row.RetentionPeriod,
	}
	if row.Vserver != "" {
		res.Svm = oasgenserver.NewOptSvmRef(oasgenserver.SvmRef{Name: oasgenserver.NewOptString(row.Vserver)})
	}
	return res, nil
}

// V1UpdateEventRetentionPolicy: CLI modify; retention_period from API (e.g. P7Y) converted to CLI format.
func (h Handler) V1UpdateEventRetentionPolicy(
	ctx context.Context,
	req *oasgenserver.V1UpdateEventRetentionPolicyReq,
	params oasgenserver.V1UpdateEventRetentionPolicyParams,
) (oasgenserver.V1UpdateEventRetentionPolicyRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing update event retention policy request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String(), "policyName", params.PolicyName)

	if !snapLockOperationEnabled {
		logger.Debug("V1UpdateEventRetentionPolicy: operation is disabled")
		return &oasgenserver.V1UpdateEventRetentionPolicyBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention policy operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1UpdateEventRetentionPolicyForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1UpdateEventRetentionPolicyUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1UpdateEventRetentionPolicyUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1UpdateEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}
	if !req.RetentionPeriod.IsSet() {
		return &oasgenserver.V1UpdateEventRetentionPolicyBadRequest{Code: http.StatusBadRequest, Message: "retention_period is required"}, nil
	}
	apiPeriod := req.RetentionPeriod.Or("")
	retentionCLI := handlers.RetentionPeriodAPIToCLI(apiPeriod)
	cliCommand := handlers.BuildEventRetentionPolicyModifyCommand(params.PolicyName, retentionCLI)
	logger.InfoContext(ctx, "Executing snaplock event-retention policy modify", "policyName", params.PolicyName)

	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "policyName", params.PolicyName, "error", err)
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			return &oasgenserver.V1UpdateEventRetentionPolicyBadRequest{Code: handlers.OntapCodeToInt(cliErr.Code), Message: cliErr.Message}, nil
		}
		return &oasgenserver.V1UpdateEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}
	logger.InfoContext(ctx, "Event retention CLI output", "operation", "update", "policyName", params.PolicyName, "cliOutput", cliResponse.Output)
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Event retention policy modify failed", "policyName", params.PolicyName, "cliOutput", cliResponse.Output)
		return &oasgenserver.V1UpdateEventRetentionPolicyBadRequest{Code: http.StatusBadRequest, Message: message}, nil
	}
	logger.InfoContext(ctx, "Event retention policy modify completed successfully", "policyName", params.PolicyName)
	return &oasgenserver.V1UpdateEventRetentionPolicyOK{}, nil
}

// V1DeleteEventRetentionPolicy: CLI delete (no -vserver).
func (h Handler) V1DeleteEventRetentionPolicy(
	ctx context.Context,
	params oasgenserver.V1DeleteEventRetentionPolicyParams,
) (oasgenserver.V1DeleteEventRetentionPolicyRes, error) {
	logger := util.GetLogger(ctx)
	logger.InfoContext(ctx, "Processing delete event retention policy request",
		"projectNumber", params.ProjectNumber, "poolId", params.PoolId.String(), "policyName", params.PolicyName)

	if !snapLockOperationEnabled {
		logger.Debug("V1DeleteEventRetentionPolicy: operation is disabled")
		return &oasgenserver.V1DeleteEventRetentionPolicyBadRequest{
			Code: http.StatusBadRequest,
			Message: "Event retention policy operation is disabled",
		}, nil
	}

	if !middleware.IsIAMRoleHeaderSnaplockExistInContext(ctx, middleware.ManageSnaplockRole) {
		return &oasgenserver.V1DeleteEventRetentionPolicyForbidden{
			Code: http.StatusForbidden,
			Message: snaplockIAMRoleRequiredMessage,
		}, nil
	}

	ctx, err := setupCredentialsForHandler(ctx, params.ProjectNumber, params.PoolId.String(), middleware.CredentialTypeAdmin)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1DeleteEventRetentionPolicyUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	if err := ensureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1DeleteEventRetentionPolicyUnauthorized{Code: http.StatusUnauthorized, Message: fmt.Sprintf("authentication error: %s", err.Error())}, nil
	}
	ontapClient, err := newOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1DeleteEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error())}, nil
	}
	cliCommand := handlers.BuildEventRetentionPolicyDeleteCommand(params.PolicyName)
	logger.InfoContext(ctx, "Executing snaplock event-retention policy delete", "policyName", params.PolicyName)

	cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "policyName", params.PolicyName, "error", err)
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			return &oasgenserver.V1DeleteEventRetentionPolicyBadRequest{Code: handlers.OntapCodeToInt(cliErr.Code), Message: cliErr.Message}, nil
		}
		return &oasgenserver.V1DeleteEventRetentionPolicyInternalServerError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error())}, nil
	}
	logger.InfoContext(ctx, "Event retention CLI output", "operation", "delete", "policyName", params.PolicyName, "cliOutput", cliResponse.Output)
	if !handlers.IsCLISuccess(cliResponse.Output) {
		message := handlers.ParseCLIError(cliResponse.Output)
		logger.WarnContext(ctx, "Event retention policy delete failed", "policyName", params.PolicyName, "cliOutput", cliResponse.Output)
		if message != "" && (strings.Contains(strings.ToLower(message), "not found") || strings.Contains(strings.ToLower(message), "does not exist")) {
			return &oasgenserver.V1DeleteEventRetentionPolicyNotFound{Code: http.StatusNotFound, Message: message}, nil
		}
		return &oasgenserver.V1DeleteEventRetentionPolicyBadRequest{Code: http.StatusBadRequest, Message: message}, nil
	}
	logger.InfoContext(ctx, "Event retention policy delete completed successfully", "policyName", params.PolicyName)
	return &oasgenserver.V1DeleteEventRetentionPolicyOK{}, nil
}
