package vlm

import (
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/temporal"
)

// VLMErrorHandler handles VLM-specific errors and converts them to user-facing errors
type VLMErrorHandler struct {
	logger log.Logger
}

// NewVLMErrorHandler creates a new VLM error handler
func NewVLMErrorHandler() *VLMErrorHandler {
	return &VLMErrorHandler{
		logger: log.NewLogger(),
	}
}

// NewVLMErrorHandlerWithLogger creates a new VLM error handler with a custom logger
func NewVLMErrorHandlerWithLogger(logger log.Logger) *VLMErrorHandler {
	return &VLMErrorHandler{
		logger: logger,
	}
}

// HandleVLMError processes VLM errors and converts them to appropriate VCP errors
func (h *VLMErrorHandler) HandleVLMError(err error) error {
	if err == nil {
		return nil
	}

	// Log the incoming error for debugging
	h.logger.Debug("Processing VLM error",
		"error", err.Error(),
		"error_type", fmt.Sprintf("%T", err))

	// First, check if it's a child workflow execution error (retry scenario)
	var childWorkflowErr *temporal.ChildWorkflowExecutionError
	if errors.As(err, &childWorkflowErr) {
		h.logger.Info("Detected child workflow execution error (retry scenario)",
			"workflow_type", childWorkflowErr.WorkflowType,
			"workflow_id", childWorkflowErr.WorkflowID,
			"run_id", childWorkflowErr.RunID)

		// Try to extract the actual error from the cause
		// For child workflow errors, we need to check if there's an underlying application error
		var appErr *temporal.ApplicationError
		if errors.As(childWorkflowErr, &appErr) {
			h.logger.Debug("Found application error in child workflow, processing it",
				"app_error_type", appErr.Type(),
				"app_error_message", appErr.Message())

			// Check if the cause is already a VLM error (avoid double processing)
			if appErr.Type() == ErrorTypeVLMClientError {
				h.logger.Info("Cause is already a VLM error, processing directly")
				return h.HandleVLMError(appErr)
			}

			// Process the application error to extract VLM client error details
			return h.HandleVLMError(appErr)
		}

		// If no cause, return generic workflow error with retry context
		return errors.NewVCPError(errors.ErrVLMWorkflowError,
			fmt.Errorf("workflow %s failed after retries: %s",
				childWorkflowErr.WorkflowType(), childWorkflowErr.Error()))
	}

	// Check if it's a temporal application error
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		h.logger.Warn("Non-temporal error received from VLM",
			"error", err.Error(),
			"error_type", fmt.Sprintf("%T", err))
		return h.handleNonTemporalError(err)
	}

	// Check if it's a VLM client error
	if appErr.Type() == ErrorTypeVLMClientError {
		var vlmClientErr VLMClientError
		if appErr.HasDetails() && appErr.Details(&vlmClientErr) == nil {
			h.logger.Info("Successfully extracted VLM client error details",
				"http_code", vlmClientErr.HttpCode,
				"code", vlmClientErr.Code,
				"message", vlmClientErr.Message,
				"component", vlmClientErr.Component,
				"external", vlmClientErr.External,
				"retryable", vlmClientErr.Retryable,
				"causes_count", len(vlmClientErr.Cause))
			return h.convertVLMClientErrorToVCPError(vlmClientErr, err)
		}
		h.logger.Warn("Failed to extract VLM client error details",
			"app_error_type", appErr.Type(),
			"has_details", appErr.HasDetails(),
			"app_error_message", appErr.Message())
	}

	// If it's not a VLM client error, return as generic VLM workflow error
	h.logger.Info("Non-VLM temporal error received",
		"app_error_type", appErr.Type(),
		"app_error_message", appErr.Message())
	return errors.NewVCPError(errors.ErrVLMWorkflowError, err)
}

// handleNonTemporalError handles non-temporal errors with fallback logic
func (h *VLMErrorHandler) handleNonTemporalError(err error) error {
	// Try to classify the error based on its message
	errMsg := strings.ToLower(err.Error())

	// Check for common error patterns
	if h.isRetryableError(errMsg) {
		h.logger.Info("Classified as retryable error", "error", err.Error())
		return errors.NewVCPError(errors.ErrVLMWorkflowError, err)
	}

	if h.isQuotaError(errMsg) {
		h.logger.Info("Classified as quota error", "error", err.Error())
		return errors.NewVCPError(errors.ErrVLMQuotaExceededGeneral, err)
	}

	if h.isPermissionError(errMsg) {
		h.logger.Info("Classified as permission error", "error", err.Error())
		return errors.NewVCPError(errors.ErrVLMInsufficientPermissions, err)
	}

	// Default to generic VLM workflow error
	return errors.NewVCPError(errors.ErrVLMWorkflowError, err)
}

// convertVLMClientErrorToVCPError converts vlm.VLMClientError to appropriate VCP error
func (h *VLMErrorHandler) convertVLMClientErrorToVCPError(vlmErr VLMClientError, originalErr error) error {
	// Log the VLM client error details
	h.logger.Info("Processing VLM client error",
		"http_code", vlmErr.HttpCode,
		"code", vlmErr.Code,
		"component", vlmErr.Component,
		"external", vlmErr.External,
		"retryable", vlmErr.Retryable,
		"message", vlmErr.Message,
		"causes", vlmErr.Cause)

	mappedError := h.mapVLMErrorToUserFacingError(vlmErr, originalErr)

	// If the error is marked as external, we should propagate it to the customer
	if vlmErr.External {
		h.logger.Info("External VLM error - propagating to customer",
			"http_code", vlmErr.HttpCode,
			"code", vlmErr.Code,
			"message", vlmErr.Message)
		return mappedError
	}

	// For internal errors, log them but still return the mapped error
	// This preserves the actual error message while indicating it's internal
	h.logger.Warn("Internal VLM error - preserving error message for debugging",
		"http_code", vlmErr.HttpCode,
		"code", vlmErr.Code,
		"message", vlmErr.Message,
		"causes", vlmErr.Cause)

	return mappedError
}

// mapVLMErrorToUserFacingError maps VLM error codes to user-facing VCP errors
func (h *VLMErrorHandler) mapVLMErrorToUserFacingError(vlmErr VLMClientError, originalErr error) error {
	h.logger.Debug("Mapping VLM error to user-facing error",
		"http_code", vlmErr.HttpCode,
		"vlm_code", vlmErr.Code,
		"message", vlmErr.Message,
		"causes", vlmErr.Cause)

	// PRIORITY 1: Map based on VLM error codes first (most specific)
	if vlmErr.Code != "" {
		switch vlmErr.Code {
		case "QUOTA_EXCEEDED":
			return h.handleQuotaError(vlmErr, originalErr)
		case "ZONE_RESOURCE_POOL_EXHAUSTED":
			return h.handleResourceExhaustionError(vlmErr, originalErr)
		case "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS":
			return h.handleResourceExhaustionWithDetailsError(vlmErr, originalErr)
		case "RESOURCE_OPERATION_RATE_EXCEEDED":
			return h.handleRateLimitError(vlmErr, originalErr)
		case "SERVICE_ACCOUNT_ACCESS_DENIED":
			return errors.NewVCPError(errors.ErrVLMServiceAccountAccessDenied, originalErr)
		case "RESOURCE_NOT_READY":
			return errors.NewVCPError(errors.ErrVLMResourceNotReady, originalErr)
		case "PROJECT_CONSTRAINT_VIOLATED":
			return errors.NewVCPError(errors.ErrVLMProjectConstraintViolated, originalErr)
		case "CPU_PLATFORM_MISMATCH":
			return errors.NewVCPError(errors.ErrVLMCPUPlatformMismatch, originalErr)
		case "INVALID_MACHINE_IMAGE_UPDATE":
			return errors.NewVCPError(errors.ErrVLMInvalidMachineImageUpdate, originalErr)
		case "CLOUD_OPERATION_FAILED":
			return h.handleCloudOperationFailed(vlmErr, originalErr)
		}
	}

	// PRIORITY 2: String-based pattern matching (medium specificity)
	if vlmErr.Message != "" {
		return h.handleStringBasedError(vlmErr.Message, originalErr)
	}

	// PRIORITY 3: Map based on HTTP status codes (least specific, fallback)
	switch vlmErr.HttpCode {
	case 429: // Too Many Requests
		return h.handleQuotaOrRateLimitError(vlmErr, originalErr)
	case 403: // Forbidden
		return h.handlePermissionError(vlmErr, originalErr)
	case 409: // Conflict
		return h.handleConflictError(vlmErr, originalErr)
	case 400: // Bad Request
		return h.handleBadRequestError(vlmErr, originalErr)
	case 404: // Not Found
		return h.handleNotFoundError(vlmErr, originalErr)
	case 401: // Unauthorized
		return h.handleUnauthorizedError(vlmErr, originalErr)
	case 500, 502, 503, 504: // Server errors
		return h.handleServerError(vlmErr, originalErr)
	}

	// Final fallback: generic VLM workflow error
	return errors.NewVCPError(errors.ErrVLMWorkflowError, originalErr)
}

// handleServerError handles server-side errors (500, 502, 503, 504)
func (h *VLMErrorHandler) handleServerError(vlmErr VLMClientError, originalErr error) error {
	h.logger.Warn("Server error from VLM",
		"http_code", vlmErr.HttpCode,
		"message", vlmErr.Message,
		"retryable", vlmErr.Retryable)

	// Check if there's a specific VLM error code that we can handle
	if vlmErr.Code != "" {
		switch vlmErr.Code {
		case "CLOUD_OPERATION_FAILED":
			// Handle cloud operation failures with detailed error information
			return h.handleCloudOperationFailed(vlmErr, originalErr)
		}
	}

	// Always prioritize cause information over generic original error
	if len(vlmErr.Cause) > 0 {
		primaryCause := vlmErr.Cause[0]
		h.logger.Info("Using detailed cause information for server error",
			"cause", primaryCause)

		// If the error is marked as retryable, return a retryable error with cause
		if vlmErr.Retryable {
			return errors.NewVCPError(errors.ErrVLMWorkflowError,
				fmt.Errorf("server error: %s", primaryCause))
		}

		// For non-retryable server errors, return with cause information
		return errors.NewVCPError(errors.ErrVLMWorkflowError,
			fmt.Errorf("server error: %s", primaryCause))
	}

	// Fallback to original error if no cause information
	if vlmErr.Retryable {
		return errors.NewVCPError(errors.ErrVLMWorkflowError, originalErr)
	}
	return errors.NewVCPError(errors.ErrVLMWorkflowError, originalErr)
}

// handleQuotaOrRateLimitError handles quota and rate limit errors
func (h *VLMErrorHandler) handleQuotaOrRateLimitError(vlmErr VLMClientError, originalErr error) error {
	message := strings.ToLower(vlmErr.Message)

	h.logger.Info("Processing quota/rate limit error",
		"http_code", vlmErr.HttpCode,
		"message", vlmErr.Message)

	// Check for quota exceeded patterns
	if strings.Contains(message, "quota") && strings.Contains(message, "exceeded") {
		if strings.Contains(message, "region") {
			return errors.NewVCPError(errors.ErrVLMQuotaExceededRegional, originalErr)
		}
		if strings.Contains(message, "zone") {
			return errors.NewVCPError(errors.ErrVLMQuotaExceededZonal, originalErr)
		}
		return errors.NewVCPError(errors.ErrVLMQuotaExceededGeneral, originalErr)
	}

	// Check for rate limit patterns
	if strings.Contains(message, "rate limit") || strings.Contains(message, "rate exceeded") {
		if strings.Contains(message, "disk") || strings.Contains(message, "provisioned") {
			return errors.NewVCPError(errors.ErrVLMDiskRateLimited, originalErr)
		}
		return errors.NewVCPError(errors.ErrVLMRateLimitExceeded, originalErr)
	}

	// Default quota/rate limit error
	return errors.NewVCPError(errors.ErrVLMQuotaExceededGeneral, originalErr)
}

// handlePermissionError handles permission-related errors
func (h *VLMErrorHandler) handlePermissionError(vlmErr VLMClientError, originalErr error) error {
	message := strings.ToLower(vlmErr.Message)

	h.logger.Info("Processing permission error",
		"http_code", vlmErr.HttpCode,
		"message", vlmErr.Message)

	if strings.Contains(message, "service_account_access_denied") {
		return errors.NewVCPError(errors.ErrVLMServiceAccountAccessDenied, originalErr)
	}

	if strings.Contains(message, "required") && strings.Contains(message, "permission") {
		return errors.NewVCPError(errors.ErrVLMInsufficientPermissions, originalErr)
	}

	return errors.NewVCPError(errors.ErrVLMInsufficientPermissions, originalErr)
}

// handleConflictError handles conflict errors (like resource already exists)
func (h *VLMErrorHandler) handleConflictError(vlmErr VLMClientError, originalErr error) error {
	message := strings.ToLower(vlmErr.Message)

	h.logger.Info("Processing conflict error",
		"http_code", vlmErr.HttpCode,
		"message", vlmErr.Message)

	if strings.Contains(message, "already exists") {
		return errors.NewVCPError(errors.ErrGCPResourceAlreadyExistsError, originalErr)
	}

	return errors.NewVCPError(errors.ErrGCPResourceAlreadyExistsError, originalErr)
}

// handleBadRequestError handles bad request errors
func (h *VLMErrorHandler) handleBadRequestError(vlmErr VLMClientError, originalErr error) error {
	message := strings.ToLower(vlmErr.Message)

	h.logger.Info("Processing bad request error",
		"http_code", vlmErr.HttpCode,
		"message", vlmErr.Message)

	if strings.Contains(message, "cpu platform") {
		return errors.NewVCPError(errors.ErrVLMCPUPlatformMismatch, originalErr)
	}

	if strings.Contains(message, "sourcemachineimage") && strings.Contains(message, "not supported") {
		return errors.NewVCPError(errors.ErrVLMInvalidMachineImageUpdate, originalErr)
	}

	return errors.NewVCPError(errors.ErrBadRequest, originalErr)
}

// handleNotFoundError handles not found errors
func (h *VLMErrorHandler) handleNotFoundError(vlmErr VLMClientError, originalErr error) error {
	message := strings.ToLower(vlmErr.Message)

	h.logger.Info("Processing not found error",
		"http_code", vlmErr.HttpCode,
		"message", vlmErr.Message)

	if strings.Contains(message, "notfound") || strings.Contains(message, "does not exist in zone") {
		return errors.NewVCPError(errors.ErrVLMResourceNotAvailableInZone, originalErr)
	}

	return errors.NewVCPError(errors.ErrVLMResourceNotAvailableInZone, originalErr)
}

// handleUnauthorizedError handles unauthorized errors
func (h *VLMErrorHandler) handleUnauthorizedError(vlmErr VLMClientError, originalErr error) error {
	h.logger.Info("Processing unauthorized error",
		"http_code", vlmErr.HttpCode,
		"message", vlmErr.Message)

	return errors.NewVCPError(errors.ErrVLMInsufficientPermissions, originalErr)
}

// handleQuotaError handles quota exceeded errors
func (h *VLMErrorHandler) handleQuotaError(vlmErr VLMClientError, originalErr error) error {
	message := strings.ToLower(vlmErr.Message)

	h.logger.Info("Processing quota error",
		"code", vlmErr.Code,
		"message", vlmErr.Message)

	if strings.Contains(message, "region") {
		return errors.NewVCPError(errors.ErrVLMQuotaExceededRegional, originalErr)
	}
	if strings.Contains(message, "zone") {
		return errors.NewVCPError(errors.ErrVLMQuotaExceededZonal, originalErr)
	}
	return errors.NewVCPError(errors.ErrVLMQuotaExceededGeneral, originalErr)
}

// handleResourceExhaustionError handles resource exhaustion errors
func (h *VLMErrorHandler) handleResourceExhaustionError(vlmErr VLMClientError, originalErr error) error {
	h.logger.Info("Processing resource exhaustion error",
		"code", vlmErr.Code,
		"message", vlmErr.Message)

	return errors.NewVCPError(errors.ErrVLMZoneResourcePoolExhausted, originalErr)
}

// handleResourceExhaustionWithDetailsError handles resource exhaustion with details errors
func (h *VLMErrorHandler) handleResourceExhaustionWithDetailsError(vlmErr VLMClientError, originalErr error) error {
	h.logger.Info("Processing resource exhaustion with details error",
		"code", vlmErr.Code,
		"message", vlmErr.Message)

	return errors.NewVCPError(errors.ErrVLMZoneResourcePoolExhaustedWithDetails, originalErr)
}

// handleRateLimitError handles rate limit errors
func (h *VLMErrorHandler) handleRateLimitError(vlmErr VLMClientError, originalErr error) error {
	message := strings.ToLower(vlmErr.Message)

	h.logger.Info("Processing rate limit error",
		"code", vlmErr.Code,
		"message", vlmErr.Message)

	if strings.Contains(message, "disk") || strings.Contains(message, "provisioned") {
		return errors.NewVCPError(errors.ErrVLMDiskRateLimited, originalErr)
	}
	return errors.NewVCPError(errors.ErrVLMRateLimitExceeded, originalErr)
}

// handleCloudOperationFailed handles cloud operation failures with detailed error information
func (h *VLMErrorHandler) handleCloudOperationFailed(vlmErr VLMClientError, originalErr error) error {
	h.logger.Info("Processing cloud operation failed error",
		"http_code", vlmErr.HttpCode,
		"code", vlmErr.Code,
		"message", vlmErr.Message,
		"causes", vlmErr.Cause)

	// Check if there are specific causes that provide more detailed error information
	if len(vlmErr.Cause) > 0 {
		// Use the first cause as it usually contains the most specific error
		primaryCause := vlmErr.Cause[0]
		h.logger.Debug("Primary cause of cloud operation failure",
			"cause", primaryCause)

		// Check for specific error patterns in the cause
		primaryCauseLower := strings.ToLower(primaryCause)

		// VM creation failures
		if strings.Contains(primaryCauseLower, "failed to create vm") {
			if strings.Contains(primaryCauseLower, "unable to get image") {
				return errors.NewVCPError(errors.ErrVLMResourceNotReady,
					fmt.Errorf("VM creation failed: %s", primaryCause))
			}
			if strings.Contains(primaryCauseLower, "quota") {
				return errors.NewVCPError(errors.ErrVLMQuotaExceededGeneral,
					fmt.Errorf("VM creation failed: %s", primaryCause))
			}
			if strings.Contains(primaryCauseLower, "permission") || strings.Contains(primaryCauseLower, "access denied") {
				return errors.NewVCPError(errors.ErrVLMInsufficientPermissions,
					fmt.Errorf("VM creation failed: %s", primaryCause))
			}
			// Generic VM creation failure
			return errors.NewVCPError(errors.ErrVLMWorkflowError,
				fmt.Errorf("VM creation failed: %s", primaryCause))
		}

		// Handle "unable to get image" specifically (from your screenshot)
		if strings.Contains(primaryCauseLower, "unable to get image") {
			return errors.NewVCPError(errors.ErrVLMResourceNotReady,
				fmt.Errorf("Image retrieval failed: %s", primaryCause))
		}

		// Network-related failures
		if strings.Contains(primaryCauseLower, "network") || strings.Contains(primaryCauseLower, "subnet") {
			return errors.NewVCPError(errors.ErrVLMResourceNotAvailableInZone,
				fmt.Errorf("Network configuration failed: %s", primaryCause))
		}

		// Storage-related failures
		if strings.Contains(primaryCauseLower, "disk") || strings.Contains(primaryCauseLower, "storage") {
			if strings.Contains(primaryCauseLower, "quota") {
				return errors.NewVCPError(errors.ErrVLMQuotaExceededGeneral,
					fmt.Errorf("Storage operation failed: %s", primaryCause))
			}
			return errors.NewVCPError(errors.ErrVLMResourceNotReady,
				fmt.Errorf("Storage operation failed: %s", primaryCause))
		}

		// Return a more specific error with the cause information
		return errors.NewVCPError(errors.ErrVLMWorkflowError,
			fmt.Errorf("Cloud operation failed: %s", primaryCause))
	}

	// If no specific causes, use the main message
	return errors.NewVCPError(errors.ErrVLMWorkflowError,
		fmt.Errorf("Cloud operation failed: %s", vlmErr.Message))
}

// handleStringBasedError processes string-based error patterns
func (h *VLMErrorHandler) handleStringBasedError(errorStr string, originalErr error) error {
	errorStr = strings.ToLower(errorStr)

	h.logger.Debug("Processing string-based error",
		"error_string", errorStr)

	// Resource exhaustion/stockout patterns
	if strings.Contains(errorStr, "notfound") || strings.Contains(errorStr, "does not exist in zone") {
		return errors.NewVCPError(errors.ErrVLMResourceNotAvailableInZone, originalErr)
	}

	if strings.Contains(errorStr, "zone_resource_pool_exhausted") {
		if strings.Contains(errorStr, "with_details") {
			return errors.NewVCPError(errors.ErrVLMZoneResourcePoolExhaustedWithDetails, originalErr)
		}
		return errors.NewVCPError(errors.ErrVLMZoneResourcePoolExhausted, originalErr)
	}

	if strings.Contains(errorStr, "does not have enough resources available") {
		return errors.NewVCPError(errors.ErrVLMInsufficientResourcesInZone, originalErr)
	}

	// VM type unavailable patterns
	if strings.Contains(errorStr, "vm instance") && strings.Contains(errorStr, "unavailable in the") {
		if strings.Contains(errorStr, "because of") {
			return errors.NewVCPError(errors.ErrVLMVMTypeUnavailableWithReason, originalErr)
		}
		return errors.NewVCPError(errors.ErrVLMVMTypeUnavailableInZone, originalErr)
	}

	// Rate limit patterns
	if strings.Contains(errorStr, "resource_operation_rate_exceeded") {
		return errors.NewVCPError(errors.ErrVLMRateLimitExceeded, originalErr)
	}

	if strings.Contains(errorStr, "rate limited") {
		return errors.NewVCPError(errors.ErrVLMDiskRateLimited, originalErr)
	}

	// Resource not ready patterns
	if strings.Contains(errorStr, "not ready") {
		return errors.NewVCPError(errors.ErrVLMResourceNotReady, originalErr)
	}

	// Project constraint patterns
	if strings.Contains(errorStr, "constraint") && strings.Contains(errorStr, "violated") {
		return errors.NewVCPError(errors.ErrVLMProjectConstraintViolated, originalErr)
	}

	// CPU platform mismatch patterns
	if strings.Contains(errorStr, "cpu platform") && strings.Contains(errorStr, "must match") {
		return errors.NewVCPError(errors.ErrVLMCPUPlatformMismatch, originalErr)
	}

	// Service account access denied patterns
	if strings.Contains(errorStr, "service_account_access_denied") {
		return errors.NewVCPError(errors.ErrVLMServiceAccountAccessDenied, originalErr)
	}

	// Machine image update patterns
	if strings.Contains(errorStr, "sourcemachineimage") && strings.Contains(errorStr, "not supported") {
		return errors.NewVCPError(errors.ErrVLMInvalidMachineImageUpdate, originalErr)
	}

	// Quota patterns (fallback)
	if strings.Contains(errorStr, "quota") && strings.Contains(errorStr, "exceeded") {
		if strings.Contains(errorStr, "region") {
			return errors.NewVCPError(errors.ErrVLMQuotaExceededRegional, originalErr)
		}
		if strings.Contains(errorStr, "zone") {
			return errors.NewVCPError(errors.ErrVLMQuotaExceededZonal, originalErr)
		}
		return errors.NewVCPError(errors.ErrVLMQuotaExceededGeneral, originalErr)
	}

	// Permission/authorization patterns
	if strings.Contains(errorStr, "permission denied") || strings.Contains(errorStr, "unauthorized") {
		return errors.NewVCPError(errors.ErrVLMInsufficientPermissions, originalErr)
	}

	// Conflict patterns (resource already exists, in use, etc.)
	if strings.Contains(errorStr, "resource already exists") || strings.Contains(errorStr, "already exists") {
		return errors.NewVCPError(errors.ErrGCPResourceAlreadyExistsError, originalErr)
	}

	// Bad request patterns (CPU platform mismatch, invalid machine image, etc.)
	if strings.Contains(errorStr, "cpu platform mismatch") {
		return errors.NewVCPError(errors.ErrVLMCPUPlatformMismatch, originalErr)
	}

	if strings.Contains(errorStr, "invalid value for field") && strings.Contains(errorStr, "not supported") {
		return errors.NewVCPError(errors.ErrVLMInvalidMachineImageUpdate, originalErr)
	}

	// Not found patterns
	if strings.Contains(errorStr, "resource not found in zone") || strings.Contains(errorStr, "not found in zone") {
		return errors.NewVCPError(errors.ErrVLMResourceNotAvailableInZone, originalErr)
	}

	// Default to generic VLM workflow error
	h.logger.Warn("No specific error pattern matched, using generic VLM workflow error",
		"error_string", errorStr)
	return errors.NewVCPError(errors.ErrVLMWorkflowError, originalErr)
}

// Helper methods for error classification

// isRetryableError checks if an error message indicates a retryable error
func (h *VLMErrorHandler) isRetryableError(errorMsg string) bool {
	retryablePatterns := []string{
		"timeout",
		"temporary",
		"retry",
		"unavailable",
		"busy",
		"overloaded",
		"rate limit",
		"throttle",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errorMsg, pattern) {
			return true
		}
	}
	return false
}

// isQuotaError checks if an error message indicates a quota error
func (h *VLMErrorHandler) isQuotaError(errorMsg string) bool {
	quotaPatterns := []string{
		"quota",
		"limit exceeded",
		"resource exhausted",
	}

	for _, pattern := range quotaPatterns {
		if strings.Contains(errorMsg, pattern) {
			return true
		}
	}
	return false
}

// isPermissionError checks if an error message indicates a permission error
func (h *VLMErrorHandler) isPermissionError(errorMsg string) bool {
	permissionPatterns := []string{
		"permission",
		"forbidden",
		"unauthorized",
		"access denied",
		"insufficient",
	}

	for _, pattern := range permissionPatterns {
		if strings.Contains(errorMsg, pattern) {
			return true
		}
	}
	return false
}
