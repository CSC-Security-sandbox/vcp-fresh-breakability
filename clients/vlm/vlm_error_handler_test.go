package vlm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/temporal"
)

// testError is a simple error implementation for testing
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestVLMErrorHandler_HandleVLMError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name     string
		err      error
		expected int // Expected error tracking ID
	}{
		{
			name:     "Non-Temporal Error",
			err:      &testError{msg: "some error"},
			expected: errors.ErrVLMWorkflowError,
		},
		{
			name:     "Non-VLM Temporal Error",
			err:      temporal.NewApplicationError("some error", "SomeOtherError"),
			expected: errors.ErrVLMWorkflowError,
		},
		{
			name:     "VLM Error with External=false",
			err:      createVLMApplicationError(VLMClientError{External: false, Message: "internal error"}, "internal error"),
			expected: errors.ErrVLMWorkflowError,
		},
		{
			name:     "VLM Error with External=true and HTTP 429",
			err:      createVLMApplicationError(VLMClientError{External: true, HttpCode: 429, Message: "Quota exceeded in region us-central1"}, "quota error"),
			expected: errors.ErrVLMQuotaExceededRegional,
		},
		{
			name:     "VLM Error with External=true and HTTP 403",
			err:      createVLMApplicationError(VLMClientError{External: true, HttpCode: 403, Message: "Permission denied"}, "permission error"),
			expected: errors.ErrVLMInsufficientPermissions,
		},
		{
			name:     "VLM Error with External=true and HTTP 409",
			err:      createVLMApplicationError(VLMClientError{External: true, HttpCode: 409, Message: "Resource already exists"}, "conflict error"),
			expected: errors.ErrGCPResourceAlreadyExistsError,
		},
		{
			name:     "VLM Error with External=true and HTTP 400",
			err:      createVLMApplicationError(VLMClientError{External: true, HttpCode: 400, Message: "CPU platform mismatch"}, "bad request error"),
			expected: errors.ErrVLMCPUPlatformMismatch,
		},
		{
			name:     "VLM Error with External=true and HTTP 404",
			err:      createVLMApplicationError(VLMClientError{External: true, HttpCode: 404, Message: "Resource not found in zone"}, "not found error"),
			expected: errors.ErrVLMResourceNotAvailableInZone,
		},
		{
			name:     "VLM Error with External=true and HTTP 401",
			err:      createVLMApplicationError(VLMClientError{External: true, HttpCode: 401, Message: "Unauthorized"}, "unauthorized error"),
			expected: errors.ErrVLMInsufficientPermissions,
		},
		{
			name:     "VLM Error with External=true and QUOTA_EXCEEDED code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "QUOTA_EXCEEDED", Message: "Quota exceeded in region us-central1"}, "quota error"),
			expected: errors.ErrVLMQuotaExceededRegional,
		},
		{
			name:     "VLM Error with External=true and ZONE_RESOURCE_POOL_EXHAUSTED code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "ZONE_RESOURCE_POOL_EXHAUSTED", Message: "Zone resource pool exhausted"}, "resource exhaustion error"),
			expected: errors.ErrVLMZoneResourcePoolExhausted,
		},
		{
			name:     "VLM Error with External=true and ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS", Message: "Zone resource pool exhausted with details"}, "resource exhaustion with details error"),
			expected: errors.ErrVLMZoneResourcePoolExhaustedWithDetails,
		},
		{
			name:     "VLM Error with External=true and RESOURCE_OPERATION_RATE_EXCEEDED code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "RESOURCE_OPERATION_RATE_EXCEEDED", Message: "Rate limit exceeded"}, "rate limit error"),
			expected: errors.ErrVLMRateLimitExceeded,
		},
		{
			name:     "VLM Error with External=true and SERVICE_ACCOUNT_ACCESS_DENIED code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "SERVICE_ACCOUNT_ACCESS_DENIED", Message: "Service account access denied"}, "service account error"),
			expected: errors.ErrVLMServiceAccountAccessDenied,
		},
		{
			name:     "VLM Error with External=true and RESOURCE_NOT_READY code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "RESOURCE_NOT_READY", Message: "Resource not ready"}, "resource not ready error"),
			expected: errors.ErrVLMResourceNotReady,
		},
		{
			name:     "VLM Error with External=true and PROJECT_CONSTRAINT_VIOLATED code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "PROJECT_CONSTRAINT_VIOLATED", Message: "Project constraint violated"}, "constraint error"),
			expected: errors.ErrVLMProjectConstraintViolated,
		},
		{
			name:     "VLM Error with External=true and CPU_PLATFORM_MISMATCH code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "CPU_PLATFORM_MISMATCH", Message: "CPU platform mismatch"}, "cpu platform error"),
			expected: errors.ErrVLMCPUPlatformMismatch,
		},
		{
			name:     "VLM Error with External=true and INVALID_MACHINE_IMAGE_UPDATE code",
			err:      createVLMApplicationError(VLMClientError{External: true, Code: "INVALID_MACHINE_IMAGE_UPDATE", Message: "Invalid machine image update"}, "machine image error"),
			expected: errors.ErrVLMInvalidMachineImageUpdate,
		},
		{
			name:     "VLM Error with External=true and string-based quota error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "Quota 'CPUS' exceeded. Limit: 8 in zone us-central1-a"}, "quota error"),
			expected: errors.ErrVLMQuotaExceededZonal,
		},
		{
			name:     "VLM Error with External=true and string-based resource exhaustion error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "ZONE_RESOURCE_POOL_EXHAUSTED"}, "resource exhaustion error"),
			expected: errors.ErrVLMZoneResourcePoolExhausted,
		},
		{
			name:     "VLM Error with External=true and string-based VM unavailable error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "A n2-standard-4 VM instance with 4 vCPUs is currently unavailable in the us-central1-a zone"}, "vm unavailable error"),
			expected: errors.ErrVLMVMTypeUnavailableInZone,
		},
		{
			name:     "VLM Error with External=true and string-based rate limit error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "Disk cannot be resized due to being rate limited"}, "rate limit error"),
			expected: errors.ErrVLMDiskRateLimited,
		},
		{
			name:     "VLM Error with External=true and string-based resource not ready error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "The resource 'projects/project/regions/region/subnetworks/default' is not ready"}, "resource not ready error"),
			expected: errors.ErrVLMResourceNotReady,
		},
		{
			name:     "VLM Error with External=true and string-based constraint error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "Constraint constraints/compute.vmExternalIpAccess violated for projects/project"}, "constraint error"),
			expected: errors.ErrVLMProjectConstraintViolated,
		},
		{
			name:     "VLM Error with External=true and string-based CPU platform error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "The selected machine type (n2-standard-4) has a required CPU platform of Intel Haswell. The minimum CPU platform must match this, but was Intel Skylake"}, "cpu platform error"),
			expected: errors.ErrVLMCPUPlatformMismatch,
		},
		{
			name:     "VLM Error with External=true and string-based service account error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "SERVICE_ACCOUNT_ACCESS_DENIED"}, "service account error"),
			expected: errors.ErrVLMServiceAccountAccessDenied,
		},
		{
			name:     "VLM Error with External=true and string-based machine image error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "Invalid value for field 'resource.sourceMachineImage': Updating 'sourceMachineImage' is not supported"}, "machine image error"),
			expected: errors.ErrVLMInvalidMachineImageUpdate,
		},
		{
			name:     "VLM Error with External=true and unknown error",
			err:      createVLMApplicationError(VLMClientError{External: true, Message: "Some unknown error"}, "unknown error"),
			expected: errors.ErrVLMWorkflowError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.HandleVLMError(tt.err)

			var customErr *errors.CustomError
			if !errors.As(result, &customErr) {
				t.Fatalf("Expected CustomError, got %T", result)
			}

			if customErr.TrackingID != tt.expected {
				t.Errorf("Expected error tracking ID %d, got %d", tt.expected, customErr.TrackingID)
			}
		})
	}
}

func TestVLMErrorHandler_HandleVLMError_NilError(t *testing.T) {
	handler := NewVLMErrorHandler()

	// Test handling nil error
	result := handler.HandleVLMError(nil)
	assert.Nil(t, result)
}

func TestVLMErrorHandler_HandleVLMError_ChildWorkflowError(t *testing.T) {
	handler := NewVLMErrorHandler()

	// Test child workflow execution error with underlying VLM error
	vlmErr := VLMClientError{
		HttpCode: 429,
		Code:     "QUOTA_EXCEEDED",
		Message:  "Quota exceeded in region us-central1",
		External: true,
	}

	appErr := temporal.NewApplicationError("VLM error", ErrorTypeVLMClientError, vlmErr)

	// Test the VLM error directly since we can't easily mock the child workflow error chain
	result := handler.HandleVLMError(appErr)

	var customErr *errors.CustomError
	assert.True(t, errors.As(result, &customErr))
	// The quota error should be handled by the VLM code, not HTTP status
	assert.Equal(t, errors.ErrVLMQuotaExceededRegional, customErr.TrackingID)
}

func TestVLMErrorHandler_HandleVLMError_ChildWorkflowErrorNoUnderlyingError(t *testing.T) {
	handler := NewVLMErrorHandler()

	// Create a mock error that simulates the child workflow error
	mockErr := fmt.Errorf("child workflow execution error (type: TestWorkflow, workflowID: test-id, runID: test-run-id): child workflow failed")

	result := handler.HandleVLMError(mockErr)

	var customErr *errors.CustomError
	assert.True(t, errors.As(result, &customErr))
	assert.Equal(t, errors.ErrVLMWorkflowError, customErr.TrackingID)
}

func TestVLMErrorHandler_HandleVLMError_VLMErrorNoDetails(t *testing.T) {
	handler := NewVLMErrorHandler()

	// Test VLM temporal error without details
	appErr := temporal.NewApplicationError("VLM error without details", ErrorTypeVLMClientError)

	result := handler.HandleVLMError(appErr)

	var customErr *errors.CustomError
	assert.True(t, errors.As(result, &customErr))
	assert.Equal(t, errors.ErrVLMWorkflowError, customErr.TrackingID)
}

func TestVLMErrorHandler_HandleNonTemporalError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name     string
		err      error
		expected int
	}{
		{
			name:     "Retryable error",
			err:      fmt.Errorf("temporary failure, please retry"),
			expected: errors.ErrVLMWorkflowError,
		},
		{
			name:     "Quota error",
			err:      fmt.Errorf("quota exceeded"),
			expected: errors.ErrVLMQuotaExceededGeneral,
		},
		{
			name:     "Permission error",
			err:      fmt.Errorf("permission denied"),
			expected: errors.ErrVLMInsufficientPermissions,
		},
		{
			name:     "Unknown error",
			err:      fmt.Errorf("some unknown error"),
			expected: errors.ErrVLMWorkflowError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.HandleVLMError(tt.err)

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expected, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleServerError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
		expectedRetry bool
	}{
		{
			name: "Server error with CLOUD_OPERATION_FAILED code",
			vlmErr: VLMClientError{
				HttpCode:  502,
				Code:      "CLOUD_OPERATION_FAILED",
				Message:   "Cloud operation failed",
				Retryable: true,
				Cause:     []string{"VM creation failed"},
			},
			expectedError: errors.ErrVLMWorkflowError,
			expectedRetry: true,
		},
		{
			name: "Server error with cause information",
			vlmErr: VLMClientError{
				HttpCode:  500,
				Message:   "Internal server error",
				Retryable: false,
				Cause:     []string{"Database connection failed"},
			},
			expectedError: errors.ErrVLMWorkflowError,
			expectedRetry: false,
		},
		{
			name: "Server error without cause, retryable",
			vlmErr: VLMClientError{
				HttpCode:  503,
				Message:   "Service unavailable",
				Retryable: true,
			},
			expectedError: errors.ErrVLMWorkflowError,
			expectedRetry: true,
		},
		{
			name: "Server error without cause, not retryable",
			vlmErr: VLMClientError{
				HttpCode:  500,
				Message:   "Internal error",
				Retryable: false,
			},
			expectedError: errors.ErrVLMWorkflowError,
			expectedRetry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleServerError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleCloudOperationFailed(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "VM creation failure",
			vlmErr: VLMClientError{
				Message: "Cloud operation failed",
				Cause:   []string{"failed to create VM"},
			},
			expectedError: errors.ErrVLMWorkflowError,
		},
		{
			name: "Image retrieval failure",
			vlmErr: VLMClientError{
				Message: "Cloud operation failed",
				Cause:   []string{"unable to get image"},
			},
			expectedError: errors.ErrVLMResourceNotReady,
		},
		{
			name: "Network failure",
			vlmErr: VLMClientError{
				Message: "Cloud operation failed",
				Cause:   []string{"network configuration failed"},
			},
			expectedError: errors.ErrVLMResourceNotAvailableInZone,
		},
		{
			name: "Storage failure",
			vlmErr: VLMClientError{
				Message: "Cloud operation failed",
				Cause:   []string{"disk creation failed"},
			},
			expectedError: errors.ErrVLMResourceNotReady,
		},
		{
			name: "Generic cloud operation failure",
			vlmErr: VLMClientError{
				Message: "Cloud operation failed",
				Cause:   []string{"some other failure"},
			},
			expectedError: errors.ErrVLMWorkflowError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleCloudOperationFailed(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleQuotaError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "Regional quota exceeded",
			vlmErr: VLMClientError{
				Message: "Quota exceeded in region us-central1",
			},
			expectedError: errors.ErrVLMQuotaExceededRegional,
		},
		{
			name: "Zonal quota exceeded",
			vlmErr: VLMClientError{
				Message: "Quota exceeded in zone us-central1-a",
			},
			expectedError: errors.ErrVLMQuotaExceededZonal,
		},
		{
			name: "General quota exceeded",
			vlmErr: VLMClientError{
				Message: "Quota exceeded",
			},
			expectedError: errors.ErrVLMQuotaExceededGeneral,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleQuotaError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleResourceExhaustionError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "Zone resource pool exhausted",
			vlmErr: VLMClientError{
				Message: "Zone resource pool exhausted",
			},
			expectedError: errors.ErrVLMZoneResourcePoolExhausted,
		},
		{
			name: "Zone resource pool exhausted with details",
			vlmErr: VLMClientError{
				Message: "Zone resource pool exhausted with details",
			},
			expectedError: errors.ErrVLMZoneResourcePoolExhausted,
		},
		{
			name: "Insufficient resources in zone",
			vlmErr: VLMClientError{
				Message: "Insufficient resources in zone",
			},
			expectedError: errors.ErrVLMZoneResourcePoolExhausted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleResourceExhaustionError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleRateLimitError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "Resource operation rate exceeded",
			vlmErr: VLMClientError{
				Message: "Resource operation rate exceeded",
			},
			expectedError: errors.ErrVLMRateLimitExceeded,
		},
		{
			name: "Disk rate limited",
			vlmErr: VLMClientError{
				Message: "Disk rate limited",
			},
			expectedError: errors.ErrVLMDiskRateLimited,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleRateLimitError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandlePermissionError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "Insufficient permissions",
			vlmErr: VLMClientError{
				Message: "Insufficient permissions",
			},
			expectedError: errors.ErrVLMInsufficientPermissions,
		},
		{
			name: "Service account access denied",
			vlmErr: VLMClientError{
				Message: "Service account access denied",
			},
			expectedError: errors.ErrVLMInsufficientPermissions,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handlePermissionError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleConflictError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "Resource already exists",
			vlmErr: VLMClientError{
				Message: "Resource already exists",
			},
			expectedError: errors.ErrGCPResourceAlreadyExistsError,
		},
		{
			name: "Resource in use",
			vlmErr: VLMClientError{
				Message: "Resource in use",
			},
			expectedError: errors.ErrGCPResourceAlreadyExistsError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleConflictError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleBadRequestError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "CPU platform mismatch",
			vlmErr: VLMClientError{
				HttpCode: 400,
				Message:  "CPU platform mismatch",
			},
			expectedError: errors.ErrVLMCPUPlatformMismatch,
		},
		{
			name: "Invalid machine image update",
			vlmErr: VLMClientError{
				HttpCode: 400,
				Message:  "Invalid value for field 'resource.sourceMachineImage': Updating 'sourceMachineImage' is not supported",
			},
			expectedError: errors.ErrVLMInvalidMachineImageUpdate,
		},
		{
			name: "Generic bad request",
			vlmErr: VLMClientError{
				HttpCode: 400,
				Message:  "Some other bad request",
			},
			expectedError: errors.ErrBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleBadRequestError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleNotFoundError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "Resource not found in zone",
			vlmErr: VLMClientError{
				Message: "Resource not found in zone",
			},
			expectedError: errors.ErrVLMResourceNotAvailableInZone,
		},
		{
			name: "VM type unavailable in zone",
			vlmErr: VLMClientError{
				Message: "VM type unavailable in zone",
			},
			expectedError: errors.ErrVLMResourceNotAvailableInZone,
		},
		{
			name: "Generic not found",
			vlmErr: VLMClientError{
				Message: "Some other not found",
			},
			expectedError: errors.ErrVLMResourceNotAvailableInZone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleNotFoundError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleUnauthorizedError(t *testing.T) {
	handler := NewVLMErrorHandler()

	result := handler.handleUnauthorizedError(VLMClientError{
		Message: "Unauthorized",
	}, fmt.Errorf("original error"))

	var customErr *errors.CustomError
	assert.True(t, errors.As(result, &customErr))
	assert.Equal(t, errors.ErrVLMInsufficientPermissions, customErr.TrackingID)
}

func TestVLMErrorHandler_HandleQuotaOrRateLimitError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		vlmErr        VLMClientError
		expectedError int
	}{
		{
			name: "Quota exceeded",
			vlmErr: VLMClientError{
				Message: "Quota exceeded",
			},
			expectedError: errors.ErrVLMQuotaExceededGeneral,
		},
		{
			name: "Rate limit exceeded",
			vlmErr: VLMClientError{
				Message: "Rate limit exceeded",
			},
			expectedError: errors.ErrVLMRateLimitExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleQuotaOrRateLimitError(tt.vlmErr, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_HandleStringBasedError(t *testing.T) {
	handler := NewVLMErrorHandler()

	tests := []struct {
		name          string
		message       string
		expectedError int
	}{
		{
			name:          "Quota exceeded pattern",
			message:       "Quota 'CPUS' exceeded. Limit: 8 in zone us-central1-a",
			expectedError: errors.ErrVLMQuotaExceededZonal,
		},
		{
			name:          "Resource exhaustion pattern",
			message:       "ZONE_RESOURCE_POOL_EXHAUSTED",
			expectedError: errors.ErrVLMZoneResourcePoolExhausted,
		},
		{
			name:          "VM unavailable pattern",
			message:       "A n2-standard-4 VM instance with 4 vCPUs is currently unavailable in the us-central1-a zone",
			expectedError: errors.ErrVLMVMTypeUnavailableInZone,
		},
		{
			name:          "Rate limit pattern",
			message:       "Disk cannot be resized due to being rate limited",
			expectedError: errors.ErrVLMDiskRateLimited,
		},
		{
			name:          "Resource not ready pattern",
			message:       "The resource 'projects/project/regions/region/subnetworks/default' is not ready",
			expectedError: errors.ErrVLMResourceNotReady,
		},
		{
			name:          "Constraint violation pattern",
			message:       "Constraint constraints/compute.vmExternalIpAccess violated for projects/project",
			expectedError: errors.ErrVLMProjectConstraintViolated,
		},
		{
			name:          "CPU platform pattern",
			message:       "The selected machine type (n2-standard-4) has a required CPU platform of Intel Haswell. The minimum CPU platform must match this, but was Intel Skylake",
			expectedError: errors.ErrVLMCPUPlatformMismatch,
		},
		{
			name:          "Service account pattern",
			message:       "SERVICE_ACCOUNT_ACCESS_DENIED",
			expectedError: errors.ErrVLMServiceAccountAccessDenied,
		},
		{
			name:          "Machine image pattern",
			message:       "Invalid value for field 'resource.sourceMachineImage': Updating 'sourceMachineImage' is not supported",
			expectedError: errors.ErrVLMInvalidMachineImageUpdate,
		},
		{
			name:          "Unknown pattern",
			message:       "Some unknown error message",
			expectedError: errors.ErrVLMWorkflowError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.handleStringBasedError(tt.message, fmt.Errorf("original error"))

			var customErr *errors.CustomError
			assert.True(t, errors.As(result, &customErr))
			assert.Equal(t, tt.expectedError, customErr.TrackingID)
		})
	}
}

func TestVLMErrorHandler_ExtractVLMClientError(t *testing.T) {
	handler := NewVLMErrorHandler()

	// Test extracting VLMClientError from temporal application error
	vlmErr := VLMClientError{
		HttpCode:  429,
		Code:      "QUOTA_EXCEEDED",
		Message:   "Quota exceeded in region us-central1",
		Component: "compute",
		Retryable: false,
		External:  true,
		Cause:     []string{"original error"},
	}

	appErr := createVLMApplicationError(vlmErr, "quota error")
	result := handler.HandleVLMError(appErr)

	var customErr *errors.CustomError
	assert.True(t, errors.As(result, &customErr))
	assert.Equal(t, errors.ErrVLMQuotaExceededRegional, customErr.TrackingID)
}

// Helper function to create a VLM temporal application error
func createVLMApplicationError(vlmErr VLMClientError, message string) error {
	return temporal.NewApplicationError(message, ErrorTypeVLMClientError, vlmErr)
}

func TestVLMErrorHandler_HandleRetryScenario(t *testing.T) {
	handler := NewVLMErrorHandler()

	// Create a VLM client error with "unable to get image" message
	vlmErr := VLMClientError{
		HttpCode:  502,
		Code:      "CLOUD_OPERATION_FAILED",
		Message:   "Cloud operation failed to complete",
		Component: "cloud_manager",
		External:  false,
		Retryable: true,
		Cause:     []string{"Failed to create VM request: unable to get image (type: VLMError, retryable: true)"},
	}

	// Create a temporal application error with VLM client error details
	appErr := temporal.NewApplicationError("VLM error", ErrorTypeVLMClientError, vlmErr)

	// Handle the error
	result := handler.HandleVLMError(appErr)

	// Verify the result - should extract the actual "unable to get image" error
	assert.Error(t, result)
	var customErr *errors.CustomError
	assert.True(t, errors.As(result, &customErr))

	// Should map to ErrVLMResourceNotReady because of "unable to get image" in the cause
	assert.Equal(t, errors.ErrVLMResourceNotReady, customErr.TrackingID)

	// The error message should contain the actual error about VM creation failure
	assert.Contains(t, customErr.OriginalErr.Error(), "VM creation failed")
}

func TestVLMErrorHandler_NewVLMErrorHandler(t *testing.T) {
	// Test default constructor
	handler := NewVLMErrorHandler()
	assert.NotNil(t, handler)
	assert.NotNil(t, handler.logger)
}

func TestVLMErrorHandler_NewVLMErrorHandlerWithLogger(t *testing.T) {
	// Test constructor with custom logger
	mockLog := log.NewLogger()
	handler := NewVLMErrorHandlerWithLogger(mockLog)

	assert.NotNil(t, handler)
	assert.Equal(t, mockLog, handler.logger)
}
