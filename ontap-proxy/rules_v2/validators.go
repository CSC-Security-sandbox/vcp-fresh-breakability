package rules_v2

import (
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/dsl"
)

// ValidateVolumeCreationWithCore validates volume creation request with core API.
// Returns (true, "") if validation passes, or (false, "reason") if it fails.
//
// TODO: Implement actual API call to core service
//
// Expected implementation:
//   - Extract relevant fields from request body (size, name, svm, etc.)
//   - Make API call to core service endpoint
//   - Return (true, "") if core approves the volume creation
//   - Return (false, "reason from core") if core denies
func ValidateVolumeCreationWithCore(r *http.Request) (bool, string) {
	// Placeholder implementation - always returns true
	// TODO: Implement actual core API validation
	//
	// Example implementation:
	//
	// 1. Parse request body to extract volume details
	// body, _ := io.ReadAll(r.Body)
	// r.Body = io.NopCloser(bytes.NewReader(body)) // restore body
	//
	// 2. Extract auth/context info from request headers
	// authToken := r.Header.Get("Authorization")
	//
	// 3. Call core API: POST /api/v1/volumes/validate
	// resp, err := coreClient.ValidateVolume(ctx, volumeDetails)
	// if err != nil {
	//     return false, fmt.Sprintf("Core API error: %v", err)
	// }
	//
	// 4. Check response for approval/denial
	// if !resp.Allowed {
	//     return false, resp.Reason // e.g., "Quota exceeded: only 10GB remaining"
	// }
	//
	// 5. Return result
	// return true, ""

	return true, ""
}

// ValidateVolumeModificationWithCore validates volume modification request with core API.
// Returns (true, "") if validation passes, or (false, "reason") if it fails.
//
// TODO: Implement actual API call to core service
func ValidateVolumeModificationWithCore(r *http.Request) (bool, string) {
	// Placeholder implementation - always returns true
	// TODO: Implement actual core API validation
	return true, ""
}

// ValidateVolumeDeletionWithCore validates volume deletion request with core API.
// Returns (true, "") if validation passes, or (false, "reason") if it fails.
//
// TODO: Implement actual API call to core service
func ValidateVolumeDeletionWithCore(r *http.Request) (bool, string) {
	// Placeholder implementation - always returns true
	// TODO: Implement actual core API validation
	return true, ""
}

// WrapValidator wraps a simple bool-returning validator into a Condition.
// Useful for validators that don't need to return specific error messages.
func WrapValidator(validator func(r *http.Request) bool, failureReason string) dsl.Condition {
	return func(r *http.Request) (bool, string) {
		if validator(r) {
			return true, ""
		}
		return false, failureReason
	}
}
