package rules_v2

import (
	"fmt"
	"net/http"
	"strings"

	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	core "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/coreapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
	proxyutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	submitExpertModeVolumeOperation      = core.SubmitExpertModeVolumeOperation
	submitExpertModeVolumeRename         = core.SubmitExpertModeVolumeRename
	validateVolumeCreation               = _validateVolumeCreation
	validateVolumeModification           = _validateVolumeModification
	validateVolumeDeletion               = _validateVolumeDeletion
	validatePrivateCLIVolumeCreation     = _validatePrivateCLIVolumeCreation
	validatePrivateCLIVolumeModification = _validatePrivateCLIVolumeModification
	validatePrivateCLIVolumeDeletion     = _validatePrivateCLIVolumeDeletion
	validatePrivateCLIVolumeRename        = _validatePrivateCLIVolumeRename
)

type VolumeRequestFields struct {
	VolumeName  string
	SizeInBytes float64
	SvmUuid     coreapi.OptString
	SvmName     coreapi.OptString
}

func parseVolumeRequestFields(requestBody map[string]interface{}) VolumeRequestFields {
	fields := VolumeRequestFields{}
	if requestBody != nil {
		fields.VolumeName, _ = requestBody["name"].(string)
		fields.SizeInBytes = parseSize(requestBody["size"])
		if svm, ok := requestBody["svm"].(map[string]interface{}); ok {
			if uuid, ok := svm["uuid"].(string); ok && uuid != "" {
				fields.SvmUuid = coreapi.NewOptString(uuid)
			}
			if name, ok := svm["name"].(string); ok && name != "" {
				fields.SvmName = coreapi.NewOptString(name)
			}
		}
	}
	return fields
}

func _validateVolumeCreation(r *http.Request) (bool, string) {
	logger := util.GetLogger(r.Context())
	requestBody, parseErr := dsl.GetParsedBody(r)
	if parseErr != "" {
		return false, parseErr
	}

	cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
	if cacheKey == "" {
		return false, "cache key not found in context"
	}
	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return false, fmt.Sprintf("auth data not found in cache for key: %s", cacheKey)
	}

	fields := parseVolumeRequestFields(requestBody)

	// Reject invalid size (parsed to 0)
	if fields.SizeInBytes == 0 {
		orig := requestBody["size"]
		return false, fmt.Sprintf("\"%v\" is an invalid value for field \"size\"", orig)
	}

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionCreate,
		VolumeName:    fields.VolumeName,
		SizeInBytes:   fields.SizeInBytes,
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		SvmUuid:       fields.SvmUuid,
		SvmName:       fields.SvmName,
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func _validateVolumeModification(r *http.Request) (bool, string) {
	logger := util.GetLogger(r.Context())
	requestBody, parseErr := dsl.GetParsedBody(r)
	if parseErr != "" {
		return false, parseErr
	}

	cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
	if cacheKey == "" {
		return false, "cache key not found in context"
	}
	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return false, fmt.Sprintf("auth data not found in cache for key: %s", cacheKey)
	}

	fields := parseVolumeRequestFields(requestBody)
	volumeUUID := extractVolumeUUIDFromRequest(r)

	// Trigger reconcile only if name or size is being modified
	_, nameExists := requestBody["name"]
	sizeValue, sizeExists := requestBody["size"]

	if sizeExists {
		if fields.SizeInBytes == 0 {
			return false, fmt.Sprintf("\"%v\" is an invalid value for field \"size\"", sizeValue)
		}
	}

	if !nameExists && !sizeExists {
		return true, ""
	}

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionUpdate,
		VolumeName:    fields.VolumeName,
		SizeInBytes:   fields.SizeInBytes,
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		SvmUuid:       fields.SvmUuid,
		SvmName:       fields.SvmName,
		VolumeUUID:    volumeUUID,
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func _validateVolumeDeletion(r *http.Request) (bool, string) {
	logger := util.GetLogger(r.Context())
	cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
	if cacheKey == "" {
		return false, "cache key not found in context"
	}

	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return false, fmt.Sprintf("auth data not found in cache for key: %s", cacheKey)
	}

	volumeUUID := extractVolumeUUIDFromRequest(r)

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionDelete,
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		VolumeUUID:    volumeUUID,
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func _validatePrivateCLIVolumeCreation(r *http.Request) (bool, string) {
	logger := util.GetLogger(r.Context())
	requestBody, parseErr := dsl.GetParsedBody(r)
	if parseErr != "" {
		return false, parseErr
	}

	cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
	if cacheKey == "" {
		return false, "cache key not found in context"
	}
	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return false, fmt.Sprintf("auth data not found in cache for key: %s", cacheKey)
	}

	fields := parsePrivateCLIVolumeRequestFields(r, requestBody)

	// Reject invalid size (parsed to 0)
	if fields.SizeInBytes == 0 {
		orig := requestBody["size"]
		return false, fmt.Sprintf("\"%v\" is an invalid value for field \"size\"", orig)
	}

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionCreate,
		VolumeName:    fields.VolumeName,
		SizeInBytes:   fields.SizeInBytes,
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		SvmName:       fields.SvmName,
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func _validatePrivateCLIVolumeModification(r *http.Request) (bool, string) {
	logger := util.GetLogger(r.Context())
	requestBody, parseErr := dsl.GetParsedBody(r)
	if parseErr != "" {
		return false, parseErr
	}

	cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
	if cacheKey == "" {
		return false, "cache key not found in context"
	}
	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return false, fmt.Sprintf("auth data not found in cache for key: %s", cacheKey)
	}

	fields := parsePrivateCLIVolumeRequestFields(r, requestBody)

	// Trigger reconcile only if volume size is being modified
	sizeValue, sizeExists := requestBody["size"]
	if !sizeExists {
		return true, ""
	}
	if fields.SizeInBytes == 0 {
		return false, fmt.Sprintf("\"%v\" is an invalid value for field \"size\"", sizeValue)
	}

	volumeName, svmName := extractVolumeFromPrivateCLIRequest(r)
	if volumeName == "" || svmName == "" {
		return false, "missing required query parameters: vserver and volume"
	}

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionUpdate,
		VolumeName:    volumeName,
		SizeInBytes:   fields.SizeInBytes,
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		SvmName:       coreapi.NewOptString(svmName),
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func _validatePrivateCLIVolumeDeletion(r *http.Request) (bool, string) {
	logger := util.GetLogger(r.Context())
	cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
	if cacheKey == "" {
		return false, "cache key not found in context"
	}
	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return false, fmt.Sprintf("auth data not found in cache for key: %s", cacheKey)
	}

	volumeName, svmName := extractVolumeFromPrivateCLIRequest(r)
	if volumeName == "" || svmName == "" {
		return false, "missing required query parameters: vserver and volume"
	}

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionDelete,
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		VolumeName:    volumeName,
		SvmName:       coreapi.NewOptString(svmName),
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}

	return true, ""
}

// _validatePrivateCLIVolumeRename validates volume rename via the private CLI API.
// Query params: vserver, volume. Body: newname. Submits to core API (expert mode volume rename).
func _validatePrivateCLIVolumeRename(r *http.Request) (bool, string) {
	logger := util.GetLogger(r.Context())
	requestBody, parseErr := dsl.GetParsedBody(r)
	if parseErr != "" {
		return false, parseErr
	}

	cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
	if cacheKey == "" {
		return false, "cache key not found in context"
	}
	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return false, fmt.Sprintf("auth data not found in cache for key: %s", cacheKey)
	}

	volumeName, svmName := extractVolumeFromPrivateCLIRequest(r)
	if volumeName == "" || svmName == "" {
		return false, "missing required query parameters: vserver and volume"
	}

	newName, _ := requestBody["newname"].(string)
	if newName == "" {
		return false, "missing required field \"newname\" in request body"
	}

	renameRequest := &coreapi.ExpertModeVolumeRenameV1{
		Name:          newName,
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		SvmName:       svmName,
	}
	params := coreapi.V1ExpertModeVolumeRenameParams{
		Name: volumeName,
	}

	if err := submitExpertModeVolumeRename(r.Context(), renameRequest, params, "", logger); err != nil {
		return false, err.Error()
	}

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

func extractVolumeUUIDFromRequest(r *http.Request) coreapi.OptString {
	pathParts := strings.Split(strings.TrimSuffix(r.URL.Path, "/"), "/")
	if len(pathParts) > 0 {
		lastSegment := pathParts[len(pathParts)-1]
		if lastSegment != "" && lastSegment != "volumes" {
			return coreapi.NewOptString(lastSegment)
		}
	}
	return coreapi.OptString{}
}

// parsePrivateCLIVolumeRequestFields extracts volume request fields from private CLI API format.
// Private CLI uses: volume (not name), vserver (not svm.name), and size in body for POST;
// for PATCH, vserver and volume are in query params, size and other fields in body.
// REST converts CLI hyphens to underscores (e.g. space_guarantee).
// requestBody must be the already-parsed body from the caller (parse once, pass in) to avoid re-reading the request.
func parsePrivateCLIVolumeRequestFields(r *http.Request, requestBody map[string]interface{}) VolumeRequestFields {
	fields := VolumeRequestFields{}
	query := r.URL.Query()

	// Use body first when provided (POST has body; PATCH may have body)
	if requestBody != nil {
		if v, ok := requestBody["volume"].(string); ok && v != "" {
			fields.VolumeName = v
		}
		if v, ok := requestBody["vserver"].(string); ok && v != "" {
			fields.SvmName = coreapi.NewOptString(v)
		}
		fields.SizeInBytes = parseSize(requestBody["size"])
	}

	// For PATCH/DELETE, keys come from query; query overrides if body was empty
	if fields.VolumeName == "" {
		if v := query.Get("volume"); v != "" {
			fields.VolumeName = v
		}
	}
	if !fields.SvmName.IsSet() {
		if v := query.Get("vserver"); v != "" {
			fields.SvmName = coreapi.NewOptString(v)
		}
	}

	return fields
}

// extractVolumeFromPrivateCLIRequest returns volume name and SVM name from private CLI query params.
// Used for PATCH and DELETE where identity is specified via vserver and volume query parameters.
func extractVolumeFromPrivateCLIRequest(r *http.Request) (volumeName, svmName string) {
	query := r.URL.Query()
	return query.Get("volume"), query.Get("vserver")
}

// parseSize parses a size that may be a float64 (bytes) or a string like "10GB" into bytes.
// Supports units: K/KB, M/MB, G/GB, T/TB, P/PB (base-1024). If invalid, returns 0.
func parseSize(raw interface{}) float64 {
	if raw == nil {
		return 0
	}
	// numeric bytes (from JSON decoding)
	if f, ok := raw.(float64); ok {
		return f
	}
	// string with optional unit
	if s, ok := raw.(string); ok {
		return proxyutils.ParseSizeString(s)
	}
	return 0
}
