package rules_v2

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	core "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/coreapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	submitExpertModeVolumeOperation = core.SubmitExpertModeVolumeOperation
	validateVolumeCreation          = _validateVolumeCreation
	validateVolumeModification      = _validateVolumeModification
	validateVolumeDeletion          = _validateVolumeDeletion
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

	// Size is optional for PATCH; only trigger reconcile if size is being modified
	if sizeValue, sizeExists := requestBody["size"]; sizeExists {
		if fields.SizeInBytes == 0 {
			return false, fmt.Sprintf("\"%v\" is an invalid value for field \"size\"", sizeValue)
		}
	} else {
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

// parseSize parses a size that may be a float64 (bytes) or a string like "10GB" into bytes.
// Supports units: KB, MB, GB, TB, PB (base-1024). If invalid, returns 0.
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
		s = strings.TrimSpace(s)
		if s == "" {
			return 0
		}
		// split number and unit
		var numPart string
		var unitPart string
		for i, r := range s {
			if r < '0' || r > '9' {
				numPart = s[:i]
				unitPart = strings.TrimSpace(strings.ToUpper(s[i:]))
				break
			}
		}
		if numPart == "" { // all digits or string starts with unit
			numPart = s
		}
		val, err := strconv.ParseFloat(numPart, 64)
		if err != nil {
			return 0
		}
		var mult float64
		switch unitPart {
		case "":
			mult = 1
		case "KB":
			mult = 1024
		case "MB":
			mult = 1024 * 1024
		case "GB":
			mult = 1024 * 1024 * 1024
		case "TB":
			mult = 1024 * 1024 * 1024 * 1024
		case "PB":
			mult = 1024 * 1024 * 1024 * 1024 * 1024
		default:
			return 0
		}
		return val * mult
	}
	return 0
}
