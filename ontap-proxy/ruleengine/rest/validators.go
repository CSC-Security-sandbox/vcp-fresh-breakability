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
	submitExpertModeFlexCloneSplit       = core.SubmitExpertModeFlexCloneSplit
	validateVolumeCreation               = _validateVolumeCreation
	validateVolumeModification           = _validateVolumeModification
	validateVolumeDeletion               = _validateVolumeDeletion
	validateFlexCacheCreation            = _validateFlexCacheCreation
	validateFlexCacheDeletion            = _validateFlexCacheDeletion
	validatePrivateCLIVolumeCreation     = _validatePrivateCLIVolumeCreation
	validatePrivateCLIVolumeCloneCreate  = _validatePrivateCLIVolumeCloneCreate
	validatePrivateCLIVolumeModification = _validatePrivateCLIVolumeModification
	validatePrivateCLIVolumeCloneSplit   = _validatePrivateCLIVolumeCloneSplit
	validatePrivateCLIVolumeDeletion     = _validatePrivateCLIVolumeDeletion
	validatePrivateCLIVolumeRename       = _validatePrivateCLIVolumeRename
)

type VolumeRequestFields struct {
	VolumeName   string
	SizeInBytes  float64
	SizeProvided bool // true when "size" or "space.size" was present in the request (so 0 can mean "invalid" not "absent")
	SvmUuid      coreapi.OptString
	SvmName      coreapi.OptString
	Clone        coreapi.OptExpertModeVolumeV1Clone
}

func parseVolumeRequestFields(requestBody map[string]interface{}) VolumeRequestFields {
	fields := VolumeRequestFields{}
	if requestBody != nil {
		fields.VolumeName, _ = requestBody["name"].(string)
		fields.SizeInBytes, fields.SizeProvided = parseSizeFromVolumeBody(requestBody)
		if svm, ok := requestBody["svm"].(map[string]interface{}); ok {
			if uuid, ok := svm["uuid"].(string); ok && uuid != "" {
				fields.SvmUuid = coreapi.NewOptString(uuid)
			}
			if name, ok := svm["name"].(string); ok && name != "" {
				fields.SvmName = coreapi.NewOptString(name)
			}
		}
		if isCloneCreateRequest(requestBody) {
			cloneObj, _ := requestBody["clone"].(map[string]interface{})
			clone := coreapi.ExpertModeVolumeV1Clone{}
			clone.IsFlexclone = coreapi.NewOptBool(true)
			if parentVolume, ok := cloneObj["parent_volume"].(map[string]interface{}); ok && parentVolume != nil {
				parentVolumeObj := coreapi.ExpertModeVolumeV1CloneParentVolume{}
				if name, ok := parentVolume["name"].(string); ok && name != "" {
					parentVolumeObj.Name = coreapi.NewOptString(name)
				}
				if uuid, ok := parentVolume["uuid"].(string); ok && uuid != "" {
					parentVolumeObj.UUID = coreapi.NewOptString(uuid)
				}
				clone.ParentVolume = coreapi.NewOptExpertModeVolumeV1CloneParentVolume(parentVolumeObj)
			}
			if parentSnapshot, ok := cloneObj["parent_snapshot"].(map[string]interface{}); ok && parentSnapshot != nil {
				parentSnapshotObj := coreapi.ExpertModeVolumeV1CloneParentSnapshot{}
				if name, ok := parentSnapshot["name"].(string); ok && name != "" {
					parentSnapshotObj.Name = coreapi.NewOptString(name)
				}
				if uuid, ok := parentSnapshot["uuid"].(string); ok && uuid != "" {
					parentSnapshotObj.UUID = coreapi.NewOptString(uuid)
				}
				clone.ParentSnapshot = coreapi.NewOptExpertModeVolumeV1CloneParentSnapshot(parentSnapshotObj)
			}
			fields.Clone = coreapi.NewOptExpertModeVolumeV1Clone(clone)
		}
	}
	return fields
}

// getSizeRawFromVolumeBody returns the raw size value and the JSON path it came from
// ("size", "space.size", or "" if neither is present). When both top-level "size" and
// "space.size" are set, space.size is used so the proxy forwards the same value to core.
func getSizeRawFromVolumeBody(requestBody map[string]interface{}) (raw interface{}, fieldPath string) {
	if requestBody == nil {
		return nil, ""
	}
	if space, ok := requestBody["space"].(map[string]interface{}); ok && space != nil {
		if raw, ok := space["size"]; ok {
			return raw, "space.size"
		}
	}
	if raw, ok := requestBody["size"]; ok {
		return raw, "size"
	}
	return nil, ""
}

// parseSizeFromVolumeBody returns size in bytes from "size" or "space.size", and whether
// either field was present. When found is false, size is 0 and callers must not treat that
// as "invalid size" (e.g. on PATCH, neither size nor space.size may be present).
// When found is true, size may still be 0 meaning the provided value was invalid.
func parseSizeFromVolumeBody(requestBody map[string]interface{}) (size float64, found bool) {
	raw, path := getSizeRawFromVolumeBody(requestBody)
	if path == "" {
		return 0, false
	}
	if raw == nil {
		return 0, true
	}
	return parseSize(raw), true
}

// getSizeValueFromVolumeBody returns the raw size value (for error messages) from "size" or "space.size".
func getSizeValueFromVolumeBody(requestBody map[string]interface{}) interface{} {
	raw, path := getSizeRawFromVolumeBody(requestBody)
	if path == "" {
		return nil
	}
	return raw
}

// hasSpaceSize returns true if request body contains space.size.
func hasSpaceSize(requestBody map[string]interface{}) bool {
	_, path := getSizeRawFromVolumeBody(requestBody)
	return path == "space.size"
}

// isCloneCreateRequest returns true only when ONTAP clone flag is explicitly set.
// We intentionally key off clone.is_flexclone for validator behavior.
func isCloneCreateRequest(requestBody map[string]interface{}) bool {
	if requestBody == nil {
		return false
	}

	cloneObj, ok := requestBody["clone"].(map[string]interface{})
	if !ok || cloneObj == nil {
		return false
	}
	isFlexclone, _ := cloneObj["is_flexclone"].(bool)
	return isFlexclone
}

// isFlexCloneSplitInitiatedRequest is true when ONTAP-style PATCH sets clone.split_initiated to true.
func isFlexCloneSplitInitiatedRequest(requestBody map[string]interface{}) bool {
	if requestBody == nil {
		return false
	}
	cloneObj, ok := requestBody["clone"].(map[string]interface{})
	if !ok || cloneObj == nil {
		return false
	}
	v, ok := cloneObj["split_initiated"].(bool)
	return ok && v
}

// volumePostCreateSizeFieldsCondition validates size-related fields on POST /api/storage/volumes create:
// flexclone create must not send size or space.size; non-clone create must supply at least one of them
// (if both are set, validateVolumeCreation uses space.size for core).
// Logically this is:
//
//	Or(
//	  And(isCloneCreateRequest, no top-level size or space.size),
//	  And(not clone, HasAtLeastOneOf("size", "space.size", ...)),
//	)
//
// Implemented as one function (not nested dsl.Or/And) because dsl.Or keeps only the last failure
// reason, which would surface the wrong error for some combinations.
func volumePostCreateSizeFieldsCondition(r *http.Request) (bool, string) {
	requestBody, parseErr := dsl.GetParsedBody(r)
	if parseErr != "" {
		return false, parseErr
	}
	if isCloneCreateRequest(requestBody) {
		_, path := getSizeRawFromVolumeBody(requestBody)
		if path != "" {
			return false, "\"size\" and \"space.size\" must not be provided for clone volume create"
		}
		return true, ""
	}
	return dsl.HasAtLeastOneOf(
		"size", "space.size",
		"missing required field(s): size or space.size",
	)(r)
}

// getSizeFieldPath returns the JSON path that supplied the size value ("size" or "space.size")
// so error messages match the client's input.
func getSizeFieldPath(requestBody map[string]interface{}) string {
	_, path := getSizeRawFromVolumeBody(requestBody)
	if path != "" {
		return path
	}
	return "size"
}

func _validateVolumeCreation(r *http.Request) (bool, string) {
	return validateVolumeCreationByStyle(r, coreapi.ExpertModeVolumeV1StyleFlexvol)
}

func validateVolumeCreationByStyle(r *http.Request, style coreapi.ExpertModeVolumeV1Style) (bool, string) {
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
	isCloneRequest := isCloneCreateRequest(requestBody)

	// For non-clone volume create, size is mandatory.
	if !isCloneRequest && !fields.SizeProvided {
		return false, "\"size\" is a required field for non-clone volume create"
	}

	if isCloneRequest && fields.SizeProvided {
		return false, "\"size\" and \"space.size\" must not be provided for clone volume create"
	}

	// Reject invalid size only when a size field was provided and parsed to 0
	if fields.SizeProvided && fields.SizeInBytes == 0 {
		orig := getSizeValueFromVolumeBody(requestBody)
		fieldPath := getSizeFieldPath(requestBody)
		return false, fmt.Sprintf("\"%v\" is an invalid value for field \"%s\"", orig, fieldPath)
	}

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionCreate,
		VolumeName:    fields.VolumeName,
		Style:         style,
		SvmUuid:       fields.SvmUuid,
		SvmName:       fields.SvmName,
		Clone:         fields.Clone,
	}
	if fields.SizeProvided {
		expertVolumeRequest.SizeInBytes = coreapi.NewOptFloat64(fields.SizeInBytes)
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

	// FlexClone split: ONTAP PATCH with clone.split_initiated -> dedicated Core API (not volume Update).
	if isFlexCloneSplitInitiatedRequest(requestBody) {
		_, nameExists := requestBody["name"]
		_, topLevelSizeExists := requestBody["size"]
		spaceSizeExists := hasSpaceSize(requestBody)
		if nameExists || topLevelSizeExists || spaceSizeExists {
			return false, "flexclone split cannot be combined with name or size changes in the same request"
		}
		if !volumeUUID.IsSet() || volumeUUID.Value == "" {
			return false, "volume UUID is required in the request path to start flexclone split"
		}
		if err := submitExpertModeFlexCloneSplit(r.Context(), volumeUUID.Value, "", authData.AccountName, authData.PoolID, "", logger); err != nil {
			return false, err.Error()
		}
		return true, ""
	}

	// Trigger reconcile only if name or size is being modified (size may be top-level or space.size)
	_, nameExists := requestBody["name"]
	_, topLevelSizeExists := requestBody["size"]
	spaceSizeExists := hasSpaceSize(requestBody)
	sizeExists := topLevelSizeExists || spaceSizeExists

	if fields.SizeProvided && fields.SizeInBytes == 0 {
		orig := getSizeValueFromVolumeBody(requestBody)
		fieldPath := getSizeFieldPath(requestBody)
		return false, fmt.Sprintf("\"%v\" is an invalid value for field \"%s\"", orig, fieldPath)
	}

	if !nameExists && !sizeExists {
		return true, ""
	}

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionUpdate,
		VolumeName:    fields.VolumeName,
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		SvmUuid:       fields.SvmUuid,
		SvmName:       fields.SvmName,
		VolumeUUID:    volumeUUID,
	}
	if fields.SizeProvided {
		expertVolumeRequest.SizeInBytes = coreapi.NewOptFloat64(fields.SizeInBytes)
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func _validateVolumeDeletion(r *http.Request) (bool, string) {
	return validateVolumeDeletionByStyle(r, coreapi.ExpertModeVolumeV1StyleFlexvol)
}

func validateVolumeDeletionByStyle(r *http.Request, style coreapi.ExpertModeVolumeV1Style) (bool, string) {
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
		Style:         style,
		VolumeUUID:    volumeUUID,
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func _validateFlexCacheCreation(r *http.Request) (bool, string) {
	return validateVolumeCreationByStyle(r, coreapi.ExpertModeVolumeV1StyleFlexcache)
}

func _validateFlexCacheDeletion(r *http.Request) (bool, string) {
	return validateVolumeDeletionByStyle(r, coreapi.ExpertModeVolumeV1StyleFlexcache)
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
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		SvmName:       fields.SvmName,
	}
	expertVolumeRequest.SizeInBytes = coreapi.NewOptFloat64(fields.SizeInBytes)

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
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		SvmName:       coreapi.NewOptString(svmName),
	}
	expertVolumeRequest.SizeInBytes = coreapi.NewOptFloat64(fields.SizeInBytes)

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

// _validatePrivateCLIVolumeCloneCreate validates private CLI derived route:
// POST /api/private/cli/volume/clone
// Body fields are expected in private-CLI style (underscores).
func _validatePrivateCLIVolumeCloneCreate(r *http.Request) (bool, string) {
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

	cloneName, _ := requestBody["flexclone"].(string)
	vserverName, _ := requestBody["vserver"].(string)

	parentVolume := ""
	if pv, ok := requestBody["parent_volume"].(string); ok {
		parentVolume = pv
	}
	if parentVolume == "" {
		if b, ok := requestBody["b"].(string); ok {
			parentVolume = b
		}
	}
	if parentVolume == "" {
		return false, "missing required field: parent_volume or b"
	}

	cloneReq := coreapi.ExpertModeVolumeV1Clone{
		IsFlexclone: coreapi.NewOptBool(true),
		ParentVolume: coreapi.NewOptExpertModeVolumeV1CloneParentVolume(
			coreapi.ExpertModeVolumeV1CloneParentVolume{
				Name: coreapi.NewOptString(parentVolume),
			},
		),
	}
	if parentSnapshot, ok := requestBody["parent_snapshot"].(string); ok && parentSnapshot != "" {
		cloneReq.ParentSnapshot = coreapi.NewOptExpertModeVolumeV1CloneParentSnapshot(
			coreapi.ExpertModeVolumeV1CloneParentSnapshot{
				Name: coreapi.NewOptString(parentSnapshot),
			},
		)
	}

	expertVolumeRequest := &coreapi.ExpertModeVolumeV1{
		ProjectNumber: authData.AccountName,
		PoolUUID:      authData.PoolID,
		Action:        coreapi.ExpertModeVolumeV1ActionCreate,
		VolumeName:    cloneName,
		Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
		SvmName:       coreapi.NewOptString(vserverName),
		Clone:         coreapi.NewOptExpertModeVolumeV1Clone(cloneReq),
	}

	if sizeRaw, exists := requestBody["size"]; exists {
		sizeInBytes := parseSize(sizeRaw)
		if sizeInBytes <= 0 {
			return false, fmt.Sprintf("\"%v\" is an invalid value for field \"size\"", sizeRaw)
		}
		expertVolumeRequest.SizeInBytes = coreapi.NewOptFloat64(sizeInBytes)
	}

	if err := submitExpertModeVolumeOperation(r.Context(), expertVolumeRequest, "", logger); err != nil {
		return false, err.Error()
	}
	return true, ""
}

// _validatePrivateCLIVolumeCloneSplit validates private CLI derived route:
// POST /api/private/cli/volume/clone/split/start
// Body fields: vserver, flexclone
func _validatePrivateCLIVolumeCloneSplit(r *http.Request) (bool, string) {
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

	cloneName, _ := requestBody["flexclone"].(string)
	if err := submitExpertModeFlexCloneSplit(r.Context(), "", cloneName, authData.AccountName, authData.PoolID, "", logger); err != nil {
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
