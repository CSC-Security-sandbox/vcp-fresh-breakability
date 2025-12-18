package vsa

import (
	"context"
	"net/http"
	"time"

	ontaprestcluster "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	ontapreststorage "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/storage"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// Quota system constants (from spec)
const (
	// Quota states
	QuotaStateOff          = "off"
	QuotaStateOn           = "on"
	QuotaStateInitializing = "initializing"
	QuotaStateResizing     = "resizing"
	QuotaStateCorrupt      = "corrupt"

	// Job response states
	JobRespSuccess = "success"
	JobRespFailure = "failure"

	// Error message patterns for quota operations
	ResizeOperationFailed     = "Quota policy rule create operation succeeded, however quota resize failed"
	ActivationOperationFailed = "Quota policy rule create operation succeeded, but the rule is not active"
	QuotaStatusFailed         = "Another quota operation is currently in progress for volume"
	DeletedRuleEnforced       = "Quota policy rule delete operation succeeded, however the rule is still being enforced"
	DeleteRuleResizeFailed    = "Quota policy rule delete operation succeeded, however quota resize failed"

	// ONTAP quota types
	QuotaRuleTypeUser  = "user"
	QuotaRuleTypeGroup = "group"

	// Conversion multipliers (from spec)
	kibsToBytesMultiplier = 1024
)

// Function variable for VolumeGetWithContext to enable mocking in tests
var volumeGetWithContextFunc = ontaprest.VolumeGetWithContext

// Internal quota type constants (from orchestrator)
const (
	IndividualUserQuota  = "INDIVIDUAL_USER_QUOTA"
	IndividualGroupQuota = "INDIVIDUAL_GROUP_QUOTA"
	DefaultUserQuota     = "DEFAULT_USER_QUOTA"
	DefaultGroupQuota    = "DEFAULT_GROUP_QUOTA"
)

// GetQuotaTypeForOntap converts internal quota type to ONTAP quota type format.
// According to the spec (create-quota-cvs-job-function.md, Step 2 of getOntapQuotaUUIDAndType):
// Internal types (INDIVIDUAL_USER_QUOTA, DEFAULT_USER_QUOTA) -> "user"
// Internal types (INDIVIDUAL_GROUP_QUOTA, DEFAULT_GROUP_QUOTA) -> "group"
func GetQuotaTypeForOntap(quotaType string) string {
	switch quotaType {
	case IndividualUserQuota, DefaultUserQuota:
		return QuotaRuleTypeUser
	case IndividualGroupQuota, DefaultGroupQuota:
		return QuotaRuleTypeGroup
	default:
		return "" // Return empty string for unknown types
	}
}

// GetDefaultQuotaRule retrieves an existing default quota rule from ONTAP for a given volume and quota type.
// A default quota rule has no target (empty string), so we search for quota rules matching:
//   - volume UUID
//   - quota type (user/group)
//   - empty target
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - volumeUUID: The ONTAP external UUID of the volume
//   - svmName: The name of the SVM containing the volume
//   - quotaType: The quota type ("user" or "group" in ONTAP format)
//
// Returns:
//   - QuotaRuleInfo: Information about the found quota rule (UUID, Type, Target, DiskLimit)
//   - NotFoundErr: If no default quota rule exists (this is acceptable - workflow will create one)
//   - Error: For any other failure (API errors, parsing errors, etc.)
//
// TODO: Implement actual ONTAP REST API call to query quota rules
// Current implementation: Returns NotFoundErr to allow workflow to proceed with creation
func (rc *OntapRestProvider) GetDefaultQuotaRule(ctx context.Context, volumeUUID, svmName, quotaType string) (*QuotaRuleInfo, error) {
	// TODO: Implement ONTAP REST API call:
	// 1. GET /api/storage/quota/rules?volume.uuid={uuid}&type={type}
	// 2. Filter for rules with empty target (default quota)
	// 3. Return the matching rule or NotFoundErr

	// For now, always return NotFoundErr to allow workflow to proceed with creation
	return nil, customerrors.NewNotFoundErr("Default quota rule not found", nil)
}

// GetQuotaRuleCollection retrieves all quota rules configured on a specific volume.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - volumeUUID: The ONTAP external UUID of the volume
//   - svmName: The name of the SVM containing the volume
//
// Returns:
//   - []*QuotaRuleCollectionItem: Array of quota rules configured on the volume
//   - error: Error from API call (nil if successful)
func (rc *OntapRestProvider) GetQuotaRuleCollection(ctx context.Context, volumeUUID, svmName string) ([]*QuotaRuleCollectionItem, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	getQuotaRuleCollectionParams := ontapreststorage.NewQuotaRuleCollectionGetParams().
		WithVolumeUUID(&volumeUUID).
		WithSvmName(&svmName).
		WithFields([]string{"users", "group", "type"})

	result, err := client.Storage().QuotaRuleCollectionGet(ctx, getQuotaRuleCollectionParams)
	if err != nil {
		return nil, err
	}

	var quotaRulesList []*QuotaRuleCollectionItem

	for _, quotaRule := range result.Payload.QuotaRuleResponseInlineRecords {
		var quotaRuleInlineUsers []*QuotaRuleInlineUser
		for _, quotaRuleInlineUser := range quotaRule.QuotaRuleInlineUsers {
			quotaRuleInlineUsers = append(quotaRuleInlineUsers, &QuotaRuleInlineUser{
				Name: quotaRuleInlineUser.Name,
				ID:   quotaRuleInlineUser.ID,
			})
		}

		var quotaRuleInlineGroup *QuotaRuleInlineGroup
		if quotaRule.Group != nil {
			quotaRuleInlineGroup = &QuotaRuleInlineGroup{
				ID:   quotaRule.Group.ID,
				Name: quotaRule.Group.Name,
			}
		}

		quotaRulesList = append(quotaRulesList, &QuotaRuleCollectionItem{
			QuotaType:            nillable.FromPointer(quotaRule.Type),
			UUID:                 nillable.FromPointer(quotaRule.UUID),
			QuotaRuleInlineGroup: quotaRuleInlineGroup,
			QuotaRuleInlineUsers: quotaRuleInlineUsers,
		})
	}

	return quotaRulesList, nil
}

// GetOntapQuotaUUIDAndType retrieves the UUID and type of an existing quota rule from ONTAP.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - volumeUUID: The ONTAP external UUID of the volume
//   - svmName: The name of the SVM containing the volume
//   - quotaType: Internal quota type (e.g., "INDIVIDUAL_USER_QUOTA", "DEFAULT_USER_QUOTA")
//   - target: Quota target (user/group ID or name, empty for default quotas)
//
// Returns:
//   - quotaUUID: UUID of matching quota rule (empty string if not found)
//   - ontapQuotaType: ONTAP quota type from the found rule (empty if not found)
//   - error: Error from API call (nil if successful, even if no match found)
func (rc *OntapRestProvider) GetOntapQuotaUUIDAndType(ctx context.Context, volumeUUID, svmName, quotaType, target string) (string, string, error) {
	quotaUUID := ""
	quotaCollectionRespQuotaType := ""
	quotaOntapType := GetQuotaTypeForOntap(quotaType)

	quotaRulesCollectionResp, err := rc.GetQuotaRuleCollection(ctx, volumeUUID, svmName)
	if err != nil {
		return "", "", err
	}

	for _, rule := range quotaRulesCollectionResp {
		// For Group Quotas (spec lines 1174-1190)
		if quotaOntapType == QuotaRuleTypeGroup && quotaOntapType == rule.QuotaType {
			var groupID string
			if rule.QuotaRuleInlineGroup != nil && rule.QuotaRuleInlineGroup.ID != nil {
				groupID = *rule.QuotaRuleInlineGroup.ID
			} else {
				groupID = nillable.FromPointer(rule.QuotaRuleInlineGroup.Name)
			}
			if groupID == target {
				quotaUUID = rule.UUID
				quotaCollectionRespQuotaType = rule.QuotaType
				break
			}
		} else if quotaOntapType == QuotaRuleTypeUser && quotaOntapType == rule.QuotaType {
			for _, userInfoRule := range rule.QuotaRuleInlineUsers {
				var userID string
				if userInfoRule.ID != nil {
					userID = *userInfoRule.ID
				} else {
					userID = nillable.FromPointer(userInfoRule.Name)
				}
				if userID == target {
					quotaUUID = rule.UUID
					quotaCollectionRespQuotaType = rule.QuotaType
					break
				}
			}
		}
	}

	return quotaUUID, quotaCollectionRespQuotaType, nil
}

// UpdateQuotaRule updates an existing quota rule's disk limit on ONTAP.
//
// The ONTAP REST API: PATCH /api/storage/quota/rules/{uuid}
// Updates only the disk limit (space.hard_limit) while preserving other settings.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - params: UpdateQuotaRuleParams containing:
//   - ExternalQuotaRuleUUID: The ONTAP UUID of the quota rule to update
//   - DiskLimitInKibs: The new disk limit in kibibytes (KiB)
//
// Returns:
//   - JobStatus: Job status with Code, State ("success"/"failure"), and Message
//   - Error: For any failure during the API call
func (rc *OntapRestProvider) UpdateQuotaRule(ctx context.Context, params *UpdateQuotaRuleParams) (*JobStatus, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	inputParams := &ontaprestmodels.QuotaRule{
		Space: &ontaprestmodels.QuotaRuleInlineSpace{
			HardLimit: nillable.ToPointer(params.DiskLimitInKibs * kibsToBytesMultiplier),
		},
	}

	quotaRuleModifyParams := ontapreststorage.NewQuotaRuleModifyParams().
		WithInfo(inputParams).
		WithUUID(params.ExternalQuotaRuleUUID)

	resp, err := client.Storage().QuotaRuleModify(ctx, quotaRuleModifyParams)
	if err != nil {
		return nil, err
	}

	// Poll the job until it completes
	jobUUID := resp.Payload.Job.UUID.String()
	if err = client.Poll(jobUUID); err != nil {
		return nil, err
	}

	// Fetch job details to get final status
	jobParams := &ontaprest.JobGetParams{
		BaseParams: ontaprest.BaseParams{
			Fields: []string{"message", "state", "code"},
		},
		UUID: jobUUID,
	}
	jobResp, err := client.Cluster().JobGet(ctx, jobParams)
	if err != nil {
		return nil, err
	}

	jobStatus := &JobStatus{
		Code:    nillable.FromPointer(jobResp.Payload.Code),
		State:   nillable.FromPointer(jobResp.Payload.State),
		Message: nillable.FromPointer(jobResp.Payload.Message),
	}
	return jobStatus, nil
}

func fetchDetailsFromJob(client ontaprest.RESTClient, jobUUID string) (*ontaprestcluster.JobGetOK, error) {
	jobParams := &ontaprest.JobGetParams{
		BaseParams: ontaprest.BaseParams{
			Fields: []string{"message", "state"},
		},
		UUID: jobUUID,
	}

	jobResponse, err := client.Cluster().JobGet(context.TODO(), jobParams)
	if err != nil {
		return nil, err
	}

	return &ontaprestcluster.JobGetOK{
		Payload: &ontaprestmodels.Job{
			Links:       jobResponse.Payload.Links,
			Code:        jobResponse.Payload.Code,
			Description: jobResponse.Payload.Description,
			EndTime:     jobResponse.Payload.EndTime,
			Error:       jobResponse.Payload.Error,
			Message:     jobResponse.Payload.Message,
			Node:        jobResponse.Payload.Node,
			StartTime:   jobResponse.Payload.StartTime,
			State:       jobResponse.Payload.State,
			Svm:         jobResponse.Payload.Svm,
			UUID:        jobResponse.Payload.UUID,
		},
	}, nil
}

// retryJob polls a job until it reaches a terminal state (success or failure).
// Polls at specified interval until job state is "success" or "failure".
func retryJob(client ontaprest.RESTClient, sleep time.Duration, uuid string,
	f func(ontaprest.RESTClient, string) (*ontaprestcluster.JobGetOK, error)) error {
	for {
		resp, err := f(client, uuid)
		if err != nil {
			return err
		}

		// Check if job is complete (success or failure)
		if resp.Payload.State != nil &&
			(*resp.Payload.State == ontaprestmodels.JobStateSuccess ||
				*resp.Payload.State == ontaprestmodels.JobStateFailure) {
			return nil
		}

		time.Sleep(sleep)
	}
}

// GetQuotaStatus retrieves the current quota status for a volume from ONTAP.
func (rc *OntapRestProvider) GetQuotaStatus(ctx context.Context, volumeUUID string) (*QuotaStatus, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	quotaStatusGetParams := &ontaprest.VolumeGetParams{
		BaseParams: ontaprest.BaseParams{Fields: []string{"quota.state"}},
		UUID:       volumeUUID,
	}

	// Call the underlying storage API directly with context
	response, err := volumeGetWithContextFunc(ctx, client, quotaStatusGetParams)
	if err != nil {
		return nil, err
	}

	if response.Payload.Quota == nil {
		return nil, customerrors.New("Unable to complete the operation. Please contact support if the problem persists")
	}

	quotaStatus := &QuotaStatus{
		Enabled: nillable.FromPointer(response.Payload.Quota.Enabled),
		State:   nillable.FromPointer(response.Payload.Quota.State),
	}

	return quotaStatus, nil
}

// CreateQuotaRule creates a new quota rule on ONTAP.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - params: CreateQuotaRuleParams containing:
//   - VolumeUUID: The ONTAP volume UUID
//   - QuotaTarget: User/group identifier (empty for default quotas)
//   - QuotaType: Type of quota ("individual_user", "default_user", etc.)
//   - DiskLimitInKib: Disk limit in kibibytes
//   - RQuota: Recursive quota flag
//
// Returns:
//   - JobStatus: Job status with Code, State ("success"/"failure"), and Message
//   - Error: For any failure during the API call
func (rc *OntapRestProvider) CreateQuotaRule(ctx context.Context, params CreateQuotaRuleParams) (*JobStatus, error) {
	qtreeName := ""
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	// Convert internal quota type to ONTAP format
	ontapQuotaType := GetQuotaTypeForOntap(params.QuotaType)

	// Prepare quota rule for ONTAP API
	inputParams := &ontaprestmodels.QuotaRule{
		Type:   &ontapQuotaType,
		Volume: &ontaprestmodels.QuotaRuleInlineVolume{UUID: &params.VolumeUUID},
		Svm:    &ontaprestmodels.QuotaRuleInlineSvm{Name: &params.SVMName},
		Space: &ontaprestmodels.QuotaRuleInlineSpace{
			HardLimit: nillable.ToPointer(params.DiskLimitInKib * 1024), // Convert KiB to bytes
		},
		Qtree: &ontaprestmodels.QuotaRuleInlineQtree{Name: nillable.ToPointer(qtreeName)}, // Empty string for volume-level quotas
	}

	// Add user/group specific fields based on quota type
	if ontapQuotaType == QuotaRuleTypeUser {
		// Individual user quota - add user ID
		var usersQuotaInfo []*ontaprestmodels.QuotaRuleInlineUsersInlineArrayItem
		userItems := &ontaprestmodels.QuotaRuleInlineUsersInlineArrayItem{ID: &params.QuotaTarget}
		usersQuotaInfo = append(usersQuotaInfo, userItems)
		inputParams.QuotaRuleInlineUsers = usersQuotaInfo
		// Default user quotas don't need users array
	}
	if ontapQuotaType == QuotaRuleTypeGroup {
		// Individual group quota - add group ID
		inputParams.Group = &ontaprestmodels.QuotaRuleInlineGroup{ID: &params.QuotaTarget}
		// Default group quotas don't need group field
	}

	// Execute ONTAP REST API call per spec (lines 110-112)
	quotaRuleCreateParams := ontapreststorage.NewQuotaRuleCreateParams().WithInfo(inputParams)
	resp, err := client.Storage().QuotaRuleCreate(ctx, quotaRuleCreateParams)
	if err != nil {
		return nil, err
	}

	var jobResp *ontaprestcluster.JobGetOK
	jobResp, err = fetchDetailsFromJob(client, resp.Payload.Job.UUID.String())
	if err != nil {
		return nil, err
	}

	retryErr := retryJob(client, 1*(time.Second), resp.Payload.Job.UUID.String(), func(c ontaprest.RESTClient, s string) (res *ontaprestcluster.JobGetOK, err error) {
		jobResp, err = fetchDetailsFromJob(c, s)
		return jobResp, err
	})
	if retryErr != nil {
		return nil, retryErr
	}

	jobStatus := &JobStatus{
		Code:    nillable.FromPointer(jobResp.Payload.Code),
		State:   nillable.FromPointer(jobResp.Payload.State),
		Message: nillable.FromPointer(jobResp.Payload.Message),
	}
	return jobStatus, nil
}

// QuotaEnableDisable enables or disables the quota system on a volume.
// This is a thin wrapper around the internal _quotaReinitialization function that provides
func (rc *OntapRestProvider) QuotaEnableDisable(ctx context.Context, volumeUUID, svmName string, enable bool) (*JobStatus, error) {
	return rc.quotaReinitialization(ctx, volumeUUID, svmName, enable)
}

func (rc *OntapRestProvider) quotaReinitialization(ctx context.Context, volumeUUID, svmName string, quotaState bool) (*JobStatus, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	volumeModifyParams := &ontaprest.VolumeModifyParams{
		UUID:         volumeUUID,
		QuotaEnabled: &quotaState,
	}

	// Volume UUID should be sufficient to identify the volume uniquely.
	isSuccess, ontapJob, err := client.Storage().VolumeModify(volumeModifyParams)
	if err != nil {
		return nil, err
	}

	if isSuccess {
		jobStatus := &JobStatus{
			Code:  http.StatusOK,
			State: JobRespSuccess,
		}
		return jobStatus, nil
	}

	// Poll the job until it completes
	if err = client.Poll(ontapJob.JobUUID); err != nil {
		return nil, err
	}

	// Fetch job details to get final status
	jobParams := &ontaprest.JobGetParams{
		BaseParams: ontaprest.BaseParams{
			Fields: []string{"message", "state", "code"},
		},
		UUID: ontapJob.JobUUID,
	}
	jobResp, err := client.Cluster().JobGet(ctx, jobParams)
	if err != nil {
		return nil, err
	}

	jobStatus := &JobStatus{
		Code:    nillable.FromPointer(jobResp.Payload.Code),
		State:   nillable.FromPointer(jobResp.Payload.State),
		Message: nillable.FromPointer(jobResp.Payload.Message),
	}

	return jobStatus, nil
}

func (rc *OntapRestProvider) DeleteQuotaRule(ctx context.Context, quotaUUID string) (*JobStatus, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	quotaDeleteParams := ontapreststorage.NewQuotaRuleDeleteParams().
		WithUUID(quotaUUID)

	resp, err := client.Storage().QuotaRuleDelete(ctx, quotaDeleteParams)
	if err != nil {
		return nil, err
	}

	jobUUID := resp.Payload.Job.UUID.String()

	// Poll the job until it completes
	if err = client.Poll(jobUUID); err != nil {
		return nil, err
	}

	// Fetch job details to get final status
	jobParams := &ontaprest.JobGetParams{
		BaseParams: ontaprest.BaseParams{
			Fields: []string{"message", "state", "code"},
		},
		UUID: jobUUID,
	}
	jobResp, err := client.Cluster().JobGet(ctx, jobParams)
	if err != nil {
		return nil, err
	}

	jobStatus := &JobStatus{
		Code:    nillable.FromPointer(jobResp.Payload.Code),
		State:   nillable.FromPointer(jobResp.Payload.State),
		Message: nillable.FromPointer(jobResp.Payload.Message),
	}
	return jobStatus, nil
}
