package errors

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/multierr"
)

var (
	Combine = multierr.Combine
	New     = errors.New
	Newf    = fmt.Errorf
)

const (
	CustomErrorType     = "CustomError"
	DefaultErrorMessage = "An internal error occurred"

	ErrWorkflowConfigurationError = 1001
	ErrBadRequest                 = 1002
	ErrResourceNotFound           = 1003
	ErrFileReadError              = 1004
	ErrFileWriteError             = 1005
	ErrJSONParsingError           = 1006
	ErrMaxRetriesExceeded         = 1007
	ErrTimeLimitExceeded          = 1008
	ErrInputValidationError       = 1009
	ErrResourceStateConflictError = 1010
	ErrInternalServerError        = 1011
	ErrModelConversionError       = 1012
	ErrBase64EncodingError        = 1013
	ErrBase64DecodingError        = 1014
	ErrResourceEmptyError         = 1015
	ErrCSRGenerationError         = 1016
	ErrWorkflowNotLaunched        = 1017

	ErrDatabaseConnectionClosed  = 2001
	ErrDatabaseTransactionError  = 2002
	ErrDatabaseDataInsertError   = 2003
	ErrDatabaseDataReadError     = 2004
	ErrDatabaseDataUpdateError   = 2005
	ErrDatabaseDataDeleteError   = 2006
	ErrDatabaseDataNotFoundError = 2007
	ErrVolumeNotFound            = 2100
	ErrAccountNotFound           = 2101
	ErrPoolNotFound              = 2102

	ErrGCPClientInitializationError               = 3001
	ErrPSAPeeringNotFoundError                    = 3002
	ErrGCPResourceProvisionError                  = 3003
	ErrGCPResourceFetchError                      = 3004
	ErrGCPResourceDeprovisionError                = 3005
	ErrGCPResourceAlreadyExistsError              = 3006
	ErrGCPServiceAccountDeletionError             = 3007
	ErrGCPServiceAccountDeletionNonRetriableError = 3008
	ErrGCPCustomerIPExhaustion                    = 3009

	// VLM-specific GCP errors (9000-9999 range)
	ErrVLMQuotaExceededRegional                = 9001
	ErrVLMQuotaExceededZonal                   = 9002
	ErrVLMQuotaExceededGeneral                 = 9003
	ErrVLMResourceNotAvailableInZone           = 9004
	ErrVLMZoneResourcePoolExhausted            = 9005
	ErrVLMZoneResourcePoolExhaustedWithDetails = 9006
	ErrVLMInsufficientResourcesInZone          = 9007
	ErrVLMVMTypeUnavailableInZone              = 9008
	ErrVLMVMTypeUnavailableWithReason          = 9009
	ErrVLMRateLimitExceeded                    = 9010
	ErrVLMDiskRateLimited                      = 9011
	ErrVLMResourceNotReady                     = 9012
	ErrVLMInsufficientPermissions              = 9013
	ErrVLMProjectConstraintViolated            = 9014
	ErrVLMCPUPlatformMismatch                  = 9015
	ErrVLMServiceAccountAccessDenied           = 9016
	ErrVLMInvalidMachineImageUpdate            = 9017
	ErrVLMWorkflowError                        = 9018
	ErrVLMCloudVMOffline                       = 9019

	ErrVSAClusterCreateError           = 4001
	ErrCouldNotFetchVSAClusterDetails  = 4002
	ErrVSAClusterDeleteError           = 4003
	ErrIncorrectVSAClusterState        = 4004
	ErrVSAClusterNodeNotFound          = 4005
	ErrVSAClusterNodeIPAddressNotFound = 4006
	ErrVSAClusterUpdateError           = 4007
	ErrVLMClientInitializationError    = 4008
	ErrAllHostGroupsNotFoundError      = 4009
	ErrMissingRequiredInputError       = 4010
	ErrUnexpectedNodeCountForPool      = 4011

	ErrONTAPVersionFetchError          = 5001
	ErrCreatingSVM                     = 5002
	ErrDeletingSVM                     = 5003
	ErrSVMNotFound                     = 5004
	ErrOntapRestAPIError               = 5006
	ErrOntapInconsistentResourceError  = 5007
	ErrONTAPClientCreationError        = 5008
	ErrConstituentVolumesLimitExceeded = 5009
	ErrVolumeExceedsMaximumSize        = 5010

	ErrIamClientNotFoundError      = 6020
	ErrFailedToParseProjectNumber  = 6021
	ErrFailedToMarshalPayload      = 6022
	ErrFailedToMarshalJson         = 6023
	ErrFailedToCreateHTTP          = 6024
	ErrFailedToExecuteHTTP         = 6025
	ErrFailedToReadResponse        = 6026
	ErrFailedToUnmarshalCCFE       = 6027
	ErrFailedToReadQuota           = 6028
	ErrFailedToCreateNewIamCred    = 6029
	ErrFailedToGenerateAccessToken = 6030

	ErrDeleteSnapshot                     = 7001
	ErrVolumeNotOnlineForSnapshotDelete   = 7002
	ErrSnapshotPolicyScheduleRequired     = 7003
	ErrSnapshotPolicyScheduleTooMany      = 7004
	ErrDeleteVolumeWhenInSplitState       = 7005
	ErrRevertReplicationDestinationVolume = 7006
	ErrLunUpdate                          = 7007
	ErrRestoreVolumeValidation            = 7008
	ErrRevertVolumeWhenSnapshotInUse      = 7009

	// CMEK Error Codes
	ErrDescribingSDEJob                            = 6057
	ErrSDEJobNotFinished                           = 6058
	ErrSDEKmsDeleteJobFailed                       = 6059
	ErrCVPClientStartProjectEventError             = 6060
	ErrInvalidOperationName                        = 6061
	ErrKMSMigration                                = 6063
	ErrKMSDelete                                   = 6066
	ErrKMSUpdate                                   = 6067
	ErrKMSCreate                                   = 6068
	ErrGeneratingUniqueSerialNumber                = 6069
	ErrCVPClientHandleResourceEventError           = 6073
	ErrCVPClientFinishProjectEventError            = 6074
	ErrHREResourceIsTransitioning                  = 6075
	ErrHandleResourceEventErrorNotFound            = 6076
	ErrHandleResourceEventErrorBadRequest          = 6077
	ErrHandleResourceEventErrorInternalServerError = 6078
	ErrHandleResourceEventErrorUnauthorized        = 6079
	ErrHandleResourceEventErrorForbidden           = 6080
	ErrHandleResourceEventErrorConflict            = 6081
	ErrHandleResourceEventErrorNotImplemented      = 6082
	ErrHandleResourceEventErrorTooManyRequests     = 6083
	ErrFinishProjectEventErrorListingResources     = 6084
	ErrFinishProjectEventErrorDeletingResources    = 6085
	ErrFinishProjectEventHardDeleteResources       = 6086

	// Replication Error Codes 6100 - 6999
	ErrReplicationScheduleUnspecified                                                        = 6100
	ErrLabelsMarshalFailure                                                                  = 6101
	ErrLabelsCountExceeded                                                                   = 6102
	ErrLabelsKeyRequired                                                                     = 6103
	ErrLabelsKeyTooLongCharacters                                                            = 6104
	ErrLabelsKeyTooLongBytes                                                                 = 6105
	ErrLabelsValueTooLongCharacters                                                          = 6106
	ErrLabelsValueTooLongBytes                                                               = 6107
	ErrGetSignedToken                                                                        = 6108
	ErrParseSourceLocation                                                                   = 6109
	ErrParseDestinationLocation                                                              = 6110
	ErrGetSrcBasePath                                                                        = 6111
	ErrGetDstBasePath                                                                        = 6112
	ErrValidateCreateResourceIdInUse                                                         = 6113
	ErrListVolumes                                                                           = 6114
	ErrListPools                                                                             = 6115
	ErrGetPoolNotFound                                                                       = 6116
	ErrInternalDescribePoolAPI                                                               = 6117
	ErrInternalDescribePoolNotFound                                                          = 6118
	ErrInternalAcceptClusterPeerAPI                                                          = 6119
	ErrInternalAcceptClusterPeerNotFound                                                     = 6120
	ErrDescribingJobNotFound                                                                 = 6121
	ErrDescribingJobAPI                                                                      = 6122
	ErrJobNotFinished                                                                        = 6123
	ErrCreatingDestinationVolume                                                             = 6124
	ErrAcceptSvmPeer                                                                         = 6125
	ErrorFailedToMarshalModel                                                                = 6126
	ErrorFailedToUnmarshal                                                                   = 6127
	ErrValidateCreateSourceVolumeIsFlexCacheVolume                                           = 6128
	ErrValidateCreateSourceVolumeInReplicationGroup                                          = 6129
	ErrValidateCreateSourceVolumeNotReady                                                    = 6130
	ErrValidateStoragePoolUri                                                                = 6131
	ErrValidateDestinationPoolTransitioning                                                  = 6132
	ErrValidateDestinationStoragePoolState                                                   = 6133
	ErrDestPoolSize                                                                          = 6134
	ErrGetSignedCallbackToken                                                                = 6135
	ErrGetReplicationQuotaLimitInternal                                                      = 6136
	ErrDescribeSourcePool                                                                    = 6137
	ErrHydrateVolumeCreate                                                                   = 6138
	ErrGettingSvmPeer                                                                        = 6139
	ErrFailedToGetSnapmirrorDetailsFromOntapGetMultiple                                      = 6140
	ErrFailedToGetSnapmirrorDetailsFromOntapMountJob                                         = 6141
	ErrRegionZoneParsingErrorCurrentRegion                                                   = 6142
	ErrRegionZoneParsingErrorDestinationRegion                                               = 6143
	ErrRegionZoneParsingErrorSourceRegion                                                    = 6144
	ErrRegionZoneParsingErrorPairedRegionURI                                                 = 6145
	ErrProjectParsingError                                                                   = 6146
	ErrGoogleProxyInternalGetMultipleReplications                                            = 6147
	ErrGoogleProxyInternalGetMultipleReplicationsBadRequest                                  = 6148
	ErrGoogleProxyInternalGetMultipleReplicationsNotFound                                    = 6149
	ErrGoogleProxyInternalGetMultipleReplicationsInternalServerError                         = 6150
	ErrGoogleProxyInternalGetMultipleReplicationsUnauthorized                                = 6151
	ErrGoogleProxyInternalGetMultipleReplicationsForbidden                                   = 6152
	ErrGoogleProxyInternalGetMultipleReplicationsUnknown                                     = 6153
	ErrGetRemoteReplicationJobs                                                              = 6154
	ErrGetLocalReplicationJobs                                                               = 6155
	ErrorCvpReplicationJobAlreadyInProcess                                                   = 6156
	ErrMountingVolumeReplication                                                             = 6157
	ErrDeHydrateSnapshots                                                                    = 6158
	ErrDeHydrateVolume                                                                       = 6160
	ErrDeHydrateVolumeReplication                                                            = 6161
	ErrorEmptyUpdateReplicationPayload                                                       = 6163
	ErrorReplicationScheduleUnspecified                                                      = 6164
	ErrDescribingVolume                                                                      = 6165
	ErrReplicationQuotaLimitExceeded                                                         = 6166
	ErrGetVolumeQuotaLimitInternal                                                           = 6167
	ErrValidateCreateReplicationCvpInternalGetVolumeCount                                    = 6168
	ErrVolumeQuotaLimitExceeded                                                              = 6169
	ErrValidateGetVolumeReplicationCreation                                                  = 6170
	ErrValidateGetVolumeReplicationCreationVolumeNotFound                                    = 6171
	ErrGetVolumeCreateTokenInUseRemoteShareName                                              = 6172
	ErrGetVolumeCreateTokenInUseRemoteResourceID                                             = 6173
	ErrValidateCreateDummyReplication                                                        = 6174
	ErrServiceLevelMismatch                                                                  = 6175
	ErrONTAPClusterNotFoundError                                                             = 6176
	ErrValidateCreateReplicationCvpInternalGetReplicationCount                               = 6177
	ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsBadRequest          = 6178
	ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsInternalServerError = 6179
	ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsUnauthorized        = 6180
	ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsForbidden           = 6181
	ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsNotFound            = 6182
	ErrGoogleProxyInternalGetMultipleReplicationsGetActiveReplicationJobsUnknown             = 6183
	ErrJobFailed                                                                             = 6184
	ErrGoogleProxyInternalResumeReplication                                                  = 6185
	ErrVolumeNotOnlineForReplicationResume                                                   = 6186
	ErrDestinationVolumeUsedSizeGreaterThanSourceVolumeAvailableQuota                        = 6187
	ErrFailedToGetLunDetailsFromOntap                                                        = 6188
	ErrGoogleProxyInternalStopReplication                                                    = 6189
	ErrProviderGetVolumeReplication                                                          = 6190
	ErrProviderBreakVolumeReplication                                                        = 6191
	ErrProviderAbortVolumeReplication                                                        = 6192
	ErrGoogleProxyInternalReverseReplication                                                 = 6193
	ErrGoogleProxyUpdateReplicationAttributes                                                = 6194
	ErrGoogleProxyInternalUpdateVolumeReplication                                            = 6195
	ErrGoogleProxyInternalDeleteVolumeReplicationError                                       = 6196
	ErrGoogleProxyInternalStopVolumeReplicationError                                         = 6197
	ErrGoogleProxyInternalReleaseVolumeReplicationError                                      = 6198
	ErrGoogleProxyDeleteVolumeError                                                          = 6199
	ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotDestinationError                     = 6200
	ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotSourceError                          = 6201
	ErrGoogleProxyInternalGetMultipleReplicationsForDeleteError                              = 6202
	ErrGoogleProxyGetMultipleReplications                                                    = 6203
	ErrGoogleProxyDescribePool                                                               = 6204
	ErrGoogleProxyInternalUpdateVolume                                                       = 6205
	ErrProviderDeleteVolumeReplication                                                       = 6206
	ErrDeleteVolume                                                                          = 6207
	ErrCreateInternalReplication                                                             = 6208
	ErrDescribingDestinationVolume                                                           = 6209
	ErrCleanupVolumeReplicationAfterReverse                                                  = 6210
	ErrVSAClusterOperationFailed                                                             = 6211
	ErrDatabaseListPoolsForAccount                                                           = 6212
	ErrDatabaseUpdateAccountState                                                            = 6213
	ErrDestPoolTieringPolicyMismatch                                                         = 6214
	ErrDestVolumeTieringThresholdOutOfRange                                                  = 6215

	ErrKMSRotate                        = 8001
	ErrServiceAccountNotFound           = 8002
	ErrKMSDeleteSDE                     = 8003
	ErrKmsConfigNotFound                = 8004
	ErrGettingKmsServiceAccount         = 8005
	ErrDecryptingServiceAccountPassword = 8006
	ErrorSynchronizingServiceAccountKey = 8007
	ErrZoneMachineTypeValidation        = 8008

	// FlexCache specific errors (10000-10999 range)
	ErrCreatingFlexCacheVolume   = 10001
	ErrUnmountingFlexCacheVolume = 10002
	ErrDeletingFlexCacheVolume   = 10003
	ErrUpdateFlexCacheVolume     = 10004

	// Peering specific errors (11000-11999 range)
	ErrClusterPeerError    = 11000
	ErrClusterPeerTimeout  = 11001
	ErrSVMPeerError        = 11002
	ErrSVMPeerTimeout      = 11003
	ErrDeletingClusterPeer = 11004
	ErrDeletingSVMPeer     = 11005

	// Backup specific errors (12000-12999 range)
	ErrImmutableValidationWithUpdatingBackupPolicy = 12001
	ErrImmutableValidationWithUpdatingBackupVault  = 12002
)

// ErrorMessage struct represents the structure of each error message in the JSON file.
type ErrorMessage struct {
	Description string `json:"description"`
	Message     string `json:"message"`
	Retriable   *bool  `json:"retriable,omitempty"`
	HttpCode    *int   `json:"http_code,omitempty"`
}

// errorMap is a map of error names to their corresponding ErrorMessage.
var errorMap map[int]ErrorMessage

// CustomError is our custom error type that includes an error code and retriable flag.
type CustomError struct {
	TrackingID  int
	Message     string
	Retriable   bool
	HttpCode    *int  // HttpCode is the HTTP code associated with the error
	OriginalErr error // OriginalErr holds the original error in case this is a wrapped error
}

// Error implements the error interface for CustomError.
func (e *CustomError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s", e.Message)
}

// Unwrap returns the originalErr error if there is one.
func (e *CustomError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.OriginalErr
}

// IsRetriable returns true if the error is marked as retriable.
func (e *CustomError) IsRetriable() bool {
	if e == nil {
		return false
	}
	return e.Retriable
}

// IsError returns true if the TrackingID is same as queried TrackingID.
func (e *CustomError) IsError(trackingID int) bool {
	if e == nil {
		return false
	}
	return e.TrackingID == trackingID
}

// LogError logs the error message along with its TrackingID.
func (e *CustomError) LogError() {
	if e == nil {
		return
	}
	slog.String("Error", e.Error())
}

// GetHttpCode returns the HTTP code associated with the error.
func (e *CustomError) GetHttpCode() (bool, int) {
	if e == nil {
		return false, 400
	}
	if e.HttpCode != nil {
		return true, *e.HttpCode
	}
	return false, 400 // Default HTTP code if not specified
}

func (e *CustomError) GetMessage() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// LogOriginalError logs the Original error message along with its code.
func (e *CustomError) LogOriginalError() {
	if e == nil || e.OriginalErr == nil {
		return
	}
	slog.String("Original Error", e.OriginalErr.Error())
}

// NewVCPError creates a new CustomError based on the given error name.
func NewVCPError(trackingID int, originalErr error) *CustomError {
	if errMsg, ok := errorMap[trackingID]; ok {
		if errMsg.Retriable == nil {
			// Default to false if retriable is not specified in the JSON file.
			errMsg.Retriable = new(bool)
			*errMsg.Retriable = false
		}

		return &CustomError{
			TrackingID:  trackingID,
			Message:     errMsg.Message,
			Retriable:   *errMsg.Retriable,
			HttpCode:    errMsg.HttpCode,
			OriginalErr: originalErr,
		}
	}
	// If the error name is not defined, create a generic non-retriable error with the original error.
	return &CustomError{
		TrackingID:  ErrInternalServerError, // Default to ErrInternalServerError
		Message:     fmt.Sprintf("%s", originalErr.Error()),
		Retriable:   false,
		OriginalErr: originalErr,
	}
}

// Is reports whether any error in err's tree matches target.
func Is(err error, target error) bool {
	return errors.Is(err, target)
}

// As finds the first error in err's tree that matches target, and if one is found, sets
// target to that error value and returns true. Otherwise, it returns false.
func As(err error, target any) bool {
	return errors.As(err, target)
}

// GetErrorMessageByTrackingID returns the error details pertaining to the given TrackingID.
func GetErrorMessageByTrackingID(trackingID int) *ErrorMessage {
	if errMsg, ok := errorMap[trackingID]; ok {
		return &errMsg
	}

	httpCode := new(int)
	*httpCode = 500
	return &ErrorMessage{HttpCode: httpCode, Message: "undefined error"}
}

// WrapAsTemporalApplicationError wraps a given error as a Temporal application error if it is a CustomError.
// Otherwise, it returns the original error unchanged.
func WrapAsTemporalApplicationError(err error) error {
	var customError *CustomError
	if As(err, &customError) {
		return temporal.NewApplicationError(err.Error(), "CustomError", customError.TrackingID, customError.OriginalErr.Error())
	}

	return err
}

// WrapAsNonRetryableTemporalApplicationError wraps a given error as a Temporal application error and marks it as non-retryable if it is a CustomError.
// Otherwise, it returns the original error unchanged.
func WrapAsNonRetryableTemporalApplicationError(err error) error {
	var customError *CustomError
	if As(err, &customError) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "CustomError", err, customError.TrackingID, customError.OriginalErr.Error())
	}

	return err
}

// ExtractCustomerFacingErrorMessage traverses the error chain and returns the most user-friendly error message
// from a CustomError, if present. If no CustomError is found, it returns a generic internal error message.
func ExtractCustomerFacingErrorMessage(ctx interface{}, err error) string {
	logger := util.GetLogger(ctx)
	errorMessage := DefaultErrorMessage
	var applicationErr *temporal.ApplicationError

	if As(err, &applicationErr) {
		if applicationErr.Type() == CustomErrorType {
			var (
				trackingID   int
				errorDetails string
			)
			err = applicationErr.Details(&trackingID, &errorDetails)
			if err != nil {
				logger.Warn("Could not extract trackingID from CustomError")
			} else {
				logger.Debugf("Extracted trackingID: %d and errorDetails: %s", trackingID, errorDetails)
				errorMessage = GetErrorMessageByTrackingID(trackingID).Message
			}
		}
	}
	return errorMessage
}

func ExtractCustomError(err error) *CustomError {
	var applicationErr *temporal.ApplicationError
	var panicError *temporal.PanicError
	var customErr *CustomError

	if As(err, &panicError) {
		return NewVCPError(ErrInternalServerError, Newf("Panic error occurred in activity: error: %v\nstackTrace: %v", panicError.Error(), panicError.StackTrace()))
	} else if As(err, &applicationErr) {
		if applicationErr.Type() == CustomErrorType {
			var (
				trackingID   int
				errorDetails string
			)
			err = applicationErr.Details(&trackingID, &errorDetails)
			if err != nil {
			} else {
				return NewVCPError(trackingID, New(errorDetails))
			}
		}
	} else if As(err, &customErr) {
		// If the error is already a CustomError, return it directly.
		return customErr
	}

	return NewVCPError(ErrInternalServerError, err)
}
