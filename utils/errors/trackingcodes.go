package errors

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// Error codes that can be used to differentiate errors by consuming programs
const (
	IPRangeClash                                           = 1000
	AllocationAlreadyExists                                = 1001
	AllocationNoLongerValid                                = 1002
	CreatingSnapshotDPVolume                               = 1003
	CreatingSnapshotConcurrentJobs                         = 1004
	DeleteSnapshotCloningOngoing                           = 1005
	DeleteSnapshotTransition                               = 1006
	DeleteSnapshotVolReplication                           = 1007
	DeleteVolumeActiveReplication                          = 1008
	GetUserAccount                                         = 1009
	QueryDNSServer                                         = 1010
	DataLifNotFound                                        = 1011
	VLANConflict                                           = 1012
	AggregationPlacementTimeout                            = 1013
	NoHostReachable                                        = 1014
	BackupCreationVaultNotAssigned                         = 1016
	RateLimitExceeded                                      = 1017
	VolumeNotFound                                         = 1018
	InodesUpdatePerDiskSpace                               = 1019
	LdapClientUserDNIsInIncorrectOrder                     = 1020
	LdapClientGroupDNIsInIncorrectOrder                    = 1021
	LdapClientUserDNMissingDCValue                         = 1022
	LdapClientGroupDNMissingDCValue                        = 1023
	DeleteVolumeRestrictedAction                           = 1024
	SvmMigrationPauseNeeded                                = 1025
	SvmMigrationCancelNeeded                               = 1026
	VolumeIsBusyONTAP                                      = 1027
	SvmMigrationRetryNeeded                                = 1028
	VolumeBreakLocksOperationRunning                       = 1029
	DoubleEncryptionHostNotFound                           = 1030
	LockedSVM                                              = 1031
	ThroughputExceedsPool                                  = 1032
	DeleteVolumeWithSnapMirrorPolicyAttached               = 1313
	CreatingVolumeWithExpiredAKVKey                        = 1314
	CreatingVolumeRPCTimedOut                              = 1315
	RekeyVServerInBlockedState                             = 1316
	SubvolumesNotEnabled                                   = 1317
	SubvolumesNotEnabledOnVolume                           = 1318
	SubvolumeNotFound                                      = 1319
	SubvolumeParentPathNotFound                            = 1320
	CreatingSnapshotShortTermCloneVolume                   = 1321
	ResizeCloneVolumeOperationRunning                      = 1322
	DeleteParentVolumeWithShortTermClones                  = 1323
	EnablingBackupWithoutBackupVaultIDNotAllowedForNewUser = 1324
	ShortTermCloneVolumesLimitReached                      = 1325
	SnapshotNotFound                                       = 1326
	EnableSubvolumesOperationNotAvailableForSMBVolume      = 1327
	CannotDeleteLatestBackup                               = 1328
	CannotDeleteLastBackupWhenBackupPolicyAssigned         = 1329
	BackupNotFound                                         = 1330
	BackupAlreadyExistsWithTheSameSnapshotName             = 1331
	BackupDeletionIsAlreadyInProgress                      = 1332
	CannotDeleteBackupWhenRestoreIsInProgress              = 1333
	CannotProcessNewRequestForVolume                       = 1334
	DailyLimitOfManualBackupsReached                       = 1335
	ActiveBackupTransferInProgress                         = 1336
	CannotModifyBackupVaultWhenBackupsPresent              = 1337
	CannotDeleteBackupVaultContainingBackups               = 1338
	CannotDeleteSnapshotInUse                              = 1339
	CreatingVolumeNodeIsDownBeforeVlanCreation             = 1340
	AuthenticationFailureWith4xxResponse                   = 1341
	VolumeInOfflineState                                   = 1343
	CannotCreateAnotherReplicationJob                      = 1344
	StaleSnapmirrorCleanupNeeded                           = 1345
	ClusterPeerExpired                                     = 1346
	CannotDeleteBackupVaultWithVolumeAssigned              = 1347
	CannotCreateMultipleBackupVaultInRegion                = 1348
	BackupVaultAlreadyExists                               = 1349
	ConcurrentBackupCreationForVolumeNotAllowed            = 1350
	CannotDisableBackupOnVolumeWhileBackupCreation         = 1351
	CannotDeleteVolumeWhileBackupCreation                  = 1352
	CannotDeleteSnapshotWhileVolumeDeletionOngoing         = 1353
	DeleteBroadcastDomainPortsStillExistError              = 1354
	CreatingVolumeWithNetworkIssues                        = 1355
	CreatingVolumeWithConfigurationIssues                  = 1356
	CreatingVolumeUnableToUseKeyConflict                   = 1357
	SvmPeerNotFound                                        = 1358
	RemoteBackupNotFound                                   = 1359
	CannotAuthorizeVolumeReplicationWhenCloneProgress      = 1360
	// New tracking IDs common for GCP 1P SO, PO and ANF
	// Ranges allotted :
	// ActiveDirectory errors : 1400  - 1499
	// ActiveDirectory with LDAP : 1500 -1549
	// ActiveDirectory with SMB : 1550 -1599
	// SMB : 1600 -1699
	// KMS Errors: 1700 - 1729

	AdUnreachableKDC               = 1400
	AdUnsetKDCHostnameOrIP         = 1402
	AdUnreachableLDAPServer        = 1403
	ADInvalidUser                  = 1404
	AdInvalidOU                    = 1405
	AdIncorrectUsername            = 1406
	ADDefaultSiteInvalid           = 1407
	ADPasswordMismatch             = 1408
	AdDefaultSiteValidationFailed  = 1409
	AdDCDiscoveryFailed            = 1411
	AdInsufficientPermission       = 1412
	AdUpdateInProgress             = 1413
	ADAESEncryptionSettingsInvalid = 1414
	ADUserDisabled                 = 1415
	ADMachineAccountDoesNotExist   = 1417
	ADDCUnreachable                = 1418

	AESEncryptionNotEnabled = 1500
	AdLdapNetworkIssue      = 1502
	LDAPServerNotIdentified = 1503
	LdapUpdateInProgress    = 1504
	LDAPUserDoesNotExists   = 1505
	ADLDAPSearchTimeout     = 1506
	ADLDAPBindPtrMissing    = 1507

	AdSmbDuplicateSMB    = 1551
	AdSmbKDCNetworkIssue = 1553
	ADPasswordNotInSync  = 1554
	LSAFirewallIssue     = 1555

	KMSKeyHealthCheckFailed = 1700
)

// GetTrackingID calls GetTrackingID for the specific error type of the error parameter
func GetTrackingID(err error) int {
	u, ok := err.(interface {
		GetTrackingID() int
	})
	if !ok {
		return 0
	}
	return u.GetTrackingID()
}

// getStatusCodeFromError returns the http error code from the error message
// Error Messages from Swagger are always in the format [<RequestType> <Path>][<ErrorCode>] <ErrorType>
func getStatusCodeFromError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	msg := err.Error()
	msgArray := strings.Split(msg, "[")
	if len(msgArray) >= 3 {
		statusCodeArray := strings.Split(msgArray[2], "]")
		if statusCodeArray[0] == "" {
			return http.StatusInternalServerError
		}

		code, err := strconv.Atoi(statusCodeArray[0])
		if err != nil {
			return http.StatusInternalServerError
		}
		return code
	}

	// default to internal server error
	return http.StatusInternalServerError
}

// GetStatusCodeFromError returns the http error code from the error message
func GetStatusCodeFromError(err error) int {
	var userInputValidationErr *UserInputValidationErr
	var notFoundErr *NotFoundErr
	var conflictErr *ConflictErr
	var goneErr *GoneErr
	var insufficientStorageErr *InsufficientStorageErr
	var notSupportedErr *NotSupportedErr
	var unavailableErr *UnavailableErr
	var notReadyErr *NotReadyErr
	switch {
	case errors.As(err, &userInputValidationErr):
		return http.StatusBadRequest
	case errors.As(err, &notFoundErr):
		return http.StatusNotFound
	case errors.As(err, &conflictErr):
		return http.StatusConflict
	case errors.As(err, &goneErr):
		return http.StatusGone
	case errors.As(err, &insufficientStorageErr):
		return http.StatusInsufficientStorage
	case errors.As(err, &notSupportedErr):
		return http.StatusNotImplemented
	case errors.As(err, &unavailableErr):
		return http.StatusServiceUnavailable
	case errors.As(err, &notReadyErr):
		return http.StatusServiceUnavailable
	default:
		// fall back
		return getStatusCodeFromError(err)
	}
}
