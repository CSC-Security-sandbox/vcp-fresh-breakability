package models

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

// LifeCycle state constants re-exported from database/datamodel.
// Storage state is owned by the database module; this layer keeps the
// re-exports so core/orchestrator and core/vsa keep their existingimport surface
const (
	LifeCycleStateCreating        = datamodel.LifeCycleStateCreating
	LifeCycleStatePreparing       = datamodel.LifeCycleStatePreparing
	LifeCycleStateOngoing         = datamodel.LifeCycleStateOngoing
	LifeCycleStateReverting       = datamodel.LifeCycleStateReverting
	LifeCycleStateUndeleting      = datamodel.LifeCycleStateUndeleting
	LifeCycleStateCompleted       = datamodel.LifeCycleStateCompleted
	LifeCycleStateRestoring       = datamodel.LifeCycleStateRestoring
	LifeCycleStateSplitting       = datamodel.LifeCycleStateSplitting
	LifeCycleStateAvailable       = datamodel.LifeCycleStateAvailable
	LifeCycleStateREADY           = datamodel.LifeCycleStateREADY
	LifeCycleStateInUse           = datamodel.LifeCycleStateInUse
	LifeCycleStateDisabled        = datamodel.LifeCycleStateDisabled
	LifeCycleStateDisabling       = datamodel.LifeCycleStateDisabling
	LifeCycleStateEnabling        = datamodel.LifeCycleStateEnabling
	LifeCycleStateUpdating        = datamodel.LifeCycleStateUpdating
	LifeCycleStateDeleting        = datamodel.LifeCycleStateDeleting
	LifeCycleStateDeleted         = datamodel.LifeCycleStateDeleted
	LifeCycleStateError           = datamodel.LifeCycleStateError
	LifeCycleStateRetained        = datamodel.LifeCycleStateRetained
	LifeCycleStateCreated         = datamodel.LifeCycleStateCreated
	LifeCycleStateKeyCheckPending = datamodel.LifeCycleStateKeyCheckPending
	LifeCycleStateMigrating       = datamodel.LifeCycleStateMigrating
	LifeCycleStateDegraded        = datamodel.LifeCycleStateDegraded // Pool degraded due to JSWAP switch to ephemeral_disk for takeover issues
	LifeCycleStateUnknown         = datamodel.LifeCycleStateUnknown  // Unknown state, used when the state is not decided yet

	// No datamodel counterpart yet — keep literal in models.
	KmsConfigV1betaKmsStateKEYSTATEUNSPECIFIED = "KEY_STATE_UNSPECIFIED"

	ServiceTypeGCNV         = datamodel.ServiceTypeGCNV
	ServiceTypeCrossProject = datamodel.ServiceTypeCrossProject

	LifeCycleStateCreatingDetails            = datamodel.LifeCycleStateCreatingDetails
	LifeCycleStateRevertingDetails           = datamodel.LifeCycleStateRevertingDetails
	LifeCycleStateUndeletingDetails          = datamodel.LifeCycleStateUndeletingDetails
	LifeCycleStateRestoringDetails           = datamodel.LifeCycleStateRestoringDetails
	LifeCycleStateAvailableDetails           = datamodel.LifeCycleStateAvailableDetails
	LifeCycleStateDisabledDetails            = datamodel.LifeCycleStateDisabledDetails
	LifeCycleStateUpdatingDetails            = datamodel.LifeCycleStateUpdatingDetails
	LifeCycleStateSyncDetails                = datamodel.LifeCycleStateSyncDetails
	LifeCycleStateDeletingDetails            = datamodel.LifeCycleStateDeletingDetails
	LifeCycleStateSplittingDetails           = datamodel.LifeCycleStateSplittingDetails
	LifeCycleStateDeletedDetails             = datamodel.LifeCycleStateDeletedDetails
	LifeCycleStateCompletedDetails           = datamodel.LifeCycleStateCompletedDetails
	LifeCycleStateRetainedDetails            = datamodel.LifeCycleStateRetainedDetails
	LifeCycleStateOngoingDetails             = datamodel.LifeCycleStateOngoingDetails
	LifeCycleStateCreationErrorDetails       = datamodel.LifeCycleStateCreationErrorDetails
	LifeCycleStateUpdateErrorDetails         = datamodel.LifeCycleStateUpdateErrorDetails
	LifeCycleStateDeletionErrorDetails       = datamodel.LifeCycleStateDeletionErrorDetails
	LifeCycleStateReadyDetails               = datamodel.LifeCycleStateReadyDetails
	LifeCycleStateCreatedDetails             = datamodel.LifeCycleStateCreatedDetails
	LifeCycleStateUnknownDetails             = datamodel.LifeCycleStateUnknownDetails // Unknown state details, used when the state is not decided yet
	LifeCycleStateInUseDetails               = datamodel.LifeCycleStateInUseDetails
	LifeCycleStateMigratingDetails           = datamodel.LifeCycleStateMigratingDetails
	LifeCycleStateDegradedDetails            = datamodel.LifeCycleStateDegradedDetails
	LifeCycleStateVolMigratingDetails        = datamodel.LifeCycleStateVolMigratingDetails
	LifeCycleStateHyperscalerDisabledDetails = datamodel.LifeCycleStateHyperscalerDisabledDetails

	// Backup vault type constants
	BackupVaultTypeCrossRegion                         = "CROSS_REGION"
	VolumeReplicationBreakRelationshipQuotaRuleFailure = "Break operation is successful and destination volume has become RW, but post break quota rule creation operation failed"

	// Backup vault CMEK encryption states (kept in sync with external API enums).
	EncryptionStatePending    = "ENCRYPTION_STATE_PENDING"
	EncryptionStateCompleted  = "ENCRYPTION_STATE_COMPLETED"
	EncryptionStateInProgress = "ENCRYPTION_STATE_IN_PROGRESS"
	EncryptionStateFailed     = "ENCRYPTION_STATE_FAILED"

	AccountStateDisabled            = datamodel.AccountStateDisabled
	AccountStateEnabled             = datamodel.AccountStateEnabled
	AccountStateDeleted             = datamodel.AccountStateDeleted
	AccountStateEnabling            = datamodel.AccountStateEnabling
	AccountStateDisabling           = datamodel.AccountStateDisabling
	AccountStateHyperscalerDisabled = datamodel.AccountStateHyperscalerDisabled

	VolumeStateOffline = "OFFLINE"

	ReadWrite                       = "READ_WRITE"
	ReadOnly                        = "READ_ONLY"
	ReadNone                        = "READ_NONE"
	AnyAccessProtocol               = "any"
	NoneAccessProtocol              = "none"
	ExportAuthenticationFlavorNever = "never"
	// ExportAuthenticationFlavorAny captures enum value "any"
	ExportAuthenticationFlavorAny  = "any"
	ExportAuthenticationFlavorNone = "none"
	ExportAuthenticationFlavorSys  = "Sys"
	// ExportAuthenticationFlavorKrb5 captures enum value "krb5"
	ExportAuthenticationFlavorKrb5 = "krb5"

	// ExportAuthenticationFlavorKrb5i captures enum value "krb5i"
	ExportAuthenticationFlavorKrb5i = "krb5i"

	// ExportAuthenticationFlavorKrb5p captures enum value "krb5p"
	ExportAuthenticationFlavorKrb5p = "krb5p"
	RootAnonymousUser               = "root"
	ChownModeRestricted             = "restricted"
	DefaultExportPolicyName         = "default"
	AllowedAllClients               = "0.0.0.0/0"
	IgnoreNtfsUnixSecurity          = "ignore"
	DefaultIndexExportPolicyRule    = int64(7)

	// Clone states
	CloneStateCloned           = "SPLIT_STATE_NOT_SPLITTING"
	CloneStateSplitting        = "SPLIT_STATE_IN_PROGRESS"
	CloneStateErrorInSplitting = "SPLIT_STATE_FAILED"

	// ZoneSwitching States
	ZoneSwitching = "SWITCHING"
	ZoneSwitched  = "SWITCHED"
	ZonePrimary   = "PRIMARY"
)

const (
	InitiatingClusterPeering = "Initiating cluster peering on destination cluster"
	InitiatingSVMPeering     = "Initiating SVM peering on destination cluster"
	WaitingForClusterPeering = "Waiting for cluster peering to be created on source cluster"
	ErrorDuringClusterPeer   = "Cluster peering failed, please try again"
	ClusterPeeringExpired    = "Cluster peering expired"
	WaitingForSVMPeering     = "Waiting for SVM peering to be accepted on source cluster"
	ErrorDuringSVMPeering    = "SVM peering failed, please try again"
	SVMPeeringExpired        = "SVM peering expired"
	ErrorUnencryptedVolume   = "Origin volume is not encrypted"
	ErrorCreatingCacheVolume = "Error creating cache volume"

	ClusterPeeringSourceUnreachable = "Source cluster unreachable, check network connections"
)

const (
	DefaultCode                  = 0
	ErrorDuringClusterPeerCode   = 100000
	ClusterPeeringExpiredCode    = 100001
	SourceClusterUnreachableCode = 100002
	WaitingForClusterPeeringCode = 100003
	ErrorDuringSVMPeeringCode    = 100004
	SVMPeeringExpiredCode        = 100005
	InitiatingSVMPeeringCode     = 100006
	WaitingForSVMPeeringCode     = 100007
	InitiatingClusterPeeringCode = 100008
	ErrorUnencryptedVolumeCode   = 100009
)

// SVM represents a single SVM resource
type SVM struct {
	BaseModel
	Name         string
	Description  string
	State        string
	StateDetails string
}

type Account struct {
	BaseModel
	Name  string
	State string
	Tags  string
}

// BaseModel describes the base model shared by all other models
type BaseModel struct {
	ID        int64
	UUID      string
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

type UserCache struct {
	Time     time.Time
	SecretID string
	Password string
}

type CertCache struct {
	Time          time.Time
	CertificateID string
	Certificate   *Certificate
}

type Certificate struct {
	SignedCertificate        string
	PrivateKey               string
	InterMediateCertificates []string
	CommonName               string
}

type OntapEndpoint struct {
	IP  string `json:"ip"`
	DNS string `json:"dns"`
}

type UserCredentials struct {
	Username       string          `json:"username"`
	SecretID       string          `json:"secret_id"`
	CertificateID  string          `json:"certificate_id"`
	Password       string          `json:"password"`
	AuthType       int             `json:"auth_type"`
	OntapEndpoints []OntapEndpoint `json:"ontap_endpoints"`
	// Format: ca_pool_deployed_project_id/ca_pool_name/ca_name
	CaURI string `json:"ca_uri,omitempty"`
}

// GetCaURIWithFallback gets ca_uri from UserCredentials, falling back to environment variables if not set.
func (uc *UserCredentials) GetCaURIWithFallback() string {
	if uc == nil || uc.CaURI == "" {
		return env.BuildCaURI("", "", "")
	}
	return uc.CaURI
}

// ParseCaURIWithFallback parses ca_uri from UserCredentials, falling back to environment variables if not set.
func (uc *UserCredentials) ParseCaURIWithFallback() (caPoolDeployedProjectID, caPoolName, caName string) {
	if uc == nil || uc.CaURI == "" {
		return env.CaPoolDeployedProjectID, env.CaPoolName, env.CaName
	}
	return env.ParseCaURI(uc.CaURI)
}
