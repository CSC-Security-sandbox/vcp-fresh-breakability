package models

import (
	"time"
)

const (
	LifeCycleStateCreating                     = "CREATING"
	LifeCycleStatePreparing                    = "PREPARING"
	LifeCycleStateOngoing                      = "ONGOING"
	LifeCycleStateReverting                    = "REVERTING"
	LifeCycleStateUndeleting                   = "UNDELETING"
	LifeCycleStateCompleted                    = "COMPLETED"
	LifeCycleStateRestoring                    = "RESTORING"
	LifeCycleStateSplitting                    = "SPLITTING"
	LifeCycleStateAvailable                    = "AVAILABLE"
	LifeCycleStateREADY                        = "READY"
	LifeCycleStateInUse                        = "IN_USE"
	LifeCycleStateDisabled                     = "DISABLED"
	LifeCycleStateDisabling                    = "DISABLING"
	LifeCycleStateEnabling                     = "ENABLING"
	LifeCycleStateUpdating                     = "UPDATING"
	LifeCycleStateDeleting                     = "DELETING"
	LifeCycleStateDeleted                      = "DELETED"
	LifeCycleStateError                        = "ERROR"
	LifeCycleStateRetained                     = "RETAINED"
	LifeCycleStateCreated                      = "CREATED"
	LifeCycleStateKeyCheckPending              = "KEY_CHECK_PENDING"
	LifeCycleStateMigrating                    = "MIGRATING"
	LifeCycleStateDegraded                     = "DEGRADED" // Pool degraded due to JSWAP switch to ephemeral_disk for takeover issues
	KmsConfigV1betaKmsStateKEYSTATEUNSPECIFIED = "KEY_STATE_UNSPECIFIED"
	LifeCycleStateUnknown                      = "UNKNOWN" // Unknown state, used when the state is not decided yet

	LifeCycleStateCreatingDetails            = "Creation in progress"
	LifeCycleStateRevertingDetails           = "Revert in progress"
	LifeCycleStateUndeletingDetails          = "Undelete in progress"
	LifeCycleStateRestoringDetails           = "Restore in progress"
	LifeCycleStateAvailableDetails           = "Available for use"
	LifeCycleStateDisabledDetails            = "Disabled"
	LifeCycleStateUpdatingDetails            = "Update in progress"
	LifeCycleStateSyncDetails                = "Sync in progress"
	LifeCycleStateDeletingDetails            = "Deletion in progress"
	LifeCycleStateSplittingDetails           = "Splitting in progress"
	LifeCycleStateDeletedDetails             = "Deleted"
	LifeCycleStateCompletedDetails           = "Completed"
	LifeCycleStateRetainedDetails            = "Retained"
	LifeCycleStateOngoingDetails             = "Ongoing"
	LifeCycleStateCreationErrorDetails       = "Error in creating"
	LifeCycleStateUpdateErrorDetails         = "Error in updating"
	LifeCycleStateDeletionErrorDetails       = "Error in deleting"
	LifeCycleStateReadyDetails               = "Ready for use"
	LifeCycleStateCreatedDetails             = "Created successfully"
	LifeCycleStateUnknownDetails             = "Unknown state" // Unknown state details, used when the state is not decided yet
	LifeCycleStateInUseDetails               = "In use"
	LifeCycleStateMigratingDetails           = "Kms config is in migrating state"
	LifeCycleStateDegradedDetails            = "Pool degraded due to takeover issues"
	LifeCycleStateVolMigratingDetails        = "Volume encryption in progress"
	LifeCycleStateHyperscalerDisabledDetails = "Hyperscaler disabled"

	AccountStateDisabled            = "DISABLED"
	AccountStateEnabled             = "ENABLED"
	AccountStateDeleted             = "DELETED"
	VolumeStateOffline              = "OFFLINE"
	AccountStateEnabling            = "ENABLING"
	AccountStateDisabling           = "DISABLING"
	AccountStateHyperscalerDisabled = "HYPERSCALERDISABLED"

	ReadWrite                       = "READ_WRITE"
	ReadOnly                        = "READ_ONLY"
	AnyAccessProtocol               = "any"
	NoneAccessProtocol              = "none"
	ExportAuthenticationFlavorNever = "never"
	ExportAuthenticationFlavorSys   = "Sys"
	RootAnonymousUser               = "root"
	ChownModeRestricted             = "restricted"
	DefaultExportPolicyName         = "default"
	AllowedAllClients               = "0.0.0.0/0"
	IgnoreNtfsUnixSecurity          = "ignore"
	DefaultIndexExportPolicyRule    = int64(7)
)

const (
	InitiatingClusterPeering = "Initiating cluster peering on destination cluster"
	WaitingForClusterPeering = "Waiting for cluster peering to be created on source cluster"
	ErrorDuringClusterPeer   = "Cluster peering failed, please try again"
	ClusterPeeringExpired    = "Cluster peering expired"
	WaitingForSVMPeering     = "Waiting for SVM peering to be established"
	ErrorDuringSVMPeering    = "SVM peering failed, please try again"
	SVMPeeringExpired        = "SVM peering expired"
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
	SecretID       string          `json:"secret_id"`
	CertificateID  string          `json:"certificate_id"`
	Password       string          `json:"password"`
	AuthType       int             `json:"auth_type"`
	OntapEndpoints []OntapEndpoint `json:"ontap_endpoints"`
}
