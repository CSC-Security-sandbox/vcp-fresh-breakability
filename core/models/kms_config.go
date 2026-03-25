package models

const (
	ProxyTypeVcp = "vcp"
	ProxyTypeCvp = "cvp"
)

// KmsConfig describes a KMS configuration in the VCP cloud model
type KmsConfig struct {
	BaseModel
	Name         string
	Description  string
	State        string
	StateDetails string

	KeyRing         string
	KeyRingLocation string
	KeyName         string

	AccountID         int64
	CustomerProjectID string
	KeyProjectID      string
	ServiceAccountID  *int64
	ResourceID        string
	KmsAttributes     *KmsAttributes
	ServiceAccount    *ServiceAccount
}
type KmsAttributes struct {
	SdeKmsConfigUUID          string
	SdeServiceAccountEmail    string
	Instructions              string
	SdeKmsConfigIsHealthy     bool
	SdeKmsConfigHealthError   string
	SdeKmsConfigOperationURI  string
	SdeKmsConfigOperationDone bool
	CreationMode              string
	VcpServiceAccountEmail    string
}

const (
	KmsCreationModeSDE = "SDE"
	KmsCreationModeVCP = "VCP"
)

// GetCreationMode returns the creation mode, defaulting to SDE for backward compatibility.
func (a *KmsAttributes) GetCreationMode() string {
	if a == nil || a.CreationMode == "" {
		return KmsCreationModeSDE
	}
	return a.CreationMode
}

// IsVCPCreated returns true if the KMS config was created via VCP (no SDE involvement).
func (a *KmsAttributes) IsVCPCreated() bool {
	return a.GetCreationMode() == KmsCreationModeVCP
}

// GetServiceAccountEmail returns the appropriate service account email
// based on the creation mode. VCP configs use the VCP SA email;
// SDE configs use the SDE SA email.
func (a *KmsAttributes) GetServiceAccountEmail() string {
	if a == nil {
		return ""
	}
	if a.VcpServiceAccountEmail != "" {
		return a.VcpServiceAccountEmail
	}
	return a.SdeServiceAccountEmail
}

// KmsConfigCheck describes an gcp kms configuration check object in the cloud volumes model
type KmsConfigCheck struct {
	ProxyType   string
	Email       string
	IsHealthy   bool
	HealthError string
	KmsConfig   *KmsConfig
}
