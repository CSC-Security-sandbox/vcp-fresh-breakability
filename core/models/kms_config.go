package models

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
	ServiceAccountID  int64
	ResourceID        string
	KmsAttributes     *KmsAttributes
	ServiceAccount    *ServiceAccount
}
type KmsAttributes struct {
	SdeKmsConfigUUID        string
	SdeServiceAccountEmail  string
	Instructions            string
	SdeKmsConfigIsHealthy   bool
	SdeKmsConfigHealthError string
}

// KmsConfigCheck describes an gcp kms configuration check object in the cloud volumes model
type KmsConfigCheck struct {
	Email       string
	IsHealthy   bool
	HealthError string
	KmsConfig   *KmsConfig
}
