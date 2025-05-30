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

	KmsAttributes *KmsAttributes
}
type KmsAttributes struct {
	SdeKmsConfigUUID       string
	SdeServiceAccountEmail string
}
