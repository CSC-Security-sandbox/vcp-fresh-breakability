package common

// CreateKmsConfigParams describes parameters supplied to CreateKmsConfigActivity
type CreateKmsConfigParams struct {
	AccountName         string
	Name                string
	Description         string
	Instructions        string
	KeyFullPath         string
	KmsState            string
	KmsStateDetails     string
	ResourceID          string
	ServiceAccountEmail string
	UUID                string
	LocationID          string
	ProjectNumber       string
	XCorrelationID      string
	OperationUri        string
	OperationDone       bool
}

// GetKmsConfigParams describes parameters supplied to CreateKmsConfigActivity
type GetKmsConfigParams struct {
	AccountName         string
	Name                string
	Description         string
	Instructions        string
	KeyFullPath         string
	KmsState            string
	KmsStateDetails     string
	ResourceID          string
	ServiceAccountEmail string
	UUID                string
	LocationID          string
	ProjectNumber       string
}

// CheckKmsConfigParams check kms config reachability
type CheckKmsConfigParams struct {
	KmsConfigUUID string
	LocationID    string
	ProjectNumber string
}

type DeleteKmsConfigParams struct {
	KmsConfigID    string
	AccountName    string
	Region         string
	XCorrelationID string
}

type MigrateKmsConfigParams struct {
	LocationID     string
	ProjectNumber  string
	UUID           string
	SdeUUID        string
	State          string
	Name           string
	AccountName    string
	XCorrelationID string
	ResourceID     string
}
