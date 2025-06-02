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
