package common

type HandleResourceEventParams struct {
	Description        string
	State              string
	ProjectNumber      string
	LocationId         string
	XCorrelationID     string
	ResourceType       string
	ResourceId         string
	ParentResourceID   string
	ParentResourceType string
}

const (
	ResourceStateV1ResourceTypeVolume       string = "Volume"
	ResourceStateV1ResourceTypeSnapshot     string = "Snapshot"
	ResourceStateV1ResourceTypeStoragePool  string = "StoragePool"
	ResourceStateV1ResourceTypeKmsConfig    string = "KmsConfig"
	ResourceStateV1ResourceTypeBackupPolicy string = "BackupPolicy"
	ResourceStateV1ResourceTypeAD           string = "ActiveDirectory"
	ResourceStateV1ResourceTypeHostGroup    string = "HostGroup"
	ResourceStateEnabled                    string = "ENABLED"
	ResourceLifeCycleStateEnabledDetails    string = "Enabled"
)

type HandleResourceEventResult struct {
	Done *bool   `json:"done,omitempty"`
	Name *string `json:"name,omitempty"`
}
