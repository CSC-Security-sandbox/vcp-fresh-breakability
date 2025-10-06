package models

import "time"

// Backup describes a backup in the cloud volumes model
type Backup struct {
	OwnerID                          string
	BackupID                         string
	VolumeID                         string
	UseExistingSnapshot              bool
	Region                           string
	Name                             string
	Tag                              string
	Type                             string
	LifeCycleState                   string
	LifeCycleStateDetails            string
	LifeCycleTrackingID              int32
	SizeInBytes                      uint64
	CreationTime                     time.Time
	SnapshotCreationTime             *time.Time
	CompletionTime                   *time.Time
	Jobs                             []*Job
	ProgressPercentage               uint64
	BytesTransferred                 uint64
	EndpointUUID                     string
	StorageClass                     string
	ExternalUUID                     string
	VolumeName                       string
	BackupVaultID                    string
	SnapshotName                     string
	VolumeVendorID                   string
	BackupPolicyName                 string
	Description                      *string
	BackupVaultName                  string
	BackupsLogicalSize               *int64
	AccountName                      *string
	ShouldHydrate                    bool
	ConstituentVolumesPerAggregate   int
	NumberOfAggregates               int
	OntapVolumeStyle                 string
	StorageAccountUUID               string
	SatisfiesPzs                     bool
	SatisfiesPzi                     bool
	BucketName                       string
	IsRemoteBackup                   bool
	IsBackupImmutable                bool
	MinimumEnforcedRetentionDuration *int64
	SourceRegion                     *string
	BackupRegion                     *string
}

type HydrateBackup struct {
	ResourceId            string                 `json:"name"`
	BackupId              string                 `json:"netapp_uuid"`
	VolumeUsageBytes      *uint64                `json:"volume_usage_bytes"`
	AssetLocationMetadata *AssetLocationMetadata `json:"asset_location_metadata"`
	SourceVolume          string                 `json:"source_volume"`
}

type AssetLocationMetadata struct {
	ChildAssets []*ChildAsset `json:"child_assets"`
}

type ChildAsset struct {
	AssetType  string   `json:"asset_type"`
	AssetNames []string `json:"asset_names"`
}
