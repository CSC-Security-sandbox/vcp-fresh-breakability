package metadata

// ResourceType represents the type of resource.
type ResourceType string

// String converts the ResourceType to a string.
func (rt ResourceType) String() string {
	return string(rt)
}

// ResourceType constants
const (
	Volume                            ResourceType = "VOLUME"
	VolumePool                        ResourceType = "VOLUME_POOL"
	VolumeRegionalHA                  ResourceType = "VOLUME_REGIONAL_HA"
	VolumePoolRegionalHA              ResourceType = "VOLUME_POOL_REGIONAL_HA"
	VolumeReplicationRelationship     ResourceType = "VOLUME_REPLICATION_RELATIONSHIP"
	Backup                            ResourceType = "BACKUP"
	BackupVault                       ResourceType = "BACKUP_VAULT"
	CBS                               ResourceType = "CBS"
	MetricsNamePrefixPoolFirstParty                = "netapp.googleapis.com/storage_pool/"
	MetricsNamePrefixVolumeFirstParty              = "netapp.googleapis.com/volume/"
	MetricsNamePrefixBackupVaultFirstParty         = "netapp.googleapis.com/backup_vault/"
)
