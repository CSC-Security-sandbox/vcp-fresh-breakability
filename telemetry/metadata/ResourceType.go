package metadata

// ResourceType represents the type of resource.
type ResourceType string

// String converts the ResourceType to a string.
func (rt ResourceType) String() string {
	return string(rt)
}

// ResourceType constants
const (
	Volume                          ResourceType = "VOLUME"
	VolumePool                      ResourceType = "VOLUME_POOL"
	VolumeReplicationRelationship   ResourceType = "VOLUME_REPLICATION_RELATIONSHIP"
	CBS                             ResourceType = "CBS"
	MetricsNamePrefixPoolFirstParty              = "netapp.googleapis.com/storage_pool/"
)
