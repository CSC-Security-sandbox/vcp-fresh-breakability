package metadata

type ResourceMetadata struct {
	ResourceUUID    *string
	ResourceName    *string
	ResourceType    ResourceType
	SizeInBytes     *int64
	RegionName      *string
	Tags            map[string]string
	AutoTierEnabled *bool
	AccountName     *string
}

func (m *ResourceMetadata) SetResourceUUID(uuid string) {
	m.ResourceUUID = &uuid
}

func (m *ResourceMetadata) SetResourceName(name string) {
	m.ResourceName = &name
}

func (m *ResourceMetadata) SetResourceType(resourceType ResourceType) {
	m.ResourceType = resourceType
}

func (m *ResourceMetadata) SetSizeInBytes(size int64) {
	m.SizeInBytes = &size
}

func (m *ResourceMetadata) SetRegionName(region string) {
	m.RegionName = &region
}

func (m *ResourceMetadata) SetAccountName(accountName string) {
	m.AccountName = &accountName
}
