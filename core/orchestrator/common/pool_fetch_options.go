package common

// PoolFetchOptions controls which optional DB relations are preloaded when fetching pools.
// Use PoolFetchOptionsFromFields to derive these flags from a swagger field set.
type PoolFetchOptions struct {
	// NeedKmsConfig requests preloading of the KmsConfig relation.
	// Required for fields: kmsConfigId, kmsConfigResourceId, encryptionType.
	NeedKmsConfig bool
	// NeedActiveDirectory requests preloading of the ActiveDirectory relation.
	// Required for fields: activeDirectoryConfigId, activeDirectoryResourceId.
	NeedActiveDirectory bool
	// NeedExpertModeCapacity requests enrichment from the expert-mode volume table.
	// Required for fields that expose used/total capacity on ONTAP-mode pools.
	NeedExpertModeCapacity bool
}

// PoolFetchOptionsFromFields derives PoolFetchOptions from the requested swagger field set.
// When fieldSet is nil (no fields were requested), the proxy layer returns only poolId,
// so no relations or enrichment are needed.
func PoolFetchOptionsFromFields(fieldSet map[string]bool) PoolFetchOptions {
	if fieldSet == nil {
		return PoolFetchOptions{}
	}
	return PoolFetchOptions{
		NeedKmsConfig:       fieldSet["kmsConfigId"] || fieldSet["kmsConfigResourceId"] || fieldSet["encryptionType"],
		NeedActiveDirectory: fieldSet["activeDirectoryConfigId"] || fieldSet["activeDirectoryResourceId"],
		NeedExpertModeCapacity: fieldSet["allocatedBytes"] || fieldSet["numberOfVolumes"] ||
			fieldSet["totalThroughputMibps"] || fieldSet["totalIops"] ||
			fieldSet["availableThroughputMibps"] || fieldSet["sizeInBytes"],
	}
}
