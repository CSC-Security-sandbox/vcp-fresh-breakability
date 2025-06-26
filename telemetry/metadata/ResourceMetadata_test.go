package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SetsResourceUUIDCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	uuid := "test-uuid"
	metadata.SetResourceUUID(uuid)
	assert.Equal(t, &uuid, metadata.ResourceUUID)
}

func Test_SetsResourceNameCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	name := "test-name"
	metadata.SetResourceName(name)
	assert.Equal(t, &name, metadata.ResourceName)
}

func Test_SetsResourceTypeCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	resourceType := Volume
	metadata.SetResourceType(resourceType)
	assert.Equal(t, resourceType, metadata.ResourceType)
}

func Test_SetsSizeInBytesCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	size := int64(1024)
	metadata.SetSizeInBytes(size)
	assert.Equal(t, &size, metadata.SizeInBytes)
}

func Test_SetsRegionNameCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	region := "us-east-1"
	metadata.SetRegionName(region)
	assert.Equal(t, &region, metadata.RegionName)
}

func Test_SetsAccountNameCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	accountName := "test-account"
	metadata.SetAccountName(accountName)
	assert.Equal(t, &accountName, metadata.AccountName)
}
