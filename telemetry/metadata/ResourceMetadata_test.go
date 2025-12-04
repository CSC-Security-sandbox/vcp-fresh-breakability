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

func Test_SetsDeploymentNameCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	deploymentName := "test-deployment"
	metadata.SetDeploymentName(deploymentName)
	assert.Equal(t, &deploymentName, metadata.DeploymentName)
}

func Test_ResourceMetadata_SetThroughput(t *testing.T) {
	metadata := &ResourceMetadata{}

	// Test setting throughput
	throughput := 250.5
	metadata.SetThroughput(throughput)

	assert.NotNil(t, metadata.Throughput)
	assert.Equal(t, throughput, *metadata.Throughput)
}

func Test_ResourceMetadata_SetResourceID(t *testing.T) {
	metadata := &ResourceMetadata{}

	// Test setting resource ID
	resourceID := int64(12345)
	metadata.SetResourceID(resourceID)

	assert.NotNil(t, metadata.ResourceID)
	assert.Equal(t, resourceID, *metadata.ResourceID)
}

func Test_ResourceMetadata_SetThroughputAndResourceID_Integration(t *testing.T) {
	metadata := &ResourceMetadata{}

	// Test setting both throughput and resource ID
	throughput := 100.25
	resourceID := int64(98765)

	metadata.SetThroughput(throughput)
	metadata.SetResourceID(resourceID)

	// Verify both values are set correctly
	assert.NotNil(t, metadata.Throughput)
	assert.NotNil(t, metadata.ResourceID)
	assert.Equal(t, throughput, *metadata.Throughput)
	assert.Equal(t, resourceID, *metadata.ResourceID)
}

func Test_SetsResourceDisplayNameCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	displayName := "test-display-name"
	metadata.SetResourceDisplayName(displayName)
	assert.Equal(t, &displayName, metadata.ResourceDisplayName)
}

func Test_SetsServiceLevelCorrectly(t *testing.T) {
	metadata := ResourceMetadata{}
	serviceLevel := "premium"
	metadata.SetServiceLevel(serviceLevel)
	assert.Equal(t, &serviceLevel, metadata.ServiceLevel)
}
