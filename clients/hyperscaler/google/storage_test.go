package google

import (
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Test storageClient.Bucket returns a BucketHandle
func Test_storageClient_Bucket(t *testing.T) {
	client := &storageClient{client: &storage.Client{}}
	bucket := client.Bucket("test-bucket")
	assert.NotNil(t, bucket)
}
