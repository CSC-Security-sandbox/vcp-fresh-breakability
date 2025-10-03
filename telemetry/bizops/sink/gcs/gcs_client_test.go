package gcs

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func Test_NewGCSClient(t *testing.T) {
	oldStorageClient := newStorageClient
	defer func() { newStorageClient = oldStorageClient }()
	newStorageClient = func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
		return &storage.Client{}, nil
	}
	t.Run("Success", func(t *testing.T) {
		_, err := NewGCSClient(nil)
		if err != nil {
			t.Errorf("Failed to create GCS client: %v", err)
		}
	})
	t.Run("Fail to create GCS Client", func(t *testing.T) {
		newStorageClient = func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
			return nil, fmt.Errorf("fail to create GCS client")
		}
		_, err := NewGCSClient(nil)
		if err == nil {
			t.Error("Expected error but got nil")
		}
	})
}

func TestGcsClient_Bucket(t *testing.T) {
	client := &gcsClient{client: &storage.Client{}}
	bucketName := "test-bucket"
	bucketHandle := client.Bucket(bucketName)
	if bucketHandle == nil {
		t.Error("Expected non-nil BucketHandle")
	}
}

func TestBucketHandle_Attrs(t *testing.T) {
	ctx := context.Background()
	mockBucket := new(MockBucketHandle)

	expectedAttrs := &storage.BucketAttrs{Name: "test-bucket"}
	mockBucket.On("Attrs", ctx).Return(expectedAttrs, nil)

	attrs, err := mockBucket.Attrs(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "test-bucket", attrs.Name)
}

func TestBucketHandle_NewWriter(t *testing.T) {
	ctx := context.Background()
	mockBucket := new(MockBucketHandle)

	mockWriter := &storage.Writer{} // You can mock behavior if needed
	mockBucket.On("NewWriter", ctx, "test-object").Return(mockWriter)

	writer := mockBucket.NewWriter(ctx, "test-object")
	assert.NotNil(t, writer)
}
