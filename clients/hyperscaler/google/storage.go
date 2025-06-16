package google

import (
	"cloud.google.com/go/storage"
	"context"
)

type StorageClient interface {
	Bucket(name string) BucketHandle
}
type BucketHandle interface {
	Attrs(ctx context.Context) (*storage.BucketAttrs, error)
	Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) error
	Delete(ctx context.Context) error
}

type storageClient struct {
	client *storage.Client
}
type bucketHandle struct {
	handle *storage.BucketHandle
}

func (r *storageClient) Bucket(name string) BucketHandle {
	return &bucketHandle{r.client.Bucket(name)}
}

func (r *bucketHandle) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return r.handle.Attrs(ctx)
}

func (r *bucketHandle) Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) error {
	return r.handle.Create(ctx, projectID, attrs)
}

func (r *bucketHandle) Delete(ctx context.Context) error {
	return r.handle.Delete(ctx)
}
