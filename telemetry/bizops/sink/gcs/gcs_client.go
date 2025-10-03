package gcs

import (
	"context"

	"cloud.google.com/go/storage"
)

var (
	newStorageClient = storage.NewClient
)

type BucketHandle interface {
	Attrs(ctx context.Context) (*storage.BucketAttrs, error)
	NewWriter(ctx context.Context, object string) *storage.Writer
}

type StorageClient interface {
	Bucket(name string) BucketHandle
}

type gcsClient struct {
	client *storage.Client
}

func NewGCSClient(ctx context.Context) (StorageClient, error) {
	client, err := newStorageClient(ctx)
	if err != nil {
		return nil, err
	}
	return &gcsClient{client: client}, nil
}

func (g *gcsClient) Bucket(name string) BucketHandle {
	return &gcsBucketHandle{bucket: g.client.Bucket(name)}
}

type gcsBucketHandle struct {
	bucket *storage.BucketHandle
}

func (b *gcsBucketHandle) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return b.bucket.Attrs(ctx)
}

func (b *gcsBucketHandle) NewWriter(ctx context.Context, object string) *storage.Writer {
	return b.bucket.Object(object).NewWriter(ctx)
}
