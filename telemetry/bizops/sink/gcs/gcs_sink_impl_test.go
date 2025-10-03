package gcs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops/sink"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"google.golang.org/api/option"
)

type mockWriter struct{}

type mockErrorReader struct{}

func (w *mockWriter) Read(b []byte) (int, error) {
	return 0, nil
}

func (w *mockWriter) Write(b []byte) (int, error) {
	return 0, nil
}

func (w *mockWriter) Close() error {
	return nil
}

func (w *mockErrorReader) Read(b []byte) (int, error) {
	return 0, errors.New("simulated read error")
}

func Test_NewGCPBizOpsSink(t *testing.T) {
	gcpSink := NewGCPBizOpsSink()
	if gcpSink == nil {
		t.Error("Expected non-nil gcp sink")
	}
	if _, ok := gcpSink.(*GCPBizOpsSink); !ok {
		t.Error("Expected type *GCPBizOpsSink")
	}
}

func Test_GCPIngest(t *testing.T) {
	gcpSink := NewGCPBizOpsSink()
	testParams := &entity.BizopsSinkParams{
		Reader:   bytes.NewReader([]byte("data")),
		Date:     time.Now(),
		Timezone: "UTC",
	}
	ctx := context.Background()
	t.Run("Success", func(t *testing.T) {
		oldStorageClient := newStorageClient
		oldBucketHandler := getBucketHandlerFunc
		oldWriteDataToBucket := writeDataToBucketFunc
		oldGetWriterFunc := getWriterFunc
		defer func() {
			getBucketHandlerFunc = oldBucketHandler
			writeDataToBucketFunc = oldWriteDataToBucket
			getWriterFunc = oldGetWriterFunc
			newStorageClient = oldStorageClient
		}()
		newStorageClient = func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
			return &storage.Client{}, nil
		}
		getWriterFunc = func(ctx context.Context, bucket BucketHandle, fullPath string) io.WriteCloser {
			return &mockWriter{}
		}
		writeDataToBucketFunc = func(ctx context.Context, reader io.Reader, writer io.Writer) error {
			return nil
		}
		var bucketWriter *storage.Writer
		var fullPath = "test-path"
		getBucketHandlerFunc = func(ctx context.Context, client StorageClient) (BucketHandle, error) {
			mockBucketHandler := &MockBucketHandle{}
			bucketWriter = &storage.Writer{}
			mockBucketHandler.On("NewWriter", ctx, fullPath).Return(bucketWriter)
			writer := mockBucketHandler.NewWriter(ctx, fullPath)
			writer.ContentType = sink.ContentType
			return mockBucketHandler, nil
		}
		err := gcpSink.Ingest(ctx, testParams)
		assert.Nil(t, err)
		assert.NotNil(t, bucketWriter)
		assert.Equal(t, sink.ContentType, bucketWriter.ContentType)
	})
	t.Run("Invalid Sink Parameters", func(t *testing.T) {
		err := gcpSink.Ingest(ctx, nil)
		assert.NotNil(t, err)
		assert.Equal(t, "sink params cannot be nil", err.Error())
	})
	t.Run("Bucket Handler Error", func(t *testing.T) {
		oldBucketHandler := getBucketHandlerFunc
		oldStorageClient := newStorageClient
		defer func() {
			getBucketHandlerFunc = oldBucketHandler
			newStorageClient = oldStorageClient
		}()
		newStorageClient = func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
			return &storage.Client{}, nil
		}
		getBucketHandlerFunc = func(ctx context.Context, client StorageClient) (BucketHandle, error) {
			return nil, errors.New("mock bucket handler error")
		}
		err := gcpSink.Ingest(ctx, testParams)
		assert.NotNil(t, err)
		assert.Equal(t, "mock bucket handler error", err.Error())
	})
	t.Run("Write Data to Bucket Error", func(t *testing.T) {
		oldStorageClient := newStorageClient
		oldBucketHandler := getBucketHandlerFunc
		oldWriteDataToBucket := writeDataToBucketFunc
		ctx := context.Background()
		defer func() {
			newStorageClient = oldStorageClient
			getBucketHandlerFunc = oldBucketHandler
			writeDataToBucketFunc = oldWriteDataToBucket
		}()
		newStorageClient = func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
			return &storage.Client{}, nil
		}
		writeDataToBucketFunc = func(ctx context.Context, reader io.Reader, writer io.Writer) error {
			return errors.New("mock write data error")
		}
		getBucketHandlerFunc = func(ctx context.Context, client StorageClient) (BucketHandle, error) {
			mockBucketHandler := &MockBucketHandle{}
			bucketWriter := &storage.Writer{}
			mockBucketHandler.On("NewWriter", ctx,
				sink.GetFilePath(testParams.Date, testParams.Timezone)).Return(bucketWriter)
			return mockBucketHandler, nil
		}
		err := gcpSink.Ingest(ctx, testParams)
		assert.NotNil(t, err)
		assert.Equal(t, "mock write data error", err.Error())
	})
}

func Test_writeDataToBucket_Success(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("hello world")
		reader := bytes.NewReader(data)
		var buf bytes.Buffer
		err := writeDataToBucket(ctx, reader, &buf)
		assert.Nil(t, err)
		assert.Equal(t, data, buf.Bytes())
	})
	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reader := bytes.NewReader([]byte("data"))
		var buf bytes.Buffer
		err := writeDataToBucket(ctx, reader, &buf)
		assert.Error(t, err)
		assert.Equal(t, "i/o on file timed out", err.Error())
	})
	t.Run("Reader Error", func(t *testing.T) {
		ctx := context.Background()
		reader := &mockErrorReader{}
		var buf bytes.Buffer
		err := writeDataToBucket(ctx, reader, &buf)
		assert.Error(t, err)
		assert.Equal(t, "simulated read error", err.Error())
	})
}
