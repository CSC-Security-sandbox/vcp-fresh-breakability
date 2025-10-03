package gcs

import (
	"context"
	"fmt"
	"io"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops/sink"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
)

var (
	getBucketHandlerFunc  = getBucketHandler
	writeDataToBucketFunc = writeDataToBucket
	getWriterFunc         = getWriter
)

type GCPBizOpsSink struct {
}

func NewGCPBizOpsSink() sink.BizOpsSink {
	return &GCPBizOpsSink{}
}

func (s *GCPBizOpsSink) Type() string {
	return "gcs"
}

func (s *GCPBizOpsSink) Ingest(ctx context.Context, sinkParams *entity.BizopsSinkParams) error {
	err := sink.ValidateSinkParams(sinkParams)
	if err != nil {
		return err
	}
	gcsClient, err := NewGCSClient(ctx)
	if err != nil {
		return err
	}
	bucket, err := getBucketHandlerFunc(ctx, gcsClient)
	if err != nil {
		return err
	}
	filePath := sink.GetFilePath(sinkParams.Date, sinkParams.Timezone)
	wc := getWriterFunc(ctx, bucket, filePath)
	err = writeDataToBucketFunc(ctx, sinkParams.Reader, wc)
	if err != nil {
		return err
	}
	return wc.Close()
}

func getBucketHandler(ctx context.Context, client StorageClient) (BucketHandle, error) {
	// Get a handle for the bucket
	bucket := client.Bucket(sink.BucketName)
	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, err
	}
	return bucket, nil
}

func getWriter(ctx context.Context, bucket BucketHandle, fullPath string) io.WriteCloser {
	// Create a writer to the object
	writer := bucket.NewWriter(ctx, fullPath)
	writer.ContentType = sink.ContentType
	return writer
}

func writeDataToBucket(ctx context.Context, r io.Reader, w io.Writer) error {
	buf := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("i/o on file timed out")
		default:
		}

		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n > 0 {
			if _, err := w.Write(buf[:n]); err != nil {
				return err
			}
		}
	}
}
