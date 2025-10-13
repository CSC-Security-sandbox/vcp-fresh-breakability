package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	errs "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestHydrateReplicationCreate(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	replication := models.ReplicationHydrateObject{
		ResourceId:       "replication-name",
		ReplicationState: "CREATING",
	}
	region := "mocked-region"
	projectId := "mocked-project"
	volumeResourceID := "mocked-volume-id"
	token := "mocked-token"

	// Save and mock hydrateToCffe
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateReplicationCreate(ctx, mockLogger, replication, region, projectId, volumeResourceID, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateReplicationCreate(ctx, mockLogger, replication, region, projectId, volumeResourceID, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateVolumeCreate(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	volume := models.VolumeHydrateObject{
		ResourceId: "vol-1",
		VolumeId:   "uuid-1",
		PoolId:     "pool-1",
		Protocols:  []string{"NFS"},
		State:      "READY",
		QuotaInGib: 10,
	}
	location := "mocked-location"
	projectId := "mocked-project"
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateVolumeCreate(ctx, mockLogger, volume, location, projectId, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateVolumeCreate(ctx, mockLogger, volume, location, projectId, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateVolumeDelete(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	volumeResourceID := "vol-1"
	region := "mocked-region"
	projectId := "mocked-project"
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateVolumeDelete(ctx, mockLogger, volumeResourceID, region, projectId, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateVolumeDelete(ctx, mockLogger, volumeResourceID, region, projectId, token)
		assert.NoError(tt, err, nil)
	})
}

func TestBatchHydrateCreatedSnapshots(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	currVolumeName := "mock-volume"
	location := "mock-location"
	projectId := "mock-project"
	token := "mock-token"

	// Save and mock hydrateToCffe
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()

	t.Run("HandlesEmptyResourcesArray", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _batchHydrateCreatedSnapshots(ctx, mockLogger, []models.Request{}, currVolumeName, location, projectId, token)
		assert.NoError(tt, err)
	})

	t.Run("HandlesSingleResource", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		resources := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: "snap-1"}},
		}
		err := _batchHydrateCreatedSnapshots(ctx, mockLogger, resources, currVolumeName, location, projectId, token)
		assert.NoError(tt, err)
	})

	t.Run("HandlesMultipleResources", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		resources := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: "snap-1"}},
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-2", ResourceId: "snap-2"}},
		}
		err := _batchHydrateCreatedSnapshots(ctx, mockLogger, resources, currVolumeName, location, projectId, token)
		assert.NoError(tt, err)
	})

	t.Run("HandlesHydrateToCffeError", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return errors.New("mock error")
		}
		resources := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: "snap-1"}},
		}
		err := _batchHydrateCreatedSnapshots(ctx, mockLogger, resources, currVolumeName, location, projectId, token)
		assert.NoError(tt, err)
	})

	t.Run("HandlesBatchSizeLimit", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		resources := []models.Request{}
		for i := 0; i < batchSize+1; i++ {
			resources = append(resources, models.Request{Snapshot: &models.HydrateSnapshot{SnapshotId: fmt.Sprintf("uuid-%d", i), ResourceId: fmt.Sprintf("snap-%d", i)}})
		}
		err := _batchHydrateCreatedSnapshots(ctx, mockLogger, resources, currVolumeName, location, projectId, token)
		assert.NoError(tt, err)
	})
}

func TestBatchHydrateDeletedSnapshots(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	currVolumeName := "mock-volume"
	region := "mock-region"
	projectId := "mock-project"
	token := "mock-token"

	// Save and mock hydrateToCffe
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()

	t.Run("HandlesEmptyHydrateSnapshot", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _batchHydrateDeletedSnapshots(ctx, mockLogger, []models.Request{}, currVolumeName, region, projectId, token)
		assert.Nil(tt, err)
	})

	t.Run("HandlesEmptyNamesError", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		hydrateSnapshot := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: ""}},
		}
		err := _batchHydrateDeletedSnapshots(ctx, mockLogger, hydrateSnapshot, currVolumeName, region, projectId, token)
		assert.Nil(tt, err)
	})

	t.Run("HandlesSingleHydrateSnapshot", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		hydrateSnapshot := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: "snap-1"}},
		}
		err := _batchHydrateDeletedSnapshots(ctx, mockLogger, hydrateSnapshot, currVolumeName, region, projectId, token)
		assert.NoError(tt, err)
	})

	t.Run("HandlesMultipleHydrateSnapshots", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		hydrateSnapshot := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: "snap-1"}},
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-2", ResourceId: "snap-2"}},
		}
		err := _batchHydrateDeletedSnapshots(ctx, mockLogger, hydrateSnapshot, currVolumeName, region, projectId, token)
		assert.NoError(tt, err)
	})

	t.Run("HandlesHydrateToCffeError", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return errors.New("mock error")
		}
		hydrateSnapshot := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: "snap-1"}},
		}
		err := _batchHydrateDeletedSnapshots(ctx, mockLogger, hydrateSnapshot, currVolumeName, region, projectId, token)
		assert.Error(tt, err)
		assert.Equal(tt, "mock error", err.Error())
	})

	t.Run("HandlesBatchSizeLimit", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		hydrateSnapshot := []models.Request{}
		for i := 0; i < batchSize+1; i++ {
			hydrateSnapshot = append(hydrateSnapshot, models.Request{Snapshot: &models.HydrateSnapshot{SnapshotId: fmt.Sprintf("uuid-%d", i), ResourceId: fmt.Sprintf("snap-%d", i)}})
		}
		err := _batchHydrateDeletedSnapshots(ctx, mockLogger, hydrateSnapshot, currVolumeName, region, projectId, token)
		assert.NoError(tt, err)
	})
}

func TestHydrateCreatedScheduledBackups(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewMockLogger(t)
	projectId := "mocked-project"
	location := "mocked-location"
	backupVaultName := "mocked-backup-vault"
	token := "mocked-token"

	resources := []models.Request{
		{
			Backup: &models.HydrateBackup{
				ResourceId:       "mock-backup",
				BackupId:         "mock-uuid",
				VolumeUsageBytes: nil,
			},
		},
	}

	originalHydrateToCcfe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCcfe }()
	t.Run("WhenHydrateToCcfeSucceeds", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		mockLogger.On("Infof", mock.Anything, mock.Anything).Return(nil)

		err := HydrateCreatedScheduledBackups(ctx, mockLogger, resources, backupVaultName, location, projectId, token)
		assert.NoError(tt, err)
	})

	t.Run("WhenHydrateToCcfeErrors", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return errors.New("could not hydrate backups to ccfe")
		}
		mockLogger.On("Errorf", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		err := HydrateCreatedScheduledBackups(ctx, mockLogger, resources, backupVaultName, location, projectId, token)
		assert.Error(tt, err)
		assert.Equal(tt, "could not hydrate backups to ccfe", err.Error())
	})
}

func TestHydrateDeletedScheduledBackups(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewMockLogger(t)
	projectId := "mocked-project"
	location := "mocked-location"
	backupVaultName := "mocked-backup-vault"
	token := "mocked-token"
	names := []string{"mock-backup-1", "mock-backup-2"}

	originalHydrateToCcfe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCcfe }()
	t.Run("WhenHydrateToCcfeSucceeds", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		mockLogger.On("Infof", mock.Anything, mock.Anything).Return(nil)

		err := HydrateDeletedScheduledBackups(ctx, mockLogger, names, backupVaultName, location, projectId, token)
		assert.NoError(tt, err)
	})

	t.Run("WhenHydrateToCcfeErrors", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return errors.New("could not hydrate backups to ccfe")
		}
		mockLogger.On("Errorf", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		err := HydrateDeletedScheduledBackups(ctx, mockLogger, names, backupVaultName, location, projectId, token)
		assert.Error(tt, err)
		assert.Equal(tt, "could not hydrate backups to ccfe", err.Error())
	})
}

func TestGetAllUUIDs(t *testing.T) {
	t.Run("ReturnsAllUUIDsAndSnapshotType", func(tt *testing.T) {
		requestArr := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: "snap-1"}},
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-2", ResourceId: "snap-2"}},
		}
		expectedUUIDs := ", uuid-1, uuid-2"
		expectedType := "snapshot"
		allUuids, resourceType := getAllUUIDs(requestArr)
		assert.Equal(tt, expectedUUIDs, allUuids)
		assert.Equal(tt, expectedType, resourceType)
	})

	t.Run("HandlesEmptyResourceId", func(tt *testing.T) {
		requestArr := []models.Request{
			{Snapshot: &models.HydrateSnapshot{SnapshotId: "uuid-1", ResourceId: ""}},
		}
		expectedUUIDs := ""
		expectedType := ""
		allUuids, resourceType := getAllUUIDs(requestArr)
		assert.Equal(tt, expectedUUIDs, allUuids)
		assert.Equal(tt, expectedType, resourceType)
	})

	t.Run("HandlesNilSnapshot", func(tt *testing.T) {
		requestArr := []models.Request{
			{Snapshot: nil},
		}
		expectedUUIDs := ""
		expectedType := ""
		allUuids, resourceType := getAllUUIDs(requestArr)
		assert.Equal(tt, expectedUUIDs, allUuids)
		assert.Equal(tt, expectedType, resourceType)
	})

	t.Run("HandlesEmptyRequestArray", func(tt *testing.T) {
		requestArr := []models.Request{}
		expectedUUIDs := ""
		expectedType := ""
		allUuids, resourceType := getAllUUIDs(requestArr)
		assert.Equal(tt, expectedUUIDs, allUuids)
		assert.Equal(tt, expectedType, resourceType)
	})
}

func TestConvertDeleteResource(tt *testing.T) {
	tt.Run("HandlesValidSnapshot", func(t *testing.T) {
		requestArr := []models.Request{
			{Snapshot: &models.HydrateSnapshot{ResourceId: "snap-1"}},
		}
		expected := models.GcpHydrateDelete{Names: []string{"snapshots/snap-1"}}
		result := convertDeleteResource(requestArr)
		assert.Equal(t, expected, result)
	})

	tt.Run("HandlesEmptyResourceId", func(t *testing.T) {
		requestArr := []models.Request{
			{Snapshot: &models.HydrateSnapshot{ResourceId: ""}},
		}
		expected := models.GcpHydrateDelete{}
		result := convertDeleteResource(requestArr)
		assert.Equal(t, expected, result)
	})

	tt.Run("HandlesNilSnapshot", func(t *testing.T) {
		requestArr := []models.Request{
			{Snapshot: nil},
		}
		expected := models.GcpHydrateDelete{}
		result := convertDeleteResource(requestArr)
		assert.Equal(t, expected, result)
	})

	tt.Run("HandlesEmptyRequestArray", func(t *testing.T) {
		requestArr := []models.Request{}
		expected := models.GcpHydrateDelete{}
		result := convertDeleteResource(requestArr)
		assert.Equal(t, expected, result)
	})
}

func TestHydrateReplicationDelete(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	replicationResourceId := "replication-1"
	volumeResourceID := "volume-1"
	region := "mocked-region"
	projectId := "mocked-project"
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateReplicationDelete(ctx, mockLogger, replicationResourceId, volumeResourceID, region, projectId, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateReplicationDelete(ctx, mockLogger, replicationResourceId, volumeResourceID, region, projectId, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateReplicationStateFunc(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	region := "mocked-region"
	projectId := "mocked-project"
	volumeResourceID := "mocked-volume-id"
	replicationId := "mocked-replication-id"
	state := models.VolumeReplicationHydrateState("READY")
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateReplicationState(ctx, mockLogger, region, projectId, volumeResourceID, replicationId, state, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateReplicationState(ctx, mockLogger, region, projectId, volumeResourceID, replicationId, state, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateReplicationStateAndTypeFunc(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	region := "mocked-region"
	projectId := "mocked-project"
	volumeResourceID := "mocked-volume-id"
	replicationId := "mocked-replication-id"
	state := models.VolumeReplicationHydrateState("READY")
	hybridReplicationType := models.HybridReplicationHydrateType("cres")
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateReplicationStateAndType(ctx, mockLogger, region, projectId, volumeResourceID, replicationId, state, hybridReplicationType, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateReplicationStateAndType(ctx, mockLogger, region, projectId, volumeResourceID, replicationId, state, hybridReplicationType, token)
		assert.NoError(tt, err, nil)
	})
}

func Test_doHydrateToCffe(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	testToken := "test-token"
	testURL := "http://example.com"
	testMethod := "POST"
	testBody := map[string]string{"foo": "bar"}

	// Save and mock dependencies
	originalJsonMarshal := jsonMarshal
	originalHttpNewRequest := httpNewRequest
	originalHttpClientDo := httpClientDo
	originalIoReadAll := ioReadAll
	originalJsonUnmarshal := jsonUnmarshal
	defer func() {
		jsonMarshal = originalJsonMarshal
		httpNewRequest = originalHttpNewRequest
		httpClientDo = originalHttpClientDo
		ioReadAll = originalIoReadAll
		jsonUnmarshal = originalJsonUnmarshal
	}()
	t.Run("WhenJsonMarshalFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return nil, errors.New("marshal error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("marshal error"))
	})

	t.Run("WhenHttpNewRequestFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			return nil, errors.New("request error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("request error"))
	})

	t.Run("WhenHttpClientDoFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			req, _ := http.NewRequest(method, url, body)
			return req, nil
		}
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("client do error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("client do error"))
	})

	t.Run("WhenResponseBodyReadFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			req, _ := http.NewRequest(method, url, body)
			return req, nil
		}
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(bytes.NewReader([]byte("body"))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return nil, errors.New("read error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("read error"))
	})

	t.Run("WhenJsonUnmarshalFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			req, _ := http.NewRequest(method, url, body)
			return req, nil
		}
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				Body: io.NopCloser(bytes.NewReader([]byte("body"))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte("body"), nil
		}
		jsonUnmarshal = func(data []byte, v any) error {
			return errors.New("unmarshal error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("unmarshal error"))
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			req, _ := http.NewRequest(method, url, body)
			return req, nil
		}
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
				StatusCode: 200,
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte("{}"), nil
		}
		jsonUnmarshal = func(data []byte, v any) error {
			return nil
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateToCffe(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	testToken := "test-token"
	testURL := "http://example.com"
	testMethod := "POST"
	testBody := map[string]string{"foo": "bar"}

	// Save and mock dependencies
	originalDoHydrateToCffe := doHydrateToCffe
	defer func() { doHydrateToCffe = originalDoHydrateToCffe }()
	t.Run("RetriesOn429WithQuotaLimit", func(tt *testing.T) {
		retryCount := 0
		doHydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			retryCount++
			httpCode := 429
			return &errs.CustomError{
				OriginalErr: errors.New("Quota limit exceeded"),
				HttpCode:    &httpCode,
				Message:     "Quota limit exceeded",
			}
		}
		_ = _hydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.GreaterOrEqual(tt, retryCount, 1)
	})
	t.Run("Getting400Error", func(tt *testing.T) {
		retryCount := 0
		doHydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			retryCount++
			httpCode := 400
			return &errs.CustomError{
				OriginalErr: errors.New("some error"),
				HttpCode:    &httpCode,
			}
		}
		err := _hydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.GreaterOrEqual(tt, retryCount, 1)
		assert.Equal(tt, "some error", err.(*errs.CustomError).OriginalErr.Error())
	})

	t.Run("WhenDoHydrateToCffeReturnsNil", func(tt *testing.T) {
		doHydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.NoError(tt, err)
	})
}

func Test_getQuotaLimit(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	region := "mock-region"
	projectId := "mock-project"
	token := "mock-token"
	resourceType := ResourceTypeVolume
	originalHydrateToCffe := getQuotaLimitsForResource
	defer func() { getQuotaLimitsForResource = originalHydrateToCffe }()

	t.Run("WhenHydrateToCffeReturnsError", func(tt *testing.T) {
		expectedErr := errors.New("some error")

		getQuotaLimitsForResource = func(ctx context.Context, projectId string, region string, quotaType QuotaType, token string, logger log.Logger) (int, error) {
			return 0, errors.New("some error")
		}
		_, err := _getQuotaLimit(ctx, mockLogger, region, projectId, token, resourceType)
		assert.Equal(tt, expectedErr, err)
	})

	t.Run("WhenHydrateToCffeReturnsNil", func(tt *testing.T) {
		getQuotaLimitsForResource = func(ctx context.Context, projectId string, region string, quotaType QuotaType, token string, logger log.Logger) (int, error) {
			return 0, nil
		}
		_, err := _getQuotaLimit(ctx, mockLogger, region, projectId, token, resourceType)

		assert.Equal(tt, err, nil)
	})
}

func TestGetQuotaLimitsForResource(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	projectId := "mock-project"
	region := "mock-region"
	quotaType := FlexVolumesPerRegion
	token := "mock-token"

	originalHttpNewRequest := httpNewRequest
	originalHttpClientDo := httpClientDo
	originalIoReadAll := ioReadAll
	originalJsonUnmarshal := jsonUnmarshal
	originalStringConvAtoi := stringConvAtoi
	defer func() {
		httpNewRequest = originalHttpNewRequest
		httpClientDo = originalHttpClientDo
		ioReadAll = originalIoReadAll
		jsonUnmarshal = originalJsonUnmarshal
		stringConvAtoi = originalStringConvAtoi
	}()

	t.Run("WhenHttpNewRequestFails", func(tt *testing.T) {
		expectedErr := errors.New("request error")
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			return nil, errors.New("request error")
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenHttpClientDoFails", func(tt *testing.T) {
		expectedErr := errors.New("client do error")
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return nil, expectedErr
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenIoReadAllFails", func(tt *testing.T) {
		expectedErr := errors.New("client do error")
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return nil, expectedErr
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenJsonUnmarshalFailsOnSuccess", func(tt *testing.T) {
		expectedErr := errors.New("unmarshal error")
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte("{}"), nil
		}
		jsonUnmarshal = func(data []byte, v any) error {
			return errors.New("unmarshal error")
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenStringConvAtoiFails", func(tt *testing.T) {
		expectedErr := errors.New("atoi error")
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"type":"quota","value":"notanint"}`))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte(`{"type":"quota","value":"notanint"}`), nil
		}
		jsonUnmarshal = originalJsonUnmarshal
		stringConvAtoi = func(s string) (int, error) {
			return 0, errors.New("atoi error")
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"type":"quota","value":"42"}`))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte(`{"type":"quota","value":"42"}`), nil
		}
		jsonUnmarshal = originalJsonUnmarshal
		stringConvAtoi = strconv.Atoi
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 42, quota)
		assert.NoError(tt, err, nil)
	})

	t.Run("WhenStatusCodeNot200AndJsonUnmarshalFails", func(tt *testing.T) {
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte(`{}`), nil
		}
		jsonUnmarshal = func(data []byte, v any) error {
			return errors.New("unmarshal error")
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
	})

	t.Run("WhenStatusCodeNot200AndSuccess", func(tt *testing.T) {
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"code":400,"message":"quota error","status":"FAILED"}`))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte(`{"code":400,"message":"quota error","status":"FAILED"}`), nil
		}
		jsonUnmarshal = originalJsonUnmarshal
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, "quota error", err.(*errs.CustomError).GetMessage())
	})
}

func TestMapStateToGcpState(t *testing.T) {
	t.Run("ReturnsDeletedState", func(tt *testing.T) {
		state := models.LifeCycleStateDeleted
		expectedState := deletedGcp
		result := _mapStateToGcpState(state)
		assert.Equal(tt, expectedState, result)
	})

	t.Run("ReturnsAvailableState", func(tt *testing.T) {
		state := models.LifeCycleStateAvailable
		expectedState := models.LifeCycleStateREADY
		result := _mapStateToGcpState(state)
		assert.Equal(tt, expectedState, result)
	})

	t.Run("ReturnsDefaultStateForEmptyInput", func(tt *testing.T) {
		state := ""
		expectedState := defaultGcp
		result := _mapStateToGcpState(state)
		assert.Equal(tt, expectedState, result)
	})

	t.Run("ReturnsInputStateForUnknownState", func(tt *testing.T) {
		state := "unknownState"
		expectedState := "unknownState"
		result := _mapStateToGcpState(state)
		assert.Equal(tt, expectedState, result)
	})
}

func TestMapToGcpBulkSnapshotDeleteTests(tt *testing.T) {
	tt.Run("HandlesEmptyResourceId", func(tt *testing.T) {
		reqArray := []models.Request{
			{Snapshot: &models.HydrateSnapshot{ResourceId: ""}},
		}
		expected := models.GcpHydrateDelete{Names: []string{}}
		result := mapToGcpBulkSnapshotDelete(reqArray)
		assert.Equal(tt, expected, result)
	})

	tt.Run("HandlesMultipleSnapshots", func(tt *testing.T) {
		reqArray := []models.Request{
			{Snapshot: &models.HydrateSnapshot{ResourceId: "snap-1"}},
			{Snapshot: &models.HydrateSnapshot{ResourceId: "snap-2"}},
		}
		expected := models.GcpHydrateDelete{Names: []string{"snapshots/snap-1", "snapshots/snap-2"}}
		result := mapToGcpBulkSnapshotDelete(reqArray)
		assert.Equal(tt, expected, result)
	})

	tt.Run("HandlesNilSnapshot", func(tt *testing.T) {
		reqArray := []models.Request{
			{Snapshot: nil},
		}
		expected := models.GcpHydrateDelete{Names: []string{}}
		result := mapToGcpBulkSnapshotDelete(reqArray)
		assert.Equal(tt, expected, result)
	})

	tt.Run("HandlesEmptyRequestArray", func(tt *testing.T) {
		reqArray := []models.Request{}
		expected := models.GcpHydrateDelete{Names: []string{}}
		result := mapToGcpBulkSnapshotDelete(reqArray)
		assert.Equal(tt, expected, result)
	})
}

func TestHydrateUpdatedPool(t *testing.T) {
	ctx := context.Background()
	mockToken := "mock-token"

	// Save original hydrateToCffe function and restore after tests
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()

	// Save original baseUri and restore after tests
	originalBaseUri := baseUri
	defer func() { baseUri = originalBaseUri }()
	baseUri = "https://mock-base-uri.com"

	t.Run("Success_StateOnly", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "AVAILABLE",
			Region:         "us-central1-a",
			HotTierSizeGib: 0, // Zero to test state-only update
		}

		var capturedPayload models.PoolUpdateCCFERequest
		var capturedURL string
		var capturedMethod string

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedPayload = v.(models.PoolUpdateCCFERequest)
			capturedURL = url
			capturedMethod = method
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.NoError(tt, err)
		assert.Equal(tt, "AVAILABLE", capturedPayload.State)
		assert.Equal(tt, nil, capturedPayload.HotTierSizeGib) // Should be nil for state-only update
		assert.Equal(tt, "PATCH", capturedMethod)
		expectedURL := "https://mock-base-uri.com/v1internal/projects/test-project/locations/us-central1-a/storagePools/test-pool?update_mask=state"
		assert.Equal(tt, expectedURL, capturedURL)
	})

	t.Run("Success_StateAndHotTierSize", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "READY",
			Region:         "us-central1-b",
			HotTierSizeGib: 100, // Non-zero to test both state and hot tier update
		}

		var capturedPayload models.PoolUpdateCCFERequest
		var capturedURL string

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedPayload = v.(models.PoolUpdateCCFERequest)
			capturedURL = url
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.NoError(tt, err)
		assert.Equal(tt, "READY", capturedPayload.State)
		assert.Equal(tt, int64(100), capturedPayload.HotTierSizeGib)
		expectedURL := "https://mock-base-uri.com/v1internal/projects/test-project/locations/us-central1-b/storagePools/test-pool?update_mask=state,hot_tier_size_gib"
		assert.Equal(tt, expectedURL, capturedURL)
	})

	t.Run("Success_LargeHotTierSize", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "UPDATING",
			Region:         "europe-west1-a",
			HotTierSizeGib: 9999, // Large value
		}

		var capturedPayload models.PoolUpdateCCFERequest

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedPayload = v.(models.PoolUpdateCCFERequest)
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.NoError(tt, err)
		assert.Equal(tt, "UPDATING", capturedPayload.State)
		assert.Equal(tt, int64(9999), capturedPayload.HotTierSizeGib)
	})

	t.Run("Success_SpecialCharactersInNames", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project-123",
			PoolID:         "test-pool-id-456",
			Name:           "test-pool-with-dashes",
			State:          "CREATING",
			Region:         "asia-east1-c",
			HotTierSizeGib: 50,
		}

		var capturedURL string

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedURL = url
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.NoError(tt, err)
		expectedURL := "https://mock-base-uri.com/v1internal/projects/test-project-123/locations/asia-east1-c/storagePools/test-pool-with-dashes?update_mask=state,hot_tier_size_gib"
		assert.Equal(tt, expectedURL, capturedURL)
	})

	t.Run("Error_HydrateToCffeError", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "AVAILABLE",
			Region:         "us-central1-a",
			HotTierSizeGib: 100,
		}

		expectedError := &errs.CustomError{
			Message:     "Failed to hydrate to CCFE",
			OriginalErr: errors.New("CCFE service unavailable"),
		}

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedError
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("Error_NetworkError", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "AVAILABLE",
			Region:         "us-central1-a",
			HotTierSizeGib: 0,
		}

		expectedError := errors.New("network timeout")

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedError
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("Error_AuthenticationError", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "AVAILABLE",
			Region:         "us-central1-a",
			HotTierSizeGib: 25,
		}

		httpCode := 401
		expectedError := &errs.CustomError{
			Message:  "Unauthorized",
			HttpCode: &httpCode,
		}

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedError
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("EdgeCase_EmptyToken", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "AVAILABLE",
			Region:         "us-central1-a",
			HotTierSizeGib: 0,
		}

		var capturedToken string

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedToken = token
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, "")

		assert.NoError(tt, err)
		assert.Equal(tt, "", capturedToken)
	})

	t.Run("EdgeCase_EmptyPoolFields", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "",
			PoolID:         "",
			Name:           "",
			State:          "",
			Region:         "",
			HotTierSizeGib: 0,
		}

		var capturedURL string
		var capturedPayload models.PoolUpdateCCFERequest

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedURL = url
			capturedPayload = v.(models.PoolUpdateCCFERequest)
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.NoError(tt, err)
		assert.Equal(tt, "", capturedPayload.State)
		assert.Equal(tt, nil, capturedPayload.HotTierSizeGib)
		expectedURL := "https://mock-base-uri.com/v1internal/projects//locations//storagePools/?update_mask=state"
		assert.Equal(tt, expectedURL, capturedURL)
	})

	t.Run("EdgeCase_NegativeHotTierSize", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "AVAILABLE",
			Region:         "us-central1-a",
			HotTierSizeGib: -10, // Negative value
		}

		var capturedPayload models.PoolUpdateCCFERequest
		var capturedURL string

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedPayload = v.(models.PoolUpdateCCFERequest)
			capturedURL = url
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.NoError(tt, err)
		assert.Equal(tt, "AVAILABLE", capturedPayload.State)
		assert.Equal(tt, nil, capturedPayload.HotTierSizeGib) // Should be nil since -10 is not > 0
		expectedURL := "https://mock-base-uri.com/v1internal/projects/test-project/locations/us-central1-a/storagePools/test-pool?update_mask=state"
		assert.Equal(tt, expectedURL, capturedURL)
	})

	t.Run("EdgeCase_HotTierSizeEqualsOne", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "test-project",
			PoolID:         "test-pool-id",
			Name:           "test-pool",
			State:          "AVAILABLE",
			Region:         "us-central1-a",
			HotTierSizeGib: 1, // Minimum positive value
		}

		var capturedPayload models.PoolUpdateCCFERequest
		var capturedURL string

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedPayload = v.(models.PoolUpdateCCFERequest)
			capturedURL = url
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, mockToken)

		assert.NoError(tt, err)
		assert.Equal(tt, "AVAILABLE", capturedPayload.State)
		assert.Equal(tt, int64(1), capturedPayload.HotTierSizeGib)
		expectedURL := "https://mock-base-uri.com/v1internal/projects/test-project/locations/us-central1-a/storagePools/test-pool?update_mask=state,hot_tier_size_gib"
		assert.Equal(tt, expectedURL, capturedURL)
	})

	t.Run("ParameterValidation_CorrectParametersPassedToHydrateToCffe", func(tt *testing.T) {
		poolHydrateObj := models.PoolHydrateObject{
			OwnerID:        "validation-project",
			PoolID:         "validation-pool-id",
			Name:           "validation-pool",
			State:          "VALIDATING",
			Region:         "us-west1-a",
			HotTierSizeGib: 200,
		}

		var capturedCtx context.Context
		var capturedLogger log.Logger
		var capturedPayload models.PoolUpdateCCFERequest
		var capturedURL string
		var capturedMethod string
		var capturedToken string

		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			capturedCtx = ctx
			capturedLogger = logger
			capturedPayload = v.(models.PoolUpdateCCFERequest)
			capturedURL = url
			capturedMethod = method
			capturedToken = token
			return nil
		}

		err := _hydrateUpdatedPool(ctx, poolHydrateObj, "validation-token")

		assert.NoError(tt, err)
		assert.Equal(tt, ctx, capturedCtx)
		assert.NotNil(tt, capturedLogger)
		assert.Equal(tt, "VALIDATING", capturedPayload.State)
		assert.Equal(tt, int64(200), capturedPayload.HotTierSizeGib)
		assert.Equal(tt, "PATCH", capturedMethod)
		assert.Equal(tt, "validation-token", capturedToken)
		expectedURL := "https://mock-base-uri.com/v1internal/projects/validation-project/locations/us-west1-a/storagePools/validation-pool?update_mask=state,hot_tier_size_gib"
		assert.Equal(tt, expectedURL, capturedURL)
	})
}
