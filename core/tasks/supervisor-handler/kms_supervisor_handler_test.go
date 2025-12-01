package supervisorhandler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/sde"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestCmekHandlerHandleSkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleSkipsMissingResource(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return((*datamodel.KmsConfig)(nil), vsaerrors.NewNotFoundErr("kms", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleSuccess(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalRegion := env.Region
	env.Region = "us-west1"
	defer func() { env.Region = originalRegion }()

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
	}

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		CorrelationID: utils.RandomUUID(),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().DeleteKmsConfig(mock.Anything, "kms-uuid", models.LifeCycleStateError, WorkflowTimeoutDetail).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestCmekHandlerHandleSkipsWhenRegionMissing(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalRegion := env.Region
	env.Region = "   "
	defer func() { env.Region = originalRegion }()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}
	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
	}

	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleInvokesSdeCleanupWhenAttributesPresent(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalRegion := env.Region
	env.Region = "us-central1"
	defer func() { env.Region = originalRegion }()

	originalGetToken := getSignedJwtTokenFn
	originalDelete := deleteSDEKmsConfigurationFn
	defer func() {
		getSignedJwtTokenFn = originalGetToken
		deleteSDEKmsConfigurationFn = originalDelete
	}()

	tokenRequested := false
	getSignedJwtTokenFn = func(accountName string) (string, error) {
		tokenRequested = true
		require.Equal(t, "acct-123", accountName)
		return "token", nil
	}

	sdeDeleteCalled := false
	deleteSDEKmsConfigurationFn = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
		sdeDeleteCalled = true
		require.NotNil(t, kmsConfig.KmsAttributes)
		require.Equal(t, "sde-kms-uuid", kmsConfig.KmsAttributes.SdeKmsConfigUUID)
		require.Equal(t, "acct-123", params.AccountName)
		require.Equal(t, "us-central1", params.Region)
		return nil, nil
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		KmsAttributes: &datamodel.KmsAttributes{
			SdeKmsConfigUUID: "sde-kms-uuid",
		},
		CustomerProjectID: "acct-123",
	}

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		CorrelationID: "corr-id",
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().DeleteKmsConfig(mock.Anything, "kms-uuid", models.LifeCycleStateError, WorkflowTimeoutDetail).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	require.True(t, tokenRequested)
	require.True(t, sdeDeleteCalled)
}

func TestCmekHandlerHandleSdeKmsCreateDeletesMatchingConfig(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	originalList := listSDEKmsConfigurationsFn
	originalDelete := deleteSDEKmsConfigurationFn
	defer func() {
		getSignedJwtTokenFn = originalGetToken
		listSDEKmsConfigurationsFn = originalList
		deleteSDEKmsConfigurationFn = originalDelete
	}()

	keyFullPath := "projects/123456789/locations/us-east1/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1"
	getSignedJwtTokenFn = func(accountName string) (string, error) {
		require.Equal(t, "123456789", accountName)
		return "token", nil
	}

	var listProject, listLocation, listCorrelation string
	listSDEKmsConfigurationsFn = func(ctx context.Context, projectNumber, locationID, correlationID string) ([]sde.KmsConfigSummary, error) {
		listProject = projectNumber
		listLocation = locationID
		listCorrelation = correlationID
		return []sde.KmsConfigSummary{
			{
				UUID:        "sde-uuid",
				ResourceID:  "resource-1",
				KeyFullPath: keyFullPath,
			},
		}, nil
	}

	deleteCalled := false
	deleteSDEKmsConfigurationFn = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
		deleteCalled = true
		require.NotNil(t, kmsConfig.KmsAttributes)
		require.Equal(t, "sde-uuid", kmsConfig.KmsAttributes.SdeKmsConfigUUID)
		require.Equal(t, "sde-uuid", params.KmsConfigID)
		require.Equal(t, "123456789", params.AccountName)
		require.Equal(t, "us-east1", params.Region)
		require.Equal(t, "corr-id", params.XCorrelationID)
		return nil, nil
	}

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		Type:          string(models.JobTypeSdeKmsCreate),
		CorrelationID: "corr-id",
		ResourceName:  "resource-1",
		JobAttributes: &datamodel.JobAttributes{
			PayloadAttributes: map[string]interface{}{
				"keyFullPath":   keyFullPath,
				"projectNumber": "123456789",
				"locationId":    "us-east1",
			},
		},
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	require.True(t, deleteCalled)
	require.Equal(t, "123456789", listProject)
	require.Equal(t, "us-east1", listLocation)
	require.Equal(t, "corr-id", listCorrelation)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleSdeKmsCreateSkipsWhenNoMatch(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	originalList := listSDEKmsConfigurationsFn
	originalDelete := deleteSDEKmsConfigurationFn
	defer func() {
		getSignedJwtTokenFn = originalGetToken
		listSDEKmsConfigurationsFn = originalList
		deleteSDEKmsConfigurationFn = originalDelete
	}()

	getSignedJwtTokenFn = func(accountName string) (string, error) {
		return "token", nil
	}

	listSDEKmsConfigurationsFn = func(ctx context.Context, projectNumber, locationID, correlationID string) ([]sde.KmsConfigSummary, error) {
		return []sde.KmsConfigSummary{
			{
				UUID:        "other-uuid",
				ResourceID:  "other-resource",
				KeyFullPath: "projects/123/locations/us/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1",
			},
		}, nil
	}

	deleteCalled := false
	deleteSDEKmsConfigurationFn = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
		deleteCalled = true
		return nil, nil
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeSdeKmsCreate),
		ResourceName: "resource-1",
		JobAttributes: &datamodel.JobAttributes{
			PayloadAttributes: map[string]interface{}{
				"keyFullPath":   "projects/123456789/locations/us-east1/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1",
				"projectNumber": "123456789",
				"locationId":    "us-east1",
			},
		},
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	require.False(t, deleteCalled)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleSdeKmsCreateSkipsWhenResourceNameMissing(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	defer func() { getSignedJwtTokenFn = originalGetToken }()

	getSignedJwtTokenFn = func(accountName string) (string, error) {
		t.Fatalf("getSignedJwtTokenFn should not be called")
		return "", nil
	}

	job := &datamodel.Job{
		Type: string(models.JobTypeSdeKmsCreate),
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestCmekHandlerHandleSdeKmsCreateSkipsWhenPayloadMissing(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	defer func() { getSignedJwtTokenFn = originalGetToken }()

	getSignedJwtTokenFn = func(accountName string) (string, error) {
		t.Fatalf("getSignedJwtTokenFn should not be called")
		return "", nil
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeSdeKmsCreate),
		ResourceName: "resource",
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestCmekHandlerHandleSdeKmsCreateSkipsWhenKeyMissing(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	defer func() { getSignedJwtTokenFn = originalGetToken }()

	getSignedJwtTokenFn = func(accountName string) (string, error) {
		t.Fatalf("getSignedJwtTokenFn should not be called")
		return "", nil
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeSdeKmsCreate),
		ResourceName: "resource",
		JobAttributes: &datamodel.JobAttributes{
			PayloadAttributes: map[string]interface{}{
				"keyFullPath": "   ",
			},
		},
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestCmekHandlerHandleSdeKmsCreateSkipsWhenProjectOrLocationMissing(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	defer func() { getSignedJwtTokenFn = originalGetToken }()

	getSignedJwtTokenFn = func(accountName string) (string, error) {
		t.Fatalf("getSignedJwtTokenFn should not be called")
		return "", nil
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeSdeKmsCreate),
		ResourceName: "resource",
		JobAttributes: &datamodel.JobAttributes{
			PayloadAttributes: map[string]interface{}{
				"keyFullPath":   "projects/123/locations/us/keyRings/ring/cryptoKeys/key",
				"projectNumber": "",
				"locationId":    "us",
			},
		},
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestCmekHandlerHandleSdeKmsCreateSkipsWhenTokenFails(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	originalList := listSDEKmsConfigurationsFn
	defer func() {
		getSignedJwtTokenFn = originalGetToken
		listSDEKmsConfigurationsFn = originalList
	}()

	getSignedJwtTokenFn = func(accountName string) (string, error) {
		return "", errors.New("token failed")
	}

	listCalled := false
	listSDEKmsConfigurationsFn = func(ctx context.Context, projectNumber, locationID, correlationID string) ([]sde.KmsConfigSummary, error) {
		listCalled = true
		return nil, nil
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeSdeKmsCreate),
		ResourceName: "resource",
		JobAttributes: &datamodel.JobAttributes{
			PayloadAttributes: map[string]interface{}{
				"keyFullPath":   "projects/123/locations/us/keyRings/ring/cryptoKeys/key",
				"projectNumber": "123",
				"locationId":    "us",
			},
		},
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	require.False(t, listCalled)
}

func TestCmekHandlerHandleSdeKmsCreateSkipsWhenListFails(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	originalList := listSDEKmsConfigurationsFn
	originalDelete := deleteSDEKmsConfigurationFn
	defer func() {
		getSignedJwtTokenFn = originalGetToken
		listSDEKmsConfigurationsFn = originalList
		deleteSDEKmsConfigurationFn = originalDelete
	}()

	getSignedJwtTokenFn = func(accountName string) (string, error) {
		return "token", nil
	}

	listSDEKmsConfigurationsFn = func(ctx context.Context, projectNumber, locationID, correlationID string) ([]sde.KmsConfigSummary, error) {
		return nil, errors.New("list failed")
	}

	deleteCalled := false
	deleteSDEKmsConfigurationFn = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
		deleteCalled = true
		return nil, nil
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeSdeKmsCreate),
		ResourceName: "resource",
		JobAttributes: &datamodel.JobAttributes{
			PayloadAttributes: map[string]interface{}{
				"keyFullPath":   "projects/123/locations/us/keyRings/ring/cryptoKeys/key",
				"projectNumber": "123",
				"locationId":    "us",
			},
		},
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	require.False(t, deleteCalled)
}

func TestCmekHandlerHandleSdeKmsCreateHandlesMultipleConfigs(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	originalGetToken := getSignedJwtTokenFn
	originalList := listSDEKmsConfigurationsFn
	originalDelete := deleteSDEKmsConfigurationFn
	defer func() {
		getSignedJwtTokenFn = originalGetToken
		listSDEKmsConfigurationsFn = originalList
		deleteSDEKmsConfigurationFn = originalDelete
	}()

	keyFullPath := "projects/123/locations/us/keyRings/ring/cryptoKeys/key"

	getSignedJwtTokenFn = func(accountName string) (string, error) {
		require.Equal(t, "123", accountName)
		return "token", nil
	}

	listSDEKmsConfigurationsFn = func(ctx context.Context, projectNumber, locationID, correlationID string) ([]sde.KmsConfigSummary, error) {
		require.Equal(t, "123", projectNumber)
		require.Equal(t, "us", locationID)
		return []sde.KmsConfigSummary{
			{}, // missing UUID to cover skip
			{
				UUID:        "uuid-1",
				ResourceID:  "resource-1",
				KeyFullPath: "different-key",
			},
			{
				UUID:        "uuid-2",
				ResourceID:  "other-resource",
				KeyFullPath: keyFullPath,
			},
			{
				UUID:        "uuid-3",
				ResourceID:  "resource-1",
				KeyFullPath: keyFullPath,
			},
		}, nil
	}

	deleteCalls := 0
	deleteSDEKmsConfigurationFn = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
		deleteCalls++
		require.Equal(t, "uuid-3", params.KmsConfigID)
		return nil, errors.New("delete failed")
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeSdeKmsCreate),
		ResourceName: "resource-1",
		JobAttributes: &datamodel.JobAttributes{
			PayloadAttributes: map[string]interface{}{
				"keyFullPath":   keyFullPath,
				"projectNumber": "123",
				"locationId":    "us",
			},
		},
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	require.Equal(t, 1, deleteCalls)
}
