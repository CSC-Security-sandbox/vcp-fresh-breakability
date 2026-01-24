package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

type mockBackupVaultStorage struct {
	mock.Mock
	database.Storage
}

func (m *mockBackupVaultStorage) GetCmekRotationJobStatuses(ctx context.Context, startTime, endTime time.Time, limit, offset int) ([]*database.CmekRotationJobStatus, error) {
	args := m.Called(ctx, startTime, endTime, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*database.CmekRotationJobStatus), args.Error(1)
}

func Test_GetBackupVaultMetrics_ReturnsMetrics(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{
		RegionName: "us-east-1",
	}
	timestamp := time.Now()

	jobStatuses := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "DONE",
			BackupVaultUUID: "vault-uuid-1",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault1",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
			AccountIdentifier: "account-1",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(jobStatuses, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset > 0
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 1)

	assert.Equal(t, metadata.CMEKBackupKeyRotationState, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(CmekRotationStateCompleted), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "vault-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, metadata.BackupVault, result.HydratedMetrics[0].Metadata.ResourceType)
	assert.Equal(t, "BackupVault1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "us-east-1", derefString(result.HydratedMetrics[0].Metadata.RegionName))
	assert.Equal(t, "account-1", derefString(result.HydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, "projects/test/locations/us/keyRings/test/cryptoKeys/key1", result.HydratedMetrics[0].Metadata.Tags["backup_crypto_key_version"])
}

func Test_GetBackupVaultMetrics_MultipleJobStatuses(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{
		RegionName: "us-east-1",
	}
	timestamp := time.Now()

	jobStatuses := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "NEW",
			BackupVaultUUID: "vault-uuid-1",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault1",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
			AccountIdentifier: "account-1",
		},
		{
			ID:              2,
			Status:          "PROCESSING",
			BackupVaultUUID: "vault-uuid-2",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault2",
			Region:          "us-west-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key2",
			AccountIdentifier: "account-2",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(jobStatuses, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset > 0
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 2)

	assert.Equal(t, metadata.CMEKBackupKeyRotationState, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(CmekRotationStatePending), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "vault-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "BackupVault1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "account-1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	assert.Equal(t, metadata.CMEKBackupKeyRotationState, result.HydratedMetrics[1].MeasuredType)
	assert.Equal(t, float64(CmekRotationStateInProgress), result.HydratedMetrics[1].Quantity)
	assert.Equal(t, "vault-uuid-2", derefString(result.HydratedMetrics[1].Metadata.ResourceUUID))
	assert.Equal(t, "BackupVault2", derefString(result.HydratedMetrics[1].Metadata.ResourceName))
	assert.Equal(t, "account-2", derefString(result.HydratedMetrics[1].Metadata.AccountName))
}

func Test_GetBackupVaultMetrics_EmptyJobStatuses(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Now()
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
}

func Test_GetBackupVaultMetrics_GetCmekRotationJobStatusesError(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Now()
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("database error"))

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.Error(t, err)
	assert.EqualError(t, err, "database error")
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
}

func Test_GetBackupVaultMetrics_MissingBackupVaultUUID(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Now()

	jobStatuses := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "DONE",
			BackupVaultUUID: "", // Missing UUID should be skipped
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault1",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
			AccountIdentifier: "account-1",
		},
		{
			ID:              2,
			Status:          "NEW",
			BackupVaultUUID: "vault-uuid-2",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault2",
			Region:          "us-west-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key2",
			AccountIdentifier: "account-2",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(jobStatuses, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset > 0
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 1)

	assert.Equal(t, "vault-uuid-2", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "BackupVault2", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
}

func Test_GetBackupVaultMetrics_MissingBackupVaultName(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Now()

	jobStatuses := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "DONE",
			BackupVaultUUID: "vault-uuid-1",
			UpdatedAt:       timestamp,
			BackupVaultName: "", // Missing name should be skipped
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
			AccountIdentifier: "account-1",
		},
		{
			ID:              2,
			Status:          "NEW",
			BackupVaultUUID: "vault-uuid-2",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault2",
			Region:          "us-west-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key2",
			AccountIdentifier: "account-2",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(jobStatuses, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset > 0
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 1)

	assert.Equal(t, "vault-uuid-2", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "BackupVault2", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
}

func Test_GetBackupVaultMetrics_Pagination(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Now()

	firstPage := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "DONE",
			BackupVaultUUID: "vault-uuid-1",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault1",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
			AccountIdentifier: "account-1",
		},
	}
	secondPage := []*database.CmekRotationJobStatus{
		{
			ID:              2,
			Status:          "NEW",
			BackupVaultUUID: "vault-uuid-2",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault2",
			Region:          "us-west-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key2",
			AccountIdentifier: "account-2",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(firstPage, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 1000
	})).Return(secondPage, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 2000
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 2)

	assert.Equal(t, "vault-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "vault-uuid-2", derefString(result.HydratedMetrics[1].Metadata.ResourceUUID))
}

func Test_GetBackupVaultMetrics_AllRotationStates(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Now()

	jobStatuses := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "NEW",
			BackupVaultUUID: "vault-uuid-1",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault1",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
			AccountIdentifier: "account-1",
		},
		{
			ID:              2,
			Status:          "PROCESSING",
			BackupVaultUUID: "vault-uuid-2",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault2",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key2",
			AccountIdentifier: "account-2",
		},
		{
			ID:              3,
			Status:          "DONE",
			BackupVaultUUID: "vault-uuid-3",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault3",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key3",
			AccountIdentifier: "account-3",
		},
		{
			ID:              4,
			Status:          "ERROR",
			BackupVaultUUID: "vault-uuid-4",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault4",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key4",
			AccountIdentifier: "account-4",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(jobStatuses, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset > 0
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 4)

	assert.Equal(t, float64(CmekRotationStatePending), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, float64(CmekRotationStateInProgress), result.HydratedMetrics[1].Quantity)
	assert.Equal(t, float64(CmekRotationStateCompleted), result.HydratedMetrics[2].Quantity)
	assert.Equal(t, float64(CmekRotationStateFailed), result.HydratedMetrics[3].Quantity)
}

func Test_mapJobStatusToRotationState(t *testing.T) {
	tests := []struct {
		name          string
		status        string
		expectedState int64
	}{
		{"NEW status", "NEW", CmekRotationStatePending},
		{"PROCESSING status", "PROCESSING", CmekRotationStateInProgress},
		{"DONE status", "DONE", CmekRotationStateCompleted},
		{"ERROR status", "ERROR", CmekRotationStateFailed},
		{"Unknown status", "unknown", CmekRotationStatePending},
		{"Empty status", "", CmekRotationStatePending},
		{"Lowercase new", "new", CmekRotationStatePending},
		{"Lowercase processing", "processing", CmekRotationStatePending},
		{"Lowercase done", "done", CmekRotationStatePending},
		{"Lowercase error", "error", CmekRotationStatePending},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapJobStatusToRotationState(tt.status)
			assert.Equal(t, tt.expectedState, result)
		})
	}
}

func Test_assembleBackupVaultMetadata(t *testing.T) {
	jobStatus := &database.CmekRotationJobStatus{
		ID:               1,
		Status:           "DONE",
		BackupVaultUUID:  "vault-uuid-1",
		UpdatedAt:        time.Now(),
		BackupVaultName:  "BackupVault1",
		Region:           "us-east-1",
		NewKmsKeyURL:     "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
		AccountIdentifier: "account-1",
	}

	config := &common.TelemetryConfig{
		RegionName: "us-east-1",
	}

	resourceMetadata := assembleBackupVaultMetadata(jobStatus, config)

	assert.Equal(t, "vault-uuid-1", derefString(resourceMetadata.ResourceUUID))
	assert.Equal(t, metadata.BackupVault, resourceMetadata.ResourceType)
	assert.Equal(t, "us-east-1", derefString(resourceMetadata.RegionName))
	assert.Equal(t, "BackupVault1", derefString(resourceMetadata.ResourceName))
	assert.Equal(t, "BackupVault1", derefString(resourceMetadata.ResourceDisplayName))
	assert.Equal(t, "account-1", derefString(resourceMetadata.AccountName))
}

func Test_GetBackupVaultMetrics_TimeRange(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	jobStatuses := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "DONE",
			BackupVaultUUID: "vault-uuid-1",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault1",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
			AccountIdentifier: "account-1",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.MatchedBy(func(startTime time.Time) bool {
		expectedStartTime := timestamp.Add(-5 * time.Minute)
		return startTime.Equal(expectedStartTime)
	}), mock.MatchedBy(func(endTime time.Time) bool {
		return endTime.Equal(timestamp)
	}), mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(jobStatuses, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset > 0
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 1)
}

func Test_GetBackupVaultMetrics_MixedValidAndInvalid(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Now()

	jobStatuses := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "DONE",
			BackupVaultUUID: "vault-uuid-1",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault1",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key1",
			AccountIdentifier: "account-1",
		},
		{
			ID:              2,
			Status:          "NEW",
			BackupVaultUUID: "", // Missing UUID - should be skipped
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault2",
			Region:          "us-west-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key2",
			AccountIdentifier: "account-2",
		},
		{
			ID:              3,
			Status:          "PROCESSING",
			BackupVaultUUID: "vault-uuid-3",
			UpdatedAt:       timestamp,
			BackupVaultName: "", // Missing name - should be skipped
			Region:          "us-west-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key3",
			AccountIdentifier: "account-3",
		},
		{
			ID:              4,
			Status:          "ERROR",
			BackupVaultUUID: "vault-uuid-4",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault4",
			Region:          "us-west-1",
			NewKmsKeyURL:    "projects/test/locations/us/keyRings/test/cryptoKeys/key4",
			AccountIdentifier: "account-4",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(jobStatuses, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset > 0
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 2)

	assert.Equal(t, "vault-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "BackupVault1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, float64(CmekRotationStateCompleted), result.HydratedMetrics[0].Quantity)

	assert.Equal(t, "vault-uuid-4", derefString(result.HydratedMetrics[1].Metadata.ResourceUUID))
	assert.Equal(t, "BackupVault4", derefString(result.HydratedMetrics[1].Metadata.ResourceName))
	assert.Equal(t, float64(CmekRotationStateFailed), result.HydratedMetrics[1].Quantity)
}

func Test_GetBackupVaultMetrics_BackupCryptoKeyVersionTag(t *testing.T) {
	m := new(mockBackupVaultStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	timestamp := time.Now()

	jobStatuses := []*database.CmekRotationJobStatus{
		{
			ID:              1,
			Status:          "DONE",
			BackupVaultUUID: "vault-uuid-1",
			UpdatedAt:       timestamp,
			BackupVaultName: "BackupVault1",
			Region:          "us-east-1",
			NewKmsKeyURL:    "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key",
			AccountIdentifier: "account-1",
		},
	}

	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset == 0
	})).Return(jobStatuses, nil)
	m.On("GetCmekRotationJobStatuses", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(offset int) bool {
		return offset > 0
	})).Return([]*database.CmekRotationJobStatus{}, nil)

	result, err := GetBackupVaultMetrics(ctx, m, config, timestamp)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 1)

	assert.NotNil(t, result.HydratedMetrics[0].Metadata.Tags)
	assert.Equal(t, "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key", result.HydratedMetrics[0].Metadata.Tags["backup_crypto_key_version"])
}
