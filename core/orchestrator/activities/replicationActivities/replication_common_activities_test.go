package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	errs "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestHydrateVolumeReplication(t *testing.T) {
	createReplicationResponse := &models.VolumeReplication{
		Name:  "replication-name",
		State: "creating",
		ReplicationAttributes: &models.ReplicationDetails{
			DestinationReplicationUUID: "mocked-replication-id",
			DestinationRegion:          "mocked-region",
		},
	}
	t.Run("WhenTokenError", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("some error")
		}
		defer func() { auth.GenerateCallbackToken = originalGetSignedCallbackToken }()
		err := HydrateVolumeReplication(ctx, *createReplicationResponse, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenHydrationReplicationCreateFail", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationCreate := hydrateReplicationCreate
		hydrateReplicationCreate = func(ctx context.Context, logger log.Logger, replication models.ReplicationHydrateObject, region, projectId, volumeResourceID, token string) error {
			return &errs.CustomError{
				OriginalErr: errors.New("some error"),
			}
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateReplicationCreate = originalHydrateReplicationCreate
		}()
		err := HydrateVolumeReplication(ctx, *createReplicationResponse, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err.(*errs.CustomError).Unwrap())
	})
	t.Run("WhenSuccessFul", func(tt *testing.T) {
		ctx := context.Background()
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationCreate := hydrateReplicationCreate
		hydrateReplicationCreate = func(ctx context.Context, logger log.Logger, replication models.ReplicationHydrateObject, region, projectId, volumeResourceID, token string) error {
			return nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateReplicationCreate = originalHydrateReplicationCreate
		}()
		err := HydrateVolumeReplication(ctx, *createReplicationResponse, "121")
		assert.NoError(tt, err, nil)
	})
}

func TestVolumeReplicationDeHydration(t *testing.T) {
	createReplicationResponse := &models.VolumeReplication{
		Name:  "replication-name",
		State: "creating",
		ReplicationAttributes: &models.ReplicationDetails{
			DestinationReplicationUUID: "mocked-replication-id",
			DestinationRegion:          "mocked-region",
		},
		Volume: &models.Volume{BaseModel: models.BaseModel{UUID: "123"}},
	}
	t.Run("WhenTokenError", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("some error")
		}
		defer func() { auth.GenerateCallbackToken = originalGetSignedCallbackToken }()
		err := DeHydrateVolumeReplication(ctx, *createReplicationResponse, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenHydrateReplicationDeleteFail", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationDelete := hydrateReplicationDelete
		hydrateReplicationDelete = func(ctx context.Context, logger log.Logger, replicationResourceId string, volumeResourceID string, region string, projectId string, token string) error {
			return &errs.CustomError{
				OriginalErr: errors.New("some error"),
			}
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateReplicationDelete = originalHydrateReplicationDelete
		}()
		err := DeHydrateVolumeReplication(ctx, *createReplicationResponse, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err.(*errs.CustomError).Unwrap())
	})
	t.Run("WhenSuccessFul", func(tt *testing.T) {
		ctx := context.Background()
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationDelete := hydrateReplicationDelete
		hydrateReplicationDelete = func(ctx context.Context, logger log.Logger, replicationResourceId string, volumeResourceID string, region string, projectId string, token string) error {
			return nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateReplicationDelete = originalHydrateReplicationDelete
		}()
		err := DeHydrateVolumeReplication(ctx, *createReplicationResponse, "121")
		assert.NoError(tt, err)
	})
}

func TestVolumeHydration(t *testing.T) {
	destVolume := &models.Volume{
		DisplayName:  "vol",
		QuotaInBytes: 1,
		BaseModel:    models.BaseModel{UUID: "123"},
		Region:       "asia-south1",
	}
	t.Run("WhenTokenError", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("some error")
		}
		defer func() { auth.GenerateCallbackToken = originalGetSignedCallbackToken }()
		err := HydrateVolume(ctx, *destVolume, "121", "pool-resource-id")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenHydrateReplicationDeleteFail", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalhydrateVolumeCreate := hydrateVolumeCreate
		hydrateVolumeCreate = func(ctx context.Context, logger log.Logger, volume models.VolumeHydrateObject, location string, projectId string, token string) error {
			return &errs.CustomError{
				OriginalErr: errors.New("some error"),
			}
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateVolumeCreate = originalhydrateVolumeCreate
		}()
		err := HydrateVolume(ctx, *destVolume, "121", "pool-resource-id")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err.(*errs.CustomError).Unwrap())
	})
	t.Run("WhenSuccessFul", func(tt *testing.T) {
		ctx := context.Background()
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateVolumeCreate := hydrateVolumeCreate
		hydrateVolumeCreate = func(ctx context.Context, logger log.Logger, volume models.VolumeHydrateObject, location string, projectId string, token string) error {
			return nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateVolumeCreate = originalHydrateVolumeCreate
		}()
		err := HydrateVolume(ctx, *destVolume, "121", "pool-resource-id")
		assert.NoError(tt, err)
	})
}

func TestVolumeDeHydration(t *testing.T) {
	destVolume := &models.Volume{
		DisplayName:  "vol",
		QuotaInBytes: 1,
		BaseModel:    models.BaseModel{UUID: "123"},
		Region:       "asia-south1",
	}
	t.Run("WhenTokenError", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("some error")
		}
		defer func() { auth.GenerateCallbackToken = originalGetSignedCallbackToken }()
		err := DeHydrateVolume(ctx, *destVolume, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenHydrateReplicationDeleteFail", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalhydrateVolumeDelete := hydrateVolumeDelete
		hydrateVolumeDelete = func(ctx context.Context, logger log.Logger, volumeResourceID string, region string, projectId string, token string) error {
			return &errs.CustomError{
				OriginalErr: errors.New("some error"),
			}
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateVolumeDelete = originalhydrateVolumeDelete
		}()
		err := DeHydrateVolume(ctx, *destVolume, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err.(*errs.CustomError).Unwrap())
	})
	t.Run("WhenSuccessFul", func(tt *testing.T) {
		ctx := context.Background()
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalhydrateVolumeDelete := hydrateVolumeDelete
		hydrateVolumeDelete = func(ctx context.Context, logger log.Logger, volumeResourceID string, region string, projectId string, token string) error {
			return nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateVolumeDelete = originalhydrateVolumeDelete
		}()
		err := DeHydrateVolume(ctx, *destVolume, "121")
		assert.NoError(tt, err)
	})
}

func TestGetQuotaLimit(t *testing.T) {
	t.Run("WhenTokenError", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("some error")
		}
		defer func() { auth.GenerateCallbackToken = originalGetSignedCallbackToken }()
		_, err := GetQuotaLimit(ctx, "us-east1", "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenHydrationReplicationCreateFail", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationCreate := getQuotaLimit
		getQuotaLimit = func(ctx context.Context, logger log.Logger, region string, projectId string, token string, resourceType common.ResourceType) (int, error) {
			return 0, errors.New("some error")
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			getQuotaLimit = originalHydrateReplicationCreate
		}()
		_, err := GetQuotaLimit(ctx, "us-east1", "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccessFul", func(tt *testing.T) {
		ctx := context.Background()
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationCreate := getQuotaLimit
		getQuotaLimit = func(ctx context.Context, logger log.Logger, region string, projectId string, token string, resourceType common.ResourceType) (int, error) {
			return 1, nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			getQuotaLimit = originalHydrateReplicationCreate
		}()
		dst, err := GetQuotaLimit(ctx, "us-east1", "121")
		assert.NoError(tt, err)
		assert.Equal(tt, dst, 1)
	})
}

func TestHydrateReplicationState(t *testing.T) {
	createReplicationResponse := &models.VolumeReplication{
		Name:  "replication-name",
		State: "creating",
		ReplicationAttributes: &models.ReplicationDetails{
			DestinationReplicationUUID: "mocked-replication-id",
			DestinationRegion:          "mocked-region",
		},
		Volume: &models.Volume{BaseModel: models.BaseModel{UUID: "123"}},
	}
	replicationState := models.VolumeReplicationHydrateState("CREATING")
	t.Run("WhenTokenError", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("some error")
		}
		defer func() { auth.GenerateCallbackToken = originalGetSignedCallbackToken }()
		err := HydrateReplicationState(ctx, *createReplicationResponse, replicationState, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenHydrateReplicationDeleteFail", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationState := hydrateReplicationState
		hydrateReplicationState = func(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, token string) error {
			return &errs.CustomError{
				OriginalErr: errors.New("some error"),
			}
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateReplicationState = originalHydrateReplicationState
		}()
		err := HydrateReplicationState(ctx, *createReplicationResponse, replicationState, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err.(*errs.CustomError).Unwrap())
	})
	t.Run("WhenSuccessFul", func(tt *testing.T) {
		ctx := context.Background()
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationState := hydrateReplicationState
		hydrateReplicationState = func(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, token string) error {
			return nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateReplicationState = originalHydrateReplicationState
		}()
		err := HydrateReplicationState(ctx, *createReplicationResponse, replicationState, "121")
		assert.NoError(tt, err)
	})
}

func TestHydrateReplicationStateAndType(t *testing.T) {
	createReplicationResponse := &models.VolumeReplication{
		Name:  "replication-name",
		State: "creating",
		ReplicationAttributes: &models.ReplicationDetails{
			DestinationReplicationUUID: "mocked-replication-id",
			DestinationRegion:          "mocked-region",
		},
		Volume: &models.Volume{BaseModel: models.BaseModel{UUID: "123"}},
	}
	var replicationState models.VolumeReplicationHydrateState
	var hybridReplicationType models.HybridReplicationHydrateType
	replicationState = "creating"
	hybridReplicationType = "cres"
	t.Run("WhenTokenError", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("some error")
		}
		defer func() { auth.GenerateCallbackToken = originalGetSignedCallbackToken }()
		err := HydrateReplicationStateAndType(ctx, *createReplicationResponse, replicationState, hybridReplicationType, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenHydrateReplicationDeleteFail", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationStateAndType := hydrateReplicationStateAndType
		hydrateReplicationStateAndType = func(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, hybridReplicationType models.HybridReplicationHydrateType, token string) error {
			return &errs.CustomError{
				OriginalErr: errors.New("some error"),
			}
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateReplicationStateAndType = originalHydrateReplicationStateAndType
		}()
		err := HydrateReplicationStateAndType(ctx, *createReplicationResponse, replicationState, hybridReplicationType, "121")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err.(*errs.CustomError).Unwrap())
	})
	t.Run("WhenSuccessFul", func(tt *testing.T) {
		ctx := context.Background()
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalHydrateReplicationStateAndType := hydrateReplicationStateAndType
		hydrateReplicationStateAndType = func(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, hybridReplicationType models.HybridReplicationHydrateType, token string) error {
			return nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
			hydrateReplicationStateAndType = originalHydrateReplicationStateAndType
		}()
		err := HydrateReplicationStateAndType(ctx, *createReplicationResponse, replicationState, hybridReplicationType, "121")
		assert.NoError(tt, err)
	})
}
