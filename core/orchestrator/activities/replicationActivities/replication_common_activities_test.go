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

func TestMapVolumeBetaToVolumeHydrateObject(t *testing.T) {
	t.Run("WhenVolumeWithAllFields", func(tt *testing.T) {
		volume := models.Volume{
			BaseModel: models.BaseModel{
				UUID: "test-volume-uuid",
			},
			DisplayName:   "test-volume-display",
			QuotaInBytes:  1073741824, // 1 GiB in bytes
			ProtocolTypes: []string{"NFS", "SMB"},
		}
		poolResourceId := "test-pool-resource-id"

		result := mapVolumeBetaToVolumeHydrateObject(volume, poolResourceId)

		// Verify all fields are correctly mapped
		assert.Equal(tt, "test-volume-display", result.ResourceId)
		assert.Equal(tt, "test-volume-uuid", result.VolumeId)
		assert.Equal(tt, "test-pool-resource-id", result.PoolId)
		assert.Equal(tt, []string{"NFS", "SMB"}, result.Protocols)
		assert.Equal(tt, "READY", result.State)
		assert.Equal(tt, int64(1), result.QuotaInGib) // 1 GiB
		assert.Equal(tt, VolumeV1betaServiceLevelFLEX, result.ServiceLevel)
	})
}

func TestMapReplicationBetaToReplicationHydrateObject(t *testing.T) {
	t.Run("WhenReplicationWithHybridReplicationAttributesAndLabels", func(tt *testing.T) {
		replication := models.VolumeReplication{
			Name:  "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication",
			State: "available",
			HybridReplicationAttributes: &models.HybridReplicationParameters{
				ReplicationType: models.HybridReplicationParametersReplicationTypeMIGRATION,
				Labels: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
			},
		}

		result := MapReplicationBetaToReplicationHydrateObject(replication)

		assert.Equal(tt, "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication", result.ResourceId)
		assert.Equal(tt, "READY", result.ReplicationState)
		assert.NotNil(tt, result.HybridReplicationType)
		assert.Equal(tt, models.HybridReplicationHydrateType("MIGRATION"), *result.HybridReplicationType)
		assert.NotNil(tt, result.Labels)
		assert.Equal(tt, map[string]string{"key1": "value1", "key2": "value2"}, result.Labels)
	})

	t.Run("WhenReplicationWithHybridReplicationAttributesWithoutLabels", func(tt *testing.T) {
		replication := models.VolumeReplication{
			Name:  "test-replication-name",
			State: "creating",
			HybridReplicationAttributes: &models.HybridReplicationParameters{
				ReplicationType: models.HybridReplicationParametersReplicationTypeCONTINUOUS,
				Labels:          nil,
			},
		}

		result := MapReplicationBetaToReplicationHydrateObject(replication)

		assert.Equal(tt, "test-replication-name", result.ResourceId)
		assert.Equal(tt, "CREATING", result.ReplicationState)
		assert.NotNil(tt, result.HybridReplicationType)
		assert.Equal(tt, models.HybridReplicationHydrateType("CONTINUOUS_REPLICATION"), *result.HybridReplicationType)
		assert.Nil(tt, result.Labels)
	})

	t.Run("WhenReplicationWithoutHybridReplicationAttributes", func(tt *testing.T) {
		replication := models.VolumeReplication{
			Name:                        "test-replication-name",
			State:                       "updating",
			HybridReplicationAttributes: nil,
		}

		result := MapReplicationBetaToReplicationHydrateObject(replication)

		assert.Equal(tt, "test-replication-name", result.ResourceId)
		assert.Equal(tt, "UPDATING", result.ReplicationState)
		// When HybridReplicationAttributes is nil, HybridReplicationType should be nil
		assert.Nil(tt, result.HybridReplicationType)
		assert.Nil(tt, result.Labels)
	})

	t.Run("WhenReplicationWithDifferentStates", func(tt *testing.T) {
		testCases := []struct {
			name            string
			state           string
			expectedState   string
			replicationType models.HybridReplicationParametersReplicationType
		}{
			{"CreatingState", "creating", "CREATING", models.HybridReplicationParametersReplicationTypeMIGRATION},
			{"AvailableState", "available", "READY", models.HybridReplicationParametersReplicationTypeCONTINUOUS},
			{"UpdatingState", "updating", "UPDATING", models.HybridReplicationParametersReplicationTypeONPREM},
			{"DisabledState", "disabled", "STOPPED", models.HybridReplicationParametersReplicationTypeREVERSE},
			{"DeletingState", "deleting", "DELETING", models.HybridReplicationParametersReplicationTypeMIGRATION},
			{"PendingClusterPeeringState", "PENDING_CLUSTER_PEERING", "PENDING_CLUSTER_PEERING", models.HybridReplicationParametersReplicationTypeCONTINUOUS},
			{"ErrorState", "error", "ERROR", models.HybridReplicationParametersReplicationTypeMIGRATION},
			{"UnknownState", "unknown-state", "STATE_UNSPECIFIED", models.HybridReplicationParametersReplicationTypeMIGRATION},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				replication := models.VolumeReplication{
					Name:  "test-replication",
					State: tc.state,
					HybridReplicationAttributes: &models.HybridReplicationParameters{
						ReplicationType: tc.replicationType,
					},
				}

				result := MapReplicationBetaToReplicationHydrateObject(replication)

				assert.Equal(tt, "test-replication", result.ResourceId)
				assert.Equal(tt, tc.expectedState, result.ReplicationState)
				assert.NotNil(tt, result.HybridReplicationType)
				assert.Equal(tt, models.HybridReplicationHydrateType(tc.replicationType), *result.HybridReplicationType)
			})
		}
	})

	t.Run("WhenReplicationWithEmptyLabels", func(tt *testing.T) {
		replication := models.VolumeReplication{
			Name:  "test-replication",
			State: "available",
			HybridReplicationAttributes: &models.HybridReplicationParameters{
				ReplicationType: models.HybridReplicationParametersReplicationTypeMIGRATION,
				Labels:          make(map[string]string),
			},
		}

		result := MapReplicationBetaToReplicationHydrateObject(replication)

		assert.Equal(tt, "test-replication", result.ResourceId)
		assert.Equal(tt, "READY", result.ReplicationState)
		assert.NotNil(tt, result.Labels)
		assert.Equal(tt, map[string]string{}, result.Labels)
	})

	t.Run("WhenReplicationWithAllReplicationTypes", func(tt *testing.T) {
		replicationTypes := []struct {
			input    models.HybridReplicationParametersReplicationType
			expected models.HybridReplicationHydrateType
		}{
			{models.HybridReplicationParametersReplicationTypeMIGRATION, models.HybridReplicationHydrateType("MIGRATION")},
			{models.HybridReplicationParametersReplicationTypeCONTINUOUS, models.HybridReplicationHydrateType("CONTINUOUS_REPLICATION")},
			{models.HybridReplicationParametersReplicationTypeONPREM, models.HybridReplicationHydrateType("ONPREM_REPLICATION")},
			{models.HybridReplicationParametersReplicationTypeREVERSE, models.HybridReplicationHydrateType("REVERSE_ONPREM_REPLICATION")},
			{models.HybridReplicationParametersReplicationTypeUNSPECIFIED, models.HybridReplicationHydrateType("REPLICATION_TYPE_UNSPECIFIED")},
		}

		for _, rt := range replicationTypes {
			tt.Run(string(rt.input), func(t *testing.T) {
				replication := models.VolumeReplication{
					Name:  "test-replication",
					State: "available",
					HybridReplicationAttributes: &models.HybridReplicationParameters{
						ReplicationType: rt.input,
					},
				}

				result := MapReplicationBetaToReplicationHydrateObject(replication)

				assert.Equal(tt, "test-replication", result.ResourceId)
				assert.NotNil(tt, result.HybridReplicationType)
				assert.Equal(tt, rt.expected, *result.HybridReplicationType)
			})
		}
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
		Volume: &models.Volume{DisplayName: "123"},
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
	var hybridReplicationType models.HybridReplicationParametersReplicationType
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
		hydrateReplicationStateAndType = func(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, hybridReplicationType models.HybridReplicationParametersReplicationType, token string) error {
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
		hydrateReplicationStateAndType = func(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, hybridReplicationType models.HybridReplicationParametersReplicationType, token string) error {
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
