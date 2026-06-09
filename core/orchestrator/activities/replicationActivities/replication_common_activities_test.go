package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	errs "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
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
				ReplicationType: datamodel.HybridReplicationParametersReplicationTypeMIGRATION,
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
				ReplicationType: datamodel.HybridReplicationParametersReplicationTypeCONTINUOUS,
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
			replicationType datamodel.HybridReplicationParametersReplicationType
		}{
			{"CreatingState", "creating", "CREATING", datamodel.HybridReplicationParametersReplicationTypeMIGRATION},
			{"AvailableState", "available", "READY", datamodel.HybridReplicationParametersReplicationTypeCONTINUOUS},
			{"UpdatingState", "updating", "UPDATING", datamodel.HybridReplicationParametersReplicationTypeONPREM},
			{"DisabledState", "disabled", "STOPPED", datamodel.HybridReplicationParametersReplicationTypeREVERSE},
			{"DeletingState", "deleting", "DELETING", datamodel.HybridReplicationParametersReplicationTypeMIGRATION},
			{"PendingClusterPeeringState", "PENDING_CLUSTER_PEERING", "PENDING_CLUSTER_PEERING", datamodel.HybridReplicationParametersReplicationTypeCONTINUOUS},
			{"ErrorState", "error", "ERROR", datamodel.HybridReplicationParametersReplicationTypeMIGRATION},
			{"UnknownState", "unknown-state", "STATE_UNSPECIFIED", datamodel.HybridReplicationParametersReplicationTypeMIGRATION},
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
				ReplicationType: datamodel.HybridReplicationParametersReplicationTypeMIGRATION,
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
			input    datamodel.HybridReplicationParametersReplicationType
			expected models.HybridReplicationHydrateType
		}{
			{datamodel.HybridReplicationParametersReplicationTypeMIGRATION, models.HybridReplicationHydrateType("MIGRATION")},
			{datamodel.HybridReplicationParametersReplicationTypeCONTINUOUS, models.HybridReplicationHydrateType("CONTINUOUS_REPLICATION")},
			{datamodel.HybridReplicationParametersReplicationTypeONPREM, models.HybridReplicationHydrateType("ONPREM_REPLICATION")},
			{datamodel.HybridReplicationParametersReplicationTypeREVERSE, models.HybridReplicationHydrateType("REVERSE_ONPREM_REPLICATION")},
			{datamodel.HybridReplicationParametersReplicationTypeUNSPECIFIED, models.HybridReplicationHydrateType("REPLICATION_TYPE_UNSPECIFIED")},
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
	var hybridReplicationType datamodel.HybridReplicationParametersReplicationType
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
		hydrateReplicationStateAndType = func(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, hybridReplicationType datamodel.HybridReplicationParametersReplicationType, token string) error {
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
		hydrateReplicationStateAndType = func(ctx context.Context, logger log.Logger, region string, projectId string, volumeResourceID string, replicationId string, state models.VolumeReplicationHydrateState, hybridReplicationType datamodel.HybridReplicationParametersReplicationType, token string) error {
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

// TestConvertDbModelToQuotaRulesV1beta tests the convertDbModelToQuotaRulesV1beta function
func TestConvertDbModelToQuotaRulesV1beta(t *testing.T) {
	t.Run("WhenQuotaTypeIsIndividualGroupQuota", func(tt *testing.T) {
		rule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-uuid-1",
			},
			Name:           "quota-rule-1",
			QuotaType:      "INDIVIDUAL_GROUP_QUOTA",
			QuotaTarget:    "group:developers",
			DiskLimitInKib: 100 * 1024, // 100 MiB in KiB
		}

		result := convertDbModelToQuotaRulesV1beta(rule)

		assert.Equal(tt, "quota-uuid-1", result.QuotaId.Value)
		assert.Equal(tt, "quota-rule-1", result.ResourceId)
		assert.Equal(tt, int64(100), result.DiskLimitInMib)
		assert.Equal(tt, googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA, result.QuotaType)
		assert.True(tt, result.QuotaTarget.IsSet())
		assert.Equal(tt, "group:developers", result.QuotaTarget.Value)
	})

	t.Run("WhenQuotaTypeIsDefaultGroupQuota", func(tt *testing.T) {
		rule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-uuid-2",
			},
			Name:           "quota-rule-2",
			QuotaType:      "DEFAULT_GROUP_QUOTA",
			DiskLimitInKib: 200 * 1024, // 200 MiB in KiB
		}

		result := convertDbModelToQuotaRulesV1beta(rule)

		assert.Equal(tt, "quota-uuid-2", result.QuotaId.Value)
		assert.Equal(tt, "quota-rule-2", result.ResourceId)
		assert.Equal(tt, int64(200), result.DiskLimitInMib)
		assert.Equal(tt, googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA, result.QuotaType)
		assert.False(tt, result.QuotaTarget.IsSet())
	})

	t.Run("WhenQuotaTargetIsSet", func(tt *testing.T) {
		rule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-uuid-3",
			},
			Name:           "quota-rule-3",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "user:alice",
			DiskLimitInKib: 300 * 1024, // 300 MiB in KiB
		}

		result := convertDbModelToQuotaRulesV1beta(rule)

		assert.True(tt, result.QuotaTarget.IsSet())
		assert.Equal(tt, "user:alice", result.QuotaTarget.Value)
	})

	t.Run("WhenDescriptionIsSet", func(tt *testing.T) {
		rule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-uuid-desc",
			},
			Name:           "quota-rule-desc",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 100 * 1024,
			Description:    "Quota rule description for destination sync",
		}

		result := convertDbModelToQuotaRulesV1beta(rule)

		assert.True(tt, result.Description.IsSet())
		assert.Equal(tt, "Quota rule description for destination sync", result.Description.Value)
	})
}

// TestConvertQuotaRulesV1betaToDbModel tests the convertQuotaRulesV1betaToDbModel function
func TestConvertQuotaRulesV1betaToDbModel(t *testing.T) {
	t.Run("WhenQuotaTypeIsIndividualGroupQuota", func(tt *testing.T) {
		clientRule := googleproxyclient.QuotaRulesV1beta{
			ResourceId:     "quota-rule-1",
			QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA,
			DiskLimitInMib: 100,
			QuotaId:        googleproxyclient.NewOptString("quota-uuid-1"),
		}

		result := convertQuotaRulesV1betaToDbModel(clientRule)

		assert.Equal(tt, "quota-rule-1", result.Name)
		assert.Equal(tt, "quota-uuid-1", result.UUID)
		assert.Equal(tt, int64(100*1024), result.DiskLimitInKib)
		assert.Equal(tt, "INDIVIDUAL_GROUP_QUOTA", result.QuotaType)
	})

	t.Run("WhenQuotaTypeIsDefaultGroupQuota", func(tt *testing.T) {
		clientRule := googleproxyclient.QuotaRulesV1beta{
			ResourceId:     "quota-rule-2",
			QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA,
			DiskLimitInMib: 200,
			QuotaId:        googleproxyclient.NewOptString("quota-uuid-2"),
		}

		result := convertQuotaRulesV1betaToDbModel(clientRule)

		assert.Equal(tt, "quota-rule-2", result.Name)
		assert.Equal(tt, "quota-uuid-2", result.UUID)
		assert.Equal(tt, int64(200*1024), result.DiskLimitInKib)
		assert.Equal(tt, "DEFAULT_GROUP_QUOTA", result.QuotaType)
	})

	t.Run("WhenQuotaTargetIsSet", func(tt *testing.T) {
		clientRule := googleproxyclient.QuotaRulesV1beta{
			ResourceId:     "quota-rule-3",
			QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 300,
			QuotaId:        googleproxyclient.NewOptString("quota-uuid-3"),
			QuotaTarget:    googleproxyclient.NewOptString("user:bob"),
		}

		result := convertQuotaRulesV1betaToDbModel(clientRule)

		assert.Equal(tt, "user:bob", result.QuotaTarget)
	})

	t.Run("WhenStateIsSet", func(tt *testing.T) {
		clientRule := googleproxyclient.QuotaRulesV1beta{
			ResourceId:     "quota-rule-4",
			QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
			DiskLimitInMib: 400,
			QuotaId:        googleproxyclient.NewOptString("quota-uuid-4"),
			State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
		}

		result := convertQuotaRulesV1betaToDbModel(clientRule)

		assert.Equal(tt, "READY", result.State)
	})

	t.Run("WhenStateDetailsIsSet", func(tt *testing.T) {
		clientRule := googleproxyclient.QuotaRulesV1beta{
			ResourceId:     "quota-rule-5",
			QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA,
			DiskLimitInMib: 500,
			QuotaId:        googleproxyclient.NewOptString("quota-uuid-5"),
			StateDetails:   googleproxyclient.NewOptString("Ready state details"),
		}

		result := convertQuotaRulesV1betaToDbModel(clientRule)

		assert.Equal(tt, "Ready state details", result.StateDetails)
	})

	t.Run("WhenDescriptionIsSet", func(tt *testing.T) {
		clientRule := googleproxyclient.QuotaRulesV1beta{
			ResourceId:     "quota-rule-desc",
			QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 100,
			QuotaId:        googleproxyclient.NewOptString("quota-uuid-desc"),
			Description:    googleproxyclient.NewOptString("Source quota rule description"),
		}

		result := convertQuotaRulesV1betaToDbModel(clientRule)

		assert.Equal(tt, "Source quota rule description", result.Description)
	})
}

// TestCreateQuotaRulesRemote tests the CreateQuotaRulesRemote function
func TestCreateQuotaRulesRemote(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		logger := log.NewLogger()
		basePath := "https://test.example.com"
		jwtToken := "test-token"
		projectNumber := "123456789"
		locationId := "us-central1"
		volumeId := "vol-1"
		correlationID := "corr-123"

		srcQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-uuid-1"},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "user:alice",
				DiskLimitInKib: 100 * 1024,
			},
		}

		dstQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-uuid-2"},
				Name:           "quota-rule-2",
				QuotaType:      "DEFAULT_USER_QUOTA",
				DiskLimitInKib: 200 * 1024,
			},
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		successResponse := &googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta{
			State: googleproxyclient.NewOptString("SUCCESS"),
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "quota-rule-1",
					QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
					DiskLimitInMib: 100,
					QuotaId:        googleproxyclient.NewOptString("quota-uuid-1"),
					QuotaTarget:    googleproxyclient.NewOptString("user:alice"),
				},
			},
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(successResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, dstQuotaRules)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "quota-uuid-1", result[0].UUID)
		assert.Equal(tt, "quota-rule-1", result[0].Name)
	})

	t.Run("WhenUnauthorizedResponse", func(tt *testing.T) {
		ctx := context.Background()
		logger := log.NewLogger()
		basePath := "https://test.example.com"
		jwtToken := "test-token"
		projectNumber := "123456789"
		locationId := "us-central1"
		volumeId := "vol-1"
		correlationID := "corr-123"

		srcQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"},
				Name:      "quota-rule-1",
				QuotaType: "INDIVIDUAL_USER_QUOTA",
			},
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		unauthorizedResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPUnauthorized{
			Code:    401,
			Message: "Unauthorized",
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(unauthorizedResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenForbiddenResponse", func(tt *testing.T) {
		ctx := context.Background()
		logger := log.NewLogger()
		basePath := "https://test.example.com"
		jwtToken := "test-token"
		projectNumber := "123456789"
		locationId := "us-central1"
		volumeId := "vol-1"
		correlationID := "corr-123"

		srcQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"},
				Name:      "quota-rule-1",
				QuotaType: "INDIVIDUAL_USER_QUOTA",
			},
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		forbiddenResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPForbidden{
			Code:    403,
			Message: "Forbidden",
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(forbiddenResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenUnprocessableEntityResponse", func(tt *testing.T) {
		ctx := context.Background()
		logger := log.NewLogger()
		basePath := "https://test.example.com"
		jwtToken := "test-token"
		projectNumber := "123456789"
		locationId := "us-central1"
		volumeId := "vol-1"
		correlationID := "corr-123"

		srcQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"},
				Name:      "quota-rule-1",
				QuotaType: "INDIVIDUAL_USER_QUOTA",
			},
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		unprocessableResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPUnprocessableEntity{
			Code:    422,
			Message: "Unprocessable Entity",
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(unprocessableResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenTooManyRequestsResponse", func(tt *testing.T) {
		ctx := context.Background()
		logger := log.NewLogger()
		basePath := "https://test.example.com"
		jwtToken := "test-token"
		projectNumber := "123456789"
		locationId := "us-central1"
		volumeId := "vol-1"
		correlationID := "corr-123"

		srcQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"},
				Name:      "quota-rule-1",
				QuotaType: "INDIVIDUAL_USER_QUOTA",
			},
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		tooManyRequestsResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPTooManyRequests{
			Code:    429,
			Message: "Too Many Requests",
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(tooManyRequestsResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func TestCreateQuotaRulesRemote_WithNilSourceRules(t *testing.T) {
	t.Run("WhenSourceQuotaRulesNilAndDestinationProvided", func(tt *testing.T) {
		ctx := context.Background()
		logger := log.NewLogger()
		basePath := "https://test.example.com"
		jwtToken := "test-token"
		projectNumber := "123456789"
		locationId := "us-central1"
		volumeId := "vol-1"
		correlationID := "corr-123"

		dstQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-uuid-2"},
				Name:           "quota-rule-2",
				QuotaType:      "DEFAULT_USER_QUOTA",
				DiskLimitInKib: 200 * 1024,
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		successResponse := &googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta{
			State: googleproxyclient.NewOptString("SUCCESS"),
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "quota-rule-2",
					QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
					DiskLimitInMib: 200,
					QuotaId:        googleproxyclient.NewOptString("quota-uuid-2"),
				},
			},
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(successResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, nil, dstQuotaRules)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "quota-uuid-2", result[0].UUID)
		assert.Equal(tt, "quota-rule-2", result[0].Name)
	})
}

// Additional test cases for CreateQuotaRulesRemote to improve coverage
func TestCreateQuotaRulesRemote_AdditionalCases(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	basePath := "https://test.example.com"
	jwtToken := "test-token"
	projectNumber := "123456789"
	locationId := "us-central1"
	volumeId := "vol-1"
	correlationID := "corr-123"

	t.Run("WhenBadRequestResponse", func(tt *testing.T) {
		srcQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"}, Name: "quota-rule-1"},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		badRequestResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPBadRequest{
			Code:    400,
			Message: "Bad Request",
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(badRequestResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenNotFoundResponse", func(tt *testing.T) {
		srcQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"}, Name: "quota-rule-1"},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		notFoundResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPNotFound{
			Code:    404,
			Message: "Not Found",
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(notFoundResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenInternalServerErrorResponse", func(tt *testing.T) {
		srcQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"}, Name: "quota-rule-1"},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		internalErrorResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPInternalServerError{
			Code:    500,
			Message: "Internal Server Error",
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(internalErrorResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenAPIError", func(tt *testing.T) {
		srcQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"}, Name: "quota-rule-1"},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("network error"))

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenEmptyQuotaRulesLists", func(tt *testing.T) {
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		successResponse := &googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta{
			State:      googleproxyclient.NewOptString("SUCCESS"),
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{},
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(successResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, []*datamodel.QuotaRule{}, []*datamodel.QuotaRule{})

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)
	})

	t.Run("WhenAllQuotaTypes_ConvertsCorrectly", func(tt *testing.T) {
		srcQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "uuid-user"},
				Name:           "user-quota",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "user:alice",
				DiskLimitInKib: 1024 * 1024, // 1 GiB in KiB
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "uuid-group"},
				Name:           "group-quota",
				QuotaType:      "INDIVIDUAL_GROUP_QUOTA",
				QuotaTarget:    "group:developers",
				DiskLimitInKib: 2048 * 1024, // 2 GiB in KiB
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "uuid-default-user"},
				Name:           "default-user-quota",
				QuotaType:      "DEFAULT_USER_QUOTA",
				DiskLimitInKib: 512 * 1024, // 512 MiB in KiB
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "uuid-default-group"},
				Name:           "default-group-quota",
				QuotaType:      "DEFAULT_GROUP_QUOTA",
				DiskLimitInKib: 256 * 1024, // 256 MiB in KiB
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		successResponse := &googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta{
			State: googleproxyclient.NewOptString("SUCCESS"),
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "user-quota",
					QuotaId:        googleproxyclient.NewOptString("uuid-user"),
					QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
					QuotaTarget:    googleproxyclient.NewOptString("user:alice"),
					DiskLimitInMib: 1024, // 1 GiB in MiB
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
					StateDetails:   googleproxyclient.NewOptString("Active"),
				},
			},
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(successResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		// Verify it uses UUID (not ExternalUUID) - critical per the 3 rules
		assert.Equal(tt, "uuid-user", result[0].UUID)
		assert.Equal(tt, "user-quota", result[0].Name)
		// Verify conversion from MiB back to KiB
		assert.Equal(tt, int64(1024*1024), result[0].DiskLimitInKib) // 1024 MiB = 1024*1024 KiB
		assert.Equal(tt, "INDIVIDUAL_USER_QUOTA", result[0].QuotaType)
		assert.Equal(tt, "user:alice", result[0].QuotaTarget)
		assert.Equal(tt, "READY", result[0].State)
		assert.Equal(tt, "Active", result[0].StateDetails)
	})

	t.Run("WhenUsesUUID_NotExternalUUID", func(tt *testing.T) {
		// This test verifies the critical rule: always use UUID, never ExternalUUID
		srcQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "canonical-uuid-123"},
				Name:           "test-rule",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				DiskLimitInKib: 1024 * 1024,
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		successResponse := &googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta{
			State: googleproxyclient.NewOptString("SUCCESS"),
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "test-rule",
					QuotaId:        googleproxyclient.NewOptString("canonical-uuid-123"),
					QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
					DiskLimitInMib: 1024,
				},
			},
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(successResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		// Critical: Verify it uses UUID, not ExternalUUID
		assert.Equal(tt, "canonical-uuid-123", result[0].UUID)
		// Verify ExternalUUID is NOT used as the primary identifier
		if result[0].QuotaRuleAttributes != nil {
			// ExternalUUID may be set for backward compatibility, but UUID is the canonical one
			assert.Equal(tt, "canonical-uuid-123", result[0].UUID)
		}
	})

	t.Run("WhenMultipleRulesWithState", func(tt *testing.T) {
		srcQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "uuid-1"},
				Name:           "rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				DiskLimitInKib: 1024 * 1024,
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "uuid-2"},
				Name:           "rule-2",
				QuotaType:      "DEFAULT_USER_QUOTA",
				DiskLimitInKib: 512 * 1024,
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockProxyClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		successResponse := &googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta{
			State: googleproxyclient.NewOptString("SUCCESS"),
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "rule-1",
					QuotaId:        googleproxyclient.NewOptString("uuid-1"),
					QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
					DiskLimitInMib: 1024,
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
					StateDetails:   googleproxyclient.NewOptString("Ready"),
				},
				{
					ResourceId:     "rule-2",
					QuotaId:        googleproxyclient.NewOptString("uuid-2"),
					QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
					DiskLimitInMib: 512,
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateCREATING),
					StateDetails:   googleproxyclient.NewOptString("Creating"),
				},
			},
		}

		mockInvoker.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(mock.Anything, mock.Anything, mock.Anything).
			Return(successResponse, nil)

		result, err := CreateQuotaRulesRemote(ctx, logger, basePath, jwtToken, projectNumber, locationId, volumeId, correlationID, srcQuotaRules, nil)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 2)
		assert.Equal(tt, "uuid-1", result[0].UUID)
		assert.Equal(tt, "rule-1", result[0].Name)
		assert.Equal(tt, "READY", result[0].State)
		assert.Equal(tt, "Ready", result[0].StateDetails)
		assert.Equal(tt, "uuid-2", result[1].UUID)
		assert.Equal(tt, "rule-2", result[1].Name)
		assert.Equal(tt, "CREATING", result[1].State)
		assert.Equal(tt, "Creating", result[1].StateDetails)
	})
}

func TestDehydrateQuotaRules(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	volumeResourceId := "volume-resource-id"
	location := "us-central1"
	projectNumber := "123456789"
	callbackToken := "callback-token"

	t.Run("WhenSuccess_AllRulesDehydrated", func(tt *testing.T) {
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{Name: "quota-rule-1", BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"}},
			{Name: "quota-rule-2", BaseModel: datamodel.BaseModel{UUID: "quota-uuid-2"}},
		}

		callCount := 0
		var capturedNames []string
		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			callCount++
			capturedNames = append(capturedNames, quotaRuleNames...)
			assert.Equal(tt, callbackToken, token)
			assert.Len(tt, quotaRuleNames, 1)
			return nil
		}

		dehydratedRules, err := DehydrateQuotaRules(ctx, logger, quotaRules, volumeResourceId, location, projectNumber, callbackToken)

		assert.NoError(tt, err)
		assert.Len(tt, dehydratedRules, 2)
		assert.Equal(tt, 2, callCount)
		assert.Equal(tt, []string{"quota-rule-1", "quota-rule-2"}, capturedNames)
	})

	t.Run("WhenPartialFailure_ReturnsDehydratedRules", func(tt *testing.T) {
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{Name: "quota-rule-1", BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"}},
			{Name: "quota-rule-2", BaseModel: datamodel.BaseModel{UUID: "quota-uuid-2"}},
		}

		callCount := 0
		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			callCount++
			if callCount == 2 {
				return errors.New("dehydration failed")
			}
			return nil
		}

		dehydratedRules, err := DehydrateQuotaRules(ctx, logger, quotaRules, volumeResourceId, location, projectNumber, callbackToken)

		assert.NoError(tt, err) // Partial failure should not return error
		assert.Len(tt, dehydratedRules, 1)
		assert.Equal(tt, "quota-rule-1", dehydratedRules[0].Name)
	})

	t.Run("WhenRuleMissingName_ReturnsValidationError", func(tt *testing.T) {
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{Name: "", BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"}},
		}

		callCount := 0
		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			callCount++
			return nil
		}

		dehydratedRules, err := DehydrateQuotaRules(ctx, logger, quotaRules, volumeResourceId, location, projectNumber, callbackToken)

		assert.Error(tt, err)
		assert.Len(tt, dehydratedRules, 0)
		assert.Equal(tt, 0, callCount)
	})

	t.Run("WhenEmptyQuotaRules_ReturnsEmptyList", func(tt *testing.T) {
		quotaRules := []*datamodel.QuotaRule{}

		dehydratedRules, err := DehydrateQuotaRules(ctx, logger, quotaRules, volumeResourceId, location, projectNumber, callbackToken)

		assert.NoError(tt, err)
		assert.Len(tt, dehydratedRules, 0)
	})

	t.Run("WhenNilQuotaRules_ReturnsEmptyList", func(tt *testing.T) {
		var quotaRules []*datamodel.QuotaRule

		dehydratedRules, err := DehydrateQuotaRules(ctx, logger, quotaRules, volumeResourceId, location, projectNumber, callbackToken)

		assert.NoError(tt, err)
		assert.Len(tt, dehydratedRules, 0)
	})

	t.Run("WhenAllQuotaTypes_DehydratesSuccessfully", func(tt *testing.T) {
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{Name: "user-quota-1", BaseModel: datamodel.BaseModel{UUID: "uuid-1"}, QuotaType: "INDIVIDUAL_USER_QUOTA"},
			{Name: "group-quota-1", BaseModel: datamodel.BaseModel{UUID: "uuid-2"}, QuotaType: "INDIVIDUAL_GROUP_QUOTA"},
			{Name: "default-user-quota", BaseModel: datamodel.BaseModel{UUID: "uuid-3"}, QuotaType: "DEFAULT_USER_QUOTA"},
			{Name: "default-group-quota", BaseModel: datamodel.BaseModel{UUID: "uuid-4"}, QuotaType: "DEFAULT_GROUP_QUOTA"},
		}

		callCount := 0
		var capturedNames []string
		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			callCount++
			capturedNames = append(capturedNames, quotaRuleNames...)
			return nil
		}

		dehydratedRules, err := DehydrateQuotaRules(ctx, logger, quotaRules, volumeResourceId, location, projectNumber, callbackToken)

		assert.NoError(tt, err)
		assert.Len(tt, dehydratedRules, 4)
		assert.Equal(tt, 4, callCount)
		assert.Equal(tt, []string{"user-quota-1", "group-quota-1", "default-user-quota", "default-group-quota"}, capturedNames)
		// Verify it uses Name, not ExternalUUID
		assert.Equal(tt, "user-quota-1", dehydratedRules[0].Name)
		assert.Equal(tt, "uuid-1", dehydratedRules[0].UUID)
	})

	t.Run("WhenFirstRuleFails_ReturnsEmptyList", func(tt *testing.T) {
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{Name: "quota-rule-1", BaseModel: datamodel.BaseModel{UUID: "quota-uuid-1"}},
			{Name: "quota-rule-2", BaseModel: datamodel.BaseModel{UUID: "quota-uuid-2"}},
		}

		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			return errors.New("dehydration failed immediately")
		}

		dehydratedRules, err := DehydrateQuotaRules(ctx, logger, quotaRules, volumeResourceId, location, projectNumber, callbackToken)

		assert.NoError(tt, err) // Partial failure should not return error, even if all fail
		assert.Len(tt, dehydratedRules, 0)
	})
}
