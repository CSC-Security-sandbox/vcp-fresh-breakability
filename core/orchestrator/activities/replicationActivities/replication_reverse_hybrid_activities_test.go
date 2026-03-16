package replicationActivities

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestReverseHybridReplicationActivity_SetHybridReplicationVariablesReverse(t *testing.T) {
	ctx := context.Background()
	activity := ReverseHybridReplicationActivity{}

	t.Run("WhenDbVolReplicationIsNil", func(tt *testing.T) {
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: nil,
		}

		updatedResult, err := activity.SetHybridReplicationVariablesReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.False(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenHybridReplicationAttributesIsNil", func(tt *testing.T) {
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: nil,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.False(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenHybridReplicationAttributesIsSetButNotReverse", func(tt *testing.T) {
		migrationType := string(models.HybridReplicationParametersReplicationTypeMIGRATION)
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: &migrationType,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenIsSrcForHybridReplicationIsTrue", func(tt *testing.T) {
		reverseType := string(models.HybridReplicationParametersReplicationTypeREVERSE)
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: &reverseType,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "customer",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.True(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenHybridReplicationTypeIsReverseButDestinationLocationIsNotEmpty", func(tt *testing.T) {
		reverseType := string(models.HybridReplicationParametersReplicationTypeREVERSE)
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: &reverseType,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenHybridReplicationTypeIsNil", func(tt *testing.T) {
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: nil,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})
}

func TestReverseHybridReplicationActivity_SetReplicationToErrorForReverseHybrid(t *testing.T) {
	ctx := context.Background()

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		err := activity.SetReplicationToErrorForReverseHybrid(ctx, nil, "some error", false)

		assert.NoError(tt, err)
		mockStorage.AssertNotCalled(tt, "UpdateVolumeReplication")
	})

	t.Run("WhenIsSrcForHybridReplicationTrue_SetsExternalManagedAndReverse", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "rep-uuid"},
			Name:      "test-replication",
		}

		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
			return r.State == models.LifeCycleStateError &&
				r.StateDetails != "" &&
				r.HybridReplicationAttributes != nil &&
				r.HybridReplicationAttributes.Status == models.HybridReplicationStatusExternalManaged &&
				r.HybridReplicationAttributes.HybridReplicationUserCommands == nil &&
				r.HybridReplicationAttributes.HybridReplicationType != nil &&
				*r.HybridReplicationAttributes.HybridReplicationType == string(models.HybridReplicationParametersReplicationTypeREVERSE)
		})).Return(nil)

		err := activity.SetReplicationToErrorForReverseHybrid(ctx, replication, "test error", true)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenIsSrcForHybridReplicationFalse_SetsPeeredAndOnprem", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "rep-uuid"},
			Name:      "test-replication",
		}

		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
			return r.State == models.LifeCycleStateError &&
				r.StateDetails != "" &&
				r.HybridReplicationAttributes != nil &&
				r.HybridReplicationAttributes.Status == models.HybridReplicationStatusPeered &&
				r.HybridReplicationAttributes.HybridReplicationUserCommands == nil &&
				r.HybridReplicationAttributes.HybridReplicationType != nil &&
				*r.HybridReplicationAttributes.HybridReplicationType == string(models.HybridReplicationParametersReplicationTypeONPREM)
		})).Return(nil)

		err := activity.SetReplicationToErrorForReverseHybrid(ctx, replication, "test error", false)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeReplicationFails_ReturnsError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "rep-uuid"},
			Name:      "test-replication",
		}

		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(errors.New("db error"))

		err := activity.SetReplicationToErrorForReverseHybrid(ctx, replication, "test error", false)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "db error")
		mockStorage.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_CheckClusterPeerHealthForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		ontapPeerUUID := "test-ontap-peer-uuid"
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ClusterPeer: &datamodel.ClusterPeerings{
					OntapPeerUUID: ontapPeerUUID,
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		expectedClusterPeer := &vsa.ClusterPeer{
			ExternalUUID:        ontapPeerUUID,
			Availability:        models.AvailabilityAvailable,
			AuthenticationState: models.AuthenticationStateOk,
		}

		mockProvider.On("GetClusterPeer", ontapPeerUUID).Return(expectedClusterPeer, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.CheckClusterPeerHealthForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, expectedClusterPeer, updatedResult.ClusterPeer)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: nil,
		}

		updatedResult, err := activity.CheckClusterPeerHealthForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenClusterPeerIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ClusterPeer: nil,
			},
		}

		updatedResult, err := activity.CheckClusterPeerHealthForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenOntapPeerUUIDIsEmpty", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ClusterPeer: &datamodel.ClusterPeerings{
					OntapPeerUUID: "",
				},
			},
		}

		updatedResult, err := activity.CheckClusterPeerHealthForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ClusterPeer: &datamodel.ClusterPeerings{
					OntapPeerUUID: "test-uuid",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("provider error")
		}

		updatedResult, err := activity.CheckClusterPeerHealthForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetClusterPeerFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		ontapPeerUUID := "test-ontap-peer-uuid"
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ClusterPeer: &datamodel.ClusterPeerings{
					OntapPeerUUID: ontapPeerUUID,
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetClusterPeer", ontapPeerUUID).Return(nil, assert.AnError)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.CheckClusterPeerHealthForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenClusterPeerNotAvailable", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		ontapPeerUUID := "test-ontap-peer-uuid"
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ClusterPeer: &datamodel.ClusterPeerings{
					OntapPeerUUID: ontapPeerUUID,
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		unavailableClusterPeer := &vsa.ClusterPeer{
			ExternalUUID:        ontapPeerUUID,
			Availability:        models.AvailabilityUnavailable,
			AuthenticationState: models.AuthenticationStateOk,
		}

		mockProvider.On("GetClusterPeer", ontapPeerUUID).Return(unavailableClusterPeer, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.CheckClusterPeerHealthForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_UpdateRbacRoleForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess_CreateNewPrivilege", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationSvmName:    "dst-svm",
					DestinationVolumeName: "dst-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		targetRole := &vsa.Role{
			OwnerID:    "owner-id",
			Privileges: []*vsa.RolePrivilege{},
		}

		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return([]*vsa.Role{targetRole}, nil)

		mockProvider.On("CreateRolePrivilege", mock.MatchedBy(func(params vsa.CreateRolePrivilegeParams) bool {
			return params.Name == onPremPeerRole &&
				params.Path == "snapmirror resync" &&
				params.Access == "readonly" &&
				params.Query == "-source-path dst-svm:dst-volume"
		})).Return("privilege-location", nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_ModifyExistingPrivilege", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationSvmName:    "dst-svm",
					DestinationVolumeName: "dst-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  "snapmirror resync",
			Query: "-source-path existing-svm:existing-volume",
		}
		targetRole := &vsa.Role{
			OwnerID: "owner-id",
			Privileges: []*vsa.RolePrivilege{
				existingPrivilege,
			},
		}

		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return([]*vsa.Role{targetRole}, nil)

		mockProvider.On("ModifyRolePrivilege", mock.MatchedBy(func(params vsa.ModifyRolePrivilegeParams) bool {
			return params.Name == onPremPeerRole &&
				params.Path == "snapmirror resync" &&
				params.Access == "readonly" &&
				params.Query == "-source-path existing-svm:existing-volume|dst-svm:dst-volume"
		})).Return(nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_PathAlreadyExists", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationSvmName:    "dst-svm",
					DestinationVolumeName: "dst-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  "snapmirror resync",
			Query: "-source-path dst-svm:dst-volume",
		}
		targetRole := &vsa.Role{
			OwnerID: "owner-id",
			Privileges: []*vsa.RolePrivilege{
				existingPrivilege,
			},
		}

		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return([]*vsa.Role{targetRole}, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: nil,
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenReplicationAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: nil,
			},
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:    "source-svm",
					SourceVolumeName: "source-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("provider error")
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetRoleCollectionFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:    "source-svm",
					SourceVolumeName: "source-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return(nil, assert.AnError)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenRoleNotFound", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:    "source-svm",
					SourceVolumeName: "source-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return([]*vsa.Role{}, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenModifyRolePrivilegeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:    "source-svm",
					SourceVolumeName: "source-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  "snapmirror resync",
			Query: "-source-path existing-svm:existing-volume",
		}
		targetRole := &vsa.Role{
			OwnerID: "owner-id",
			Privileges: []*vsa.RolePrivilege{
				existingPrivilege,
			},
		}

		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return([]*vsa.Role{targetRole}, nil)

		mockProvider.On("ModifyRolePrivilege", mock.Anything).Return(assert.AnError)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenCreateRolePrivilegeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:    "source-svm",
					SourceVolumeName: "source-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		targetRole := &vsa.Role{
			OwnerID:    "owner-id",
			Privileges: []*vsa.RolePrivilege{},
		}

		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return([]*vsa.Role{targetRole}, nil)

		mockProvider.On("CreateRolePrivilege", mock.Anything).Return("", assert.AnError)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRoleForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_GenerateReverseCommandsForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess_10Minutely", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationSchedule:   vsa.VolumeReplicationSchedule10Minutely,
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-volume",
				},
			},
		}

		updatedResult, err := activity.GenerateReverseCommandsForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.NotNil(tt, updatedResult.HybridReplicationUserCommands)
		assert.Len(tt, updatedResult.HybridReplicationUserCommands, 3)
		assert.Contains(tt, updatedResult.HybridReplicationUserCommands[0], "job schedule cron create")
		assert.Contains(tt, updatedResult.HybridReplicationUserCommands[0], "-minute \"0,10,20,30,40,50\"")
		assert.Contains(tt, updatedResult.HybridReplicationUserCommands[1], "snapmirror resync")
		assert.Contains(tt, updatedResult.HybridReplicationUserCommands[1], "source-svm:source-volume")
		assert.Contains(tt, updatedResult.HybridReplicationUserCommands[1], "dest-svm:dest-volume")
		assert.Contains(tt, updatedResult.HybridReplicationUserCommands[2], "snapmirror modify")
	})

	t.Run("WhenSuccess_Hourly", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationSchedule:   vsa.VolumeReplicationScheduleHourly,
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-volume",
				},
			},
		}

		updatedResult, err := activity.GenerateReverseCommandsForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.NotNil(tt, updatedResult.HybridReplicationUserCommands)
		assert.Len(tt, updatedResult.HybridReplicationUserCommands, 3)
		assert.Contains(tt, updatedResult.HybridReplicationUserCommands[0], "-hour all -minute 0")
	})

	t.Run("WhenSuccess_Daily", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationSchedule:   vsa.VolumeReplicationScheduleDaily,
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-volume",
				},
			},
		}

		updatedResult, err := activity.GenerateReverseCommandsForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.NotNil(tt, updatedResult.HybridReplicationUserCommands)
		assert.Len(tt, updatedResult.HybridReplicationUserCommands, 3)
		assert.Contains(tt, updatedResult.HybridReplicationUserCommands[0], "-dayofweek all -hour 0 -minute 0")
	})

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: nil,
		}

		updatedResult, err := activity.GenerateReverseCommandsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenReplicationAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: nil,
			},
		}

		updatedResult, err := activity.GenerateReverseCommandsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenInvalidSchedule", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationSchedule:   "invalid-schedule",
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-volume",
				},
			},
		}

		updatedResult, err := activity.GenerateReverseCommandsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestReverseHybridReplicationActivity_UpdateReplicationWithReverseCommandsForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		commands := []string{"command1", "command2", "command3"}
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{
					UUID: "test-replication-uuid",
				},
				HybridReplicationAttributes: nil,
			},
			HybridReplicationUserCommands: commands,
		}

		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(repl *datamodel.VolumeReplication) bool {
			return repl.UUID == "test-replication-uuid" &&
				repl.HybridReplicationAttributes != nil &&
				repl.HybridReplicationAttributes.Status == models.HybridReplicationStatusPendingRemoteResync &&
				repl.HybridReplicationAttributes.StatusDetails == "Please execute the SnapMirror commands on the on-premises system to establish a new SnapMirror relationship."
		})).Return(nil)

		updatedResult, err := activity.UpdateReplicationWithReverseCommandsForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		commands := []string{"command1", "command2"}
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{
					UUID: "test-replication-uuid",
				},
			},
			HybridReplicationUserCommands: commands,
		}

		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(assert.AnError)

		updatedResult, err := activity.UpdateReplicationWithReverseCommandsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_ListSnapmirrorDestinationsForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		expectedDestinations := []*vsa.SnapmirrorDestination{
			{
				SourcePath:       "dest-svm:dest-volume",
				DestinationPath:  "source-svm:source-volume",
				RelationshipUUID: "rel-uuid-123",
			},
		}

		mockProvider.On("ListSnapmirrorDestinations", mock.MatchedBy(func(params *ontapRest.SnapmirrorRelationshipListDestinationsParams) bool {
			return params != nil &&
				params.SourcePath != nil &&
				*params.SourcePath == "dest-svm:dest-volume" &&
				params.DestinationPath != nil &&
				*params.DestinationPath == "source-svm:source-volume"
		})).Return(expectedDestinations, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.ListSnapmirrorDestinationsForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenNodeProviderIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			NodeProvider: nil,
		}

		updatedResult, err := activity.ListSnapmirrorDestinationsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: nil,
			NodeProvider:     &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.ListSnapmirrorDestinationsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("provider error")
		}

		updatedResult, err := activity.ListSnapmirrorDestinationsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenListSnapmirrorDestinationsFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		mockProvider.On("ListSnapmirrorDestinations", mock.Anything).Return(nil, assert.AnError)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.ListSnapmirrorDestinationsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenNoDestinationsFound", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-volume",
				},
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		mockProvider := &vsa.MockProvider{}
		mockProvider.On("ListSnapmirrorDestinations", mock.Anything).Return([]*vsa.SnapmirrorDestination{}, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.ListSnapmirrorDestinationsForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_UpdateReplicationStateForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{
					UUID: "test-replication-uuid",
				},
			},
		}

		updatedReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-replication-uuid",
			},
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
		}

		mockStorage.On("GetVolumeReplication", ctx, "test-replication-uuid").Return(updatedReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(repl *datamodel.VolumeReplication) bool {
			return repl.UUID == "test-replication-uuid" &&
				repl.HybridReplicationAttributes != nil &&
				repl.HybridReplicationAttributes.Status == models.HybridReplicationStatusExternalManaged &&
				repl.HybridReplicationAttributes.StatusDetails == "Replication is being externally managed by the On-Prem cluster" &&
				repl.HybridReplicationAttributes.HybridReplicationUserCommands == nil
		})).Return(nil)

		updatedResult, err := activity.UpdateReplicationStateForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: nil,
		}

		updatedResult, err := activity.UpdateReplicationStateForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{
					UUID: "test-replication-uuid",
				},
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, "test-replication-uuid").Return(nil, assert.AnError)

		updatedResult, err := activity.UpdateReplicationStateForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{
					UUID: "test-replication-uuid",
				},
			},
		}

		updatedReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-replication-uuid",
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, "test-replication-uuid").Return(updatedReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(assert.AnError)

		updatedResult, err := activity.UpdateReplicationStateForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_CreateJobForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstProjectNumber := "test-project"
		result := &replication.ReverseHybridReplicationResult{
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{
					UUID: "test-replication-uuid",
					ID:   123,
				},
				AccountID: 456,
				Name:      "test-replication",
				Volume: &datamodel.Volume{
					Name: "test-volume",
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		expectedJob := &datamodel.Job{
			AccountID:     sql.NullInt64{Int64: 456, Valid: true},
			Type:          string(models.JobTypeReverseHybridReplicationInternal),
			State:         string(models.JobsStateNEW),
			ResourceName:  fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s", dstProjectNumber, "us-central1", "test-volume", "test-replication"),
			JobAttributes: &datamodel.JobAttributes{ResourceUUID: "test-replication-uuid"},
		}

		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.AccountID.Int64 == 456 &&
				job.Type == string(models.JobTypeReverseHybridReplicationInternal) &&
				job.State == string(models.JobsStateNEW) &&
				job.JobAttributes.ResourceUUID == "test-replication-uuid"
		})).Return(expectedJob, nil)

		createdJob, err := activity.CreateJobForHybridReverse(ctx, result, string(models.JobTypeReverseHybridReplicationInternal))

		assert.NoError(tt, err)
		assert.Equal(tt, expectedJob, createdJob)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: nil,
		}

		createdJob, err := activity.CreateJobForHybridReverse(ctx, result, string(models.JobTypeReverseHybridReplicationInternal))

		assert.Error(tt, err)
		assert.Nil(tt, createdJob)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstProjectNumber := "test-project"
		result := &replication.ReverseHybridReplicationResult{
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{
					UUID: "test-replication-uuid",
					ID:   123,
				},
				AccountID: 456,
				Name:      "test-replication",
				Volume: &datamodel.Volume{
					Name: "test-volume",
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, assert.AnError)

		createdJob, err := activity.CreateJobForHybridReverse(ctx, result, string(models.JobTypeReverseHybridReplicationInternal))

		assert.Error(tt, err)
		assert.Nil(tt, createdJob)
		mockStorage.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_GetNodeProviderForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		poolID := int64(123)
		expectedNodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "node-uuid-1"},
				Name:      "node-1",
			},
		}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					PoolID: poolID,
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{ID: poolID},
						DeploymentName: "test-deployment",
						PoolCredentials: &datamodel.PoolCredentials{
							Password: "test-password",
						},
					},
				},
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolID).Return(expectedNodes, nil)

		updatedResult, err := activity.GetNodeProviderForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.NotNil(tt, updatedResult.NodeProvider)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetNodesFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		poolID := int64(123)
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					PoolID: poolID,
					Pool: &datamodel.Pool{
						BaseModel: datamodel.BaseModel{ID: poolID},
					},
				},
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolID).Return(nil, assert.AnError)

		updatedResult, err := activity.GetNodeProviderForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenNoNodesFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		poolID := int64(123)
		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					PoolID: poolID,
					Pool: &datamodel.Pool{
						BaseModel: datamodel.BaseModel{ID: poolID},
					},
				},
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolID).Return([]*datamodel.Node{}, nil)

		updatedResult, err := activity.GetNodeProviderForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenPoolCredentialsIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		poolID := int64(123)
		expectedNodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "node-uuid-1"},
				Name:      "node-1",
			},
		}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					PoolID: poolID,
					Pool: &datamodel.Pool{
						BaseModel:       datamodel.BaseModel{ID: poolID},
						DeploymentName:  "test-deployment",
						PoolCredentials: nil,
					},
				},
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolID).Return(expectedNodes, nil)

		updatedResult, err := activity.GetNodeProviderForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_GetDstBasePathForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		updatedResult, err := activity.GetDstBasePathForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})

	t.Run("WhenGetBasePathFails", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetDstBasePathForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestReverseHybridReplicationActivity_GetSignedDstTokenForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		originalGetSignedToken := replication.InternalUtilGetSignedToken
		defer func() {
			replication.InternalUtilGetSignedToken = originalGetSignedToken
		}()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstProjectNumber := "test-project"
		result := &replication.ReverseHybridReplicationResult{
			DstProjectNumber: &dstProjectNumber,
		}

		replication.InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}

		updatedResult, err := activity.GetSignedDstTokenForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "test-jwt-token", *updatedResult.DstJwtToken)
	})

	t.Run("WhenGetSignedTokenFails", func(tt *testing.T) {
		originalGetSignedToken := replication.InternalUtilGetSignedToken
		defer func() {
			replication.InternalUtilGetSignedToken = originalGetSignedToken
		}()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstProjectNumber := "test-project"
		result := &replication.ReverseHybridReplicationResult{
			DstProjectNumber: &dstProjectNumber,
		}

		replication.InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("failed to get signed token")
		}

		updatedResult, err := activity.GetSignedDstTokenForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestReverseHybridReplicationActivity_CleanupOldReplicationForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseHybridReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dest-location",
					DestinationReplicationUUID: "dest-replication-uuid",
				},
			},
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		resp := &googleproxyclient.VolumeReplicationInternalV1beta{
			Jobs: []googleproxyclient.JobV1beta{
				{
					JobId: googleproxyclient.OptString{
						Value: "cleanup-job-uuid-12345",
						Set:   true,
					},
				},
			},
		}

		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(resp, nil)

		updatedResult, err := activity.CleanupOldReplicationForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.NotNil(tt, updatedResult.JobId)
		assert.Equal(tt, "cleanup-job-uuid-12345", *updatedResult.JobId)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenDeleteFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseHybridReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dest-location",
					DestinationReplicationUUID: "dest-replication-uuid",
				},
			},
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(nil, errors.New("cleanup error"))

		updatedResult, err := activity.CleanupOldReplicationForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenBadRequest", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseHybridReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dest-location",
					DestinationReplicationUUID: "dest-replication-uuid",
				},
			},
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		badRequestResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationBadRequest{
			Message: "bad request",
		}

		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(badRequestResp, nil)

		updatedResult, err := activity.CleanupOldReplicationForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseHybridReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dest-location",
					DestinationReplicationUUID: "dest-replication-uuid",
				},
			},
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		notFoundResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationNotFound{
			Message: "not found",
		}

		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(notFoundResp, nil)

		updatedResult, err := activity.CleanupOldReplicationForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseHybridReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dest-location",
					DestinationReplicationUUID: "dest-replication-uuid",
				},
			},
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		// Return an unexpected type that implements the interface but isn't handled in switch
		unexpectedResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationConflict{
			Message: "conflict",
		}

		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(unexpectedResp, nil)

		updatedResult, err := activity.CleanupOldReplicationForHybridReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockInvoker.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_DescribeRemoteJobOnDstForHybridReverse(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		jobId := "test-job-id"
		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		correlationID := "test-xcorrelation-id"
		result := &replication.ReverseHybridReplicationResult{
			JobId:            &jobId,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "dest-location",
				},
			},
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    jobId,
			ProjectNumber:  dstProjectNumber,
			LocationId:     result.DbVolReplication.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}

		mockInvoker.On("V1betaInternalDescribeOperation", ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{
			Done: googleproxyclient.NewOptBool(true),
		}, nil)

		err := activity.DescribeRemoteJobOnDstForHybridReverse(ctx, result)

		assert.NoError(tt, err)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenJobIdIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		result := &replication.ReverseHybridReplicationResult{
			JobId: nil,
		}

		err := activity.DescribeRemoteJobOnDstForHybridReverse(ctx, result)

		assert.NoError(tt, err)
	})

	t.Run("WhenDescribeJobFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		jobId := "test-job-id"
		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		correlationID := "test-xcorrelation-id"
		result := &replication.ReverseHybridReplicationResult{
			JobId:            &jobId,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "dest-location",
				},
			},
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    jobId,
			ProjectNumber:  dstProjectNumber,
			LocationId:     result.DbVolReplication.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}

		mockInvoker.On("V1betaInternalDescribeOperation", ctx, describeOperationParams).Return(nil, errors.New("describe operation failed"))

		err := activity.DescribeRemoteJobOnDstForHybridReverse(ctx, result)

		assert.Error(tt, err)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenJobNotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseHybridReplicationActivity{SE: mockStorage}

		jobId := "test-job-id"
		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		correlationID := "test-xcorrelation-id"
		result := &replication.ReverseHybridReplicationResult{
			JobId:            &jobId,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "dest-location",
				},
			},
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    jobId,
			ProjectNumber:  dstProjectNumber,
			LocationId:     result.DbVolReplication.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}

		mockInvoker.On("V1betaInternalDescribeOperation", ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{
			Done: googleproxyclient.NewOptBool(false),
		}, nil)

		err := activity.DescribeRemoteJobOnDstForHybridReverse(ctx, result)

		assert.Error(tt, err)
		mockInvoker.AssertExpectations(tt)
	})
}

func TestReverseHybridReplicationActivity_HydrateReplicationSateAndTypeForReverseHybridReplication(t *testing.T) {
	ctx := context.Background()
	activity := ReverseHybridReplicationActivity{}

	t.Run("WhenSuccess_WithHydrationEnabled", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Mock hydrateReplicationStateAndTypeForHybrid to return success
		originalHydrateReplicationStateAndTypeForHybrid := hydrateReplicationStateAndTypeForHybrid
		hydrateReplicationStateAndTypeForHybrid = func(ctx context.Context, volumeRepModel models.VolumeReplication, hydrateState models.VolumeReplicationHydrateState, hydrateType models.HybridReplicationParametersReplicationType, projectNumber string) error {
			return nil
		}
		defer func() { hydrateReplicationStateAndTypeForHybrid = originalHydrateReplicationStateAndTypeForHybrid }()

		result := &replication.ReverseHybridReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					Location:         "us-east1",
					VolumeResourceID: "dest-volume",
				},
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				Uri:  "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "us-east1",
					DestinationVolumeName: "dest-volume",
				},
			},
		}

		updatedResult, err := activity.HydrateReplicationSateAndTypeForReverseHybridReplication(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
	})

	t.Run("WhenSuccess_WithHydrationDisabled", func(tt *testing.T) {
		// Mock hydrationEnabled to be false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		result := &replication.ReverseHybridReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					Location:         "us-east1",
					VolumeResourceID: "dest-volume",
				},
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				Uri:  "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "us-east1",
					DestinationVolumeName: "dest-volume",
				},
			},
		}

		updatedResult, err := activity.HydrateReplicationSateAndTypeForReverseHybridReplication(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
	})

	t.Run("WhenParseProjectNumberFails", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		result := &replication.ReverseHybridReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					Location:         "us-east1",
					VolumeResourceID: "dest-volume",
				},
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				Uri:  "invalid-uri",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "us-east1",
					DestinationVolumeName: "dest-volume",
				},
			},
		}

		updatedResult, err := activity.HydrateReplicationSateAndTypeForReverseHybridReplication(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "failed to parse project number")
	})

	t.Run("WhenHydrateReplicationStateAndTypeForHybridFails", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Mock hydrateReplicationStateAndTypeForHybrid to return error
		originalHydrateReplicationStateAndTypeForHybrid := hydrateReplicationStateAndTypeForHybrid
		hydrateReplicationStateAndTypeForHybrid = func(ctx context.Context, volumeRepModel models.VolumeReplication, hydrateState models.VolumeReplicationHydrateState, hydrateType models.HybridReplicationParametersReplicationType, projectNumber string) error {
			return fmt.Errorf("hydration error")
		}
		defer func() { hydrateReplicationStateAndTypeForHybrid = originalHydrateReplicationStateAndTypeForHybrid }()

		result := &replication.ReverseHybridReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					Location: "us-east1",
				},
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				Uri:  "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "us-east1",
					DestinationVolumeName: "dest-volume",
				},
			},
		}

		updatedResult, err := activity.HydrateReplicationSateAndTypeForReverseHybridReplication(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "hydration error")
	})
}

func TestHydrateReplicationStateAndTypeForHybridReplication(t *testing.T) {
	ctx := context.Background()

	t.Run("WhenSuccess_WithHydrationEnabled", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Mock hydrateReplicationStateAndTypeForHybrid to return success
		originalHydrateReplicationStateAndTypeForHybrid := hydrateReplicationStateAndTypeForHybrid
		hydrateReplicationStateAndTypeForHybrid = func(ctx context.Context, volumeRepModel models.VolumeReplication, hydrateState models.VolumeReplicationHydrateState, hydrateType models.HybridReplicationParametersReplicationType, projectNumber string) error {
			assert.Equal(tt, "test-replication", volumeRepModel.Name)
			assert.Equal(tt, "us-east1", volumeRepModel.ReplicationAttributes.DestinationRegion)
			assert.Equal(tt, "dest-volume", volumeRepModel.ReplicationAttributes.DestinationVolumeName)
			assert.Equal(tt, models.VolumeReplicationHydrateStateReady, hydrateState)
			assert.Equal(tt, models.HybridReplicationParametersReplicationTypeONPREM, hydrateType)
			assert.Equal(tt, "123456789", projectNumber)
			return nil
		}
		defer func() { hydrateReplicationStateAndTypeForHybrid = originalHydrateReplicationStateAndTypeForHybrid }()

		dbVolRep := &datamodel.VolumeReplication{
			Name: "test-replication",
			Uri:  "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:   "us-east1",
				DestinationVolumeName: "dest-volume",
			},
		}

		err := HydrateReplicationStateAndTypeForHybridReplication(ctx, dbVolRep, models.VolumeReplicationHydrateStateReady, models.HybridReplicationParametersReplicationTypeONPREM, "us-east1", "dest-volume")

		assert.NoError(tt, err)
	})

	t.Run("WhenSuccess_WithHydrationDisabled", func(tt *testing.T) {
		// Mock hydrationEnabled to be false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		dbVolRep := &datamodel.VolumeReplication{
			Name: "test-replication",
			Uri:  "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:   "us-east1",
				DestinationVolumeName: "dest-volume",
			},
		}

		err := HydrateReplicationStateAndTypeForHybridReplication(ctx, dbVolRep, models.VolumeReplicationHydrateStateReady, models.HybridReplicationParametersReplicationTypeONPREM, "us-east1", "dest-volume")

		assert.NoError(tt, err)
	})

	t.Run("WhenParseProjectNumberFails", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		dbVolRep := &datamodel.VolumeReplication{
			Name: "test-replication",
			Uri:  "invalid-uri",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:   "us-east1",
				DestinationVolumeName: "dest-volume",
			},
		}

		err := HydrateReplicationStateAndTypeForHybridReplication(ctx, dbVolRep, models.VolumeReplicationHydrateStateReady, models.HybridReplicationParametersReplicationTypeONPREM, "us-east1", "dest-volume")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to parse project number")
	})

	t.Run("WhenHydrateReplicationStateAndTypeForHybridFails", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Mock hydrateReplicationStateAndTypeForHybrid to return error
		originalHydrateReplicationStateAndTypeForHybrid := hydrateReplicationStateAndTypeForHybrid
		hydrateReplicationStateAndTypeForHybrid = func(ctx context.Context, volumeRepModel models.VolumeReplication, hydrateState models.VolumeReplicationHydrateState, hydrateType models.HybridReplicationParametersReplicationType, projectNumber string) error {
			return fmt.Errorf("hydration error")
		}
		defer func() { hydrateReplicationStateAndTypeForHybrid = originalHydrateReplicationStateAndTypeForHybrid }()

		dbVolRep := &datamodel.VolumeReplication{
			Name: "test-replication",
			Uri:  "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:   "us-east1",
				DestinationVolumeName: "dest-volume",
			},
		}

		err := HydrateReplicationStateAndTypeForHybridReplication(ctx, dbVolRep, models.VolumeReplicationHydrateStateReady, models.HybridReplicationParametersReplicationTypeONPREM, "us-east1", "dest-volume")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "hydration error")
	})

	t.Run("WhenSuccess_WithExternalManagedState", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Mock hydrateReplicationStateAndTypeForHybrid to return success
		originalHydrateReplicationStateAndTypeForHybrid := hydrateReplicationStateAndTypeForHybrid
		hydrateReplicationStateAndTypeForHybrid = func(ctx context.Context, volumeRepModel models.VolumeReplication, hydrateState models.VolumeReplicationHydrateState, hydrateType models.HybridReplicationParametersReplicationType, projectNumber string) error {
			assert.Equal(tt, models.VolumeReplicationHydrateStateExternalManaged, hydrateState)
			assert.Equal(tt, models.HybridReplicationParametersReplicationTypeREVERSE, hydrateType)
			return nil
		}
		defer func() { hydrateReplicationStateAndTypeForHybrid = originalHydrateReplicationStateAndTypeForHybrid }()

		dbVolRep := &datamodel.VolumeReplication{
			Name: "test-replication",
			Uri:  "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:   "us-east1",
				DestinationVolumeName: "dest-volume",
			},
		}

		err := HydrateReplicationStateAndTypeForHybridReplication(ctx, dbVolRep, models.VolumeReplicationHydrateStateExternalManaged, models.HybridReplicationParametersReplicationTypeREVERSE, "us-east1", "dest-volume")

		assert.NoError(tt, err)
	})
}
