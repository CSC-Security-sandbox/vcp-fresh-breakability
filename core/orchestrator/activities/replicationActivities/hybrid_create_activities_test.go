package replicationActivities

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestHybridReplicationActivity_GetLocalBasePath(t *testing.T) {
	t.Run("ValidDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.CreateHybridReplicationResult{
			DestinationRegion: "us-central1",
		}
		activity := HybridReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		updatedResult, err := activity.GetDstBasePathForHybridReplication(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})

	t.Run("ErrorDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.CreateHybridReplicationResult{
			DestinationRegion: "us-central1",
		}
		activity := HybridReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetDstBasePathForHybridReplication(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestHybridReplicationActivity_GetSignedLocalToken(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		dstPrj := "dstPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := HybridReplicationActivity{SE: mockStorage}
		result := &replication.CreateHybridReplicationResult{
			DestinationProjectNumber: dstPrj,
		}

		replication.InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "signed-token", nil
		}

		updatedResult, err := activity.GetDstSignedTokenForHybridReplication(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		dstPrj := "dstPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := HybridReplicationActivity{SE: mockStorage}
		result := &replication.CreateHybridReplicationResult{
			DestinationProjectNumber: dstPrj,
		}

		replication.InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("failed to get signed token")
		}

		updatedResult, err := activity.GetDstSignedTokenForHybridReplication(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestHybridReplicationActivity_GetNodeForHybridReplication(t *testing.T) {
	t.Run("SuccessWhenNodesAreFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		poolID := int64(123)
		expectedNodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-node-uuid-1",
				},
				Name:            "test-node-1",
				EndpointAddress: "192.168.1.1",
			},
			{
				BaseModel: datamodel.BaseModel{
					ID:   2,
					UUID: "test-node-uuid-2",
				},
				Name:            "test-node-2",
				EndpointAddress: "192.168.1.2",
			},
		}

		replicationResult := &replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: poolID},
				},
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolID).Return(expectedNodes, nil)

		result, err := activity.GetNodeForHybridReplication(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedNodes, result.DbNodes)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetNodesByPoolIDFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		poolID := int64(123)
		replicationResult := &replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: poolID},
				},
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolID).Return(nil, errors.New("database connection error"))

		result, err := activity.GetNodeForHybridReplication(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database connection error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenNoNodesFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		poolID := int64(123)
		replicationResult := &replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: poolID},
				},
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolID).Return([]*datamodel.Node{}, nil)

		result, err := activity.GetNodeForHybridReplication(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Node not found for the pool", vsaerrors.ExtractCustomError(err).OriginalErr.Error())
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_CreateNodesForHybridReplication(t *testing.T) {
	t.Run("SuccessWhenNodeIsCreatedWithCertificateAuth", func(tt *testing.T) {
		originalCreateNodeForProvider := hyperscaler.CreateNodeForProvider
		defer func() { hyperscaler.CreateNodeForProvider = originalCreateNodeForProvider }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		dbNodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-node-uuid-1",
				},
				Name:            "test-node-1",
				EndpointAddress: "192.168.1.1",
				HostDNSName:     "test-node-1.example.com",
			},
			{
				BaseModel: datamodel.BaseModel{
					ID:   2,
					UUID: "test-node-uuid-2",
				},
				Name:            "test-node-2",
				EndpointAddress: "192.168.1.2",
				HostDNSName:     "test-node-2.example.com",
			},
		}

		expectedNode := &models.Node{
			DeploymentName: "test-deployment",
			CertificateID:  "test-cert-id",
			SecretID:       "test-secret-id",
			AuthType:       2, // USER_CERTIFICATE
			EndpointAddressesToHostNameMap: map[string]string{
				"192.168.1.1": "test-node-1.example.com",
				"192.168.1.2": "test-node-2.example.com",
			},
		}

		replicationResult := &replication.CreateHybridReplicationResult{
			DbNodes: dbNodes,
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					DeploymentName: "test-deployment",
					PoolCredentials: &datamodel.PoolCredentials{
						CertificateID: "test-cert-id",
						SecretID:      "test-secret-id",
						AuthType:      2, // USER_CERTIFICATE
					},
				},
			},
		}

		hyperscaler.CreateNodeForProvider = func(input hyperscaler.NodeProviderInput) *models.Node {
			return expectedNode
		}

		result, err := activity.CreateNodesForHybridReplication(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedNode, result.NodeProvider)
		assert.Equal(tt, "test-deployment", result.NodeProvider.DeploymentName)
		assert.Equal(tt, "test-cert-id", result.NodeProvider.CertificateID)
		assert.Equal(tt, "test-secret-id", result.NodeProvider.SecretID)
	})

	t.Run("SuccessWhenNodeIsCreatedWithPasswordAuth", func(tt *testing.T) {
		originalCreateNodeForProvider := hyperscaler.CreateNodeForProvider
		defer func() { hyperscaler.CreateNodeForProvider = originalCreateNodeForProvider }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		dbNodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-node-uuid-1",
				},
				Name:            "test-node-1",
				EndpointAddress: "192.168.1.1",
			},
		}

		expectedNode := &models.Node{
			DeploymentName: "test-deployment",
			Password:       "test-password",
			SecretID:       "test-secret-id",
			AuthType:       0, // USERNAME_PWD
			EndpointAddressesToHostNameMap: map[string]string{
				"192.168.1.1": "192.168.1.1",
			},
		}

		replicationResult := &replication.CreateHybridReplicationResult{
			DbNodes: dbNodes,
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					DeploymentName: "test-deployment",
					PoolCredentials: &datamodel.PoolCredentials{
						Password: "test-password",
						SecretID: "test-secret-id",
						AuthType: 0, // USERNAME_PWD
					},
				},
			},
		}

		hyperscaler.CreateNodeForProvider = func(input hyperscaler.NodeProviderInput) *models.Node {
			return expectedNode
		}

		result, err := activity.CreateNodesForHybridReplication(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedNode, result.NodeProvider)
		assert.Equal(tt, "test-deployment", result.NodeProvider.DeploymentName)
		assert.Equal(tt, "test-password", result.NodeProvider.Password)
	})

	t.Run("ErrorWhenCreateNodeForProviderReturnsNil", func(tt *testing.T) {
		originalCreateNodeForProvider := hyperscaler.CreateNodeForProvider
		defer func() { hyperscaler.CreateNodeForProvider = originalCreateNodeForProvider }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		dbNodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-node-uuid-1",
				},
				Name:            "test-node-1",
				EndpointAddress: "192.168.1.1",
			},
		}

		replicationResult := &replication.CreateHybridReplicationResult{
			DbNodes: dbNodes,
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					DeploymentName: "test-deployment",
					PoolCredentials: &datamodel.PoolCredentials{
						Password: "test-password",
						SecretID: "test-secret-id",
						AuthType: 0,
					},
				},
			},
		}

		hyperscaler.CreateNodeForProvider = func(input hyperscaler.NodeProviderInput) *models.Node {
			return nil
		}

		result, err := activity.CreateNodesForHybridReplication(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "failed to create destination node", vsaerrors.ExtractCustomError(err).OriginalErr.Error())
	})

	t.Run("SuccessWhenNodeIsCreatedWithNilCredentials", func(tt *testing.T) {
		originalCreateNodeForProvider := hyperscaler.CreateNodeForProvider
		defer func() { hyperscaler.CreateNodeForProvider = originalCreateNodeForProvider }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		dbNodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-node-uuid-1",
				},
				Name:            "test-node-1",
				EndpointAddress: "192.168.1.1",
			},
		}

		expectedNode := &models.Node{
			DeploymentName: "test-deployment",
		}

		replicationResult := &replication.CreateHybridReplicationResult{
			DbNodes: dbNodes,
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					DeploymentName:  "test-deployment",
					PoolCredentials: nil,
				},
			},
		}

		hyperscaler.CreateNodeForProvider = func(input hyperscaler.NodeProviderInput) *models.Node {
			// When credentials are nil, CreateNodeForProvider returns a node with just DeploymentName
			return expectedNode
		}

		result, err := activity.CreateNodesForHybridReplication(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedNode, result.NodeProvider)
		assert.Equal(tt, "test-deployment", result.NodeProvider.DeploymentName)
	})
}

func TestHybridReplicationActivity_DescribeJobForHybridReplicationWorkflow(t *testing.T) {
	t.Run("DescribeJobSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)

		activity := HybridReplicationActivity{SE: mockStorage}
		result := &replication.CreateHybridReplicationResult{
			JobId:                    nillable.GetStringPtr("test-job-id"),
			DestinationProjectNumber: "test-project-number",
			DestinationRegion:        "test-location-id",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DstBasePath:              nillable.GetStringPtr("base-path"),
			DstJwtToken:              nillable.GetStringPtr("jwt-token"),
		}

		// Mock GetJob returning a job with DONE state
		mockStorage.On("GetJob", ctx, *result.JobId).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: *result.JobId},
			State:     string(models.JobsStateDONE),
		}, nil)

		err := activity.DescribeJobForHybridReplicationWorkflow(ctx, result)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("DescribeJobNotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)

		activity := HybridReplicationActivity{SE: mockStorage}
		result := &replication.CreateHybridReplicationResult{
			JobId:                    nillable.GetStringPtr("test-job-id"),
			DestinationProjectNumber: "test-project-number",
			DestinationRegion:        "test-location-id",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DstBasePath:              nillable.GetStringPtr("base-path"),
			DstJwtToken:              nillable.GetStringPtr("jwt-token"),
		}

		// Mock GetJob returning a job with PROCESSING state (not finished)
		mockStorage.On("GetJob", ctx, *result.JobId).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: *result.JobId},
			State:     string(models.JobsStatePROCESSING),
		}, nil)

		err := activity.DescribeJobForHybridReplicationWorkflow(ctx, result)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Job not finished")
		mockStorage.AssertExpectations(tt)
	})
	t.Run("DescribeJobError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)

		activity := HybridReplicationActivity{SE: mockStorage}
		result := &replication.CreateHybridReplicationResult{
			JobId:                    nillable.GetStringPtr("test-job-id"),
			DestinationProjectNumber: "test-project-number",
			DestinationRegion:        "test-location-id",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DstBasePath:              nillable.GetStringPtr("base-path"),
			DstJwtToken:              nillable.GetStringPtr("jwt-token"),
		}

		// Mock GetJob returning a job with ERROR state
		mockStorage.On("GetJob", ctx, *result.JobId).Return(&datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: *result.JobId},
			State:        string(models.JobsStateERROR),
			TrackingID:   int(vsaerrors.ErrDescribingJobNotFound),
			ErrorDetails: "job failed with error",
		}, nil)

		err := activity.DescribeJobForHybridReplicationWorkflow(ctx, result)

		assert.Error(tt, err)
		// The error message comes from GetErrorMessageByTrackingID, so check for the error being returned
		assert.Contains(tt, err.Error(), "Job not found")
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_CreateJobForEstablishReplicationWorkflow(t *testing.T) {
	t.Run("CreateJobForCreateVolumeSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-replication-id",
			},
		}

		expectedJob := &datamodel.Job{
			AccountID:     sql.NullInt64{Int64: 123, Valid: true},
			Type:          string(models.JobTypeCreateVolume),
			State:         string(models.JobsStateNEW),
			ResourceName:  "test-volume",
			JobAttributes: &datamodel.JobAttributes{ResourceUUID: "test-volume-uuid"},
		}

		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.AccountID.Int64 == 123 &&
				job.Type == string(models.JobTypeCreateVolume) &&
				job.State == string(models.JobsStateNEW) &&
				job.ResourceName == "test-volume" &&
				job.JobAttributes.ResourceUUID == "test-volume-uuid"
		})).Return(expectedJob, nil)

		result, err := activity.CreateJobForHybridReplication(ctx, replicationResult, string(models.JobTypeCreateVolume))

		assert.NoError(tt, err)
		assert.Equal(tt, expectedJob, result)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CreateJobForHybridReplicationEstablishPeeringSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-replication-id",
			},
		}

		expectedJob := &datamodel.Job{
			AccountID:     sql.NullInt64{Int64: 123, Valid: true},
			Type:          string(models.JobTypeHybridReplicationEstablishPeering),
			State:         string(models.JobsStateNEW),
			ResourceName:  "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id",
			JobAttributes: &datamodel.JobAttributes{ResourceUUID: "test-volume-uuid"},
		}

		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.AccountID.Int64 == 123 &&
				job.Type == string(models.JobTypeHybridReplicationEstablishPeering) &&
				job.State == string(models.JobsStateNEW) &&
				job.ResourceName == "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id" &&
				job.JobAttributes.ResourceUUID == "test-volume-uuid"
		})).Return(expectedJob, nil)

		result, err := activity.CreateJobForHybridReplication(ctx, replicationResult, string(models.JobTypeHybridReplicationEstablishPeering))

		assert.NoError(tt, err)
		assert.Equal(tt, expectedJob, result)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CreateJobError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-replication-id",
			},
		}

		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("database error"))

		result, err := activity.CreateJobForHybridReplication(ctx, replicationResult, string(models.JobTypeCreateVolume))

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "database error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_GetOrCreateClusterPeerForHybridReplication(t *testing.T) {
	t.Run("GetExistingClusterPeerSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				PoolID:    456,
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerClusterName: "test-cluster",
				ClusterLocation: "us-central1",
			},
		}

		existingClusterPeer := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "existing-cluster-peer-uuid"},
			State:          models.CvpClusterPeeringStatusCREATING,
			OnprempCluster: "test-cluster",
			AccountID:      123,
			PoolID:         456,
		}

		mockStorage.On("GetClusterPeerByAccountIDExternalClusterAndPoolID", ctx, int64(123), "test-cluster", int64(456)).Return(existingClusterPeer, nil)

		result, err := activity.GetOrCreateClusterPeerForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, existingClusterPeer, result.ClusterPeeringRow)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CreateNewClusterPeerSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				PoolID:    456,
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerClusterName: "test-cluster",
				ClusterLocation: "us-central1",
			},
		}

		newClusterPeer := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "new-cluster-peer-uuid"},
			State:          models.CvpClusterPeeringStatusCREATING,
			OnprempCluster: "test-cluster",
			AccountID:      123,
			PoolID:         456,
			ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
				ClusterLocation: nillable.GetStringPtr("us-central1"),
			},
		}

		// Mock GetClusterPeerByAccountIDExternalClusterAndPoolID to return NotFound error
		mockStorage.On("GetClusterPeerByAccountIDExternalClusterAndPoolID", ctx, int64(123), "test-cluster", int64(456)).Return(nil, customerrors.NewNotFoundErr("ClusterPeer", nil))
		mockStorage.On("CreateClusterPeeringRow", ctx, mock.Anything).Return(newClusterPeer, nil)

		result, err := activity.GetOrCreateClusterPeerForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, newClusterPeer, result.ClusterPeeringRow)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("GetClusterPeerDatabaseError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				PoolID:    456,
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerClusterName: "test-cluster",
				ClusterLocation: "us-central1",
			},
		}

		mockStorage.On("GetClusterPeerByAccountIDExternalClusterAndPoolID", ctx, int64(123), "test-cluster", int64(456)).Return(nil, errors.New("database connection error"))

		result, err := activity.GetOrCreateClusterPeerForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "database connection error", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CreateClusterPeerDatabaseError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				PoolID:    456,
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerClusterName: "test-cluster",
				ClusterLocation: "us-central1",
			},
		}

		// Mock GetClusterPeerByAccountIDExternalClusterAndPoolID to return NotFound error
		mockStorage.On("GetClusterPeerByAccountIDExternalClusterAndPoolID", ctx, int64(123), "test-cluster", int64(456)).Return(nil, customerrors.NewNotFoundErr("ClusterPeer", nil))

		// Mock CreateClusterPeeringRow to return error
		mockStorage.On("CreateClusterPeeringRow", ctx, mock.Anything).Return(nil, errors.New("failed to create cluster peer"))

		result, err := activity.GetOrCreateClusterPeerForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "failed to create cluster peer", err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_WaitForClusterPeerActivityForHybridReplication(t *testing.T) {
	t.Run("SuccessWhenClusterPeerIsAvailableAndAuthenticated", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
		}

		mockProvider := &vsa.MockProvider{}
		expectedClusterPeer := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityAvailable,
			AuthenticationState: models.AuthenticationStateOk,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(expectedClusterPeer, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedClusterPeer, result.ClusterPeer)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenAuthenticationStateIsProblem", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
		}

		mockProvider := &vsa.MockProvider{}
		clusterPeerWithProblem := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityAvailable,
			AuthenticationState: models.AuthenticationStateProblem,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerWithProblem, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, clusterPeerWithProblem, result.ClusterPeer)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "cluster peer authentication state is problem, waiting for recovery")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenAuthenticationStateIsAbsent", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
		}

		mockProvider := &vsa.MockProvider{}
		clusterPeerWithAbsent := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityAvailable,
			AuthenticationState: models.AuthenticationStateAbsent,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerWithAbsent, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, clusterPeerWithAbsent, result.ClusterPeer)
		assert.Equal(tt, "cluster peer authentication state is absent", vsaerrors.ExtractCustomError(err).OriginalErr.Error())
		mockProvider.AssertExpectations(tt)
	})

	t.Run("TimeoutWhenClusterPeerIsNotReadyYet", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
		}

		mockProvider := &vsa.MockProvider{}
		clusterPeerNotReady := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityPending,
			AuthenticationState: models.AuthenticationStateOk,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerNotReady, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "cluster peer is not ready yet")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenProviderFailsToGetClusterPeer", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
		}

		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(nil, errors.New("failed to get cluster peer"))

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "failed to get cluster peer", vsaerrors.ExtractCustomError(err).OriginalErr.Error())
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get provider")
	})

	t.Run("TimeoutWhenClusterPeerIsPartialAvailability", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
		}

		mockProvider := &vsa.MockProvider{}
		clusterPeerPartial := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityPartial,
			AuthenticationState: models.AuthenticationStateOk,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerPartial, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "cluster peer is not ready yet")

		mockProvider.AssertExpectations(tt)
	})

	t.Run("TimeoutWhenClusterPeerIsUnavailable", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
		}

		mockProvider := &vsa.MockProvider{}
		clusterPeerUnavailable := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityUnavailable,
			AuthenticationState: models.AuthenticationStateOk,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerUnavailable, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "cluster peer is not ready yet")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenProblemStateExceeds10Minutes", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Set FirstProblemStateTime to 11 minutes ago (exceeds 10 minute limit)
		elevenMinutesAgo := time.Now().Add(-11 * time.Minute)
		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
			FirstProblemStateTime: &elevenMinutesAgo,
		}

		mockProvider := &vsa.MockProvider{}
		clusterPeerWithProblem := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityAvailable,
			AuthenticationState: models.AuthenticationStateProblem,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerWithProblem, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, clusterPeerWithProblem, result.ClusterPeer)
		// Should return non-retryable error when 10 minutes exceeded
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "exceeded 10 minute limit")
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "cluster peer authentication state has been")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenProblemStateWithin10Minutes", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Set FirstProblemStateTime to 5 minutes ago (within 10 minute limit)
		fiveMinutesAgo := time.Now().Add(-5 * time.Minute)
		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
			FirstProblemStateTime: &fiveMinutesAgo,
		}

		mockProvider := &vsa.MockProvider{}
		clusterPeerWithProblem := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityAvailable,
			AuthenticationState: models.AuthenticationStateProblem,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerWithProblem, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, clusterPeerWithProblem, result.ClusterPeer)
		// Should return retryable error with elapsed time when within 10 minutes
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "waiting for recovery")
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "elapsed:")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenProblemStateExactly10Minutes", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Set FirstProblemStateTime to exactly 10 minutes ago (at the limit)
		tenMinutesAgo := time.Now().Add(-10 * time.Minute)
		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
			FirstProblemStateTime: &tenMinutesAgo,
		}

		mockProvider := &vsa.MockProvider{}
		clusterPeerWithProblem := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityAvailable,
			AuthenticationState: models.AuthenticationStateProblem,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerWithProblem, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, clusterPeerWithProblem, result.ClusterPeer)
		// Should return non-retryable error when exactly 10 minutes (>= 10 minutes)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "exceeded 10 minute limit")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenRecoveredFromProblemState", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Set FirstProblemStateTime to indicate it was previously in problem state
		fiveMinutesAgo := time.Now().Add(-5 * time.Minute)
		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
			FirstProblemStateTime: &fiveMinutesAgo,
		}

		mockProvider := &vsa.MockProvider{}
		// Cluster peer has recovered - authentication state is now Ok (not Problem)
		clusterPeerRecovered := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityAvailable,
			AuthenticationState: models.AuthenticationStateOk,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerRecovered, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, clusterPeerRecovered, result.ClusterPeer)
		// Verify that FirstProblemStateTime is reset to nil after recovery
		assert.Nil(tt, result.FirstProblemStateTime)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenRecoveredFromProblemStateWithOtherNonProblemState", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Set FirstProblemStateTime to indicate it was previously in problem state
		threeMinutesAgo := time.Now().Add(-3 * time.Minute)
		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-cluster-peer-uuid",
			},
			FirstProblemStateTime: &threeMinutesAgo,
		}

		mockProvider := &vsa.MockProvider{}
		// Cluster peer has recovered but availability is still pending
		// Authentication state is not Problem, so FirstProblemStateTime should be reset
		clusterPeerRecovered := &vsa.ClusterPeer{
			ExternalUUID:        "test-cluster-peer-uuid",
			Availability:        models.AvailabilityPending,
			AuthenticationState: models.AuthenticationStateOk,
		}

		mockProvider.On("GetClusterPeer", "test-cluster-peer-uuid").Return(clusterPeerRecovered, nil)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.WaitForClusterPeerActivityForHybridReplication(ctx, &replicationResult)

		// Should return error because availability is not Available yet
		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, clusterPeerRecovered, result.ClusterPeer)
		// Verify that FirstProblemStateTime is reset to nil after recovery from problem state
		assert.Nil(tt, result.FirstProblemStateTime)
		mockProvider.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_SetClusterPeeringStatusToPeeredForHybridReplication(t *testing.T) {
	t.Run("SuccessWhenClusterPeeringIsUpdatedToPeered", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:    datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:        models.CvpClusterPeeringStatusCREATING,
			StateDetails: "Creating cluster peer",
			ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
				PassPhrase: nillable.GetStringPtr("test-passphrase"),
				Command:    nillable.GetStringPtr("test-command"),
				ExpiryTime: nillable.GetTimePtr(time.Now().Add(time.Hour)),
			},
		}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				StateDetailsCode: models.InitiatingClusterPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			ClusterPeeringRow: clusterPeeringRow,
			DbVolReplication:  volumeReplication,
		}

		// Mock UpdateClusterPeeringRow
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(cpr *datamodel.ClusterPeerings) bool {
			return cpr.State == models.CvpClusterPeeringStatusPEERED &&
				cpr.StateDetails == "" &&
				cpr.ClusterPeeringAttributes != nil &&
				cpr.ClusterPeeringAttributes.PassPhrase == nil &&
				cpr.ClusterPeeringAttributes.Command == nil &&
				cpr.ClusterPeeringAttributes.ExpiryTime == nil
		})).Return(nil)

		// Mock UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes.StateDetailsCode == int32(models.DefaultCode)
		})).Return(nil)

		result, err := activity.SetClusterPeeringStatusToPeeredForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.CvpClusterPeeringStatusPEERED, result.ClusterPeeringRow.State)
		assert.Equal(tt, "", result.ClusterPeeringRow.StateDetails)
		assert.Nil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes.PassPhrase)
		assert.Nil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes.Command)
		assert.Nil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes.ExpiryTime)
		assert.Equal(tt, int32(models.DefaultCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenClusterPeeringRowAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:                datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:                    models.CvpClusterPeeringStatusCREATING,
			StateDetails:             "Creating cluster peer",
			ClusterPeeringAttributes: nil,
		}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				StateDetailsCode: models.InitiatingClusterPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			ClusterPeeringRow: clusterPeeringRow,
			DbVolReplication:  volumeReplication,
		}

		// Mock UpdateClusterPeeringRow
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(cpr *datamodel.ClusterPeerings) bool {
			return cpr.State == models.CvpClusterPeeringStatusPEERED &&
				cpr.StateDetails == "" &&
				cpr.ClusterPeeringAttributes == nil
		})).Return(nil)

		// Mock UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes.StateDetailsCode == int32(models.DefaultCode)
		})).Return(nil)

		result, err := activity.SetClusterPeeringStatusToPeeredForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.CvpClusterPeeringStatusPEERED, result.ClusterPeeringRow.State)
		assert.Equal(tt, "", result.ClusterPeeringRow.StateDetails)
		assert.Equal(tt, int32(models.DefaultCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenUpdateClusterPeeringRowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:    datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:        models.CvpClusterPeeringStatusCREATING,
			StateDetails: "Creating cluster peer",
			ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
				PassPhrase: nillable.GetStringPtr("test-passphrase"),
			},
		}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				StateDetailsCode: models.InitiatingClusterPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			ClusterPeeringRow: clusterPeeringRow,
			DbVolReplication:  volumeReplication,
		}

		// Mock UpdateClusterPeeringRow to return error
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.Anything).Return(errors.New("database error"))

		result, err := activity.SetClusterPeeringStatusToPeeredForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenUpdateReplicationStateDetailsCodeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:    datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:        models.CvpClusterPeeringStatusCREATING,
			StateDetails: "Creating cluster peer",
			ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
				PassPhrase: nillable.GetStringPtr("test-passphrase"),
			},
		}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				StateDetailsCode: models.InitiatingClusterPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			ClusterPeeringRow: clusterPeeringRow,
			DbVolReplication:  volumeReplication,
		}

		// Mock UpdateClusterPeeringRow to succeed
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.Anything).Return(nil)

		// Mock UpdateVolumeReplication to return error
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(errors.New("replication update error"))

		result, err := activity.SetClusterPeeringStatusToPeeredForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "replication update error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenClusterPeeringRowAttributesHasAllFields", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:    datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:        models.CvpClusterPeeringStatusCREATING,
			StateDetails: "Creating cluster peer",
			ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
				PassPhrase:      nillable.GetStringPtr("test-passphrase"),
				Command:         nillable.GetStringPtr("cluster peer create"),
				ExpiryTime:      nillable.GetTimePtr(time.Now().Add(time.Hour)),
				ClusterLocation: nillable.GetStringPtr("us-central1"),
			},
		}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				StateDetailsCode: models.InitiatingClusterPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			ClusterPeeringRow: clusterPeeringRow,
			DbVolReplication:  volumeReplication,
		}

		// Mock UpdateClusterPeeringRow
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(cpr *datamodel.ClusterPeerings) bool {
			return cpr.State == models.CvpClusterPeeringStatusPEERED &&
				cpr.StateDetails == "" &&
				cpr.ClusterPeeringAttributes != nil &&
				cpr.ClusterPeeringAttributes.PassPhrase == nil &&
				cpr.ClusterPeeringAttributes.Command == nil &&
				cpr.ClusterPeeringAttributes.ExpiryTime == nil &&
				cpr.ClusterPeeringAttributes.ClusterLocation != nil &&
				*cpr.ClusterPeeringAttributes.ClusterLocation == "us-central1"
		})).Return(nil)

		// Mock UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes.StateDetailsCode == int32(models.DefaultCode)
		})).Return(nil)

		result, err := activity.SetClusterPeeringStatusToPeeredForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.CvpClusterPeeringStatusPEERED, result.ClusterPeeringRow.State)
		assert.Equal(tt, "", result.ClusterPeeringRow.StateDetails)
		assert.Nil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes.PassPhrase)
		assert.Nil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes.Command)
		assert.Nil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes.ExpiryTime)
		assert.NotNil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes.ClusterLocation)
		assert.Equal(tt, "us-central1", *result.ClusterPeeringRow.ClusterPeeringAttributes.ClusterLocation)
		assert.Equal(tt, int32(models.DefaultCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenVolumeReplicationHasInitializedHybridReplicationAttributes", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:    datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:        models.CvpClusterPeeringStatusCREATING,
			StateDetails: "Creating cluster peer",
			ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
				PassPhrase: nillable.GetStringPtr("test-passphrase"),
			},
		}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				StateDetailsCode: models.InitiatingClusterPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			ClusterPeeringRow: clusterPeeringRow,
			DbVolReplication:  volumeReplication,
		}

		// Mock UpdateClusterPeeringRow
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.Anything).Return(nil)

		// Mock UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.DefaultCode)
		})).Return(nil)

		result, err := activity.SetClusterPeeringStatusToPeeredForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.CvpClusterPeeringStatusPEERED, result.ClusterPeeringRow.State)
		assert.Equal(tt, "", result.ClusterPeeringRow.StateDetails)
		assert.NotNil(tt, result.DbVolReplication.HybridReplicationAttributes)
		assert.Equal(tt, int32(models.DefaultCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_SetVolumeReplicationPeeringStatusToPendingSVMPeering(t *testing.T) {
	t.Run("SuccessWhenVolumeReplicationStatusIsUpdatedToPendingSVMPeer", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				Status:           models.HybridReplicationStatusPendingClusterPeer,
				StatusDetails:    "Waiting for cluster peering",
				StateDetailsCode: models.WaitingForClusterPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.Status == models.HybridReplicationStatusPendingSVMPeer &&
				vr.HybridReplicationAttributes.StatusDetails == models.InitiatingSVMPeering &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.InitiatingSVMPeeringCode)
		})).Return(nil)

		result, err := activity.SetVolumeReplicationPeeringStatusToPendingSVMPeering(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.HybridReplicationStatusPendingSVMPeer, result.DbVolReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, models.InitiatingSVMPeering, result.DbVolReplication.HybridReplicationAttributes.StatusDetails)
		assert.Equal(tt, int32(models.InitiatingSVMPeeringCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenHybridReplicationAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel:                   datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:                        "test-replication",
			HybridReplicationAttributes: nil,
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication - should be called with nil attributes unchanged
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes == nil
		})).Return(nil)

		result, err := activity.SetVolumeReplicationPeeringStatusToPendingSVMPeering(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.DbVolReplication.HybridReplicationAttributes)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenUpdateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				Status:           models.HybridReplicationStatusPendingClusterPeer,
				StatusDetails:    "Waiting for cluster peering",
				StateDetailsCode: models.WaitingForClusterPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication to return error
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(errors.New("database error"))

		result, err := activity.SetVolumeReplicationPeeringStatusToPendingSVMPeering(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenHybridReplicationAttributesHasAllFields", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				Status:                        models.HybridReplicationStatusPendingClusterPeer,
				StatusDetails:                 "Waiting for cluster peering",
				StateDetailsCode:              models.WaitingForClusterPeeringCode,
				SvmPeerCommand:                nillable.GetStringPtr("vserver peer create"),
				SvmPeerExpiryTime:             nillable.GetTimePtr(time.Now().Add(time.Hour)),
				Description:                   "Test replication",
				Labels:                        map[string]string{"env": "test"},
				PeerVolumeName:                "test-peer-volume",
				PeerSvmName:                   "test-peer-svm",
				ReplicationSchedule:           "hourly",
				HybridReplicationType:         nillable.GetStringPtr("migration"),
				HybridReplicationUserCommands: []string{"command1", "command2"},
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.Status == models.HybridReplicationStatusPendingSVMPeer &&
				vr.HybridReplicationAttributes.StatusDetails == models.InitiatingSVMPeering &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.InitiatingSVMPeeringCode) &&
				vr.HybridReplicationAttributes.SvmPeerCommand != nil &&
				*vr.HybridReplicationAttributes.SvmPeerCommand == "vserver peer create" &&
				vr.HybridReplicationAttributes.Description == "Test replication" &&
				vr.HybridReplicationAttributes.PeerVolumeName == "test-peer-volume"
		})).Return(nil)

		result, err := activity.SetVolumeReplicationPeeringStatusToPendingSVMPeering(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.HybridReplicationStatusPendingSVMPeer, result.DbVolReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, models.InitiatingSVMPeering, result.DbVolReplication.HybridReplicationAttributes.StatusDetails)
		assert.Equal(tt, int32(models.InitiatingSVMPeeringCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		// Verify other fields are preserved
		assert.NotNil(tt, result.DbVolReplication.HybridReplicationAttributes.SvmPeerCommand)
		assert.Equal(tt, "vserver peer create", *result.DbVolReplication.HybridReplicationAttributes.SvmPeerCommand)
		assert.Equal(tt, "Test replication", result.DbVolReplication.HybridReplicationAttributes.Description)
		assert.Equal(tt, "test-peer-volume", result.DbVolReplication.HybridReplicationAttributes.PeerVolumeName)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenVolumeReplicationHasEmptyHybridReplicationAttributes", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				Status:           "",
				StatusDetails:    "",
				StateDetailsCode: 0,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.Status == models.HybridReplicationStatusPendingSVMPeer &&
				vr.HybridReplicationAttributes.StatusDetails == models.InitiatingSVMPeering &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.InitiatingSVMPeeringCode)
		})).Return(nil)

		result, err := activity.SetVolumeReplicationPeeringStatusToPendingSVMPeering(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.HybridReplicationStatusPendingSVMPeer, result.DbVolReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, models.InitiatingSVMPeering, result.DbVolReplication.HybridReplicationAttributes.StatusDetails)
		assert.Equal(tt, int32(models.InitiatingSVMPeeringCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_createSVMPeerForHybridReplication(t *testing.T) {
	t.Run("SuccessWhenSVMPeerIsCreatedSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		mockProvider := &vsa.MockProvider{}
		svmPeer := &vsa.SvmPeer{
			UUID:            "test-svm-peer-uuid",
			State:           "initializing",
			LocalSvmName:    "test-local-svm",
			PeerSvmName:     "test-peer-svm",
			PeerClusterName: "test-peer-cluster",
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock CreateSVMPeer
		mockProvider.On("CreateSVMPeer", mock.Anything).Return(svmPeer, nil)

		err := activity.createSVMPeerForHybridReplication(ctx, mockProvider, &replicationResult)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenCreateSVMPeerFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		mockProvider := &vsa.MockProvider{}

		replicationResult := replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock CreateSVMPeer to return error
		mockProvider.On("CreateSVMPeer", mock.Anything).Return(nil, errors.New("failed to create SVM peer"))

		err := activity.createSVMPeerForHybridReplication(ctx, mockProvider, &replicationResult)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to create SVM peer")
		mockProvider.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_deleteSVMPeerForHybridReplication(t *testing.T) {
	t.Run("SuccessWhenSVMPeerIsDeletedSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		mockProvider := &vsa.MockProvider{}
		svmPeerUUID := "test-svm-peer-uuid"

		// Mock DeleteSVMPeer
		mockProvider.On("DeleteSVMPeer", svmPeerUUID, false).Return(nil)

		err := activity.deleteSVMPeerForHybridReplication(ctx, mockProvider, svmPeerUUID)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenDeleteSVMPeerFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		mockProvider := &vsa.MockProvider{}
		svmPeerUUID := "test-svm-peer-uuid"

		// Mock DeleteSVMPeer to return error
		mockProvider.On("DeleteSVMPeer", svmPeerUUID, false).Return(fmt.Errorf("failed to delete SVM peer"))

		err := activity.deleteSVMPeerForHybridReplication(ctx, mockProvider, svmPeerUUID)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to delete SVM peer")
		mockProvider.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_CleanupReplicationIfNeeded(t *testing.T) {
	t.Run("ErrorWhenGetProviderByNodeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				LifeCycleState: googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError),
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "test-endpoint",
					SourceHostName:        "source-host",
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-vol",
					DestinationHostName:   "dest-host",
					DestinationSvmName:    "dest-svm",
					ReplicationSchedule:   "test-schedule",
					DestinationVolumeName: "dest-vol",
					ExternalUUID:          "test-uuid",
				},
			},
		}

		// Mock GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("provider error")
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		result, err := activity.CleanupReplicationIfNeeded(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("ErrorWhenDeleteVolumeReplicationFailsWithConflict", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				LifeCycleState: googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError),
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "test-endpoint",
					SourceHostName:        "source-host",
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-vol",
					DestinationHostName:   "dest-host",
					DestinationSvmName:    "dest-svm",
					ReplicationSchedule:   "test-schedule",
					DestinationVolumeName: "dest-vol",
					ExternalUUID:          "test-uuid",
				},
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock DeleteVolumeReplication to return conflict error
		mockProvider.On("DeleteVolumeReplication", mock.Anything).Return(nil, customerrors.NewConflictErr("conflict error"))

		result, err := activity.CleanupReplicationIfNeeded(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "conflict error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenDeleteVolumeReplicationFailsWithOtherError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				LifeCycleState: googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError),
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "test-endpoint",
					SourceHostName:        "source-host",
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-vol",
					DestinationHostName:   "dest-host",
					DestinationSvmName:    "dest-svm",
					ReplicationSchedule:   "test-schedule",
					DestinationVolumeName: "dest-vol",
					ExternalUUID:          "test-uuid",
				},
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock DeleteVolumeReplication to return other error
		mockProvider.On("DeleteVolumeReplication", mock.Anything).Return(nil, fmt.Errorf("delete error"))

		result, err := activity.CleanupReplicationIfNeeded(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "delete error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenDstReplicationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider:   &models.Node{Name: "test-node"},
			DstReplication: nil,
		}

		result, err := activity.CleanupReplicationIfNeeded(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
	})

	t.Run("SuccessWhenLifeCycleStateIsNotSet", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{Name: "test-node"},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				LifeCycleState: googleproxyclient.OptVolumeReplicationInternalV1betaLifeCycleState{},
			},
		}

		result, err := activity.CleanupReplicationIfNeeded(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
	})

	t.Run("SuccessWhenLifeCycleStateIsNotError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{Name: "test-node"},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				LifeCycleState: googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateAvailable),
			},
		}

		result, err := activity.CleanupReplicationIfNeeded(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
	})

	t.Run("SuccessWhenReplicationCleanupIsPerformed", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				LifeCycleState: googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError),
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "test-endpoint",
					SourceHostName:        "source-host",
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-vol",
					DestinationHostName:   "dest-host",
					DestinationSvmName:    "dest-svm",
					ReplicationSchedule:   "test-schedule",
					DestinationVolumeName: "dest-vol",
					ExternalUUID:          "test-uuid",
				},
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock DeleteVolumeReplication to return success
		deletedReplication := &vsa.VolumeReplication{
			RelationshipID: "test-uuid",
		}
		mockProvider.On("DeleteVolumeReplication", mock.Anything).Return(deletedReplication, nil)

		result, err := activity.CleanupReplicationIfNeeded(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.DstReplication) // Should be cleared after cleanup
		mockProvider.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_CreateSVMPeerInOntapForHybridReplication(t *testing.T) {
	t.Run("ErrorWhenGetProviderByNodeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("failed to get provider")
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		result, err := activity.CreateSVMPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get provider")
	})

	t.Run("ErrorWhenGetSVMPeerFailsWithNonNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return non-not-found error
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("connection error"))

		result, err := activity.CreateSVMPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "connection error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenCreateSVMPeerForHybridReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return not found error
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(nil, customerrors.NewNotFoundErr("SVM peer", nil))
		// Mock CreateSVMPeer to return error
		mockProvider.On("CreateSVMPeer", mock.Anything).Return(nil, fmt.Errorf("failed to create SVM peer"))

		result, err := activity.CreateSVMPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to create SVM peer")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenDeleteSVMPeerForHybridReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return SVM peer with rejected state
		svmPeer := &vsa.SvmPeer{
			UUID:  "test-svm-peer-uuid",
			State: "rejected",
		}
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(svmPeer, nil)
		// Mock DeleteSVMPeer to return error
		mockProvider.On("DeleteSVMPeer", "test-svm-peer-uuid", false).Return(fmt.Errorf("failed to delete SVM peer"))

		result, err := activity.CreateSVMPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to delete SVM peer")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenCreateSVMPeerForHybridReplicationFailsAfterDelete", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return SVM peer with rejected state
		svmPeer := &vsa.SvmPeer{
			UUID:  "test-svm-peer-uuid",
			State: "rejected",
		}
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(svmPeer, nil)
		// Mock DeleteSVMPeer to return success
		mockProvider.On("DeleteSVMPeer", "test-svm-peer-uuid", false).Return(nil)
		// Mock CreateSVMPeer to return error
		mockProvider.On("CreateSVMPeer", mock.Anything).Return(nil, fmt.Errorf("failed to create SVM peer"))

		result, err := activity.CreateSVMPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to create SVM peer")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenSVMPeerNotFoundAndCreated", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return not found error
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(nil, customerrors.NewNotFoundErr("SVM peer", nil))
		// Mock CreateSVMPeer to return success
		mockProvider.On("CreateSVMPeer", mock.Anything).Return(&vsa.SvmPeer{UUID: "test-svm-peer-uuid"}, nil)

		result, err := activity.CreateSVMPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenSVMPeerExistsWithAcceptableState", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return SVM peer with acceptable state (not rejected/suspended)
		svmPeer := &vsa.SvmPeer{
			UUID:  "test-svm-peer-uuid",
			State: "initializing",
		}
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(svmPeer, nil)

		result, err := activity.CreateSVMPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenSVMPeerDeletedAndRecreated", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName:     "test-peer-svm",
				PeerClusterName: "test-peer-cluster",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return SVM peer with rejected state
		svmPeer := &vsa.SvmPeer{
			UUID:  "test-svm-peer-uuid",
			State: "rejected",
		}
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(svmPeer, nil)
		// Mock DeleteSVMPeer to return success
		mockProvider.On("DeleteSVMPeer", "test-svm-peer-uuid", false).Return(nil)
		// Mock CreateSVMPeer to return success
		mockProvider.On("CreateSVMPeer", mock.Anything).Return(&vsa.SvmPeer{UUID: "new-svm-peer-uuid"}, nil)

		result, err := activity.CreateSVMPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		mockProvider.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_SetVolumeReplicationSVMPeeringDetails(t *testing.T) {
	t.Run("ErrorWhenUpdateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
				Name:      "test-replication",
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status:           models.HybridReplicationStatusPendingClusterPeer,
					StatusDetails:    "Waiting for cluster peering",
					StateDetailsCode: models.WaitingForClusterPeeringCode,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock UpdateVolumeReplication to return error
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(errors.New("database error"))

		result, err := activity.SetVolumeReplicationSVMPeeringDetails(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenVolumeReplicationIsUpdated", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
				Name:      "test-replication",
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status:           models.HybridReplicationStatusPendingClusterPeer,
					StatusDetails:    "Waiting for cluster peering",
					StateDetailsCode: models.WaitingForClusterPeeringCode,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock UpdateVolumeReplication to return success
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.Status == models.HybridReplicationStatusPendingSVMPeer &&
				vr.HybridReplicationAttributes.StatusDetails == models.WaitingForSVMPeering &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.WaitingForSVMPeeringCode) &&
				vr.HybridReplicationAttributes.SvmPeerCommand != nil &&
				*vr.HybridReplicationAttributes.SvmPeerCommand == "vserver peer accept -vserver test-peer-svm -peer-vserver test-local-svm"
		})).Return(nil)

		result, err := activity.SetVolumeReplicationSVMPeeringDetails(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.HybridReplicationStatusPendingSVMPeer, result.DbVolReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, models.WaitingForSVMPeering, result.DbVolReplication.HybridReplicationAttributes.StatusDetails)
		assert.Equal(tt, int32(models.WaitingForSVMPeeringCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		assert.NotNil(tt, result.DbVolReplication.HybridReplicationAttributes.SvmPeerCommand)
		assert.Equal(tt, "vserver peer accept -vserver test-peer-svm -peer-vserver test-local-svm", *result.DbVolReplication.HybridReplicationAttributes.SvmPeerCommand)
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_SetSVMPeeringToPeered(t *testing.T) {
	t.Run("SuccessWhenVolumeReplicationStatusIsUpdatedToSVMPeered", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				Status:           models.HybridReplicationStatusPendingSVMPeer,
				StatusDetails:    "Waiting for SVM peering",
				StateDetailsCode: models.WaitingForSVMPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.Status == models.HybridReplicationStatusSVMPeered &&
				vr.HybridReplicationAttributes.StatusDetails == "" &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.DefaultCode)
		})).Return(nil)

		result, err := activity.SetSVMPeeringToPeered(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.HybridReplicationStatusSVMPeered, result.DbVolReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, "", result.DbVolReplication.HybridReplicationAttributes.StatusDetails)
		assert.Equal(tt, int32(models.DefaultCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenHybridReplicationAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel:                   datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:                        "test-replication",
			HybridReplicationAttributes: nil,
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication - should be called with nil attributes unchanged
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes == nil
		})).Return(nil)

		result, err := activity.SetSVMPeeringToPeered(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.DbVolReplication.HybridReplicationAttributes)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenUpdateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				Status:           models.HybridReplicationStatusPendingSVMPeer,
				StatusDetails:    "Waiting for SVM peering",
				StateDetailsCode: models.WaitingForSVMPeeringCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication to return error
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(errors.New("database error"))

		result, err := activity.SetSVMPeeringToPeered(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenHybridReplicationAttributesHasAllFields", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				Status:                        models.HybridReplicationStatusPendingSVMPeer,
				StatusDetails:                 "Waiting for SVM peering",
				StateDetailsCode:              models.WaitingForSVMPeeringCode,
				SvmPeerCommand:                nillable.ToPointer("vserver peer accept -vserver peer-svm -peer-vserver local-svm"),
				SvmPeerExpiryTime:             nillable.ToPointer(time.Now().Add(24 * time.Hour)),
				Description:                   "Test replication",
				Labels:                        map[string]string{"env": "test"},
				PeerVolumeName:                "peer-volume",
				PeerSvmName:                   "peer-svm",
				ReplicationSchedule:           "hourly",
				HybridReplicationType:         nillable.ToPointer("continuous"),
				HybridReplicationUserCommands: []string{"command1", "command2"},
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.Status == models.HybridReplicationStatusSVMPeered &&
				vr.HybridReplicationAttributes.StatusDetails == "" &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.DefaultCode) &&
				// Verify that other fields are preserved
				vr.HybridReplicationAttributes.SvmPeerCommand != nil &&
				*vr.HybridReplicationAttributes.SvmPeerCommand == "vserver peer accept -vserver peer-svm -peer-vserver local-svm" &&
				vr.HybridReplicationAttributes.Description == "Test replication" &&
				vr.HybridReplicationAttributes.PeerVolumeName == "peer-volume" &&
				vr.HybridReplicationAttributes.PeerSvmName == "peer-svm" &&
				vr.HybridReplicationAttributes.ReplicationSchedule == "hourly"
		})).Return(nil)

		result, err := activity.SetSVMPeeringToPeered(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.HybridReplicationStatusSVMPeered, result.DbVolReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, "", result.DbVolReplication.HybridReplicationAttributes.StatusDetails)
		assert.Equal(tt, int32(models.DefaultCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		// Verify that other fields are preserved
		assert.NotNil(tt, result.DbVolReplication.HybridReplicationAttributes.SvmPeerCommand)
		assert.Equal(tt, "vserver peer accept -vserver peer-svm -peer-vserver local-svm", *result.DbVolReplication.HybridReplicationAttributes.SvmPeerCommand)
		assert.Equal(tt, "Test replication", result.DbVolReplication.HybridReplicationAttributes.Description)
		assert.Equal(tt, "peer-volume", result.DbVolReplication.HybridReplicationAttributes.PeerVolumeName)
		assert.Equal(tt, "peer-svm", result.DbVolReplication.HybridReplicationAttributes.PeerSvmName)
		assert.Equal(tt, "hourly", result.DbVolReplication.HybridReplicationAttributes.ReplicationSchedule)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenVolumeReplicationHasEmptyHybridReplicationAttributes", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				// Empty attributes
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.Status == models.HybridReplicationStatusSVMPeered &&
				vr.HybridReplicationAttributes.StatusDetails == "" &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.DefaultCode)
		})).Return(nil)

		result, err := activity.SetSVMPeeringToPeered(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.HybridReplicationStatusSVMPeered, result.DbVolReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, "", result.DbVolReplication.HybridReplicationAttributes.StatusDetails)
		assert.Equal(tt, int32(models.DefaultCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenStatusIsAlreadySVMPeered", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test-replication",
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				Status:           models.HybridReplicationStatusSVMPeered,
				StatusDetails:    "Already peered",
				StateDetailsCode: models.DefaultCode,
			},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: volumeReplication,
		}

		// Mock UpdateVolumeReplication
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(vr *datamodel.VolumeReplication) bool {
			return vr.HybridReplicationAttributes != nil &&
				vr.HybridReplicationAttributes.Status == models.HybridReplicationStatusSVMPeered &&
				vr.HybridReplicationAttributes.StatusDetails == "" &&
				vr.HybridReplicationAttributes.StateDetailsCode == int32(models.DefaultCode)
		})).Return(nil)

		result, err := activity.SetSVMPeeringToPeered(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.HybridReplicationStatusSVMPeered, result.DbVolReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, "", result.DbVolReplication.HybridReplicationAttributes.StatusDetails)
		assert.Equal(tt, int32(models.DefaultCode), result.DbVolReplication.HybridReplicationAttributes.StateDetailsCode)
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_WaitForSVMPeerForHybridReplication(t *testing.T) {
	t.Run("ErrorWhenGetProviderByNodeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("failed to get provider")
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		result, err := activity.WaitForSVMPeerForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get provider")
	})

	t.Run("ErrorWhenGetSVMPeerFailsWithNonNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return non-not-found error
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("connection error"))

		result, err := activity.WaitForSVMPeerForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "connection error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenDeleteSVMPeerForHybridReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return SVM peer with rejected state
		svmPeer := &vsa.SvmPeer{
			UUID:  "test-svm-peer-uuid",
			State: "rejected",
		}
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(svmPeer, nil)
		// Mock DeleteSVMPeer to return error
		mockProvider.On("DeleteSVMPeer", "test-svm-peer-uuid", false).Return(fmt.Errorf("failed to delete SVM peer"))

		result, err := activity.WaitForSVMPeerForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to delete SVM peer")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenSvmPeerIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return nil (not found)
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(nil, customerrors.NewNotFoundErr("SVM peer", nil))

		result, err := activity.WaitForSVMPeerForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "svm peer not found")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenSvmPeerStateIsPeered", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetSVMPeer to return SVM peer with peered state
		svmPeer := &vsa.SvmPeer{
			UUID:  "test-svm-peer-uuid",
			State: "peered",
		}
		mockProvider.On("GetSVMPeer", mock.Anything, mock.Anything).Return(svmPeer, nil)

		result, err := activity.WaitForSVMPeerForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		mockProvider.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_CreateHybridVolumeReplicationInternal(t *testing.T) {
	t.Run("ErrorWhenGetProviderByNodeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DstReplication: nil,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "test-endpoint",
					SourceHostName:        "source-host",
					SourceVolumeName:      "source-vol",
					DestinationHostName:   "dest-host",
					DestinationVolumeName: "dest-vol",
				},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					ReplicationSchedule: "test-schedule",
				},
				Volume: &datamodel.Volume{
					VolumeAttributes: &datamodel.VolumeAttributes{
						ExternalUUID: "test-external-uuid",
					},
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-dest-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("provider error")
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		result, err := activity.CreateHybridVolumeReplicationInternal(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("ErrorWhenCreateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DstReplication: nil,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "test-endpoint",
					SourceHostName:        "source-host",
					SourceVolumeName:      "source-vol",
					DestinationHostName:   "dest-host",
					DestinationVolumeName: "dest-vol",
				},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					ReplicationSchedule: "test-schedule",
				},
				Volume: &datamodel.Volume{
					VolumeAttributes: &datamodel.VolumeAttributes{
						ExternalUUID: "test-external-uuid",
					},
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-dest-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock CreateVolumeReplication to return error
		mockProvider.On("CreateVolumeReplication", mock.Anything).Return(nil, fmt.Errorf("create replication error"))

		result, err := activity.CreateHybridVolumeReplicationInternal(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "create replication error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenDstReplicationIsNotNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		existingReplication := &googleproxyclient.VolumeReplicationInternalV1beta{
			LifeCycleState: googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateAvailable),
		}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider:   &models.Node{Name: "test-node"},
			DstReplication: existingReplication,
		}

		result, err := activity.CreateHybridVolumeReplicationInternal(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.Equal(tt, existingReplication, result.DstReplication)
	})

	t.Run("SuccessWhenReplicationIsCreated", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			NodeProvider: &models.Node{
				Name: "test-node",
			},
			DstReplication: nil,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "test-endpoint",
					SourceHostName:        "source-host",
					SourceVolumeName:      "source-vol",
					DestinationHostName:   "dest-host",
					DestinationVolumeName: "dest-vol",
				},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					ReplicationSchedule: "test-schedule",
				},
				Volume: &datamodel.Volume{
					VolumeAttributes: &datamodel.VolumeAttributes{
						ExternalUUID: "test-external-uuid",
					},
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-dest-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
		}

		// Mock GetProviderByNode to return success
		mockProvider := &vsa.MockProvider{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock CreateVolumeReplication to return success
		createdReplication := &vsa.VolumeReplication{
			RelationshipID: "test-relationship-id",
		}
		mockProvider.On("CreateVolumeReplication", mock.Anything).Return(createdReplication, nil)

		result, err := activity.CreateHybridVolumeReplicationInternal(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, createdReplication, result.ReplicationCreateResponseONTAP)
		mockProvider.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_UpdateHybridVolumeReplicationDetailsAndSetPeeringStatusToPeered(t *testing.T) {
	t.Run("ErrorWhenUpdateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingSVMPeer,
				},
			},
			ReplicationCreateResponseONTAP: &vsa.VolumeReplication{
				RelationshipID:        "test-relationship-id",
				ReplicationSchedule:   "test-schedule",
				MirrorState:           "test-mirror-state",
				RelationshipStatus:    "test-relationship-status",
				TotalTransferBytes:    1000,
				TotalTransferTimeSecs: 60,
				LastTransferSize:      500,
				LastTransferError:     "test-error",
				LastTransferDuration:  30,
				LastTransferEndTime:   nillable.GetTimePtr(time.Now()),
				LagTime:               5,
			},
		}

		// Mock UpdateVolumeReplication to return error
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(fmt.Errorf("update error"))

		result, err := activity.UpdateHybridVolumeReplicationDetailsAndSetPeeringStatusToPeered(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "update error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenReplicationIsUpdated", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingSVMPeer,
				},
			},
			ReplicationCreateResponseONTAP: &vsa.VolumeReplication{
				RelationshipID:        "test-relationship-id",
				ReplicationSchedule:   "test-schedule",
				MirrorState:           "test-mirror-state",
				RelationshipStatus:    "test-relationship-status",
				TotalTransferBytes:    1000,
				TotalTransferTimeSecs: 60,
				LastTransferSize:      500,
				LastTransferError:     "test-error",
				LastTransferDuration:  30,
				LastTransferEndTime:   nillable.GetTimePtr(time.Now()),
				LagTime:               5,
			},
		}

		// Mock UpdateVolumeReplication to return success
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(nil)

		result, err := activity.UpdateHybridVolumeReplicationDetailsAndSetPeeringStatusToPeered(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)

		// Verify that the replication was updated with correct values
		updatedReplication := result.DbVolReplication
		assert.Equal(tt, models.LifeCycleStateAvailable, updatedReplication.State)
		assert.Equal(tt, models.LifeCycleStateAvailableDetails, updatedReplication.StateDetails)
		assert.Equal(tt, "test-relationship-id", updatedReplication.ReplicationAttributes.ExternalUUID)
		assert.Equal(tt, "test-schedule", updatedReplication.ReplicationAttributes.ReplicationSchedule)
		assert.Equal(tt, "test-mirror-state", *updatedReplication.MirrorState)
		assert.Equal(tt, "test-relationship-status", *updatedReplication.RelationshipStatus)
		assert.Equal(tt, int64(1000), updatedReplication.TotalTransferBytes)
		assert.Equal(tt, int64(60), updatedReplication.TotalTransferTimeSecs)
		assert.Equal(tt, int64(500), updatedReplication.LastTransferSize)
		assert.Equal(tt, "test-error", updatedReplication.LastTransferError)
		assert.Equal(tt, int64(30), updatedReplication.LastTransferDuration)
		assert.Equal(tt, int64(5), updatedReplication.LagTime)

		// Verify hybrid replication attributes were updated
		assert.Equal(tt, int32(models.DefaultCode), updatedReplication.HybridReplicationAttributes.StateDetailsCode)
		assert.Equal(tt, models.HybridReplicationStatusPeered, updatedReplication.HybridReplicationAttributes.Status)
		assert.Equal(tt, "", updatedReplication.HybridReplicationAttributes.StatusDetails)
		assert.Nil(tt, updatedReplication.HybridReplicationAttributes.SvmPeerExpiryTime)
		assert.Nil(tt, updatedReplication.HybridReplicationAttributes.SvmPeerCommand)
		assert.Equal(tt, "", updatedReplication.HybridReplicationAttributes.Description)
		assert.Nil(tt, updatedReplication.HybridReplicationAttributes.Labels)
		assert.Equal(tt, "", updatedReplication.HybridReplicationAttributes.ReplicationSchedule)

		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_UpdateClusterPeeringInReplication(t *testing.T) {
	t.Run("ErrorWhenUpdateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				BaseModel: datamodel.BaseModel{ID: 123},
			},
		}

		// Mock UpdateVolumeReplication to return error
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(fmt.Errorf("update error"))

		result, err := activity.UpdateClusterPeeringInReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "update error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenReplicationIsUpdatedWithClusterPeerID", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{ID: 123},
		}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
			},
			ClusterPeeringRow: clusterPeeringRow,
		}

		// Mock UpdateVolumeReplication to return success
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(nil)

		result, err := activity.UpdateClusterPeeringInReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)

		// Verify that the replication was updated with cluster peer ID
		updatedReplication := result.DbVolReplication
		assert.True(tt, updatedReplication.ClusterPeerId.Valid)
		assert.Equal(tt, int64(123), updatedReplication.ClusterPeerId.Int64)

		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenReplicationIsUpdatedWithoutClusterPeerID", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
			},
			ClusterPeeringRow: nil,
		}

		// Mock UpdateVolumeReplication to return success
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(nil)

		result, err := activity.UpdateClusterPeeringInReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)

		// Verify that the replication was updated without cluster peer ID
		updatedReplication := result.DbVolReplication
		assert.False(tt, updatedReplication.ClusterPeerId.Valid)

		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_HydrateVolumeReplicationForHybridReplication(t *testing.T) {
	t.Run("ErrorWhenHydrateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DestinationProjectNumber: "test-project",
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "test-region",
					DestinationVolumeName: "test-volume",
				},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: nillable.ToPointer(string(models.HybridReplicationParametersReplicationTypeONPREM)),
					Labels:                map[string]string{"key": "value"},
				},
			},
		}

		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Mock HydrateVolumeReplication to return error
		originalHydrateVolumeReplication := HydrateVolumeReplication
		HydrateVolumeReplication = func(ctx context.Context, volumeReplication models.VolumeReplication, project string) error {
			return fmt.Errorf("hydration error")
		}
		defer func() { HydrateVolumeReplication = originalHydrateVolumeReplication }()

		result, err := activity.HydrateVolumeReplicationForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "hydration error")
	})

	t.Run("SuccessWhenHydrationIsEnabledAndSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DestinationProjectNumber: "test-project",
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "test-region",
					DestinationVolumeName: "test-volume",
				},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: nillable.ToPointer(string(models.HybridReplicationParametersReplicationTypeONPREM)),
					Labels:                map[string]string{"key": "value"},
				},
			},
		}

		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Mock HydrateVolumeReplication to return success
		originalHydrateVolumeReplication := HydrateVolumeReplication
		HydrateVolumeReplication = func(ctx context.Context, volumeReplication models.VolumeReplication, project string) error {
			return nil
		}
		defer func() { HydrateVolumeReplication = originalHydrateVolumeReplication }()

		result, err := activity.HydrateVolumeReplicationForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
	})

	t.Run("SuccessWhenHydrationIsDisabled", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
		}

		// Mock hydrationEnabled to be false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		result, err := activity.HydrateVolumeReplicationForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
	})
}

func TestHybridReplicationActivity_HydrateReplicationStateForHybridReplication(t *testing.T) {
	t.Run("SuccessWhenHydrationIsDisabled", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
		}

		// Mock hydrationEnabled to be false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		result, err := activity.HydrateReplicationStateForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
	})

	t.Run("SuccessWhenHydrationIsEnabledButNoStateChangeNeeded", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DestinationProjectNumber: "test-project",
			ClusterPeer: &vsa.ClusterPeer{
				Availability: models.AvailabilityAvailable,
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name:  "test-replication",
				State: models.LifeCycleStateAvailable,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "test-region",
					DestinationVolumeName: "test-volume",
				},
			},
			CurrentHydrateState: models.VolumeReplicationHydrateStateReady,
		}

		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		result, err := activity.HydrateReplicationStateForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
	})

	t.Run("SuccessWhenHydrationIsEnabledButAlreadyInCorrectState", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DestinationProjectNumber: "test-project",
			ClusterPeer: &vsa.ClusterPeer{
				Availability: "unavailable",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "test-region",
					DestinationVolumeName: "test-volume",
				},
			},
			CurrentHydrateState: models.VolumeReplicationHydrateStatePendingClusterPeering,
		}

		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		result, err := activity.HydrateReplicationStateForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
	})

	t.Run("SuccessWhenClusterPeerUnavailableAndNeedsStateChange", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DestinationProjectNumber: "test-project",
			ClusterPeer: &vsa.ClusterPeer{
				Availability: "unavailable",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "test-region",
					DestinationVolumeName: "test-volume",
				},
			},
			CurrentHydrateState: "",
		}

		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true

		// Mock the hydrateReplicationStateForHybrid function to avoid actual hydration calls
		originalHydrateReplicationStateForHybrid := hydrateReplicationStateForHybrid
		hydrateReplicationStateForHybrid = func(ctx context.Context, createReplicationResponse models.VolumeReplication, replicationState models.VolumeReplicationHydrateState, project string) error {
			// Mock successful hydration
			return nil
		}
		defer func() {
			hydrateReplicationStateForHybrid = originalHydrateReplicationStateForHybrid
			hydrationEnabled = originalHydrationEnabled
		}()

		result, err := activity.HydrateReplicationStateForHybridReplication(ctx, &replicationResult)

		// Now the test should pass without authentication errors
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.VolumeReplicationHydrateStatePendingClusterPeering, result.CurrentHydrateState)
	})

	t.Run("SuccessWhenClusterPeerAvailableAndReplicationAvailableAndNeedsStateChange", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DestinationProjectNumber: "test-project",
			ClusterPeer: &vsa.ClusterPeer{
				Availability: models.AvailabilityAvailable,
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name:  "test-replication",
				State: models.LifeCycleStateAvailable,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "test-region",
					DestinationVolumeName: "test-volume",
				},
			},
			CurrentHydrateState: "",
		}

		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true

		// Mock the hydrateReplicationStateForHybrid function to avoid actual hydration calls
		originalHydrateReplicationStateForHybrid := hydrateReplicationStateForHybrid
		hydrateReplicationStateForHybrid = func(ctx context.Context, createReplicationResponse models.VolumeReplication, replicationState models.VolumeReplicationHydrateState, project string) error {
			// Mock successful hydration
			return nil
		}
		defer func() {
			hydrateReplicationStateForHybrid = originalHydrateReplicationStateForHybrid
			hydrationEnabled = originalHydrationEnabled
		}()

		result, err := activity.HydrateReplicationStateForHybridReplication(ctx, &replicationResult)

		// Now the test should pass without authentication errors
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.VolumeReplicationHydrateStateReady, result.CurrentHydrateState)
	})

	t.Run("SuccessWhenClusterPeerAvailableAndReplicationNotAvailableAndNeedsStateChange", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DestinationProjectNumber: "test-project",
			ClusterPeer: &vsa.ClusterPeer{
				Availability: models.AvailabilityAvailable,
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name:  "test-replication",
				State: "creating", // Not available state
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "test-region",
					DestinationVolumeName: "test-volume",
				},
			},
			CurrentHydrateState: "",
		}

		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true

		// Mock the hydrateReplicationStateForHybrid function to avoid actual hydration calls
		originalHydrateReplicationStateForHybrid := hydrateReplicationStateForHybrid
		hydrateReplicationStateForHybrid = func(ctx context.Context, createReplicationResponse models.VolumeReplication, replicationState models.VolumeReplicationHydrateState, project string) error {
			// Mock successful hydration
			return nil
		}
		defer func() {
			hydrateReplicationStateForHybrid = originalHydrateReplicationStateForHybrid
			hydrationEnabled = originalHydrationEnabled
		}()

		result, err := activity.HydrateReplicationStateForHybridReplication(ctx, &replicationResult)

		// Now the test should pass without authentication errors
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.VolumeReplicationHydrateStatePendingSvmPeering, result.CurrentHydrateState)
	})

	t.Run("SuccessWhenClusterPeerAvailableAndReplicationNotAvailableAndNeedsStateChangeToPendingSvmPeering", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DestinationProjectNumber: "test-project",
			ClusterPeer: &vsa.ClusterPeer{
				Availability: models.AvailabilityAvailable,
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:   "test-region",
					DestinationVolumeName: "test-volume",
				},
			},
			CurrentHydrateState: "",
		}

		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true

		// Mock the hydrateReplicationStateForHybrid function to avoid actual hydration calls
		originalHydrateReplicationStateForHybrid := hydrateReplicationStateForHybrid
		hydrateReplicationStateForHybrid = func(ctx context.Context, createReplicationResponse models.VolumeReplication, replicationState models.VolumeReplicationHydrateState, project string) error {
			// Mock successful hydration
			return nil
		}
		defer func() {
			hydrateReplicationStateForHybrid = originalHydrateReplicationStateForHybrid
			hydrationEnabled = originalHydrationEnabled
		}()

		result, err := activity.HydrateReplicationStateForHybridReplication(ctx, &replicationResult)

		// Now the test should pass without authentication errors
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.VolumeReplicationHydrateStatePendingSvmPeering, result.CurrentHydrateState)
	})
}

func TestHybridReplicationActivity_UpdateClusterPeerDetailsOnErrorActivity(t *testing.T) {
	t.Run("ErrorWhenUpdateClusterPeeringRowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				BaseModel:     datamodel.BaseModel{ID: 123},
				State:         models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
				OntapPeerUUID: "test-uuid",
				ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
					PassPhrase: nillable.ToPointer("test-passphrase"),
					Command:    nillable.ToPointer("test-command"),
					ExpiryTime: nillable.GetTimePtr(time.Now()),
				},
			},
		}

		// Mock se.UpdateClusterPeeringRow to return error
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(clusterPeering *datamodel.ClusterPeerings) bool {
			return clusterPeering.ID == 123
		})).Return(fmt.Errorf("database error"))

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("DeleteClusterPeer", mock.Anything).Return(nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		err := activity.UpdateClusterPeerDetailsOnErrorActivity(ctx, &replicationResult)

		assert.Error(tt, err)
		customErr := vsaerrors.ExtractCustomError(err)
		assert.NotNil(tt, customErr)
		assert.NotNil(tt, customErr.OriginalErr)
		assert.Contains(tt, customErr.OriginalErr.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenCleanupAndDeletionSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				BaseModel:     datamodel.BaseModel{ID: 123},
				State:         models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
				OntapPeerUUID: "test-uuid",
				ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
					PassPhrase: nillable.ToPointer("test-passphrase"),
					Command:    nillable.ToPointer("test-command"),
					ExpiryTime: nillable.GetTimePtr(time.Now()),
				},
			},
		}

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(clusterPeering *datamodel.ClusterPeerings) bool {
			return clusterPeering.ID == 123
		})).Return(nil)

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("DeleteClusterPeer", mock.Anything).Return(nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		err := activity.UpdateClusterPeerDetailsOnErrorActivity(ctx, &replicationResult)

		assert.NoError(tt, err)

		// Verify that the cluster peering row was updated correctly
		clusterPeering := replicationResult.ClusterPeeringRow
		assert.Equal(tt, "", clusterPeering.OntapPeerUUID)
		assert.Nil(tt, clusterPeering.ClusterPeeringAttributes.PassPhrase)
		assert.Nil(tt, clusterPeering.ClusterPeeringAttributes.Command)
		assert.Nil(tt, clusterPeering.ClusterPeeringAttributes.ExpiryTime)
		assert.Equal(tt, models.LifeCycleStateDeleted, string(clusterPeering.State))
		assert.Equal(tt, models.LifeCycleStateDeletedDetails, clusterPeering.StateDetails)
		assert.True(tt, clusterPeering.DeletedAt.Valid)

		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_UpdateReplicationRowDetailsOnErrorActivity(t *testing.T) {
	t.Run("ErrorWhenUpdateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{ID: 123},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					SvmPeerExpiryTime: nillable.GetTimePtr(time.Now()),
					SvmPeerCommand:    nillable.ToPointer("test-command"),
				},
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				State: models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
			},
		}

		// Mock se.UpdateVolumeReplication to return error
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(fmt.Errorf("database error"))

		err := activity.UpdateReplicationRowDetailsOnErrorActivity(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenClusterPeerIsPending", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{ID: 123},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					SvmPeerExpiryTime: nillable.GetTimePtr(time.Now()),
					SvmPeerCommand:    nillable.ToPointer("test-command"),
				},
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				State: models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
			},
		}

		// Mock se.UpdateVolumeReplication to return success
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		err := activity.UpdateReplicationRowDetailsOnErrorActivity(ctx, &replicationResult)

		assert.NoError(tt, err)

		// Verify that the replication was updated correctly
		replication := replicationResult.DbVolReplication
		assert.Equal(tt, models.HybridReplicationStatusPendingClusterPeer, replication.HybridReplicationAttributes.Status)
		assert.Nil(tt, replication.HybridReplicationAttributes.SvmPeerExpiryTime)
		assert.Nil(tt, replication.HybridReplicationAttributes.SvmPeerCommand)
		assert.False(tt, replication.ClusterPeerId.Valid)

		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenClusterPeerIsPeeredAndSvmPeerIsPending", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{ID: 123},
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					SvmPeerExpiryTime: nillable.GetTimePtr(time.Now()),
					SvmPeerCommand:    nillable.ToPointer("test-command"),
				},
			},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				State: models.CvpClusterPeeringStatusPEERED,
			},
		}

		// Mock se.UpdateVolumeReplication to return success
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		err := activity.UpdateReplicationRowDetailsOnErrorActivity(ctx, &replicationResult)

		assert.NoError(tt, err)

		// Verify that the replication was updated correctly
		replication := replicationResult.DbVolReplication
		assert.Equal(tt, models.HybridReplicationStatusPendingClusterPeer, replication.HybridReplicationAttributes.Status)
		assert.Nil(tt, replication.HybridReplicationAttributes.SvmPeerExpiryTime)
		assert.Nil(tt, replication.HybridReplicationAttributes.SvmPeerCommand)
		assert.False(tt, replication.ClusterPeerId.Valid)

		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_GetOrCreateClusterPeerInOntapForHybridReplication(t *testing.T) {
	t.Run("ErrorWhenGetProviderByNodeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			NodeProvider: &models.Node{Name: "test-node"},
		}

		// Mock hyperscaler.GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("provider error")
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("ErrorWhenGetClusterPeerFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "test-uuid",
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetClusterPeer", mock.Anything).Return(nil, fmt.Errorf("connection error"))
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "connection error")
	})

	t.Run("ErrorWhenUpdateClusterPeeringRowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "",
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(&vsa.ClusterPeer{
			ExternalUUID: "new-uuid",
			Passphrase:   (*slogger.Secret)(nillable.ToPointer("test-passphrase")),
		}, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return error
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(fmt.Errorf("database error"))

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenCreateClusterPeerFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:  "test-resource-id",
				PeerSvmName: "test-svm",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(nil, fmt.Errorf("create cluster peer error"))
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Error during cluster peering")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenExistingClusterPeerIsReused", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "existing-uuid",
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetClusterPeer", mock.Anything).Return(&vsa.ClusterPeer{
			UUID:         "existing-uuid",
			Availability: models.AvailabilityAvailable,
		}, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.Equal(tt, "existing-uuid", result.ClusterPeer.UUID)
		assert.Equal(tt, models.AvailabilityAvailable, result.ClusterPeer.Availability)
	})

	t.Run("SuccessWhenNewClusterPeerIsCreated", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:  "test-resource-id",
				PeerSvmName: "test-svm",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(&vsa.ClusterPeer{
			ExternalUUID: "new-uuid",
			Passphrase:   (*slogger.Secret)(nillable.ToPointer("test-passphrase")),
		}, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.Equal(tt, "new-uuid", result.ClusterPeeringRow.OntapPeerUUID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SuccessWhenClusterPeerNotFoundInOntap", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:  "test-resource-id",
				PeerSvmName: "test-svm",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "existing-uuid",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetClusterPeer", mock.Anything).Return(nil, fmt.Errorf("cluster peer not found"))
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(&vsa.ClusterPeer{
			ExternalUUID: "new-uuid",
			Passphrase:   (*slogger.Secret)(nillable.ToPointer("test-passphrase")),
		}, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.Equal(tt, "new-uuid", result.ClusterPeer.ExternalUUID)
		assert.Equal(tt, "new-uuid", result.ClusterPeeringRow.OntapPeerUUID) // Should be set to new UUID
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenClusterPeeringRowAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:  "test-resource-id",
				PeerSvmName: "test-svm",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID:            "",
				ClusterPeeringAttributes: nil, // This should be initialized
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(&vsa.ClusterPeer{
			ExternalUUID: "new-uuid",
			Passphrase:   (*slogger.Secret)(nillable.ToPointer("test-passphrase")),
		}, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.NotNil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes) // Should be initialized
		assert.Equal(tt, "new-uuid", result.ClusterPeer.ExternalUUID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenCreateRoleFailsWithOtherError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:  "test-resource-id",
				PeerSvmName: "test-svm",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("", fmt.Errorf("role creation error"))
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "role creation error")
	})

	t.Run("ErrorWhenListClusterPeersFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:  "test-resource-id",
				PeerSvmName: "test-svm",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return(nil, fmt.Errorf("list cluster peers error"))
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "list cluster peers error")
	})

	t.Run("ErrorWhenExistingClusterPeerNotAvailable", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:      "test-resource-id",
				PeerSvmName:     "test-svm",
				PeerClusterName: "test-cluster",
				PeerIPAddresses: []string{"192.168.1.1"},
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{
			{
				PeerClusterName: "test-cluster",
				PeerAddresses:   []string{"192.168.1.1"},
				ExternalUUID:    "existing-uuid",
				Availability:    "unavailable", // Not available
			},
		}, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "cluster peer existing-uuid is not available")
	})

	t.Run("ErrorWhenCreateClusterPeerFailsWithSpecificErrors", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:  "test-resource-id",
				PeerSvmName: "test-svm",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(nil, fmt.Errorf("Error creating cluster peer - Max retries reached"))
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Source cluster is unreachable. Please verify that the peer address is correct and try again")
	})

	t.Run("SuccessWhenPassphraseIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:  "test-resource-id",
				PeerSvmName: "test-svm",
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(&vsa.ClusterPeer{
			ExternalUUID: "new-uuid",
			Passphrase:   nil, // No passphrase
		}, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to return success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil)
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.Equal(tt, "new-uuid", result.ClusterPeer.ExternalUUID)
		assert.Nil(tt, result.ClusterPeeringRow.ClusterPeeringAttributes.PassPhrase) // Should be nil
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ErrorWhenUpdateClusterPeeringRowFailsDuringReuse", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:      "test-resource-id",
				PeerSvmName:     "test-svm",
				PeerClusterName: "test-cluster",
				PeerIPAddresses: []string{"192.168.1.1"},
			},
			NodeProvider: &models.Node{Name: "test-node"},
			ClusterPeeringRow: &datamodel.ClusterPeerings{
				OntapPeerUUID: "", // Empty UUID to trigger Case 2
			},
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
			},
			DestinationVolume: &datamodel.Volume{
				Pool: &datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						InterclusterLifIPs: []string{"192.168.1.1"},
					},
				},
			},
		}

		// Mock hyperscaler.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("CreateRole", mock.Anything).Return("role-name", nil)
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{
			{
				PeerClusterName: "test-cluster",
				PeerAddresses:   []string{"192.168.1.1"},
				ExternalUUID:    "existing-uuid",
				Availability:    models.AvailabilityAvailable, // Available
			},
		}, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock se.UpdateClusterPeeringRow to succeed first (for Case 2), then fail during reuse
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(nil).Once()                                              // Case 2 success
		mockStorage.On("UpdateClusterPeeringRow", mock.Anything, mock.Anything).Return(fmt.Errorf("database update error during reuse")).Once() // Reuse failure
		// Mock se.UpdateVolumeReplication for updateReplicationStateDetailsCode
		mockStorage.On("UpdateVolumeReplication", mock.Anything, mock.Anything).Return(nil)

		result, err := activity.GetOrCreateClusterPeerInOntapForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database update error during reuse")
		mockStorage.AssertExpectations(tt)
	})
}

func TestModifyExternalVolumeReplicationSecurityRoleIfNeeded(t *testing.T) {
	t.Run("ErrorWhenGetRoleCollectionFails", func(tt *testing.T) {
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetRoleCollection", mock.Anything).Return(nil, fmt.Errorf("get role collection error"))

		// Function should return early without error when GetRoleCollection fails
		modifyExternalVolumeReplicationSecurityRoleIfNeeded(mockProvider, "test-role")

		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenModifyRolePrivilegeFails", func(tt *testing.T) {
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetRoleCollection", mock.Anything).Return([]*vsa.Role{
			{
				Name:    "test-role",
				OwnerID: "test-owner-id",
			},
		}, nil)
		mockProvider.On("ModifyRolePrivilege", mock.Anything).Return(fmt.Errorf("modify role privilege error"))

		// Function should return early without error when ModifyRolePrivilege fails
		modifyExternalVolumeReplicationSecurityRoleIfNeeded(mockProvider, "test-role")

		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenRoleIsFoundAndModified", func(tt *testing.T) {
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetRoleCollection", mock.Anything).Return([]*vsa.Role{
			{
				Name:    "test-role",
				OwnerID: "test-owner-id",
			},
		}, nil)
		mockProvider.On("ModifyRolePrivilege", mock.Anything).Return(nil)

		// Function should complete successfully
		modifyExternalVolumeReplicationSecurityRoleIfNeeded(mockProvider, "test-role")

		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenRoleIsNotFound", func(tt *testing.T) {
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetRoleCollection", mock.Anything).Return([]*vsa.Role{
			{
				Name:    "other-role",
				OwnerID: "test-owner-id",
			},
		}, nil)

		// Function should return early when role is not found
		modifyExternalVolumeReplicationSecurityRoleIfNeeded(mockProvider, "test-role")

		mockProvider.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_GetReplicationForHybridReplication(t *testing.T) {
	t.Run("ErrorWhenV1betaGetMultipleReplicationsInternalFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
			},
			DstBasePath:              nillable.ToPointer("test-base-path"),
			DstJwtToken:              nillable.ToPointer("test-jwt-token"),
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "test-region",
			CorrelationID:            nillable.ToPointer("test-correlation-id"),
		}

		// Mock googleproxyclient.GetGProxyClient to return a mock client
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("google proxy error"))
		mockClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}
		googleproxyclient.GetGProxyClient = func(basePath, jwtToken string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		result, err := activity.GetReplicationForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "google proxy error")
	})

	t.Run("ErrorWhenResponseIsBadRequest", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
			},
			DstBasePath:              nillable.ToPointer("test-base-path"),
			DstJwtToken:              nillable.ToPointer("test-jwt-token"),
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "test-region",
			CorrelationID:            nillable.ToPointer("test-correlation-id"),
		}

		// Mock googleproxyclient.GetGProxyClient to return a mock client
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		badRequestResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest{
			Message: "bad request error",
		}
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", mock.Anything, mock.Anything, mock.Anything).Return(badRequestResponse, nil)
		mockClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}
		googleproxyclient.GetGProxyClient = func(basePath, jwtToken string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		result, err := activity.GetReplicationForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "bad request error")
	})

	t.Run("ErrorWhenResponseIsInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
			},
			DstBasePath:              nillable.ToPointer("test-base-path"),
			DstJwtToken:              nillable.ToPointer("test-jwt-token"),
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "test-region",
			CorrelationID:            nillable.ToPointer("test-correlation-id"),
		}

		// Mock googleproxyclient.GetGProxyClient to return a mock client
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		internalServerErrorResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError{
			Message: "internal server error",
		}
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", mock.Anything, mock.Anything, mock.Anything).Return(internalServerErrorResponse, nil)
		mockClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}
		googleproxyclient.GetGProxyClient = func(basePath, jwtToken string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		result, err := activity.GetReplicationForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "internal server error")
	})

	t.Run("ErrorWhenResponseIsUnauthorized", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
			},
			DstBasePath:              nillable.ToPointer("test-base-path"),
			DstJwtToken:              nillable.ToPointer("test-jwt-token"),
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "test-region",
			CorrelationID:            nillable.ToPointer("test-correlation-id"),
		}

		// Mock googleproxyclient.GetGProxyClient to return a mock client
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		unauthorizedResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized{
			Message: "unauthorized error",
		}
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", mock.Anything, mock.Anything, mock.Anything).Return(unauthorizedResponse, nil)
		mockClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}
		googleproxyclient.GetGProxyClient = func(basePath, jwtToken string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		result, err := activity.GetReplicationForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "unauthorized error")
	})

	t.Run("ErrorWhenResponseIsForbidden", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
			},
			DstBasePath:              nillable.ToPointer("test-base-path"),
			DstJwtToken:              nillable.ToPointer("test-jwt-token"),
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "test-region",
			CorrelationID:            nillable.ToPointer("test-correlation-id"),
		}

		// Mock googleproxyclient.GetGProxyClient to return a mock client
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		forbiddenResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden{
			Message: "forbidden error",
		}
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", mock.Anything, mock.Anything, mock.Anything).Return(forbiddenResponse, nil)
		mockClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}
		googleproxyclient.GetGProxyClient = func(basePath, jwtToken string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		result, err := activity.GetReplicationForHybridReplication(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "forbidden error")
	})

	t.Run("SuccessWhenResponseIsOKWithReplications", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
			},
			DstBasePath:              nillable.ToPointer("test-base-path"),
			DstJwtToken:              nillable.ToPointer("test-jwt-token"),
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "test-region",
			CorrelationID:            nillable.ToPointer("test-correlation-id"),
		}

		// Mock googleproxyclient.GetGProxyClient to return a mock client
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		okResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					VolumeReplicationUuid: googleproxyclient.OptString{
						Value: "test-replication-uuid",
					},
				},
			},
		}
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", mock.Anything, mock.Anything, mock.Anything).Return(okResponse, nil)
		mockClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}
		googleproxyclient.GetGProxyClient = func(basePath, jwtToken string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		result, err := activity.GetReplicationForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.NotNil(tt, result.DstReplication)
		assert.Equal(tt, "test-replication-uuid", result.DstReplication.VolumeReplicationUuid.Value)
	})

	t.Run("SuccessWhenResponseIsNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ExternalUUID: "test-uuid",
				},
			},
			DstBasePath:              nillable.ToPointer("test-base-path"),
			DstJwtToken:              nillable.ToPointer("test-jwt-token"),
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "test-region",
			CorrelationID:            nillable.ToPointer("test-correlation-id"),
		}

		// Mock googleproxyclient.GetGProxyClient to return a mock client
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		notFoundResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound{
			Message: "not found",
		}
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", mock.Anything, mock.Anything, mock.Anything).Return(notFoundResponse, nil)
		mockClient := &googleproxyclient.ProxyClient{Invoker: mockInvoker}
		googleproxyclient.GetGProxyClient = func(basePath, jwtToken string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		result, err := activity.GetReplicationForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.Nil(tt, result.DstReplication)
	})

	t.Run("SuccessWhenDbVolReplicationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := replication.CreateHybridReplicationResult{
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-resource-id",
			},
			DbVolReplication: nil,
		}

		result, err := activity.GetReplicationForHybridReplication(ctx, &replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &replicationResult, result)
		assert.Nil(tt, result.DstReplication)
	})
}

func TestHybridReplicationActivity_CreateLocalVolumeReplicationRow(t *testing.T) {
	t.Run("WhenVolumeReplicationAlreadyExists", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		existingReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "existing-replication-uuid"},
			Name:      "test-replication",
			Uri:       "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id",
		}

		replicationResult := &replication.CreateHybridReplicationResult{
			DbVolReplication: existingReplication,
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID: "test-replication-id",
			},
		}

		result, err := activity.CreateLocalHybridReplicationRow(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.Equal(tt, replicationResult, result)
		assert.Equal(tt, existingReplication, result.DbVolReplication)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenExistingReplicationFoundInDatabase", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		existingReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "existing-replication-uuid"},
			Name:      "test-replication",
			Uri:       "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id",
		}

		replicationResult := &replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
					Name:      "test-pool",
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "test-cluster",
					},
				},
				Svm: &datamodel.Svm{
					Name: "test-svm",
				},
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:          "test-replication-id",
				ReplicationType:     models.HybridReplicationParametersReplicationTypeCONTINUOUS,
				ReplicationSchedule: "hourly",
				PeerSvmName:         "peer-svm",
				PeerClusterName:     "peer-cluster",
				PeerVolumeName:      "peer-volume",
				Description:         "test description",
				Labels:              map[string]string{"env": "test"},
			},
		}

		// Mock the filter and ListVolumeReplications call
		mockStorage.On("ListVolumeReplications", ctx, mock.MatchedBy(func(filter utils.Filter) bool {
			return true // We'll verify the filter conditions in the assertion
		}), mock.Anything).Return([]*datamodel.VolumeReplication{existingReplication}, nil)

		result, err := activity.CreateLocalHybridReplicationRow(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, customerrors.IsConflictErr(err), "Expected conflict error")
		assert.Contains(tt, err.Error(), "Volume replication with URI 'projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id' already exists")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenNoExistingReplicationFoundAndCreateNewSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := &replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
					Name:      "test-pool",
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "test-cluster",
					},
				},
				Svm: &datamodel.Svm{
					Name: "test-svm",
				},
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:          "test-replication-id",
				ReplicationType:     models.HybridReplicationParametersReplicationTypeCONTINUOUS,
				ReplicationSchedule: "hourly",
				PeerSvmName:         "peer-svm",
				PeerClusterName:     "peer-cluster",
				PeerVolumeName:      "peer-volume",
				Description:         "test description",
				Labels:              map[string]string{"env": "test"},
			},
		}

		createdReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "new-replication-uuid"},
			Name:      "test-replication-id",
			Uri:       "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id",
			AccountID: 123,
			VolumeID:  456,
		}

		// Mock the filter and ListVolumeReplications call to return empty result
		mockStorage.On("ListVolumeReplications", ctx, mock.MatchedBy(func(filter utils.Filter) bool {
			return true // We'll verify the filter conditions in the assertion
		}), mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)

		// Mock CreateVolumeReplication call
		mockStorage.On("CreateVolumeReplication", ctx, mock.MatchedBy(func(replication *datamodel.VolumeReplication) bool {
			return replication.Uri == "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id" &&
				replication.Name == "test-replication-id" &&
				replication.AccountID == 123 &&
				replication.VolumeID == 456 &&
				replication.ReplicationAttributes != nil &&
				replication.HybridReplicationAttributes != nil
		})).Return(createdReplication, nil)

		result, err := activity.CreateLocalHybridReplicationRow(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.Equal(tt, replicationResult, result)
		assert.Equal(tt, createdReplication, result.DbVolReplication)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenListVolumeReplicationsReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := &replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
					Name:      "test-pool",
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "test-cluster",
					},
				},
				Svm: &datamodel.Svm{
					Name: "test-svm",
				},
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:          "test-replication-id",
				ReplicationType:     models.HybridReplicationParametersReplicationTypeCONTINUOUS,
				ReplicationSchedule: "hourly",
				PeerSvmName:         "peer-svm",
				PeerClusterName:     "peer-cluster",
				PeerVolumeName:      "peer-volume",
				Description:         "test description",
				Labels:              map[string]string{"env": "test"},
			},
		}

		createdReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "new-replication-uuid"},
			Name:      "test-replication-id",
			Uri:       "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id",
			AccountID: 123,
			VolumeID:  456,
		}

		// Mock the filter and ListVolumeReplications call to return error
		mockStorage.On("ListVolumeReplications", ctx, mock.MatchedBy(func(filter utils.Filter) bool {
			return true
		}), mock.Anything).Return(nil, errors.New("database error"))

		// Mock CreateVolumeReplication call
		mockStorage.On("CreateVolumeReplication", ctx, mock.MatchedBy(func(replication *datamodel.VolumeReplication) bool {
			return replication.Uri == "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id" &&
				replication.Name == "test-replication-id" &&
				replication.AccountID == 123 &&
				replication.VolumeID == 456
		})).Return(createdReplication, nil)

		result, err := activity.CreateLocalHybridReplicationRow(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.Equal(tt, replicationResult, result)
		assert.Equal(tt, createdReplication, result.DbVolReplication)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenCreateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := &replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
					Name:      "test-pool",
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "test-cluster",
					},
				},
				Svm: &datamodel.Svm{
					Name: "test-svm",
				},
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:          "test-replication-id",
				ReplicationType:     models.HybridReplicationParametersReplicationTypeCONTINUOUS,
				ReplicationSchedule: "hourly",
				PeerSvmName:         "peer-svm",
				PeerClusterName:     "peer-cluster",
				PeerVolumeName:      "peer-volume",
				Description:         "test description",
				Labels:              map[string]string{"env": "test"},
			},
		}

		// Mock the filter and ListVolumeReplications call to return empty result
		mockStorage.On("ListVolumeReplications", ctx, mock.MatchedBy(func(filter utils.Filter) bool {
			return true
		}), mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)

		// Mock CreateVolumeReplication call to return error
		mockStorage.On("CreateVolumeReplication", ctx, mock.MatchedBy(func(replication *datamodel.VolumeReplication) bool {
			return replication.Uri == "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id" &&
				replication.Name == "test-replication-id" &&
				replication.AccountID == 123 &&
				replication.VolumeID == 456
		})).Return(nil, errors.New("failed to create volume replication"))

		result, err := activity.CreateLocalHybridReplicationRow(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to create volume replication with AccountID 123, VolumeID 456")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenReplicationAttributesAreCorrectlySet", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		replicationResult := &replication.CreateHybridReplicationResult{
			DestinationVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 456},
				Name:      "test-volume",
				AccountID: 123,
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
					Name:      "test-pool",
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "test-cluster",
					},
				},
				Svm: &datamodel.Svm{
					Name: "test-svm",
				},
			},
			DestinationProjectNumber: "test-project",
			DestinationRegion:        "us-central1",
			HybridReplicationParameters: &models.HybridReplicationParameters{
				ResourceID:          "test-replication-id",
				ReplicationType:     models.HybridReplicationParametersReplicationTypeONPREM,
				ReplicationSchedule: "hourly",
				PeerSvmName:         "peer-svm",
				PeerClusterName:     "peer-cluster",
				PeerVolumeName:      "peer-volume",
				Description:         "test description",
				Labels:              map[string]string{"env": "test"},
			},
		}

		createdReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "new-replication-uuid"},
			Name:      "test-replication-id",
			Uri:       "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id",
			AccountID: 123,
			VolumeID:  456,
		}

		// Mock the filter and ListVolumeReplications call to return empty result
		mockStorage.On("ListVolumeReplications", ctx, mock.MatchedBy(func(filter utils.Filter) bool {
			return true
		}), mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)

		// Mock CreateVolumeReplication call with detailed verification
		mockStorage.On("CreateVolumeReplication", ctx, mock.MatchedBy(func(replication *datamodel.VolumeReplication) bool {
			// Verify replication attributes
			replAttrs := replication.ReplicationAttributes
			if replAttrs == nil {
				return false
			}

			// Verify hybrid replication attributes
			hybridAttrs := replication.HybridReplicationAttributes
			if hybridAttrs == nil {
				return false
			}

			return replication.Uri == "projects/test-project/locations/us-central1/volumes/test-volume/replications/test-replication-id" &&
				replication.Name == "test-replication-id" &&
				replication.AccountID == 123 &&
				replication.VolumeID == 456 &&
				replAttrs.EndpointType == database.VolumeReplicationEndpointTypeDestination &&
				replAttrs.ReplicationType == ReplicationTypeExternalDisasterRecovery &&
				replAttrs.ReplicationSchedule == "hourly" &&
				replAttrs.SourceSvmName == "peer-svm" &&
				replAttrs.SourceHostName == "peer-cluster" &&
				replAttrs.SourceVolumeName == "peer-volume" &&
				replAttrs.DestinationPoolUUID == "test-pool-uuid" &&
				replAttrs.DestinationVolumeUUID == "test-volume-uuid" &&
				replAttrs.DestinationLocation == "us-central1" &&
				replAttrs.DestinationHostName == "test-cluster" &&
				replAttrs.DestinationVolumeName == "test-volume" &&
				replAttrs.DestinationSvmName == "test-svm" &&
				hybridAttrs.Status == models.HybridReplicationStatusPendingClusterPeer &&
				hybridAttrs.HybridReplicationType != nil &&
				*hybridAttrs.HybridReplicationType == string(models.HybridReplicationParametersReplicationTypeONPREM) &&
				hybridAttrs.Description == "test description" &&
				hybridAttrs.PeerVolumeName == "peer-volume" &&
				hybridAttrs.PeerSvmName == "peer-svm" &&
				hybridAttrs.ReplicationSchedule == "hourly" &&
				hybridAttrs.StateDetailsCode == models.DefaultCode
		})).Return(createdReplication, nil)

		result, err := activity.CreateLocalHybridReplicationRow(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.Equal(tt, replicationResult, result)
		assert.Equal(tt, createdReplication, result.DbVolReplication)
		mockStorage.AssertExpectations(tt)
	})
}

func TestHybridReplicationActivity_UpdateSVMPeerOnErrorActivity(t *testing.T) {
	t.Run("SuccessWhenStatusIsPendingSVMPeerAndSVMPeerExists", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Create test data
		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingSVMPeer,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
			NodeProvider: &models.Node{
				Name: "test-node",
			},
		}

		// Mock hyperscaler provider
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetSVMPeer", mock.AnythingOfType("*string"), mock.AnythingOfType("*string")).Return(&vsa.SvmPeer{
			UUID:  "test-svm-peer-uuid",
			State: "peered",
		}, nil)
		mockProvider.On("DeleteSVMPeer", "test-svm-peer-uuid", false).Return(nil)

		// Mock hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		err := activity.UpdateSVMPeerOnErrorActivity(ctx, &replicationResult)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenStatusIsNotPendingSVMPeer", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Create test data with different status
		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingClusterPeer,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
			NodeProvider: &models.Node{
				Name: "test-node",
			},
		}

		err := activity.UpdateSVMPeerOnErrorActivity(ctx, &replicationResult)

		assert.NoError(tt, err)
		// No provider calls should be made since status is not PendingSVMPeer
	})

	t.Run("SuccessWhenStatusIsPendingSVMPeerButSVMPeerNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Create test data
		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingSVMPeer,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
			NodeProvider: &models.Node{
				Name: "test-node",
			},
		}

		// Mock hyperscaler provider to return NotFound error
		mockProvider := &vsa.MockProvider{}
		notFoundErr := customerrors.NewNotFoundErr("SVM peer", nil)
		mockProvider.On("GetSVMPeer", mock.AnythingOfType("*string"), mock.AnythingOfType("*string")).Return(nil, notFoundErr)

		// Mock hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		err := activity.UpdateSVMPeerOnErrorActivity(ctx, &replicationResult)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SuccessWhenStatusIsPendingSVMPeerButSVMPeerIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Create test data
		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingSVMPeer,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
			NodeProvider: &models.Node{
				Name: "test-node",
			},
		}

		// Mock hyperscaler provider to return nil SVM peer
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetSVMPeer", mock.AnythingOfType("*string"), mock.AnythingOfType("*string")).Return(nil, nil)

		// Mock hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		err := activity.UpdateSVMPeerOnErrorActivity(ctx, &replicationResult)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetProviderByNodeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Create test data
		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingSVMPeer,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
			NodeProvider: &models.Node{
				Name: "test-node",
			},
		}

		// Mock hyperscaler.GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		err := activity.UpdateSVMPeerOnErrorActivity(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("ErrorWhenGetSVMPeerFailsWithNonNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Create test data
		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingSVMPeer,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
			NodeProvider: &models.Node{
				Name: "test-node",
			},
		}

		// Mock hyperscaler provider to return non-NotFound error
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetSVMPeer", mock.AnythingOfType("*string"), mock.AnythingOfType("*string")).Return(nil, errors.New("connection error"))

		// Mock hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		err := activity.UpdateSVMPeerOnErrorActivity(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "connection error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ErrorWhenDeleteSVMPeerFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := HybridReplicationActivity{SE: mockStorage}

		// Create test data
		replicationResult := replication.CreateHybridReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					Status: models.HybridReplicationStatusPendingSVMPeer,
				},
			},
			DestinationVolume: &datamodel.Volume{
				Svm: &datamodel.Svm{
					Name: "test-local-svm",
				},
			},
			HybridReplicationParameters: &models.HybridReplicationParameters{
				PeerSvmName: "test-peer-svm",
			},
			NodeProvider: &models.Node{
				Name: "test-node",
			},
		}

		// Mock hyperscaler provider
		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetSVMPeer", mock.AnythingOfType("*string"), mock.AnythingOfType("*string")).Return(&vsa.SvmPeer{
			UUID:  "test-svm-peer-uuid",
			State: "peered",
		}, nil)
		mockProvider.On("DeleteSVMPeer", "test-svm-peer-uuid", false).Return(errors.New("delete error"))

		// Mock hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		err := activity.UpdateSVMPeerOnErrorActivity(ctx, &replicationResult)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "delete error")
		mockProvider.AssertExpectations(tt)
	})
}

func TestMountReplicationAfterHybridReplicationCreate(t *testing.T) {
	t.Run("Success_ReturnsJobIdFromInternalJobResponse", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		// Setup test data
		dstBasePath := "https://test-dst-base-path.com"
		dstJwtToken := "test-jwt-token"
		destinationProjectNumber := "123456789"
		correlationID := "test-correlation-id"
		jobUUID := "test-job-uuid-12345"
		destinationLocation := "us-central1"
		destinationReplicationUUID := "dest-replication-uuid-123"

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              &dstBasePath,
			DstJwtToken:              &dstJwtToken,
			DestinationProjectNumber: destinationProjectNumber,
			CorrelationID:            &correlationID,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        destinationLocation,
					DestinationReplicationUUID: destinationReplicationUUID,
				},
			},
		}

		// Setup expected parameters
		expectedParams := googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       destinationProjectNumber,
			LocationId:          destinationLocation,
			VolumeReplicationId: destinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(correlationID),
		}

		// Setup mock response
		mockResponse := &googleproxyclient.InternalJobV1beta{
			JobUuid: googleproxyclient.OptString{
				Value: jobUUID,
				Set:   true,
			},
		}

		// Setup mock client
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			assert.Equal(tt, dstBasePath, basePath)
			assert.Equal(tt, dstJwtToken, jwt)
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, expectedParams).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, &jobUUID, updatedResult.JobId)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_WhenV1betaInternalMountVolumeReplicationFails", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		// Setup test data
		dstBasePath := "https://test-dst-base-path.com"
		dstJwtToken := "test-jwt-token"
		destinationProjectNumber := "123456789"
		correlationID := "test-correlation-id"
		destinationLocation := "us-central1"
		destinationReplicationUUID := "dest-replication-uuid-123"

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              &dstBasePath,
			DstJwtToken:              &dstJwtToken,
			DestinationProjectNumber: destinationProjectNumber,
			CorrelationID:            &correlationID,
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        destinationLocation,
					DestinationReplicationUUID: destinationReplicationUUID,
				},
			},
		}

		// Setup expected parameters
		expectedParams := googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       destinationProjectNumber,
			LocationId:          destinationLocation,
			VolumeReplicationId: destinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(correlationID),
		}

		apiError := errors.New("network timeout")

		// Setup mock client
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, expectedParams).Return(nil, apiError)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_BadRequest", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:              nillable.GetStringPtr("test-jwt-token"),
			DestinationProjectNumber: "123456789",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-central1",
					DestinationReplicationUUID: "dest-replication-uuid-123",
				},
			},
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationBadRequest{
			Message: "Invalid request parameters",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_Unauthorized", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:              nillable.GetStringPtr("test-jwt-token"),
			DestinationProjectNumber: "123456789",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-central1",
					DestinationReplicationUUID: "dest-replication-uuid-123",
				},
			},
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationUnauthorized{
			Message: "Unauthorized access",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_Forbidden", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:              nillable.GetStringPtr("test-jwt-token"),
			DestinationProjectNumber: "123456789",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-central1",
					DestinationReplicationUUID: "dest-replication-uuid-123",
				},
			},
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationForbidden{
			Message: "Forbidden access",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_NotFound", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:              nillable.GetStringPtr("test-jwt-token"),
			DestinationProjectNumber: "123456789",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-central1",
					DestinationReplicationUUID: "dest-replication-uuid-123",
				},
			},
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationNotFound{
			Message: "Replication not found",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_Conflict", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:              nillable.GetStringPtr("test-jwt-token"),
			DestinationProjectNumber: "123456789",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-central1",
					DestinationReplicationUUID: "dest-replication-uuid-123",
				},
			},
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationConflict{
			Message: "Resource conflict",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_MethodNotAllowed", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:              nillable.GetStringPtr("test-jwt-token"),
			DestinationProjectNumber: "123456789",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-central1",
					DestinationReplicationUUID: "dest-replication-uuid-123",
				},
			},
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationMethodNotAllowed{
			Message: "Method not allowed",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_UnprocessableEntity", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:              nillable.GetStringPtr("test-jwt-token"),
			DestinationProjectNumber: "123456789",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-central1",
					DestinationReplicationUUID: "dest-replication-uuid-123",
				},
			},
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationUnprocessableEntity{
			Message: "Unprocessable entity",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_InternalServerError", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.CreateHybridReplicationResult{
			DstBasePath:              nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:              nillable.GetStringPtr("test-jwt-token"),
			DestinationProjectNumber: "123456789",
			CorrelationID:            nillable.GetStringPtr("test-correlation-id"),
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-central1",
					DestinationReplicationUUID: "dest-replication-uuid-123",
				},
			},
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationInternalServerError{
			Message: "Internal server error",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger slogger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &HybridReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterHybridReplicationCreate(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaerrors.ErrMountingVolumeReplication, customErr.TrackingID)
		mockClient.AssertExpectations(tt)
	})
}
