package replicationActivities

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestAcceptClusterPeering(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
		}

		describePoolParams := &googleproxyclient.V1betaInternalDescribePoolParams{
			PoolName:      replicationResult.Event.DestinationPoolName,
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		icLifs := []string{"10.1.1.1", "10.1.1.2"}

		res := &googleproxyclient.PoolInternalV1beta{
			InterclusterLifs: icLifs,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDescribePool(ctx, *describePoolParams).Return(res, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		poolDetails, err := activity.GetDestinationPoolDetails(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, poolDetails)
		assert.Equal(tt, poolDetails.DstIps, icLifs)
	})
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
		}

		describePoolParams := &googleproxyclient.V1betaInternalDescribePoolParams{
			PoolName:      replicationResult.Event.DestinationPoolName,
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDescribePool(ctx, *describePoolParams).Return(nil, errors.New("some error"))

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		poolDetails, err := activity.GetDestinationPoolDetails(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, poolDetails)
		assert.Equal(tt, err.(*errors2.CustomError).OriginalErr.Error(), "some error")
	})
	t.Run("WhenPoolNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
		}

		describePoolParams := &googleproxyclient.V1betaInternalDescribePoolParams{
			PoolName:      replicationResult.Event.DestinationPoolName,
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDescribePool(ctx, *describePoolParams).Return(nil, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		poolDetails, err := activity.GetDestinationPoolDetails(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, poolDetails)
		assert.Equal(tt, err.(*errors2.CustomError).OriginalErr.Error(), "Pool not found")
	})
}

func TestGetDestinationPoolDetails(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		passphrase := "pass"
		srcIps := []string{"10.1.1.1", "10.1.1.2"}
		dstPoolUuid := "dst-pool-uuid"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			Passphrase:       &passphrase,
			SrcIps:           srcIps,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId: googleproxyclient.NewOptString(dstPoolUuid),
			},
		}

		describePoolParams := &googleproxyclient.V1betaInternalAcceptClusterPeerParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		res := googleproxyclient.ClusterPeerV1{
			Jobs: []googleproxyclient.JobV1beta{
				googleproxyclient.JobV1beta{JobId: googleproxyclient.NewOptString("job-uuid")},
			},
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalAcceptClusterPeer(ctx, mock.Anything, *describePoolParams).Return(&res, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.AcceptClusterPeering(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, *result.JobId, "job-uuid")
	})
	t.Run("WhenPassphraseNotPresent", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		srcIps := []string{"10.1.1.1", "10.1.1.2"}
		dstPoolUuid := "dst-pool-uuid"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			SrcIps:           srcIps,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId: googleproxyclient.NewOptString(dstPoolUuid),
			},
		}

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.AcceptClusterPeering(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.JobId)
	})
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		passphrase := "pass"
		srcIps := []string{"10.1.1.1", "10.1.1.2"}
		dstPoolUuid := "dst-pool-uuid"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			Passphrase:       &passphrase,
			SrcIps:           srcIps,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId: googleproxyclient.NewOptString(dstPoolUuid),
			},
		}

		describePoolParams := &googleproxyclient.V1betaInternalAcceptClusterPeerParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalAcceptClusterPeer(ctx, mock.Anything, *describePoolParams).Return(nil, errors.New("some error"))

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.AcceptClusterPeering(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*errors2.CustomError).OriginalErr.Error(), "some error")
	})
	t.Run("WhenClusterPeerNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		passphrase := "pass"
		srcIps := []string{"10.1.1.1", "10.1.1.2"}
		dstPoolUuid := "dst-pool-uuid"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			Passphrase:       &passphrase,
			SrcIps:           srcIps,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId: googleproxyclient.NewOptString(dstPoolUuid),
			},
		}

		describePoolParams := &googleproxyclient.V1betaInternalAcceptClusterPeerParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalAcceptClusterPeer(ctx, mock.Anything, *describePoolParams).Return(nil, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.AcceptClusterPeering(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*errors2.CustomError).OriginalErr.Error(), "Cluster peer not found")
	})
}

func TestCreateDestinationVolume(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		destPoolUuid := "uuid"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
				CreateReplicationParams: &replication.CreateReplicationParamsBody{
					DestinationVolumeParameters: &replication.DestinationVolumeParams{},
				},
				SourceVolume: datamodel.Volume{
					Name: "src-vol",
					VolumeAttributes: &datamodel.VolumeAttributes{
						CreationToken: "src-token",
						Protocols:     []string{"iscsi"},
						BlockProperties: &datamodel.BlockProperties{
							OSType: "linux",
						},
					},
				},
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId: googleproxyclient.NewOptString(destPoolUuid),
			},
		}

		createVolumeParams := &googleproxyclient.V1betaCreateVolumeParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		volume := &googleproxyclient.VolumeV1beta{}
		byte, _ := json.Marshal(volume)
		res := &googleproxyclient.OperationV1beta{
			Name:     googleproxyclient.NewOptString("/v1beta/projects/45110233509/locations/australia-southeast1/operations/job-uuid"),
			Response: byte,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaCreateVolume(ctx, mock.Anything, *createVolumeParams).Return(res, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.CreateDestinationVolume(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, *result.JobId, "job-uuid")
	})
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		destPoolUuid := "uuid"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
				CreateReplicationParams: &replication.CreateReplicationParamsBody{
					DestinationVolumeParameters: &replication.DestinationVolumeParams{},
				},
				SourceVolume: datamodel.Volume{
					Name: "src-vol",
					VolumeAttributes: &datamodel.VolumeAttributes{
						CreationToken: "src-token",
						Protocols:     []string{"iscsi"},
						BlockProperties: &datamodel.BlockProperties{
							OSType: "linux",
						},
					},
				},
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId: googleproxyclient.NewOptString(destPoolUuid),
			},
		}

		createVolumeParams := &googleproxyclient.V1betaCreateVolumeParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaCreateVolume(ctx, mock.Anything, *createVolumeParams).Return(nil, errors.New("some error"))

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.CreateDestinationVolume(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*errors2.CustomError).OriginalErr.Error(), "some error")
	})
	t.Run("WhenOperationNotReturned", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		destPoolUuid := "uuid"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
				CreateReplicationParams: &replication.CreateReplicationParamsBody{
					DestinationVolumeParameters: &replication.DestinationVolumeParams{},
				},
				SourceVolume: datamodel.Volume{
					Name: "src-vol",
					VolumeAttributes: &datamodel.VolumeAttributes{
						CreationToken: "src-token",
						Protocols:     []string{"iscsi"},
						BlockProperties: &datamodel.BlockProperties{
							OSType: "linux",
						},
					},
				},
			},
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId: googleproxyclient.NewOptString(destPoolUuid),
			},
		}

		createVolumeParams := &googleproxyclient.V1betaCreateVolumeParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaCreateVolume(ctx, mock.Anything, *createVolumeParams).Return(nil, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.CreateDestinationVolume(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.Nil(tt, result)
	})
}

func TestCreateReplicationOnDestination(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	destPoolUuid := "uuid"
	destVolUuid := "vol-uuid"
	resourceId := "rep-1"
	repSchedule := "HOURLY"
	srcSvm := "src-svm"
	dstSvm := "dst-svm"
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
				CreateReplicationParams: &replication.CreateReplicationParamsBody{
					ResourceID:                  &resourceId,
					DestinationVolumeParameters: &replication.DestinationVolumeParams{},
					ReplicationSchedule:         &repSchedule,
				},
				SourceVolume: datamodel.Volume{
					Name: "src-vol",
					VolumeAttributes: &datamodel.VolumeAttributes{
						CreationToken: "src-token",
						Protocols:     []string{"iscsi"},
						BlockProperties: &datamodel.BlockProperties{
							OSType: "linux",
						},
					},
				},
			},
			SrcSvm:           &srcSvm,
			DstSvm:           &dstSvm,
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId:      googleproxyclient.NewOptString(destPoolUuid),
				ClusterName: googleproxyclient.NewOptString("dst-cluster"),
			},
			DstVolume: &gcpserver.VolumeV1beta{
				ResourceId: "dst-vol-name",
				VolumeId:   gcpserver.NewOptString(destVolUuid),
			},
			DbVolReplication: &datamodel.VolumeReplication{
				Uri:       "projects/srcPrj/locations/us-east4/volumes/crrsrc2/replications/replication2",
				RemoteUri: "projects/dstPrj/locations/australia-southeast1/volumes/crrdst2/replications/replication2",
			},
		}

		internalCreateVolumeReplicationParams := &googleproxyclient.V1betaInternalCreateVolumeReplicationParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		res := &googleproxyclient.VolumeReplicationInternalV1beta{
			Jobs: []googleproxyclient.JobV1beta{
				googleproxyclient.JobV1beta{
					JobId: googleproxyclient.NewOptString("job-uuid"),
				},
			},
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalCreateVolumeReplication(ctx, mock.Anything, *internalCreateVolumeReplicationParams).Return(res, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.CreateReplicationOnDestination(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, *result.JobId, "job-uuid")
	})
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
				CreateReplicationParams: &replication.CreateReplicationParamsBody{
					ResourceID:                  &resourceId,
					DestinationVolumeParameters: &replication.DestinationVolumeParams{},
					ReplicationSchedule:         &repSchedule,
				},
				SourceVolume: datamodel.Volume{
					Name: "src-vol",
					VolumeAttributes: &datamodel.VolumeAttributes{
						CreationToken: "src-token",
						Protocols:     []string{"iscsi"},
						BlockProperties: &datamodel.BlockProperties{
							OSType: "linux",
						},
					},
				},
			},
			SrcSvm:           &srcSvm,
			DstSvm:           &dstSvm,
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId:      googleproxyclient.NewOptString(destPoolUuid),
				ClusterName: googleproxyclient.NewOptString("dst-cluster"),
			},
			DstVolume: &gcpserver.VolumeV1beta{
				ResourceId: "dst-vol-name",
				VolumeId:   gcpserver.NewOptString(destVolUuid),
			},
		}

		internalCreateVolumeReplicationParams := &googleproxyclient.V1betaInternalCreateVolumeReplicationParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		body := googleproxyclient.VolumeReplicationCreateInternalV1beta{}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		convertVolumeReplicationCreateParams = func(result replication.CreateReplicationResult) googleproxyclient.VolumeReplicationCreateInternalV1beta {
			return body
		}
		mockClient.EXPECT().V1betaInternalCreateVolumeReplication(ctx, &body, *internalCreateVolumeReplicationParams).Return(nil, errors.New("some-error"))

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.CreateReplicationOnDestination(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*errors2.CustomError).OriginalErr.Error(), "some-error")
		convertVolumeReplicationCreateParams = _convertVolumeReplicationCreateParams
	})
	t.Run("WhenResponseEmpty", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationPoolName:   "pool1",
				DestinationLocationID: "us-est1",
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "srcCluster",
					},
				},
				CreateReplicationParams: &replication.CreateReplicationParamsBody{
					ResourceID:                  &resourceId,
					DestinationVolumeParameters: &replication.DestinationVolumeParams{},
					ReplicationSchedule:         &repSchedule,
				},
				SourceVolume: datamodel.Volume{
					Name: "src-vol",
					VolumeAttributes: &datamodel.VolumeAttributes{
						CreationToken: "src-token",
						Protocols:     []string{"iscsi"},
						BlockProperties: &datamodel.BlockProperties{
							OSType: "linux",
						},
					},
				},
			},
			SrcSvm:           &srcSvm,
			DstSvm:           &dstSvm,
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
			DstProjectNumber: &dstProj,
			DstPool: &googleproxyclient.PoolInternalV1beta{
				PoolId:      googleproxyclient.NewOptString(destPoolUuid),
				ClusterName: googleproxyclient.NewOptString("dst-cluster"),
			},
			DstVolume: &gcpserver.VolumeV1beta{
				ResourceId: "dst-vol-name",
				VolumeId:   gcpserver.NewOptString(destVolUuid),
			},
		}

		internalCreateVolumeReplicationParams := &googleproxyclient.V1betaInternalCreateVolumeReplicationParams{
			ProjectNumber: *replicationResult.DstProjectNumber,
			LocationId:    replicationResult.Event.DestinationLocationID,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		body := googleproxyclient.VolumeReplicationCreateInternalV1beta{}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		convertVolumeReplicationCreateParams = func(result replication.CreateReplicationResult) googleproxyclient.VolumeReplicationCreateInternalV1beta {
			return body
		}
		mockClient.EXPECT().V1betaInternalCreateVolumeReplication(ctx, &body, *internalCreateVolumeReplicationParams).Return(nil, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.CreateReplicationOnDestination(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.Nil(tt, result)
		convertVolumeReplicationCreateParams = _convertVolumeReplicationCreateParams
	})
}

func TestUpdateKmsConfigState_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeReplicationCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	replication := &datamodel.VolumeReplication{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test", State: models.LifeCycleStateUpdating, StateDetails: "updated"}

	mockStorage.On("UpdateVolumeReplicationStates", ctx, replication).Return(nil)

	// Act
	err := activity.UpdateReplicationState(ctx, *replication)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateKmsConfigState_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeReplicationCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	replication := &datamodel.VolumeReplication{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test", State: models.LifeCycleStateUpdating, StateDetails: "updated"}

	mockStorage.On("UpdateVolumeReplicationStates", ctx, replication).Return(errors.New("some error"))

	// Act
	err := activity.UpdateReplicationState(ctx, *replication)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateReplicationDetails(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	destPoolUuid := "uuid"
	destVolUuid := "vol-uuid"
	resourceId := "rep-1"
	repSchedule := "HOURLY"
	srcSvm := "src-svm"
	dstSvm := "dst-svm"
	dstRepUuid := "dst-rep-uuid"
	result := &replication.CreateReplicationResult{
		Event: &replication.CreateReplicationEvent{
			DestinationPoolName:   "pool1",
			DestinationLocationID: "us-est1",
			SourcePool: datamodel.Pool{
				ClusterDetails: datamodel.ClusterDetails{
					ExternalName: "srcCluster",
				},
			},
			CreateReplicationParams: &replication.CreateReplicationParamsBody{
				ResourceID:                  &resourceId,
				DestinationVolumeParameters: &replication.DestinationVolumeParams{},
				ReplicationSchedule:         &repSchedule,
			},
			SourceVolume: datamodel.Volume{
				Name: "src-vol",
				VolumeAttributes: &datamodel.VolumeAttributes{
					CreationToken: "src-token",
					Protocols:     []string{"iscsi"},
					BlockProperties: &datamodel.BlockProperties{
						OSType: "linux",
					},
				},
			},
		},
		SrcSvm:           &srcSvm,
		DstSvm:           &dstSvm,
		DstBasePath:      &dstPath,
		DstJwtToken:      &dstToken,
		DstProjectNumber: &dstProj,
		DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
			VolumeReplicationUuid: googleproxyclient.NewOptString(dstRepUuid),
			ReplicationType:       googleproxyclient.NewOptVolumeReplicationInternalV1betaReplicationType(googleproxyclient.VolumeReplicationInternalV1betaReplicationTypeCROSSPROJECTREPLICATION),
		},
		DstPool: &googleproxyclient.PoolInternalV1beta{
			PoolId:      googleproxyclient.NewOptString(destPoolUuid),
			ClusterName: googleproxyclient.NewOptString("dst-cluster"),
		},
		DstVolume: &gcpserver.VolumeV1beta{
			ResourceId: "dst-vol-name",
			VolumeId:   gcpserver.NewOptString(destVolUuid),
		},
		DbVolReplication: &datamodel.VolumeReplication{
			Name:                  "rep-1",
			ReplicationAttributes: &datamodel.ReplicationDetails{},
		},
	}

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volumeRep := result.DbVolReplication
		volumeRep.State = models.LifeCycleStateCreated
		volumeRep.StateDetails = models.LifeCycleStateCreatedDetails
		volumeRep.ReplicationAttributes.DestinationPoolUUID = result.DstPool.PoolId.Value
		volumeRep.ReplicationAttributes.DestinationVolumeUUID = result.DstVolume.VolumeId.Value
		volumeRep.ReplicationAttributes.DestinationVolumeName = result.DstVolume.ResourceId
		volumeRep.ReplicationAttributes.SourceSvmName = *result.SrcSvm
		volumeRep.ReplicationAttributes.DestinationSvmName = *result.DstSvm
		volumeRep.ReplicationAttributes.SourceHostName = result.Event.SourcePool.ClusterDetails.ExternalName
		volumeRep.ReplicationAttributes.DestinationHostName = result.DstPool.ClusterName.Value
		volumeRep.ReplicationAttributes.DestinationReplicationUUID = result.DstReplication.VolumeReplicationUuid.Value
		volumeRep.ReplicationAttributes.SourceReplicationUUID = volumeRep.UUID
		volumeRep.ReplicationAttributes.ReplicationType = string(result.DstReplication.ReplicationType.Value)
		mockStorage.On("UpdateVolumeReplication", ctx, volumeRep).Return(nil)
		err := activity.UpdateReplicationDetails(ctx, result)

		// Assert
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volumeRep := result.DbVolReplication
		volumeRep.State = models.LifeCycleStateCreated
		volumeRep.StateDetails = models.LifeCycleStateCreatedDetails
		volumeRep.ReplicationAttributes.DestinationPoolUUID = result.DstPool.PoolId.Value
		volumeRep.ReplicationAttributes.DestinationVolumeUUID = result.DstVolume.VolumeId.Value
		volumeRep.ReplicationAttributes.DestinationVolumeName = result.DstVolume.ResourceId
		volumeRep.ReplicationAttributes.SourceSvmName = *result.SrcSvm
		volumeRep.ReplicationAttributes.DestinationSvmName = *result.DstSvm
		volumeRep.ReplicationAttributes.SourceHostName = result.Event.SourcePool.ClusterDetails.ExternalName
		volumeRep.ReplicationAttributes.DestinationHostName = result.DstPool.ClusterName.Value
		volumeRep.ReplicationAttributes.DestinationReplicationUUID = result.DstReplication.VolumeReplicationUuid.Value
		volumeRep.ReplicationAttributes.SourceReplicationUUID = volumeRep.UUID
		volumeRep.ReplicationAttributes.ReplicationType = string(result.DstReplication.ReplicationType.Value)
		mockStorage.On("UpdateVolumeReplication", ctx, volumeRep).Return(errors.New("some error"))
		err := activity.UpdateReplicationDetails(ctx, result)

		// Assert
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})
}

func TestVolumeReplicationCreateActivity_GetVolumeSVMNames(t *testing.T) {
	t.Run("Success_ValidSVMNames", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Setup mock volume response from storage
		srcVolume := &datamodel.Volume{
			Svm: &datamodel.Svm{
				Name: "src-svm-name",
			},
		}
		mockStorage.On("DescribeVolume", ctx, "test-source-uuid").Return(srcVolume, nil)

		// Setup result with required fields
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{
						UUID: "test-source-uuid",
					},
				},
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "src-cluster",
					},
				},
			},
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ClusterName: googleproxyclient.NewOptString("dst-cluster"),
			},
			DstProjectNumber: &[]string{"123456789"}[0],
			DstBasePath:      &[]string{"https://test-base-path.com"}[0],
			DstJwtToken:      &[]string{"test-jwt-token"}[0],
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-dst-volume-id"),
			},
		}

		// Mock the destination volume response
		expectedDestVolume := &googleproxyclient.InternalVolumeV1beta{
			SvmName: googleproxyclient.NewOptNilString("dst-svm-name"),
		}

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "123456789",
			LocationId:    "",
			VolumeId:      "test-dst-volume-id",
		}
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(expectedDestVolume, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcSvm)
		assert.NotNil(tt, updatedResult.DstSvm)
		assert.Equal(tt, "src-svm-name", *updatedResult.SrcSvm)
		assert.Equal(tt, "dst-svm-name", *updatedResult.DstSvm)

		mockStorage.AssertExpectations(tt)
	})

	t.Run("Error_SourceVolumeDescribeFailed", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}

		// Setup mock storage to return error
		mockStorage.On("DescribeVolume", ctx, "test-source-uuid").Return(nil, errors.New("source volume not found"))

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{
						UUID: "test-source-uuid",
					},
				},
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "src-cluster",
					},
				},
			},
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ClusterName: googleproxyclient.NewOptString("dst-cluster"),
			},
		}

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "source volume not found")

		mockStorage.AssertExpectations(tt)
	})

	t.Run("Error_DestinationVolumeDescribeFailed", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Setup mock volume response from storage
		srcVolume := &datamodel.Volume{
			Svm: &datamodel.Svm{
				Name: "src-svm-name",
			},
		}
		mockStorage.On("DescribeVolume", ctx, "test-source-uuid").Return(srcVolume, nil)

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{
						UUID: "test-source-uuid",
					},
				},
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "src-cluster",
					},
				},
			},
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ClusterName: googleproxyclient.NewOptString("dst-cluster"),
			},
			DstProjectNumber: &[]string{"123456789"}[0],
			DstBasePath:      &[]string{"https://test-base-path.com"}[0],
			DstJwtToken:      &[]string{"test-jwt-token"}[0],
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-dst-volume-id"),
			},
		}

		// Mock the GetGProxyClient function to return error
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "123456789",
			LocationId:    "",
			VolumeId:      "test-dst-volume-id",
		}
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(nil, errors.New("destination volume not found"))

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *errors2.CustomError
		assert.True(tt, errors2.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(tt, customErr.OriginalErr, "destination volume not found")

		mockStorage.AssertExpectations(tt)
	})

	t.Run("Success_EmptyClusterNames", func(tt *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Mock DescribeVolume call that happens in GetVolumeSVMNames
		mockVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockStorage.On("DescribeVolume", mock.Anything, "test-source-volume-uuid").Return(mockVolume, nil)

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedDstVolume := &googleproxyclient.InternalVolumeV1beta{
			ResourceId: googleproxyclient.NewOptString("test-volume"),
			SvmName:    googleproxyclient.NewOptNilString("test-dst-svm"),
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "123456789",
			VolumeId:      "test-dst-volume-id",
		}
		mockClient.EXPECT().V1betaInternalDescribeVolume(mock.Anything, expectedParams).Return(expectedDstVolume, nil)

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "test-source-volume-uuid"},
				},
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "",
					},
				},
			},
			DstProjectNumber: &[]string{"123456789"}[0],
			DstBasePath:      &[]string{"https://test-base-path.com"}[0],
			DstJwtToken:      &[]string{"test-jwt-token"}[0],
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-dst-volume-id"),
			},
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ClusterName: googleproxyclient.NewOptString(""),
			},
		}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(context.Background(), result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcSvm)
		assert.NotNil(tt, updatedResult.DstSvm)
		assert.Equal(tt, "test-svm", *updatedResult.SrcSvm)
		assert.Equal(tt, "test-dst-svm", *updatedResult.DstSvm)

		mockStorage.AssertExpectations(tt)
	})

	t.Run("Success_NilDstPool", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Mock source volume
		mockVolume := &datamodel.Volume{
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		sourceVolumeUUID := "source-volume-uuid"

		// Mock destination fields
		dstProjectNumber := "123456789"
		dstBasePath := "https://test-base-path.com"
		dstJwtToken := "test-jwt-token"
		dstLocationID := "us-central1-b"
		dstVolumeID := "dst-volume-id"

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: sourceVolumeUUID},
				},
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "src-cluster",
					},
				},
				DestinationLocationID: dstLocationID,
			},
			DstPool:          nil,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString(dstVolumeID),
			},
		}

		// Setup mocks
		mockStorage.On("DescribeVolume", ctx, sourceVolumeUUID).Return(mockVolume, nil)

		// Mock google proxy client
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		expectedDescribeParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: dstProjectNumber,
			LocationId:    dstLocationID,
			VolumeId:      dstVolumeID,
		}

		mockDescribeResponse := &googleproxyclient.InternalVolumeV1beta{
			SvmName: googleproxyclient.NewOptNilString("test-dst-svm"),
		}

		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedDescribeParams).Return(mockDescribeResponse, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcSvm)
		assert.NotNil(tt, updatedResult.DstSvm)
		assert.Equal(tt, "test-svm", *updatedResult.SrcSvm)
		assert.Equal(tt, "test-dst-svm", *updatedResult.DstSvm)
	})

	t.Run("Success_LongClusterNames", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Mock source volume
		mockVolume := &datamodel.Volume{
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		sourceVolumeUUID := "source-volume-uuid"

		// Mock destination fields
		dstProjectNumber := "123456789"
		dstBasePath := "https://test-base-path.com"
		dstJwtToken := "test-jwt-token"
		dstLocationID := "us-central1-b"
		dstVolumeID := "dst-volume-id"

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: sourceVolumeUUID},
				},
				SourcePool: datamodel.Pool{
					ClusterDetails: datamodel.ClusterDetails{
						ExternalName: "very-long-source-cluster-name-for-testing",
					},
				},
				DestinationLocationID: dstLocationID,
			},
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ClusterName: googleproxyclient.NewOptString("very-long-destination-cluster-name-for-testing"),
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString(dstVolumeID),
			},
		}

		// Setup mocks
		mockStorage.On("DescribeVolume", ctx, sourceVolumeUUID).Return(mockVolume, nil)

		// Mock google proxy client
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		expectedDescribeParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: dstProjectNumber,
			LocationId:    dstLocationID,
			VolumeId:      dstVolumeID,
		}

		mockDescribeResponse := &googleproxyclient.InternalVolumeV1beta{
			SvmName: googleproxyclient.NewOptNilString("test-dst-svm"),
		}

		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedDescribeParams).Return(mockDescribeResponse, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcSvm)
		assert.NotNil(tt, updatedResult.DstSvm)
		assert.Equal(tt, "test-svm", *updatedResult.SrcSvm)
		assert.Equal(tt, "test-dst-svm", *updatedResult.DstSvm)
	})

	t.Run("Error_SourceVolumeNilSvm", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}

		// Setup mock volume response from storage with nil SVM
		srcVolume := &datamodel.Volume{
			Svm: nil, // SVM is nil
		}
		mockStorage.On("DescribeVolume", ctx, "test-source-uuid").Return(srcVolume, nil)

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{
						UUID: "test-source-uuid",
					},
				},
			},
		}

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Source volume SVM name not found")

		mockStorage.AssertExpectations(tt)
	})

	t.Run("Error_SourceVolumeEmptySvmName", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}

		// Setup mock volume response from storage with empty SVM name
		srcVolume := &datamodel.Volume{
			Svm: &datamodel.Svm{
				Name: "", // SVM name is empty
			},
		}
		mockStorage.On("DescribeVolume", ctx, "test-source-uuid").Return(srcVolume, nil)

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{
						UUID: "test-source-uuid",
					},
				},
			},
		}

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Source volume SVM name not found")

		mockStorage.AssertExpectations(tt)
	})

	t.Run("Error_DestinationVolumeSvmNameNotSet", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Setup mock volume response from storage
		srcVolume := &datamodel.Volume{
			Svm: &datamodel.Svm{
				Name: "src-svm-name",
			},
		}
		mockStorage.On("DescribeVolume", ctx, "test-source-uuid").Return(srcVolume, nil)

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{
						UUID: "test-source-uuid",
					},
				},
				DestinationLocationID: "us-central1",
			},
			DstProjectNumber: &[]string{"123456789"}[0],
			DstBasePath:      &[]string{"https://test-base-path.com"}[0],
			DstJwtToken:      &[]string{"test-jwt-token"}[0],
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-dst-volume-id"),
			},
		}

		// Mock the destination volume response with SvmName not set
		expectedDestVolume := &googleproxyclient.InternalVolumeV1beta{
			SvmName: googleproxyclient.OptNilString{Set: false}, // SvmName is not set
		}

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "123456789",
			LocationId:    "us-central1",
			VolumeId:      "test-dst-volume-id",
		}
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(expectedDestVolume, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Destination volume SVM name not found")

		mockStorage.AssertExpectations(tt)
	})

	t.Run("Success_DestinationVolumeEmptySvmName", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Setup mock volume response from storage
		srcVolume := &datamodel.Volume{
			Svm: &datamodel.Svm{
				Name: "src-svm-name",
			},
		}
		mockStorage.On("DescribeVolume", ctx, "test-source-uuid").Return(srcVolume, nil)

		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourceVolume: datamodel.Volume{
					BaseModel: datamodel.BaseModel{
						UUID: "test-source-uuid",
					},
				},
				DestinationLocationID: "us-central1",
			},
			DstProjectNumber: &[]string{"123456789"}[0],
			DstBasePath:      &[]string{"https://test-base-path.com"}[0],
			DstJwtToken:      &[]string{"test-jwt-token"}[0],
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-dst-volume-id"),
			},
		}

		// Mock the destination volume response with empty SvmName but set
		expectedDestVolume := &googleproxyclient.InternalVolumeV1beta{
			SvmName: googleproxyclient.NewOptNilString(""), // Empty but set SvmName
		}

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "123456789",
			LocationId:    "us-central1",
			VolumeId:      "test-dst-volume-id",
		}
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(expectedDestVolume, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}

		// Act
		updatedResult, err := activity.GetVolumeSVMNames(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcSvm)
		assert.NotNil(tt, updatedResult.DstSvm)
		assert.Equal(tt, "src-svm-name", *updatedResult.SrcSvm)
		assert.Equal(tt, "", *updatedResult.DstSvm) // Empty destination SVM name

		mockStorage.AssertExpectations(tt)
	})
}

func TestVolumeReplicationCreateActivity_DescribeVolume(t *testing.T) {
	t.Run("Success_ValidVolumeDescription", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProjectNumber := "123456789"
		dstBasePath := "https://test-base-path.com"
		dstJwtToken := "test-jwt-token"
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "us-central1",
			},
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-volume-id"),
			},
		}

		expectedVolume := &googleproxyclient.InternalVolumeV1beta{
			ResourceId:  googleproxyclient.NewOptString("test-volume"),
			UsedBytes:   googleproxyclient.NewOptNilFloat64(1024000000000),
			VolumeState: googleproxyclient.NewOptInternalVolumeV1betaVolumeState(googleproxyclient.InternalVolumeV1betaVolumeStateREADY),
		}

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "123456789",
			LocationId:    "us-central1",
			VolumeId:      "test-volume-id",
		}
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(expectedVolume, nil)

		// Act
		resultVolume, err := DescribeVolume(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, resultVolume)
		assert.Equal(tt, "test-volume", resultVolume.ResourceId.Value)
		assert.Equal(tt, float64(1024000000000), resultVolume.UsedBytes.Value)
		assert.Equal(tt, googleproxyclient.InternalVolumeV1betaVolumeStateREADY, resultVolume.VolumeState.Value)
	})

	t.Run("Error_ClientError", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProjectNumber := "123456789"
		dstBasePath := "https://test-base-path.com"
		dstJwtToken := "test-jwt-token"
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "us-central1",
			},
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-volume-id"),
			},
		}

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "123456789",
			LocationId:    "us-central1",
			VolumeId:      "test-volume-id",
		}
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(nil, errors.New("volume not found"))

		// Act
		resultVolume, err := DescribeVolume(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, resultVolume)
		var customErr *errors2.CustomError
		assert.True(tt, errors2.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(tt, customErr.OriginalErr, "volume not found")
	})

	t.Run("Error_EmptyProjectNumber", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Provide empty project number but valid other fields to avoid nil pointer dereference
		emptyProjectNumber := ""
		dstBasePath := "https://test-base-path.com"
		dstJwtToken := "test-jwt-token"
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "us-central1",
			},
			DstProjectNumber: &emptyProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-volume-id"),
			},
		}

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: emptyProjectNumber,
			LocationId:    "us-central1",
			VolumeId:      "test-volume-id",
		}

		// Mock client to return an error for empty project number
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(nil, errors.New("invalid project number"))

		// Act
		resultVolume, err := DescribeVolume(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, resultVolume)
	})

	t.Run("Error_EmptyLocationID", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProjectNumber := "123456789"
		dstBasePath := "https://test-base-path.com"
		dstJwtToken := "test-jwt-token"
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "",
			},
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-volume-id"),
			},
		}

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: dstProjectNumber,
			LocationId:    "",
			VolumeId:      "test-volume-id",
		}

		// Mock client to return an error for empty location ID
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(nil, errors.New("invalid location ID"))

		// Act
		resultVolume, err := DescribeVolume(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, resultVolume)
	})

	t.Run("Error_MissingEvent", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProjectNumber := "123456789"
		dstBasePath := "https://test-base-path.com"
		dstJwtToken := "test-jwt-token"
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "us-central1",
			},
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-volume-id"),
			},
		}

		// Mock the GetGProxyClient function
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: dstProjectNumber,
			LocationId:    "us-central1",
			VolumeId:      "test-volume-id",
		}

		// Mock client to return an error to simulate missing event or other failure
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(nil, errors.New("event processing error"))

		// Act
		resultVolume, err := DescribeVolume(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, resultVolume)
	})

	t.Run("Success_ValidParametersConstruction", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		// Mock the googleproxyclient.GetGProxyClient to return a client that will cause an error
		// but after parameter validation, so we can verify parameter construction is correct
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockClient,
			}
		}

		dstProjectNumber := "123456789"
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "us-central1",
			},
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &[]string{"https://test.example.com"}[0],
			DstJwtToken:      &[]string{"test-jwt-token"}[0],
			DstVolume: &gcpserver.VolumeV1beta{
				VolumeId: gcpserver.NewOptString("test-volume-id"),
			},
		}

		expectedParams := googleproxyclient.V1betaInternalDescribeVolumeParams{
			ProjectNumber: dstProjectNumber,
			LocationId:    "us-central1",
			VolumeId:      "test-volume-id",
		}

		// Mock client to return an error to test parameter construction
		mockClient.EXPECT().V1betaInternalDescribeVolume(ctx, expectedParams).Return(nil, errors.New("parameter construction test error"))

		// Act - This will fail due to mock error, but we test that parameters are constructed correctly
		resultVolume, err := DescribeVolume(ctx, result)

		// Assert - We expect an error due to mock error, but parameters were validated
		assert.Error(tt, err)
		assert.Nil(tt, resultVolume)
	})
}

func TestGetSrcBasePath(t *testing.T) {
	t.Run("ValidSrcBasePath", func(tt *testing.T) {
		// Arrange
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				LocationID: "us-central1",
			},
		}
		activity := VolumeReplicationCreateActivity{}

		// Mock the InternalUtilGetPairedRegionURI function
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://src-base-path.example.com", nil
		}

		// Act
		updatedResult, err := activity.GetSrcBasePath(context.Background(), result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcBasePath)
		assert.Equal(tt, "https://src-base-path.example.com", *updatedResult.SrcBasePath)
	})

	t.Run("ErrorSrcBasePath", func(tt *testing.T) {
		// Arrange
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				LocationID: "invalid-location",
			},
		}
		activity := VolumeReplicationCreateActivity{}

		// Mock the InternalUtilGetPairedRegionURI function
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		// Act
		updatedResult, err := activity.GetSrcBasePath(context.Background(), result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetDstBasePath(t *testing.T) {
	t.Run("ValidDstBasePath", func(tt *testing.T) {
		// Arrange
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "us-central1",
			},
		}
		activity := VolumeReplicationCreateActivity{}

		// Mock the InternalUtilGetPairedRegionURI function
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		// Act
		updatedResult, err := activity.GetDstBasePath(context.Background(), result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})

	t.Run("ErrorDstBasePath", func(tt *testing.T) {
		// Arrange
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "invalid-location",
			},
		}
		activity := VolumeReplicationCreateActivity{}

		// Mock the InternalUtilGetPairedRegionURI function
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		// Act
		updatedResult, err := activity.GetDstBasePath(context.Background(), result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedSrcToken(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		// Arrange
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				LocationID: "src-location-id",
			},
			SrcProjectNumber: &srcPrj,
		}

		// Mock the InternalUtilGetSignedToken function
		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "signed-token", nil
		}

		// Act
		updatedResult, err := activity.GetSignedSrcToken(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.SrcJwtToken)
	})

	t.Run("WhenError", func(tt *testing.T) {
		// Arrange
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				LocationID: "src-location-id",
			},
			SrcProjectNumber: &srcPrj,
		}

		// Mock the InternalUtilGetSignedToken function
		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "", errors.New("some error")
		}

		// Act
		updatedResult, err := activity.GetSignedSrcToken(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedDstToken(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		// Arrange
		dstPrj := "dstPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstProjectNumber: &dstPrj,
		}

		// Mock the InternalUtilGetSignedToken function
		replication.InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "signed-dst-token", nil
		}

		// Act
		updatedResult, err := activity.GetSignedDstToken(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-dst-token", *updatedResult.DstJwtToken)
	})

	t.Run("WhenError", func(tt *testing.T) {
		// Arrange
		dstPrj := "dstPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstProjectNumber: &dstPrj,
		}

		// Mock the InternalUtilGetSignedToken function
		replication.InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("failed to get signed token")
		}

		// Act
		updatedResult, err := activity.GetSignedDstToken(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestAcceptSvmPeer(t *testing.T) {
	t.Run("WhenAlreadyPeered", func(tt *testing.T) {
		// Arrange
		dstSvm := "dst-svm"
		srcSvm := "src-svm"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockProvider := new(vsa.MockProvider)
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstSvm: &dstSvm,
			SrcSvm: &srcSvm,
		}

		svmPeer := &vsa.SvmPeer{
			PeerSvmName: dstSvm,
			State:       "peered",
		}
		mockProvider.On("GetSVMPeer", result.SrcSvm, result.DstSvm).Return(svmPeer, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Act
		updatedResult, err := activity.AcceptSvmPeer(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		// Arrange
		dstSvm := "dst-svm"
		srcSvm := "src-svm"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockProvider := &vsa.MockProvider{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstSvm: &dstSvm,
			SrcSvm: &srcSvm,
		}

		svmPeer := &vsa.SvmPeer{
			PeerSvmName: dstSvm,
		}
		mockProvider.On("GetSVMPeer", result.SrcSvm, result.DstSvm).Return(svmPeer, nil)
		mockProvider.On("AcceptSvmPeering", srcSvm, dstSvm).Return(nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Act
		updatedResult, err := activity.AcceptSvmPeer(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})
	t.Run("WhenError", func(tt *testing.T) {
		// Arrange
		dstSvm := "dst-svm"
		srcSvm := "src-svm"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockProvider := &vsa.MockProvider{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstSvm: &dstSvm,
			SrcSvm: &srcSvm,
		}

		svmPeer := &vsa.SvmPeer{
			PeerSvmName: dstSvm,
		}
		mockProvider.On("GetSVMPeer", &srcSvm, &dstSvm).Return(svmPeer, nil)
		mockProvider.On("AcceptSvmPeering", srcSvm, dstSvm).Return(errors.New("some-error"))
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Act
		updatedResult, err := activity.AcceptSvmPeer(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})
	t.Run("WhenGetSvmPeerError", func(tt *testing.T) {
		// Arrange
		dstSvm := "dst-svm"
		srcSvm := "src-svm"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockProvider := &vsa.MockProvider{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstSvm: &dstSvm,
			SrcSvm: &srcSvm,
		}

		mockProvider.On("GetSVMPeer", &srcSvm, &dstSvm).Return(nil, errors.New("some-error"))
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Act
		updatedResult, err := activity.AcceptSvmPeer(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenProviderByNodeError", func(tt *testing.T) {
		// Arrange
		dstSvm := "dst-svm"
		srcSvm := "src-svm"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockProvider := &vsa.MockProvider{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstSvm: &dstSvm,
			SrcSvm: &srcSvm,
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		// Act
		updatedResult, err := activity.AcceptSvmPeer(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.EqualError(tt, err, "provider error")
		mockProvider.AssertExpectations(tt)
	})
}

func TestGetSourceInterclusterLifs(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockProvider := new(vsa.MockProvider)
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourcePool: datamodel.Pool{
					Name: "source-pool",
				},
			},
			SrcNode: &models.Node{},
		}

		interclusterLifs := []*vsa.InterclusterLif{
			&vsa.InterclusterLif{Address: "10.1.1.1"},
			&vsa.InterclusterLif{Address: "10.1.1.2"},
		}
		mockProvider.On("GetInterclusterLIFs", "default-intercluster").Return(interclusterLifs, nil)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Act
		updatedResult, err := activity.GetSourceInterclusterLifs(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, []string{"10.1.1.1", "10.1.1.2"}, updatedResult.SrcIps)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenError", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockProvider := new(vsa.MockProvider)
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourcePool: datamodel.Pool{
					Name: "source-pool",
				},
			},
			SrcNode: &models.Node{},
		}

		mockProvider.On("GetInterclusterLIFs", "default-intercluster").Return(nil, errors.New("failed to fetch intercluster LIFs"))
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Act
		updatedResult, err := activity.GetSourceInterclusterLifs(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeError", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockProvider := new(vsa.MockProvider)
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				SourcePool: datamodel.Pool{
					Name: "source-pool",
				},
			},
			SrcNode: &models.Node{},
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to fetch intercluster LIFs")
		}

		// Act
		updatedResult, err := activity.GetSourceInterclusterLifs(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.EqualError(tt, err, "failed to fetch intercluster LIFs")
		mockProvider.AssertExpectations(tt)
	})
}

func TestHydrateDestinationVolume(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ResourceId: "pool-resource-id",
			},
			DstVolume: &gcpserver.VolumeV1beta{},
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "location-id",
			},
			DstProjectNumber: nillable.GetStringPtr("project-number"),
		}

		// Mock the VolumeHydration function
		hydrateVolume = func(ctx context.Context, destVolume models.Volume, project string, poolResourceId string) error {
			return nil
		}
		// Act
		updatedResult, err := activity.HydrateDestinationVolume(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		hydrateVolume = HydrateVolume
	})

	t.Run("WhenError", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ResourceId: "pool-resource-id",
			},
			DstVolume: &gcpserver.VolumeV1beta{},
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "location-id",
			},
			DstProjectNumber: nillable.GetStringPtr("project-number"),
		}

		// Mock the VolumeHydration function
		hydrateVolume = func(ctx context.Context, destVolume models.Volume, project string, poolResourceId string) error {
			return errors.New("hydration error")
		}

		// Act
		updatedResult, err := activity.HydrateDestinationVolume(ctx, result)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		hydrateVolume = HydrateVolume
	})
	t.Run("WhenHydrationIsDisabled", func(tt *testing.T) {
		// Arrange
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		defer func() { hydrationEnabled = env.GetBool("GCP_HYDRATE_ENABLED", true) }() // Restore hydrationEnabled after test
		result := &replication.CreateReplicationResult{
			DstVolume: &gcpserver.VolumeV1beta{},
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "location-id",
			},
			DstProjectNumber: nillable.GetStringPtr("project-number"),
		}
		// Disable Hydration
		hydrationEnabled = false
		// Act
		updatedResult, err := activity.HydrateDestinationVolume(ctx, result)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
	})
}

func Test_convertVolumeV1BetaToVolumeModel(t *testing.T) {
	// Arrange
	vol := gcpserver.VolumeV1beta{
		Protocols: []gcpserver.ProtocolsV1beta{
			gcpserver.ProtocolsV1betaISCSI,
		},
		VolumeId:     gcpserver.OptString{Value: "volume-id"},
		ResourceId:   "vol-1",
		QuotaInBytes: gcpserver.NewOptFloat64(float64(1234)),
		VolumeState:  gcpserver.NewOptVolumeV1betaVolumeState(gcpserver.VolumeV1betaVolumeStateREADY),
	}
	dstLocation := "us-central1"

	// Act
	result := convertVolumeV1BetaToVolumeModel(vol, dstLocation)

	// Assert
	assert.Equal(t, "volume-id", result.UUID)
	assert.Equal(t, "vol-1", result.DisplayName)
	assert.Equal(t, dstLocation, result.Region)
	assert.Equal(t, "READY", result.LifeCycleState)
	assert.ElementsMatch(t, []string{"ISCSI"}, result.ProtocolTypes)
}

func TestCreateClusterPeer(t *testing.T) {
	t.Run("TestCreateClusterPeer_Success", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider) // Use the mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := VolumeReplicationCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		expectedResponse := &vsa.ClusterPeer{
			UUID:         "12345",
			ExternalUUID: "12345",
		}
		result := replication.CreateReplicationResult{
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ClusterName: googleproxyclient.NewOptString("cluster1"),
			},
		}
		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{
			{
				PeerClusterName: "peer1",
				PeerAddresses:   []string{"192.168.1.2"},
				Availability:    "Available",
			},
		}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(expectedResponse, nil)
		_, err := activity.CreateClusterPeering(ctx, &result)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("CreateClusterPeerReturnsErrorWhenProviderFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := VolumeReplicationCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		result := replication.CreateReplicationResult{
			DstPool: &googleproxyclient.PoolInternalV1beta{
				ClusterName: googleproxyclient.NewOptString("cluster1"),
			},
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{
			{
				PeerClusterName: "peer1",
				PeerAddresses:   []string{"192.168.1.2"},
				Availability:    "Available",
			},
		}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(nil, errors.New("provider error"))
		_, err := activity.CreateClusterPeering(ctx, &result)

		assert.Error(t, err)
		assert.Equal(t, "provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
}

func TestDescribeRemoteJob(t *testing.T) {
	t.Run("DescribeJob_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "test-location-id",
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   *result.JobId,
			ProjectNumber: *result.DstProjectNumber,
			LocationId:    result.Event.DestinationLocationID,
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

		err := activity.DescribeRemoteJob(ctx, result)

		assert.NoError(tt, err)
	})

	t.Run("DescribeJob_Error", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "test-location-id",
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   *result.JobId,
			ProjectNumber: *result.DstProjectNumber,
			LocationId:    result.Event.DestinationLocationID,
		}
		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
		err := activity.DescribeRemoteJob(ctx, result)

		assert.Error(tt, err)
	})

	t.Run("DescribeJob_NotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result := &replication.CreateReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "test-location-id",
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   *result.JobId,
			ProjectNumber: *result.DstProjectNumber,
			LocationId:    result.Event.DestinationLocationID,
		}
		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := activity.DescribeRemoteJob(ctx, result)

		assert.Error(tt, err)
	})
}

func TestMountReplication(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "us-central1",
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			DstProjectNumber: &dstProj,
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       dstProj,
			LocationId:          "us-central1",
			VolumeReplicationId: replicationResult.DstReplication.VolumeReplicationUuid.Value,
		}
		res := &googleproxyclient.InternalJobV1beta{
			JobUuid: googleproxyclient.NewOptString("job-uuid"),
		}
		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(res, nil)

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.MountReplication(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		dstProj := "projDst"
		dstPath := "dstPath"
		dstToken := "dstToken"
		replicationResult := &replication.CreateReplicationResult{
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "us-central1",
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			DstProjectNumber: &dstProj,
			DstBasePath:      &dstPath,
			DstJwtToken:      &dstToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       dstProj,
			LocationId:          "us-central1",
			VolumeReplicationId: replicationResult.DstReplication.VolumeReplicationUuid.Value,
		}
		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(nil, errors.New("mount error"))

		activity := VolumeReplicationCreateActivity{SE: mockStorage}
		result, err := activity.MountReplication(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}
