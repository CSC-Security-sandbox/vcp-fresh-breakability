package replicationActivities

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestGetSrcBasePathCleanup(t *testing.T) {
	t.Run("ValidSrcBasePath", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "location-id",
						},
					},
				},
			},
		}
		activity := CleanupVolumeReplicationActivity{}

		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://src-base-path.example.com", nil
		}

		updatedResult, err := activity.GetSrcBasePathCleanup(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcBasePath)
		assert.Equal(tt, "https://src-base-path.example.com", *updatedResult.SrcBasePath)
	})
	t.Run("ErrorSrcBasePath", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "location-id",
						},
					},
				},
			},
		}
		activity := CleanupVolumeReplicationActivity{}

		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetSrcBasePathCleanup(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetDstBasePathCleanup(t *testing.T) {
	t.Run("ValidDstBasePath", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "location-id",
						},
					},
				},
			},
		}
		activity := CleanupVolumeReplicationActivity{}

		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		updatedResult, err := activity.GetDstBasePathCleanup(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})
	t.Run("ErrorDstBasePath", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "location-id",
						},
					},
				},
			},
		}
		activity := CleanupVolumeReplicationActivity{}

		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetDstBasePathCleanup(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedSrcTokenCleanup(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "location-id",
						},
					},
				},
			},
			SrcProjectNumber: &srcPrj,
		}

		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "signed-token", nil
		}

		updatedResult, err := activity.GetSignedSrcTokenCleanup(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.SrcJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "location-id",
						},
					},
				},
			},
			SrcProjectNumber: &srcPrj,
		}

		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "", errors.New("failed to get signed token")
		}

		updatedResult, err := activity.GetSignedSrcTokenCleanup(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedDstTokenCleanup(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "location-id",
						},
					},
				},
			},
			DstProjectNumber: &dstPrj,
			SrcProjectNumber: &srcPrj,
		}

		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "signed-token", nil
		}

		updatedResult, err := activity.GetSignedDstTokenCleanup(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenSuccessSameProject", func(tt *testing.T) {
		prj := "prj"
		token := "signed-token"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "location-id",
						},
					},
				},
			},
			SrcJwtToken:      &token,
			SrcProjectNumber: &prj,
			DstProjectNumber: &prj,
		}

		updatedResult, err := activity.GetSignedDstTokenCleanup(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "location-id",
						},
					},
				},
			},
			DstProjectNumber: &dstPrj,
			SrcProjectNumber: &srcPrj,
		}

		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "", errors.New("failed to get signed token")
		}

		updatedResult, err := activity.GetSignedDstTokenCleanup(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestDeleteReplicationOnDestinationForCleanup(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			DstReplication:   &googleproxyclient.VolumeReplicationInternalV1beta{},
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(nil, errors.New("some-error"))
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestinationForCleanup(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "some-error", err.Error())
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		res := &googleproxyclient.VolumeReplicationInternalV1beta{
			Jobs: []googleproxyclient.JobV1beta{
				googleproxyclient.JobV1beta{
					JobId: googleproxyclient.NewOptString("job-uuid"),
				},
			},
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			DstReplication:   &googleproxyclient.VolumeReplicationInternalV1beta{},
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(res, nil)
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestinationForCleanup(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.JobId, "job-uuid")
	})
}

func TestGetReplicationOnDestinationForCleanup(t *testing.T) {
	dstPrj := "dstPrj"
	dstPath := "dstPath"
	dstToken := "dstToken"
	replicationUUID := "replication-uuid"
	locationID := "location-id"

	t.Run("Success", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		replicationObj := googleproxyclient.VolumeReplicationInternalV1beta{}
		okResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{replicationObj},
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetReplicationOnDestinationForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstReplication)
	})

	t.Run("Error", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(nil, errors.New("some-error"))

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetReplicationOnDestinationForCleanup(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
	t.Run("SuccessNotFound", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound{}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetReplicationOnDestinationForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.Equal(tt, updatedResult.Error, nil)
	})
	t.Run("SuccessBadRequest", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest{
			Code:    400,
			Message: "dfdgh",
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetReplicationOnDestinationForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.Error)
	})
	t.Run("SuccessInternalServerError", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError{
			Code:    500,
			Message: "dfdgh",
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetReplicationOnDestinationForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.Error)
	})
	t.Run("SuccessUnauthorized", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized{
			Code:    500,
			Message: "dfdgh",
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetReplicationOnDestinationForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.Error)
	})
	t.Run("SuccessForbidden", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden{
			Code:    500,
			Message: "dfdgh",
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetReplicationOnDestinationForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.Error)
	})
}

func TestGetDestinationVolumeForCleanup(t *testing.T) {
	dstPrj := "dstPrj"
	dstPath := "dstPath"
	dstToken := "dstToken"
	replicationUUID := "replication-uuid"
	locationID := "location-id"
	volumeId := "vol-1"
	t.Run("Success", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.VolumeV1beta{
			ResourceId: "vol-1",
		}
		params := googleproxyclient.V1betaDescribeVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volumeId,
		}
		mockClient.EXPECT().V1betaDescribeVolume(
			mock.Anything, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
							DestinationVolumeUUID:      volumeId,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetDestinationVolumeForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstVolume)
	})

	t.Run("Error", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := googleproxyclient.V1betaDescribeVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volumeId,
		}
		mockClient.EXPECT().V1betaDescribeVolume(
			mock.Anything, params,
		).Return(nil, errors.New("some-error"))

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
							DestinationVolumeUUID:      volumeId,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetDestinationVolumeForCleanup(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
	t.Run("SuccessNotFound", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaDescribeVolumeNotFound{}
		params := googleproxyclient.V1betaDescribeVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volumeId,
		}
		mockClient.EXPECT().V1betaDescribeVolume(
			mock.Anything, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
							DestinationVolumeUUID:      volumeId,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetDestinationVolumeForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.Equal(tt, updatedResult.Error, nil)
	})
	t.Run("SuccessBadRequest", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaDescribeVolumeBadRequest{
			Code:    400,
			Message: "dfdgh",
		}
		params := googleproxyclient.V1betaDescribeVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volumeId,
		}
		mockClient.EXPECT().V1betaDescribeVolume(
			mock.Anything, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
							DestinationVolumeUUID:      volumeId,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetDestinationVolumeForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.Error)
	})
	t.Run("SuccessInternalServerError", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaDescribeVolumeInternalServerError{
			Code:    500,
			Message: "dfdgh",
		}
		params := googleproxyclient.V1betaDescribeVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volumeId,
		}
		mockClient.EXPECT().V1betaDescribeVolume(
			mock.Anything, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
							DestinationVolumeUUID:      volumeId,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetDestinationVolumeForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.Error)
	})
	t.Run("SuccessUnauthorized", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaDescribeVolumeUnauthorized{
			Code:    500,
			Message: "dfdgh",
		}
		params := googleproxyclient.V1betaDescribeVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volumeId,
		}
		mockClient.EXPECT().V1betaDescribeVolume(
			mock.Anything, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
							DestinationVolumeUUID:      volumeId,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetDestinationVolumeForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.Error)
	})
	t.Run("SuccessForbidden", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		okResp := &googleproxyclient.V1betaDescribeVolumeForbidden{
			Code:    500,
			Message: "dfdgh",
		}
		params := googleproxyclient.V1betaDescribeVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volumeId,
		}
		mockClient.EXPECT().V1betaDescribeVolume(
			mock.Anything, params,
		).Return(okResp, nil)

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        locationID,
							DestinationReplicationUUID: replicationUUID,
							DestinationVolumeUUID:      volumeId,
						},
					},
				},
			},
		}
		updatedResult, err := activity.GetDestinationVolumeForCleanup(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.Error)
	})
}

func TestReleaseReplicationOnSourceForCleanup(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &dstPath,
			SrcProjectNumber: &dstProj,
			SrcJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(nil, errors.New("some-error"))
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ReleaseReplicationOnSourceForCleanup(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "some-error", err.Error())
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &dstPath,
			SrcProjectNumber: &dstProj,
			SrcJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(&googleproxyclient.OperationV1beta{Name: googleproxyclient.NewOptString("projects/123/locations/us-central1/volumes/vol-uuid/operations/job-uuid"), Done: googleproxyclient.NewOptBool(true)}, nil)
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ReleaseReplicationOnSourceForCleanup(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.JobId, "job-uuid")
	})
}

func TestDeHydrateDestinationVolumeReplicationForCleanup(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		originalHydrateVolumeReplication := deHydrateVolumeReplication
		defer func() {
			deHydrateVolumeReplication = originalHydrateVolumeReplication
			hydrationEnabled = false
		}()

		deHydrateVolumeReplication = func(ctx context.Context, createReplicationResponse models.VolumeReplication, project string) error {
			return errors.New("hydration error")
		}
		_, err := activity.DeHydrateDestinationVolumeReplicationForCleanup(ctx, inputResult)

		assert.Error(t, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		originalHydrateVolumeReplication := deHydrateVolumeReplication
		defer func() {
			deHydrateVolumeReplication = originalHydrateVolumeReplication
			hydrationEnabled = false
		}()

		deHydrateVolumeReplication = func(ctx context.Context, createReplicationResponse models.VolumeReplication, project string) error {
			return nil
		}
		_, err := activity.DeHydrateDestinationVolumeReplicationForCleanup(ctx, inputResult)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestDeHydrateDestinationVolumeForCleanup(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	volume := &googleproxyclient.VolumeV1beta{}

	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			DstVolume:        volume,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		originalDeHydrateVolume := deHydrateVolume
		defer func() {
			deHydrateVolume = originalDeHydrateVolume
			hydrationEnabled = false
		}()
		deHydrateVolume = func(ctx context.Context, destVolume models.Volume, project string) error {
			return errors.New("hydration error")
		}
		_, err := activity.DeHydrateDestinationVolumeForCleanup(ctx, inputResult)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			DstVolume:        volume,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		originalDeHydrateVolume := deHydrateVolume
		defer func() {
			deHydrateVolume = originalDeHydrateVolume
			hydrationEnabled = false
		}()
		deHydrateVolume = func(ctx context.Context, destVolume models.Volume, project string) error {
			return nil
		}
		_, err := activity.DeHydrateDestinationVolumeForCleanup(ctx, inputResult)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestStopReplicationOnDestinationForCleanup(t *testing.T) {
	dstPrj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	replicationUUID := "replication-uuid"
	locationID := "location-id"

	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       dstPrj,
			LocationId:          locationID,
			VolumeReplicationId: replicationUUID,
		}
		req := googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(true),
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, &req, params).Return(nil, errors.New("some-error"))
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		result, err := activity.StopReplicationOnDestinationForCleanup(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "some-error", err.Error())
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		res := &googleproxyclient.VolumeReplicationInternalV1beta{
			Jobs: []googleproxyclient.JobV1beta{
				{JobId: googleproxyclient.NewOptString("job-uuid")},
			},
		}
		params := googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       dstPrj,
			LocationId:          locationID,
			VolumeReplicationId: replicationUUID,
		}
		req := googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(true),
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, &req, params).Return(res, nil)
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		result, err := activity.StopReplicationOnDestinationForCleanup(ctx, inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "job-uuid", result.JobId)
	})
}

func TestDeleteVolumeOnDestinationForCleanup(t *testing.T) {
	dstPrj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	volUUID := "vol-uuid"
	locationID := "location-id"

	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volUUID,
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(nil, errors.New("some-error"))
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestinationForCleanup(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "some-error", err.Error())
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		volume := &googleproxyclient.VolumeV1beta{}
		byte, _ := json.Marshal(volume)
		operation := &googleproxyclient.OperationV1beta{
			Name:     googleproxyclient.NewOptString("projects/123/locations/us-central1/volumes/vol-uuid/operations/job-uuid"),
			Response: byte,
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber: dstPrj,
			LocationId:    locationID,
			VolumeId:      volUUID,
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(operation, nil)
		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestinationForCleanup(ctx, inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "job-uuid", result.JobId)
	})
}

func TestDescribeRemoteJobForCleanup(t *testing.T) {
	t.Run("DescribeJobSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "test-location-id",
						},
					},
				},
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   result.JobId,
			ProjectNumber: *result.DstProjectNumber,
			LocationId:    result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

		err := activity.DescribeRemoteJobForCleanup(ctx, result)

		assert.NoError(tt, err)
	})
	t.Run("DescribeJobNotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "test-location-id",
						},
					},
				},
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}
		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   result.JobId,
			ProjectNumber: *result.DstProjectNumber,
			LocationId:    result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := activity.DescribeRemoteJobForCleanup(ctx, result)

		assert.Error(tt, err)
	})
	t.Run("DescribeJobError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "test-location-id",
						},
					},
				},
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   result.JobId,
			ProjectNumber: *result.DstProjectNumber,
			LocationId:    result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
		err := activity.DescribeRemoteJobForCleanup(ctx, result)

		assert.Error(tt, err)
	})
}

func TestDescribeSourceJobForCleanup(t *testing.T) {
	t.Run("DescribeJobSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "test-location-id",
						},
					},
				},
			},
			SrcBasePath: nillable.GetStringPtr("base-path"),
			SrcJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   result.JobId,
			ProjectNumber: *result.SrcProjectNumber,
			LocationId:    result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

		err := activity.DescribeSourceJobForCleanup(ctx, result)

		assert.NoError(tt, err)
	})
	t.Run("DescribeJobNotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "test-location-id",
						},
					},
				},
			},
			SrcBasePath: nillable.GetStringPtr("base-path"),
			SrcJwtToken: nillable.GetStringPtr("jwt-token"),
		}
		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   result.JobId,
			ProjectNumber: *result.SrcProjectNumber,
			LocationId:    result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := activity.DescribeSourceJobForCleanup(ctx, result)

		assert.Error(tt, err)
	})
	t.Run("DescribeJobError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := CleanupVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "test-location-id",
						},
					},
				},
			},
			SrcBasePath: nillable.GetStringPtr("base-path"),
			SrcJwtToken: nillable.GetStringPtr("jwt-token"),
		}
		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:   result.JobId,
			ProjectNumber: *result.SrcProjectNumber,
			LocationId:    result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
		err := activity.DescribeSourceJobForCleanup(ctx, result)

		assert.Error(tt, err)
	})
}
