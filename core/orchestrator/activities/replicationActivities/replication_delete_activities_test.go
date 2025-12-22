package replicationActivities

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGetSrcBasePathDelete(t *testing.T) {
	t.Run("ValidSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}
		activity := DeleteVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://src-base-path.example.com", nil
		}

		updatedResult, err := activity.GetSrcBasePathDelete(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcBasePath)
		assert.Equal(tt, "https://src-base-path.example.com", *updatedResult.SrcBasePath)
	})
	t.Run("WhenHybridReplication", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: RemoteRegionCustomer,
						},
					},
				},
			},
		}
		activity := DeleteVolumeReplicationActivity{}

		updatedResult, err := activity.GetSrcBasePathDelete(context.Background(), result)

		assert.NoError(tt, err)
		assert.Nil(tt, updatedResult.SrcBasePath)
	})
	t.Run("ErrorSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}
		activity := DeleteVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetSrcBasePathDelete(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetDstBasePathDelete(t *testing.T) {
	t.Run("ValidDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := DeleteVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		updatedResult, err := activity.GetDstBasePathDelete(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})
	t.Run("WhenHybridReplication", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: RemoteRegionCustomer,
						},
					},
				},
			},
		}
		activity := DeleteVolumeReplicationActivity{}

		updatedResult, err := activity.GetDstBasePathDelete(context.Background(), result)

		assert.NoError(tt, err)
		assert.Nil(tt, updatedResult.DstBasePath)
	})
	t.Run("ErrorDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := DeleteVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetDstBasePathDelete(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedSrcTokenDelete(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
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

		updatedResult, err := activity.GetSignedSrcTokenDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.SrcJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
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

		updatedResult, err := activity.GetSignedSrcTokenDelete(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedDstTokenDelete(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
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

		updatedResult, err := activity.GetSignedDstTokenDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenSuccessSameProject", func(tt *testing.T) {
		prj := "prj"
		token := "signed-token"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
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

		updatedResult, err := activity.GetSignedDstTokenDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
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

		updatedResult, err := activity.GetSignedDstTokenDelete(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestDeleteReplicationOnDestination(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	correlationID := "correlation-id"

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
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(res, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.JobId, "job-uuid")
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		badRequestResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationBadRequest{
			Message: "Bad request error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(badRequestResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Error deleting volume replication")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		internalServerErrorResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationInternalServerError{
			Message: "Internal server error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(internalServerErrorResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Error deleting volume replication")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unauthorizedResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationUnauthorized{
			Message: "Unauthorized error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(unauthorizedResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Error deleting volume replication")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		forbiddenResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationForbidden{
			Message: "Forbidden error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(forbiddenResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Error deleting volume replication")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		notFoundResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationNotFound{
			Message: "Not found error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(notFoundResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Error deleting volume replication")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		// Return a different response type that's not handled in the switch
		unknownResp := &googleproxyclient.V1betaInternalDeleteVolumeReplicationNoContent{}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(unknownResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Error deleting volume replication")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
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
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Error deleting volume replication", err.Error())
	})
}

func TestReleaseReplicationOnSource(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	correlationID := "correlation-id"

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
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(&googleproxyclient.OperationV1beta{Name: googleproxyclient.NewOptString("projects/123/locations/us-central1/volumes/vol-uuid/operations/job-uuid"), Done: googleproxyclient.NewOptBool(true)}, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnSource(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.JobId, "job-uuid")
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		badRequestResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationBadRequest{
			Message: "Bad request error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &dstPath,
			SrcProjectNumber: &dstProj,
			SrcJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(badRequestResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		internalServerErrorResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationInternalServerError{
			Message: "Internal server error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &dstPath,
			SrcProjectNumber: &dstProj,
			SrcJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(internalServerErrorResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unauthorizedResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationUnauthorized{
			Message: "Unauthorized error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &dstPath,
			SrcProjectNumber: &dstProj,
			SrcJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(unauthorizedResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		forbiddenResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationForbidden{
			Message: "Forbidden error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &dstPath,
			SrcProjectNumber: &dstProj,
			SrcJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(forbiddenResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		notFoundResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationNotFound{
			Message: "Not found error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &dstPath,
			SrcProjectNumber: &dstProj,
			SrcJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(notFoundResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		// Return a different response type that's not handled in the switch
		unknownResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationNoContent{}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &dstPath,
			SrcProjectNumber: &dstProj,
			SrcJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(unknownResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
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
			CorrelationID:    &correlationID,
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
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *deleteReplicationParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Error releasing volume replication", err.Error())
	})
}

func TestDeleteSnapmirrorSnapshotsOnDestination(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	correlationID := "correlation-id"

	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		res := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString("projects/123/locations/us-central1/volumes/vol-uuid/operations/job-uuid"),
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.DstProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(res, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.JobId, "job-uuid")
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		badRequestResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest{
			Message: "Bad request error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.DstProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(badRequestResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on destination")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		internalServerErrorResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotInternalServerError{
			Message: "Internal server error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.DstProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(internalServerErrorResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on destination")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unauthorizedResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotUnauthorized{
			Message: "Unauthorized error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.DstProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(unauthorizedResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on destination")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		forbiddenResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotForbidden{
			Message: "Forbidden error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.DstProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(forbiddenResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on destination")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		notFoundResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotNotFound{
			Message: "Not found error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.DstProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(notFoundResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on destination")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		// Return a different response type that's not handled in the switch
		unknownResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotTooManyRequests{
			Message: "Too many requests error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.DstProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(unknownResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on destination")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
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
			CorrelationID:    &correlationID,
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.DstProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Cannot delete snapshot while snapshot is in use", err.Error())
	})
}

func TestDeleteSnapmirrorSnapshotsOnSource(t *testing.T) {
	srcProj := "projSrc"
	srcPath := "srcPath"
	srcToken := "srcToken"
	correlationID := "correlation-id"

	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		res := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString("projects/123/locations/us-central1/volumes/vol-uuid/operations/job-uuid"),
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
							SourceVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.SrcProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(res, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "job-uuid", result.JobId)
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		badRequestResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest{
			Message: "Bad request error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
							SourceVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.SrcProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(badRequestResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on source")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		internalServerErrorResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotInternalServerError{
			Message: "Internal server error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
							SourceVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.SrcProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(internalServerErrorResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on source")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unauthorizedResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotUnauthorized{
			Message: "Unauthorized error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
							SourceVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.SrcProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(unauthorizedResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on source")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		forbiddenResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotForbidden{
			Message: "Forbidden error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
							SourceVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.SrcProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(forbiddenResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on source")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		notFoundResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotNotFound{
			Message: "Not found error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
							SourceVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.SrcProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(notFoundResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on source")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		// Return a different response type that's not handled in the switch
		unknownResp := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotTooManyRequests{
			Message: "Too many requests error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
							SourceVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.SrcProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(unknownResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Error deleting volume snapmirror snapshot on source")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "location-id",
							SourceReplicationUUID: "replication-uuid",
							SourceVolumeUUID:      "vol-uuid",
						},
					},
				},
			},
		}
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber:  *inputResult.SrcProjectNumber,
			LocationId:     inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Cannot delete snapshot while snapshot is in use", err.Error())
	})
}

func TestDeHydrateDestinationVolumeReplication(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	correlationID := "correlation-id"
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					Location:                 "us-east4-a",
					SourceProjectNumber:      "src-proj",
					DestinationProjectNumber: "dst-proj",
					ReplicationModel: &datamodel.VolumeReplication{
						Name: "replication-name",
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1-a",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
							DestinationVolumeName:      "volume-name",
							SourceLocation:             "us-central1",
							SourceVolumeName:           "vol-1",
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
		_, err := activity.DeHydrateDestinationVolumeReplication(ctx, inputResult)

		assert.Error(t, err)
		var customErr *vsaerrors.CustomError
		assert.True(t, vsaerrors.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(t, customErr.OriginalErr, "hydration error")
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccessForDestinationRegion", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					Location:                 "us-east4-a",
					SourceProjectNumber:      "src-proj",
					DestinationProjectNumber: "dst-proj",
					ReplicationModel: &datamodel.VolumeReplication{
						Name: "replication-name",
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1-a",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
							DestinationVolumeName:      "volume-name",
							SourceLocation:             "us-east4-a",
							SourceVolumeName:           "vol-1",
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
		_, err := activity.DeHydrateDestinationVolumeReplication(ctx, inputResult)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					Location:                 "us-central1-a",
					SourceProjectNumber:      "src-proj",
					DestinationProjectNumber: "dst-proj",
					ReplicationModel: &datamodel.VolumeReplication{
						Name: "replication-name",
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1-a",
							DestinationReplicationUUID: "replication-uuid",
							DestinationVolumeUUID:      "vol-uuid",
							DestinationVolumeName:      "volume-name",
							SourceLocation:             "us-east4-a",
							SourceVolumeName:           "vol-1",
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
		_, err := activity.DeHydrateDestinationVolumeReplication(ctx, inputResult)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestDescribeRemoteJobDelete(t *testing.T) {
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

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

		err := activity.DescribeRemoteJobForDelete(ctx, result)

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

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
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
		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := activity.DescribeRemoteJobForDelete(ctx, result)

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

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
		err := activity.DescribeRemoteJobForDelete(ctx, result)

		assert.Error(tt, err)
	})
}

func TestDescribeJobDeleteOnSource(t *testing.T) {
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

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

		err := activity.DescribeSourceJobForDelete(ctx, result)

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

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
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
		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := activity.DescribeSourceJobForDelete(ctx, result)

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

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			JobId:            "test-job-id",
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
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
		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
		err := activity.DescribeSourceJobForDelete(ctx, result)

		assert.Error(tt, err)
	})
}

func TestGetReplicationOnDestinationForDelete(t *testing.T) {
	dstPrj := "dstPrj"
	dstPath := "dstPath"
	dstToken := "dstToken"
	replicationUUID := "replication-uuid"
	locationID := "location-id"
	correlationID := "correlation-id"

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
			ProjectNumber:  dstPrj,
			LocationId:     locationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(okResp, nil)

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstReplication)
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		badRequestResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest{
			Message: "Bad request error message",
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  dstPrj,
			LocationId:     locationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(badRequestResp, nil)

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Error getting multiple replications for delete")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		internalServerErrorResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError{
			Message: "Internal server error message",
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  dstPrj,
			LocationId:     locationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(internalServerErrorResp, nil)

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Error getting multiple replications for delete")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		unauthorizedResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized{
			Message: "Unauthorized error message",
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  dstPrj,
			LocationId:     locationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(unauthorizedResp, nil)

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Error getting multiple replications for delete")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		forbiddenResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden{
			Message: "Forbidden error message",
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  dstPrj,
			LocationId:     locationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(forbiddenResp, nil)

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Error getting multiple replications for delete")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		notFoundResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound{}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  dstPrj,
			LocationId:     locationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(notFoundResp, nil)

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "")
		assert.Contains(tt, err.Error(), "Error getting multiple replications for delete")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		// Return a different response type that's not handled in the switch
		unknownResp := &googleproxyclient.V1betaGetMultipleReplicationsInternalNotImplemented{}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  dstPrj,
			LocationId:     locationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(unknownResp, nil)

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Error getting multiple replications for delete")
	})

	t.Run("Error", func(tt *testing.T) {
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  dstPrj,
			LocationId:     locationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{replicationUUID}}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(
			mock.Anything, &body, params,
		).Return(nil, errors.New("some-error"))

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstPrj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestDeleteVolumeOnDestination(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	volUUID := "vol-uuid"
	locationID := "location-id"
	correlationID := "correlation-id"

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
			ProjectNumber:  dstProj,
			LocationId:     locationID,
			VolumeId:       volUUID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(operation, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   locationID,
							DestinationVolumeUUID: volUUID,
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestination(ctx, inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "job-uuid", result.JobId)
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		badRequestResp := &googleproxyclient.V1betaDeleteVolumeBadRequest{
			Message: "Bad request error message",
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber:  dstProj,
			LocationId:     locationID,
			VolumeId:       volUUID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(badRequestResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   locationID,
							DestinationVolumeUUID: volUUID,
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestination(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Error deleting volume")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		internalServerErrorResp := &googleproxyclient.V1betaDeleteVolumeInternalServerError{
			Message: "Internal server error message",
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber:  dstProj,
			LocationId:     locationID,
			VolumeId:       volUUID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(internalServerErrorResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   locationID,
							DestinationVolumeUUID: volUUID,
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestination(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Error deleting volume")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		unauthorizedResp := &googleproxyclient.V1betaDeleteVolumeUnauthorized{
			Message: "Unauthorized error message",
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber:  dstProj,
			LocationId:     locationID,
			VolumeId:       volUUID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(unauthorizedResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   locationID,
							DestinationVolumeUUID: volUUID,
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestination(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Error deleting volume")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		forbiddenResp := &googleproxyclient.V1betaDeleteVolumeForbidden{
			Message: "Forbidden error message",
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber:  dstProj,
			LocationId:     locationID,
			VolumeId:       volUUID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(forbiddenResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   locationID,
							DestinationVolumeUUID: volUUID,
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestination(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Error deleting volume")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		notFoundResp := &googleproxyclient.V1betaDeleteVolumeNotFound{
			Message: "Not found error message",
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber:  dstProj,
			LocationId:     locationID,
			VolumeId:       volUUID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(notFoundResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   locationID,
							DestinationVolumeUUID: volUUID,
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestination(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Error deleting volume")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		// Return a different response type that's not handled in the switch
		unknownResp := &googleproxyclient.V1betaDeleteVolumeNoContent{}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber:  dstProj,
			LocationId:     locationID,
			VolumeId:       volUUID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(unknownResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   locationID,
							DestinationVolumeUUID: volUUID,
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestination(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Error deleting volume")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber:  dstProj,
			LocationId:     locationID,
			VolumeId:       volUUID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		body := googleproxyclient.OptV1betaDeleteVolumeReq{
			Set:   true,
			Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
		}
		mockClient.EXPECT().V1betaDeleteVolume(ctx, body, params).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   locationID,
							DestinationVolumeUUID: volUUID,
						},
					},
				},
			},
		}
		result, err := activity.DeleteVolumeOnDestination(ctx, inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to delete volume", err.Error())
	})
}

func TestUpdateReplicationRecordOnDestination(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	correlationID := "correlation-id"

	t.Run("WhenSuccessful", func(tt *testing.T) {
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
			CorrelationID:    &correlationID,
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
		releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams).Return(&googleproxyclient.OperationV1beta{Name: googleproxyclient.NewOptString("projects/123/locations/us-central1/volumes/vol-uuid/operations/job-uuid"), Done: googleproxyclient.NewOptBool(true)}, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnDestination(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.JobId, "job-uuid")
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		badRequestResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationBadRequest{
			Message: "Bad request error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams).Return(badRequestResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		internalServerErrorResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationInternalServerError{
			Message: "Internal server error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams).Return(internalServerErrorResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unauthorizedResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationUnauthorized{
			Message: "Unauthorized error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams).Return(unauthorizedResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		forbiddenResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationForbidden{
			Message: "Forbidden error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams).Return(forbiddenResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		notFoundResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationNotFound{
			Message: "Not found error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams).Return(notFoundResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unknownResp := &googleproxyclient.V1betaInternalReleaseVolumeReplicationMethodNotAllowed{}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams).Return(unknownResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
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
			CorrelationID:    &correlationID,
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
		releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationRecordOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
	})
}

func TestUpdateReplicationOnDestinationToErrorState(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	correlationID := "correlation-id"

	t.Run("WhenSuccessful", func(tt *testing.T) {
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
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(&googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestinationToErrorState(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		badRequestResp := &googleproxyclient.V1betaInternalUpdateStateBadRequest{
			Message: "Bad request error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(badRequestResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestinationToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		internalServerErrorResp := &googleproxyclient.V1betaInternalUpdateStateInternalServerError{
			Message: "Internal server error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(internalServerErrorResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestinationToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unauthorizedResp := &googleproxyclient.V1betaInternalUpdateStateUnauthorized{
			Message: "Unauthorized error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(unauthorizedResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestinationToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		forbiddenResp := &googleproxyclient.V1betaInternalUpdateStateForbidden{
			Message: "Forbidden error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(forbiddenResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestinationToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		notFoundResp := &googleproxyclient.V1betaInternalUpdateStateNotFound{
			Message: "Not found error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(notFoundResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestinationToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unknownResp := &googleproxyclient.V1betaInternalUpdateStateMethodNotAllowed{}
		inputResult := &replication.DeleteReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(unknownResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestinationToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
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
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestinationToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})
}

func TestUpdateReplicationOnSourceToErrorState(t *testing.T) {
	srcProj := "projSrc"
	srcPath := "srcPath"
	srcToken := "srcToken"
	correlationID := "correlation-id"

	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(&googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnSourceToErrorState(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		badRequestResp := &googleproxyclient.V1betaInternalUpdateStateBadRequest{
			Message: "Bad request error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(badRequestResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnSourceToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		internalServerErrorResp := &googleproxyclient.V1betaInternalUpdateStateInternalServerError{
			Message: "Internal server error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(internalServerErrorResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnSourceToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unauthorizedResp := &googleproxyclient.V1betaInternalUpdateStateUnauthorized{
			Message: "Unauthorized error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(unauthorizedResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnSourceToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		forbiddenResp := &googleproxyclient.V1betaInternalUpdateStateForbidden{
			Message: "Forbidden error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(forbiddenResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnSourceToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		notFoundResp := &googleproxyclient.V1betaInternalUpdateStateNotFound{
			Message: "Not found error message",
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(notFoundResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnSourceToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unknownResp := &googleproxyclient.V1betaInternalUpdateStateMethodNotAllowed{}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(unknownResp, nil)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnSourceToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
			CorrelationID:    &correlationID,
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
		updateRequest := &googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
			State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
			StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
		}
		updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
			ProjectNumber:       *inputResult.SrcProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalUpdateState(ctx, updateRequest, updateParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnSourceToErrorState(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Failed to update volume replication state on")
	})
}

func TestSetHybridReplicationVariablesDelete(t *testing.T) {
	ctx := context.Background()
	activity := DeleteVolumeReplicationActivity{}

	t.Run("WhenEventIsNil", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: nil,
		}

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.False(tt, updatedResult.IsHybridReplicationVolume)
	})

	t.Run("WhenReplicationModelIsNil", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: nil,
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.False(tt, updatedResult.IsHybridReplicationVolume)
	})

	t.Run("WhenHybridReplicationAttributesIsNil", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: nil,
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.False(tt, updatedResult.IsHybridReplicationVolume)
	})

	t.Run("WhenHybridReplicationAttributesIsSet", func(tt *testing.T) {
		migrationType := string(models.HybridReplicationParametersReplicationTypeMIGRATION)
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: &migrationType,
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
	})

	t.Run("WhenHybridReplicationTypeIsReverse", func(tt *testing.T) {
		reverseType := string(models.HybridReplicationParametersReplicationTypeREVERSE)
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: &reverseType,
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
	})

	t.Run("WhenHybridReplicationTypeIsNil", func(tt *testing.T) {
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: nil,
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "",
						},
					},
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
	})

	t.Run("WhenHybridReplicationAttributesIsSetButClusterPeerIdIsNotValid", func(tt *testing.T) {
		migrationType := string(models.HybridReplicationParametersReplicationTypeMIGRATION)
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: &migrationType,
						},
						ClusterPeerId: sql.NullInt64{Valid: false},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.CleanupClusterPeering)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetVolumeReplicationCountByClusterPeerIDFails", func(tt *testing.T) {
		migrationType := string(models.HybridReplicationParametersReplicationTypeMIGRATION)
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		clusterPeerID := int64(123)
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: &migrationType,
						},
						ClusterPeerId: sql.NullInt64{Int64: clusterPeerID, Valid: true},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		expectedError := errors.New("database error")
		mockStorage.On("GetVolumeReplicationCountByClusterPeerID", ctx, clusterPeerID).Return(int64(0), expectedError)

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetFlexCacheVolumeCountByClusterPeerIDFails", func(tt *testing.T) {
		migrationType := string(models.HybridReplicationParametersReplicationTypeMIGRATION)
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		clusterPeerID := int64(123)
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: &migrationType,
						},
						ClusterPeerId: sql.NullInt64{Int64: clusterPeerID, Valid: true},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		expectedError := errors.New("database error")
		mockStorage.On("GetVolumeReplicationCountByClusterPeerID", ctx, clusterPeerID).Return(int64(1), nil)
		mockStorage.On("GetFlexCacheVolumeCountByClusterPeerID", ctx, clusterPeerID).Return(int64(0), expectedError)

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenVolumeReplicationCountIsNotOne", func(tt *testing.T) {
		migrationType := string(models.HybridReplicationParametersReplicationTypeMIGRATION)
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		clusterPeerID := int64(123)
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: &migrationType,
						},
						ClusterPeerId: sql.NullInt64{Int64: clusterPeerID, Valid: true},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		mockStorage.On("GetVolumeReplicationCountByClusterPeerID", ctx, clusterPeerID).Return(int64(2), nil)
		mockStorage.On("GetFlexCacheVolumeCountByClusterPeerID", ctx, clusterPeerID).Return(int64(0), nil)

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.CleanupClusterPeering)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenFlexCacheCountIsNotZero", func(tt *testing.T) {
		migrationType := string(models.HybridReplicationParametersReplicationTypeMIGRATION)
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		clusterPeerID := int64(123)
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: &migrationType,
						},
						ClusterPeerId: sql.NullInt64{Int64: clusterPeerID, Valid: true},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		mockStorage.On("GetVolumeReplicationCountByClusterPeerID", ctx, clusterPeerID).Return(int64(1), nil)
		mockStorage.On("GetFlexCacheVolumeCountByClusterPeerID", ctx, clusterPeerID).Return(int64(1), nil)

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.CleanupClusterPeering)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenLastReplicationAndNoFlexCacheSetsCleanupFlag", func(tt *testing.T) {
		migrationType := string(models.HybridReplicationParametersReplicationTypeMIGRATION)
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		clusterPeerID := int64(123)
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
							HybridReplicationType: &migrationType,
						},
						ClusterPeerId: sql.NullInt64{Int64: clusterPeerID, Valid: true},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-central1",
						},
					},
				},
			},
		}

		mockStorage.On("GetVolumeReplicationCountByClusterPeerID", ctx, clusterPeerID).Return(int64(1), nil)
		mockStorage.On("GetFlexCacheVolumeCountByClusterPeerID", ctx, clusterPeerID).Return(int64(0), nil)

		updatedResult, err := activity.SetHybridReplicationVariablesDelete(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.True(tt, updatedResult.CleanupClusterPeering)
		mockStorage.AssertExpectations(tt)
	})
}

func TestDeleteRoleInOntap(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	activity := DeleteVolumeReplicationActivity{SE: database.NewMockStorage(t)}
	node := &models.Node{}

	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		roleName := "external-peer"
		ownerUUID := "owner-uuid-123"
		roles := []*vsa.Role{
			{
				Name:    roleName,
				OwnerID: ownerUUID,
			},
		}

		mockProvider.On("GetRoleCollection", mock.MatchedBy(func(params vsa.GetRoleCollectionParams) bool {
			return params.Name != nil && *params.Name == roleName
		})).Return(roles, nil)

		mockProvider.On("DeleteRole", mock.MatchedBy(func(params vsa.DeleteRoleParams) bool {
			return params.Name == roleName && params.OwnerUUID != nil && *params.OwnerUUID == ownerUUID
		})).Return(nil)

		err := activity.DeleteRoleInOntap(ctx, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		expectedError := errors.New("failed to get provider")
		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		err := activity.DeleteRoleInOntap(ctx, node)

		assert.Error(tt, err)
	})

	t.Run("WhenGetRoleCollectionFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		roleName := "external-peer"
		expectedError := errors.New("failed to get role collection")

		mockProvider.On("GetRoleCollection", mock.MatchedBy(func(params vsa.GetRoleCollectionParams) bool {
			return params.Name != nil && *params.Name == roleName
		})).Return(nil, expectedError)

		err := activity.DeleteRoleInOntap(ctx, node)

		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, expectedError.Error(), customErr.OriginalErr.Error())
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenRoleDoesNotExist", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		roleName := "external-peer"
		roles := []*vsa.Role{}

		mockProvider.On("GetRoleCollection", mock.MatchedBy(func(params vsa.GetRoleCollectionParams) bool {
			return params.Name != nil && *params.Name == roleName
		})).Return(roles, nil)

		err := activity.DeleteRoleInOntap(ctx, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenDeleteRoleFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		roleName := "external-peer"
		ownerUUID := "owner-uuid-123"
		roles := []*vsa.Role{
			{
				Name:    roleName,
				OwnerID: ownerUUID,
			},
		}
		expectedError := errors.New("failed to delete role")

		mockProvider.On("GetRoleCollection", mock.MatchedBy(func(params vsa.GetRoleCollectionParams) bool {
			return params.Name != nil && *params.Name == roleName
		})).Return(roles, nil)

		mockProvider.On("DeleteRole", mock.MatchedBy(func(params vsa.DeleteRoleParams) bool {
			return params.Name == roleName && params.OwnerUUID != nil && *params.OwnerUUID == ownerUUID
		})).Return(expectedError)

		err := activity.DeleteRoleInOntap(ctx, node)

		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, expectedError.Error(), customErr.OriginalErr.Error())
		mockProvider.AssertExpectations(tt)
	})
}

func TestDeleteClusterPeeringInOntap(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	activity := DeleteVolumeReplicationActivity{SE: database.NewMockStorage(t)}
	node := &models.Node{}
	clusterPeerUUID := "cluster-peer-uuid-123"

	result := &replication.DeleteReplicationResult{
		Event: &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ClusterPeer: &datamodel.ClusterPeerings{
						OntapPeerUUID: clusterPeerUUID,
					},
				},
			},
		},
	}

	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("DeleteClusterPeer", clusterPeerUUID).Return(nil)

		err := activity.DeleteClusterPeeringInOntap(ctx, result, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		expectedError := errors.New("failed to get provider")
		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		err := activity.DeleteClusterPeeringInOntap(ctx, result, node)

		assert.Error(tt, err)
	})

	t.Run("WhenDeleteClusterPeerFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		expectedError := errors.New("failed to delete cluster peer")
		mockProvider.On("DeleteClusterPeer", clusterPeerUUID).Return(expectedError)

		err := activity.DeleteClusterPeeringInOntap(ctx, result, node)

		assert.Error(tt, err)
		mockProvider.AssertExpectations(tt)
	})
}

func TestDeleteClusterPeeringDB(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	clusterPeerUUID := "cluster-peer-uuid-123"

	result := &replication.DeleteReplicationResult{
		Event: &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: clusterPeerUUID},
						State:     models.CvpClusterPeeringStatusPEERED,
					},
				},
			},
		},
	}

	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(cpr *datamodel.ClusterPeerings) bool {
			return cpr.State == models.CvpClusterPeeringStatusDELETED &&
				cpr.DeletedAt != nil &&
				cpr.DeletedAt.Valid == true &&
				cpr.UpdatedAt.Equal(cpr.DeletedAt.Time)
		})).Return(nil)

		err := activity.DeleteClusterPeeringDB(ctx, result)

		assert.NoError(tt, err)
		assert.Equal(tt, models.CvpClusterPeeringStatusDELETED, result.Event.ReplicationModel.ClusterPeer.State)
		assert.NotNil(tt, result.Event.ReplicationModel.ClusterPeer.DeletedAt)
		assert.True(tt, result.Event.ReplicationModel.ClusterPeer.DeletedAt.Valid)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateClusterPeeringRowFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		expectedError := errors.New("database update error")
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.Anything).Return(expectedError)

		err := activity.DeleteClusterPeeringDB(ctx, result)

		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestRemovePathFromSnapmirrorQuery(t *testing.T) {
	t.Run("WhenExistingPrivilegeIsNil", func(tt *testing.T) {
		result := removePathFromSnapmirrorQuery(nil, "svm1:vol1")
		assert.Equal(tt, "", result)
	})

	t.Run("WhenQueryIsEmpty", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm1:vol1")
		assert.Equal(tt, "", result)
	})

	t.Run("WhenSinglePathMatchesAndGetsRemoved", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm1:vol1")
		assert.Equal(tt, "", result)
	})

	t.Run("WhenSinglePathDoesNotMatch", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm2:vol2")
		assert.Equal(tt, "-source-path svm1:vol1", result)
	})

	t.Run("WhenMultiplePathsRemoveFirst", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1|svm2:vol2|svm3:vol3",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm1:vol1")
		assert.Equal(tt, "-source-path svm2:vol2|svm3:vol3", result)
	})

	t.Run("WhenMultiplePathsRemoveMiddle", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1|svm2:vol2|svm3:vol3",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm2:vol2")
		assert.Equal(tt, "-source-path svm1:vol1|svm3:vol3", result)
	})

	t.Run("WhenMultiplePathsRemoveLast", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1|svm2:vol2|svm3:vol3",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm3:vol3")
		assert.Equal(tt, "-source-path svm1:vol1|svm2:vol2", result)
	})

	t.Run("WhenMultiplePathsRemoveNonExistent", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1|svm2:vol2|svm3:vol3",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm4:vol4")
		assert.Equal(tt, "-source-path svm1:vol1|svm2:vol2|svm3:vol3", result)
	})

	t.Run("WhenAllPathsGetRemoved", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1|svm1:vol1|svm1:vol1",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm1:vol1")
		assert.Equal(tt, "", result)
	})

	t.Run("WhenPathsHaveWhitespace", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path  svm1:vol1  |  svm2:vol2  |  svm3:vol3  ",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm2:vol2")
		assert.Equal(tt, "-source-path svm1:vol1|svm3:vol3", result)
	})

	t.Run("WhenQueryHasNoPrefix", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "svm1:vol1|svm2:vol2",
		}
		// TrimPrefix won't remove anything, so pathsStr will be the full query
		result := removePathFromSnapmirrorQuery(privilege, "svm1:vol1")
		// This will still work, just without the prefix in the result
		assert.Equal(tt, "-source-path svm2:vol2", result)
	})

	t.Run("WhenTwoPathsRemoveOne", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1|svm2:vol2",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm1:vol1")
		assert.Equal(tt, "-source-path svm2:vol2", result)
	})

	t.Run("WhenTwoPathsRemoveBoth", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path svm1:vol1|svm1:vol1",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm1:vol1")
		assert.Equal(tt, "", result)
	})

	t.Run("WhenMultiplePathsWithComplexNames", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path gcnv-3f9278cc0d104d0-svm-01:pcdst22|gcnv-3f9278cc0d104d0-svm-01:pcdst23|gcnv-3f9278cc0d104d0-svm-01:pcdst24",
		}
		result := removePathFromSnapmirrorQuery(privilege, "gcnv-3f9278cc0d104d0-svm-01:pcdst23")
		assert.Equal(tt, "-source-path gcnv-3f9278cc0d104d0-svm-01:pcdst22|gcnv-3f9278cc0d104d0-svm-01:pcdst24", result)
	})

	t.Run("WhenRealWorldExampleFromReference", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path gcnv-3f9278cc0d104d0-svm-01:pcdst22|gcnv-3f9278cc0d104d0-svm-01:pcdst23",
		}
		result := removePathFromSnapmirrorQuery(privilege, "gcnv-3f9278cc0d104d0-svm-01:pcdst22")
		assert.Equal(tt, "-source-path gcnv-3f9278cc0d104d0-svm-01:pcdst23", result)
	})

	t.Run("WhenRealWorldExampleRemoveLast", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path gcnv-3f9278cc0d104d0-svm-01:pcdst22|gcnv-3f9278cc0d104d0-svm-01:pcdst23",
		}
		result := removePathFromSnapmirrorQuery(privilege, "gcnv-3f9278cc0d104d0-svm-01:pcdst23")
		assert.Equal(tt, "-source-path gcnv-3f9278cc0d104d0-svm-01:pcdst22", result)
	})

	t.Run("WhenQueryHasExtraSpaces", func(tt *testing.T) {
		privilege := &vsa.RolePrivilege{
			Path:   "snapmirror resync",
			Access: "readonly",
			Query:  "-source-path   svm1:vol1   |   svm2:vol2   ",
		}
		result := removePathFromSnapmirrorQuery(privilege, "svm1:vol1")
		assert.Equal(tt, "-source-path svm2:vol2", result)
	})
}

func TestDeleteVolumeReplicationActivity_UpdateRbacRole(t *testing.T) {
	t.Run("WhenNotHybridReplicationVolume", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: false,
			IsSrcForHybridReplication: true,
		}
		node := &models.Node{Name: "test-node"}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
	})

	t.Run("WhenNotSrcForHybridReplication", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: false,
		}
		node := &models.Node{Name: "test-node"}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
	})

	t.Run("WhenEventIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event:                     nil,
		}
		node := &models.Node{Name: "test-node"}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenReplicationModelIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: nil,
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenReplicationAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: nil,
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("provider error")
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetRoleCollectionFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return(nil, assert.AnError)

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenRoleNotFound", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return([]*vsa.Role{}, nil)

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSnapmirrorPrivilegeNotFound", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		targetRole := &vsa.Role{
			OwnerID: "owner-id",
			Privileges: []*vsa.RolePrivilege{
				{
					Path:  "other-privilege",
					Query: "some-query",
				},
			},
		}

		mockProvider.On("GetRoleCollection", vsa.GetRoleCollectionParams{
			Name: nillable.GetStringPtr(onPremPeerRole),
		}).Return([]*vsa.Role{targetRole}, nil)

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_ModifyPrivilegeWithRemainingPaths", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  SnapmirrorResyncPrivilegePath,
			Query: "-source-path source-svm:source-volume|other-svm:other-volume",
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
				params.Path == SnapmirrorResyncPrivilegePath &&
				params.Access == SnapmirrorResyncPrivilegeAccess &&
				params.Query == "-source-path other-svm:other-volume"
		})).Return(nil)

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_AllPathsRemoved", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  SnapmirrorResyncPrivilegePath,
			Query: "-source-path source-svm:source-volume",
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

		mockProvider.On("DeleteRolePrivilege", mock.MatchedBy(func(params vsa.DeleteRolePrivilegeParams) bool {
			return params.OwnerID == "owner-id" &&
				params.Name == onPremPeerRole &&
				params.Path == SnapmirrorResyncPrivilegePath
		})).Return(nil)

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenDeleteRolePrivilegeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  SnapmirrorResyncPrivilegePath,
			Query: "-source-path source-svm:source-volume",
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

		mockProvider.On("DeleteRolePrivilege", mock.MatchedBy(func(params vsa.DeleteRolePrivilegeParams) bool {
			return params.OwnerID == "owner-id" &&
				params.Name == onPremPeerRole &&
				params.Path == SnapmirrorResyncPrivilegePath
		})).Return(fmt.Errorf("failed to delete privilege"))

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenModifyRolePrivilegeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  SnapmirrorResyncPrivilegePath,
			Query: "-source-path source-svm:source-volume|other-svm:other-volume",
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

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_MultiplePathsRemoveMiddle", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "source-svm",
							SourceVolumeName: "source-volume",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  SnapmirrorResyncPrivilegePath,
			Query: "-source-path first-svm:first-volume|source-svm:source-volume|last-svm:last-volume",
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
				params.Path == SnapmirrorResyncPrivilegePath &&
				params.Access == SnapmirrorResyncPrivilegeAccess &&
				params.Query == "-source-path first-svm:first-volume|last-svm:last-volume"
		})).Return(nil)

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_RealWorldExample", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceSvmName:    "gcnv-3f9278cc0d104d0-svm-01",
							SourceVolumeName: "pcdst22",
						},
					},
				},
			},
		}
		node := &models.Node{Name: "test-node"}

		mockProvider := &vsa.MockProvider{}
		existingPrivilege := &vsa.RolePrivilege{
			Path:  SnapmirrorResyncPrivilegePath,
			Query: "-source-path gcnv-3f9278cc0d104d0-svm-01:pcdst22|gcnv-3f9278cc0d104d0-svm-01:pcdst23",
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
				params.Path == SnapmirrorResyncPrivilegePath &&
				params.Access == SnapmirrorResyncPrivilegeAccess &&
				params.Query == "-source-path gcnv-3f9278cc0d104d0-svm-01:pcdst23"
		})).Return(nil)

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updatedResult, err := activity.UpdateRbacRole(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})
}

func TestDeleteVolumeReplicationActivity_ReleaseReplicationOnSrc(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		node := &models.Node{
			Name: "test-node",
		}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						Volume: &datamodel.Volume{
							VolumeAttributes: &datamodel.VolumeAttributes{
								ExternalUUID: "volume-external-uuid",
							},
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							EndpointType:          "src",
							SourceHostName:        "src-host",
							SourceSvmName:         "src-svm",
							SourceVolumeName:      "src-vol",
							DestinationHostName:   "dst-host",
							DestinationSvmName:    "dst-svm",
							DestinationVolumeName: "dst-vol",
							ReplicationSchedule:   "hourly",
							ReplicationType:       "ExternalDisasterRecovery",
						},
					},
				},
			},
		}

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ReleaseVolumeReplication", mock.MatchedBy(func(params *vsa.ReleaseVolumeReplicationParams) bool {
			return params.VolumeReplication != nil &&
				params.VolumeReplication.EndpointType == "src" &&
				params.VolumeReplication.SourceHostName == "src-host" &&
				params.VolumeReplication.SourceSVMName == "src-svm" &&
				params.VolumeReplication.SourceVolumeName == "src-vol" &&
				params.VolumeReplication.DestinationHostName == "dst-host" &&
				params.VolumeReplication.DestinationSVMName == "dst-svm" &&
				params.VolumeReplication.DestinationVolumeName == "dst-vol" &&
				params.VolumeReplication.ReplicationSchedule == "hourly" &&
				params.VolumeReplication.ReplicationType == "ExternalDisasterRecovery" &&
				params.VolumeReplication.Volume != nil &&
				params.VolumeReplication.Volume.ExternalUUID == "volume-external-uuid" &&
				params.ReverseResync == false
		})).Return(&vsa.VolumeReplication{}, nil)

		updatedResult, err := activity.ReleaseReplicationOnSrc(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		node := &models.Node{
			Name: "test-node",
		}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						Volume: &datamodel.Volume{
							VolumeAttributes: &datamodel.VolumeAttributes{
								ExternalUUID: "volume-external-uuid",
							},
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							EndpointType: "src",
						},
					},
				},
			},
		}

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("failed to get provider")
		}

		updatedResult, err := activity.ReleaseReplicationOnSrc(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenReleaseVolumeReplicationFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		node := &models.Node{
			Name: "test-node",
		}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						Volume: &datamodel.Volume{
							VolumeAttributes: &datamodel.VolumeAttributes{
								ExternalUUID: "volume-external-uuid",
							},
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							EndpointType:          "src",
							SourceHostName:        "src-host",
							SourceSvmName:         "src-svm",
							SourceVolumeName:      "src-vol",
							DestinationHostName:   "dst-host",
							DestinationSvmName:    "dst-svm",
							DestinationVolumeName: "dst-vol",
							ReplicationSchedule:   "hourly",
							ReplicationType:       "ExternalDisasterRecovery",
						},
					},
				},
			},
		}

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ReleaseVolumeReplication", mock.AnythingOfType("*vsa.ReleaseVolumeReplicationParams")).Return(nil, errors.New("failed to release replication"))

		updatedResult, err := activity.ReleaseReplicationOnSrc(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenReleaseVolumeReplicationFailsWithSVMPeeringCleanupTimeout", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		node := &models.Node{
			Name: "test-node",
		}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						Volume: &datamodel.Volume{
							VolumeAttributes: &datamodel.VolumeAttributes{
								ExternalUUID: "volume-external-uuid",
							},
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							EndpointType:          "src",
							SourceHostName:        "src-host",
							SourceSvmName:         "src-svm",
							SourceVolumeName:      "src-vol",
							DestinationHostName:   "dst-host",
							DestinationSvmName:    "dst-svm",
							DestinationVolumeName: "dst-vol",
							ReplicationSchedule:   "hourly",
							ReplicationType:       "ExternalDisasterRecovery",
						},
					},
				},
			},
		}

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ReleaseVolumeReplication", mock.AnythingOfType("*vsa.ReleaseVolumeReplicationParams")).Return(nil, errors.New("Timeout during cleanup of peering infrastructure."))

		updatedResult, err := activity.ReleaseReplicationOnSrc(ctx, result, node)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Relationship is in use by SnapMirror in peer cluster, Delete the replication first on on-prem cluster and then try again")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccessWithAllFields", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		node := &models.Node{
			Name: "test-node",
		}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						Volume: &datamodel.Volume{
							VolumeAttributes: &datamodel.VolumeAttributes{
								ExternalUUID: "gcnv-volume-uuid-123",
							},
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							EndpointType:          "dst",
							SourceHostName:        "gcnv-3f9278cc0d104d0-svm-01",
							SourceSvmName:         "source-svm-name",
							SourceVolumeName:      "source-volume-name",
							DestinationHostName:   "destination-host",
							DestinationSvmName:    "destination-svm",
							DestinationVolumeName: "destination-volume",
							ReplicationSchedule:   "10minutely",
							ReplicationType:       "ExternalDisasterRecovery",
						},
					},
				},
			},
		}

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ReleaseVolumeReplication", mock.MatchedBy(func(params *vsa.ReleaseVolumeReplicationParams) bool {
			return params.VolumeReplication != nil &&
				params.VolumeReplication.EndpointType == "dst" &&
				params.VolumeReplication.SourceHostName == "gcnv-3f9278cc0d104d0-svm-01" &&
				params.VolumeReplication.SourceSVMName == "source-svm-name" &&
				params.VolumeReplication.SourceVolumeName == "source-volume-name" &&
				params.VolumeReplication.DestinationHostName == "destination-host" &&
				params.VolumeReplication.DestinationSVMName == "destination-svm" &&
				params.VolumeReplication.DestinationVolumeName == "destination-volume" &&
				params.VolumeReplication.ReplicationSchedule == "10minutely" &&
				params.VolumeReplication.ReplicationType == "ExternalDisasterRecovery" &&
				params.VolumeReplication.Volume != nil &&
				params.VolumeReplication.Volume.ExternalUUID == "gcnv-volume-uuid-123" &&
				params.ReverseResync == false
		})).Return(&vsa.VolumeReplication{}, nil)

		updatedResult, err := activity.ReleaseReplicationOnSrc(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccessWithEmptyFields", func(tt *testing.T) {
		originalGetProviderByNode := hyperscalerGetProviderByNode
		defer func() { hyperscalerGetProviderByNode = originalGetProviderByNode }()

		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		node := &models.Node{
			Name: "test-node",
		}
		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						Volume: &datamodel.Volume{
							VolumeAttributes: &datamodel.VolumeAttributes{
								ExternalUUID: "",
							},
						},
						ReplicationAttributes: &datamodel.ReplicationDetails{
							EndpointType:          "",
							SourceHostName:        "",
							SourceSvmName:         "",
							SourceVolumeName:      "",
							DestinationHostName:   "",
							DestinationSvmName:    "",
							DestinationVolumeName: "",
							ReplicationSchedule:   "",
							ReplicationType:       "",
						},
					},
				},
			},
		}

		hyperscalerGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ReleaseVolumeReplication", mock.MatchedBy(func(params *vsa.ReleaseVolumeReplicationParams) bool {
			return params.VolumeReplication != nil &&
				params.VolumeReplication.EndpointType == "" &&
				params.VolumeReplication.SourceHostName == "" &&
				params.VolumeReplication.SourceSVMName == "" &&
				params.VolumeReplication.SourceVolumeName == "" &&
				params.VolumeReplication.DestinationHostName == "" &&
				params.VolumeReplication.DestinationSVMName == "" &&
				params.VolumeReplication.DestinationVolumeName == "" &&
				params.VolumeReplication.ReplicationSchedule == "" &&
				params.VolumeReplication.ReplicationType == "" &&
				params.VolumeReplication.Volume != nil &&
				params.VolumeReplication.Volume.ExternalUUID == "" &&
				params.ReverseResync == false
		})).Return(&vsa.VolumeReplication{}, nil)

		updatedResult, err := activity.ReleaseReplicationOnSrc(ctx, result, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})
}

func TestDeleteVolumeReplicationActivity_UpdateReplicationInDBToErrorState(t *testing.T) {
	t.Run("WhenReplicationModelIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: nil,
				},
			},
		}

		err := activity.UpdateReplicationInDBToErrorState(ctx, result)

		assert.Error(tt, err)
	})

	t.Run("WhenUpdateVolumeReplicationStatesFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						BaseModel: datamodel.BaseModel{
							UUID: "test-uuid",
						},
						State:        "ACTIVE",
						StateDetails: "Active state",
					},
				},
			},
		}

		expectedError := errors.New("database update error")
		mockStorage.On("UpdateVolumeReplicationStates", ctx, result.Event.ReplicationModel).Return(expectedError)

		err := activity.UpdateReplicationInDBToErrorState(ctx, result)

		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, expectedError.Error(), customErr.OriginalErr.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						BaseModel: datamodel.BaseModel{
							UUID: "test-uuid",
						},
						State:        "ACTIVE",
						StateDetails: "Active state",
					},
				},
			},
		}

		mockStorage.On("UpdateVolumeReplicationStates", ctx, mock.MatchedBy(func(volumeRep *datamodel.VolumeReplication) bool {
			return volumeRep.State == models.LifeCycleStateError &&
				volumeRep.StateDetails == models.LifeCycleStateDeletionErrorDetails &&
				volumeRep.UUID == "test-uuid"
		})).Return(nil)

		err := activity.UpdateReplicationInDBToErrorState(ctx, result)

		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateError, result.Event.ReplicationModel.State)
		assert.Equal(tt, models.LifeCycleStateDeletionErrorDetails, result.Event.ReplicationModel.StateDetails)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccessWithExistingState", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						BaseModel: datamodel.BaseModel{
							UUID: "test-uuid-123",
						},
						State:        "CREATED",
						StateDetails: "Created successfully",
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}

		mockStorage.On("UpdateVolumeReplicationStates", ctx, mock.MatchedBy(func(volumeRep *datamodel.VolumeReplication) bool {
			return volumeRep.State == models.LifeCycleStateError &&
				volumeRep.StateDetails == models.LifeCycleStateDeletionErrorDetails &&
				volumeRep.UUID == "test-uuid-123" &&
				volumeRep.ReplicationAttributes != nil &&
				volumeRep.ReplicationAttributes.SourceLocation == "us-central1"
		})).Return(nil)

		err := activity.UpdateReplicationInDBToErrorState(ctx, result)

		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateError, result.Event.ReplicationModel.State)
		assert.Equal(tt, models.LifeCycleStateDeletionErrorDetails, result.Event.ReplicationModel.StateDetails)
		// Verify that other fields are preserved
		assert.Equal(tt, "us-central1", result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
		mockStorage.AssertExpectations(tt)
	})
}

func TestDeleteVolumeReplicationActivity_DeleteReplicationRecordOnSource(t *testing.T) {
	t.Run("WhenEventIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			Event: nil,
		}

		err := activity.DeleteReplicationRecordOnSource(ctx, result)

		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Contains(tt, customErr.OriginalErr.Error(), "replication model is nil")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenReplicationModelIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: nil,
				},
			},
		}

		err := activity.DeleteReplicationRecordOnSource(ctx, result)

		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Contains(tt, customErr.OriginalErr.Error(), "replication model is nil")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenDeleteVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		replicationModel := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-replication-uuid",
			},
			StateDetails: "Active state",
		}

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: replicationModel,
				},
			},
		}

		expectedError := errors.New("database delete error")
		mockStorage.On("DeleteVolumeReplication", ctx, replicationModel).Return(nil, expectedError)

		err := activity.DeleteReplicationRecordOnSource(ctx, result)

		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}

		replicationModel := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-replication-uuid",
			},
			StateDetails: "Active state",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourceLocation:        "us-central1",
				SourceReplicationUUID: "src-repl-uuid",
			},
		}

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: replicationModel,
				},
			},
		}

		deletedReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-replication-uuid",
			},
			State:        models.LifeCycleStateDeleted,
			StateDetails: models.LifeCycleStateDeletedDetails,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourceLocation:        "us-central1",
				SourceReplicationUUID: "src-repl-uuid",
			},
		}

		mockStorage.On("DeleteVolumeReplication", ctx, mock.MatchedBy(func(volRep *datamodel.VolumeReplication) bool {
			return volRep.UUID == "test-replication-uuid" &&
				volRep.ReplicationAttributes != nil &&
				volRep.ReplicationAttributes.SourceLocation == "us-central1"
		})).Return(deletedReplication, nil)

		err := activity.DeleteReplicationRecordOnSource(ctx, result)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}
