package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGetSrcBasePathStop(t *testing.T) {
	t.Run("ValidSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}
		activity := StopVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://src-base-path.example.com", nil
		}

		updatedResult, err := activity.GetSrcBasePathStop(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcBasePath)
		assert.Equal(tt, "https://src-base-path.example.com", *updatedResult.SrcBasePath)
	})
	t.Run("ErrorSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}
		activity := StopVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetSrcBasePathStop(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetDstBasePathStop(t *testing.T) {
	t.Run("ValidDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := StopVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		updatedResult, err := activity.GetDstBasePathStop(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})
	t.Run("ErrorDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := StopVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetDstBasePathStop(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedSrcTokenStop(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
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

		updatedResult, err := activity.GetSignedSrcTokenStop(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.SrcJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
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

		updatedResult, err := activity.GetSignedSrcTokenStop(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedDstTokenStop(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenStop(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenSuccessSameProject", func(tt *testing.T) {
		prj := "prj"
		token := "signed-token"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenStop(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result := &replication.StopReplicationResult{
			Event: &replication.StopReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenStop(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestStopReplicationOnDestination(t *testing.T) {
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
		inputResult := &replication.StopReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.StopReplicationEvent{
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
		stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(inputResult.Event.ForceStop),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams).Return(res, nil)
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.StopReplicationOnDestination(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, *result.JobId, "job-uuid")
	})

	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		badRequestResp := &googleproxyclient.V1betaInternalStopVolumeReplicationBadRequest{
			Message: "Bad request error message",
		}
		inputResult := &replication.StopReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.StopReplicationEvent{
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
		stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(inputResult.Event.ForceStop),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams).Return(badRequestResp, nil)
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.StopReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Bad request error message")
		assert.Contains(tt, err.Error(), "Error stopping volume replication")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		internalServerErrorResp := &googleproxyclient.V1betaInternalStopVolumeReplicationInternalServerError{
			Message: "Internal server error message",
		}
		inputResult := &replication.StopReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.StopReplicationEvent{
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
		stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(inputResult.Event.ForceStop),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams).Return(internalServerErrorResp, nil)
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.StopReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Internal server error message")
		assert.Contains(tt, err.Error(), "Error stopping volume replication")
	})

	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unauthorizedResp := &googleproxyclient.V1betaInternalStopVolumeReplicationUnauthorized{
			Message: "Unauthorized error message",
		}
		inputResult := &replication.StopReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.StopReplicationEvent{
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
		stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(inputResult.Event.ForceStop),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams).Return(unauthorizedResp, nil)
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.StopReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Unauthorized error message")
		assert.Contains(tt, err.Error(), "Error stopping volume replication")
	})

	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		forbiddenResp := &googleproxyclient.V1betaInternalStopVolumeReplicationForbidden{
			Message: "Forbidden error message",
		}
		inputResult := &replication.StopReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.StopReplicationEvent{
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
		stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(inputResult.Event.ForceStop),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams).Return(forbiddenResp, nil)
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.StopReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Forbidden error message")
		assert.Contains(tt, err.Error(), "Error stopping volume replication")
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		notFoundResp := &googleproxyclient.V1betaInternalStopVolumeReplicationNotFound{
			Message: "Not found error message",
		}
		inputResult := &replication.StopReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.StopReplicationEvent{
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
		stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(inputResult.Event.ForceStop),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams).Return(notFoundResp, nil)
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.StopReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "Not found error message")
		assert.Contains(tt, err.Error(), "Error stopping volume replication")
	})

	t.Run("WhenUnknownResponseType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		unknownResp := &googleproxyclient.V1betaInternalStopVolumeReplicationMethodNotAllowed{
			Message: "Method not allowed error message",
		}
		inputResult := &replication.StopReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.StopReplicationEvent{
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
		stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(inputResult.Event.ForceStop),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams).Return(unknownResp, nil)
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.StopReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "unknown response type")
		assert.Contains(tt, err.Error(), "Error stopping volume replication")
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.StopReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			CorrelationID:    &correlationID,
			Event: &replication.StopReplicationEvent{
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
		stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.CorrelationID),
		}
		stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
			Force: googleproxyclient.NewOptBool(inputResult.Event.ForceStop),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams).Return(nil, errors.New("some-error"))
		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.StopReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		// The error gets wrapped with NewVCPError, so we need to check the wrapped error
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, "some-error", customErr.OriginalErr.Error())
	})
}

func TestDescribeRemoteJobStop(t *testing.T) {
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

		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result := &replication.StopReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.StopReplicationEvent{
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

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

		err := activity.DescribeDestJobStop(ctx, result)

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

		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result := &replication.StopReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.StopReplicationEvent{
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

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := activity.DescribeDestJobStop(ctx, result)

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

		activity := StopVolumeReplicationActivity{SE: mockStorage}
		result := &replication.StopReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			CorrelationID:    nillable.GetStringPtr("test-correlation-id"),
			Event: &replication.StopReplicationEvent{
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

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
		err := activity.DescribeDestJobStop(ctx, result)

		assert.Error(tt, err)
	})
}
