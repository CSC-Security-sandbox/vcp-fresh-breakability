package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGetSrcBasePathUpdate(t *testing.T) {
	t.Run("ValidSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}
		activity := VolumeReplicationUpdateActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://src-base-path.example.com", nil
		}

		updatedResult, err := activity.GetSrcBasePathUpdate(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcBasePath)
		assert.Equal(tt, "https://src-base-path.example.com", *updatedResult.SrcBasePath)
	})
	t.Run("ErrorSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}
		activity := VolumeReplicationUpdateActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetSrcBasePathUpdate(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
	t.Run("WhenSourceLocationIsRemoteRegionCustomer", func(tt *testing.T) {
		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: RemoteRegionCustomer,
						},
					},
				},
			},
		}
		activity := VolumeReplicationUpdateActivity{}

		updatedResult, err := activity.GetSrcBasePathUpdate(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Nil(tt, updatedResult.SrcBasePath)
	})
}

func TestGetDstBasePathUpdate(t *testing.T) {
	t.Run("ValidDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := VolumeReplicationUpdateActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		updatedResult, err := activity.GetDstBasePathUpdate(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})
	t.Run("ErrorDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := VolumeReplicationUpdateActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetDstBasePathUpdate(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
	t.Run("WhenDestinationLocationIsRemoteRegionCustomer", func(tt *testing.T) {
		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: RemoteRegionCustomer,
						},
					},
				},
			},
		}
		activity := VolumeReplicationUpdateActivity{}

		updatedResult, err := activity.GetDstBasePathUpdate(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Nil(tt, updatedResult.DstBasePath)
	})
}

func TestGetSignedSrcTokenUpdate(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
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

		updatedResult, err := activity.GetSignedSrcTokenUpdate(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.SrcJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
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

		updatedResult, err := activity.GetSignedSrcTokenUpdate(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedDstTokenUpdate(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenUpdate(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenSuccessSameProject", func(tt *testing.T) {
		prj := "prj"
		token := "signed-token"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenUpdate(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result := &replication.UpdateReplicationResult{
			Event: &replication.UpdateReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenUpdate(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestUpdateReplicationOnDestination(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("schedule"),
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(nil, errors.New("some-error"))
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "some-error", err.Error())
	})
	t.Run("WhenBadRequest", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("schedule"),
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
		}
		badRequestResponse := &googleproxyclient.V1betaInternalUpdateVolumeReplicationBadRequest{
			Code:    400,
			Message: "Bad Request",
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(badRequestResponse, nil)
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to update volume replication", err.Error())
	})
	t.Run("WhenUnauthorized", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("schedule"),
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
		}
		unauthorizedResponse := &googleproxyclient.V1betaInternalUpdateVolumeReplicationUnauthorized{
			Code:    401,
			Message: "Unauthorized",
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(unauthorizedResponse, nil)
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to update volume replication", err.Error())
	})
	t.Run("WhenForbidden", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("schedule"),
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
		}
		forbiddenResponse := &googleproxyclient.V1betaInternalUpdateVolumeReplicationForbidden{
			Code:    403,
			Message: "Access denied",
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(forbiddenResponse, nil)
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to update volume replication", err.Error())
	})
	t.Run("WhenNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("schedule"),
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
		}
		notFoundResponse := &googleproxyclient.V1betaInternalUpdateVolumeReplicationNotFound{
			Code:    404,
			Message: "Not found",
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(notFoundResponse, nil)
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to update volume replication", err.Error())
	})
	t.Run("WhenConflict", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("schedule"),
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
		}
		conflictResponse := &googleproxyclient.V1betaInternalUpdateVolumeReplicationConflict{
			Code:    409,
			Message: "conflict",
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(conflictResponse, nil)
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to update volume replication", err.Error())
	})
	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("schedule"),
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
		}
		internalErrorResponse := &googleproxyclient.V1betaInternalUpdateVolumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(internalErrorResponse, nil)
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to update volume replication", err.Error())
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
				{
					JobId: googleproxyclient.NewOptString("job-uuid"),
				},
			},
		}
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("schedule"),
				Labels: map[string]string{
					"key": "value",
				},
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
			Labels:              googleproxyclient.NewOptVolumeReplicationUpdateInternalV1betaLabels(inputResult.Event.Labels),
		}
		if inputResult.Event.ClusterLocation != nil {
			req.ClusterLocation = googleproxyclient.NewOptString(*inputResult.Event.ClusterLocation)
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(res, nil)
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, *result.JobId, "job-uuid")
	})
	t.Run("WhenClusterLocationProvided", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		clusterLocation := "us-west1"
		inputResult := &replication.UpdateReplicationResult{
			DstBasePath:      nillable.GetStringPtr("dstPath"),
			DstProjectNumber: nillable.GetStringPtr("projDst"),
			DstJwtToken:      nillable.GetStringPtr("dstToken"),
			Event: &replication.UpdateReplicationEvent{
				Description:         nillable.GetStringPtr("description"),
				ReplicationSchedule: nillable.GetStringPtr("HOURLY"),
				ClusterLocation:     &clusterLocation,
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("correlationId"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "location-id",
							DestinationReplicationUUID: "replication-uuid",
						},
					},
				},
			},
		}
		updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			XCorrelationID:      googleproxyclient.NewOptString(*inputResult.Event.XCorrelationID),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		res := &googleproxyclient.VolumeReplicationInternalV1beta{
			Jobs: []googleproxyclient.JobV1beta{
				{
					JobId: googleproxyclient.OptString{Value: "job-uuid", Set: true},
				},
			},
		}
		req := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{
			Description:         googleproxyclient.NewOptNilString(nillable.GetString(inputResult.Event.Description, "")),
			ReplicationSchedule: googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*inputResult.Event.ReplicationSchedule)),
			ClusterLocation:     googleproxyclient.NewOptString(*inputResult.Event.ClusterLocation),
		}
		mockClient.EXPECT().V1betaInternalUpdateVolumeReplication(ctx, req, *updateReplicationParams).Return(res, nil)
		activity := VolumeReplicationUpdateActivity{SE: mockStorage}
		result, err := activity.UpdateReplicationOnDestination(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, *result.JobId, "job-uuid")
	})
}
