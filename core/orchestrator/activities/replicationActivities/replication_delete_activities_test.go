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
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
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
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteReplicationOnDestination(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.JobId, "job-uuid")
	})
}

func TestReleaseReplicationOnSource(t *testing.T) {
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
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ReleaseReplicationOnSource(context.Background(), inputResult)
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
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ReleaseReplicationOnSource(context.Background(), inputResult)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.JobId, "job-uuid")
	})
}

func TestDeleteSnapmirrorSnapshotsOnDestination(t *testing.T) {
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
			ProjectNumber: *inputResult.DstProjectNumber,
			LocationId:    inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:      inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnDestination(context.Background(), inputResult)
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
		res := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString("projects/123/locations/us-central1/volumes/vol-uuid/operations/job-uuid"),
		}
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
		deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
			ProjectNumber: *inputResult.DstProjectNumber,
			LocationId:    inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:      inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
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
}

func TestDeleteSnapmirrorSnapshotsOnSource(t *testing.T) {
	srcProj := "projSrc"
	srcPath := "srcPath"
	srcToken := "srcToken"
	t.Run("WhenError", func(tt *testing.T) {
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
			ProjectNumber: *inputResult.SrcProjectNumber,
			LocationId:    inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:      inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteReplicationParams).Return(nil, errors.New("some-error"))
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.DeleteSnapmirrorSnapshotsOnSource(context.Background(), inputResult)
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
		res := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString("projects/123/locations/us-central1/volumes/vol-uuid/operations/job-uuid"),
		}
		inputResult := &replication.DeleteReplicationResult{
			SrcBasePath:      &srcPath,
			SrcProjectNumber: &srcProj,
			SrcJwtToken:      &srcToken,
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
			ProjectNumber: *inputResult.SrcProjectNumber,
			LocationId:    inputResult.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:      inputResult.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
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
}

func TestDeHydrateDestinationVolumeReplication(t *testing.T) {
	dstProj := "projDst"
	dstPath := "dstPath"
	dstToken := "dstToken"
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
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
		_, err := activity.DeHydrateDestinationVolumeReplication(ctx, inputResult)

		assert.Error(t, err)
		var customErr *errors2.CustomError
		assert.True(t, errors2.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(t, customErr.OriginalErr, "hydration error")
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

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
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
		updatedResult, err := activity.GetReplicationOnDestinationForDelete(context.Background(), result)
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

		activity := DeleteVolumeReplicationActivity{SE: mockStorage}
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
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := googleproxyclient.V1betaDeleteVolumeParams{
			ProjectNumber: dstProj,
			LocationId:    locationID,
			VolumeId:      volUUID,
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
			ProjectNumber: dstProj,
			LocationId:    locationID,
			VolumeId:      volUUID,
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
}
