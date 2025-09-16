package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	logger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func stringPtr(s string) *string {
	return &s
}

func TestGetSrcBasePathReverse(t *testing.T) {
	t.Run("ValidSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := ReverseVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://src-base-path.example.com", nil
		}

		updatedResult, err := activity.GetSrcBasePathReverse(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcBasePath)
		assert.Equal(tt, "https://src-base-path.example.com", *updatedResult.SrcBasePath)
	})
	t.Run("ErrorSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "location-id",
						},
					},
				},
			},
		}
		activity := ReverseVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetSrcBasePathReverse(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetDstBasePathReverse(t *testing.T) {
	t.Run("ValidDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := ReverseVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		updatedResult, err := activity.GetDstBasePathReverse(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})
	t.Run("ErrorDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "location-id",
						},
					},
				},
			},
		}
		activity := ReverseVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetDstBasePathReverse(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedSrcTokenReverse(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
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

		updatedResult, err := activity.GetSignedSrcTokenReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.SrcJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
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

		updatedResult, err := activity.GetSignedSrcTokenReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedDstTokenReverse(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenSuccessSameProject", func(tt *testing.T) {
		prj := "prj"
		token := "signed-token"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenReverse(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenReverse(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestDescribeRemoteJobOnsrc(t *testing.T) {
	t.Run("DescribeJobSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ReverseReplicationEvent{
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

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

		err := activity.DescribeRemoteJobOnSrc(ctx, result)

		assert.NoError(tt, err)
	})
	t.Run("DescribeJobNotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ReverseReplicationEvent{
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

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := activity.DescribeRemoteJobOnSrc(ctx, result)

		assert.Error(tt, err)
	})
	t.Run("DescribeJobError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			SrcProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ReverseReplicationEvent{
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

		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
		err := activity.DescribeRemoteJobOnSrc(ctx, result)

		assert.Error(tt, err)
	})
}

func TestDescribeRemoteJobReverseOnDst(t *testing.T) {
	t.Run("DescribeJobSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ReverseReplicationEvent{
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

		err := activity.DescribeRemoteJobOnDst(ctx, result)

		assert.NoError(tt, err)
	})
	t.Run("DescribeJobNotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ReverseReplicationEvent{
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
		err := activity.DescribeRemoteJobOnDst(ctx, result)

		assert.Error(tt, err)
	})
	t.Run("DescribeJobError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ReverseReplicationEvent{
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
		err := activity.DescribeRemoteJobOnDst(ctx, result)

		assert.Error(tt, err)
	})
}

func TestUpdateVolumeReplicationAttributes(t *testing.T) {
	t.Run("WhenUpdateVolumeReplicationAttributesError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcBasePath := "https://src-base-path.example.com"
		srcJwtToken := "src-jwt-token"
		srcProjectNumber := "123456"
		jobId := "operation-123"

		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:             "src-location",
							SourceHostName:             "src-host",
							SourceSvmName:              "src-svm",
							SourceVolumeName:           "src-volume",
							SourceVolumeUUID:           "src-volume-uuid",
							SourcePoolUUID:             "src-pool-uuid",
							SourceReplicationUUID:      "src-replication-uuid",
							DestinationLocation:        "dest-location",
							DestinationHostName:        "dest-host",
							DestinationSvmName:         "dest-svm",
							DestinationVolumeName:      "dest-volume",
							DestinationVolumeUUID:      "dest-volume-uuid",
							DestinationPoolUUID:        "dest-pool-uuid",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			SrcProjectNumber: &srcProjectNumber,
			JobId:            &jobId,
		}

		updateParams := googleproxyclient.V1betaInternalUpdateVolumeReplicationAttributesParams{
			ProjectNumber:       srcProjectNumber,
			LocationId:          "src-location",
			VolumeReplicationId: "src-replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalUpdateVolumeReplicationAttributes(ctx,
			mock.AnythingOfType("*googleproxyclient.VolumeReplicationInternalV1beta"),
			updateParams).Return(nil, errors.New("update error"))

		_, err := activity.UpdateVolumeReplicationAttributesSrc(ctx, result)

		assert.Error(tt, err)
		assert.Equal(tt, "Failed to update volume replication details", err.Error())
	})

	t.Run("WhenUpdateVolumeReplicationAttributesSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(tt)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcBasePath := "https://src-base-path.example.com"
		srcJwtToken := "src-jwt-token"
		srcProjectNumber := "123456"
		jobId := "operation-123"

		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:             "src-location",
							SourceHostName:             "src-host",
							SourceSvmName:              "src-svm",
							SourceVolumeName:           "src-volume",
							SourceVolumeUUID:           "src-volume-uuid",
							SourcePoolUUID:             "src-pool-uuid",
							SourceReplicationUUID:      "src-replication-uuid",
							DestinationLocation:        "dest-location",
							DestinationHostName:        "dest-host",
							DestinationSvmName:         "dest-svm",
							DestinationVolumeName:      "dest-volume",
							DestinationVolumeUUID:      "dest-volume-uuid",
							DestinationPoolUUID:        "dest-pool-uuid",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			SrcProjectNumber: &srcProjectNumber,
			JobId:            &jobId,
		}

		updateParams := googleproxyclient.V1betaInternalUpdateVolumeReplicationAttributesParams{
			ProjectNumber:       srcProjectNumber,
			LocationId:          "src-location",
			VolumeReplicationId: "src-replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		operationResponse := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.OptString{
				Value: "projects/123456/locations/src-location/volumes/src-volume-uuid/operations/new-operation-456",
				Set:   true,
			},
			Done: googleproxyclient.OptBool{
				Value: false,
				Set:   true,
			},
		}

		mockClient.EXPECT().V1betaInternalUpdateVolumeReplicationAttributes(ctx,
			mock.AnythingOfType("*googleproxyclient.VolumeReplicationInternalV1beta"),
			updateParams).Return(operationResponse, nil)

		res, err := activity.UpdateVolumeReplicationAttributesSrc(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.NotNil(tt, res.JobId)
		assert.Equal(tt, "new-operation-456", *res.JobId)
	})
}

func TestReverseAndResumeReplication(t *testing.T) {
	t.Run("WhenGoogleProxyClientReturnsSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		ctx := context.Background()

		result := &replication.ReverseReplicationResult{
			SrcBasePath:      stringPtr("https://src-example.com"),
			SrcJwtToken:      stringPtr("src-jwt-token"),
			SrcProjectNumber: stringPtr("67890"),
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-west1",
							SourceReplicationUUID: "src-repl-uuid",
						},
					},
				},
			},
		}

		params := &common.ReverseAndResumeReplicationParams{
			CorrelationId: "correlation-123",
		}

		// Mock the google proxy client to return successful response
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		expectedJobResponse := &googleproxyclient.VolumeReplicationInternalV1beta{
			Jobs: []googleproxyclient.JobV1beta{
				{
					JobId: googleproxyclient.OptString{
						Value: "reverse-job-uuid-12345",
						Set:   true,
					},
				},
			},
		}

		expectedParams := googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "67890",
			LocationId:          "us-west1",
			VolumeReplicationId: "src-repl-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("correlation-123"),
		}

		mockInvoker.On("V1betaInternalReverseVolumeReplication", ctx, expectedParams).Return(expectedJobResponse, nil)

		res, err := activity.ReverseAndResumeReplication(ctx, result, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, result, res)
		assert.NotNil(tt, res.JobId)
		assert.Equal(tt, "reverse-job-uuid-12345", *res.JobId)
		assert.Equal(tt, expectedJobResponse, res.DstReplication)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenGoogleProxyClientReturnsBadRequest", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		ctx := context.Background()

		result := &replication.ReverseReplicationResult{
			SrcBasePath:      stringPtr("https://src-example.com"),
			SrcJwtToken:      stringPtr("src-jwt-token"),
			SrcProjectNumber: stringPtr("67890"),
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-west1",
							SourceReplicationUUID: "src-repl-uuid",
						},
					},
				},
			},
		}

		params := &common.ReverseAndResumeReplicationParams{
			CorrelationId: "correlation-123",
		}

		// Mock the google proxy client to return bad request error
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		badRequestResponse := &googleproxyclient.V1betaInternalReverseVolumeReplicationBadRequest{
			Code:    400,
			Message: "Invalid request parameters",
		}

		expectedParams := googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "67890",
			LocationId:          "us-west1",
			VolumeReplicationId: "src-repl-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("correlation-123"),
		}

		mockInvoker.On("V1betaInternalReverseVolumeReplication", ctx, expectedParams).Return(badRequestResponse, nil)

		res, err := activity.ReverseAndResumeReplication(ctx, result, params)

		assert.Error(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to reverse replication on ontap", err.Error())
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenGoogleProxyClientReturnsUnauthorized", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		ctx := context.Background()

		result := &replication.ReverseReplicationResult{
			SrcBasePath:      stringPtr("https://src-example.com"),
			SrcJwtToken:      stringPtr("src-jwt-token"),
			SrcProjectNumber: stringPtr("67890"),
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-west1",
							SourceReplicationUUID: "src-repl-uuid",
						},
					},
				},
			},
		}

		params := &common.ReverseAndResumeReplicationParams{
			CorrelationId: "correlation-123",
		}

		// Mock the google proxy client to return unauthorized error
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		unauthorizedResponse := &googleproxyclient.V1betaInternalReverseVolumeReplicationUnauthorized{
			Code:    401,
			Message: "Authentication failed",
		}

		expectedParams := googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "67890",
			LocationId:          "us-west1",
			VolumeReplicationId: "src-repl-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("correlation-123"),
		}

		mockInvoker.On("V1betaInternalReverseVolumeReplication", ctx, expectedParams).Return(unauthorizedResponse, nil)

		res, err := activity.ReverseAndResumeReplication(ctx, result, params)

		assert.Error(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to reverse replication on ontap", err.Error())
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenGoogleProxyClientReturnsForbidden", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		ctx := context.Background()

		result := &replication.ReverseReplicationResult{
			SrcBasePath:      stringPtr("https://src-example.com"),
			SrcJwtToken:      stringPtr("src-jwt-token"),
			SrcProjectNumber: stringPtr("67890"),
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-west1",
							SourceReplicationUUID: "src-repl-uuid",
						},
					},
				},
			},
		}

		params := &common.ReverseAndResumeReplicationParams{
			CorrelationId: "correlation-123",
		}

		// Mock the google proxy client to return forbidden error
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		forbiddenResponse := &googleproxyclient.V1betaInternalReverseVolumeReplicationForbidden{
			Code:    403,
			Message: "Access denied",
		}

		expectedParams := googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "67890",
			LocationId:          "us-west1",
			VolumeReplicationId: "src-repl-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("correlation-123"),
		}

		mockInvoker.On("V1betaInternalReverseVolumeReplication", ctx, expectedParams).Return(forbiddenResponse, nil)

		res, err := activity.ReverseAndResumeReplication(ctx, result, params)

		assert.Error(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to reverse replication on ontap", err.Error())
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenGoogleProxyClientReturnsNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		ctx := context.Background()

		result := &replication.ReverseReplicationResult{
			SrcBasePath:      stringPtr("https://src-example.com"),
			SrcJwtToken:      stringPtr("src-jwt-token"),
			SrcProjectNumber: stringPtr("67890"),
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-west1",
							SourceReplicationUUID: "src-repl-uuid",
						},
					},
				},
			},
		}

		params := &common.ReverseAndResumeReplicationParams{
			CorrelationId: "correlation-123",
		}

		// Mock the google proxy client to return not found error
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		notFoundResponse := &googleproxyclient.V1betaInternalReverseVolumeReplicationNotFound{
			Code:    404,
			Message: "Volume replication not found",
		}

		expectedParams := googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "67890",
			LocationId:          "us-west1",
			VolumeReplicationId: "src-repl-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("correlation-123"),
		}

		mockInvoker.On("V1betaInternalReverseVolumeReplication", ctx, expectedParams).Return(notFoundResponse, nil)

		res, err := activity.ReverseAndResumeReplication(ctx, result, params)

		assert.Error(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to reverse replication on ontap", err.Error())
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenGoogleProxyClientReturnsConflict", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		ctx := context.Background()

		result := &replication.ReverseReplicationResult{
			SrcBasePath:      stringPtr("https://src-example.com"),
			SrcJwtToken:      stringPtr("src-jwt-token"),
			SrcProjectNumber: stringPtr("67890"),
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-west1",
							SourceReplicationUUID: "src-repl-uuid",
						},
					},
				},
			},
		}

		params := &common.ReverseAndResumeReplicationParams{
			CorrelationId: "correlation-123",
		}

		// Mock the google proxy client to return conflict error
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		conflictResponse := &googleproxyclient.V1betaInternalReverseVolumeReplicationConflict{
			Code:    409,
			Message: "Volume replication conflict",
		}

		expectedParams := googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "67890",
			LocationId:          "us-west1",
			VolumeReplicationId: "src-repl-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("correlation-123"),
		}

		mockInvoker.On("V1betaInternalReverseVolumeReplication", ctx, expectedParams).Return(conflictResponse, nil)

		res, err := activity.ReverseAndResumeReplication(ctx, result, params)

		assert.Error(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to reverse replication on ontap", err.Error())
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenGoogleProxyClientReturnsInternalServerError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		ctx := context.Background()

		result := &replication.ReverseReplicationResult{
			SrcBasePath:      stringPtr("https://src-example.com"),
			SrcJwtToken:      stringPtr("src-jwt-token"),
			SrcProjectNumber: stringPtr("67890"),
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-west1",
							SourceReplicationUUID: "src-repl-uuid",
						},
					},
				},
			},
		}

		params := &common.ReverseAndResumeReplicationParams{
			CorrelationId: "correlation-123",
		}

		// Mock the google proxy client to return internal server error
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		serverErrorResponse := &googleproxyclient.V1betaInternalReverseVolumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}

		expectedParams := googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "67890",
			LocationId:          "us-west1",
			VolumeReplicationId: "src-repl-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("correlation-123"),
		}

		mockInvoker.On("V1betaInternalReverseVolumeReplication", ctx, expectedParams).Return(serverErrorResponse, nil)

		res, err := activity.ReverseAndResumeReplication(ctx, result, params)

		assert.Error(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to reverse replication on ontap", err.Error())
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenGoogleProxyClientConnectionError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		ctx := context.Background()

		result := &replication.ReverseReplicationResult{
			SrcBasePath:      stringPtr("https://src-example.com"),
			SrcJwtToken:      stringPtr("src-jwt-token"),
			SrcProjectNumber: stringPtr("67890"),
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-west1",
							SourceReplicationUUID: "src-repl-uuid",
						},
					},
				},
			},
		}

		params := &common.ReverseAndResumeReplicationParams{
			CorrelationId: "correlation-123",
		}

		// Mock the google proxy client to return connection error
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		expectedParams := googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "67890",
			LocationId:          "us-west1",
			VolumeReplicationId: "src-repl-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("correlation-123"),
		}

		mockInvoker.On("V1betaInternalReverseVolumeReplication", ctx, expectedParams).Return(nil, errors.New("connection error"))

		res, err := activity.ReverseAndResumeReplication(ctx, result, params)

		assert.Error(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to reverse replication on ontap", err.Error())
		mockInvoker.AssertExpectations(tt)
	})
}

func TestVerifyNewDstVolume(t *testing.T) {
	dstBasePath := "https://dst-base-path.example.com"
	srcBasePath := "https://src-base-path.example.com"
	dstPrj := "dstPrj"
	srcPrj := "srcPrj"
	dstToken := "signed-token"
	srcToken := "signed-token"
	t.Run("WhenError", func(tt *testing.T) {
		defer func() {
			verifyDstVolume = replication.VerifyDstVolume
		}()
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationVolumeUUID: "invalid-uuid",
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			SrcBasePath: &srcBasePath,
			DstJwtToken: &dstToken,
			SrcJwtToken: &srcToken,
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrDescribingVolume, errors.New("volume not found"))
		verifyDstVolume = func(ctx context.Context, attributes *datamodel.ReplicationDetails, srcBasePath string, destBasePath string, srcToken string, dstToken string, srcProjectNumber, dstProjectNumber string, correlationId *string, isReverse bool) (googleproxyclient.VolumeV1beta, googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, expectedError
		}
		updatedResult, err := activity.VerifyNewDstVolume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenBadRequestError", func(tt *testing.T) {
		defer func() {
			verifyDstVolume = replication.VerifyDstVolume
		}()
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationVolumeUUID: "invalid-uuid",
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			SrcBasePath: &srcBasePath,
			DstJwtToken: &dstToken,
			SrcJwtToken: &srcToken,
		}
		verifyError := vsaErrors.NewVCPError(vsaErrors.ErrVolumeNotFound, errors.New("volume not found"))
		verifyDstVolume = func(ctx context.Context, attributes *datamodel.ReplicationDetails, srcBasePath string, destBasePath string, srcToken string, dstToken string, srcProjectNumber, dstProjectNumber string, correlationId *string, isReverse bool) (googleproxyclient.VolumeV1beta, googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, verifyError
		}
		updatedResult, err := activity.VerifyNewDstVolume(ctx, result)
		expectedError := errors.NewNonRetryableErr(verifyError.Error())
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		defer func() {
			verifyDstVolume = replication.VerifyDstVolume
		}()
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationVolumeUUID: "invalid-uuid",
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			SrcBasePath:      &srcBasePath,
			DstProjectNumber: &dstPrj,
			SrcProjectNumber: &srcPrj,
			DstJwtToken:      &dstToken,
			SrcJwtToken:      &srcToken,
		}

		verifyDstVolume = func(ctx context.Context, attributes *datamodel.ReplicationDetails, srcBasePath string, destBasePath string, srcToken string, dstToken string, srcProjectNumber, dstProjectNumber string, correlationId *string, isReverse bool) (googleproxyclient.VolumeV1beta, googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, googleproxyclient.VolumeV1beta{}, nil
		}

		updatedResult, err := activity.VerifyNewDstVolume(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, result, updatedResult)
	})
}

func TestCleanupOldReplication(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "dest-location",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
		}

		// Mock the google proxy client and its method
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		resp := &googleproxyclient.VolumeReplicationInternalV1beta{
			Jobs: []googleproxyclient.JobV1beta{
				googleproxyclient.JobV1beta{
					JobId: googleproxyclient.OptString{
						Value: "delete-job-uuid-12345",
						Set:   true,
					},
				},
			},
		}
		// Mock the cleanup method to return success
		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(resp, nil)

		updatedResult, err := activity.CleanupOldReplication(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "dest-location",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
		}

		// Mock the google proxy client and its method
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		// Mock the cleanup method to return error
		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(nil, errors.New("cleanup error"))

		updatedResult, err := activity.CleanupOldReplication(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenBadRequestResponse", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "dest-location",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		badRequestResponse := &googleproxyclient.V1betaInternalDeleteVolumeReplicationBadRequest{
			Message: "Invalid cleanup request parameters",
		}
		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(badRequestResponse, nil)

		updatedResult, err := activity.CleanupOldReplication(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to cleanup volume replication after reverse")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenUnauthorizedResponse", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "dest-location",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		unauthorizedResponse := &googleproxyclient.V1betaInternalDeleteVolumeReplicationUnauthorized{
			Message: "Authentication failed for cleanup",
		}
		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(unauthorizedResponse, nil)

		updatedResult, err := activity.CleanupOldReplication(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to cleanup volume replication after reverse")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenForbiddenResponse", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "dest-location",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		forbiddenResponse := &googleproxyclient.V1betaInternalDeleteVolumeReplicationForbidden{
			Message: "Access denied for cleanup operation",
		}
		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(forbiddenResponse, nil)

		updatedResult, err := activity.CleanupOldReplication(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to cleanup volume replication after reverse")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenNotFoundResponse", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "dest-location",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		notFoundResponse := &googleproxyclient.V1betaInternalDeleteVolumeReplicationNotFound{
			Message: "Volume replication not found for cleanup",
		}
		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(notFoundResponse, nil)

		updatedResult, err := activity.CleanupOldReplication(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to cleanup volume replication after reverse")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenInternalServerErrorResponse", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		dstBasePath := "https://dst-base-path.example.com"
		dstJwtToken := "dst-jwt-token"
		dstProjectNumber := "dst-project-number"
		result := &replication.ReverseReplicationResult{
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-xcorrelation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "dest-location",
							DestinationReplicationUUID: "dest-replication-uuid",
						},
					},
				},
			},
		}

		mockInvoker := googleproxyclient.NewMockInvoker(tt)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		serverErrorResponse := &googleproxyclient.V1betaInternalDeleteVolumeReplicationInternalServerError{
			Message: "Internal server error during cleanup",
		}
		mockInvoker.On("V1betaInternalDeleteVolumeReplication", ctx, mock.Anything).Return(serverErrorResponse, nil)

		updatedResult, err := activity.CleanupOldReplication(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to cleanup volume replication after reverse")
		mockInvoker.AssertExpectations(tt)
	})
}

func TestMountReplicationAfterReverse(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}
		res := &googleproxyclient.InternalJobV1beta{
			JobUuid: googleproxyclient.NewOptString("job-uuid"),
		}
		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(res, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "job-uuid", *result.JobId)
	})

	t.Run("WhenGoogleProxyClientReturnsBadRequest", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		badRequestResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationBadRequest{
			Code:    400,
			Message: "Invalid mount request parameters",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(badRequestResponse, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})

	t.Run("WhenGoogleProxyClientReturnsUnauthorized", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		unauthorizedResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationUnauthorized{
			Code:    401,
			Message: "Authentication failed for mount",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(unauthorizedResponse, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})

	t.Run("WhenGoogleProxyClientReturnsForbidden", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		forbiddenResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationForbidden{
			Code:    403,
			Message: "Access denied for mount operation",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(forbiddenResponse, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})

	t.Run("WhenGoogleProxyClientReturnsNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		notFoundResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationNotFound{
			Code:    404,
			Message: "Volume replication not found for mount",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(notFoundResponse, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})

	t.Run("WhenGoogleProxyClientReturnsConflict", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		conflictResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationConflict{
			Code:    409,
			Message: "Mount operation conflict",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(conflictResponse, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})

	t.Run("WhenGoogleProxyClientReturnsMethodNotAllowed", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		methodNotAllowedResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationMethodNotAllowed{
			Code:    405,
			Message: "Mount method not allowed",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(methodNotAllowedResponse, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})

	t.Run("WhenGoogleProxyClientReturnsUnprocessableEntity", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		unprocessableEntityResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationUnprocessableEntity{
			Code:    422,
			Message: "Unprocessable mount entity",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(unprocessableEntityResponse, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})

	t.Run("WhenGoogleProxyClientReturnsInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		serverErrorResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error during mount",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(serverErrorResponse, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})

	t.Run("WhenGoogleProxyClientConnectionError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockClient := googleproxyclient.NewMockInvoker(t)

		srcProj := "projSrc"
		srcPath := "srcPath"
		srcToken := "srcToken"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid"),
			},
			SrcProjectNumber: &srcProj,
			SrcBasePath:      &srcPath,
			SrcJwtToken:      &srcToken,
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger logger.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, *params).Return(nil, errors.New("connection error"))

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.MountReplicationAfterReverse(ctx, replicationResult)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
	})
}

func TestResizeNewDstVolumeIfNeeded(t *testing.T) {
	t.Run("WhenSrcVolumeQuotaEqualToDestination", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		result := &replication.ReverseReplicationResult{
			NewSrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
		}

		_, err := activity.ResizeNewDstVolumeIfNeeded(ctx, result)
		assert.NoError(tt, err)
	})
	t.Run("WhenQuotasAreDifferentAndUpdateVolumeSucceeds", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		srcBasePath := "src-base-path"
		srcJwtToken := "src-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ReverseReplicationResult{
			NewSrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dst-volume-uuid",
						},
					},
				},
			},
		}

		expectedResponse := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString("operations/operation-name/job-uuid"),
			Done: googleproxyclient.NewOptBool(false),
		}

		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		updatedResult, err := activity.ResizeNewDstVolumeIfNeeded(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, "job-uuid", *updatedResult.JobId)
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenUpdateVolumeReturnsBadRequest", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		srcBasePath := "src-base-path"
		srcJwtToken := "src-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ReverseReplicationResult{
			NewSrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dst-volume-uuid",
						},
					},
				},
			},
		}

		badRequestResponse := &googleproxyclient.V1betaInternalUpdateVolumeBadRequest{
			Code:    400,
			Message: "Invalid request parameters",
		}

		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(badRequestResponse, nil)

		updatedResult, err := activity.ResizeNewDstVolumeIfNeeded(ctx, result)
		assert.Error(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to update volume internal")
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenUpdateVolumeReturnsUnauthorized", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		srcBasePath := "src-base-path"
		srcJwtToken := "src-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ReverseReplicationResult{
			NewSrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dst-volume-uuid",
						},
					},
				},
			},
		}

		unauthorizedResponse := &googleproxyclient.V1betaInternalUpdateVolumeUnauthorized{
			Code:    401,
			Message: "Unauthorized access",
		}

		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(unauthorizedResponse, nil)

		updatedResult, err := activity.ResizeNewDstVolumeIfNeeded(ctx, result)
		assert.Error(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to update volume internal")
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenUpdateVolumeReturnsForbidden", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		srcBasePath := "src-base-path"
		srcJwtToken := "src-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ReverseReplicationResult{
			NewSrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dst-volume-uuid",
						},
					},
				},
			},
		}

		forbiddenResponse := &googleproxyclient.V1betaInternalUpdateVolumeForbidden{
			Code:    403,
			Message: "Access denied",
		}

		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(forbiddenResponse, nil)

		updatedResult, err := activity.ResizeNewDstVolumeIfNeeded(ctx, result)
		assert.Error(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to update volume internal")
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenUpdateVolumeReturnsNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		srcBasePath := "src-base-path"
		srcJwtToken := "src-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ReverseReplicationResult{
			NewSrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dst-volume-uuid",
						},
					},
				},
			},
		}

		notFoundResponse := &googleproxyclient.V1betaInternalUpdateVolumeNotFound{
			Code:    404,
			Message: "Volume not found",
		}

		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(notFoundResponse, nil)

		updatedResult, err := activity.ResizeNewDstVolumeIfNeeded(ctx, result)
		assert.Error(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to update volume internal")
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenUpdateVolumeReturnsInternalServerError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		srcBasePath := "src-base-path"
		srcJwtToken := "src-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ReverseReplicationResult{
			NewSrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dst-volume-uuid",
						},
					},
				},
			},
		}

		internalErrorResponse := &googleproxyclient.V1betaInternalUpdateVolumeInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}

		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(internalErrorResponse, nil)

		updatedResult, err := activity.ResizeNewDstVolumeIfNeeded(ctx, result)
		assert.Error(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to update volume internal")
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenUpdateVolumeReturnsError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		srcBasePath := "src-base-path"
		srcJwtToken := "src-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ReverseReplicationResult{
			NewSrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SrcBasePath:      &srcBasePath,
			SrcJwtToken:      &srcJwtToken,
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dst-volume-uuid",
						},
					},
				},
			},
		}

		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("network error"))

		updatedResult, err := activity.ResizeNewDstVolumeIfNeeded(ctx, result)
		assert.Error(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to update volume internal")
		mockClient.AssertExpectations(tt)
	})
}
