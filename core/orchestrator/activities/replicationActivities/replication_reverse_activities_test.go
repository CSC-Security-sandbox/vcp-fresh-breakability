package replicationActivities

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			XCorrelationID: googleproxyclient.NewOptString("test-xcorrelation-id"),
		}

		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
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

func TestReleaseReplicationOnOldSrc(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)

		nodeProvider := &models.Node{
			Name: "node1",
		}
		result := &replication.ReverseReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
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
				},
			},
			NodeProvider: nodeProvider,
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		expectedReleaseParams := &vsa.ReleaseVolumeReplicationParams{
			VolumeReplication: &vsa.VolumeReplication{
				EndpointType:          "src",
				SourceHostName:        "src-host",
				SourceSVMName:         "src-svm",
				SourceVolumeName:      "src-vol",
				DestinationHostName:   "dst-host",
				DestinationSVMName:    "dst-svm",
				DestinationVolumeName: "dst-vol",
				ReplicationSchedule:   "hourly",
				Volume: &vsa.Volume{
					ExternalUUID: "volume-external-uuid",
				},
			},
			ReverseResync: false,
		}

		mockProvider.On("ReleaseVolumeReplication", expectedReleaseParams).Return(&vsa.VolumeReplication{}, nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		updatedResult, err := activity.ReleaseReplicationOnOldSrc(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenProviderError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)

		nodeProvider := &models.Node{
			Name: "node1",
		}
		result := &replication.ReverseReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
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
				},
			},
			NodeProvider: nodeProvider,
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ReleaseVolumeReplication", mock.AnythingOfType("*vsa.ReleaseVolumeReplicationParams")).Return(nil, errors.New("provider error"))

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		updatedResult, err := activity.ReleaseReplicationOnOldSrc(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Error releasing volume replication")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenNodeProviderIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)

		result := &replication.ReverseReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					VolumeAttributes: &datamodel.VolumeAttributes{
						ExternalUUID: "volume-external-uuid",
					},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType: "src",
				},
			},
			NodeProvider: nil,
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("node provider is nil")
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		updatedResult, err := activity.ReleaseReplicationOnOldSrc(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)

		nodeProvider := &models.Node{
			Name: "node1",
		}
		result := &replication.ReverseReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					VolumeAttributes: &datamodel.VolumeAttributes{
						ExternalUUID: "volume-external-uuid",
					},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType: "src",
				},
			},
			NodeProvider: nodeProvider,
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		updatedResult, err := activity.ReleaseReplicationOnOldSrc(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "failed to get provider")
	})
}

func TestSetVolumeReplicationStatusToOnpremReplication(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)

		replicationUUID := "test-replication-uuid"
		hybridReplicationType := string(models.HybridReplicationParametersReplicationTypeREVERSE)
		replicationModel := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				HybridReplicationType:         &hybridReplicationType,
				Status:                        models.HybridReplicationStatusExternalManaged,
				StatusDetails:                 "test-status-details",
				HybridReplicationUserCommands: []string{"command1", "command2"},
			},
		}

		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: replicationModel,
				},
			},
		}

		expectedReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				HybridReplicationType:         &hybridReplicationType,
				Status:                        models.HybridReplicationStatusExternalManaged,
				StatusDetails:                 "test-status-details",
				HybridReplicationUserCommands: []string{"command1", "command2"},
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(expectedReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
			return r.UUID == replicationUUID &&
				r.HybridReplicationAttributes != nil &&
				*r.HybridReplicationAttributes.HybridReplicationType == string(models.HybridReplicationParametersReplicationTypeONPREM) &&
				r.HybridReplicationAttributes.Status == models.HybridReplicationStatusPeered &&
				r.HybridReplicationAttributes.StatusDetails == "" &&
				r.HybridReplicationAttributes.HybridReplicationUserCommands == nil
		})).Return(nil)

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		updatedResult, err := activity.SetVolumeReplicationStatusToOnpremReplication(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)

		replicationUUID := "test-replication-uuid"
		replicationModel := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
		}

		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: replicationModel,
				},
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(nil, errors.New("database error"))

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		updatedResult, err := activity.SetVolumeReplicationStatusToOnpremReplication(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaErrors.CustomError
		assert.True(tt, vsaErrors.As(err, &customErr))
		assert.Contains(tt, customErr.OriginalErr.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)

		replicationUUID := "test-replication-uuid"
		hybridReplicationType := string(models.HybridReplicationParametersReplicationTypeREVERSE)
		replicationModel := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				HybridReplicationType: &hybridReplicationType,
				Status:                models.HybridReplicationStatusExternalManaged,
			},
		}

		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: replicationModel,
				},
			},
		}

		expectedReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				HybridReplicationType: &hybridReplicationType,
				Status:                models.HybridReplicationStatusExternalManaged,
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(expectedReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.Anything).Return(errors.New("update error"))

		activity := ReverseVolumeReplicationActivity{SE: mockStorage}
		updatedResult, err := activity.SetVolumeReplicationStatusToOnpremReplication(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		var customErr *vsaErrors.CustomError
		assert.True(tt, vsaErrors.As(err, &customErr))
		assert.Contains(tt, customErr.OriginalErr.Error(), "update error")
		mockStorage.AssertExpectations(tt)
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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
		xCorrelationID := "test-correlation-id"
		replicationResult := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:        "us-central1",
							SourceReplicationUUID: "replication-uuid",
						},
					},
					XCorrelationID: &xCorrelationID,
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		params := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       srcProj,
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-uuid",
			XCorrelationID:      googleproxyclient.NewOptString("test-correlation-id"),
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

func TestConvertToReversedAttributesForHybridRep(t *testing.T) {
	t.Run("WhenSuccess_WithAllFields", func(tt *testing.T) {
		originalAttrs := &datamodel.ReplicationDetails{
			SourceHostName:        "source-host",
			SourceSvmName:         "source-svm",
			SourceVolumeName:      "source-volume",
			SourceVolumeUUID:      "source-volume-uuid",
			SourcePoolUUID:        "source-pool-uuid",
			DestinationHostName:   "dest-host",
			DestinationSvmName:    "dest-svm",
			DestinationVolumeName: "dest-volume",
			DestinationVolumeUUID: "dest-volume-uuid",
			DestinationPoolUUID:   "dest-pool-uuid",
		}

		var result *gcpserver.VolumeReplicationInternalV1beta
		result = ConvertToReversedAttributesForHybridRep(originalAttrs)

		assert.NotNil(tt, result)
		// Original destination becomes new source
		assert.Equal(tt, "dest-host", result.SourceHostName)
		assert.Equal(tt, "dest-svm", result.SourceServerName)
		assert.Equal(tt, "dest-volume", result.SourceVolumeName)
		assert.True(tt, result.SourceVolumeUuid.Set)
		assert.Equal(tt, "dest-volume-uuid", result.SourceVolumeUuid.Value)
		assert.True(tt, result.SourcePoolUuid.Set)
		assert.Equal(tt, "dest-pool-uuid", result.SourcePoolUuid.Value)

		// Original source becomes new destination
		assert.Equal(tt, "source-host", result.DestinationHostName)
		assert.Equal(tt, "source-svm", result.DestinationServerName)
		assert.Equal(tt, "source-volume", result.DestinationVolumeName)
		assert.True(tt, result.DestinationVolumeUuid.Set)
		assert.Equal(tt, "source-volume-uuid", result.DestinationVolumeUuid.Value)
		assert.True(tt, result.DestinationPoolUuid.Set)
		assert.Equal(tt, "source-pool-uuid", result.DestinationPoolUuid.Value)
		assert.Equal(tt, result.EndpointType, gcpserver.VolumeReplicationInternalV1betaEndpointType(googleproxyclient.VolumeReplicationInternalV1betaEndpointTypeDst))
	})

	t.Run("WhenSuccess_WithEmptyUUIDs", func(tt *testing.T) {
		originalAttrs := &datamodel.ReplicationDetails{
			SourceHostName:        "source-host",
			SourceSvmName:         "source-svm",
			SourceVolumeName:      "source-volume",
			SourceVolumeUUID:      "",
			SourcePoolUUID:        "",
			DestinationHostName:   "dest-host",
			DestinationSvmName:    "dest-svm",
			DestinationVolumeName: "dest-volume",
			DestinationVolumeUUID: "",
			DestinationPoolUUID:   "",
		}

		var result *gcpserver.VolumeReplicationInternalV1beta
		result = ConvertToReversedAttributesForHybridRep(originalAttrs)

		assert.NotNil(tt, result)
		// Original destination becomes new source
		assert.Equal(tt, "dest-host", result.SourceHostName)
		assert.Equal(tt, "dest-svm", result.SourceServerName)
		assert.Equal(tt, "dest-volume", result.SourceVolumeName)
		assert.False(tt, result.SourceVolumeUuid.Set)
		assert.Equal(tt, "", result.SourceVolumeUuid.Value)
		assert.False(tt, result.SourcePoolUuid.Set)
		assert.Equal(tt, "", result.SourcePoolUuid.Value)

		// Original source becomes new destination
		assert.Equal(tt, "source-host", result.DestinationHostName)
		assert.Equal(tt, "source-svm", result.DestinationServerName)
		assert.Equal(tt, "source-volume", result.DestinationVolumeName)
		assert.False(tt, result.DestinationVolumeUuid.Set)
		assert.Equal(tt, "", result.DestinationVolumeUuid.Value)
		assert.False(tt, result.DestinationPoolUuid.Set)
		assert.Equal(tt, "", result.DestinationPoolUuid.Value)
	})

	t.Run("WhenSuccess_WithPartialFields", func(tt *testing.T) {
		originalAttrs := &datamodel.ReplicationDetails{
			SourceHostName:        "source-host",
			SourceSvmName:         "source-svm",
			SourceVolumeName:      "source-volume",
			SourceVolumeUUID:      "source-volume-uuid",
			SourcePoolUUID:        "",
			DestinationHostName:   "dest-host",
			DestinationSvmName:    "dest-svm",
			DestinationVolumeName: "dest-volume",
			DestinationVolumeUUID: "",
			DestinationPoolUUID:   "dest-pool-uuid",
		}

		var result *gcpserver.VolumeReplicationInternalV1beta
		result = ConvertToReversedAttributesForHybridRep(originalAttrs)

		assert.NotNil(tt, result)
		// Original destination becomes new source
		assert.Equal(tt, "dest-host", result.SourceHostName)
		assert.Equal(tt, "dest-svm", result.SourceServerName)
		assert.Equal(tt, "dest-volume", result.SourceVolumeName)
		assert.False(tt, result.SourceVolumeUuid.Set)
		assert.Equal(tt, "", result.SourceVolumeUuid.Value)
		assert.True(tt, result.SourcePoolUuid.Set)
		assert.Equal(tt, "dest-pool-uuid", result.SourcePoolUuid.Value)

		// Original source becomes new destination
		assert.Equal(tt, "source-host", result.DestinationHostName)
		assert.Equal(tt, "source-svm", result.DestinationServerName)
		assert.Equal(tt, "source-volume", result.DestinationVolumeName)
		assert.True(tt, result.DestinationVolumeUuid.Set)
		assert.Equal(tt, "source-volume-uuid", result.DestinationVolumeUuid.Value)
		assert.False(tt, result.DestinationPoolUuid.Set)
		assert.Equal(tt, "", result.DestinationPoolUuid.Value)
	})
}

func TestReverseVolumeReplicationActivity_HydrateReplicationSateAndTypeForReverseFallbackHybridReplication(t *testing.T) {
	ctx := context.Background()
	activity := ReverseVolumeReplicationActivity{}

	t.Run("WhenSuccess_WithHydrationEnabled", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Mock hydrateReplicationStateAndTypeForHybrid to return success
		// Note: This variable is defined in replication_reverse_hybrid_activities.go
		// We need to access it through the package
		originalHydrateReplicationStateAndTypeForHybrid := hydrateReplicationStateAndTypeForHybrid
		hydrateReplicationStateAndTypeForHybrid = func(ctx context.Context, volumeRepModel models.VolumeReplication, hydrateState models.VolumeReplicationHydrateState, hydrateType models.HybridReplicationParametersReplicationType, projectNumber string) error {
			assert.Equal(tt, models.VolumeReplicationHydrateStateReady, hydrateState)
			assert.Equal(tt, models.HybridReplicationParametersReplicationTypeONPREM, hydrateType)
			return nil
		}
		defer func() { hydrateReplicationStateAndTypeForHybrid = originalHydrateReplicationStateAndTypeForHybrid }()

		result := &replication.ReverseReplicationResult{
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

		updatedResult, err := activity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
	})

	t.Run("WhenSuccess_WithHydrationDisabled", func(tt *testing.T) {
		// Mock hydrationEnabled to be false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		result := &replication.ReverseReplicationResult{
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

		updatedResult, err := activity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
	})

	t.Run("WhenParseProjectNumberFails", func(tt *testing.T) {
		// Mock hydrationEnabled to be true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		result := &replication.ReverseReplicationResult{
			Event: &replication.ReverseReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					Location: "us-east1",
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

		updatedResult, err := activity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication(ctx, result)

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

		result := &replication.ReverseReplicationResult{
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

		updatedResult, err := activity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "hydration error")
	})
}
