package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGetSrcBasePathResume(t *testing.T) {
	t.Run("ValidSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}
		activity := ResumeVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://src-base-path.example.com", nil
		}

		updatedResult, err := activity.GetSrcBasePathResume(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcBasePath)
		assert.Equal(tt, "https://src-base-path.example.com", *updatedResult.SrcBasePath)
	})
	t.Run("ErrorSrcBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: "us-central1",
						},
					},
				},
			},
		}
		activity := ResumeVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetSrcBasePathResume(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
	t.Run("WhenSourceLocationIsEmpty", func(tt *testing.T) {
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation: RemoteRegionCustomer,
						},
					},
				},
			},
		}
		activity := ResumeVolumeReplicationActivity{}

		updatedResult, err := activity.GetSrcBasePathResume(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Nil(tt, updatedResult.SrcBasePath)
	})
}

func TestGetDstBasePathResume(t *testing.T) {
	t.Run("ValidDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := ResumeVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "https://dst-base-path.example.com", nil
		}

		updatedResult, err := activity.GetDstBasePathResume(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstBasePath)
		assert.Equal(tt, "https://dst-base-path.example.com", *updatedResult.DstBasePath)
	})
	t.Run("ErrorDstBasePath", func(tt *testing.T) {
		defer func() {
			replicationInternalParseRegionAndZone = replication.InternalParseRegionAndZone
			replicationInternalUtilGetPairedRegionURI = replication.InternalUtilGetPairedRegionURI
		}()

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: "us-east1",
						},
					},
				},
			},
		}
		activity := ResumeVolumeReplicationActivity{}

		replicationInternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-east1", "", nil
		}
		replicationInternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		updatedResult, err := activity.GetDstBasePathResume(context.Background(), result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
	t.Run("WhenDestinationLocationIsEmpty", func(tt *testing.T) {
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation: RemoteRegionCustomer,
						},
					},
				},
			},
		}
		activity := ResumeVolumeReplicationActivity{}

		updatedResult, err := activity.GetDstBasePathResume(context.Background(), result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Nil(tt, updatedResult.DstBasePath)
	})
}

func TestGetSignedSrcTokenResume(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.GetSignedSrcTokenResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.SrcJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.SrcJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.GetSignedSrcTokenResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestGetSignedDstTokenResume(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenSuccessSameProject", func(tt *testing.T) {
		prj := "prj"
		token := "signed-token"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult.DstJwtToken)
		assert.Equal(tt, "signed-token", *updatedResult.DstJwtToken)
	})
	t.Run("WhenError", func(tt *testing.T) {
		dstPrj := "dstPrj"
		srcPrj := "srcPrj"
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.GetSignedDstTokenResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
	})
}

func TestVerifyDstVolume(t *testing.T) {
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
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
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
		updatedResult, err := activity.VerifyDstVolume(ctx, result)

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
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
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
		updatedResult, err := activity.VerifyDstVolume(ctx, result)
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
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.VerifyDstVolume(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, result, updatedResult)
	})
}

func TestResumeReplicationOnDestination(t *testing.T) {
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
		inputResult := &replication.ResumeReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.ResumeReplicationEvent{
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
		params := &common.ResumeReplicationParams{
			Force:         false,
			CorrelationId: "correlation-id",
		}
		resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			ForceResume:         googleproxyclient.NewOptBool(params.Force),
			XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrGoogleProxyInternalResumeReplication, errors.New("some-error"))
		mockClient.EXPECT().V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams).Return(nil, errors.New("some-error"))
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ResumeReplicationOnDestination(context.Background(), inputResult, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenBadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.ResumeReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.ResumeReplicationEvent{
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
		params := &common.ResumeReplicationParams{
			Force:         false,
			CorrelationId: "correlation-id",
		}
		resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			ForceResume:         googleproxyclient.NewOptBool(params.Force),
			XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		badRequestResponse := &googleproxyclient.V1betaInternalResumeVolumeReplicationBadRequest{
			Code:    400,
			Message: "invalid parameter",
		}

		mockClient.EXPECT().V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams).Return(badRequestResponse, nil)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ResumeReplicationOnDestination(context.Background(), inputResult, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to resume replication", err.Error())
	})
	t.Run("WhenUnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.ResumeReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.ResumeReplicationEvent{
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
		params := &common.ResumeReplicationParams{
			Force:         false,
			CorrelationId: "correlation-id",
		}
		resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			ForceResume:         googleproxyclient.NewOptBool(params.Force),
			XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		unauthorizedResponse := &googleproxyclient.V1betaInternalResumeVolumeReplicationUnauthorized{
			Code:    401,
			Message: "Authentication failed",
		}

		mockClient.EXPECT().V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams).Return(unauthorizedResponse, nil)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ResumeReplicationOnDestination(context.Background(), inputResult, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to resume replication", err.Error())
	})
	t.Run("WhenForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.ResumeReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.ResumeReplicationEvent{
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
		params := &common.ResumeReplicationParams{
			Force:         false,
			CorrelationId: "correlation-id",
		}
		resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			ForceResume:         googleproxyclient.NewOptBool(params.Force),
			XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		forbiddenResponse := &googleproxyclient.V1betaInternalResumeVolumeReplicationForbidden{
			Code:    403,
			Message: "Access denied",
		}

		mockClient.EXPECT().V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams).Return(forbiddenResponse, nil)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ResumeReplicationOnDestination(context.Background(), inputResult, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to resume replication", err.Error())
	})
	t.Run("WhenNotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.ResumeReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.ResumeReplicationEvent{
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
		params := &common.ResumeReplicationParams{
			Force:         false,
			CorrelationId: "correlation-id",
		}
		resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			ForceResume:         googleproxyclient.NewOptBool(params.Force),
			XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		notFoundResponse := &googleproxyclient.V1betaInternalResumeVolumeReplicationNotFound{
			Code:    404,
			Message: "Not found",
		}

		mockClient.EXPECT().V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams).Return(notFoundResponse, nil)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ResumeReplicationOnDestination(context.Background(), inputResult, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to resume replication", err.Error())
	})
	t.Run("WhenConflictError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.ResumeReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.ResumeReplicationEvent{
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
		params := &common.ResumeReplicationParams{
			Force:         false,
			CorrelationId: "correlation-id",
		}
		resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			ForceResume:         googleproxyclient.NewOptBool(params.Force),
			XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		conflictResponse := &googleproxyclient.V1betaInternalResumeVolumeReplicationConflict{
			Code:    409,
			Message: "conflict",
		}

		mockClient.EXPECT().V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams).Return(conflictResponse, nil)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ResumeReplicationOnDestination(context.Background(), inputResult, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to resume replication", err.Error())
	})
	t.Run("WhenInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := &database.MockStorage{}
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		inputResult := &replication.ResumeReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.ResumeReplicationEvent{
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
		params := &common.ResumeReplicationParams{
			Force:         false,
			CorrelationId: "correlation-id",
		}
		resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			ForceResume:         googleproxyclient.NewOptBool(params.Force),
			XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		internalErrorResponse := &googleproxyclient.V1betaInternalResumeVolumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}

		mockClient.EXPECT().V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams).Return(internalErrorResponse, nil)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ResumeReplicationOnDestination(context.Background(), inputResult, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "Failed to resume replication", err.Error())
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
		inputResult := &replication.ResumeReplicationResult{
			DstBasePath:      &dstPath,
			DstProjectNumber: &dstProj,
			DstJwtToken:      &dstToken,
			Event: &replication.ResumeReplicationEvent{
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
		params := &common.ResumeReplicationParams{
			Force:         true,
			CorrelationId: "correlation-id",
		}
		resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       *inputResult.DstProjectNumber,
			LocationId:          inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeReplicationId: inputResult.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
			ForceResume:         googleproxyclient.NewOptBool(params.Force),
			XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams).Return(res, nil)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result, err := activity.ResumeReplicationOnDestination(context.Background(), inputResult, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, *result.JobId, "job-uuid")
	})
}

func TestDescribeRemoteJobResume(t *testing.T) {
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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ResumeReplicationEvent{
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

		err := activity.DescribeRemoteJobResume(ctx, result)

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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ResumeReplicationEvent{
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
		err := activity.DescribeRemoteJobResume(ctx, result)

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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}
		result := &replication.ResumeReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.ResumeReplicationEvent{
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
		err := activity.DescribeRemoteJobResume(ctx, result)

		assert.Error(tt, err)
	})
}

func TestResizeVolumeIfNeeded(t *testing.T) {
	t.Run("WhenSrcVolumeQuotaEqualToDestination", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(tt)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		result := &replication.ResumeReplicationResult{
			SrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			DstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
		}

		_, err := activity.ResizeVolumeIfNeeded(ctx, result)
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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ResumeReplicationResult{
			SrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			DstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.ResizeVolumeIfNeeded(ctx, result)
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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ResumeReplicationResult{
			SrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			DstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.ResizeVolumeIfNeeded(ctx, result)
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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ResumeReplicationResult{
			SrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			DstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.ResizeVolumeIfNeeded(ctx, result)
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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ResumeReplicationResult{
			SrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			DstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.ResizeVolumeIfNeeded(ctx, result)
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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ResumeReplicationResult{
			SrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			DstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.ResizeVolumeIfNeeded(ctx, result)
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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ResumeReplicationResult{
			SrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			DstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.ResizeVolumeIfNeeded(ctx, result)
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

		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"
		dstBasePath := "dst-base-path"
		dstJwtToken := "dst-jwt-token"
		correlationID := "test-correlation-id"

		result := &replication.ResumeReplicationResult{
			SrcVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 2000.0},
			},
			DstVolume: &googleproxyclient.VolumeV1beta{
				QuotaInBytes: googleproxyclient.OptFloat64{Set: true, Value: 1000.0},
			},
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			Event: &replication.ResumeReplicationEvent{
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

		updatedResult, err := activity.ResizeVolumeIfNeeded(ctx, result)
		assert.Error(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Contains(tt, err.Error(), "Failed to update volume internal")
		mockClient.AssertExpectations(tt)
	})
}

func TestMountReplicationAfterResume(t *testing.T) {
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
		dstProjectNumber := "123456789"
		correlationID := "test-correlation-id"
		jobUUID := "test-job-uuid-12345"

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
		}

		// Setup expected parameters
		expectedParams := googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       dstProjectNumber,
			LocationId:          "us-central1",
			VolumeReplicationId: "dest-replication-uuid-123",
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
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			assert.Equal(tt, dstBasePath, basePath)
			assert.Equal(tt, dstJwtToken, jwt)
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, expectedParams).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

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
		dstProjectNumber := "123456789"
		correlationID := "test-correlation-id"

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: &correlationID,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			DstProjectNumber: &dstProjectNumber,
		}

		// Setup expected parameters
		expectedParams := googleproxyclient.V1betaInternalMountVolumeReplicationParams{
			ProjectNumber:       dstProjectNumber,
			LocationId:          "us-central1",
			VolumeReplicationId: "dest-replication-uuid-123",
			XCorrelationID:      googleproxyclient.NewOptString(correlationID),
		}

		apiError := errors.New("network timeout")

		// Setup mock client
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, expectedParams).Return(nil, apiError)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_BadRequest", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-correlation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:      nillable.GetStringPtr("test-jwt-token"),
			DstProjectNumber: nillable.GetStringPtr("123456789"),
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationBadRequest{
			Message: "Invalid request parameters",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_Unauthorized", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-correlation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:      nillable.GetStringPtr("invalid-jwt-token"),
			DstProjectNumber: nillable.GetStringPtr("123456789"),
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationUnauthorized{
			Message: "Authentication failed",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_Forbidden", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-correlation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:      nillable.GetStringPtr("test-jwt-token"),
			DstProjectNumber: nillable.GetStringPtr("123456789"),
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationForbidden{
			Message: "Access denied to resource",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_NotFound", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-correlation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "nonexistent-replication-uuid",
						},
					},
				},
			},
			DstBasePath:      nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:      nillable.GetStringPtr("test-jwt-token"),
			DstProjectNumber: nillable.GetStringPtr("123456789"),
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationNotFound{
			Message: "Volume replication not found",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_Conflict", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-correlation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:      nillable.GetStringPtr("test-jwt-token"),
			DstProjectNumber: nillable.GetStringPtr("123456789"),
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationConflict{
			Message: "Volume already mounted",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_MethodNotAllowed", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-correlation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:      nillable.GetStringPtr("test-jwt-token"),
			DstProjectNumber: nillable.GetStringPtr("123456789"),
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationMethodNotAllowed{
			Message: "Method not allowed for this operation",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_UnprocessableEntity", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-correlation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:      nillable.GetStringPtr("test-jwt-token"),
			DstProjectNumber: nillable.GetStringPtr("123456789"),
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationUnprocessableEntity{
			Message: "Invalid entity format",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})

	t.Run("Error_InternalServerError", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID: nillable.GetStringPtr("test-correlation-id"),
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:        "us-central1",
							DestinationReplicationUUID: "dest-replication-uuid-123",
						},
					},
				},
			},
			DstBasePath:      nillable.GetStringPtr("https://test-dst-base-path.com"),
			DstJwtToken:      nillable.GetStringPtr("test-jwt-token"),
			DstProjectNumber: nillable.GetStringPtr("123456789"),
		}

		mockResponse := &googleproxyclient.V1betaInternalMountVolumeReplicationInternalServerError{
			Message: "Internal server error occurred",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		mockClient.EXPECT().V1betaInternalMountVolumeReplication(ctx, mock.Anything).Return(mockResponse, nil)

		// Execute test
		activity := &ResumeVolumeReplicationActivity{}
		updatedResult, err := activity.MountReplicationAfterResume(ctx, result)

		// Verify results
		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		assert.Equal(tt, "Failed to mount volume replication", err.Error())
		mockClient.AssertExpectations(tt)
	})
}

func TestHandleHybridReplicationResumeWhenGcnvIsSrc(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		replicationUUID := "test-replication-uuid"
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						BaseModel: datamodel.BaseModel{
							UUID: replicationUUID,
						},
					},
				},
			},
		}

		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-volume",
				DestinationSvmName:    "dst-svm",
				DestinationVolumeName: "dst-volume",
			},
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(dbReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.AnythingOfType("*datamodel.VolumeReplication")).Return(nil)

		updatedResult, err := activity.HandleHybridReplicationResumeWhenGcnvIsSrc(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		replicationUUID := "test-replication-uuid"
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						BaseModel: datamodel.BaseModel{
							UUID: replicationUUID,
						},
					},
				},
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(nil, errors.New("database error"))

		updatedResult, err := activity.HandleHybridReplicationResumeWhenGcnvIsSrc(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		replicationUUID := "test-replication-uuid"
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						BaseModel: datamodel.BaseModel{
							UUID: replicationUUID,
						},
					},
				},
			},
		}

		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-volume",
				DestinationSvmName:    "dst-svm",
				DestinationVolumeName: "dst-volume",
			},
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(dbReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.AnythingOfType("*datamodel.VolumeReplication")).Return(errors.New("update error"))

		updatedResult, err := activity.HandleHybridReplicationResumeWhenGcnvIsSrc(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenHybridReplicationAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		replicationUUID := "test-replication-uuid"
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						BaseModel: datamodel.BaseModel{
							UUID: replicationUUID,
						},
					},
				},
			},
		}

		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-volume",
				DestinationSvmName:    "dst-svm",
				DestinationVolumeName: "dst-volume",
			},
			HybridReplicationAttributes: nil,
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(dbReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
			return r.HybridReplicationAttributes != nil
		})).Return(nil)

		updatedResult, err := activity.HandleHybridReplicationResumeWhenGcnvIsSrc(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenReplicationAttributesIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		activity := ResumeVolumeReplicationActivity{SE: mockStorage}

		replicationUUID := "test-replication-uuid"
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						BaseModel: datamodel.BaseModel{
							UUID: replicationUUID,
						},
					},
				},
			},
		}

		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: replicationUUID,
			},
			ReplicationAttributes:       nil,
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(dbReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
			return r.HybridReplicationAttributes != nil && len(r.HybridReplicationAttributes.HybridReplicationUserCommands) == 0
		})).Return(nil)

		updatedResult, err := activity.HandleHybridReplicationResumeWhenGcnvIsSrc(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		mockStorage.AssertExpectations(tt)
	})
}

func TestSetHybridReplicationVariablesResume(t *testing.T) {
	ctx := context.Background()
	activity := ResumeVolumeReplicationActivity{}

	t.Run("WhenDbVolReplicationIsNil", func(tt *testing.T) {
		result := &replication.ResumeReplicationResult{
			DbVolReplication: nil,
		}

		// IsSrcForHybridReplication will panic if replication is nil, so we expect a panic
		assert.Panics(tt, func() {
			_, _ = activity.SetHybridReplicationVariablesResume(ctx, result)
		})
	})

	t.Run("WhenHybridReplicationAttributesIsNil", func(tt *testing.T) {
		result := &replication.ResumeReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: nil,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.False(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenHybridReplicationAttributesIsSetButNotReverse", func(tt *testing.T) {
		migrationType := string(coreModels.HybridReplicationParametersReplicationTypeMIGRATION)
		result := &replication.ResumeReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: &migrationType,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenIsSrcForHybridReplicationIsTrue", func(tt *testing.T) {
		reverseType := string(coreModels.HybridReplicationParametersReplicationTypeREVERSE)
		result := &replication.ResumeReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: &reverseType,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: RemoteRegionCustomer,
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.True(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenHybridReplicationTypeIsReverseButDestinationLocationIsNotEmpty", func(tt *testing.T) {
		reverseType := string(coreModels.HybridReplicationParametersReplicationTypeREVERSE)
		result := &replication.ResumeReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: &reverseType,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})

	t.Run("WhenHybridReplicationTypeIsNil", func(tt *testing.T) {
		result := &replication.ResumeReplicationResult{
			DbVolReplication: &datamodel.VolumeReplication{
				HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
					HybridReplicationType: nil,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "",
				},
			},
		}

		updatedResult, err := activity.SetHybridReplicationVariablesResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Equal(tt, result, updatedResult)
		assert.True(tt, updatedResult.IsHybridReplicationVolume)
		assert.False(tt, updatedResult.IsSrcForHybridReplication)
	})
}

func TestListQuotaRulesOnSourceResume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := ResumeVolumeReplicationActivity{SE: mockStorage}

	srcBasePath := "https://src-base-path.com"
	srcJwtToken := "src-jwt-token"
	srcProjectNumber := "123456789"
	srcLocation := "us-central1"
	srcVolumeUUID := "src-volume-uuid"
	correlationID := "test-correlation-id"

	t.Run("WhenSuccess_WithQuotaRules", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		expectedResponse := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "quota-rule-1",
					QuotaId:        googleproxyclient.NewOptString("quota-uuid-1"),
					DiskLimitInMib: int64(1024),
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
				},
				{
					ResourceId:     "quota-rule-2",
					QuotaId:        googleproxyclient.NewOptString("quota-uuid-2"),
					DiskLimitInMib: int64(2048),
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
				},
			},
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(expectedResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRules)
		assert.Len(tt, quotaRules, 2)
		assert.Equal(tt, "quota-rule-1", quotaRules[0].Name)
		assert.Equal(tt, "quota-uuid-1", quotaRules[0].UUID)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_QuotaRuleWithDescription_DescriptionSyncedToDbModel", func(tt *testing.T) {
		// Ensures list response Description is converted so destination gets it on resume
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		sourceDescription := "Source quota rule description for resume sync"
		expectedResponse := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "quota-rule-with-desc",
					QuotaId:        googleproxyclient.NewOptString("quota-uuid-desc"),
					DiskLimitInMib: int64(512),
					QuotaType:      googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
					Description:    googleproxyclient.NewOptString(sourceDescription),
				},
			},
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(expectedResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRules)
		assert.Len(tt, quotaRules, 1)
		assert.Equal(tt, "quota-rule-with-desc", quotaRules[0].Name)
		assert.Equal(tt, sourceDescription, quotaRules[0].Description, "Description from source must be in db model so destination gets it on AddSrcQuotaRulesToDstDB")
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_NoQuotaRules", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		expectedResponse := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{},
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(expectedResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRules)
		assert.Len(tt, quotaRules, 0)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenAPIError", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(nil, errors.New("network error"))

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenBadRequest", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		badRequestResponse := &googleproxyclient.V1betaListAllQuotaRulesBadRequest{
			Message: "Invalid request",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(badRequestResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenUnauthorized", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		unauthorizedResponse := &googleproxyclient.V1betaListAllQuotaRulesUnauthorized{
			Message: "Unauthorized",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(unauthorizedResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		internalErrorResponse := &googleproxyclient.V1betaListAllQuotaRulesInternalServerError{
			Message: "Internal server error",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(internalErrorResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenForbidden", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		forbiddenResponse := &googleproxyclient.V1betaListAllQuotaRulesForbidden{
			Message: "Forbidden",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(forbiddenResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenNotFound", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		notFoundResponse := &googleproxyclient.V1betaListAllQuotaRulesNotFound{
			Message: "Not found",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(notFoundResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenConflict", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		conflictResponse := &googleproxyclient.V1betaListAllQuotaRulesConflict{
			Message: "Conflict",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(conflictResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenUnprocessableEntity", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		unprocessableEntityResponse := &googleproxyclient.V1betaListAllQuotaRulesUnprocessableEntity{
			Message: "Unprocessable entity",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(unprocessableEntityResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenTooManyRequests", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:      &correlationID,
					SourceProjectNumber: srcProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							SourceLocation:   srcLocation,
							SourceVolumeUUID: srcVolumeUUID,
						},
					},
				},
			},
			SrcBasePath: &srcBasePath,
			SrcJwtToken: &srcJwtToken,
		}

		tooManyRequestsResponse := &googleproxyclient.V1betaListAllQuotaRulesTooManyRequests{
			Message: "Too many requests",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(tooManyRequestsResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnSourceResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})
}

func TestListQuotaRulesOnDestinationResume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := ResumeVolumeReplicationActivity{SE: mockStorage}

	dstBasePath := "https://dst-base-path.com"
	dstJwtToken := "dst-jwt-token"
	dstProjectNumber := "987654321"
	dstLocation := "us-east1"
	dstVolumeUUID := "dst-volume-uuid"
	correlationID := "test-correlation-id"

	t.Run("WhenSuccess_WithQuotaRules", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		expectedResponse := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "quota-rule-1",
					QuotaId:        googleproxyclient.NewOptString("quota-uuid-1"),
					DiskLimitInMib: int64(1024),
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
				},
			},
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(expectedResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRules)
		assert.Len(tt, quotaRules, 1)
		assert.Equal(tt, "quota-rule-1", quotaRules[0].Name)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_NoQuotaRules", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		expectedResponse := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{},
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(expectedResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRules)
		assert.Len(tt, quotaRules, 0)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenAPIError", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(nil, errors.New("network error"))

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenBadRequest", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		badRequestResponse := &googleproxyclient.V1betaListAllQuotaRulesBadRequest{
			Message: "Invalid request",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(badRequestResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenUnauthorized", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		unauthorizedResponse := &googleproxyclient.V1betaListAllQuotaRulesUnauthorized{
			Message: "Unauthorized",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(unauthorizedResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		internalErrorResponse := &googleproxyclient.V1betaListAllQuotaRulesInternalServerError{
			Message: "Internal server error",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(internalErrorResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenForbidden", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		forbiddenResponse := &googleproxyclient.V1betaListAllQuotaRulesForbidden{
			Message: "Forbidden",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(forbiddenResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenNotFound", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		notFoundResponse := &googleproxyclient.V1betaListAllQuotaRulesNotFound{
			Message: "Not found",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(notFoundResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenConflict", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		conflictResponse := &googleproxyclient.V1betaListAllQuotaRulesConflict{
			Message: "Conflict",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(conflictResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenUnprocessableEntity", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		unprocessableEntityResponse := &googleproxyclient.V1betaListAllQuotaRulesUnprocessableEntity{
			Message: "Unprocessable entity",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(unprocessableEntityResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenTooManyRequests", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath: &dstBasePath,
			DstJwtToken: &dstJwtToken,
		}

		tooManyRequestsResponse := &googleproxyclient.V1betaListAllQuotaRulesTooManyRequests{
			Message: "Too many requests",
		}

		mockClient.EXPECT().V1betaListAllQuotaRules(ctx, mock.Anything).Return(tooManyRequestsResponse, nil)

		quotaRules, err := activity.ListQuotaRulesOnDestinationResume(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRules)
		mockClient.AssertExpectations(tt)
	})
}

func TestDehydrateQuotaRulesResume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := ResumeVolumeReplicationActivity{SE: mockStorage}

	volumeResourceId := "volume-resource-id"
	location := "us-central1"
	projectNumber := "123456789"

	t.Run("WhenSuccess_WithQuotaRules", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
			{
				Name: "quota-rule-2",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-2",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		callCount := 0
		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			callCount++
			assert.Equal(tt, "test-callback-token", token)
			assert.Len(tt, quotaRuleNames, 1) // Batch size = 1
			return nil
		}

		dehydratedRules, err := activity.DehydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err)
		assert.Len(tt, dehydratedRules, 2)
		assert.Equal(tt, "quota-rule-1", dehydratedRules[0].Name)
		assert.Equal(tt, "quota-rule-2", dehydratedRules[1].Name)
		assert.Equal(tt, 2, callCount) // Called once per rule
	})

	t.Run("WhenSuccess_EmptyQuotaRules", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		quotaRules := []*datamodel.QuotaRule{}

		dehydratedRules, err := activity.DehydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err)
		assert.Len(tt, dehydratedRules, 0)
	})

	t.Run("WhenSuccess_NilQuotaRules", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		var quotaRules []*datamodel.QuotaRule

		dehydratedRules, err := activity.DehydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err)
		assert.Len(tt, dehydratedRules, 0)
	})

	t.Run("WhenGenerateCallbackTokenFails", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("failed to generate token")
		}

		dehydratedRules, err := activity.DehydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed To Generate Access Token")
		assert.Len(tt, dehydratedRules, 0) // No rules dehydrated on token generation failure
	})

	t.Run("WhenQuotaRuleHasNoName", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		dehydratedRules, err := activity.DehydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		// Should fail with fatal error since quota rule has no name
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid input parameters provided")
		assert.Len(tt, dehydratedRules, 0) // No rules dehydrated before the error
	})

	t.Run("WhenQuotaRuleUsesName", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		var capturedNames []string
		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			capturedNames = append(capturedNames, quotaRuleNames...)
			return nil
		}

		dehydratedRules, err := activity.DehydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err)
		assert.Equal(tt, []string{"quota-rule-1"}, capturedNames)
		assert.Len(tt, dehydratedRules, 1)
		assert.Equal(tt, "quota-rule-1", dehydratedRules[0].Name)
	})

	t.Run("WhenHydrateQuotaRulesDeleteFails", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			return errors.New("dehydration failed")
		}

		dehydratedRules, err := activity.DehydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err)            // Partial failure should not return error
		assert.Len(tt, dehydratedRules, 0) // No rules dehydrated on first failure
	})

	t.Run("WhenPartialDehydrationFailure", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRulesDelete := common.HydrateQuotaRulesDelete
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			common.HydrateQuotaRulesDelete = originalHydrateQuotaRulesDelete
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
			{
				Name: "quota-rule-2",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-2",
				},
			},
			{
				Name: "quota-rule-3",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-3",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		callCount := 0
		common.HydrateQuotaRulesDelete = func(ctx context.Context, logger log.Logger, quotaRuleNames []string, volumeResourceId string, location string, projectNumber string, token string) error {
			callCount++
			// Fail on the second quota rule
			if callCount == 2 {
				return errors.New("dehydration failed for second quota rule")
			}
			return nil
		}

		dehydratedRules, err := activity.DehydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err) // Partial failure should not return error
		// Only the first quota rule should be successfully dehydrated
		assert.Len(tt, dehydratedRules, 1)
		assert.Equal(tt, "quota-rule-1", dehydratedRules[0].Name)
		assert.Equal(tt, "quota-uuid-1", dehydratedRules[0].UUID)
	})
}

// TestAddSrcQuotaRulesToDstDB tests the AddSrcQuotaRulesToDstDB activity
func TestAddSrcQuotaRulesToDstDB(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := ResumeVolumeReplicationActivity{SE: mockStorage}

	dstBasePath := "https://dst-base-path.com"
	dstJwtToken := "dst-jwt-token"
	dstProjectNumber := "987654321"
	dstLocation := "us-east1"
	dstVolumeUUID := "dst-volume-uuid"
	correlationID := "test-correlation-id"

	t.Run("WhenSuccess_WithQuotaRules", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "src-quota-uuid-1",
				},
			},
		}

		destinationQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "dst-quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "dst-quota-uuid-1",
				},
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:           &dstBasePath,
			DstJwtToken:           &dstJwtToken,
			SourceQuotaRules:      sourceQuotaRules,
			DestinationQuotaRules: destinationQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		expectedResponse := &googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "synced-quota-rule-1",
					QuotaId:        googleproxyclient.NewOptString("synced-quota-uuid-1"),
					DiskLimitInMib: int64(1024),
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
				},
			},
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.NotNil(tt, updatedResult.DestinationQuotaRules)
		assert.Len(tt, updatedResult.DestinationQuotaRules, 1)
		assert.Equal(tt, "synced-quota-rule-1", updatedResult.DestinationQuotaRules[0].Name)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_NoSourceQuotaRules", func(tt *testing.T) {
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:           &dstBasePath,
			DstJwtToken:           &dstJwtToken,
			SourceQuotaRules:      nil,
			DestinationQuotaRules: nil,
		}

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Nil(tt, updatedResult.DestinationQuotaRules)
	})

	t.Run("WhenSuccess_EmptySourceQuotaRules", func(tt *testing.T) {
		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:           &dstBasePath,
			DstJwtToken:           &dstJwtToken,
			SourceQuotaRules:      []*datamodel.QuotaRule{},
			DestinationQuotaRules: nil,
		}

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.Nil(tt, updatedResult.DestinationQuotaRules)
	})

	t.Run("WhenRecoveryScenario_NilSourceWithDehydratedDestination", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		// This represents dehydrated quota rules that need to be re-created
		dehydratedQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "dehydrated-quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "dehydrated-quota-uuid-1",
				},
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:           &dstBasePath,
			DstJwtToken:           &dstJwtToken,
			SourceQuotaRules:      nil, // nil source indicates recovery scenario
			DestinationQuotaRules: dehydratedQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		expectedResponse := &googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId:     "recovered-quota-rule-1",
					QuotaId:        googleproxyclient.NewOptString("recovered-quota-uuid-1"),
					DiskLimitInMib: int64(1024),
					State:          googleproxyclient.NewOptQuotaRulesV1betaState(googleproxyclient.QuotaRulesV1betaStateREADY),
				},
			},
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.NoError(tt, err)
		assert.NotNil(tt, updatedResult)
		assert.NotNil(tt, updatedResult.DestinationQuotaRules)
		assert.Len(tt, updatedResult.DestinationQuotaRules, 1)
		assert.Equal(tt, "recovered-quota-rule-1", updatedResult.DestinationQuotaRules[0].Name)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenAPIError", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SourceQuotaRules: sourceQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(nil, errors.New("network error"))

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenBadRequest", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SourceQuotaRules: sourceQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		badRequestResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPBadRequest{
			Message: "Invalid request",
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(badRequestResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SourceQuotaRules: sourceQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		internalErrorResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPInternalServerError{
			Message: "Internal server error",
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(internalErrorResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenUnauthorized", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SourceQuotaRules: sourceQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		unauthorizedResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPUnauthorized{
			Message: "Unauthorized",
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(unauthorizedResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenForbidden", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SourceQuotaRules: sourceQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		forbiddenResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPForbidden{
			Message: "Forbidden",
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(forbiddenResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenNotFound", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SourceQuotaRules: sourceQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		notFoundResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPNotFound{
			Message: "Not found",
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(notFoundResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenUnprocessableEntity", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SourceQuotaRules: sourceQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		unprocessableEntityResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPUnprocessableEntity{
			Message: "Unprocessable entity",
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(unprocessableEntityResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenTooManyRequests", func(tt *testing.T) {
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{
				Name: "src-quota-rule-1",
			},
		}

		result := &replication.ResumeReplicationResult{
			Event: &replication.ResumeReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					XCorrelationID:           &correlationID,
					DestinationProjectNumber: dstProjectNumber,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   dstLocation,
							DestinationVolumeUUID: dstVolumeUUID,
						},
					},
				},
			},
			DstBasePath:      &dstBasePath,
			DstJwtToken:      &dstJwtToken,
			SourceQuotaRules: sourceQuotaRules,
			DstVolume: &googleproxyclient.VolumeV1beta{
				VolumeId: googleproxyclient.NewOptString(dstVolumeUUID),
			},
		}

		tooManyRequestsResponse := &googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPTooManyRequests{
			Message: "Too many requests",
		}

		mockClient.EXPECT().V1betaUpdateDestinationQuotaRulesVCP(ctx, mock.Anything, mock.Anything).Return(tooManyRequestsResponse, nil)

		updatedResult, err := activity.AddSrcQuotaRulesToDstDB(ctx, result)

		assert.Error(tt, err)
		assert.Nil(tt, updatedResult)
		mockClient.AssertExpectations(tt)
	})
}

// TestHydrateQuotaRulesResume tests the HydrateQuotaRulesResume activity
func TestHydrateQuotaRulesResume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := ResumeVolumeReplicationActivity{SE: mockStorage}

	volumeResourceId := "volume-resource-id"
	location := "us-central1"
	projectNumber := "123456789"

	t.Run("WhenSuccess_WithQuotaRules", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRuleCreate := hydrateQuotaRuleCreate
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			hydrateQuotaRuleCreate = originalHydrateQuotaRuleCreate
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
			{
				Name: "quota-rule-2",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-2",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		callCount := 0
		hydrateQuotaRuleCreate = func(ctx context.Context, logger log.Logger, quotaRule coreModels.QuotaRuleHydrateObject, volumeResourceID string, location string, projectId string, token string) error {
			callCount++
			assert.Equal(tt, "test-callback-token", token)
			assert.Equal(tt, volumeResourceId, volumeResourceID)
			assert.Equal(tt, location, location)
			assert.Equal(tt, projectNumber, projectId)
			return nil
		}

		err := activity.HydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err)
		assert.Equal(tt, 2, callCount)
	})

	t.Run("WhenSuccess_EmptyQuotaRules", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		quotaRules := []*datamodel.QuotaRule{}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		err := activity.HydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err)
	})

	t.Run("WhenSuccess_NilQuotaRules", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		var quotaRules []*datamodel.QuotaRule

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		err := activity.HydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.NoError(tt, err)
	})

	t.Run("WhenGenerateCallbackTokenFails", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("failed to generate token")
		}

		err := activity.HydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to hydrate volume creation")
	})

	t.Run("WhenQuotaRuleHasNoUUID", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRuleCreate := hydrateQuotaRuleCreate
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			hydrateQuotaRuleCreate = originalHydrateQuotaRuleCreate
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		hydrateQuotaRuleCreate = func(ctx context.Context, logger log.Logger, quotaRule coreModels.QuotaRuleHydrateObject, volumeResourceID string, location string, projectId string, token string) error {
			return nil
		}

		err := activity.HydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to hydrate volume creation")
	})

	t.Run("WhenQuotaRuleCreateFails", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRuleCreate := hydrateQuotaRuleCreate
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			hydrateQuotaRuleCreate = originalHydrateQuotaRuleCreate
		}()

		quotaRules := []*datamodel.QuotaRule{
			{
				Name: "quota-rule-1",
				BaseModel: datamodel.BaseModel{
					UUID: "quota-uuid-1",
				},
			},
		}

		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "test-callback-token", nil
		}

		hydrateQuotaRuleCreate = func(ctx context.Context, logger log.Logger, quotaRule coreModels.QuotaRuleHydrateObject, volumeResourceID string, location string, projectId string, token string) error {
			return errors.New("hydration failed")
		}

		err := activity.HydrateQuotaRulesResume(ctx, quotaRules, volumeResourceId, location, projectNumber)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to hydrate volume creation")
	})
}
