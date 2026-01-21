package coreapi

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestSubmitExpertModeVolumeOperation(t *testing.T) {
	originalCreateClient := createCoreAPIClient
	defer func() { createCoreAPIClient = originalCreateClient }()

	t.Run("SuccessCases", func(tt *testing.T) {
		tt.Run("VolumeCreatedSuccessfully", func(ttt *testing.T) {
			mockInvoker := coreapi.NewMockInvoker(ttt)
			mockLogger := &log.MockLogger{}

			mockLogger.On("InfoContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
			mockLogger.On("ErrorContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

			createCoreAPIClient = func(host, jwtToken string, logger log.Logger) *coreapi.CoreAPIClient {
				return &coreapi.CoreAPIClient{
					Invoker: mockInvoker,
				}
			}

			request := &coreapi.ExpertModeVolumeV1{
				ProjectNumber: "12345",
				PoolUUID:      "pool-uuid-123",
				VolumeName:    "test-volume",
				Action:        coreapi.ExpertModeVolumeV1ActionCreate,
				Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
				SizeInBytes:   1073741824,
			}

			mockInvoker.On("V1ExpertModeVolume", mock.Anything, request, mock.MatchedBy(func(params coreapi.V1ExpertModeVolumeParams) bool {
				return params.XCorrelationID.IsSet() && params.XCorrelationID.Value == "corr-id-123"
			})).Return(&coreapi.V1ExpertModeVolumeOK{}, nil)

			ctx := context.WithValue(context.Background(), middleware.CorrelationContextKey, "corr-id-123")
			err := SubmitExpertModeVolumeOperation(ctx, request, "test-jwt-token", mockLogger)

			assert.NoError(ttt, err)
			mockInvoker.AssertExpectations(ttt)
		})

		tt.Run("VolumeUpdatedSuccessfully", func(ttt *testing.T) {
			mockInvoker := coreapi.NewMockInvoker(ttt)
			mockLogger := &log.MockLogger{}

			mockLogger.On("InfoContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
			mockLogger.On("ErrorContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

			createCoreAPIClient = func(host, jwtToken string, logger log.Logger) *coreapi.CoreAPIClient {
				return &coreapi.CoreAPIClient{
					Invoker: mockInvoker,
				}
			}

			request := &coreapi.ExpertModeVolumeV1{
				ProjectNumber: "12345",
				PoolUUID:      "pool-uuid-123",
				VolumeUUID:    coreapi.NewOptString("volume-uuid-456"),
				VolumeName:    "test-volume",
				Action:        coreapi.ExpertModeVolumeV1ActionUpdate,
				Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
				SizeInBytes:   2147483648,
			}

			mockInvoker.On("V1ExpertModeVolume", mock.Anything, request, mock.MatchedBy(func(params coreapi.V1ExpertModeVolumeParams) bool {
				return params.XCorrelationID.IsSet() && params.XCorrelationID.Value == "corr-id-456"
			})).Return(&coreapi.V1ExpertModeVolumeOK{}, nil)

			ctx := context.WithValue(context.Background(), middleware.CorrelationContextKey, "corr-id-456")
			err := SubmitExpertModeVolumeOperation(ctx, request, "test-jwt-token", mockLogger)

			assert.NoError(ttt, err)
			mockInvoker.AssertExpectations(ttt)
		})

		tt.Run("VolumeDeletedSuccessfully", func(ttt *testing.T) {
			mockInvoker := coreapi.NewMockInvoker(ttt)
			mockLogger := &log.MockLogger{}

			mockLogger.On("InfoContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
			mockLogger.On("ErrorContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

			createCoreAPIClient = func(host, jwtToken string, logger log.Logger) *coreapi.CoreAPIClient {
				return &coreapi.CoreAPIClient{
					Invoker: mockInvoker,
				}
			}

			request := &coreapi.ExpertModeVolumeV1{
				ProjectNumber: "12345",
				PoolUUID:      "pool-uuid-123",
				VolumeUUID:    coreapi.NewOptString("volume-uuid-789"),
				Action:        coreapi.ExpertModeVolumeV1ActionDelete,
			}

			mockInvoker.On("V1ExpertModeVolume", mock.Anything, request, mock.Anything).Return(&coreapi.V1ExpertModeVolumeOK{}, nil)

			ctx := context.Background()
			err := SubmitExpertModeVolumeOperation(ctx, request, "test-jwt-token", mockLogger)

			assert.NoError(ttt, err)
			mockInvoker.AssertExpectations(ttt)
		})

		tt.Run("NoCorrelationIDInContext", func(ttt *testing.T) {
			mockInvoker := coreapi.NewMockInvoker(ttt)
			mockLogger := &log.MockLogger{}

			mockLogger.On("InfoContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
			mockLogger.On("ErrorContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

			createCoreAPIClient = func(host, jwtToken string, logger log.Logger) *coreapi.CoreAPIClient {
				return &coreapi.CoreAPIClient{
					Invoker: mockInvoker,
				}
			}

			request := &coreapi.ExpertModeVolumeV1{
				ProjectNumber: "12345",
				PoolUUID:      "pool-uuid-123",
				VolumeName:    "test-volume",
				Action:        coreapi.ExpertModeVolumeV1ActionCreate,
				Style:         coreapi.ExpertModeVolumeV1StyleFlexvol,
				SizeInBytes:   1073741824,
			}

			mockInvoker.On("V1ExpertModeVolume", mock.Anything, request, mock.MatchedBy(func(params coreapi.V1ExpertModeVolumeParams) bool {
				return !params.XCorrelationID.IsSet() || params.XCorrelationID.Value == ""
			})).Return(&coreapi.V1ExpertModeVolumeOK{}, nil)

			ctx := context.Background()
			err := SubmitExpertModeVolumeOperation(ctx, request, "test-jwt-token", mockLogger)

			assert.NoError(ttt, err)
			mockInvoker.AssertExpectations(ttt)
		})
	})

	t.Run("ErrorCases", func(tt *testing.T) {
		tt.Run("BadRequest", func(ttt *testing.T) {
			mockInvoker := coreapi.NewMockInvoker(ttt)
			mockLogger := &log.MockLogger{}

			mockLogger.On("InfoContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
			mockLogger.On("ErrorContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

			createCoreAPIClient = func(host, jwtToken string, logger log.Logger) *coreapi.CoreAPIClient {
				return &coreapi.CoreAPIClient{
					Invoker: mockInvoker,
				}
			}

			request := &coreapi.ExpertModeVolumeV1{
				ProjectNumber: "12345",
				PoolUUID:      "pool-uuid-123",
				VolumeName:    "",
				Action:        coreapi.ExpertModeVolumeV1ActionCreate,
			}

			mockInvoker.On("V1ExpertModeVolume", mock.Anything, request, mock.Anything).Return(&coreapi.V1ExpertModeVolumeBadRequest{
				Code:    400,
				Message: "Volume name is required",
			}, nil)

			ctx := context.Background()
			err := SubmitExpertModeVolumeOperation(ctx, request, "test-jwt-token", mockLogger)

			assert.Error(ttt, err)
			assert.Contains(ttt, err.Error(), "bad request: Volume name is required")
			mockInvoker.AssertExpectations(ttt)
		})

		tt.Run("Conflict", func(ttt *testing.T) {
			mockInvoker := coreapi.NewMockInvoker(ttt)
			mockLogger := &log.MockLogger{}

			mockLogger.On("InfoContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
			mockLogger.On("ErrorContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

			createCoreAPIClient = func(host, jwtToken string, logger log.Logger) *coreapi.CoreAPIClient {
				return &coreapi.CoreAPIClient{
					Invoker: mockInvoker,
				}
			}

			request := &coreapi.ExpertModeVolumeV1{
				ProjectNumber: "12345",
				PoolUUID:      "pool-uuid-123",
				VolumeName:    "existing-volume",
				Action:        coreapi.ExpertModeVolumeV1ActionCreate,
			}

			mockInvoker.On("V1ExpertModeVolume", mock.Anything, request, mock.Anything).Return(&coreapi.V1ExpertModeVolumeConflict{
				Code:    409,
				Message: "Volume already exists",
			}, nil)

			ctx := context.Background()
			err := SubmitExpertModeVolumeOperation(ctx, request, "test-jwt-token", mockLogger)

			assert.Error(ttt, err)
			assert.Contains(ttt, err.Error(), "conflict: Volume already exists")
			mockInvoker.AssertExpectations(ttt)
		})
	})
}
