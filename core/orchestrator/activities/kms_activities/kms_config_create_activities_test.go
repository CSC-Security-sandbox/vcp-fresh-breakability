package kms_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateKmsConfigSDEActivity(t *testing.T) {
	t.Run("CreateKmsConfigSDEActivityReturnsKmsConfigOnSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)
		// Define mock response
		kfp := "kfp"
		mockResponse := &kms_configurations.V1betaCreateKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(true),
				Response: models.KmsConfigV1beta{UUID: "test", KeyFullPath: &kfp},
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		activity := &KmsConfigActivity{SE: mockSE}

		_, err := activity.CreateKmsConfigSDEActivity(ctx, params)
		if err != nil {
			t.Fatal("expected no error, got error:", err)
		}
	})
	t.Run("CreateKmsConfigSDEActivityReturnsErrorOnCreateFailure", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, errors.New("create error"))
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.CreateKmsConfigSDEActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("CreateKmsConfigSDEActivityReturnsErrorOnNilPayload", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.CreateKmsConfigSDEActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
