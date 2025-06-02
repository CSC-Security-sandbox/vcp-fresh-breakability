package kms_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestGetKmsConfigSDEActivity(t *testing.T) {
	t.Run("DescribeKmsConfigurationActivityReturnsKmsConfigOnSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:       datamodel.BaseModel{UUID: "uuid"},
			KmsAttributes:   &datamodel.KmsAttributes{SdeKmsConfigUUID: "external-uuid"},
			KeyRingLocation: "location",
			Account:         &datamodel.Account{Name: "project-number"},
		}
		mockClient := kms_configurations.NewMockClientService(t)
		keyFullPath := "key-full-path"
		resourceID := "resource-id"
		uuid := "external-uuid"
		serviceAccountEmail := "svc@account"
		instructions := "instructions"
		mockResponse := &kms_configurations.V1betaDescribeKmsConfigurationOK{
			Payload: &models.KmsConfigV1beta{
				UUID:                uuid,
				KeyFullPath:         &keyFullPath,
				ResourceID:          &resourceID,
				ServiceAccountEmail: serviceAccountEmail,
				Instructions:        instructions,
			},
		}
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		mockSE.On("UpdateKmsConfigAttributes", mock.Anything, "uuid", mock.Anything).Return(kmsConfig, nil)

		activity := &KmsConfigActivity{SE: mockSE}
		result, err := activity.DescribeKmsConfigurationActivity(ctx, kmsConfig)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result != kmsConfig {
			t.Errorf("expected returned config to match input")
		}
	})
	t.Run("DescribeKmsConfigurationActivityReturnsErrorOnDescribeFailure", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "uuid"},
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "external-uuid"},
			Account:       &datamodel.Account{},
		}
		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, errors.New("describe error"))
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.DescribeKmsConfigurationActivity(ctx, kmsConfig)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("DescribeKmsConfigurationActivityReturnsErrorOnNilPayload", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "uuid"},
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "external-uuid"},
			Account:       &datamodel.Account{},
		}
		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(&kms_configurations.V1betaDescribeKmsConfigurationOK{Payload: nil}, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.DescribeKmsConfigurationActivity(ctx, kmsConfig)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("DescribeKmsConfigurationActivityReturnsErrorOnUpdateKmsConfigAttributesFailure", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "uuid"},
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "external-uuid"},
			Account:       &datamodel.Account{},
		}
		mockClient := kms_configurations.NewMockClientService(t)
		keyFullPath := "key-full-path"
		resourceID := "resource-id"
		uuid := "external-uuid"
		serviceAccountEmail := "svc@account"
		instructions := "instructions"
		mockResponse := &kms_configurations.V1betaDescribeKmsConfigurationOK{
			Payload: &models.KmsConfigV1beta{
				UUID:                uuid,
				KeyFullPath:         &keyFullPath,
				ResourceID:          &resourceID,
				ServiceAccountEmail: serviceAccountEmail,
				Instructions:        instructions,
			},
		}
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		mockSE.On("UpdateKmsConfigAttributes", mock.Anything, "uuid", mock.Anything).Return(nil, errors.New("update attributes error"))
		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.DescribeKmsConfigurationActivity(ctx, kmsConfig)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
