package active_directory_activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	adHelper "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestActiveDirectoryActivity_GetActiveDirectoryForPool(t *testing.T) {
	t.Run("returns error when datastore call fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolID := int64(7)

		expectedErr := errors.New("db failure")
		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, poolID).Return((*datamodel.ActiveDirectory)(nil), expectedErr)

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.Nil(tt, ad)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when active directory missing", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolID := int64(8)

		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, poolID).Return((*datamodel.ActiveDirectory)(nil), nil)

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.Nil(tt, ad)
		assert.EqualError(tt, err, "active directory not found for the pool")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns active directory when present", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolID := int64(9)
		adAttribute := &datamodel.ActiveDirectoryAttributes{AesEncryption: true}
		expectedDBAD := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{ID: 11},
			AdName:                    "corp-ad",
			CredentialPath:            "secretPath",
			ActiveDirectoryAttributes: adAttribute,
		}
		expectedAD := &vsa.ActiveDirectory{AdName: "corp-ad"}
		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, poolID).Return(expectedDBAD, nil)

		// Mock GetPasswordSecret
		origGetPasswordSecret := adHelper.GetPasswordSecret
		adHelper.GetPasswordSecret = func(ctx context.Context, secretID string) (*hyperscalermodels.CustomSecret, error) {
			return &hyperscalermodels.CustomSecret{
				SecretVersion: &hyperscalermodels.CustomSecretVersion{
					Value: "decrypted-password",
				},
			}, nil
		}
		defer func() { adHelper.GetPasswordSecret = origGetPasswordSecret }()

		// Mock EncryptPassword
		origEncryptPassword := utils.EncryptPassword
		utils.EncryptPassword = func(password log.Secret) (*string, error) {
			encrypted := "encrypted-password"
			return &encrypted, nil
		}
		defer func() { utils.EncryptPassword = origEncryptPassword }()

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedAD.Name, ad.Name)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when credential path is empty", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolID := int64(10)

		adAttribute := &datamodel.ActiveDirectoryAttributes{AesEncryption: true}
		expectedDBAD := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{ID: 12},
			AdName:                    "corp-ad",
			CredentialPath:            "", // Empty credential path
			ActiveDirectoryAttributes: adAttribute,
		}

		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, poolID).Return(expectedDBAD, nil)

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.Nil(tt, ad)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "active directory credential path is empty")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when GetPasswordSecret fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolID := int64(11)

		adAttribute := &datamodel.ActiveDirectoryAttributes{AesEncryption: true}
		expectedDBAD := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{ID: 13},
			AdName:                    "corp-ad",
			CredentialPath:            "secretPath",
			ActiveDirectoryAttributes: adAttribute,
		}

		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, poolID).Return(expectedDBAD, nil)

		// Mock GetPasswordSecret to fail
		origGetPasswordSecret := adHelper.GetPasswordSecret
		adHelper.GetPasswordSecret = func(ctx context.Context, secretID string) (*hyperscalermodels.CustomSecret, error) {
			return nil, errors.New("failed to get secret")
		}
		defer func() { adHelper.GetPasswordSecret = origGetPasswordSecret }()

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.Nil(tt, ad)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when password secret is nil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolID := int64(12)

		adAttribute := &datamodel.ActiveDirectoryAttributes{AesEncryption: true}
		expectedDBAD := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{ID: 14},
			AdName:                    "corp-ad",
			CredentialPath:            "secretPath",
			ActiveDirectoryAttributes: adAttribute,
		}

		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, poolID).Return(expectedDBAD, nil)

		// Mock GetPasswordSecret to return nil
		origGetPasswordSecret := adHelper.GetPasswordSecret
		adHelper.GetPasswordSecret = func(ctx context.Context, secretID string) (*hyperscalermodels.CustomSecret, error) {
			return nil, nil
		}
		defer func() { adHelper.GetPasswordSecret = origGetPasswordSecret }()

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.Nil(tt, ad)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "password secret fetch unsuccessful")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when secret version is nil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolID := int64(13)

		adAttribute := &datamodel.ActiveDirectoryAttributes{AesEncryption: true}
		expectedDBAD := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{ID: 15},
			AdName:                    "corp-ad",
			CredentialPath:            "secretPath",
			ActiveDirectoryAttributes: adAttribute,
		}

		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, poolID).Return(expectedDBAD, nil)

		// Mock GetPasswordSecret to return secret with nil version
		origGetPasswordSecret := adHelper.GetPasswordSecret
		adHelper.GetPasswordSecret = func(ctx context.Context, secretID string) (*hyperscalermodels.CustomSecret, error) {
			return &hyperscalermodels.CustomSecret{
				SecretVersion: nil,
			}, nil
		}
		defer func() { adHelper.GetPasswordSecret = origGetPasswordSecret }()

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.Nil(tt, ad)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "password secret fetch unsuccessful")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when password encryption fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolID := int64(14)

		adAttribute := &datamodel.ActiveDirectoryAttributes{AesEncryption: true}
		expectedDBAD := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{ID: 16},
			AdName:                    "corp-ad",
			CredentialPath:            "secretPath",
			ActiveDirectoryAttributes: adAttribute,
		}

		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, poolID).Return(expectedDBAD, nil)

		// Mock GetPasswordSecret
		origGetPasswordSecret := adHelper.GetPasswordSecret
		adHelper.GetPasswordSecret = func(ctx context.Context, secretID string) (*hyperscalermodels.CustomSecret, error) {
			return &hyperscalermodels.CustomSecret{
				SecretVersion: &hyperscalermodels.CustomSecretVersion{
					Value: "decrypted-password",
				},
			}, nil
		}
		defer func() { adHelper.GetPasswordSecret = origGetPasswordSecret }()

		// Mock EncryptPassword to fail
		origEncryptPassword := utils.EncryptPassword
		utils.EncryptPassword = func(password log.Secret) (*string, error) {
			return nil, errors.New("encryption failed")
		}
		defer func() { utils.EncryptPassword = origEncryptPassword }()

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.Nil(tt, ad)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to encrypt AD password")
		mockStorage.AssertExpectations(tt)
	})
}
