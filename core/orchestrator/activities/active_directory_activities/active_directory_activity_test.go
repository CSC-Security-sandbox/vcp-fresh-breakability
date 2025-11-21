package active_directory_activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
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

func TestActiveDirectoryActivity_GetSvmsForAd(t *testing.T) {
	t.Run("returns error when database call fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		oldAd := &models.ActiveDirectory{
			BaseModel: models.BaseModel{ID: 123},
			AdName:    "test-ad",
		}

		expectedErr := vsaerrors.New("database connection failed")
		mockStorage.On("GetSVMsUsingActiveDirectory", ctx, oldAd.ID).Return(([]*datamodel.Svm)(nil), expectedErr)

		svms, err := activity.GetSvmsForAd(ctx, oldAd)

		assert.Nil(tt, svms)
		assert.Error(tt, err)
		assert.ErrorContains(tt, err, "database connection failed")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns empty list when no SVMs found", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		oldAd := &models.ActiveDirectory{
			BaseModel: models.BaseModel{ID: 456},
			AdName:    "test-ad-2",
		}

		mockStorage.On("GetSVMsUsingActiveDirectory", ctx, oldAd.ID).Return([]*datamodel.Svm{}, nil)

		svms, err := activity.GetSvmsForAd(ctx, oldAd)

		assert.NoError(tt, err)
		assert.NotNil(tt, svms)
		assert.Empty(tt, svms)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns SVMs when found", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		oldAd := &models.ActiveDirectory{
			BaseModel: models.BaseModel{ID: 789},
			AdName:    "test-ad-3",
		}

		expectedSvms := []*datamodel.Svm{
			{BaseModel: datamodel.BaseModel{ID: 1}, Name: "svm-1"},
			{BaseModel: datamodel.BaseModel{ID: 2}, Name: "svm-2"},
		}
		mockStorage.On("GetSVMsUsingActiveDirectory", ctx, oldAd.ID).Return(expectedSvms, nil)

		svms, err := activity.GetSvmsForAd(ctx, oldAd)

		assert.NoError(tt, err)
		assert.NotNil(tt, svms)
		assert.Len(tt, svms, 2)
		assert.Equal(tt, "svm-1", svms[0].Name)
		assert.Equal(tt, "svm-2", svms[1].Name)
		mockStorage.AssertExpectations(tt)
	})
}

func TestActiveDirectoryActivity_GenerateUpdateAdCredentialsParams(t *testing.T) {
	t.Run("returns error when GetActiveDirectoryByUUID fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		oldAd := models.ActiveDirectory{
			BaseModel: models.BaseModel{UUID: "ad-uuid-1"},
		}
		params := common.UpdateActiveDirectoryParams{}

		expectedErr := vsaerrors.New("database query failed")
		mockStorage.On("GetActiveDirectoryByUUID", ctx, oldAd.UUID).Return((*datamodel.ActiveDirectory)(nil), expectedErr)

		result, err := activity.GenerateUpdateAdCredentialsParams(ctx, oldAd, params)

		assert.Nil(tt, result)
		assert.Error(tt, err)
		assert.ErrorContains(tt, err, "database query failed")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when active directory attributes missing", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		oldAd := models.ActiveDirectory{
			BaseModel: models.BaseModel{UUID: "ad-uuid-2"},
		}
		params := common.UpdateActiveDirectoryParams{}

		oldDbAd := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{UUID: "ad-uuid-2"},
			ActiveDirectoryAttributes: nil,
		}
		mockStorage.On("GetActiveDirectoryByUUID", ctx, oldAd.UUID).Return(oldDbAd, nil)

		result, err := activity.GenerateUpdateAdCredentialsParams(ctx, oldAd, params)

		assert.Nil(tt, result)
		assert.Error(tt, err)
		assert.ErrorContains(tt, err, "active directory attributes not populated")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when credential path is empty", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		oldAd := models.ActiveDirectory{
			BaseModel: models.BaseModel{UUID: "ad-uuid-3"},
		}
		params := common.UpdateActiveDirectoryParams{}

		oldDbAd := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{UUID: "ad-uuid-3"},
			CredentialPath:            "",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{},
		}
		mockStorage.On("GetActiveDirectoryByUUID", ctx, oldAd.UUID).Return(oldDbAd, nil)

		result, err := activity.GenerateUpdateAdCredentialsParams(ctx, oldAd, params)

		assert.Nil(tt, result)
		assert.Error(tt, err)
		assert.ErrorContains(tt, err, "active directory credential path is empty")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("successfully generates update params with new password", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		newPassword := "new-password-123"
		newDomain := "new-domain.com"
		oldAd := models.ActiveDirectory{
			BaseModel: models.BaseModel{UUID: "ad-uuid-5"},
			Domain:    "old-domain.com",
			DNS:       "dns1.com",
			NetBIOS:   "NETBIOS",
			Username:  "admin",
			ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
				OrganizationalUnit:   "OU=test",
				EncryptDCConnections: true,
				Site:                 "site1",
			},
		}
		params := common.UpdateActiveDirectoryParams{
			Password: &newPassword,
			Domain:   &newDomain,
		}

		aesEncryption := true
		oldDbAd := &datamodel.ActiveDirectory{
			BaseModel:      datamodel.BaseModel{UUID: "ad-uuid-5"},
			CredentialPath: "secret-path",
			Domain:         "old-domain.com",
			DNS:            "dns1.com",
			NetBIOS:        "NETBIOS",
			Username:       "admin",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AesEncryption:        aesEncryption,
				OrganizationalUnit:   "OU=test",
				EncryptDCConnections: true,
				Site:                 "site1",
			},
		}
		mockStorage.On("GetActiveDirectoryByUUID", ctx, oldAd.UUID).Return(oldDbAd, nil)

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

		result, err := activity.GenerateUpdateAdCredentialsParams(ctx, oldAd, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.NotNil(tt, result.OldCredentials)
		assert.NotNil(tt, result.NewCredentials)
		assert.Equal(tt, newDomain, result.NewCredentials.Domain)
		assert.Equal(tt, "old-domain.com", result.OldCredentials.Domain)
		mockStorage.AssertExpectations(tt)
	})
}

func TestActiveDirectoryActivity_BuildNewCredentials(t *testing.T) {
	t.Run("uses new values when all params provided", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		newDomain := "new.domain.com"
		newDNS := "dns.new.com"
		newNetBIOS := "NEWBIOS"
		newUsername := "newuser"
		newPassword := "newpass123"
		newOU := "OU=NewUnit"
		newSite := "NewSite"
		encryptDC := true
		aesEncrypt := true
		ldapSigning := true
		backupOps := []string{"user1", "user2"}
		admins := []string{"admin1"}
		secOps := []string{"secop1"}

		params := common.UpdateActiveDirectoryParams{
			Domain:               &newDomain,
			DNS:                  &newDNS,
			NetBIOS:              &newNetBIOS,
			Username:             &newUsername,
			Password:             &newPassword,
			OrganizationalUnit:   &newOU,
			Site:                 &newSite,
			EncryptDCConnections: &encryptDC,
			AesEncryption:        &aesEncrypt,
			LdapSigning:          &ldapSigning,
			BackupOperators:      backupOps,
			Administrators:       admins,
			SecurityOperators:    secOps,
		}

		oldAd := models.ActiveDirectory{
			BaseModel: models.BaseModel{UUID: "ad-uuid-6"},
			Domain:    "old.domain.com",
			DNS:       "dns.old.com",
			NetBIOS:   "OLDBIOS",
			Username:  "olduser",
			ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
				OrganizationalUnit:   "OU=OldUnit",
				Site:                 "OldSite",
				EncryptDCConnections: false,
			},
		}

		aesEncryption := true
		oldDbAd := &datamodel.ActiveDirectory{
			BaseModel:      datamodel.BaseModel{UUID: "ad-uuid-6"},
			CredentialPath: "secret-path",
			Domain:         "old.domain.com",
			DNS:            "dns.old.com",
			NetBIOS:        "OLDBIOS",
			Username:       "olduser",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AesEncryption:        aesEncryption,
				OrganizationalUnit:   "OU=OldUnit",
				Site:                 "OldSite",
				EncryptDCConnections: false,
			},
		}
		mockStorage.On("GetActiveDirectoryByUUID", ctx, oldAd.UUID).Return(oldDbAd, nil)

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

		result, err := activity.GenerateUpdateAdCredentialsParams(ctx, oldAd, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("returns error when password missing and secret retrieval fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := common.UpdateActiveDirectoryParams{
			// Password intentionally nil
		}

		oldAd := models.ActiveDirectory{
			BaseModel: models.BaseModel{UUID: "test-uuid"},
			Domain:    "domain.com",
			ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
				OrganizationalUnit: "OU=Test",
			},
		}

		oldDbAd := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{UUID: "test-uuid"},
			CredentialPath:            "secret-path",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{},
		}
		mockStorage.On("GetActiveDirectoryByUUID", ctx, oldAd.UUID).Return(oldDbAd, nil)

		// Mock GetPasswordSecret
		origGetPasswordSecret := adHelper.GetPasswordSecret
		adHelper.GetPasswordSecret = func(ctx context.Context, secretID string) (*hyperscalermodels.CustomSecret, error) {
			return nil, errors.New("password secret not properly populated")
		}
		defer func() { adHelper.GetPasswordSecret = origGetPasswordSecret }()

		result, err := activity.GenerateUpdateAdCredentialsParams(ctx, oldAd, params)

		assert.Nil(tt, result)
		assert.Error(tt, err)
		assert.ErrorContains(tt, err, "password secret not properly populated")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("constructs users map correctly", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ActiveDirectoryActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		backupOps := []string{"backup1"}
		admins := []string{"admin1", "admin2"}
		secOps := []string{"sec1"}
		password := "testpass"

		params := common.UpdateActiveDirectoryParams{
			Password:          &password,
			BackupOperators:   backupOps,
			Administrators:    admins,
			SecurityOperators: secOps,
		}

		oldAd := models.ActiveDirectory{
			BaseModel: models.BaseModel{UUID: "test-uuid-2"},
			Domain:    "domain.com",
			ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
				OrganizationalUnit: "OU=Test",
			},
		}

		aesEncryption := true
		oldDbAd := &datamodel.ActiveDirectory{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-2"},
			CredentialPath: "secret-path",
			Domain:         "domain.com",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AesEncryption: aesEncryption,
			},
		}
		mockStorage.On("GetActiveDirectoryByUUID", ctx, oldAd.UUID).Return(oldDbAd, nil)

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

		result, err := activity.GenerateUpdateAdCredentialsParams(ctx, oldAd, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.NotNil(tt, result.NewCredentials.Users)
		assert.Contains(tt, result.NewCredentials.Users, utils.ActiveDirectoryGroupBuiltInBackupOperators)
		assert.Contains(tt, result.NewCredentials.Users, utils.ActiveDirectoryGroupBuiltInAdministrators)
		assert.Contains(tt, result.NewCredentials.Users, utils.ActiveDirectorySeSecurityPrivilege)
		assert.Equal(tt, backupOps, result.NewCredentials.Users[utils.ActiveDirectoryGroupBuiltInBackupOperators])
		mockStorage.AssertExpectations(tt)
	})
}

func TestValidateAndGetVsaActiveDirectory(t *testing.T) {
	t.Run("returns error when attributes are nil", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		ad := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{UUID: "test-uuid"},
			ActiveDirectoryAttributes: nil,
		}

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, int64(1)).Return(ad, nil)
		activity := ActiveDirectoryActivity{SE: mockStorage}

		result, err := activity.GetActiveDirectoryForPool(ctx, 1)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when credential path is empty", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		ad := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{UUID: "test-uuid-2"},
			CredentialPath:            "",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{},
		}

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, int64(1)).Return(ad, nil)
		activity := ActiveDirectoryActivity{SE: mockStorage}

		result, err := activity.GetActiveDirectoryForPool(ctx, 1)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("returns error when secret retrieval fails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		ad := &datamodel.ActiveDirectory{
			BaseModel:                 datamodel.BaseModel{UUID: "test-uuid-3"},
			CredentialPath:            "secret-path",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{},
		}

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("GetActiveDirectoryForPoolByPoolID", ctx, int64(1)).Return(ad, nil)
		activity := ActiveDirectoryActivity{SE: mockStorage}

		origGetPasswordSecret := getPasswordSecret
		getPasswordSecret = func(ctx context.Context, secretID string) (*hyperscalermodels.CustomSecret, error) {
			return nil, errors.New("secret retrieval failed")
		}
		defer func() { getPasswordSecret = origGetPasswordSecret }()

		result, err := activity.GetActiveDirectoryForPool(ctx, 1)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockStorage.AssertExpectations(tt)
	})
}
