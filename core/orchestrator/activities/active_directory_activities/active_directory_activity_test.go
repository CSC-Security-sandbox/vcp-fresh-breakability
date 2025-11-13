package active_directory_activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	log "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
		origGetPasswordFromCacheOrSecretManager := hyperscaler.GetPasswordFromCacheOrSecretManager
		hyperscaler.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "password", nil
		}
		defer func() { hyperscaler.GetPasswordFromCacheOrSecretManager = origGetPasswordFromCacheOrSecretManager }()

		ad, err := activity.GetActiveDirectoryForPool(ctx, poolID)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedAD.Name, ad.Name)
		mockStorage.AssertExpectations(tt)
	})
}
