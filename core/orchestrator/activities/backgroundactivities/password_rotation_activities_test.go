package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

// Test helper functions
func createTestPoolForPassword() *datamodel.Pool {
	return &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
			ID:   1,
		},
		Name:           "test-pool",
		State:          "READY",
		DeploymentName: "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType:      env.USERNAME_PWD_SEC_MGR,
			SecretID:      "test-secret-id",
			SecretIDNew:   "test-secret-id-new",
			CertificateID: "test-cert-id",
		},
	}
}

func createTestPoolViewForPassword() *datamodel.PoolView {
	return &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
				ID:   1,
			},
			Name:           "test-pool",
			State:          "READY",
			DeploymentName: "test-deployment",
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType:      env.USERNAME_PWD_SEC_MGR,
				SecretID:      "test-secret-id",
				SecretIDNew:   "test-secret-id-new",
				CertificateID: "test-cert-id",
			},
		},
	}
}

// Tests for GenerateNewPassword
func TestRotateVcpToVsaCertificateActivity_GenerateNewPassword(t *testing.T) {
	ctx := context.Background()

	t.Run("GenerateNewPassword_PoolNotFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "non-existent-pool"

		// Mock database call to return empty list
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil)

		// Execute
		result, err := activity.GenerateNewPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "pool non-existent-pool not found")
		mockSE.AssertExpectations(tt)
	})

	t.Run("GenerateNewPassword_DatabaseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		dbError := errors.New("database connection failed")

		// Mock database call to return error
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, dbError)

		// Execute
		result, err := activity.GenerateNewPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})

	t.Run("GenerateNewPassword_PasswordGenerationError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Mock password generation to fail
		originalGeneratePassword := utils.GenerateStrongPassword
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "", errors.New("password generation failed")
		}
		defer func() { utils.GenerateStrongPassword = originalGeneratePassword }()

		// Execute
		result, err := activity.GenerateNewPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		// The error is wrapped in VCPError, so check for the generic message
		assert.Contains(tt, err.Error(), "Failed to rotate password for VSA cluster")
		mockSE.AssertExpectations(tt)
	})
}

// Tests for TestPasswordConnectivity
func TestRotateVcpToVsaCertificateActivity_TestPasswordConnectivity(t *testing.T) {
	ctx := context.Background()

	t.Run("TestPasswordConnectivity_PoolNotFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "non-existent-pool"
		password := "test-password"

		// Mock database call to return empty list
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil)

		// Execute
		err := activity.TestPasswordConnectivity(ctx, poolUUID, password)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "pool non-existent-pool not found")
		mockSE.AssertExpectations(tt)
	})

	t.Run("TestPasswordConnectivity_DatabaseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		password := "test-password"

		// Mock database call to return error - covers lines 113-114
		dbError := errors.New("database connection failed")
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, dbError)

		// Execute
		err := activity.TestPasswordConnectivity(ctx, poolUUID, password)

		// Assert
		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("TestPasswordConnectivity_Success", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		password := "test-password"
		poolView := createTestPoolViewForPassword()

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Mock testPasswordConnectivity to succeed - covers line 131
		activity.testPasswordConnectivityFunc = func(ctx context.Context, p *datamodel.Pool, pwd string) error {
			return nil
		}

		// Execute
		err := activity.TestPasswordConnectivity(ctx, poolUUID, password)

		// Assert - should succeed (line 131)
		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

// Tests for UpdateVSAPassword
func TestRotateVcpToVsaCertificateActivity_UpdateVSAPassword(t *testing.T) {
	ctx := context.Background()

	t.Run("UpdateVSAPassword_NoSecretIDNew", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretIDNew = ""

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.UpdateVSAPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "secret_id_new is empty for pool test-pool-uuid")
		mockSE.AssertExpectations(tt)
	})

	t.Run("UpdateVSAPassword_PoolNotFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "non-existent-pool"

		// Mock database call to return empty list
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil)

		// Execute
		err := activity.UpdateVSAPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "pool non-existent-pool not found")
		mockSE.AssertExpectations(tt)
	})
}

// Tests for SwapSecretIDs
func TestRotateVcpToVsaCertificateActivity_SwapSecretIDs(t *testing.T) {
	ctx := context.Background()

	t.Run("SwapSecretIDs_PoolNotFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "non-existent-pool"

		// Mock database call to return empty list
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil)

		// Execute
		err := activity.SwapSecretIDs(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "pool non-existent-pool not found")
		mockSE.AssertExpectations(tt)
	})

	t.Run("SwapSecretIDs_SuccessWithVerification", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretID = "old-secret-id"
		poolView.Pool.PoolCredentials.SecretIDNew = "new-secret-id"

		// Create updated pool view after swap
		updatedPoolView := createTestPoolViewForPassword()
		updatedPoolView.Pool.PoolCredentials.SecretID = "new-secret-id"
		updatedPoolView.Pool.PoolCredentials.SecretIDNew = "old-secret-id"

		// Mock database calls:
		// 1. First call in SwapSecretIDs to get pool (line 206)
		// 2. Call in swapSecretIDs to get current pool (line 2109)
		// 3. UpdatePoolFields call
		// 4. Final call for verification (line 238)
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil).Once()
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil).Once()
		mockSE.On("UpdatePoolFields", ctx, poolUUID, mock.Anything).Return(nil).Once()
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{updatedPoolView}, nil).Once()

		// Execute
		err := activity.SwapSecretIDs(ctx, poolUUID)

		// Assert
		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("SwapSecretIDs_VerificationError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretID = "old-secret-id"
		poolView.Pool.PoolCredentials.SecretIDNew = "new-secret-id"

		// Mock database calls:
		// 1. First call in SwapSecretIDs to get pool (line 206)
		// 2. Call in swapSecretIDs to get current pool (line 2109)
		// 3. UpdatePoolFields call
		// 4. Final call for verification fails (line 238)
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil).Once()
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil).Once()
		mockSE.On("UpdatePoolFields", ctx, poolUUID, mock.Anything).Return(nil).Once()
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, errors.New("verification error")).Once()

		// Execute - should still succeed but log warning
		err := activity.SwapSecretIDs(ctx, poolUUID)

		// Assert - should not fail, just log warning
		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

// Tests for UpdateCacheWithNewSecret
func TestRotateVcpToVsaCertificateActivity_UpdateCacheWithNewSecret(t *testing.T) {
	ctx := context.Background()

	t.Run("UpdateCacheWithNewSecret_NoSecretID", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretID = ""

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.UpdateCacheWithNewSecret(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "secret_id is empty for pool test-pool-uuid")
		mockSE.AssertExpectations(tt)
	})

	t.Run("UpdateCacheWithNewSecret_PoolNotFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "non-existent-pool"

		// Mock database call to return empty list
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil)

		// Execute
		err := activity.UpdateCacheWithNewSecret(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "pool non-existent-pool not found")
		mockSE.AssertExpectations(tt)
	})

	t.Run("UpdateCacheWithNewSecret_PasswordRetrievalError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretID = "test-secret-id"

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Mock password retrieval to fail (lines 286-290)
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", errors.New("failed to retrieve password")
		}
		defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()

		// Execute
		err := activity.UpdateCacheWithNewSecret(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		// The error is wrapped in VCPError with ErrGCPResourceFetchError
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})

	t.Run("UpdateCacheWithNewSecret_Success", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretID = "test-secret-id"

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Mock password retrieval to succeed (lines 295-296, 298-300)
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "test-password", nil
		}
		defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()

		// Execute
		err := activity.UpdateCacheWithNewSecret(ctx, poolUUID)

		// Assert
		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

// Tests for GetOldSecretID
func TestRotateVcpToVsaCertificateActivity_GetOldSecretID(t *testing.T) {
	ctx := context.Background()

	t.Run("GetOldSecretID_Success", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		result, err := activity.GetOldSecretID(ctx, poolUUID)

		// Assert
		assert.NoError(tt, err)
		assert.Equal(tt, "test-secret-id-new", result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetOldSecretID_PoolNotFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "non-existent-pool"

		// Mock database call to return empty list
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil)

		// Execute
		result, err := activity.GetOldSecretID(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Empty(tt, result)
		assert.Contains(tt, err.Error(), "pool non-existent-pool not found")
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetOldSecretID_NilPoolCredentials", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials = nil

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		result, err := activity.GetOldSecretID(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Empty(tt, result)
		assert.Contains(tt, err.Error(), "pool credentials are nil for pool test-pool-uuid")
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetOldSecretID_DatabaseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		dbError := errors.New("database connection failed")

		// Mock database call to return error (line 113-114)
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, dbError)

		// Execute
		result, err := activity.GetOldSecretID(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Empty(tt, result)
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})
}

// Tests for RemoveOldSecretFromCache
func TestRotateVcpToVsaCertificateActivity_RemoveOldSecretFromCache(t *testing.T) {
	ctx := context.Background()

	t.Run("RemoveOldSecretFromCache_Success", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		oldSecretID := "old-secret-id"

		// Execute
		err := activity.RemoveOldSecretFromCache(ctx, poolUUID, oldSecretID)

		// Assert
		assert.NoError(tt, err)
	})
}

// Tests for ValidateNewPasswordConnectivity
func TestRotateVcpToVsaCertificateActivity_ValidateNewPasswordConnectivity(t *testing.T) {
	ctx := context.Background()

	t.Run("ValidateNewPasswordConnectivity_NoSecretIDNew", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretIDNew = ""

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.ValidateNewPasswordConnectivity(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "secret_id_new is empty for pool test-pool-uuid")
		mockSE.AssertExpectations(tt)
	})

	t.Run("ValidateNewPasswordConnectivity_PoolNotFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "non-existent-pool"

		// Mock database call to return empty list
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil)

		// Execute
		err := activity.ValidateNewPasswordConnectivity(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "pool non-existent-pool not found")
		mockSE.AssertExpectations(tt)
	})

	t.Run("ValidateNewPasswordConnectivity_DatabaseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		dbError := errors.New("database connection failed")

		// Mock database call to return error
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, dbError)

		// Execute
		err := activity.ValidateNewPasswordConnectivity(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})

	t.Run("ValidateNewPasswordConnectivity_NilCredentials", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials = nil

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.ValidateNewPasswordConnectivity(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ValidateNewPasswordConnectivity_PasswordRetrievalError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretIDNew = "test-secret-id-new"

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Mock password retrieval to fail (lines 401-405)
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", errors.New("failed to retrieve password")
		}
		defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()

		// Execute
		err := activity.ValidateNewPasswordConnectivity(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		// The error is wrapped in VCPError with ErrGCPResourceFetchError
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})

	t.Run("ValidateNewPasswordConnectivity_ConnectivityTestError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretIDNew = "test-secret-id-new"

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Mock password retrieval to succeed
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "test-password", nil
		}
		defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()

		// Mock connectivity test to fail (lines 409, 411-414)
		originalTestConnectivity := activity.testPasswordConnectivityFunc
		activity.testPasswordConnectivityFunc = func(ctx context.Context, pool *datamodel.Pool, password string) error {
			return errors.New("connectivity test failed")
		}
		defer func() { activity.testPasswordConnectivityFunc = originalTestConnectivity }()

		// Execute
		err := activity.ValidateNewPasswordConnectivity(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "connectivity test failed")
		mockSE.AssertExpectations(tt)
	})

	t.Run("ValidateNewPasswordConnectivity_Success", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretIDNew = "test-secret-id-new"

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Mock password retrieval to succeed
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "test-password", nil
		}
		defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()

		// Mock connectivity test to succeed (line 417-418)
		originalTestConnectivity := activity.testPasswordConnectivityFunc
		activity.testPasswordConnectivityFunc = func(ctx context.Context, pool *datamodel.Pool, password string) error {
			return nil
		}
		defer func() { activity.testPasswordConnectivityFunc = originalTestConnectivity }()

		// Execute
		err := activity.ValidateNewPasswordConnectivity(ctx, poolUUID)

		// Assert
		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

// Tests for ListPoolsWithPasswordAuth
func TestRotateVcpToVsaCertificateActivity_ListPoolsWithPasswordAuth(t *testing.T) {
	ctx := context.Background()

	t.Run("ListPoolsWithPasswordAuth_Success", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolView1 := createTestPoolViewForPassword()
		poolView1.Pool.UUID = "pool-1"
		poolView1.Pool.PoolCredentials.AuthType = env.USERNAME_PWD_SEC_MGR
		poolView2 := createTestPoolViewForPassword()
		poolView2.Pool.UUID = "pool-2"
		poolView2.Pool.PoolCredentials.AuthType = env.USERNAME_PWD_SEC_MGR

		// Mock database calls
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView1, poolView2}, nil).Twice()

		// Execute
		result, err := activity.ListPoolsWithPasswordAuth(ctx)

		// Assert
		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
		assert.Equal(tt, "pool-1", result[0].UUID)
		assert.Equal(tt, "pool-2", result[1].UUID)
		assert.Equal(tt, env.USERNAME_PWD_SEC_MGR, result[0].PoolCredentials.AuthType)
		assert.Equal(tt, env.USERNAME_PWD_SEC_MGR, result[1].PoolCredentials.AuthType)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsWithPasswordAuth_EmptyResult", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		// Mock database calls to return empty lists
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil).Twice()

		// Execute
		result, err := activity.ListPoolsWithPasswordAuth(ctx)

		// Assert
		assert.NoError(tt, err)
		assert.Empty(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsWithPasswordAuth_DatabaseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		dbError := errors.New("database connection failed")

		// Mock database call to return error
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, dbError)

		// Execute
		result, err := activity.ListPoolsWithPasswordAuth(ctx)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsWithPasswordAuth_PoolWithNilCredentials", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolView1 := createTestPoolViewForPassword()
		poolView1.Pool.UUID = "pool-1"
		poolView1.Pool.PoolCredentials.AuthType = env.USERNAME_PWD_SEC_MGR
		poolView2 := createTestPoolViewForPassword()
		poolView2.Pool.UUID = "pool-2"
		poolView2.Pool.PoolCredentials = nil // Nil credentials to test line 449

		// Mock database calls - first call for all pools, second for filtered pools
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView1, poolView2}, nil).Once()
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView1}, nil).Once()

		// Execute
		result, err := activity.ListPoolsWithPasswordAuth(ctx)

		// Assert
		assert.NoError(tt, err)
		assert.Len(tt, result, 1) // Only pool-1 should be returned (pool-2 has nil credentials)
		assert.Equal(tt, "pool-1", result[0].UUID)
		mockSE.AssertExpectations(tt)
	})
}

// Basic integration test
func TestRotateVcpToVsaCertificateActivity_PasswordRotationBasicIntegration(t *testing.T) {
	ctx := context.Background()

	t.Run("BasicPasswordWorkflowSteps", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()

		// Mock database calls
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil).Maybe()

		// Test basic workflow steps that don't require complex dependencies
		
		// Step 1: List pools with password auth (should work)
		pools, err := activity.ListPoolsWithPasswordAuth(ctx)
		assert.NoError(tt, err)
		assert.NotNil(tt, pools)

		// Step 2: Get old secret ID (should work)
		oldSecretID, err := activity.GetOldSecretID(ctx, poolUUID)
		assert.NoError(tt, err)
		assert.Equal(tt, "test-secret-id-new", oldSecretID)

		// Step 3: Remove old secret from cache (should work)
		err = activity.RemoveOldSecretFromCache(ctx, poolUUID, oldSecretID)
		assert.NoError(tt, err)

		// Step 4: Generate new password (should work as it only needs database access)
		passwordResponse, err := activity.GenerateNewPassword(ctx, poolUUID)
		assert.NoError(tt, err) // This should work as it only needs database access
		assert.NotNil(tt, passwordResponse)
		assert.NotEmpty(tt, passwordResponse.NewPassword)
		assert.NotEmpty(tt, passwordResponse.NewSecretID)

		mockSE.AssertExpectations(tt)
	})
}

// Additional tests for better coverage
func TestRotateVcpToVsaCertificateActivity_ListPoolsWithPasswordAuth_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("ListPoolsWithPasswordAuth_MixedAuthTypes", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		// Create pools with different auth types
		poolView1 := createTestPoolViewForPassword()
		poolView1.Pool.UUID = "pool-1"
		poolView1.Pool.PoolCredentials.AuthType = env.USERNAME_PWD_SEC_MGR

		poolView2 := createTestPoolViewForPassword()
		poolView2.Pool.UUID = "pool-2"
		poolView2.Pool.PoolCredentials.AuthType = env.USER_CERTIFICATE // Should be filtered out

		poolView3 := createTestPoolViewForPassword()
		poolView3.Pool.UUID = "pool-3"
		poolView3.Pool.PoolCredentials.AuthType = env.USERNAME_PWD // Should be filtered out

		// Mock database calls - first call for all pools, second for filtered
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView1, poolView2, poolView3}, nil).Once()
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView1}, nil).Once()

		// Execute
		result, err := activity.ListPoolsWithPasswordAuth(ctx)

		// Assert - should only return pools with USERNAME_PWD_SEC_MGR
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "pool-1", result[0].UUID)
		assert.Equal(tt, env.USERNAME_PWD_SEC_MGR, result[0].PoolCredentials.AuthType)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsWithPasswordAuth_PoolsInDeletedState", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		// Create pool in DELETED state
		poolView := createTestPoolViewForPassword()
		poolView.Pool.UUID = "pool-deleted"
		poolView.Pool.State = "DELETED"
		poolView.Pool.PoolCredentials.AuthType = env.USERNAME_PWD_SEC_MGR

		// Mock database calls - filter should exclude DELETED pools
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{}, nil).Twice()

		// Execute
		result, err := activity.ListPoolsWithPasswordAuth(ctx)

		// Assert - DELETED pools should be filtered out
		assert.NoError(tt, err)
		assert.Empty(tt, result)
		mockSE.AssertExpectations(tt)
	})
}

func TestRotateVcpToVsaCertificateActivity_GenerateNewPassword_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("GenerateNewPassword_PoolInCreatingState", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.UUID = poolUUID
		poolView.Pool.State = "CREATING"

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		result, err := activity.GenerateNewPassword(ctx, poolUUID)

		// Assert - should still work even if pool is in CREATING state
		// (The method doesn't check state, it just generates password)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("GenerateNewPassword_PoolWithNilCredentials", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.UUID = poolUUID
		poolView.Pool.PoolCredentials = nil

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		result, err := activity.GenerateNewPassword(ctx, poolUUID)

		// Assert - actually succeeds and creates new credentials
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.NotEmpty(tt, result.NewPassword)
		assert.NotEmpty(tt, result.NewSecretID)
		mockSE.AssertExpectations(tt)
	})
}

func TestRotateVcpToVsaCertificateActivity_TestPasswordConnectivity_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("TestPasswordConnectivity_EmptyPassword", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.UUID = poolUUID
		poolView.Pool.PoolCredentials.Password = "" // Empty password

		// Mock database calls
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)
		// Mock password retrieval to fail (since password is empty, it will try to fetch from Secret Manager)
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", errors.New("failed to retrieve password")
		}
		defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()

		// Execute with empty password
		err := activity.TestPasswordConnectivity(ctx, poolUUID, "")

		// Assert - should fail when password retrieval fails
		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("TestPasswordConnectivity_PoolWithNoSecretID", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.UUID = poolUUID
		poolView.Pool.PoolCredentials.SecretID = "" // No secret ID

		// Mock database calls
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)
		// Mock GetNodesByPoolID since the method calls it
		mockSE.On("GetNodesByPoolID", ctx, int64(1)).Return([]*datamodel.Node{}, nil)

		// Execute
		err := activity.TestPasswordConnectivity(ctx, poolUUID, "test-password")

		// Assert - should fail due to missing secret ID or empty nodes
		// The actual behavior depends on implementation, but it should handle the error gracefully
		// It may fail at secret retrieval or node connectivity check
		assert.Error(tt, err) // Expected to fail
		mockSE.AssertExpectations(tt)
	})
}

// Additional tests for SwapSecretIDs to improve coverage
func TestRotateVcpToVsaCertificateActivity_SwapSecretIDs_Additional(t *testing.T) {
	ctx := context.Background()

	t.Run("SwapSecretIDs_DatabaseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		dbError := errors.New("database connection failed")

		// Mock database call to return error
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, dbError)

		// Execute
		err := activity.SwapSecretIDs(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})

	t.Run("SwapSecretIDs_NilCredentials", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials = nil

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.SwapSecretIDs(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

// Additional tests for UpdateVSAPassword to improve coverage
func TestRotateVcpToVsaCertificateActivity_UpdateVSAPassword_Additional(t *testing.T) {
	ctx := context.Background()

	t.Run("UpdateVSAPassword_DatabaseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		dbError := errors.New("database connection failed")

		// Mock database call to return error
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, dbError)

		// Execute
		err := activity.UpdateVSAPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})

		t.Run("UpdateVSAPassword_NilCredentials", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials = nil

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.UpdateVSAPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})

		t.Run("UpdateVSAPassword_EmptySecretIDNew", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretIDNew = "" // Empty secret ID new

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.UpdateVSAPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "secret_id_new is empty")
		mockSE.AssertExpectations(tt)
	})

	t.Run("UpdateVSAPassword_PasswordRetrievalError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Save original function
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		defer func() {
			hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword
		}()

		// Mock password retrieval to fail - covers lines 173-177
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", errors.New("failed to retrieve password")
		}

		// Execute
		err := activity.UpdateVSAPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("UpdateVSAPassword_UpdateVSAPasswordError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Save original function
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		defer func() {
			hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword
		}()

		// Mock password retrieval to succeed
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "new-password", nil
		}

		// Mock updateVSAPassword to fail - covers lines 182-186
		// This requires mocking GetNodesByPoolID
		mockSE.On("GetNodesByPoolID", ctx, int64(1)).Return(nil, errors.New("failed to get nodes"))

		// Execute
		err := activity.UpdateVSAPassword(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("UpdateVSAPassword_Success", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Save original functions
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		originalGetProvider := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword
			hyperscaler2.GetProviderByNode = originalGetProvider
		}()

		// Mock password retrieval to succeed (for both new password and current password)
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "test-password", nil
		}

		// Mock updateVSAPassword to succeed - covers lines 189-190
		// This requires mocking GetNodesByPoolID and the provider
		nodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{ID: 1},
				EndpointAddress: "1.2.3.4",
			},
		}
		mockSE.On("GetNodesByPoolID", ctx, int64(1)).Return(nodes, nil).Once()

		// Mock provider to return a mock that has UpdateAdminPassword
		mockProvider := &vsa.MockProvider{}
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock executeAPIRequestWithResponse to succeed - this allows us to test lines 189-190
		activity.executeAPIRequestWithResponseFunc = func(ctx context.Context, method, url string, headers map[string]string, data interface{}, auth string) (int, string, error) {
			// Verify it's a POST request to the password update endpoint
			assert.Equal(tt, "POST", method)
			assert.Contains(tt, url, "/api/private/cli/security/login/password")
			// Return success status code
			return 200, `{"status": "success"}`, nil
		}

		// Execute - this will test the success path (lines 189-190)
		err := activity.UpdateVSAPassword(ctx, poolUUID)

		// Assert - should succeed and cover lines 189-190
		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

// Additional tests for SwapSecretIDs to improve coverage
func TestRotateVcpToVsaCertificateActivity_SwapSecretIDs_AdditionalCoverage(t *testing.T) {
	ctx := context.Background()

	t.Run("SwapSecretIDs_NoSecretIDNew", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretIDNew = "" // No new secret ID to swap

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.SwapSecretIDs(ctx, poolUUID)

		// Assert - should fail because there's no secret_id_new to swap
		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

// Additional tests for UpdateCacheWithNewSecret to improve coverage
func TestRotateVcpToVsaCertificateActivity_UpdateCacheWithNewSecret_AdditionalCoverage(t *testing.T) {
	ctx := context.Background()

	t.Run("UpdateCacheWithNewSecret_EmptySecretID", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		poolView := createTestPoolViewForPassword()
		poolView.Pool.PoolCredentials.SecretID = "" // Empty secret ID

		// Mock database call
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*datamodel.PoolView{poolView}, nil)

		// Execute
		err := activity.UpdateCacheWithNewSecret(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "secret_id is empty")
		mockSE.AssertExpectations(tt)
	})

	t.Run("UpdateCacheWithNewSecret_DatabaseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		poolUUID := "test-pool-uuid"
		dbError := errors.New("database connection failed")

		// Mock database call to return error
		mockSE.On("ListPools", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, dbError)

		// Execute
		err := activity.UpdateCacheWithNewSecret(ctx, poolUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred")
		mockSE.AssertExpectations(tt)
	})
}

// Tests for GetCertificateExpirationInfo
func TestRotateVcpToVsaCertificateActivity_GetCertificateExpirationInfo(t *testing.T) {
	ctx := context.Background()

	t.Run("GetCertificateExpirationInfo_EmptyCertificateID", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		certificateID := ""

		// Execute - will fail due to missing GCP service, but we test the function structure
		result, err := activity.GetCertificateExpirationInfo(ctx, certificateID)

		// Assert - expected to fail due to missing GCP credentials
		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetCertificateExpirationInfo_NonExistentCertificate", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		certificateID := "non-existent-cert-id"

		// Execute - will fail due to missing GCP service, but we test the function structure
		result, err := activity.GetCertificateExpirationInfo(ctx, certificateID)

		// Assert - expected to fail due to missing GCP credentials
		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetCertificateExpirationInfo_CertificateRetrievalError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		certificateID := "test-cert-id"
		retrievalError := errors.New("failed to retrieve certificate")

		// Save original function
		originalGetCert := getCertificateFromCacheOrSecretManager
		defer func() {
			getCertificateFromCacheOrSecretManager = originalGetCert
		}()

		// Mock certificate retrieval to return error
		getCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return nil, retrievalError
		}

		// Execute
		result, err := activity.GetCertificateExpirationInfo(ctx, certificateID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, retrievalError, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetCertificateExpirationInfo_NilCertificate", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		certificateID := "test-cert-id"

		// Save original function
		originalGetCert := getCertificateFromCacheOrSecretManager
		defer func() {
			getCertificateFromCacheOrSecretManager = originalGetCert
		}()

		// Mock certificate retrieval to return nil
		getCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return nil, nil
		}

		// Execute
		result, err := activity.GetCertificateExpirationInfo(ctx, certificateID)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, certificateID, result.CertificateID)
		assert.False(tt, result.Exists)
		assert.True(tt, result.NeedsRotation)
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetCertificateExpirationInfo_EmptySignedCertificate", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		certificateID := "test-cert-id"

		// Save original function
		originalGetCert := getCertificateFromCacheOrSecretManager
		defer func() {
			getCertificateFromCacheOrSecretManager = originalGetCert
		}()

		// Mock certificate retrieval to return certificate with empty SignedCertificate
		getCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate: "",
			}, nil
		}

		// Execute
		result, err := activity.GetCertificateExpirationInfo(ctx, certificateID)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, certificateID, result.CertificateID)
		assert.True(tt, result.Exists)
		assert.True(tt, result.NeedsRotation)
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetCertificateExpirationInfo_ParseError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateVcpToVsaCertificateActivity{SE: mockSE}

		certificateID := "test-cert-id"

		// Save original function
		originalGetCert := getCertificateFromCacheOrSecretManager
		defer func() {
			getCertificateFromCacheOrSecretManager = originalGetCert
		}()

		// Mock certificate retrieval to return certificate with invalid PEM
		getCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate: "invalid-pem-certificate",
			}, nil
		}

		// Execute
		result, err := activity.GetCertificateExpirationInfo(ctx, certificateID)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, certificateID, result.CertificateID)
		assert.True(tt, result.Exists)
		assert.True(tt, result.NeedsRotation) // Should be true when parse fails
		mockSE.AssertExpectations(tt)
	})
}