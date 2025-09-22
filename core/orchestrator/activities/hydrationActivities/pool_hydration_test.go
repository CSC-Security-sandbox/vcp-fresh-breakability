package hydrationActivities

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// Helper function to create a valid test pool
func createValidTestPool() datamodel.Pool {
	return datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:  "test-pool",
		State: "AVAILABLE",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			PrimaryZone: "us-central1-a",
		},
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes: 1073741824, // 1 GiB in bytes
		},
	}
}

// Test for HydrateUpdatedPoolToCCFE function - Success case
func TestHydrateUpdatedPoolToCCFE_Success(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := createValidTestPool()

	// Mock auth.GenerateCallbackToken
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}

	// Mock common.HydrateUpdatedPool
	originalHydrateUpdatedPool := common.HydrateUpdatedPool
	defer func() { common.HydrateUpdatedPool = originalHydrateUpdatedPool }()
	common.HydrateUpdatedPool = func(ctx context.Context, poolHydrateObj models.PoolHydrateObject, token string) error {
		// Verify the correct parameters are passed
		assert.Equal(t, "test-account", poolHydrateObj.OwnerID)
		assert.Equal(t, "test-pool-uuid", poolHydrateObj.PoolID)
		assert.Equal(t, "test-pool", poolHydrateObj.Name)
		assert.Equal(t, "AVAILABLE", poolHydrateObj.State)
		assert.Equal(t, "us-central1-a", poolHydrateObj.Region)
		assert.Equal(t, int64(1), poolHydrateObj.HotTierSizeGib) // 1 GiB
		assert.Equal(t, "mock-token", token)
		return nil
	}

	err := HydrateUpdatedPoolToCCFE(ctx, pool)

	assert.NoError(t, err)
}

// Test for HydrateUpdatedPoolToCCFE function - Validation failure
func TestHydrateUpdatedPoolToCCFE_ValidationFailure(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	testCases := []struct {
		name        string
		pool        datamodel.Pool
		expectedErr string
	}{
		{
			name: "Missing Account Name",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: ""}, // Empty name
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 1073741824,
				},
			},
			expectedErr: "OwnerID/AccountName missing for hydrate pool",
		},
		{
			name: "Missing Pool UUID",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: ""}, // Empty UUID
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 1073741824,
				},
			},
			expectedErr: "PoolID missing for hydrate pool",
		},
		{
			name: "Missing Pool Name",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "", // Empty name
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 1073741824,
				},
			},
			expectedErr: "Name missing for hydrate pool",
		},
		{
			name: "Missing Pool State",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "", // Empty state
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 1073741824,
				},
			},
			expectedErr: "State missing for hydrate pool",
		},
		{
			name: "Missing Primary Zone",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "", // Empty zone
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 1073741824,
				},
			},
			expectedErr: "Region missing for hydrate pool",
		},
		{
			name: "Invalid HotTierSizeInBytes",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 0, // Invalid size
				},
			},
			expectedErr: "HotTierSizeInBytes missing for hydrate pool",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := HydrateUpdatedPoolToCCFE(ctx, tc.pool)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

// Test for HydrateUpdatedPoolToCCFE function - Token generation failure
func TestHydrateUpdatedPoolToCCFE_TokenGenerationFailure(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := createValidTestPool()

	// Mock auth.GenerateCallbackToken to return error
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "", errors.New("token generation failed")
	}

	err := HydrateUpdatedPoolToCCFE(ctx, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "token generation failed")
}

// Test for HydrateUpdatedPoolToCCFE function - CCFE hydration failure
func TestHydrateUpdatedPoolToCCFE_CCFEHydrationFailure(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := createValidTestPool()

	// Mock auth.GenerateCallbackToken
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}

	// Mock common.HydrateUpdatedPool to return error
	originalHydrateUpdatedPool := common.HydrateUpdatedPool
	defer func() { common.HydrateUpdatedPool = originalHydrateUpdatedPool }()
	common.HydrateUpdatedPool = func(ctx context.Context, poolHydrateObj models.PoolHydrateObject, token string) error {
		return errors.New("CCFE hydration failed")
	}

	err := HydrateUpdatedPoolToCCFE(ctx, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CCFE hydration failed")
}

// Test for HydrateUpdatedPoolToCCFE function - Nil PoolAttributes
func TestHydrateUpdatedPoolToCCFE_NilPoolAttributes(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := createValidTestPool()
	pool.PoolAttributes = nil // Set to nil

	// This should panic due to nil pointer dereference in the current implementation
	defer func() {
		if r := recover(); r != nil {
			// Expected panic due to nil pointer dereference
			assert.Contains(t, fmt.Sprintf("%v", r), "runtime error: invalid memory address or nil pointer dereference")
		}
	}()

	_ = HydrateUpdatedPoolToCCFE(ctx, pool)
	t.Fatal("Expected panic due to nil pointer dereference, but function completed")
}

// Test for HydrateUpdatedPoolToCCFE function - Nil AutoTieringConfig
func TestHydrateUpdatedPoolToCCFE_NilAutoTieringConfig(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := createValidTestPool()
	pool.AutoTieringConfig = nil // Set to nil

	// This should panic due to nil pointer dereference in the current implementation
	defer func() {
		if r := recover(); r != nil {
			// Expected panic due to nil pointer dereference
			assert.Contains(t, fmt.Sprintf("%v", r), "runtime error: invalid memory address or nil pointer dereference")
		}
	}()

	_ = HydrateUpdatedPoolToCCFE(ctx, pool)
	t.Fatal("Expected panic due to nil pointer dereference, but function completed")
}

// Test for HydrateUpdatedPoolToCCFE function - Nil Account
func TestHydrateUpdatedPoolToCCFE_NilAccount(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := createValidTestPool()
	pool.Account = nil // Set to nil

	// This should panic due to nil pointer dereference in the current implementation
	defer func() {
		if r := recover(); r != nil {
			// Expected panic due to nil pointer dereference
			assert.Contains(t, fmt.Sprintf("%v", r), "runtime error: invalid memory address or nil pointer dereference")
		}
	}()

	_ = HydrateUpdatedPoolToCCFE(ctx, pool)
	t.Fatal("Expected panic due to nil pointer dereference, but function completed")
}

// Test for HydrateUpdatedPoolToCCFE function - Large HotTierSizeInBytes
func TestHydrateUpdatedPoolToCCFE_LargeHotTierSize(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := createValidTestPool()
	pool.AutoTieringConfig.HotTierSizeInBytes = 10737418240 // 10 GiB

	// Mock auth.GenerateCallbackToken
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}

	// Mock common.HydrateUpdatedPool
	originalHydrateUpdatedPool := common.HydrateUpdatedPool
	defer func() { common.HydrateUpdatedPool = originalHydrateUpdatedPool }()
	common.HydrateUpdatedPool = func(ctx context.Context, poolHydrateObj models.PoolHydrateObject, token string) error {
		// Verify the conversion from bytes to GiB
		assert.Equal(t, int64(10), poolHydrateObj.HotTierSizeGib) // 10 GiB
		return nil
	}

	err := HydrateUpdatedPoolToCCFE(ctx, pool)

	assert.NoError(t, err)
}

// Test for validateHydratePool function - Success case
func TestValidateHydratePool_Success(t *testing.T) {
	pool := createValidTestPool()

	err := validateHydratePool(pool)

	assert.NoError(t, err)
}

// Test for validateHydratePool function - All validation errors
func TestValidateHydratePool_AllValidationErrors(t *testing.T) {
	testCases := []struct {
		name        string
		pool        datamodel.Pool
		expectedErr string
	}{
		{
			name: "Empty Account Name",
			pool: datamodel.Pool{
				Account: &datamodel.Account{Name: ""},
			},
			expectedErr: "OwnerID/AccountName missing for hydrate pool",
		},
		{
			name: "Nil Account",
			pool: datamodel.Pool{
				Account: nil,
			},
			expectedErr: "runtime error: invalid memory address or nil pointer dereference",
		},
		{
			name: "Empty Pool UUID",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: ""},
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 1073741824,
				},
			},
			expectedErr: "PoolID missing for hydrate pool",
		},
		{
			name: "Empty Pool Name",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 1073741824,
				},
			},
			expectedErr: "Name missing for hydrate pool",
		},
		{
			name: "Empty Pool State",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 1073741824,
				},
			},
			expectedErr: "State missing for hydrate pool",
		},
		{
			name: "Nil PoolAttributes",
			pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "test-uuid"},
				Name:           "test-pool",
				State:          "AVAILABLE",
				Account:        &datamodel.Account{Name: "test-account"},
				PoolAttributes: nil,
			},
			expectedErr: "runtime error: invalid memory address or nil pointer dereference",
		},
		{
			name: "Empty PrimaryZone",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "",
				},
			},
			expectedErr: "Region missing for hydrate pool",
		},
		{
			name: "Nil AutoTieringConfig",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: nil,
			},
			expectedErr: "runtime error: invalid memory address or nil pointer dereference",
		},
		{
			name: "Zero HotTierSizeInBytes",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 0,
				},
			},
			expectedErr: "HotTierSizeInBytes missing for hydrate pool",
		},
		{
			name: "Negative HotTierSizeInBytes",
			pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-pool",
				State:     "AVAILABLE",
				Account:   &datamodel.Account{Name: "test-account"},
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: -1,
				},
			},
			expectedErr: "HotTierSizeInBytes missing for hydrate pool",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectedErr == "runtime error: invalid memory address or nil pointer dereference" {
				// Handle panic cases
				defer func() {
					if r := recover(); r != nil {
						assert.Contains(t, fmt.Sprintf("%v", r), tc.expectedErr)
					}
				}()
				_ = validateHydratePool(tc.pool)
				t.Fatal("Expected panic due to nil pointer dereference, but function completed")
			} else {
				// Handle normal error cases
				err := validateHydratePool(tc.pool)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

// Test for validateHydratePool function - Edge cases
func TestValidateHydratePool_EdgeCases(t *testing.T) {
	t.Run("Minimum valid HotTierSizeInBytes", func(t *testing.T) {
		pool := createValidTestPool()
		pool.AutoTieringConfig.HotTierSizeInBytes = 1 // Minimum valid value

		err := validateHydratePool(pool)
		assert.NoError(t, err)
	})

	t.Run("Large HotTierSizeInBytes", func(t *testing.T) {
		pool := createValidTestPool()
		pool.AutoTieringConfig.HotTierSizeInBytes = 1099511627776 // 1 TiB

		err := validateHydratePool(pool)
		assert.NoError(t, err)
	})

	t.Run("Whitespace in required string fields", func(t *testing.T) {
		pool := datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "   "},
			Name:      "   ",
			State:     "   ",
			Account:   &datamodel.Account{Name: "   "},
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "   ",
			},
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: 1073741824,
			},
		}

		// All these should pass validation since we're not trimming whitespace
		err := validateHydratePool(pool)
		assert.NoError(t, err)
	})
}
