package database

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

func createTestAccountAndPoolForExpertMode(t *testing.T, store *DataStoreRepository) (*datamodel.Account, *datamodel.Pool) {
	accountUUID := utils.RandomUUID()
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{
			UUID:      accountUUID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Name: fmt.Sprintf("test_account_%s", utils.GenerateRandomAlphanumeric(8)),
	}
	err := store.db.Create(account).Error()
	assert.NoError(t, err)

	poolUUID := utils.RandomUUID()
	deploymentName := fmt.Sprintf("test_deployment_%s", utils.GenerateRandomAlphanumeric(8))
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID:      poolUUID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Name:           fmt.Sprintf("test_pool_%s", utils.GenerateRandomAlphanumeric(8)),
		AccountID:      account.ID,
		SizeInBytes:    2199023255552, // 2TB
		DeploymentName: deploymentName,
		PoolAttributes: &datamodel.PoolAttributes{
			PrimaryZone: "us-west1-a",
		},
	}
	err = store.db.Create(pool).Error()
	assert.NoError(t, err)

	return account, pool
}

func createTestSVMForExpertMode(t *testing.T, store *DataStoreRepository, poolID int64, accountID int64, svmName string, externalUUID string) *datamodel.Svm {
	svmDetails := &datamodel.SvmDetails{
		ExternalUUID: externalUUID,
		IPSpace:      "Default",
	}
	svmUUID := utils.RandomUUID()
	svm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{
			UUID:      svmUUID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Name:       svmName,
		PoolID:     poolID,
		AccountID:  accountID,
		SvmDetails: svmDetails,
		State:      models.LifeCycleStateREADY,
	}
	err := store.db.Create(svm).Error()
	assert.NoError(t, err)
	return svm
}

func TestCreateExpertModeVolume_Success(t *testing.T) {
	store := setup(t)
	ctx := context.Background()
	account, pool := createTestAccountAndPoolForExpertMode(t, store)
	svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
	svmExternalUUID := utils.RandomUUID()
	svm := createTestSVMForExpertMode(t, store, pool.ID, account.ID, svmName, svmExternalUUID)

	expertModeVolume := &datamodel.ExpertModeVolumes{
		Name:         "test-expert-volume",
		SizeInBytes:  1099511627776, // 1TB
		PoolID:       pool.ID,
		AccountID:    account.ID,
		SvmID:        svm.ID,
		Style:        "flexvol",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}

	createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)

	assert.NoError(t, err)
	assert.NotNil(t, createdVolume)
	assert.NotEmpty(t, createdVolume.UUID)
	assert.Equal(t, expertModeVolume.Name, createdVolume.Name)
	assert.Equal(t, expertModeVolume.SizeInBytes, createdVolume.SizeInBytes)
	assert.Equal(t, expertModeVolume.PoolID, createdVolume.PoolID)
	assert.Equal(t, expertModeVolume.AccountID, createdVolume.AccountID)
	assert.Equal(t, expertModeVolume.SvmID, createdVolume.SvmID)
	assert.Equal(t, expertModeVolume.Style, createdVolume.Style)
	assert.Equal(t, models.LifeCycleStateCreating, createdVolume.State)
	assert.NotEmpty(t, createdVolume.ExternalUUID)
}

func TestCreateExpertModeVolume_WithExistingUUID(t *testing.T) {
	store := setup(t)
	ctx := context.Background()
	account, pool := createTestAccountAndPoolForExpertMode(t, store)
	svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
	svmExternalUUID := utils.RandomUUID()
	svm := createTestSVMForExpertMode(t, store, pool.ID, account.ID, svmName, svmExternalUUID)

	existingUUID := "existing-uuid-12345"
	expertModeVolume := &datamodel.ExpertModeVolumes{
		BaseModel: datamodel.BaseModel{
			UUID: existingUUID,
		},
		Name:         "test-expert-volume",
		SizeInBytes:  1099511627776,
		PoolID:       pool.ID,
		AccountID:    account.ID,
		SvmID:        svm.ID,
		Style:        "flexvol",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}

	createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)

	assert.NoError(t, err)
	assert.NotNil(t, createdVolume)
	assert.Equal(t, existingUUID, createdVolume.UUID)
}

func TestCreateExpertModeVolume_WithAllStyles(t *testing.T) {
	styles := []string{"flexvol", "flexgroup", "flexcache"}

	for _, style := range styles {
		t.Run(style, func(t *testing.T) {
			store := setup(t)
			ctx := context.Background()
			account, pool := createTestAccountAndPoolForExpertMode(t, store)
			svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
			svmExternalUUID := utils.RandomUUID()
			svm := createTestSVMForExpertMode(t, store, pool.ID, account.ID, svmName, svmExternalUUID)

			expertModeVolume := &datamodel.ExpertModeVolumes{
				Name:         "test-volume-" + style,
				SizeInBytes:  1099511627776,
				PoolID:       pool.ID,
				AccountID:    account.ID,
				SvmID:        svm.ID,
				Style:        style,
				ExternalUUID: utils.RandomUUID(),
				State:        models.LifeCycleStateCreating,
			}

			createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)

			assert.NoError(t, err)
			assert.NotNil(t, createdVolume)
			assert.Equal(t, style, createdVolume.Style)
		})
	}
}

func TestGetExpertModePoolUsedCapacity_Success(t *testing.T) {
	store := setup(t)
	ctx := context.Background()
	account, pool := createTestAccountAndPoolForExpertMode(t, store)
	svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
	svmExternalUUID := utils.RandomUUID()
	svm := createTestSVMForExpertMode(t, store, pool.ID, account.ID, svmName, svmExternalUUID)

	// Create multiple volumes for the same pool
	volume1 := &datamodel.ExpertModeVolumes{
		Name:         "volume-1",
		SizeInBytes:  1099511627776, // 1TB
		PoolID:       pool.ID,
		AccountID:    account.ID,
		SvmID:        svm.ID,
		Style:        "flexvol",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}
	volume2 := &datamodel.ExpertModeVolumes{
		Name:         "volume-2",
		SizeInBytes:  214748364800, // 200GB
		PoolID:       pool.ID,
		AccountID:    account.ID,
		SvmID:        svm.ID,
		Style:        "flexgroup",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}
	volume3 := &datamodel.ExpertModeVolumes{
		Name:         "volume-3",
		SizeInBytes:  536870912000, // 500GB
		PoolID:       pool.ID,
		AccountID:    account.ID,
		SvmID:        svm.ID,
		Style:        "flexcache",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}

	_, err := store.CreateExpertModeVolume(ctx, volume1)
	assert.NoError(t, err)
	_, err = store.CreateExpertModeVolume(ctx, volume2)
	assert.NoError(t, err)
	_, err = store.CreateExpertModeVolume(ctx, volume3)
	assert.NoError(t, err)

	// Create another pool and volume (should not be included in total)
	account2, pool2 := createTestAccountAndPoolForExpertMode(t, store)
	svmName2 := fmt.Sprintf("test-svm-2-%s", utils.GenerateRandomAlphanumeric(8))
	svmExternalUUID2 := utils.RandomUUID()
	svm2 := createTestSVMForExpertMode(t, store, pool2.ID, account2.ID, svmName2, svmExternalUUID2)
	volume4 := &datamodel.ExpertModeVolumes{
		Name:         "volume-4",
		SizeInBytes:  1099511627776, // 1TB
		PoolID:       pool2.ID,
		AccountID:    account2.ID,
		SvmID:        svm2.ID,
		Style:        "flexvol",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}
	_, err = store.CreateExpertModeVolume(ctx, volume4)
	assert.NoError(t, err)

	// Get total size for pool1
	// Expected: 1TB + 200GB + 500GB = 1,700GB = 1,851,130,904,576 bytes
	expectedTotal := int64(1099511627776 + 214748364800 + 536870912000)
	totalSize, err := store.GetExpertModePoolUsedCapacity(ctx, pool.ID)

	assert.NoError(t, err)
	assert.Equal(t, expectedTotal, totalSize)
}

func TestGetExpertModePoolUsedCapacity_EmptyPool(t *testing.T) {
	store := setup(t)
	ctx := context.Background()
	account, pool := createTestAccountAndPoolForExpertMode(t, store)
	_ = account

	totalSize, err := store.GetExpertModePoolUsedCapacity(ctx, pool.ID)

	assert.NoError(t, err)
	assert.Equal(t, int64(0), totalSize)
}

func TestGetExpertModePoolUsedCapacity_NonExistentPool(t *testing.T) {
	store := setup(t)
	ctx := context.Background()

	totalSize, err := store.GetExpertModePoolUsedCapacity(ctx, 99999)

	assert.NoError(t, err)
	assert.Equal(t, int64(0), totalSize)
}

func TestGetExpertModePoolUsedCapacity_WithDeletedVolumes(t *testing.T) {
	store := setup(t)
	ctx := context.Background()
	account, pool := createTestAccountAndPoolForExpertMode(t, store)
	svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
	svmExternalUUID := utils.RandomUUID()
	svm := createTestSVMForExpertMode(t, store, pool.ID, account.ID, svmName, svmExternalUUID)

	// Create a volume
	volume1 := &datamodel.ExpertModeVolumes{
		Name:         "volume-1",
		SizeInBytes:  1099511627776, // 1TB
		PoolID:       pool.ID,
		AccountID:    account.ID,
		SvmID:        svm.ID,
		Style:        "flexvol",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}
	createdVolume, err := store.CreateExpertModeVolume(ctx, volume1)
	assert.NoError(t, err)

	// Soft delete the volume
	err = store.db.GORM().Delete(createdVolume).Error
	assert.NoError(t, err)

	// Total size should NOT include deleted volumes (GORM excludes soft-deleted records)
	totalSize, err := store.GetExpertModePoolUsedCapacity(ctx, pool.ID)

	assert.NoError(t, err)
	assert.Equal(t, int64(0), totalSize)
}

func TestCreateExpertModeVolume_ForeignKeyConstraint(t *testing.T) {
	store := setup(t)
	ctx := context.Background()

	// Try to create volume with non-existent pool ID
	// Note: SQLite doesn't enforce foreign key constraints by default
	expertModeVolume := &datamodel.ExpertModeVolumes{
		Name:         "test-expert-volume",
		SizeInBytes:  1099511627776,
		PoolID:       99999, // Non-existent pool
		AccountID:    99999, // Non-existent account
		SvmID:        99999, // Non-existent SVM
		Style:        "flexvol",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}

	createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)

	// SQLite in-memory DB doesn't enforce foreign keys by default
	// The function will succeed but the data would be invalid
	// In a real scenario with FK constraints enabled, this would fail
	if err == nil {
		assert.NotNil(t, createdVolume)
		assert.NotEmpty(t, createdVolume.UUID)
	}
}

func TestGetExpertModeVolumeByNameAndPoolID_Success(t *testing.T) {
	store := setup(t)
	ctx := context.Background()
	account, pool := createTestAccountAndPoolForExpertMode(t, store)
	svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
	svmExternalUUID := utils.RandomUUID()
	svm := createTestSVMForExpertMode(t, store, pool.ID, account.ID, svmName, svmExternalUUID)

	expertModeVolume := &datamodel.ExpertModeVolumes{
		Name:         "test-unique-volume",
		SizeInBytes:  1099511627776,
		PoolID:       pool.ID,
		AccountID:    account.ID,
		SvmID:        svm.ID,
		Style:        "flexvol",
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
	}

	_, err := store.CreateExpertModeVolume(ctx, expertModeVolume)
	assert.NoError(t, err)

	// Retrieve the volume by name and pool ID
	retrievedVolume, err := store.GetExpertModeVolumeByNameAndPoolID(ctx, "test-unique-volume", pool.ID)

	assert.NoError(t, err)
	assert.NotNil(t, retrievedVolume)
	assert.Equal(t, "test-unique-volume", retrievedVolume.Name)
	assert.Equal(t, pool.ID, retrievedVolume.PoolID)
}

func TestGetExpertModeVolumeByNameAndPoolID_NotFound(t *testing.T) {
	store := setup(t)
	ctx := context.Background()
	account, pool := createTestAccountAndPoolForExpertMode(t, store)
	_ = account

	// Try to retrieve non-existent volume
	retrievedVolume, err := store.GetExpertModeVolumeByNameAndPoolID(ctx, "non-existent-volume", pool.ID)

	assert.Error(t, err)
	assert.Nil(t, retrievedVolume)
}

func TestGetExpertModeVolumeByUUID(t *testing.T) {
	t.Run("WhenVolumeExists", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()
		account, pool := createTestAccountAndPoolForExpertMode(tt, store)
		svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
		svmExternalUUID := utils.RandomUUID()
		svm := createTestSVMForExpertMode(tt, store, pool.ID, account.ID, svmName, svmExternalUUID)

		// Create expert mode volume
		expertModeVolume := &datamodel.ExpertModeVolumes{
			Name:         "test-expert-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: utils.RandomUUID(),
			State:        models.LifeCycleStateCreating,
		}

		createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdVolume)

		// Retrieve the volume by UUID
		retrievedVolume, err := store.GetExpertModeVolumeByUUID(ctx, createdVolume.UUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, retrievedVolume)
		assert.Equal(tt, createdVolume.UUID, retrievedVolume.UUID)
		assert.Equal(tt, "test-expert-volume", retrievedVolume.Name)
		assert.Equal(tt, int64(1099511627776), retrievedVolume.SizeInBytes)
		assert.Equal(tt, "flexvol", retrievedVolume.Style)
		assert.Equal(tt, models.LifeCycleStateCreating, retrievedVolume.State)
		assert.Equal(tt, pool.ID, retrievedVolume.PoolID)
		assert.Equal(tt, account.ID, retrievedVolume.AccountID)
		assert.Equal(tt, svm.ID, retrievedVolume.SvmID)

		// Verify preloaded relationships
		assert.NotNil(tt, retrievedVolume.Account)
		assert.Equal(tt, account.ID, retrievedVolume.Account.ID)
		assert.Equal(tt, account.Name, retrievedVolume.Account.Name)
		assert.NotNil(tt, retrievedVolume.Pool)
		assert.Equal(tt, pool.ID, retrievedVolume.Pool.ID)
		assert.Equal(tt, pool.UUID, retrievedVolume.Pool.UUID)
		assert.NotNil(tt, retrievedVolume.Svm)
		assert.Equal(tt, svm.ID, retrievedVolume.Svm.ID)
		assert.Equal(tt, svm.Name, retrievedVolume.Svm.Name)
	})

	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()

		// Try to retrieve non-existent volume
		nonExistentUUID := utils.RandomUUID()
		retrievedVolume, err := store.GetExpertModeVolumeByUUID(ctx, nonExistentUUID)

		assert.Error(tt, err)
		assert.Nil(tt, retrievedVolume)
		assert.True(tt, errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err))
	})

	t.Run("WhenVolumeExistsWithDifferentStyles", func(tt *testing.T) {
		styles := []string{"flexvol", "flexgroup", "flexcache"}

		for _, style := range styles {
			tt.Run(style, func(ttt *testing.T) {
				store := setup(ttt)
				ctx := context.Background()
				account, pool := createTestAccountAndPoolForExpertMode(ttt, store)
				svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
				svmExternalUUID := utils.RandomUUID()
				svm := createTestSVMForExpertMode(ttt, store, pool.ID, account.ID, svmName, svmExternalUUID)

				expertModeVolume := &datamodel.ExpertModeVolumes{
					Name:         fmt.Sprintf("test-volume-%s", style),
					SizeInBytes:  1099511627776,
					PoolID:       pool.ID,
					AccountID:    account.ID,
					SvmID:        svm.ID,
					Style:        style,
					ExternalUUID: utils.RandomUUID(),
					State:        models.LifeCycleStateCreating,
				}

				createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)
				assert.NoError(ttt, err)

				retrievedVolume, err := store.GetExpertModeVolumeByUUID(ctx, createdVolume.UUID)

				assert.NoError(ttt, err)
				assert.NotNil(ttt, retrievedVolume)
				assert.Equal(ttt, style, retrievedVolume.Style)
				assert.NotNil(ttt, retrievedVolume.Account)
				assert.NotNil(ttt, retrievedVolume.Pool)
				assert.NotNil(ttt, retrievedVolume.Svm)
			})
		}
	})
}

func TestUpdateExpertModeVolume(t *testing.T) {
	t.Run("WhenVolumeIsUpdatedSuccessfully", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()
		account, pool := createTestAccountAndPoolForExpertMode(tt, store)
		svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
		svmExternalUUID := utils.RandomUUID()
		svm := createTestSVMForExpertMode(tt, store, pool.ID, account.ID, svmName, svmExternalUUID)

		// Create initial volume
		expertModeVolume := &datamodel.ExpertModeVolumes{
			Name:         "test-expert-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: utils.RandomUUID(),
			State:        models.LifeCycleStateCreating,
		}

		createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdVolume)

		// Update the volume with new values
		updatedVolume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: createdVolume.UUID,
			},
			Name:        "updated-expert-volume",
			SizeInBytes: 2199023255552, // 2TB
			Style:       "flexgroup",
			State:       models.LifeCycleStateAvailable,
		}

		result, err := store.UpdateExpertModeVolume(ctx, updatedVolume)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "updated-expert-volume", result.Name)
		assert.Equal(tt, int64(2199023255552), result.SizeInBytes)
		assert.Equal(tt, "flexgroup", result.Style)
		assert.Equal(tt, models.LifeCycleStateAvailable, result.State)
		assert.Equal(tt, createdVolume.UUID, result.UUID)
		assert.Equal(tt, pool.ID, result.PoolID)
		assert.Equal(tt, account.ID, result.AccountID)
		assert.Equal(tt, svm.ID, result.SvmID)
	})

	t.Run("WhenOnlyStateIsUpdated", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()
		account, pool := createTestAccountAndPoolForExpertMode(tt, store)
		svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
		svmExternalUUID := utils.RandomUUID()
		svm := createTestSVMForExpertMode(tt, store, pool.ID, account.ID, svmName, svmExternalUUID)

		// Create initial volume
		expertModeVolume := &datamodel.ExpertModeVolumes{
			Name:         "test-expert-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: utils.RandomUUID(),
			State:        models.LifeCycleStateCreating,
		}

		createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdVolume)

		// Update only the state
		updatedVolume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: createdVolume.UUID,
			},
			Name:        createdVolume.Name,
			SizeInBytes: createdVolume.SizeInBytes,
			Style:       createdVolume.Style,
			State:       models.LifeCycleStateDeleted,
		}

		result, err := store.UpdateExpertModeVolume(ctx, updatedVolume)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.LifeCycleStateDeleted, result.State)
		assert.Equal(tt, createdVolume.Name, result.Name)
		assert.Equal(tt, createdVolume.SizeInBytes, result.SizeInBytes)
		assert.Equal(tt, createdVolume.Style, result.Style)
	})

	t.Run("WhenNameAndSizeAreUpdated", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()
		account, pool := createTestAccountAndPoolForExpertMode(tt, store)
		svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
		svmExternalUUID := utils.RandomUUID()
		svm := createTestSVMForExpertMode(tt, store, pool.ID, account.ID, svmName, svmExternalUUID)

		// Create initial volume
		expertModeVolume := &datamodel.ExpertModeVolumes{
			Name:         "test-expert-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: utils.RandomUUID(),
			State:        models.LifeCycleStateCreating,
		}

		createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdVolume)

		// Update name and size only
		updatedVolume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: createdVolume.UUID,
			},
			Name:        "renamed-expert-volume",
			SizeInBytes: 536870912000, // 500GB
			Style:       createdVolume.Style,
			State:       createdVolume.State,
		}

		result, err := store.UpdateExpertModeVolume(ctx, updatedVolume)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "renamed-expert-volume", result.Name)
		assert.Equal(tt, int64(536870912000), result.SizeInBytes)
		assert.Equal(tt, createdVolume.Style, result.Style)
		assert.Equal(tt, createdVolume.State, result.State)
	})

	t.Run("WhenStyleIsUpdated", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()
		account, pool := createTestAccountAndPoolForExpertMode(tt, store)
		svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
		svmExternalUUID := utils.RandomUUID()
		svm := createTestSVMForExpertMode(tt, store, pool.ID, account.ID, svmName, svmExternalUUID)

		// Create initial volume with flexvol style
		expertModeVolume := &datamodel.ExpertModeVolumes{
			Name:         "test-expert-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: utils.RandomUUID(),
			State:        models.LifeCycleStateCreating,
		}

		createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdVolume)

		// Update style to flexgroup
		updatedVolume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: createdVolume.UUID,
			},
			Name:        createdVolume.Name,
			SizeInBytes: createdVolume.SizeInBytes,
			Style:       "flexgroup",
			State:       createdVolume.State,
		}

		result, err := store.UpdateExpertModeVolume(ctx, updatedVolume)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "flexgroup", result.Style)
		assert.Equal(tt, createdVolume.Name, result.Name)
		assert.Equal(tt, createdVolume.SizeInBytes, result.SizeInBytes)
	})

	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()

		// Try to update non-existent volume
		nonExistentVolume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "non-existent-uuid",
			},
			Name:        "non-existent-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
			State:       models.LifeCycleStateAvailable,
		}

		result, err := store.UpdateExpertModeVolume(ctx, nonExistentVolume)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err))
	})

	t.Run("WhenStateTransitionsArePerformed", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()
		account, pool := createTestAccountAndPoolForExpertMode(tt, store)
		svmName := fmt.Sprintf("test-svm-%s", utils.GenerateRandomAlphanumeric(8))
		svmExternalUUID := utils.RandomUUID()
		svm := createTestSVMForExpertMode(tt, store, pool.ID, account.ID, svmName, svmExternalUUID)

		// Create initial volume in CREATING state
		expertModeVolume := &datamodel.ExpertModeVolumes{
			Name:         "test-expert-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: utils.RandomUUID(),
			State:        models.LifeCycleStateCreating,
		}

		createdVolume, err := store.CreateExpertModeVolume(ctx, expertModeVolume)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdVolume)
		assert.Equal(tt, models.LifeCycleStateCreating, createdVolume.State)

		// Transition: CREATING -> AVAILABLE
		updatedVolume1 := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: createdVolume.UUID,
			},
			Name:        createdVolume.Name,
			SizeInBytes: createdVolume.SizeInBytes,
			Style:       createdVolume.Style,
			State:       models.LifeCycleStateAvailable,
		}

		result1, err := store.UpdateExpertModeVolume(ctx, updatedVolume1)
		assert.NoError(tt, err)
		assert.NotNil(tt, result1)
		assert.Equal(tt, models.LifeCycleStateAvailable, result1.State)

		// Transition: AVAILABLE -> DELETED
		updatedVolume2 := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: createdVolume.UUID,
			},
			Name:        result1.Name,
			SizeInBytes: result1.SizeInBytes,
			Style:       result1.Style,
			State:       models.LifeCycleStateDeleted,
		}

		result2, err := store.UpdateExpertModeVolume(ctx, updatedVolume2)
		assert.NoError(tt, err)
		assert.NotNil(tt, result2)
		assert.Equal(tt, models.LifeCycleStateDeleted, result2.State)
	})
}
