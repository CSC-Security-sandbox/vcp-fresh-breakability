package database

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"gorm.io/gorm"
)

func TestGetSvmsByPoolID(t *testing.T) {
	t.Run("WhenSvmExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:   "test_svm",
			PoolID: 1234,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		result, err := store.GetSvmsByPoolID(context.Background(), svm.PoolID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, svm.Name, result[0].Name, "Expected svm name %v, got %v", svm.Name, result[0].Name)
	})
	t.Run("WhenSvmDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		result, err := store.GetSvmsByPoolID(context.Background(), 12)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, result, "Expected result to be empty, but got %v", result)
	})
}

func TestGetNextSVMIndexByPoolID(t *testing.T) {
	t.Run("WhenSvmsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create multiple SVMs for the same pool
		svm1 := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid-1",
			},
			Name:   "test_svm_1",
			PoolID: 1234,
		}
		err = store.db.Create(svm1).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm1: %v", err)
		}

		svm2 := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-svm-uuid-2",
			},
			Name:   "test_svm_2",
			PoolID: 1234,
		}
		err = store.db.Create(svm2).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm2: %v", err)
		}

		// Create SVM for different pool
		svm3 := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   3,
				UUID: "test-svm-uuid-3",
			},
			Name:   "test_svm_3",
			PoolID: 5678,
		}
		err = store.db.Create(svm3).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm3: %v", err)
		}

		result, err := store.GetNextSVMIndexByPoolID(context.Background(), 1234)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(3), result, "Expected next index to be 3 (count 2 + 1), got %v", result)
	})
	t.Run("WhenSvmDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		result, err := store.GetNextSVMIndexByPoolID(context.Background(), 12)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(1), result, "Expected next index to be 1 (count 0 + 1), got %v", result)
	})
	t.Run("WhenDeletedSvmsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an SVM
		svm1 := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid-1",
			},
			Name:   "test_svm_1",
			PoolID: 1234,
		}
		err = store.db.Create(svm1).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm1: %v", err)
		}

		// Create another SVM
		svm2 := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-svm-uuid-2",
			},
			Name:   "test_svm_2",
			PoolID: 1234,
		}
		err = store.db.Create(svm2).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm2: %v", err)
		}

		// Delete one SVM (soft delete)
		err = store.db.Delete(svm2).Error()
		if err != nil {
			tt.Fatalf("Failed to delete svm2: %v", err)
		}

		// Count should include deleted SVMs for name uniqueness
		result, err := store.GetNextSVMIndexByPoolID(context.Background(), 1234)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(3), result, "Expected next index to be 3 (count 2 including deleted SVM + 1), got %v", result)
	})
}

func TestCreateSVM(t *testing.T) {
	t.Run("WhenSvmIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}

		createdSvm, err := store.CreateSVM(context.Background(), svm)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, svm.Name, createdSvm.Name, "Expected svm name %v, got %v", svm.Name, createdSvm.Name)
	})
	t.Run("WhenSvmAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:   "test_svm",
			PoolID: 1234,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		_, err = store.CreateSVM(context.Background(), svm)
		var customErr *vsaerrors.CustomError
		if errors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "svm already exists")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
	t.Run("WhenDatabaseErrorOccursDuringCheck", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}

		// Drop the table to simulate a database error during the First query
		err = store.db.GORM().Exec("DROP TABLE svms").Error
		assert.NoError(tt, err)

		_, err = store.CreateSVM(context.Background(), svm)
		assert.Error(tt, err, "Expected an error when database query fails")
		var vcpErr *vsaerrors.CustomError
		assert.True(tt, errors.As(err, &vcpErr), "Expected a CustomError")
		assert.Contains(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "no such table")
	})

	// CreateSVM is now responsible for finalizing rows that were pre-allocated in
	// CREATING state by the OCI flow's CreateSvmInCreatingState step. A second call
	// to CreateSVM (with VLM-derived SvmDetails) must upgrade the row to READY in
	// place rather than returning a conflict.
	t.Run("UpgradesCreatingRowToReady", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct"}, Name: "acct"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 7}, Name: "pool", AccountID: account.ID, State: models.LifeCycleStateREADY}
		assert.NoError(tt, store.db.Create(pool).Error())

		// Step 1: pre-allocate row in CREATING (mirrors the OCI workflow's first DB step).
		preallocated, err := store.CreateSvmInCreatingState(context.Background(), &datamodel.Svm{
			Name: "svm-1", AccountID: account.ID, PoolID: pool.ID,
		})
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateCreating, preallocated.State)

		// Step 2: CreateSVM is invoked again with VLM-derived fields populated. This
		// must upgrade the existing row in place rather than fail with conflict.
		finalized, err := store.CreateSVM(context.Background(), &datamodel.Svm{
			Name:                  "svm-1",
			AccountID:             account.ID,
			PoolID:                pool.ID,
			SvmExternalIdentifier: "ocid1.svm..a",
			SvmDetails:            &datamodel.SvmDetails{ExternalUUID: "ext-uuid", IPSpace: "Default"},
		})
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateREADY, finalized.State)
		assert.Equal(tt, models.LifeCycleStateAvailableDetails, finalized.StateDetails)
		assert.Equal(tt, "ocid1.svm..a", finalized.SvmExternalIdentifier)
		if assert.NotNil(tt, finalized.SvmDetails) {
			assert.Equal(tt, "ext-uuid", finalized.SvmDetails.ExternalUUID)
		}
		// Same row (idempotent UUID).
		assert.Equal(tt, preallocated.UUID, finalized.UUID)
	})

	// Temporal retry: an already-READY row must be returned idempotently rather
	// than producing a conflict, so a worker crash between row finalization and
	// activity completion does not strand the workflow.
	t.Run("ReturnsExistingRowWhenAlreadyReady", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct"}, Name: "acct"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 7}, Name: "pool", AccountID: account.ID, State: models.LifeCycleStateREADY}
		assert.NoError(tt, store.db.Create(pool).Error())

		first, err := store.CreateSVM(context.Background(), &datamodel.Svm{
			Name: "svm-1", AccountID: account.ID, PoolID: pool.ID,
		})
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateREADY, first.State)

		second, err := store.CreateSVM(context.Background(), &datamodel.Svm{
			Name: "svm-1", AccountID: account.ID, PoolID: pool.ID,
		})
		assert.NoError(tt, err)
		assert.Equal(tt, first.UUID, second.UUID, "retry should return the same row")
	})
}

func TestCreateSvmInCreatingState(t *testing.T) {
	t.Run("InsertsRowInCreatingState", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct"}, Name: "acct"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 7}, Name: "pool", AccountID: account.ID, State: models.LifeCycleStateREADY}
		assert.NoError(tt, store.db.Create(pool).Error())

		svm, err := store.CreateSvmInCreatingState(context.Background(), &datamodel.Svm{
			Name: "svm-1", AccountID: account.ID, PoolID: pool.ID, SvmExternalIdentifier: "ocid1.svm..a",
		})
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateCreating, svm.State)
		assert.Equal(tt, models.LifeCycleStateCreatingDetails, svm.StateDetails)
		assert.Equal(tt, "ocid1.svm..a", svm.SvmExternalIdentifier)
		assert.NotEmpty(tt, svm.UUID)
	})

	// Idempotent on Temporal retry: must return the existing row instead of
	// erroring (which is the key behavioural difference from CreateSVM's strict
	// conflict-when-state-unknown branch).
	t.Run("IdempotentReturnsExistingRowOnRetry", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct"}, Name: "acct"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 7}, Name: "pool", AccountID: account.ID, State: models.LifeCycleStateREADY}
		assert.NoError(tt, store.db.Create(pool).Error())

		first, err := store.CreateSvmInCreatingState(context.Background(), &datamodel.Svm{
			Name: "svm-1", AccountID: account.ID, PoolID: pool.ID,
		})
		assert.NoError(tt, err)

		second, err := store.CreateSvmInCreatingState(context.Background(), &datamodel.Svm{
			Name: "svm-1", AccountID: account.ID, PoolID: pool.ID,
		})
		assert.NoError(tt, err)
		assert.Equal(tt, first.UUID, second.UUID)
		assert.Equal(tt, models.LifeCycleStateCreating, second.State)
	})
}

func TestDeleteSVM(t *testing.T) {
	t.Run("WhenSvmIsDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		err = store.DeleteSVM(context.Background(), svm)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		deletedSvm := &datamodel.Svm{}
		err = store.db.GORM().First(deletedSvm, "uuid = ?", svm.UUID).Error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected record not found error, got %v", err)
		}
	})
}

func TestDeletingSVM(t *testing.T) {
	t.Run("UpdatesSvmStateToDeletingSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		err = store.DeletingSVM(context.Background(), svm)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedSvm := &datamodel.Svm{}
		err = store.db.GORM().First(updatedSvm, "uuid = ?", svm.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated svm: %v", err)
		}
		if updatedSvm.State != models.LifeCycleStateDeleting {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateDeleting, updatedSvm.State)
		}
		if updatedSvm.StateDetails != models.LifeCycleStateDeletingDetails {
			tt.Errorf("Expected state details %v, got %v", models.LifeCycleStateDeletingDetails, updatedSvm.StateDetails)
		}
	})
}

func TestGetSvmForPoolID(t *testing.T) {
	t.Run("WhenSvmExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:   "test_svm",
			PoolID: 1234,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		result, err := store.GetSvmForPoolID(context.Background(), svm.PoolID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, svm.Name, result.Name, "Expected svm name %v, got %v", svm.Name, result.Name)
		assert.Equal(tt, svm.PoolID, result.PoolID, "Expected svm pool id %v, got %v", svm.PoolID, result.PoolID)
	})
}

func TestGetSvmByKmsId(t *testing.T) {
	t.Run("WhenExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		kms := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-kms-uuid",
			},
			Name: "test_kms",
		}

		svm := &datamodel.Svm{
			BaseModel:   datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:        "test_svm",
			KmsConfigID: sql.NullInt64{Int64: kms.ID, Valid: true},
		}
		err = store.db.Create(kms).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		result, err := store.GetSvmsByKmsConfigID(context.Background(), 1)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result[0].Name != svm.Name {
			tt.Errorf("Expected svm name %v, got %v", svm.Name, result[0].Name)
		}
	})

	t.Run("WhenDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		kms := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-kms-uuid",
			},
			Name: "test_kms",
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
		}
		err = store.db.Create(kms).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		svms, err := store.GetSvmsByKmsConfigID(context.Background(), 1)
		if err != nil {
			tt.Errorf("Expected nil, got error")
		}
		assert.Equal(tt, 0, len(svms), "Expected no SVMs to be returned when KMS ID does not match")
	})
}

func TestErroredSVM(t *testing.T) {
	t.Run("WhenSvmIsMarkedErroredSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_svm",
			AccountID: int64(10),
			PoolID:    1234,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		errMsg := "error during svm update"
		err = store.ErroredSVM(context.Background(), svm, errMsg)
		assert.NoError(tt, err)

		updatedSvm := &datamodel.Svm{}
		err = store.db.GORM().First(updatedSvm, "uuid = ?", svm.UUID).Error
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateError, updatedSvm.State)
		assert.Equal(tt, errMsg, updatedSvm.StateDetails)
		assert.WithinDuration(tt, time.Now(), updatedSvm.UpdatedAt, 2*time.Second)
	})

	t.Run("WhenUpdatingSvmFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-svm-uuid-2",
			},
			Name:      "failing_svm",
			AccountID: int64(20),
			PoolID:    5678,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		err = store.db.GORM().Exec("DROP TABLE svms").Error
		assert.NoError(tt, err)

		errMsg := "simulated update error"
		err = store.ErroredSVM(context.Background(), svm, errMsg)
		assert.Error(tt, err)
		var vcpErr *vsaerrors.CustomError
		assert.True(tt, errors.As(err, &vcpErr))
		assert.Contains(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "no such table")
	})
}

func TestGetSvmsByKmsConfigID(t *testing.T) {
	t.Run("UpdateSvmWithKmsConfigIDsReturnsErrorWhenKmsConfigNotFound", func(t *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err)

		svm := &datamodel.Svm{
			BaseModel:  datamodel.BaseModel{ID: 1, UUID: "svm-uuid"},
			Name:       "test_svm",
			SvmDetails: &datamodel.SvmDetails{},
		}
		err = store.db.Create(svm).Error()
		assert.NoError(t, err)

		updated, err := store.UpdateSvmWithKmsConfigIDs(context.Background(), svm, "non-existent-uuid", "external-uuid")
		assert.Error(t, err)
		assert.Nil(t, updated)
	})
	t.Run("UpdateSvmWithKmsConfigIDsReturnsErrorOnSaveFailure", func(t *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err)

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "kms-uuid"},
			Name:      "test_kms",
		}
		err = store.db.Create(kmsConfig).Error()
		assert.NoError(t, err)

		svm := &datamodel.Svm{
			BaseModel:  datamodel.BaseModel{ID: 1, UUID: "svm-uuid"},
			Name:       "test_svm",
			SvmDetails: &datamodel.SvmDetails{},
		}
		err = store.db.Create(svm).Error()
		assert.NoError(t, err)

		// Simulate error by closing the DB connection
		sqlDB, _ := db.DB()
		err = sqlDB.Close()
		assert.NoError(t, err)

		updated, err := store.UpdateSvmWithKmsConfigIDs(context.Background(), svm, "kms-uuid", "external-uuid")
		assert.Error(t, err)
		assert.Nil(t, updated)
	})
}

func TestListSvmsWithAccountId(t *testing.T) {
	t.Run("WhenSoftDeletedSvmsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		// soft delete
		svm.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
		assert.NoError(tt, store.db.GORM().Unscoped().Save(svm).Error)

		svms, err := store.ListSvmsWithAccountId(context.Background(), account.ID)
		assert.NoError(tt, err)
		// soft-deleted SVMs should not be returned by the non-unscoped listing
		assert.Len(tt, svms, 0)
	})

	t.Run("WhenNoSoftDeletedSvms", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		svms, err := store.ListSvmsWithAccountId(context.Background(), 9999)
		assert.NoError(tt, err)
		assert.Empty(tt, svms)
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		assert.NoError(tt, sqlDB.Close())

		svms, err := store.ListSvmsWithAccountId(context.Background(), 1)
		assert.Error(tt, err)
		assert.Nil(tt, svms)
	})
}

func TestGetSvmByExternalIdentifier(t *testing.T) {
	const externalID = "ocid1.svm.oc1..aaaa"

	t.Run("WhenSvmExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		svm := &datamodel.Svm{
			BaseModel:             datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:                  "test_svm",
			SvmExternalIdentifier: externalID,
			AccountID:             account.ID,
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		got, err := store.GetSvmByExternalIdentifier(context.Background(), externalID, account.ID)
		assert.NoError(tt, err)
		assert.NotNil(tt, got)
		assert.Equal(tt, svm.UUID, got.UUID)
	})

	t.Run("WhenSoftDeleted_ReturnsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		svm := &datamodel.Svm{
			BaseModel:             datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:                  "test_svm",
			SvmExternalIdentifier: externalID,
			AccountID:             account.ID,
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		svm.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
		assert.NoError(tt, store.db.GORM().Unscoped().Save(svm).Error)

		got, err := store.GetSvmByExternalIdentifier(context.Background(), externalID, account.ID)
		assert.Error(tt, err)
		assert.Nil(tt, got)
	})

	t.Run("WhenAccountMismatch_ReturnsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		svm := &datamodel.Svm{
			BaseModel:             datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:                  "test_svm",
			SvmExternalIdentifier: externalID,
			AccountID:             account.ID,
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		got, err := store.GetSvmByExternalIdentifier(context.Background(), externalID, 9999)
		assert.Error(tt, err)
		assert.Nil(tt, got)
	})

	t.Run("WhenSvmDoesNotExist_ReturnsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		got, err := store.GetSvmByExternalIdentifier(context.Background(), externalID, 1)
		assert.Error(tt, err)
		assert.Nil(tt, got)
	})
}

func TestUnsetSvmActiveDirectoryID(t *testing.T) {
	t.Run("WhenSvmActiveDirectoryIsUnsetSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			ActiveDirectoryID: sql.NullInt64{
				Int64: 1,
				Valid: true,
			},
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		updatedSvm, err := store.UnsetSvmActiveDirectoryID(context.Background(), svm)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedSvm)
		assert.False(tt, updatedSvm.ActiveDirectoryID.Valid)

		// Verify in database
		verifySvm := &datamodel.Svm{}
		err = store.db.GORM().First(verifySvm, "uuid = ?", svm.UUID).Error
		assert.NoError(tt, err)
		assert.False(tt, verifySvm.ActiveDirectoryID.Valid)
	})

	t.Run("WhenTransactionStartFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Close the database connection to simulate transaction start failure
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		assert.NoError(tt, sqlDB.Close())

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
		}

		updatedSvm, err := store.UnsetSvmActiveDirectoryID(context.Background(), svm)
		assert.Error(tt, err)
		assert.Nil(tt, updatedSvm)
	})

	t.Run("WhenSaveFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		// Drop the table to simulate save failure
		err = store.db.GORM().Exec("DROP TABLE svms").Error
		assert.NoError(tt, err)

		updatedSvm, err := store.UnsetSvmActiveDirectoryID(context.Background(), svm)
		assert.Error(tt, err)
		assert.Nil(tt, updatedSvm)
		var vcpErr *vsaerrors.CustomError
		assert.True(tt, errors.As(err, &vcpErr))
	})
}

func TestDataStoreRepository_GetSvmByExternalUUID(t *testing.T) {
	t.Run("WhenSvmExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-external-1"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-external-1"},
			Name:           "test_pool",
			AccountID:      account.ID,
			State:          models.LifeCycleStateREADY,
			DeploymentName: "deployment-1",
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		const externalUUID = "550e8400-e29b-41d4-a716-446655440000"
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "svm-external-1"},
			Name:      "test_svm",
			PoolID:    pool.ID,
			AccountID: account.ID,
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: externalUUID,
				IPSpace:      "Default",
			},
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		result, err := store.GetSvmByExternalUUID(context.Background(), externalUUID, pool.ID)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, svm.UUID, result.UUID)
		assert.Equal(tt, externalUUID, result.SvmDetails.ExternalUUID)
	})

	t.Run("WhenSvmDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		result, err := store.GetSvmByExternalUUID(context.Background(), "missing-external", 999)
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenPoolDoesNotMatch", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-external-2"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		sourcePool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-source"},
			Name:           "source_pool",
			AccountID:      account.ID,
			State:          models.LifeCycleStateREADY,
			DeploymentName: "deployment-source",
		}
		assert.NoError(tt, store.db.Create(sourcePool).Error())

		targetPool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-target"},
			Name:           "target_pool",
			AccountID:      account.ID,
			State:          models.LifeCycleStateREADY,
			DeploymentName: "deployment-target",
		}
		assert.NoError(tt, store.db.Create(targetPool).Error())

		const externalUUID = "550e8400-e29b-41d4-a716-446655440001"
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "svm-external-2"},
			Name:      "test_svm",
			PoolID:    sourcePool.ID,
			AccountID: account.ID,
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: externalUUID,
				IPSpace:      "Default",
			},
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		result, err := store.GetSvmByExternalUUID(context.Background(), externalUUID, targetPool.ID)
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenMultipleSvmsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-external-3"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-external-3"},
			Name:           "test_pool",
			AccountID:      account.ID,
			State:          models.LifeCycleStateREADY,
			DeploymentName: "deployment-3",
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		svm1 := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "svm-external-3a"},
			Name:      "svm-1",
			PoolID:    pool.ID,
			AccountID: account.ID,
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "550e8400-e29b-41d4-a716-446655440010",
				IPSpace:      "Default",
			},
		}
		assert.NoError(tt, store.db.Create(svm1).Error())

		svm2 := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "svm-external-3b"},
			Name:      "svm-2",
			PoolID:    pool.ID,
			AccountID: account.ID,
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "550e8400-e29b-41d4-a716-446655440011",
				IPSpace:      "Default",
			},
		}
		assert.NoError(tt, store.db.Create(svm2).Error())

		result1, err := store.GetSvmByExternalUUID(context.Background(), svm1.SvmDetails.ExternalUUID, pool.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, svm1.UUID, result1.UUID)

		result2, err := store.GetSvmByExternalUUID(context.Background(), svm2.SvmDetails.ExternalUUID, pool.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, svm2.UUID, result2.UUID)

		assert.NotEqual(tt, result1.UUID, result2.UUID)
	})
}

func TestUpdateSvmCurrentKmsKeyID(t *testing.T) {
	t.Run("WhenSvmExistsWithSvmDetails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID:    "external-uuid",
				IPSpace:         "Default",
				CurrentKmsKeyID: "old-key-id",
			},
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		newKeyID := "new-key-id"
		err = store.UpdateSvmCurrentKmsKeyID(context.Background(), svm.UUID, newKeyID)
		assert.NoError(tt, err)

		// Verify the update
		updatedSvm := &datamodel.Svm{}
		err = store.db.GORM().First(updatedSvm, "uuid = ?", svm.UUID).Error
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedSvm.SvmDetails)
		assert.Equal(tt, newKeyID, updatedSvm.SvmDetails.CurrentKmsKeyID)
		assert.WithinDuration(tt, time.Now(), updatedSvm.UpdatedAt, 2*time.Second)
	})

	t.Run("WhenSvmExistsWithoutSvmDetails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		svm := &datamodel.Svm{
			BaseModel:  datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:       "test_svm",
			AccountID:  account.ID,
			PoolID:     pool.ID,
			SvmDetails: nil,
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		newKeyID := "new-key-id"
		err = store.UpdateSvmCurrentKmsKeyID(context.Background(), svm.UUID, newKeyID)
		assert.NoError(tt, err)

		// Verify the update - SvmDetails should be initialized
		updatedSvm := &datamodel.Svm{}
		err = store.db.GORM().First(updatedSvm, "uuid = ?", svm.UUID).Error
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedSvm.SvmDetails)
		assert.Equal(tt, newKeyID, updatedSvm.SvmDetails.CurrentKmsKeyID)
		assert.WithinDuration(tt, time.Now(), updatedSvm.UpdatedAt, 2*time.Second)
	})

	t.Run("WhenSvmNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		err = store.UpdateSvmCurrentKmsKeyID(context.Background(), "non-existent-uuid", "key-id")
		assert.Error(tt, err)
		var vcpErr *vsaerrors.CustomError
		assert.True(tt, errors.As(err, &vcpErr))
		assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, vcpErr.TrackingID)
	})

	t.Run("WhenTransactionStartFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Close the database connection to simulate transaction start failure
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		assert.NoError(tt, sqlDB.Close())

		err = store.UpdateSvmCurrentKmsKeyID(context.Background(), "test-uuid", "key-id")
		assert.Error(tt, err)
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "external-uuid",
			},
		}
		assert.NoError(tt, store.db.Create(svm).Error())

		// Drop the table to simulate database error
		// This causes First() to fail with ErrDatabaseDataReadError
		// (Save() would fail with ErrDatabaseDataUpdateError, but First() fails first)
		err = store.db.GORM().Exec("DROP TABLE svms").Error
		assert.NoError(tt, err)

		err = store.UpdateSvmCurrentKmsKeyID(context.Background(), svm.UUID, "key-id")
		assert.Error(tt, err)
		var vcpErr *vsaerrors.CustomError
		assert.True(tt, errors.As(err, &vcpErr))
		// When table is dropped, First() fails first, so we get ErrDatabaseDataReadError
		assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, vcpErr.TrackingID)
	})
}
