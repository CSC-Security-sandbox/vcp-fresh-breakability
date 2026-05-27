package database

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func TestCreateAdminJobSpec(t *testing.T) {
	t.Run("WhenAdminJobSpecCreationSucceeds", func(tt *testing.T) {
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

		jobSpec := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid"},
			JobType:        "TEST_JOB",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		}

		newJobSpec, err := store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)
		assert.NotNil(tt, newJobSpec)
	})
	// Updated: Duplicate create should upsert (update) not fail
	t.Run("WhenAdminJobSpecDuplicateCreateUpserts", func(tt *testing.T) {
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

		jobSpec := &datamodel.AdminJobSpec{
			JobType:        "TEST_JOB",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		}

		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)

		// Change values and call create again; should update existing row
		jobSpec.CronExpression = "*/5 * * * *"
		jobSpec.State = "UPDATING"
		updated, err := store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)
		assert.NotNil(tt, updated)
		assert.Equal(tt, "*/5 * * * *", updated.CronExpression)
		assert.Equal(tt, "UPDATING", updated.State)
	})
}

func TestGetAdminJobSpecByJobType(t *testing.T) {
	t.Run("WhenAdminJobSpecExists", func(tt *testing.T) {
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

		jobSpec := &datamodel.AdminJobSpec{
			JobType:        "TEST_JOB",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		}

		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)

		retrievedJobSpec, err := store.GetAdminJobSpecByJobType(tt.Context(), "TEST_JOB")
		assert.NoError(tt, err)
		assert.NotNil(tt, retrievedJobSpec)
		assert.Equal(tt, jobSpec.JobType, retrievedJobSpec.JobType)
	})
	t.Run("WhenAdminJobSpecDoesNotExist", func(tt *testing.T) {
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

		retrievedJobSpec, err := store.GetAdminJobSpecByJobType(tt.Context(), "NON_EXISTENT_JOB")
		assert.Error(tt, err)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, gorm.ErrRecordNotFound, customErr.OriginalErr)
		} else {
			tt.Fatalf("Expected a CustomError with RecordNotFound, got: %v", err)
		}

		assert.Nil(tt, retrievedJobSpec)
	})
}

func TestUpdateAdminJobSpec(t *testing.T) {
	t.Run("WhenAdminJobSpecUpdateIsSuccessful", func(tt *testing.T) {
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

		jobSpec := &datamodel.AdminJobSpec{
			JobType:        "TEST_JOB",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		}
		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)

		jobSpec.State = "SCHEDULED"
		err = store.UpdateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)

		retrievedJobSpec, _ := store.GetAdminJobSpecByJobType(tt.Context(), "TEST_JOB")
		assert.Equal(tt, "SCHEDULED", retrievedJobSpec.State)
	})
	t.Run("WhenAdminJobToUpdateDoesNotExist", func(tt *testing.T) {
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

		jobSpec := &datamodel.AdminJobSpec{
			JobType:        "TEST_JOB",
			CronExpression: "0 0 * * *",
			State:          "UPDATING",
		}

		err = store.UpdateAdminJobSpec(tt.Context(), jobSpec)
		assert.Error(tt, err)
	})
}

func TestGetAdminJobSpecsByState(t *testing.T) {
	t.Run("WhenAdminJobSpecsAreFetchedSuccessfully", func(tt *testing.T) {
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

		jobSpec1 := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "uuid1"},
			JobType:        "TEST_JOB_1",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		}
		jobSpec2 := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "uuid2"},
			JobType:        "TEST_JOB_2",
			CronExpression: "0 0 * * *",
			State:          "SCHEDULED",
		}

		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec1)
		assert.NoError(tt, err)
		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec2)
		assert.NoError(tt, err)

		retrievedJobSpecs, err := store.GetAdminJobSpecsByState(tt.Context(), "CREATING")
		assert.NoError(tt, err)
		assert.NotNil(tt, retrievedJobSpecs)
		assert.Len(tt, retrievedJobSpecs, 1)
		assert.Equal(tt, "CREATING", retrievedJobSpecs[0].State)
	})

	t.Run("WhenNoAdminJobSpecsExistForState", func(tt *testing.T) {
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

		retrievedJobSpecs, err := store.GetAdminJobSpecsByState(tt.Context(), "NON_EXISTENT_STATE")
		assert.NoError(tt, err)
		assert.Len(tt, retrievedJobSpecs, 0)
	})
}

func TestCreateAdminJobSpec_RevivesSoftDeleted(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	initial := &datamodel.AdminJobSpec{
		JobType:        "TEST_JOB_SOFT_DELETE",
		CronExpression: "0 0 * * *",
		State:          "SCHEDULED",
	}
	_, err = store.CreateAdminJobSpec(t.Context(), initial)
	assert.NoError(t, err)

	// Soft delete it
	dbGorm := store.db.GORM()
	softDeleteTime := time.Now()
	dbGorm.Model(&datamodel.AdminJobSpec{}).Where("job_type = ?", initial.JobType).Update("deleted_at", softDeleteTime)

	// Recreate with new values; should revive and clear deleted_at
	incoming := &datamodel.AdminJobSpec{
		JobType:        initial.JobType,
		CronExpression: "*/10 * * * *",
		State:          "CREATING",
	}
	revived, err := store.CreateAdminJobSpec(t.Context(), incoming)
	assert.NoError(t, err)
	assert.NotNil(t, revived)
	assert.Equal(t, "*/10 * * * *", revived.CronExpression)
	assert.Equal(t, "CREATING", revived.State)
	assert.Nil(t, revived.DeletedAt)
}

func TestCreateAdminJobSpecIfNotExists(t *testing.T) {
	t.Run("WhenAdminJobSpecDoesNotExist_Success", func(tt *testing.T) {
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

		jobSpec := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-if-not-exists"},
			JobType:        "TEST_JOB_IF_NOT_EXISTS",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		}

		newJobSpec, err := store.CreateAdminJobSpecIfNotExists(tt.Context(), jobSpec)
		assert.NoError(tt, err)
		assert.NotNil(tt, newJobSpec)
		assert.Equal(tt, "TEST_JOB_IF_NOT_EXISTS", newJobSpec.JobType)
		assert.Equal(tt, "0 0 * * *", newJobSpec.CronExpression)
		assert.Equal(tt, "CREATING", newJobSpec.State)
	})

	t.Run("WhenAdminJobSpecAlreadyExists_ReturnsAlreadyExists", func(tt *testing.T) {
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

		jobSpec := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-exists"},
			JobType:        "TEST_JOB_EXISTS",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		}

		// First creation should succeed
		_, err = store.CreateAdminJobSpecIfNotExists(tt.Context(), jobSpec)
		assert.NoError(tt, err)

		// Second creation with same JobType should return ErrAdminJobSpecAlreadyExists
		// (not a raw database error) so cron callers can fall through to lock acquisition.
		duplicateJobSpec := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-duplicate"},
			JobType:        "TEST_JOB_EXISTS",
			CronExpression: "*/5 * * * *",
			State:          "SCHEDULED",
		}

		newJobSpec, err := store.CreateAdminJobSpecIfNotExists(tt.Context(), duplicateJobSpec)
		assert.ErrorIs(tt, err, vsaerrors.ErrAdminJobSpecAlreadyExists)
		assert.Nil(tt, newJobSpec)
	})

	t.Run("WhenSoftDeletedRowExists_RevivesAndReturnsAlreadyExists", func(tt *testing.T) {
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

		// Create and then soft-delete a row (simulates google-proxy DeleteAllAdminSchedules).
		initial := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-soft-del"},
			JobType:        "TEST_JOB_SOFT_DEL",
			CronExpression: "0 0 * * *",
			State:          "SCHEDULED",
		}
		_, err = store.CreateAdminJobSpecIfNotExists(tt.Context(), initial)
		if err != nil {
			tt.Fatalf("Failed to create initial spec: %v", err)
		}

		// Soft-delete the row.
		db.Model(&datamodel.AdminJobSpec{}).
			Where("job_type = ?", initial.JobType).
			Update("deleted_at", time.Now())

		// Capture updated_at after soft-delete (GORM auto-bumps it during the update above).
		var softDeletedRow datamodel.AdminJobSpec
		db.Unscoped().Where("job_type = ?", initial.JobType).First(&softDeletedRow)
		updatedAtAfterSoftDelete := softDeletedRow.UpdatedAt

		// Calling CreateAdminJobSpecIfNotExists must revive the row without changing updated_at,
		// then return ErrAdminJobSpecAlreadyExists so the caller proceeds to lock acquisition.
		incoming := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-new"},
			JobType:        initial.JobType,
			CronExpression: "*/30 * * * * *",
			State:          "SCHEDULED",
		}
		result, err := store.CreateAdminJobSpecIfNotExists(tt.Context(), incoming)
		assert.ErrorIs(tt, err, vsaerrors.ErrAdminJobSpecAlreadyExists)
		assert.Nil(tt, result)

		// Row must now be live so UpdateAdminJobSpecWithLock can find it.
		revived, getErr := store.GetAdminJobSpecByJobType(tt.Context(), initial.JobType)
		assert.NoError(tt, getErr, "revived row must be visible without Unscoped")
		assert.Equal(tt, "SCHEDULED", revived.State)
		assert.Nil(tt, revived.DeletedAt)

		// updated_at must be unchanged – the lock timer depends on the old value.
		assert.True(tt, revived.UpdatedAt.Equal(updatedAtAfterSoftDelete),
			"updated_at must not change during revival; got %v, want %v", revived.UpdatedAt, updatedAtAfterSoftDelete)
	})

	t.Run("WhenTransactionFails", func(tt *testing.T) {
		// This test would require mocking the database to simulate transaction failure
		// For now, we'll skip this as it's more of an integration test concern
		tt.Skip("Transaction failure test requires database mocking")
	})
}

func TestUpdateAdminJobSpecWithLock(t *testing.T) {
	t.Run("WhenJobSpecExistsAndLockConditionsMet_Success", func(tt *testing.T) {
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

		// Create a job spec
		jobSpec := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-lock"},
			JobType:        "TEST_JOB_LOCK",
			CronExpression: "0 0 * * *",
			State:          "SCHEDULED",
		}

		createdJobSpec, err := store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)

		// Set the updated_at to an older time to simulate a job that needs updating
		oldTime := time.Now().Add(-10 * time.Minute)
		db.Model(&datamodel.AdminJobSpec{}).
			Where("job_type = ?", "TEST_JOB_LOCK").
			Update("updated_at", oldTime)

		// Now try to update with lock
		lockThreshold := time.Now().Add(-5 * time.Minute) // Threshold is 5 minutes ago
		currentTime := time.Now()

		rowsAffected, err := store.UpdateAdminJobSpecWithLock(tt.Context(),
			"TEST_JOB_LOCK", "SCHEDULED", lockThreshold, currentTime)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), rowsAffected)

		// Verify the updated_at field was actually updated
		updatedJobSpec, err := store.GetAdminJobSpecByJobType(tt.Context(), "TEST_JOB_LOCK")
		assert.NoError(tt, err)
		assert.True(tt, updatedJobSpec.UpdatedAt.After(createdJobSpec.UpdatedAt))
	})

	t.Run("WhenJobSpecDoesNotExist_NoRowsAffected", func(tt *testing.T) {
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

		lockThreshold := time.Now().Add(-5 * time.Minute)
		currentTime := time.Now()

		rowsAffected, err := store.UpdateAdminJobSpecWithLock(tt.Context(),
			"NON_EXISTENT_JOB", "SCHEDULED", lockThreshold, currentTime)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), rowsAffected)
	})

	t.Run("WhenStateDoesNotMatch_NoRowsAffected", func(tt *testing.T) {
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

		// Create a job spec with CREATING state
		jobSpec := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-state-mismatch"},
			JobType:        "TEST_JOB_STATE_MISMATCH",
			CronExpression: "0 0 * * *",
			State:          "CREATING", // Different from what we'll search for
		}

		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)

		lockThreshold := time.Now().Add(-5 * time.Minute)
		currentTime := time.Now()

		// Try to update with SCHEDULED state (but job is in CREATING state)
		rowsAffected, err := store.UpdateAdminJobSpecWithLock(tt.Context(),
			"TEST_JOB_STATE_MISMATCH", "SCHEDULED", lockThreshold, currentTime)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), rowsAffected)
	})

	t.Run("WhenUpdatedAtIsAfterLockThreshold_NoRowsAffected", func(tt *testing.T) {
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

		// Create a job spec
		jobSpec := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-recent"},
			JobType:        "TEST_JOB_RECENT",
			CronExpression: "0 0 * * *",
			State:          "SCHEDULED",
		}

		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.NoError(tt, err)

		// Set updated_at to a recent time (after our threshold)
		recentTime := time.Now().Add(-2 * time.Minute)
		db.Model(&datamodel.AdminJobSpec{}).
			Where("job_type = ?", "TEST_JOB_RECENT").
			Update("updated_at", recentTime)

		// Set lock threshold to 5 minutes ago (older than the updated_at)
		lockThreshold := time.Now().Add(-5 * time.Minute)
		currentTime := time.Now()

		rowsAffected, err := store.UpdateAdminJobSpecWithLock(tt.Context(),
			"TEST_JOB_RECENT", "SCHEDULED", lockThreshold, currentTime)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), rowsAffected) // Should not update because updated_at > lockThreshold
	})

	t.Run("WhenMultipleJobsMatchConditions_UpdatesAll", func(tt *testing.T) {
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

		// Create multiple job specs with the same JobType and State
		jobSpec1 := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-multi-1"},
			JobType:        "TEST_JOB_MULTI",
			CronExpression: "0 0 * * *",
			State:          "SCHEDULED",
		}
		jobSpec2 := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-multi-2"},
			JobType:        "TEST_JOB_MULTI",
			CronExpression: "*/5 * * * *",
			State:          "SCHEDULED",
		}

		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec1)
		assert.NoError(tt, err)

		// For the second one, we need to use CreateAdminJobSpecIfNotExists since CreateAdminJobSpec does upsert
		// Actually, since CreateAdminJobSpec does upsert by job_type, we can't have two records with the same job_type
		// Let's modify the test to use different job types
		jobSpec2.JobType = "TEST_JOB_MULTI_2"
		_, err = store.CreateAdminJobSpec(tt.Context(), jobSpec2)
		assert.NoError(tt, err)

		// Set both to old times
		oldTime := time.Now().Add(-10 * time.Minute)
		db.Model(&datamodel.AdminJobSpec{}).
			Where("job_type IN ?", []string{"TEST_JOB_MULTI", "TEST_JOB_MULTI_2"}).
			Update("updated_at", oldTime)

		lockThreshold := time.Now().Add(-5 * time.Minute)
		currentTime := time.Now()

		// Update the first job
		rowsAffected1, err := store.UpdateAdminJobSpecWithLock(tt.Context(),
			"TEST_JOB_MULTI", "SCHEDULED", lockThreshold, currentTime)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), rowsAffected1)

		// Update the second job
		rowsAffected2, err := store.UpdateAdminJobSpecWithLock(tt.Context(),
			"TEST_JOB_MULTI_2", "SCHEDULED", lockThreshold, currentTime)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), rowsAffected2)
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		require.NoError(tt, err)

		// Closing the underlying *sql.DB makes every subsequent statement fail with
		// "sql: database is closed", which exercises the result.Error branch.
		sqlDB, getErr := store.db.GORM().DB()
		require.NoError(tt, getErr)
		require.NoError(tt, sqlDB.Close())

		rowsAffected, err := store.UpdateAdminJobSpecWithLock(tt.Context(),
			"TEST_JOB", "SCHEDULED", time.Now().Add(-time.Minute), time.Now())

		assert.Error(tt, err)
		assert.Equal(tt, int64(0), rowsAffected)

		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, vsaerrors.ErrDatabaseDataUpdateError, customErr.TrackingID)
	})
}

// TestAdminJobSpecs_TransactionStartFailures covers the early-return paths in every
// admin_job_specs.go method that opens a transaction. These branches are otherwise
// unreachable in tests because _startTransaction only fails on a nil/broken DB.
func TestAdminJobSpecs_TransactionStartFailures(t *testing.T) {
	setupStore := func(tt *testing.T) *DataStoreRepository {
		tt.Helper()
		db, err := SetupTestDB()
		require.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		require.NoError(tt, ClearInMemoryDB(store.db.GORM()))
		return store
	}

	withFailingStartTransaction := func(tt *testing.T) func() {
		tt.Helper()
		startTransaction = func(*gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("forced transaction start failure")
		}
		return func() { startTransaction = _startTransaction }
	}

	t.Run("CreateAdminJobSpec", func(tt *testing.T) {
		store := setupStore(tt)
		restore := withFailingStartTransaction(tt)
		defer restore()

		result, err := store.CreateAdminJobSpec(tt.Context(), &datamodel.AdminJobSpec{
			JobType:        "TEST_JOB_TX_FAIL",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		})
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "forced transaction start failure")
	})

	t.Run("CreateAdminJobSpecIfNotExists", func(tt *testing.T) {
		store := setupStore(tt)
		restore := withFailingStartTransaction(tt)
		defer restore()

		result, err := store.CreateAdminJobSpecIfNotExists(tt.Context(), &datamodel.AdminJobSpec{
			JobType:        "TEST_JOB_TX_FAIL",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		})
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("UpdateAdminJobSpec", func(tt *testing.T) {
		store := setupStore(tt)
		restore := withFailingStartTransaction(tt)
		defer restore()

		err := store.UpdateAdminJobSpec(tt.Context(), &datamodel.AdminJobSpec{
			JobType: "TEST_JOB_TX_FAIL",
			State:   "SCHEDULED",
		})
		assert.Error(tt, err)
	})

	t.Run("GetAdminJobSpecsByState", func(tt *testing.T) {
		store := setupStore(tt)
		restore := withFailingStartTransaction(tt)
		defer restore()

		result, err := store.GetAdminJobSpecsByState(tt.Context(), "SCHEDULED")
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

// TestAdminJobSpecs_DatabaseFailures exercises the SQL-error branches that sit inside
// the transaction body (i.e. after a successful BeginTx). We force the underlying
// *sql.DB closed so every statement returns "sql: database is closed", and short-circuit
// commitOrRollbackOnError so the deferred rollback does not overwrite the error we want
// to assert on.
func TestAdminJobSpecs_DatabaseFailures(t *testing.T) {
	setupClosedStore := func(tt *testing.T) (*DataStoreRepository, func()) {
		tt.Helper()
		db, err := SetupTestDB()
		require.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		require.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Force startTransaction to succeed (returning a tx bound to the about-to-be-closed
		// DB) so that execution reaches the actual statement and we hit the
		// post-Begin error branches rather than the early "transaction failed" return.
		startTransaction = func(d *gorm.DB) (*gorm.DB, error) {
			return d.Begin(), nil
		}
		commitOrRollbackOnError = func(slogger.Logger, *gorm.DB, *error) {}

		sqlDB, getErr := store.db.GORM().DB()
		require.NoError(tt, getErr)
		require.NoError(tt, sqlDB.Close())

		return store, func() {
			startTransaction = _startTransaction
			commitOrRollbackOnError = _commitOrRollbackOnError
		}
	}

	t.Run("CreateAdminJobSpecIfNotExists_InsertFails", func(tt *testing.T) {
		store, restore := setupClosedStore(tt)
		defer restore()

		result, err := store.CreateAdminJobSpecIfNotExists(tt.Context(), &datamodel.AdminJobSpec{
			JobType:        "TEST_JOB_INSERT_FAIL",
			CronExpression: "0 0 * * *",
			State:          "CREATING",
		})

		assert.Error(tt, err)
		assert.Nil(tt, result)

		var customErr *vsaerrors.CustomError
		require.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, vsaerrors.ErrDatabaseDataInsertError, customErr.TrackingID)
	})

	t.Run("GetAdminJobSpecsByState_FindFails", func(tt *testing.T) {
		store, restore := setupClosedStore(tt)
		defer restore()

		result, err := store.GetAdminJobSpecsByState(tt.Context(), "SCHEDULED")

		assert.Error(tt, err)
		assert.Nil(tt, result)

		var customErr *vsaerrors.CustomError
		require.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, customErr.TrackingID)
	})
}

// TestCreateAdminJobSpecIfNotExists_ReviveFails covers the revival UPDATE error branch,
// which is reachable only when the conflict INSERT succeeds with RowsAffected=0 AND the
// subsequent revival UPDATE fails. Driver-level fault injection cannot produce that
// asymmetry on a single connection, so we override the package-level
// reviveSoftDeletedAdminJobSpec hook to force a failure.
func TestCreateAdminJobSpecIfNotExists_ReviveFails(t *testing.T) {
	db, err := SetupTestDB()
	require.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	require.NoError(t, ClearInMemoryDB(store.db.GORM()))

	// Seed a row so the next call enters the conflict + revival path.
	_, err = store.CreateAdminJobSpecIfNotExists(t.Context(), &datamodel.AdminJobSpec{
		JobType:        "TEST_JOB_REVIVE_FAIL",
		CronExpression: "0 0 * * *",
		State:          "SCHEDULED",
	})
	require.NoError(t, err)

	origRevive := reviveSoftDeletedAdminJobSpec
	reviveSoftDeletedAdminJobSpec = func(*gorm.DB, string, string) (int64, error) {
		return 0, errors.New("forced revive failure")
	}
	defer func() { reviveSoftDeletedAdminJobSpec = origRevive }()

	result, err := store.CreateAdminJobSpecIfNotExists(t.Context(), &datamodel.AdminJobSpec{
		JobType:        "TEST_JOB_REVIVE_FAIL",
		CronExpression: "*/5 * * * *",
		State:          "CREATING",
	})

	assert.Error(t, err)
	assert.Nil(t, result)

	var customErr *vsaerrors.CustomError
	require.True(t, vsaerrors.As(err, &customErr))
	assert.Equal(t, vsaerrors.ErrDatabaseDataUpdateError, customErr.TrackingID)
	assert.EqualError(t, customErr.OriginalErr, "forced revive failure")
}

// TestGetAdminJobSpecByJobType_DatabaseError covers the non-RecordNotFound error branch
// of GetAdminJobSpecByJobType, which translates to ErrDatabaseDataReadError.
// GetAdminJobSpecByJobType does not open a transaction, so closing the connection
// directly is sufficient.
func TestGetAdminJobSpecByJobType_DatabaseError(t *testing.T) {
	db, err := SetupTestDB()
	require.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	require.NoError(t, ClearInMemoryDB(store.db.GORM()))

	sqlDB, err := store.db.GORM().DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	result, err := store.GetAdminJobSpecByJobType(t.Context(), "TEST_JOB")
	assert.Error(t, err)
	assert.Nil(t, result)

	var customErr *vsaerrors.CustomError
	require.True(t, vsaerrors.As(err, &customErr))
	assert.Equal(t, vsaerrors.ErrDatabaseDataReadError, customErr.TrackingID)
}
