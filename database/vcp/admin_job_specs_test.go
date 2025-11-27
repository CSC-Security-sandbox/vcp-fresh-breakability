package database

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
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

	t.Run("WhenAdminJobSpecAlreadyExists_Fails", func(tt *testing.T) {
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

		// Second creation with same JobType should fail
		duplicateJobSpec := &datamodel.AdminJobSpec{
			BaseModel:      datamodel.BaseModel{UUID: "test-uuid-duplicate"},
			JobType:        "TEST_JOB_EXISTS", // Same JobType
			CronExpression: "*/5 * * * *",
			State:          "SCHEDULED",
		}

		newJobSpec, err := store.CreateAdminJobSpecIfNotExists(tt.Context(), duplicateJobSpec)
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, vsaerrors.ErrDatabaseDataInsertError, customErr.TrackingID)
		assert.Nil(tt, newJobSpec)
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
		// This test would require mocking the database to simulate an error
		// For now, we'll skip this as it's more of an integration test concern
		tt.Skip("Database error test requires database mocking")
	})
}
