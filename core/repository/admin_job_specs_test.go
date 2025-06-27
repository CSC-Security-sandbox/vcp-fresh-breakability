package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
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
	t.Run("WhenAdminJobSpecCreationFails", func(tt *testing.T) {
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

		newJobSpec, err := store.CreateAdminJobSpec(tt.Context(), jobSpec)
		assert.Error(tt, err)
		assert.Nil(tt, newJobSpec)
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
