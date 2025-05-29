package repository

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"testing"
)

func TestGetJob(t *testing.T) {
	t.Run("WhenJobExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-job-uuid",
			},
		}
		err = store.db.Create(job).Error()
		if err != nil {
			tt.Fatalf("Failed to create job: %v", err)
		}

		result, err := store.GetJob(context.Background(), job.UUID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, job.UUID, result.UUID, "Expected job id %v, got %v", job.UUID, result.UUID)
	})
	t.Run("WhenJobDoesNotExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err1 := store.GetJob(context.Background(), "test-job-uuid")
		assert.EqualError(tt, err1, "record not found")
	})
}

func TestCreateJob(t *testing.T) {
	t.Run("WhenJobIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-job-uuid",
			},
		}

		createdJob, err := store.CreateJob(context.Background(), job)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, job.UUID, createdJob.UUID, "Expected job uuid %v, got %v", job.UUID, createdJob.UUID)
	})
	t.Run("WhenJobAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-job-uuid",
			},
		}
		err = store.db.Create(job).Error()
		if err != nil {
			tt.Fatalf("Failed to create job: %v", err)
		}

		_, err1 := store.CreateJob(context.Background(), job)
		assert.EqualError(tt, err1, "UNIQUE constraint failed: jobs.id")
	})
}

func TestUpdateJob(t *testing.T) {
	t.Run("WhenJobIsUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-job-uuid",
			},
			State: models.LifeCycleStateCreating,
		}

		_, err = store.CreateJob(context.Background(), job)
		assert.NoError(tt, err, "Failed to create job: %v", err)
		err = store.UpdateJob(context.Background(), job.UUID, models.LifeCycleStateREADY, nil)
		assert.NoError(tt, err, "Failed to update job: %v", err)
		updatedJob, err := store.GetJob(context.Background(), job.UUID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, job.UUID, updatedJob.UUID, "Expected job UUID %v, got %v", job.UUID, updatedJob.UUID)
		assert.Equal(tt, models.LifeCycleStateREADY, updatedJob.State, "Expected job state %v, got %v", models.LifeCycleStateREADY, updatedJob.State)
	})
}
