package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
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
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err1, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "job not found")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
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
		err = store.UpdateJob(context.Background(), job.UUID, models.LifeCycleStateREADY, 0, "")
		assert.NoError(tt, err, "Failed to update job: %v", err)
		updatedJob, err := store.GetJob(context.Background(), job.UUID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, job.UUID, updatedJob.UUID, "Expected job UUID %v, got %v", job.UUID, updatedJob.UUID)
		assert.Equal(tt, models.LifeCycleStateREADY, updatedJob.State, "Expected job state %v, got %v", models.LifeCycleStateREADY, updatedJob.State)
	})
}

func TestGetJobsWithCondition(t *testing.T) {
	t.Run("WhenNoJobsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Empty conditions
		filter := utils.CreateFilterWithConditions(&utils.FilterCondition{})
		jobs, err := store.GetJobsWithCondition(context.Background(), *filter)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, jobs, "Expected empty jobs list, got %v", jobs)
	})

	t.Run("WhenJobsExistMatchingCondition", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create jobs with different states
		job1 := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "job-1-uuid",
			},
			State: models.LifeCycleStateCreating,
			Type:  "test-type",
		}
		job2 := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "job-2-uuid",
			},
			State: models.LifeCycleStateREADY,
			Type:  "test-type",
		}
		job3 := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   3,
				UUID: "job-3-uuid",
			},
			State: models.LifeCycleStateCreating,
			Type:  "other-type",
		}

		_, err = store.CreateJob(context.Background(), job1)
		assert.NoError(tt, err, "Failed to create job1: %v", err)
		_, err = store.CreateJob(context.Background(), job2)
		assert.NoError(tt, err, "Failed to create job2: %v", err)
		_, err = store.CreateJob(context.Background(), job3)
		assert.NoError(tt, err, "Failed to create job3: %v", err)

		// Filter by state
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("state", "=", models.LifeCycleStateCreating))
		jobs, err := store.GetJobsWithCondition(context.Background(), *filter)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, jobs, 2, "Expected 2 jobs, got %d", len(jobs))

		// Verify the correct jobs were returned
		jobUUIDs := []string{jobs[0].UUID, jobs[1].UUID}
		assert.Contains(tt, jobUUIDs, job1.UUID, "Expected job1 UUID in results")
		assert.Contains(tt, jobUUIDs, job3.UUID, "Expected job3 UUID in results")
		assert.NotContains(tt, jobUUIDs, job2.UUID, "Did not expect job2 UUID in results")

		// Filter by state and type
		filter = utils.CreateFilterWithConditions(
			utils.NewFilterCondition("state", "=", models.LifeCycleStateCreating),
			utils.NewFilterCondition("type", "=", "test-type"))
		jobs, err = store.GetJobsWithCondition(context.Background(), *filter)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, jobs, 1, "Expected 1 job, got %d", len(jobs))
		assert.Equal(tt, job1.UUID, jobs[0].UUID, "Expected job1 UUID, got %v", jobs[0].UUID)
	})

	t.Run("WhenJobsExistButNoneMatchCondition", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create a job
		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-job-uuid",
			},
			State: models.LifeCycleStateCreating,
		}
		_, err = store.CreateJob(context.Background(), job)
		assert.NoError(tt, err, "Failed to create job: %v", err)

		// Filter with non-matching condition
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("state", "=", "non-existent-state"))
		jobs, err := store.GetJobsWithCondition(context.Background(), *filter)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, jobs, "Expected empty jobs list, got %v", jobs)
	})
}

func TestListOngoingPoolJobsWithKmsConfigId(t *testing.T) {
	t.Run("WhenNoJobsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create pool and job with matching kms_config_id
		pool := &datamodel.Pool{
			Name:        "test-pool",
			KmsConfigID: sql.NullInt64{Int64: 123, Valid: true},
			AccountID:   456,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool: %v", err)

		jobs, err := store.ListOngoingPoolJobsWithKmsConfigId(context.Background(), 123, 456)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, jobs, "Expected no jobs, got %v", jobs)
	})

	t.Run("WhenJobsExistWithMatchingKmsId", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create pool and job with matching kms_config_id
		pool := &datamodel.Pool{
			Name:        "test-pool",
			KmsConfigID: sql.NullInt64{Int64: 123, Valid: true},
			AccountID:   456,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool: %v", err)

		job := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "job-uuid"},
			ResourceName: "test-pool",
			State:        string(models.JobsStatePROCESSING),
			Type:         string(models.JobTypeCreatePool),
		}
		_, err = store.CreateJob(context.Background(), job)
		assert.NoError(tt, err, "Failed to create job: %v", err)

		jobs, err := store.ListOngoingPoolJobsWithKmsConfigId(context.Background(), 123, 456)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, jobs, 1, "Expected 1 job, got %d", len(jobs))
		assert.Equal(tt, job.UUID, jobs[0].UUID, "Expected job UUID %v, got %v", job.UUID, jobs[0].UUID)
	})

	t.Run("WhenJobsExistButKmsIdDoesNotMatch", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create pool and job with different kms_config_id
		pool := &datamodel.Pool{
			Name:        "test-pool",
			KmsConfigID: sql.NullInt64{Int64: 999, Valid: true},
			AccountID:   456,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool: %v", err)

		job := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{ID: 2, UUID: "job-uuid-2"},
			ResourceName: "test-pool",
			State:        string(models.JobsStatePROCESSING),
			Type:         string(models.JobTypeCreatePool),
		}
		_, err = store.CreateJob(context.Background(), job)
		assert.NoError(tt, err, "Failed to create job: %v", err)

		jobs, err := store.ListOngoingPoolJobsWithKmsConfigId(context.Background(), 123, 456)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, jobs, "Expected no jobs, got %v", jobs)
	})
}

func TestGetOngoingMigrateKmsConfigJob(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	jobs := []*datamodel.Job{
		{BaseModel: datamodel.BaseModel{UUID: "uuid1"}, Type: "MIGRATE_KMS_CONFIG", State: "NEW",
			AccountID: sql.NullInt64{Int64: 1, Valid: true}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2"}, Type: "CREATE_POOL", State: "NEW",
			AccountID: sql.NullInt64{Int64: 2, Valid: true}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid3"}, Type: "MIGRATE_KMS_CONFIG", State: "DONE",
			AccountID: sql.NullInt64{Int64: 3, Valid: true}},
	}

	err = store.db.Create(jobs).Error()
	if err != nil {
		t.Fatalf("Failed to create Jobs table: %v", err)
	}
	t.Run("WhenQueriedJobIsPresent", func(tt *testing.T) {
		result, errQuery := store.GetOngoingMigrateKmsConfigJob(context.Background(), int64(1))
		assert.NoError(tt, errQuery)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.UUID, "uuid1")
	})
	t.Run("WhenQueriedJobIsPerformingAnotherAction", func(tt *testing.T) {
		result, errQuery := store.GetOngoingMigrateKmsConfigJob(context.Background(), int64(2))
		assert.Nil(tt, result)
		assert.Error(tt, errQuery)
		assert.EqualError(tt, errQuery, "[0] undefined error: job not found")
	})
	t.Run("WhenQueriedJobStateIsNeitherNewNorProcessing", func(tt *testing.T) {
		result, errQuery := store.GetOngoingMigrateKmsConfigJob(context.Background(), int64(3))
		assert.Nil(tt, result)
		assert.Error(tt, errQuery)
		assert.EqualError(tt, errQuery, "[0] undefined error: job not found")
	})
	t.Run("WhenQueriedJobAccountIdIsNotPresent", func(tt *testing.T) {
		result, errQuery := store.GetOngoingMigrateKmsConfigJob(context.Background(), int64(4))
		assert.Nil(tt, result)
		assert.Error(tt, errQuery)
		assert.EqualError(tt, errQuery, "[0] undefined error: job not found")
	})
}
