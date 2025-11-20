package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	getJobWithDetails = _getJobWithDetails
)

func (d *DataStoreRepository) CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	job.UUID = utils.RandomUUID()
	job.CreatedAt = time.Now()
	job.UpdatedAt = job.CreatedAt
	job.WorkflowID = job.UUID

	if err := tx.Create(job).Error; err != nil {
		return nil, err
	}

	dbJob, err := getJobWithDetails(tx, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: job.UUID}})
	if err != nil {
		return nil, err
	}

	return dbJob, nil
}

func (d *DataStoreRepository) GetJob(ctx context.Context, id string) (*datamodel.Job, error) {
	return getJobWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: id}})
}

func (d *DataStoreRepository) UpdateJob(ctx context.Context, id, status string, trackingID int, errorDetails string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	job, err := getJobWithDetails(tx, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: id}})
	if err != nil {
		return err
	}

	job.UpdatedAt = time.Now()
	job.State = status
	job.TrackingID = trackingID
	job.ErrorDetails = errorDetails
	if err = tx.Updates(job).Error; err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) UpdateJobAttributes(ctx context.Context, uuid string, jobAttributes *datamodel.JobAttributes) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	job, err := getJobWithDetails(tx, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: uuid}})
	if err != nil {
		return err
	}
	job.UpdatedAt = time.Now()
	job.JobAttributes = jobAttributes
	if err = tx.Updates(job).Error; err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) DeleteJob(ctx context.Context, id, errorDetails string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	job, err := getJobWithDetails(tx, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: id}})
	if err != nil {
		return err
	}

	job.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	job.State = string(models.JobsStateERROR)
	job.ErrorDetails = errorDetails
	if err = tx.Updates(job).Error; err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) ListOngoingPoolJobsWithKmsConfigId(ctx context.Context, kmsId, accountId int64) ([]*datamodel.Job, error) {
	db := d.db.GORM().WithContext(ctx)
	jobs := make([]*datamodel.Job, 0)

	err := db.Joins("INNER JOIN pools on pools.name = jobs.resource_name").Where("jobs.state = ? and jobs.type IN (?, ?) and pools.kms_config_id = ? and pools.account_id = ?", models.JobsStatePROCESSING, models.JobTypeCreatePool, models.JobTypeCreateLargePool, kmsId, accountId).Find(&jobs).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return jobs, nil
}

func (d *DataStoreRepository) GetJobsWithCondition(ctx context.Context, filter utils2.Filter) ([]*datamodel.Job, error) {
	db := d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
	jobs := make([]*datamodel.Job, 0)
	err := db.Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// GetJobByResourceUUID retrieves the job by its resource UUID
func (d *DataStoreRepository) GetJobByResourceUUID(ctx context.Context, resourceUUID string, jobType string) (*datamodel.Job, error) {
	job := &datamodel.Job{}
	query := d.db.GORM().WithContext(ctx).Where("job_attributes ->> 'resource_uuid' = ?", resourceUUID)

	// Add job type filter if provided
	if jobType != "" {
		query = query.Where("type = ?", jobType)
	}

	err := query.First(job).Error
	if err != nil {
		return nil, errors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "Job", nil)
	}
	return job, nil
}

func _getJobWithDetails(db *gorm.DB, query *datamodel.Job) (*datamodel.Job, error) {
	job := &datamodel.Job{}
	err := db.First(&job, query).Error
	if err != nil {
		if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("job", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return job, nil
}

func (d *DataStoreRepository) GetOngoingMigrateKmsConfigJob(ctx context.Context, accountId int64) (*datamodel.Job, error) {
	var job datamodel.Job
	err := d.db.GORM().WithContext(ctx).Where(
		"account_id = ? AND type = ? AND (state = ? OR state = ?)",
		accountId, models.JobTypeMigrateKmsConfig, models.JobsStateNEW, models.JobsStatePROCESSING,
	).First(&job).Error

	if err != nil {
		if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("job", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return &job, nil
}

func (d *DataStoreRepository) CheckAndFetchDuplicateJobs(ctx context.Context, jobType string, correlationID string) (*datamodel.Job, error) {
	var job datamodel.Job
	err := d.db.GORM().Unscoped().WithContext(ctx).Where(
		"correlation_id = ? AND type = ?",
		correlationID, jobType,
	).First(&job).Error

	if err != nil {
		if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
			// do not return any error if no job is found
			return nil, nil
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return &job, nil
}

func (d *DataStoreRepository) CancelRunningJobsForResource(ctx context.Context, resourceUUID string) error {
	db := d.db.GORM().WithContext(ctx)
	err := db.Model(&datamodel.Job{}).
		Where("job_attributes ->> 'resource_uuid' = ? AND state = ?", resourceUUID, models.JobsStatePROCESSING).
		Update("state", string(models.JobsStateCANCELLED)).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}
