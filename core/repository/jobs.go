package repository

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
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

func (d *DataStoreRepository) UpdateJob(ctx context.Context, id, status string, trackingID int, errorDetails []byte) error {
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

func (d *DataStoreRepository) ListOngoingPoolJobsWithKmsConfigId(ctx context.Context, kmsId, accountId int64) ([]*datamodel.Job, error) {
	db := d.db.GORM().WithContext(ctx)
	jobs := make([]*datamodel.Job, 0)

	err := db.Joins("INNER JOIN pools on pools.name = jobs.resource_name").Where("jobs.state = ? and jobs.type = ? and pools.kms_config_id = ? and pools.account_id = ?", models.JobsStatePROCESSING, models.JobTypeCreatePool, kmsId, accountId).Find(&jobs).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return jobs, nil
}

func (d *DataStoreRepository) GetJobsWithCondition(ctx context.Context, filter utils.Filter) ([]*datamodel.Job, error) {
	db := d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
	jobs := make([]*datamodel.Job, 0)
	err := db.Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	return jobs, nil
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
