package repository

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"gorm.io/gorm"
)

var (
	getJobWithDetails = _getJobWithDetails
)

func (d *DataStoreRepository) CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error) {
	db := d.db.GORM().WithContext(ctx)
	job.UUID = utils.RandomUUID()
	job.CreatedAt = time.Now()
	job.UpdatedAt = job.CreatedAt
	job.WorkflowID = job.UUID
	err := db.Create(job).Error
	if err != nil {
		return nil, err
	}
	dbPool, err := getJobWithDetails(db, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: job.UUID}})
	if err != nil {
		return nil, err
	}
	return dbPool, nil
}

func (d *DataStoreRepository) GetJob(ctx context.Context, id string) (*datamodel.Job, error) {
	return getJobWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: id}})
}

func (d *DataStoreRepository) UpdateJob(ctx context.Context, id string, status string) error {
	// TODO implement me
	db := d.db.GORM().WithContext(ctx)
	job, err := getJobWithDetails(db, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: id}})
	if err != nil {
		return err
	}
	job.UpdatedAt = time.Now()
	job.State = status
	err = db.Updates(job).Error
	if err != nil {
		return err
	}
	return nil
}

func _getJobWithDetails(db *gorm.DB, query *datamodel.Job) (*datamodel.Job, error) {
	job := &datamodel.Job{}
	err := db.First(&job, query).Error
	if err != nil {
		return nil, err
	}
	return job, nil
}
