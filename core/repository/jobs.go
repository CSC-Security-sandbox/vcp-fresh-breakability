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
	var result *datamodel.Job
	err := d.db.GORM().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job.UUID = utils.RandomUUID()
		job.CreatedAt = time.Now()
		job.UpdatedAt = job.CreatedAt
		job.WorkflowID = job.UUID
		if err := tx.Create(job).Error; err != nil {
			return err
		}
		dbPool, err := getJobWithDetails(tx, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: job.UUID}})
		if err != nil {
			return err
		}
		result = dbPool
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *DataStoreRepository) GetJob(ctx context.Context, id string) (*datamodel.Job, error) {
	return getJobWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: id}})
}

func (d *DataStoreRepository) UpdateJob(ctx context.Context, id string, status string) error {
	return d.db.GORM().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := getJobWithDetails(tx, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: id}})
		if err != nil {
			return err
		}

		job.UpdatedAt = time.Now()
		job.State = status
		if err := tx.Updates(job).Error; err != nil {
			return err
		}
		return nil
	})
}

func _getJobWithDetails(db *gorm.DB, query *datamodel.Job) (*datamodel.Job, error) {
	job := &datamodel.Job{}
	err := db.First(&job, query).Error
	if err != nil {
		return nil, err
	}
	return job, nil
}
