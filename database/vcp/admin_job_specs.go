package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (d *DataStoreRepository) CreateAdminJobSpec(ctx context.Context, jobSpec *datamodel.AdminJobSpec) (*datamodel.AdminJobSpec, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	// Ensure we always attempt to (re)activate the record on conflict by clearing DeletedAt
	jobSpec.DeletedAt = nil

	// Upsert by unique job_type: if a row exists (even soft-deleted), update fields and revive
	err = tx.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "job_type"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"cron_expression": jobSpec.CronExpression,
				"state":           jobSpec.State,
				"deleted_at":      nil,
				"updated_at":      time.Now(),
			}),
		},
		clause.Returning{},
	).Create(&jobSpec).Error
	if err != nil {
		logger.Errorf("Failed to create admin job spec for jobType: %s, error: %v", jobSpec.JobType, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}

	return jobSpec, nil
}

// CreateAdminJobSpecIfNotExists creates a new AdminJobSpec only if one with the same JobType doesn't already exist.
// Returns an error if a record with the same JobType already exists.
func (d *DataStoreRepository) CreateAdminJobSpecIfNotExists(ctx context.Context, jobSpec *datamodel.AdminJobSpec) (*datamodel.AdminJobSpec, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	jobSpec.DeletedAt = nil

	// Pure INSERT - will fail if job_type already exists due to unique constraint
	err = tx.Create(&jobSpec).Error
	if err != nil {
		logger.Errorf("Failed to create admin job spec for jobType: %s, error: %v", jobSpec.JobType, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}

	return jobSpec, nil
}

func (d *DataStoreRepository) GetAdminJobSpecByJobType(ctx context.Context, jobType string) (*datamodel.AdminJobSpec, error) {
	db := d.db.GORM().WithContext(ctx)
	adminJobSpec := &datamodel.AdminJobSpec{JobType: jobType}
	err := db.Where(adminJobSpec).First(&adminJobSpec).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return adminJobSpec, nil
}

func (d *DataStoreRepository) UpdateAdminJobSpec(ctx context.Context, jobSpec *datamodel.AdminJobSpec) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	if err = tx.Updates(jobSpec).Error; err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) GetAdminJobSpecsByState(ctx context.Context, state string) ([]*datamodel.AdminJobSpec, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	var adminJobSpecs []*datamodel.AdminJobSpec
	err = tx.Where("state = ?", state).Find(&adminJobSpecs).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return adminJobSpecs, nil
}

// UpdateAdminJobSpecWithLock atomically updates an admin job spec's updated_at field if it exists,
// is in the specified state, and enough time has passed since the last update.
// Uses a single atomic SQL UPDATE with WHERE conditions to ensure only one pod can acquire the lock.
// Returns the number of rows affected and any error.
func (d *DataStoreRepository) UpdateAdminJobSpecWithLock(ctx context.Context, jobType, state string, lockThreshold, currentTime time.Time) (int64, error) {
	db := d.db.GORM().WithContext(ctx)
	logger := util.GetLogger(ctx)

	// Perform atomic update with time check in a single SQL operation
	// This ensures only one pod can successfully update when multiple pods run simultaneously
	result := db.Model(&datamodel.AdminJobSpec{}).
		Where("job_type = ? AND state = ? AND updated_at <= ?", jobType, state, lockThreshold).
		Update("updated_at", currentTime)

	if result.Error != nil {
		logger.ErrorContext(ctx, "Failed to update admin job spec with lock", "error", result.Error)
		return 0, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, result.Error)
	}

	logger.InfoContext(ctx, "Admin job spec lock update completed",
		"jobType", jobType,
		"rowsAffected", result.RowsAffected,
		"lockThreshold", lockThreshold,
		"currentTime", currentTime)

	return result.RowsAffected, nil
}
