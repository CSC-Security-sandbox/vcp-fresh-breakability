package repository

import (
	"errors"
	"fmt"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

var (
	commitOrRollbackTransaction = _commitOrRollbackTransaction
	startTransaction            = _startTransaction
	commitOrRollbackOnError     = _commitOrRollbackOnError
)

// startTransaction starts a new transaction
func _startTransaction(db *gorm.DB) (*gorm.DB, error) {
	if db == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseConnectionClosed, errors.New("DB connection is closed"))
	}

	tx := db.Begin()
	if tx.Error != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseTransactionError, tx.Error)
	}

	return tx, nil
}

// commitOrRollbackTransaction commits or rollbacks the transaction
func _commitOrRollbackOnError(log slogger.Logger, tx *gorm.DB, err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("panic: %v", r)
	}

	commitErr := commitOrRollbackTransaction(log, tx, err)
	if commitErr != nil {
		if *err != nil {
			*err = fmt.Errorf("%v; commit/rollback error: %w", *err, commitErr)
		} else {
			*err = commitErr
		}
	}
}

// commitOrRollbackTransaction commits the transaction if no error occurred, otherwise rolls back
func _commitOrRollbackTransaction(log slogger.Logger, tx *gorm.DB, err *error) error {
	if *err != nil {
		rollbackErr := parseDBError(tx.Rollback())
		if rollbackErr != nil {
			log.Error("Failed to rollback transaction", "error", rollbackErr)
			return rollbackErr
		}
	} else {
		commitErr := parseDBError(tx.Commit())
		if commitErr != nil {
			log.Error("Failed to commit transaction", "error", commitErr)
			return commitErr
		}
	}
	return *err
}

// parseDBError checks if there is any error in the transaction and returns it
func parseDBError(db *gorm.DB) error {
	if db == nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseConnectionClosed, errors.New("DB connection is closed"))
	}
	if db.Error != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseTransactionError, db.Error)
	}
	return nil
}
