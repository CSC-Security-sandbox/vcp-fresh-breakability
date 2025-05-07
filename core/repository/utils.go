package repository

import (
	"errors"
	"fmt"

	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

var (
	commitOrRollbackTransaction = _commitOrRollbackTransaction
)

// startTransaction starts a new transaction
func startTransaction(db *gorm.DB) (*gorm.DB, error) {
	if db == nil {
		return nil, errors.New("DB connection is closed")
	}
	tx := db.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	return tx, nil
}

// commitOrRollbackTransaction commits or rollbacks the transaction
func commitOrRollbackOnError(log slogger.Logger, tx *gorm.DB, err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("panic: %v", r)
	}

	*err = commitOrRollbackTransaction(log, tx, err)
}

// commitOrRollbackTransaction commits the transaction if no error occurred
func _commitOrRollbackTransaction(log slogger.Logger, tx *gorm.DB, err *error) error {
	defer func() {
		rollbackErr := parseDBError(tx.Rollback())
		if rollbackErr != nil {
			log.Error("Failed to rollback transaction", rollbackErr)
		}
	}()

	if *err != nil {
		return *err
	}

	return parseDBError(tx.Commit())
}

// parseDBError checks if there is any error in the transaction and returns it
func parseDBError(db *gorm.DB) error {
	if db == nil {
		return errors.New("DB connection is closed")
	}
	if db.Error != nil {
		return db.Error
	}
	return nil
}
