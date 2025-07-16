package database

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func Test_startTransaction(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		tx, err := _startTransaction(db)
		assert.NoError(t, err)
		assert.NotNil(t, tx)
	})

	t.Run("nil db", func(t *testing.T) {
		tx, err := _startTransaction(nil)
		assert.Error(t, err)
		assert.Nil(t, tx)
	})
}

func Test_commitOrRollbackTransaction(t *testing.T) {
	db, _ := SetupTestDB()
	logger := &log.MockLogger{}

	t.Run("commit", func(t *testing.T) {
		tx := db.Begin()
		err := error(nil)
		result := _commitOrRollbackTransaction(logger, tx, &err)
		assert.NoError(t, result)
	})

	t.Run("rollback", func(t *testing.T) {
		tx := db.Begin()
		err := errors.New("fail")
		result := _commitOrRollbackTransaction(logger, tx, &err)
		assert.Error(t, result)
	})

	t.Run("commit error", func(t *testing.T) {
		tx := db.Begin()
		tx.Error = errors.New("commit error")
		err := error(nil)
		logger.On("Error", mock.Anything, mock.Anything, mock.Anything).Return()
		result := _commitOrRollbackTransaction(logger, tx, &err)
		assert.Error(t, result)
	})

	t.Run("rollback error", func(t *testing.T) {
		tx := db.Begin()
		err := errors.New("fail")
		tx.Error = errors.New("rollback error")
		logger.On("Error", mock.Anything, mock.Anything, mock.Anything).Return()
		result := _commitOrRollbackTransaction(logger, tx, &err)
		assert.Error(t, result)
		assert.Contains(t, result.Error(), "rollback error")
	})
}

func Test_commitOrRollbackOnError(t *testing.T) {
	db, _ := SetupTestDB()
	logger := &log.MockLogger{}

	t.Run("panic", func(t *testing.T) {
		tx := db.Begin()
		err := error(nil)
		defer func() { _ = recover() }()
		_commitOrRollbackOnError(logger, tx, &err)
	})

	t.Run("commitOrRollbackTransaction error", func(t *testing.T) {
		tx := db.Begin()
		err := error(nil)
		logger.On("Error", mock.Anything, mock.Anything, mock.Anything).Return()
		orig := commitOrRollbackTransaction
		commitOrRollbackTransaction = func(log log.Logger, tx *gorm.DB, err *error) error {
			return errors.New("forced error")
		}
		defer func() { commitOrRollbackTransaction = orig }()
		_commitOrRollbackOnError(logger, tx, &err)
		assert.EqualError(t, err, "forced error")
	})

	t.Run("error branch", func(t *testing.T) {
		tx := db.Begin()
		err := error(nil)
		logger.On("Error", mock.Anything, mock.Anything, mock.Anything).Return()
		old := commitOrRollbackTransaction
		commitOrRollbackTransaction = func(log log.Logger, tx *gorm.DB, err *error) error {
			return errors.New("commitOrRollback error branch")
		}
		defer func() { commitOrRollbackTransaction = old }()
		_commitOrRollbackOnError(logger, tx, &err)
		assert.EqualError(t, err, "commitOrRollback error branch")
	})
}

func Test_parseDBError(t *testing.T) {
	db, _ := SetupTestDB()
	tx := db.Begin()
	assert.NoError(t, parseDBError(tx))
	assert.Error(t, parseDBError(nil))

	tx.Error = errors.New("db error")
	assert.Error(t, parseDBError(tx))
}

func TestCommitOrRollbackOnError_Panic(t *testing.T) {
	t.Run("WhenPanicOccurs", func(tt *testing.T) {
		tx := &gorm.DB{}
		err := error(nil)
		logger := &log.MockLogger{}
		defer func() {
			commitOrRollbackTransaction = _commitOrRollbackTransaction
			if r := recover(); r == nil {
				tt.Errorf("Expected panic, but got none")
			}
		}()
		commitOrRollbackTransaction = func(log log.Logger, tx *gorm.DB, err *error) error {
			panic("Simulated panic")
		}
		commitOrRollbackOnError(logger, tx, &err)
		if err == nil {
			tt.Errorf("Expected error, but got nil")
		}
		if err.Error() != "panic: Simulated panic" {
			tt.Errorf("Expected error message 'panic: Simulated panic', got '%v'", err)
		}
	})
}
