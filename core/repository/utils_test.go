package repository

import (
	"testing"

	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func TestCommitOrRollbackOnError(t *testing.T) {
	t.Run("WhenPanicOccurs", func(tt *testing.T) {
		tx := &gorm.DB{}
		err := error(nil)
		log := slogger.NewLogger()

		// Simulate a panic
		defer func() {
			if r := recover(); r == nil {
				tt.Errorf("Expected panic, but got none")
			}
		}()

		commitOrRollbackTransaction = func(log slogger.Logger, tx *gorm.DB, err *error) error {
			panic("Simulated panic")
		}

		commitOrRollbackOnError(log, tx, &err)

		if err == nil {
			tt.Errorf("Expected error, but got nil")
		}
		if err.Error() != "panic: Simulated panic" {
			tt.Errorf("Expected error message 'panic: Simulated panic', got '%v'", err)
		}

		// Unpatch
		commitOrRollbackTransaction = _commitOrRollbackTransaction
	})
}
