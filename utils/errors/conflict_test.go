package errors

import (
	"testing"
)

func TestNewConflictErr(t *testing.T) {
	err := NewConflictErr("Network is already being created")
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "Network is already being created" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		if _, ok := err.(*ConflictErr); !ok {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestIsConflictErr(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsConflictErr(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotConflictErr", func(tt *testing.T) {
		if IsConflictErr(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsConflictErr", func(tt *testing.T) {
		if !IsConflictErr(NewConflictErr("Aggregate")) {
			tt.Fail()
		}
	})
}
