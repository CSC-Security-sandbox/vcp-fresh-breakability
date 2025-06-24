package errors

import (
	"testing"
)

func TestNewTransientErr(t *testing.T) {
	err := NewTransientErr("All operatives are currently busy - please hold")
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "Transient error: All operatives are currently busy - please hold" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		if _, ok := err.(*TransientErr); !ok {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestIsTransientErr(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsTransientErr(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotTransientErr", func(tt *testing.T) {
		if IsTransientErr(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsTransientErr", func(tt *testing.T) {
		if !IsTransientErr(NewTransientErr("Service temporarily unavailable")) {
			tt.Fail()
		}
	})
}
