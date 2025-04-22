package errors

import (
	"testing"
)

func TestNewNotImplementedYetErr(t *testing.T) {
	err := NewNotImplementedYetErr()
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "Not implemented yet" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		if _, ok := err.(*NotImplementedYetErr); !ok {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestIsNotImplementedYetErr(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsNotImplementedYetErr(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotNotImplementedYetErr", func(tt *testing.T) {
		if IsNotImplementedYetErr(New("Not implemented yet")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotImplementedYetErr", func(tt *testing.T) {
		if !IsNotImplementedYetErr(NewNotImplementedYetErr()) {
			tt.Fail()
		}
	})
}
