package errors

import (
	"testing"
)

func TestNewUserInputValidationErr(t *testing.T) {
	err := NewUserInputValidationErr("whack cidr: Invalid CIDR input")
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "whack cidr: Invalid CIDR input" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		if _, ok := err.(*UserInputValidationErr); !ok {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestNewUserInputValidationErrWithTrackingID(t *testing.T) {
	err := NewUserInputValidationErrWithTrackingID("IP range clash", 1)
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "IP range clash" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		uie, ok := err.(*UserInputValidationErr)
		if !ok {
			t.Error("Error type does not match expected one")
		}
		if uie.GetTrackingID() != 1 {
			t.Errorf("Unexpected tracking ID, got: %d, expected: %d", uie.GetTrackingID(), 1)
		}
	}
}

func TestIsUserInputValidationErr(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsUserInputValidationErr(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotUserInputValidationErr", func(tt *testing.T) {
		if IsUserInputValidationErr(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsUserInputValidationErr", func(tt *testing.T) {
		if !IsUserInputValidationErr(NewUserInputValidationErr("Aggregate")) {
			tt.Fail()
		}
	})
}
