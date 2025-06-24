package errors

import (
	"testing"
)

func TestNonRetryableErr(t *testing.T) {
	err := NewNonRetryableErr("whack cidr: Invalid CIDR input")
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "whack cidr: Invalid CIDR input" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		if _, ok := err.(*NonRetryableErr); !ok {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestNonRetryableErrWithTrackingID(t *testing.T) {
	err := NewNonRetryableErrWithTrackingID("IP range clash", 1)
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "IP range clash" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		uie, ok := err.(*NonRetryableErr)
		if !ok {
			t.Error("Error type does not match expected one")
		}
		if uie.GetTrackingID() != 1 {
			t.Errorf("Unexpected tracking ID, got: %d, expected: %d", uie.GetTrackingID(), 1)
		}
	}
}

func TestIsNonRetryableErr(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsNonRetryableErr(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotNonRetryableErr", func(tt *testing.T) {
		if IsNonRetryableErr(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsUserInputValidationErr", func(tt *testing.T) {
		if !IsUserInputValidationErr(NewUserInputValidationErr("Aggregate")) {
			tt.Fail()
		}
	})
}
