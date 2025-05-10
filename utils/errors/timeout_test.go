package errors

import (
	"errors"
	"testing"
)

func TestNewTimeoutErr(t *testing.T) {
	err := NewTimeoutErr("Timed out")
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "Timed out" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		var timeoutErr *TimeoutErr
		if !errors.As(err, &timeoutErr) {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestIsTimeoutErr(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsTimeoutErr(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotTimeoutErr", func(tt *testing.T) {
		if IsTimeoutErr(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsTimeoutErr", func(tt *testing.T) {
		if !IsTimeoutErr(NewTimeoutErr("Aggregate")) {
			tt.Fail()
		}
	})
}

func TestConvertToTimeoutErrorIfIOtimeout(t *testing.T) {
	t.Run("WhenErrorIsNil", func(tt *testing.T) {
		result := ConvertToTimeoutErrrIfIOtimeout(nil, "module")
		if result != nil {
			tt.Errorf("Expected nil, got: %v", result)
		}
	})

	t.Run("WhenErrorIsIOTimeoutAndModuleIsEmpty", func(tt *testing.T) {
		err := New("i/o timeout")
		result := ConvertToTimeoutErrrIfIOtimeout(err, "")
		if result == nil {
			tt.Error("Expected a non-nil error")
		} else {
			if result.Error() != "i/o timeout" {
				tt.Errorf("Unexpected error message, got: %s", result.Error())
			}
			var timeoutErr *TimeoutErr
			if !errors.As(result, &timeoutErr) {
				tt.Error("Expected error to be of type TimeoutErr")
			}
		}
	})

	t.Run("WhenErrorIsIOTimeoutAndModuleIsProvided", func(tt *testing.T) {
		err := New("i/o timeout")
		result := ConvertToTimeoutErrrIfIOtimeout(err, "TestModule")
		if result == nil {
			tt.Error("Expected a non-nil error")
		} else {
			expectedMessage := "TestModule call timeout."
			if result.Error() != expectedMessage {
				tt.Errorf("Unexpected error message, got: %s", result.Error())
			}
			var timeoutErr *TimeoutErr
			if !errors.As(result, &timeoutErr) {
				tt.Error("Expected error to be of type TimeoutErr")
			}
		}
	})

	t.Run("WhenErrorIsContextDeadlineExceeded", func(tt *testing.T) {
		err := New("context deadline exceeded")
		result := ConvertToTimeoutErrrIfIOtimeout(err, "AnotherModule")
		if result == nil {
			tt.Error("Expected a non-nil error")
		} else {
			expectedMessage := "AnotherModule call timeout."
			if result.Error() != expectedMessage {
				tt.Errorf("Unexpected error message, got: %s", result.Error())
			}
			var timeoutErr *TimeoutErr
			if !errors.As(result, &timeoutErr) {
				tt.Error("Expected error to be of type TimeoutErr")
			}
		}
	})

	t.Run("WhenErrorDoesNotMatchConditions", func(tt *testing.T) {
		err := New("some other error")
		result := ConvertToTimeoutErrrIfIOtimeout(err, "module")
		if !errors.Is(result, err) {
			tt.Errorf("Expected original error, got: %v", result)
		}
	})
}
