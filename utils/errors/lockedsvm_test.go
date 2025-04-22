package errors

import "testing"

func TestNewSvmLockedError(t *testing.T) {
	err := NewSvmLockedError()
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "Unable to perform operation. Wait a few minutes, and then try the operation again. If the error persists, contact technical support for assistance" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		if _, ok := err.(*SvmLockedError); !ok {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestNewSvmLockedErrorWithTrackingID(t *testing.T) {
	err := NewSvmLockedErrorWithTrackingID()
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "Unable to perform operation. Wait a few minutes, and then try the operation again. If the error persists, contact technical support for assistance" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		_, ok := err.(*SvmLockedError)
		if !ok {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestIsSvmLockedError(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsSvmLockedError(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotNotReadyErr", func(tt *testing.T) {
		if IsSvmLockedError(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsCommunicationErr", func(tt *testing.T) {
		if !IsSvmLockedError(NewSvmLockedError()) {
			tt.Fail()
		}
	})
}

func TestNewSvmDegradedError(t *testing.T) {
	err := NewSvmDegradedError()
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "Unable to perform operation. Wait a few minutes, and then try the operation again. If the error persists, contact technical support for assistance" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		if _, ok := err.(*SvmDegradedError); !ok {
			t.Error("Error type does not match expected one")
		}
	}
}

func TestIsSvmDegradedError(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsSvmDegradedError(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotNotReadyErr", func(tt *testing.T) {
		if IsSvmDegradedError(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsCommunicationErr", func(tt *testing.T) {
		if !IsSvmDegradedError(NewSvmDegradedError()) {
			tt.Fail()
		}
	})
}
