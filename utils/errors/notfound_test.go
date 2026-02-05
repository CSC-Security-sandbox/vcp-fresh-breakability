package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestIs(t *testing.T) {
	var tests = []struct {
		name       string
		nfErr      error
		targetErr  error
		expectedIs bool
	}{
		{name: "different error", nfErr: NewNotFoundErr("", nil), targetErr: New("Not the same error"), expectedIs: false},
		{name: "NF error, different object type", nfErr: NewNotFoundErr("object 1", nil), targetErr: NewNotFoundErr("object 2", nil), expectedIs: false},
		{name: "NF error, different object identifier", nfErr: NewNotFoundErr("object 1", nillable.GetStringPtr("identifier 1")), targetErr: NewNotFoundErr("object 1", nillable.GetStringPtr("identifier 2")), expectedIs: false},
		{name: "NF error, different tracking ID", nfErr: NewNotFoundErrWithTrackingID("object 1", nillable.GetStringPtr("identifier 1"), 1), targetErr: NewNotFoundErrWithTrackingID("object 1", nillable.GetStringPtr("identifier 1"), 2), expectedIs: false},
		{name: "equal error", nfErr: NewNotFoundErr("object 1", nillable.GetStringPtr("identifier 1")), targetErr: NewNotFoundErr("object 1", nillable.GetStringPtr("identifier 1")), expectedIs: true},
		{name: "equal error with tracking ID", nfErr: NewNotFoundErrWithTrackingID("object 1", nillable.GetStringPtr("identifier 1"), 1), targetErr: NewNotFoundErrWithTrackingID("object 1", nillable.GetStringPtr("identifier 1"), 1), expectedIs: true},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(tt *testing.T) {
			is := errors.Is(tcase.nfErr, tcase.targetErr)
			assert.Equal(tt, tcase.expectedIs, is)
		})
	}
}

func TestNewNotFoundErr(t *testing.T) {
	t.Run("WhenNotSpecifyingObjectIdentifier", func(tt *testing.T) {
		err := NewNotFoundErr("Aggregate", nil)
		if err == nil {
			tt.Fail()
		} else {
			if err.Error() != "Aggregate not found" {
				tt.Errorf("Error message '%s' does not match expected one", err.Error())
			}
			if _, ok := err.(*NotFoundErr); !ok {
				tt.Error("Error type does not match expected one")
			}
		}
	})
	t.Run("WhenSpecifyingObjectIdentifier", func(tt *testing.T) {
		id := "aggr-1"
		err := NewNotFoundErr("Aggregate", &id)
		if err == nil {
			tt.Fail()
		} else {
			if err.Error() != "Aggregate 'aggr-1' not found" {
				tt.Errorf("Error message '%s' does not match expected one", err.Error())
			}
			if _, ok := err.(*NotFoundErr); !ok {
				tt.Error("Error type does not match expected one")
			}
		}
	})
}

func TestNewNotFoundErrWithTrackingID(t *testing.T) {
	t.Run("WhenNotSpecifyingObjectIdentifier", func(tt *testing.T) {
		err := NewNotFoundErrWithTrackingID("Aggregate", nil, 1000)
		if err == nil {
			tt.Fail()
		} else {
			if err.Error() != "Aggregate not found" {
				tt.Errorf("Error message '%s' does not match expected one", err.Error())
			}
			nfe, ok := err.(*NotFoundErr)
			if !ok {
				tt.Error("Error type does not match expected one")
			}
			if nfe.GetTrackingID() != 1000 {
				tt.Errorf("Unexpected tracking ID, got: %d, expected: %d", nfe.GetTrackingID(), 1000)
			}
		}
	})
	t.Run("WhenSpecifyingObjectIdentifier", func(tt *testing.T) {
		id := "aggr-1"
		err := NewNotFoundErrWithTrackingID("Aggregate", &id, 1000)
		if err == nil {
			tt.Fail()
		} else {
			if err.Error() != "Aggregate 'aggr-1' not found" {
				tt.Errorf("Error message '%s' does not match expected one", err.Error())
			}
			nfe, ok := err.(*NotFoundErr)
			if !ok {
				tt.Error("Error type does not match expected one")
			}
			if nfe.GetTrackingID() != 1000 {
				tt.Errorf("Unexpected tracking ID, got: %d, expected: %d", nfe.GetTrackingID(), 1000)
			}
		}
	})
}

func TestConvertToNotFoundErrIfContainsMessage(t *testing.T) {
	t.Run("WhenNotSpecifyingError", func(tt *testing.T) {
		err := ConvertToNotFoundErrIfContainsMessage(nil, "entry doesn't exist", "Aggregate", nil)
		if err != nil {
			tt.Fail()
		}
	})
	t.Run("WhenErrorDoesNotContainMessage", func(tt *testing.T) {
		err := ConvertToNotFoundErrIfContainsMessage(New("you do not have access to perform that action"), "entry doesn't exist", "Aggregate", nil)
		if err == nil {
			tt.Fail()
		} else {
			if err.Error() != "you do not have access to perform that action" {
				tt.Errorf("Error message '%s' does not match expected one", err.Error())
			}
			if _, ok := err.(*NotFoundErr); ok {
				tt.Error("Error type does not match expected one")
			}
		}
	})
	t.Run("WhenErrorDoesContainMessage", func(tt *testing.T) {
		err := ConvertToNotFoundErrIfContainsMessage(New("error when deleting: entry doesn't exist"), "entry doesn't exist", "Aggregate", nil)
		if err == nil {
			tt.Fail()
		} else {
			if err.Error() != "Aggregate not found" {
				tt.Errorf("Error message '%s' does not match expected one", err.Error())
			}
			if _, ok := err.(*NotFoundErr); !ok {
				tt.Error("Error type does not match expected one")
			}
		}
	})
}

func TestIsNotFoundErr(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsNotFoundErr(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotNotFoundErr", func(tt *testing.T) {
		if IsNotFoundErr(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotFoundErr", func(tt *testing.T) {
		if !IsNotFoundErr(NewNotFoundErr("Aggregate", nil)) {
			tt.Fail()
		}
	})
}

func TestIsNotFoundErrForObjectType(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsNotFoundErrForObjectType(nil, "Job") {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotFoundErrAndObjectTypeMatchesSameCase", func(tt *testing.T) {
		if !IsNotFoundErrForObjectType(NewNotFoundErr("Job", nil), "Job") {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotFoundErrAndObjectTypeMatchesDifferentCase", func(tt *testing.T) {
		if !IsNotFoundErrForObjectType(NewNotFoundErr("Job", nil), "jOB") {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotFoundErrAndObjectTypeDoesNotMatch", func(tt *testing.T) {
		if IsNotFoundErrForObjectType(NewNotFoundErr("Job", nil), "Volume") {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotNotFoundErr", func(tt *testing.T) {
		if IsNotFoundErrForObjectType(New("Job"), "Job") {
			tt.Fail()
		}
	})
}

func TestIsNotFoundErrForObjectTypeInChain(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		assert.False(tt, IsNotFoundErrForObjectTypeInChain(nil, "volume"))
	})
	t.Run("WhenErrorIsUnwrappedNotFoundErrAndObjectTypeMatches", func(tt *testing.T) {
		assert.True(tt, IsNotFoundErrForObjectTypeInChain(NewNotFoundErr("volume", nil), "volume"))
		assert.True(tt, IsNotFoundErrForObjectTypeInChain(NewNotFoundErr("account", nil), "account"))
	})
	t.Run("WhenErrorIsUnwrappedNotFoundErrAndObjectTypeDoesNotMatch", func(tt *testing.T) {
		assert.False(tt, IsNotFoundErrForObjectTypeInChain(NewNotFoundErr("volume", nil), "account"))
		assert.False(tt, IsNotFoundErrForObjectTypeInChain(NewNotFoundErr("quota rule", nil), "volume"))
	})
	t.Run("WhenErrorIsWrappedNotFoundErrAndObjectTypeMatches", func(tt *testing.T) {
		nfErr := NewNotFoundErr("volume", nil)
		wrapped := fmt.Errorf("wrapped: %w", nfErr)
		assert.True(tt, IsNotFoundErrForObjectTypeInChain(wrapped, "volume"))
		assert.True(tt, IsNotFoundErrForObjectTypeInChain(wrapped, "VOLUME"))
	})
	t.Run("WhenErrorIsWrappedNotFoundErrAndObjectTypeDoesNotMatch", func(tt *testing.T) {
		nfErr := NewNotFoundErr("volume", nil)
		wrapped := fmt.Errorf("wrapped: %w", nfErr)
		assert.False(tt, IsNotFoundErrForObjectTypeInChain(wrapped, "account"))
	})
	t.Run("WhenErrorIsWrappedButNotNotFoundErr", func(tt *testing.T) {
		wrapped := fmt.Errorf("wrapped: %w", errors.New("other error"))
		assert.False(tt, IsNotFoundErrForObjectTypeInChain(wrapped, "volume"))
	})
	t.Run("WhenErrorIsNotNotFoundErr", func(tt *testing.T) {
		assert.False(tt, IsNotFoundErrForObjectTypeInChain(errors.New("other"), "volume"))
	})
}
