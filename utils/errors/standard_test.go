package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	err := New("7% is standard")
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "7% is standard" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
	}
}

func TestNewWithTrackingID(t *testing.T) {
	err := NewWithTrackingID("7% is standard", 1000)
	if err == nil {
		t.Fail()
	} else {
		if err.Error() != "7% is standard" {
			t.Errorf("Error message '%s' does not match expected one", err.Error())
		}
		se, ok := err.(*StandardErr)
		if !ok {
			t.Error("Error type does not match expected one")
		}
		if se.GetTrackingID() != 1000 {
			t.Errorf("Unexpected tracking ID, got: %d, expected: %d", se.GetTrackingID(), 1000)
		}
	}
}

type testError struct {
	err error
}

func (t *testError) Error() string {
	return t.err.Error()
}

func (t *testError) Is(err error) bool {
	_, ok := err.(*testError)
	return ok
}

func TestErrorf(t *testing.T) {
	t.Run("WhenErrorIsStandard", func(tt *testing.T) {
		err := Errorf("This error is %s", "standard")
		if err == nil {
			t.Fail()
		} else {
			if err.Error() != "This error is standard" {
				t.Errorf("Error message '%s' does not match expected one", err.Error())
			}
		}
	})
	t.Run("WhenErrorIsWrapped", func(tt *testing.T) {
		w := &testError{err: New("wrapped")}
		err := Errorf("This error is %w", w)
		if err == nil {
			t.Fail()
		} else {
			if err.Error() != "This error is wrapped" {
				t.Errorf("Error message '%s' does not match expected one", err.Error())
			}

			assert.True(tt, errors.Is(err, &testError{}))
		}
	})
}
