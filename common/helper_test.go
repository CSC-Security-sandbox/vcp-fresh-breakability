package common

import (
	"testing"
)

func TestHelperReturnsHello(t *testing.T) {
	expected := "Hello"
	actual := Helper()
	if actual != expected {
		t.Errorf("expected %s but got %s", expected, actual)
	}
}

func TestHelperReturnsNonEmptyString(t *testing.T) {
	actual := Helper()
	if actual == "" {
		t.Errorf("expected non-empty string but got empty string")
	}
}
