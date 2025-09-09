package common

import (
	"testing"

	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
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

func TestGetBoolOrDefault(t *testing.T) {
	t.Run("WhenOptBoolIsSet", func(t *testing.T) {
		t.Run("ReturnsTrue", func(t *testing.T) {
			optBool := gcpgenserver.NewOptBool(true)
			result := GetBoolOrDefault(optBool, false)
			if result != true {
				t.Errorf("expected true but got %v", result)
			}
		})

		t.Run("ReturnsFalse", func(t *testing.T) {
			optBool := gcpgenserver.NewOptBool(false)
			result := GetBoolOrDefault(optBool, true)
			if result != false {
				t.Errorf("expected false but got %v", result)
			}
		})
	})

	t.Run("WhenOptBoolIsNotSet", func(t *testing.T) {
		t.Run("ReturnsDefaultTrue", func(t *testing.T) {
			optBool := gcpgenserver.OptBool{} // not set
			result := GetBoolOrDefault(optBool, true)
			if result != true {
				t.Errorf("expected true (default) but got %v", result)
			}
		})

		t.Run("ReturnsDefaultFalse", func(t *testing.T) {
			optBool := gcpgenserver.OptBool{} // not set
			result := GetBoolOrDefault(optBool, false)
			if result != false {
				t.Errorf("expected false (default) but got %v", result)
			}
		})
	})
}
