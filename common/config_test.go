package common

import (
	"os"
	"strings"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

func TestValidateRegionMap(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		err := os.Setenv("LOCAL_REGION", "us-central1")
		if err != nil {
			return
		}
		err = os.Setenv("REGION_CODE_MAP", `{"us-central1": "34"}`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_CODE_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_CODE_MAP", "")
		err = validateRegionMap(region, regionMapJsonForNodeSerialNumber)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
	t.Run("RegionNotMapped", func(t *testing.T) {
		err := os.Setenv("LOCAL_REGION", "unknown-region")
		if err != nil {
			return
		}
		err = os.Setenv("REGION_CODE_MAP", `{"africa-south1": "01"`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_CODE_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_CODE_MAP", "")
		err = validateRegionMap(region, regionMapJsonForNodeSerialNumber)
		if err == nil {
			t.Errorf("expected error for unmapped region, got nil")
		}
	})
	t.Run("InvalidJSON", func(t *testing.T) {
		err := os.Setenv("LOCAL_REGION", "us-central1")
		if err != nil {
			return
		}
		err = os.Setenv("REGION_CODE_MAP", `{africa-south1}`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_CODE_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_CODE_MAP", "")
		err = validateRegionMap(region, regionMapJsonForNodeSerialNumber)
		if err == nil {
			t.Errorf("expected error for invalid JSON, got nil")
		}
	})
	t.Run("DuplicateRegionNumber", func(t *testing.T) {
		err := os.Setenv("LOCAL_REGION", "us-central1")
		if err != nil {
			return
		}
		err = os.Setenv("REGION_CODE_MAP", `{"africa-south1": "34","us-central1": "34"}`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_CODE_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_CODE_MAP", "")
		err = validateRegionMap(region, regionMapJsonForNodeSerialNumber)
		if !strings.Contains(err.Error(), "duplicate region code value found") {
			t.Errorf("expected error for duplicate region code")
		}
	})
}
