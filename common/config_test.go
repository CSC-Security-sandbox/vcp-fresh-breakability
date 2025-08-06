package common

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

func TestLoadConfig(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		err := os.Setenv("LOCAL_REGION", "us-central1")
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
		}()
		config := LoadConfig()
		if config == nil {
			t.Errorf("expected config to be set")
		}
	})
	t.Run("Failure", func(t *testing.T) {
		config := LoadConfig()
		if config != nil {
			t.Errorf("expected config to be nil")
		}
	})
}

func TestValidateRegionMap(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		err := os.Setenv("LOCAL_REGION", "us-central1")
		if err != nil {
			return
		}
		err = os.Setenv("REGION_NUMBER_MAP", `{"us-central1": "34"}`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_NUMBER_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_NUMBER_MAP", "")
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
		err = os.Setenv("REGION_NUMBER_MAP", `{"africa-south1": "01"`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_NUMBER_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_NUMBER_MAP", "")
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
		err = os.Setenv("REGION_NUMBER_MAP", `{africa-south1}`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_NUMBER_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_NUMBER_MAP", "")
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
		err = os.Setenv("REGION_NUMBER_MAP", `{"africa-south1": "34","us-central1": "34"}`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_NUMBER_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_NUMBER_MAP", "")
		err = validateRegionMap(region, regionMapJsonForNodeSerialNumber)
		if !strings.Contains(err.Error(), "duplicate region code value found") {
			t.Errorf("expected error for duplicate region code")
		}
	})
	t.Run("EmptyRegionMap", func(t *testing.T) {
		err := os.Setenv("LOCAL_REGION", "us-central1")
		if err != nil {
			return
		}
		err = os.Setenv("REGION_NUMBER_MAP", `{}`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_NUMBER_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_NUMBER_MAP", "")
		err = validateRegionMap(region, regionMapJsonForNodeSerialNumber)
		if err == nil {
			t.Errorf("expected error for empty region map, got nil")
		}
	})
	t.Run("EmptyLocalRegion", func(t *testing.T) {
		err := os.Setenv("LOCAL_REGION", "")
		if err != nil {
			return
		}
		err = os.Setenv("REGION_NUMBER_MAP", `{"us-central1": "34"}`)
		if err != nil {
			return
		}
		defer func() {
			err := os.Unsetenv("LOCAL_REGION")
			if err != nil {
				t.Errorf("Failed to unset region")
			}
			err = os.Unsetenv("REGION_NUMBER_MAP")
			if err != nil {
				return
			}
		}()
		region := env.GetString("LOCAL_REGION", "")
		regionMapJsonForNodeSerialNumber := env.GetString("REGION_NUMBER_MAP", "")
		err = validateRegionMap(region, regionMapJsonForNodeSerialNumber)
		if err == nil {
			t.Errorf("expected error for empty local region, got nil")
		}
	})
}

func TestLoadConfig_MetricsDBEnvVars(t *testing.T) {
	// Set environment variables for metrics DB
	_ = os.Setenv("LOCAL_REGION", "us-central1")
	_ = os.Setenv("METRICS_DB_TYPE", "sqlite")
	_ = os.Setenv("METRICS_DB_HOST", "localhost")
	_ = os.Setenv("METRICS_DB_PORT", "1234")
	_ = os.Setenv("METRICS_DB_USER", "metricsuser")
	_ = os.Setenv("METRICS_DB_PASSWORD", "metricspass")
	_ = os.Setenv("METRICS_DB_NAME", "metricsdb")
	_ = os.Setenv("METRICS_DB_SSL_MODE", "require")
	_ = os.Setenv("METRICS_DB_TIMEZONE", "Asia/Kolkata")
	_ = os.Setenv("METRICS_DB_MAX_OPEN_CONNS", "99")
	_ = os.Setenv("METRICS_DB_MAX_IDLE_CONNS", "77")
	_ = os.Setenv("METRICS_DB_CONN_MAX_LIFETIME", "2h")

	cfg := LoadConfig()
	assert.NotNil(t, cfg)
	assert.Equal(t, "sqlite", cfg.MetricsDBType)
	assert.Equal(t, "localhost", cfg.MetricsDBHost)
	assert.Equal(t, "1234", cfg.MetricsDBPort)
	assert.Equal(t, "metricsuser", cfg.MetricsDBUser)
	assert.Equal(t, "metricspass", cfg.MetricsDBPassword)
	assert.Equal(t, "metricsdb", cfg.MetricsDBName)
	assert.Equal(t, "require", cfg.MetricsDBSSLMode)
	assert.Equal(t, 99, cfg.MetricsDBMaxOpenConns)
	assert.Equal(t, 77, cfg.MetricsDBMaxIdleConns)
	assert.Equal(t, 2*time.Hour, cfg.MetricsDBConnMaxLifetime)
	assert.Equal(t, "Asia/Kolkata", cfg.MetricsDBTimeZone.String())

	// Clean up
	_ = os.Unsetenv("METRICS_DB_TYPE")
	_ = os.Unsetenv("METRICS_DB_HOST")
	_ = os.Unsetenv("METRICS_DB_PORT")
	_ = os.Unsetenv("METRICS_DB_USER")
	_ = os.Unsetenv("METRICS_DB_PASSWORD")
	_ = os.Unsetenv("METRICS_DB_NAME")
	_ = os.Unsetenv("METRICS_DB_SSL_MODE")
	_ = os.Unsetenv("METRICS_DB_TIMEZONE")
	_ = os.Unsetenv("METRICS_DB_MAX_OPEN_CONNS")
	_ = os.Unsetenv("METRICS_DB_MAX_IDLE_CONNS")
	_ = os.Unsetenv("METRICS_DB_CONN_MAX_LIFETIME")
}
