package utils

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestValidateIPv4Address(t *testing.T) {
	tests := []struct {
		ipAddr string
		want   bool
	}{
		{"192.168.1.1", true},
		{"255.255.255.255", true},
		{"0.0.0.0", true},
		{"256.256.256.256", false},
		{"abc.def.ghi.jkl", false},
	}

	for _, tt := range tests {
		t.Run(tt.ipAddr, func(t *testing.T) {
			if got := ValidateIPv4Address(tt.ipAddr); got != tt.want {
				t.Errorf("ValidateIPv4Address(%v) = %v, want %v", tt.ipAddr, got, tt.want)
			}
		})
	}
}

func TestItemsInSliceUnique(t *testing.T) {
	tests := []struct {
		in   []string
		want bool
	}{
		{[]string{"a", "b", "c"}, true},
		{[]string{"a", "b", "a"}, false},
		{[]string{"A", "a"}, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := ItemsInSliceUnique(tt.in); got != tt.want {
				t.Errorf("ItemsInSliceUnique(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestContainsString(t *testing.T) {
	tests := []struct {
		arr  []string
		elem string
		want bool
	}{
		{[]string{"a", "b", "c"}, "b", true},
		{[]string{"a", "b", "c"}, "d", false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := ContainsString(tt.arr, tt.elem); got != tt.want {
				t.Errorf("ContainsString(%v, %v) = %v, want %v", tt.arr, tt.elem, got, tt.want)
			}
		})
	}
}

func TestEnvToInt32Conversion(t *testing.T) {
	tests := []struct {
		envVal string
		def    int32
		want   int32
	}{
		{"123", 0, 123},
		{"abc", 0, 0},
		{"2147483647", 0, 2147483647},
		{"-2147483648", 0, -2147483648},
	}

	for _, tt := range tests {
		t.Run(tt.envVal, func(t *testing.T) {
			if got := EnvToInt32Conversion(tt.envVal, tt.def); got != tt.want {
				t.Errorf("EnvToInt32Conversion(%v, %v) = %v, want %v", tt.envVal, tt.def, got, tt.want)
			}
		})
	}
}

func TestCheckForRetriableError(t *testing.T) {
	tests := []struct {
		errorMessage    string
		retriableErrors []string
		want            bool
	}{
		{"network timeout", []string{"timeout", "temporary"}, true},
		{"disk full", []string{"timeout", "temporary"}, false},
		{"", []string{"timeout", "temporary"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.errorMessage, func(t *testing.T) {
			if got := CheckForRetriableError(tt.errorMessage, tt.retriableErrors); got != tt.want {
				t.Errorf("CheckForRetriableError(%v, %v) = %v, want %v", tt.errorMessage, tt.retriableErrors, got, tt.want)
			}
		})
	}
}

func TestContainsInt(t *testing.T) {
	tests := []struct {
		arr  []int
		elem int
		want bool
	}{
		{[]int{1, 2, 3}, 2, true},
		{[]int{1, 2, 3}, 4, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := ContainsInt(tt.arr, tt.elem); got != tt.want {
				t.Errorf("ContainsInt(%v, %v) = %v, want %v", tt.arr, tt.elem, got, tt.want)
			}
		})
	}
}

func TestContainsFloat64(t *testing.T) {
	tests := []struct {
		arr  []float64
		elem float64
		want bool
	}{
		{[]float64{1.1, 2.2, 3.3}, 2.2, true},
		{[]float64{1.1, 2.2, 3.3}, 4.4, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := ContainsFloat64(tt.arr, tt.elem); got != tt.want {
				t.Errorf("ContainsFloat64(%v, %v) = %v, want %v", tt.arr, tt.elem, got, tt.want)
			}
		})
	}
}

func TestIsDuplicateUUID(t *testing.T) {
	tests := []struct {
		keys map[string]bool
		uuid string
		want bool
	}{
		{map[string]bool{"uuid1": true, "uuid2": true}, "uuid1", true},
		{map[string]bool{"uuid1": true, "uuid2": true}, "uuid3", false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := IsDuplicateUUID(tt.keys, tt.uuid); got != tt.want {
				t.Errorf("IsDuplicateUUID(%v, %v) = %v, want %v", tt.keys, tt.uuid, got, tt.want)
			}
		})
	}
}

func TestParseProjectId(t *testing.T) {
	t.Run("ValidNetwork", func(tt *testing.T) {
		project, network, err := ParseProjectId("projects/12345/global/networks/my-network")
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		}
		if project != "12345" {
			tt.Errorf("Unexpected project ID: %s", project)
		}
		if network != "my-network" {
			tt.Errorf("Unexpected network name: %s", network)
		}
	})
	t.Run("InvalidNetwork", func(tt *testing.T) {
		_, _, err := ParseProjectId("invalid/network/format")
		if err == nil {
			tt.Error("Expected an error but got none")
		} else if !strings.Contains(err.Error(), "parseProjectId failed for network ") {
			tt.Errorf("Unexpected error message: %s", err.Error())
		}
	})
}

func TestConvertToBytes(t *testing.T) {
	tests := []struct {
		size   float64
		unit   Unit
		want   int64
		hasErr bool
	}{
		{1, B, 1, false},
		{1, KiB, 1024, false},
		{1, MiB, 1024 * 1024, false},
		{1, GiB, 1024 * 1024 * 1024, false},
		{0.5, GiB, 536870912, false},
		{0.33, GiB, 354334801, false}, // actual 354334801.92 floored to 354334801
		{1, TiB, 1024 * 1024 * 1024 * 1024, false},
		{1, PiB, 1024 * 1024 * 1024 * 1024 * 1024, false},
		{1, Unit(0), 0, true}, // Invalid unit
	}

	for _, tt := range tests {
		t.Run("TestConvertToBytes", func(t *testing.T) {
			got, err := ConvertToBytes(tt.size, tt.unit)
			if (err != nil) != tt.hasErr {
				t.Errorf("ConvertToBytes(%v, %v) error = %v, wantErr %v", tt.size, tt.unit, err, tt.hasErr)
				return
			}
			if got != tt.want {
				t.Errorf("ConvertToBytes(%v, %v) = %v, want %v", tt.size, tt.unit, got, tt.want)
			}
		})
	}
}

func TestSafeOptFloat64(t *testing.T) {
	tests := []struct {
		name  string
		input *float64
		want  gcpgenserver.OptFloat64
	}{
		{"NilInput", nil, gcpgenserver.OptFloat64{}},
		{"ValidInput", func() *float64 { v := 3.14; return &v }(), gcpgenserver.NewOptFloat64(3.14)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeOptFloat64(tt.input)
			if got != tt.want {
				t.Errorf("SafeOptFloat64(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSafeBool(t *testing.T) {
	tests := []struct {
		name  string
		input *bool
		want  gcpgenserver.OptNilBool
	}{
		{"NilInput", nil, gcpgenserver.NewOptNilBool(false)},
		{"ValidInput", func() *bool { v := true; return &v }(), gcpgenserver.NewOptNilBool(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeBool(tt.input)
			if got != tt.want {
				t.Errorf("SafeBool(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSafeString(t *testing.T) {
	tests := []struct {
		name  string
		input *string
		want  gcpgenserver.OptNilString
	}{
		{"NilInput", nil, gcpgenserver.OptNilString{}},
		{"ValidInput", func() *string { v := "true"; return &v }(), gcpgenserver.NewOptNilString("true")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeString(tt.input)
			if got != tt.want {
				t.Errorf("SafeBool(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSafeFloat64(t *testing.T) {
	tests := []struct {
		name  string
		input *float64
		want  gcpgenserver.OptNilFloat64
	}{
		{"NilInput", nil, gcpgenserver.OptNilFloat64{}},
		{"ValidInput", func() *float64 { var v float64 = 1; return &v }(), gcpgenserver.NewOptNilFloat64(1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeFloat64(tt.input)
			if got != tt.want {
				t.Errorf("SafeFloat64(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSafeInt64(t *testing.T) {
	tests := []struct {
		name  string
		input *int64
		want  gcpgenserver.OptNilInt64
	}{
		{"NilInput", nil, gcpgenserver.OptNilInt64{}},
		{"ValidInput", func() *int64 { var v int64 = 1; return &v }(), gcpgenserver.NewOptNilInt64(1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeInt64(tt.input)
			if got != tt.want {
				t.Errorf("SafeInt64(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSafeInt64ToInt32(t *testing.T) {
	tests := []struct {
		name  string
		input *int64
		want  gcpgenserver.OptNilInt32
	}{
		{"NilInput", nil, gcpgenserver.OptNilInt32{}},
		{"ValidInput", func() *int64 { var v int64 = 1; return &v }(), gcpgenserver.NewOptNilInt32(1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SafeInt64ToInt32(tt.input)
			if got != tt.want {
				t.Errorf("SafeInt64ToInt32(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetOptString(t *testing.T) {
	tests := []struct {
		name  string
		input *string
		want  gcpgenserver.OptString
	}{
		{"NilInput", nil, gcpgenserver.OptString{}},
		{"ValidInput", func() *string { v := "a"; return &v }(), gcpgenserver.NewOptString("a")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetOptString(tt.input)
			if got != tt.want {
				t.Errorf("GetOptString(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetOptInt64(t *testing.T) {
	tests := []struct {
		name  string
		input *int64
		want  gcpgenserver.OptInt64
	}{
		{"NilInput", nil, gcpgenserver.OptInt64{}},
		{"ValidInput", func() *int64 { var v int64 = 1; return &v }(), gcpgenserver.NewOptInt64(1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetOptInt64(tt.input)
			if got != tt.want {
				t.Errorf("GetOptString(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetOptBool(t *testing.T) {
	tests := []struct {
		name  string
		input *bool
		want  gcpgenserver.OptBool
	}{
		{"NilInput", nil, gcpgenserver.OptBool{}},
		{"ValidInput", func() *bool { var v = true; return &v }(), gcpgenserver.NewOptBool(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetOptBool(tt.input)
			if got != tt.want {
				t.Errorf("GetOptBool(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestConvertStringToMap(t *testing.T) {
	t.Run("WhenErrorMarshalling", func(tt *testing.T) {
		inputString := "{\"an-awesome-place\" : }"
		s, err := ConvertStringToMap(inputString)
		require.EqualError(tt, err, "error when unmarshalling response")
		assert.Nil(tt, s)
	})
	t.Run("WhenOk", func(tt *testing.T) {
		inputString := "{\"an-awesome-place\" : \"http:10.10.10.1\", \"ok-place\" : \"10.100\"}"
		s, err := ConvertStringToMap(inputString)
		assert.NoError(tt, err)
		assert.Equal(tt, "http:10.10.10.1", s["an-awesome-place"])
	})
}

func TestGetPairedRegionURI(t *testing.T) {
	PairedRegions = "{\"an-awesome-place\" : \"someIp\"}"
	t.Run("WhenNotFound", func(tt *testing.T) {
		regions := make(map[string]string)
		regions["ok-place"] = "someIp"
		defer func() {
			ConvertStringToMap = _convertStringToMap
		}()
		ConvertStringToMap = func(s string) (map[string]string, error) {
			return regions, nil
		}
		region, err := GetPairedRegionURI("an-awesome-place")
		require.EqualError(tt, err, "region not found in paired regions list")
		assert.Equal(tt, "", region)
	})
	t.Run("WhenConvertReturnsError", func(tt *testing.T) {
		defer func() {
			ConvertStringToMap = _convertStringToMap
		}()
		ConvertStringToMap = func(s string) (map[string]string, error) {
			return nil, errors.New("some error")
		}
		region, err := GetPairedRegionURI("west")
		require.EqualError(tt, err, "some error")
		assert.Equal(tt, "", region)
	})
	t.Run("WhenFound", func(tt *testing.T) {
		regions := make(map[string]string)
		regions["an-awesome-place"] = "someIp"
		region, err := GetPairedRegionURI("an-awesome-place")
		require.NoError(tt, err, "region unexpectedly not found")
		assert.Equal(tt, "someIp", region)
	})
	t.Run("WhenNothingDefinedInConfig", func(tt *testing.T) {
		regions := make(map[string]string)
		regions["an-awesome-place"] = "someIp"
		PairedRegions = ""
		defer func() {
			ConvertStringToMap = _convertStringToMap
			PairedRegions = "{\"an-awesome-place\" : \"someIp\"}"
		}()
		ConvertStringToMap = func(s string) (map[string]string, error) {
			return regions, nil
		}
		region, err := GetPairedRegionURI("an-awesome-place")
		require.EqualError(tt, err, "paired regions not defined for this region")
		assert.Equal(tt, "", region)
	})
}

func TestGetCoRelationIDFromContext(t *testing.T) {
	// Create a context with http.Header
	header := http.Header{}
	header.Set(string(middleware.CorrelationIDName), "test-correlation-id")
	ctxWithHeader := context.WithValue(context.Background(), middleware.CorrelationContextKey, header)

	// Create a context with log.Fields
	fields := log.Fields{
		string(middleware.RequestCorrelationID): "test-correlation-id-from-logger",
	}
	ctxWithFields := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)

	// Test case 1: Context with http.Header
	t.Run("Context with http.Header", func(t *testing.T) {
		result := GetCoRelationIDFromContext(ctxWithHeader)
		assert.Equal(t, "test-correlation-id", result)
	})

	// Test case 2: Context with log.Fields
	t.Run("Context with log.Fields", func(t *testing.T) {
		result := GetCoRelationIDFromContext(ctxWithFields)
		assert.Equal(t, "test-correlation-id-from-logger", result)
	})

	// Test case 3: Context with no correlation ID
	t.Run("Context with no correlation ID", func(t *testing.T) {
		ctx := context.Background()
		result := GetCoRelationIDFromContext(ctx)
		assert.Equal(t, "", result)
	})
}
