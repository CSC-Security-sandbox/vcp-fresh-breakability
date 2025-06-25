package utils

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	errs "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
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

func TestGenerateStrongPassword(t *testing.T) {
	// Test valid password generation
	length := 12
	password, err := GenerateStrongPassword(length)
	if err != nil {
		t.Fatalf("expected no error, but got: %v", err)
	}
	if len(password) != length {
		t.Errorf("expected password length %d, but got %d", length, len(password))
	}

	// Check if the password contains at least one character from each category
	hasLower, hasUpper, hasDigit, hasSpecial := false, false, false, false
	for _, char := range password {
		switch {
		case unicode.IsLower(char):
			hasLower = true
		case unicode.IsUpper(char):
			hasUpper = true
		case unicode.IsDigit(char):
			hasDigit = true
		case unicode.IsPunct(char) || unicode.IsSymbol(char):
			hasSpecial = true
		}
	}
	if !hasLower || !hasUpper || !hasDigit || !hasSpecial {
		t.Errorf("password does not contain all required character types: %s", password)
	}

	// Test invalid password length
	_, err = GenerateStrongPassword(6)
	if err == nil {
		t.Errorf("expected error for password length less than 8, but got none")
	}
}

func TestRetrierOnCodes(t *testing.T) {
	mockLogger := log.NewLogger()
	httpCode := 429
	t.Run("RetriesOnRetryCode", func(t *testing.T) {
		attempts := 0
		fn := func() (bool, error) {
			attempts++
			return false, &errs.CustomError{
				OriginalErr: errors.New("some error"),
				HttpCode:    &httpCode,
			}
		}
		RetrierOnCodes(mockLogger, fn, []int{429}, 2, time.Millisecond)
		if attempts != 2 {
			t.Errorf("expected 2 attempts, got %d", attempts)
		}
	})

	t.Run("StopsOnNonRetryableError", func(t *testing.T) {
		attempts := 0
		httpCode := 500
		fn := func() (bool, error) {
			attempts++
			return false, &errs.CustomError{
				OriginalErr: errors.New("some error"),
				HttpCode:    &httpCode,
			}
		}
		RetrierOnCodes(mockLogger, fn, []int{429}, 3, time.Millisecond)
		if attempts != 1 {
			t.Errorf("expected 1 attempt, got %d", attempts)
		}
	})

	t.Run("ReturnsOnNoError", func(t *testing.T) {
		attempts := 0
		fn := func() (bool, error) {
			attempts++
			return false, nil
		}
		RetrierOnCodes(mockLogger, fn, []int{429}, 3, time.Millisecond)
		if attempts != 1 {
			t.Errorf("expected 1 attempt, got %d", attempts)
		}
	})

	t.Run("StopsOnStopRetry", func(t *testing.T) {
		attempts := 0
		httpCode := 429
		fn := func() (bool, error) {
			attempts++
			return true, &errs.CustomError{
				OriginalErr: errors.New("some error"),
				HttpCode:    &httpCode,
			}
		}
		RetrierOnCodes(mockLogger, fn, []int{429}, 3, time.Millisecond)
		if attempts != 1 {
			t.Errorf("expected 1 attempt, got %d", attempts)
		}
	})
}

func Test_convertBytesToGib(t *testing.T) {
	tests := []struct {
		name  string
		input float64
		want  int64
	}{
		{"ZeroBytes", 0, 0},
		{"OneGiB", 1024 * 1024 * 1024, 1},
		{"HalfGiB", 0.5 * 1024 * 1024 * 1024, 0},
		{"NegativeBytes", -1024 * 1024 * 1024, -1},
		{"NonIntegerGiB", 1.7 * 1024 * 1024 * 1024, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := _convertBytesToGib(tt.input)
			if got != tt.want {
				t.Errorf("_convertBytesToGib(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestConvertsValidJsonToModel(t *testing.T) {
	t.Run("ValidJson", func(tt *testing.T) {
		jsonData := []byte(`{"name": "test", "age": 30}`)
		var model struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		err := ConvertJsonToModel(jsonData, &model)
		require.NoError(t, err)
		assert.Equal(t, "test", model.Name)
		assert.Equal(t, 30, model.Age)
	})
	t.Run("InvalidJson", func(tt *testing.T) {
		jsonData := []byte(`{"name": "test", "age": }`)
		var model struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		err := ConvertJsonToModel(jsonData, &model)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to unmarshal json")
	})
	t.Run("EmptyJson", func(tt *testing.T) {
		jsonData := []byte(``)
		var model struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		err := ConvertJsonToModel(jsonData, &model)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to unmarshal json")
	})
	t.Run("ConvertsJsonWithExtraFieldsToModel", func(tt *testing.T) {
		jsonData := []byte(`{"name": "test", "age": 30, "extra": "ignored"}`)
		var model struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		err := ConvertJsonToModel(jsonData, &model)
		require.NoError(t, err)
		assert.Equal(t, "test", model.Name)
		assert.Equal(t, 30, model.Age)
	})
	t.Run("FailsToConvertNonJsonInput", func(tt *testing.T) {
		jsonData := []byte(`not a json`)
		var model struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		err := ConvertJsonToModel(jsonData, &model)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to unmarshal json")
	})
}

func TestGetRequestIDFromContext(t *testing.T) {
	t.Run("RequestIDPresent", func(tt *testing.T) {
		fields := log.Fields{
			string(middleware.RequestID): "test-request-id",
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)
		got := GetRequestIDFromContext(ctx)
		assert.Equal(tt, "test-request-id", got)
	})

	t.Run("RequestIDAbsent", func(tt *testing.T) {
		fields := log.Fields{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)
		got := GetRequestIDFromContext(ctx)
		assert.Equal(tt, "", got)
	})

	t.Run("NoFieldsInContext", func(tt *testing.T) {
		ctx := context.Background()
		got := GetRequestIDFromContext(ctx)
		assert.Equal(tt, "", got)
	})
}

func TestGenerateRandomString(t *testing.T) {
	t.Run("GeneratesRandomStringOfCorrectLength", func(t *testing.T) {
		result, err := generateRandomString(10)
		assert.NoError(t, err)
		assert.Equal(t, 10, len(result))
	})
	t.Run("FailsToGenerateRandomStringWhenLengthIsZero", func(t *testing.T) {
		result, err := generateRandomString(0)
		assert.NoError(t, err)
		assert.Equal(t, "", result)
	})
	t.Run("GeneratesRandomStringWithValidCharacters", func(t *testing.T) {
		result, err := generateRandomString(15)
		assert.NoError(t, err)
		for _, char := range result {
			assert.Contains(t, "abcdefghijklmnopqrstuvwxyz0123456789", string(char))
		}
	})
}

func TestSliceRegionForServiceAccount(t *testing.T) {
	t.Run("RegionUnchangedWhenWithinMaxLength", func(t *testing.T) {
		region := "short-region-name"
		expected := "short-region-name"
		result := sliceRegionForServiceAccount(region)
		assert.Equal(t, expected, result)
	})
	t.Run("RegionEmptyStringReturnsEmptyString", func(t *testing.T) {
		region := ""
		expected := ""
		result := sliceRegionForServiceAccount(region)
		assert.Equal(t, expected, result)
	})
	t.Run("RegionExactlyMaxLengthReturnsSameRegion", func(t *testing.T) {
		region := "this-is-exactly-25-characters"
		expected := "this-is-exactly-25-charac"
		result := sliceRegionForServiceAccount(region)
		assert.Equal(t, expected, result)
	})
}

func TestGeneratesValidResourceNames(t *testing.T) {
	gcpRegion := "us-central1"
	tenantProjectRegion := "us-central1"
	tenantProjectNumber := "123456789"
	backupVaultUUID := "vault-uuid"
	email, bucketName, serviceAccountId, err := GetResourcesNameForBackup(gcpRegion, tenantProjectRegion, tenantProjectNumber, backupVaultUUID)
	require.NoError(t, err)
	assert.Equal(t, "vsa-backup-us-cent", serviceAccountId[:18])
	assert.Contains(t, email, "@123456789.iam.gserviceaccount.com")
	assert.Contains(t, bucketName, "vsa-backup-vault-uuid")
}

func TestHandlesEmptyRegion(t *testing.T) {
	gcpRegion := ""
	tenantProjectRegion := "us-central1"
	tenantProjectNumber := "123456789"
	backupVaultUUID := "vault-uuid"
	email, bucketName, serviceAccountId, err := GetResourcesNameForBackup(gcpRegion, tenantProjectRegion, tenantProjectNumber, backupVaultUUID)
	require.NoError(t, err)
	assert.Equal(t, "vsa-backup", serviceAccountId[:10])
	assert.Contains(t, email, "@123456789.iam.gserviceaccount.com")
	assert.Contains(t, bucketName, "vsa-backup-vault-uuid")
}

func TestHandlesErrorOnRandomStringGeneration(t *testing.T) {
	gcpRegion := "us-central1"
	tenantProjectRegion := "us-central1"
	tenantProjectNumber := "123456789"
	backupVaultUUID := "vault-uuid"
	defer func() {
		generateRandomString = _generateRandomString
	}()
	generateRandomString = func(length int) (string, error) {
		return "", errors.New("random string generation failed")
	}
	email, bucketName, serviceAccountId, err := GetResourcesNameForBackup(gcpRegion, tenantProjectRegion, tenantProjectNumber, backupVaultUUID)
	require.Error(t, err)
	assert.Equal(t, "", email)
	assert.Equal(t, "", bucketName)
	assert.Equal(t, "", serviceAccountId)
}

func TestGetLunName(t *testing.T) {
	tests := []struct {
		testName   string
		volumeName string
		want       string
	}{
		{"WhenVolumeNameIsPassed_ThenReturnLunName_1", "my_volume", "lun_my_volume"},
		{"WhenVolumeNameIsPassed_ThenReturnLunName_2", "volume", "lun_volume"},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			got := GetLunName(tt.volumeName)
			if got != tt.want {
				t.Errorf("GetLunName(%s) = %s, want %s", tt.volumeName, got, tt.want)
			}
		})
	}
}

func TestIsTransitionalState(t *testing.T) {
	tests := []struct {
		name  string
		state string
		want  bool
	}{
		{"CreatingState", "CREATING", true},
		{"UpdatingState", models.LifeCycleStateCreating, true},
		{"UpdatingState", models.LifeCycleStateUpdating, true},
		{"DeletingState", models.LifeCycleStateDeleting, true},
		{"ReadyState", models.LifeCycleStateREADY, false},
		{"AvailableState", models.LifeCycleStateAvailable, false},
		{"EmptyString", "", false},
		{"RandomString", "SOME_UNKNOWN_STATE", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsTransitionalState(tt.state)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateCcfeReplicationUri(t *testing.T) {
	tests := []struct {
		name  string
		uri   string
		valid bool
	}{
		{"ValidURI", "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6", true},
		{"InvalidURI", "invalid://project/region/volume", false},
		{"EmptyURI", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCcfeReplicationUri(tt.uri)
			if tt.valid {
				assert.NoError(t, err, "Expected no error for valid URI")
			} else {
				assert.Error(t, err, "Expected error for invalid URI")
			}
		})
	}
}

func TestCFFEURIToMap(t *testing.T) {
	tests := []struct {
		name     string
		ccfeUri  string
		expected map[string]string
	}{
		{"ValidURI", "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6",
			map[string]string{"projects": "45110233509", "locations": "us-east4", "volumes": "gosrcvolume1", "replications": "replication-name-6"}},
		{"InvalidURI", "invalid://project/region/volume", nil},
		{"EmptyURI", "", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CFFEURIToMap(tt.ccfeUri)
			if tt.expected == nil {
				assert.Error(t, err, "Expected error for invalid URI")
				assert.Empty(t, result, "Expected result to be nil for invalid URI")
			} else {
				assert.NoError(t, err, "Expected no error for valid URI")
				assert.Equal(t, tt.expected, result, "Expected map to match")
			}
		})
	}
}

func TestParseProjectNumberFromURI(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		expected string
	}{
		{"ValidURI", "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6", "45110233509"},
		{"InvalidURI", "invalid/uri/format", ""},
		{"EmptyURI", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := _parseProjectNumberFromURI(tt.uri)
			if err != nil {
				assert.Empty(t, result, "Expected empty result for invalid URI")
				return
			}
			assert.Equal(t, tt.expected, result, "Expected project number to match")
		})
	}
}

func TestGetEncryptionType(t *testing.T) {
	cloudKms := "kms-id"
	servManaged := (*string)(nil)

	got := GetEncryptionType(&cloudKms)
	assert.Equal(t, "CLOUD_KMS", got)

	got = GetEncryptionType(servManaged)
	assert.Equal(t, "SERVICE_MANAGED", got)

	empty := ""
	got = GetEncryptionType(&empty)
	assert.Equal(t, "SERVICE_MANAGED", got)
}
