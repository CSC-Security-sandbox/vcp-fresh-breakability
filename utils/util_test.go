package utils

import (
	"strings"
	"testing"
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
		} else if !strings.Contains(err.Error(), "VPC peering network for TenancyUnit") {
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
