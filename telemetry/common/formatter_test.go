package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func createPoolMetadata(billable bool, coolTier bool, serviceLevel string) metadata.ResourceMetadata {
	resourceMetadata := metadata.ResourceMetadata{}
	resourceMetadata.SetResourceUUID("resource-uuid")
	resourceMetadata.SetResourceName("resource-name")
	resourceMetadata.SetResourceType(metadata.VolumePool)
	resourceMetadata.SetAccountName("account-name")
	// resourceMetadata.SetVendorResourceId("vendor-resource-id") // Method not available
	// resourceMetadata.SetSdeAccountUUID("sde-account-uuid") // Method not available
	resourceMetadata.SetServiceLevel(serviceLevel) // Method not available
	// resourceMetadata.SetTags("tags") // Method not available
	// resourceMetadata.SetDoubleEncryption(true) // Method not available
	// resourceMetadata.SetState("state") // Method not available
	// resourceMetadata.SetBillable(billable) // Method not available
	// resourceMetadata.SetCoolTierEnabled(coolTier) // Method not available
	// The cluster name is not a property of interest in these tests, but setting it here to a random UUID
	// helps us differentiate between metrics that otherwise have the same values in all the metadata
	// properties that we are interested in. This helps us determine whether we are setting the correct
	// metadata on time series.
	// resourceMetadata.SetClusterName(uuid.New().String()) // Method not available

	return resourceMetadata
}

func createHydratedMetric(timestamp time.Time, deletedAt *time.Time, quantity float64, billable bool, coolTier bool, serviceLevel string) entity.HydratedMetric {
	resourceMetadata := createPoolMetadata(billable, coolTier, serviceLevel)

	if deletedAt != nil {
		// resourceMetadata.SetDeletedAt(*deletedAt) // Method not available
	}

	return entity.HydratedMetric{
		Metadata:     resourceMetadata,
		Timestamp:    entity.UnixNano(timestamp.UnixNano()),
		MeasuredType: metadata.AllocatedSize,
		Quantity:     quantity,
	}
}

func Test_hasMetadataChanged(t *testing.T) {
	metric1 := entity.HydratedMetric{
		Metadata: createPoolMetadata(true, false, "low"),
	}
	metric2 := entity.HydratedMetric{
		Metadata: createPoolMetadata(true, false, "low"),
	}

	t.Run("All properties have same value", func(t *testing.T) {
		if got := hasMetadataChanged(metric1, metric2); got != false {
			t.Errorf("hasMetadataChanged() = %v, want %v", got, false)
		}
	})

	t.Run("Different resource names", func(t *testing.T) {
		metric2.Metadata.SetResourceName("something-different")
		if got := hasMetadataChanged(metric1, metric2); got != true {
			t.Errorf("hasMetadataChanged() = %v, want %v", got, true)
		}
	})

	t.Run("Different service levels", func(t *testing.T) {
		metric2.Metadata.SetServiceLevel("something-different")
		if got := hasMetadataChanged(metric1, metric2); got != true {
			t.Errorf("hasMetadataChanged() = %v, want %v", got, true)
		}
	})

	t.Run("Different account names", func(t *testing.T) {
		metric2.Metadata.SetAccountName("something-different")
		if got := hasMetadataChanged(metric1, metric2); got != true {
			t.Errorf("hasMetadataChanged() = %v, want %v", got, true)
		}
	})

	t.Run("Nil resource name in first metric", func(t *testing.T) {
		metric1.Metadata.ResourceName = nil
		metric2.Metadata.SetResourceName("some-name")
		if got := hasMetadataChanged(metric1, metric2); got != true {
			t.Errorf("hasMetadataChanged() = %v, want %v", got, true)
		}
	})

	t.Run("Nil resource name in second metric", func(t *testing.T) {
		metric1.Metadata.SetResourceName("some-name")
		metric2.Metadata.ResourceName = nil
		if got := hasMetadataChanged(metric1, metric2); got != true {
			t.Errorf("hasMetadataChanged() = %v, want %v", got, true)
		}
	})

	t.Run("Both resource names nil", func(t *testing.T) {
		metric1.Metadata.ResourceName = nil
		metric2.Metadata.ResourceName = nil
		metric1.Metadata.SetAccountName("account")
		metric2.Metadata.SetAccountName("account")
		metric1.Metadata.SetServiceLevel("low")
		metric2.Metadata.SetServiceLevel("low")
		if got := hasMetadataChanged(metric1, metric2); got != false {
			t.Errorf("hasMetadataChanged() = %v, want %v", got, false)
		}
	})
}

func Test_hasMetadataChanged_EdgeCases(t *testing.T) {
	// Test to cover specific missing lines: 35,42-43,45-46,48

	t.Run("Edge case - all nil metadata fields", func(t *testing.T) {
		metric1 := entity.HydratedMetric{
			Metadata: metadata.ResourceMetadata{
				ResourceType: metadata.VolumePool,
				ResourceName: nil,
				AccountName:  nil,
				ServiceLevel: nil,
			},
		}
		metric2 := entity.HydratedMetric{
			Metadata: metadata.ResourceMetadata{
				ResourceType: metadata.VolumePool,
				ResourceName: nil,
				AccountName:  nil,
				ServiceLevel: nil,
			},
		}

		// Should return false when all comparable fields are nil in both
		if got := hasMetadataChanged(metric1, metric2); got != false {
			t.Errorf("hasMetadataChanged() with all nil fields = %v, want %v", got, false)
		}
	})

	t.Run("Edge case - mixed nil conditions", func(t *testing.T) {
		resourceName := "test-resource"
		accountName := "test-account"

		metric1 := entity.HydratedMetric{
			Metadata: metadata.ResourceMetadata{
				ResourceType: metadata.VolumePool,
				ResourceName: &resourceName,
				AccountName:  nil,
				ServiceLevel: nil,
			},
		}
		metric2 := entity.HydratedMetric{
			Metadata: metadata.ResourceMetadata{
				ResourceType: metadata.VolumePool,
				ResourceName: nil,
				AccountName:  &accountName,
				ServiceLevel: nil,
			},
		}

		// Should return true when one has resource name and other has account name
		if got := hasMetadataChanged(metric1, metric2); got != true {
			t.Errorf("hasMetadataChanged() with mixed nil conditions = %v, want %v", got, true)
		}
	})
}

func Test_bothNilOrEqual(t *testing.T) {
	t.Run("strings", func(t *testing.T) {
		// Create variables to take addresses from
		someString := "some string"
		anotherString := "another string"

		type args struct {
			value1 *string
			value2 *string
		}
		tests := []struct {
			name string
			args args
			want bool
		}{
			{
				name: "Both nil",
				args: args{nil, nil},
				want: true,
			},
			{
				name: "First nil, second not nil",
				args: args{nil, &someString},
				want: false,
			},
			{
				name: "First not nil, second nil",
				args: args{&someString, nil},
				want: false,
			},
			{
				name: "Both same string ",
				args: args{&someString, &someString},
				want: true,
			},
			{
				name: "Both different strings",
				args: args{&someString, &anotherString},
				want: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := bothNilOrEqual(tt.args.value1, tt.args.value2); got != tt.want {
					t.Errorf("bothNilOrEqual() = %v, want %v", got, tt.want)
				}
			})
		}
	})
	t.Run("Bools", func(t *testing.T) {
		// Create variables to take addresses from
		trueVal := true
		falseVal := false

		type args struct {
			value1 *bool
			value2 *bool
		}
		tests := []struct {
			name string
			args args
			want bool
		}{
			{
				name: "Both nil",
				args: args{nil, nil},
				want: true,
			},
			{
				name: "First nil, second not nil",
				args: args{nil, &trueVal},
				want: false,
			},
			{
				name: "First not nil, second nil",
				args: args{&falseVal, nil},
				want: false,
			},
			{
				name: "Both same value ",
				args: args{&falseVal, &falseVal},
				want: true,
			},
			{
				name: "Both different values",
				args: args{&trueVal, &falseVal},
				want: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := bothNilOrEqual(tt.args.value1, tt.args.value2); got != tt.want {
					t.Errorf("bothNilOrEqual() = %v, want %v", got, tt.want)
				}
			})
		}
	})
	t.Run("integers", func(t *testing.T) {
		var nilInt *int = nil
		int1 := 42
		int2 := 42
		int3 := 24

		// Test all combinations
		tests := []struct {
			name   string
			value1 *int
			value2 *int
			want   bool
		}{
			{"both nil", nilInt, nilInt, true},
			{"first nil", nilInt, &int1, false},
			{"second nil", &int1, nilInt, false},
			{"same values", &int1, &int2, true},
			{"different values", &int1, &int3, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := bothNilOrEqual(tt.value1, tt.value2); got != tt.want {
					t.Errorf("bothNilOrEqual() = %v, want %v", got, tt.want)
				}
			})
		}
	})
}

// Helper function to create string pointers
func strPtr(s string) *string {
	return &s
}

// Helper function to create int pointers
func intPtr(i int) *int {
	return &i
}

func TestHasMetadataChanged(t *testing.T) {
	tests := []struct {
		name     string
		metric1  entity.HydratedMetric
		metric2  entity.HydratedMetric
		expected bool
	}{
		{
			name: "All metadata fields are equal (non-nil)",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			expected: false,
		},
		{
			name: "ResourceName differs",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-2"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			expected: true,
		},
		{
			name: "AccountName differs",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-2"),
					ServiceLevel: strPtr("premium"),
				},
			},
			expected: true,
		},
		{
			name: "ServiceLevel differs",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("standard"),
				},
			},
			expected: true,
		},
		{
			name: "Both ResourceName nil",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: nil,
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: nil,
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			expected: false,
		},
		{
			name: "One ResourceName nil, other not nil",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: nil,
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			expected: true,
		},
		{
			name: "One AccountName nil, other not nil",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  nil,
					ServiceLevel: strPtr("premium"),
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			expected: true,
		},
		{
			name: "One ServiceLevel nil, other not nil",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: nil,
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: strPtr("vol-1"),
					AccountName:  strPtr("account-1"),
					ServiceLevel: strPtr("premium"),
				},
			},
			expected: true,
		},
		{
			name: "All metadata fields are nil",
			metric1: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: nil,
					AccountName:  nil,
					ServiceLevel: nil,
				},
			},
			metric2: entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceName: nil,
					AccountName:  nil,
					ServiceLevel: nil,
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasMetadataChanged(tt.metric1, tt.metric2)
			if result != tt.expected {
				t.Errorf("hasMetadataChanged() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBothNilOrEqual(t *testing.T) {
	t.Run("String type - both nil", func(t *testing.T) {
		var val1, val2 *string
		if !bothNilOrEqual(val1, val2) {
			t.Error("Expected true when both string pointers are nil")
		}
	})

	t.Run("String type - both non-nil and equal", func(t *testing.T) {
		val1 := strPtr("test")
		val2 := strPtr("test")
		if !bothNilOrEqual(val1, val2) {
			t.Error("Expected true when both string pointers have equal values")
		}
	})

	t.Run("String type - both non-nil and not equal", func(t *testing.T) {
		val1 := strPtr("test1")
		val2 := strPtr("test2")
		if bothNilOrEqual(val1, val2) {
			t.Error("Expected false when string pointers have different values")
		}
	})

	t.Run("String type - first nil, second not nil", func(t *testing.T) {
		var val1 *string
		val2 := strPtr("test")
		if bothNilOrEqual(val1, val2) {
			t.Error("Expected false when first string pointer is nil and second is not")
		}
	})

	t.Run("String type - first not nil, second nil", func(t *testing.T) {
		val1 := strPtr("test")
		var val2 *string
		if bothNilOrEqual(val1, val2) {
			t.Error("Expected false when first string pointer is not nil and second is nil")
		}
	})

	t.Run("Int type - both nil", func(t *testing.T) {
		var val1, val2 *int
		if !bothNilOrEqual(val1, val2) {
			t.Error("Expected true when both int pointers are nil")
		}
	})

	t.Run("Int type - both non-nil and equal", func(t *testing.T) {
		val1 := intPtr(42)
		val2 := intPtr(42)
		if !bothNilOrEqual(val1, val2) {
			t.Error("Expected true when both int pointers have equal values")
		}
	})

	t.Run("Int type - both non-nil and not equal", func(t *testing.T) {
		val1 := intPtr(42)
		val2 := intPtr(99)
		if bothNilOrEqual(val1, val2) {
			t.Error("Expected false when int pointers have different values")
		}
	})

	t.Run("Int type - first nil, second not nil", func(t *testing.T) {
		var val1 *int
		val2 := intPtr(42)
		if bothNilOrEqual(val1, val2) {
			t.Error("Expected false when first int pointer is nil and second is not")
		}
	})

	t.Run("Int type - first not nil, second nil", func(t *testing.T) {
		val1 := intPtr(42)
		var val2 *int
		if bothNilOrEqual(val1, val2) {
			t.Error("Expected false when first int pointer is not nil and second is nil")
		}
	})
}

// TestFormatterBasicFunctions tests the basic formatter functions for coverage
func TestFormatterBasicFunctions(t *testing.T) {
	// Test hasMetadataChanged function
	t.Run("hasMetadataChanged", func(t *testing.T) {
		// Create test metrics with different metadata
		metric1 := entity.HydratedMetric{}
		resourceName1 := "resource1"
		accountName1 := "account1"
		serviceLevel1 := "standard"

		metric1.Metadata.ResourceName = &resourceName1
		metric1.Metadata.AccountName = &accountName1
		metric1.Metadata.ServiceLevel = &serviceLevel1

		metric2 := entity.HydratedMetric{}
		resourceName2 := "resource2"
		accountName2 := "account2"
		serviceLevel2 := "premium"

		metric2.Metadata.ResourceName = &resourceName2
		metric2.Metadata.AccountName = &accountName2
		metric2.Metadata.ServiceLevel = &serviceLevel2

		// Test different metadata
		changed := hasMetadataChanged(metric1, metric2)
		assert.True(t, changed, "Should detect different metadata")

		// Test same metadata
		unchanged := hasMetadataChanged(metric1, metric1)
		assert.False(t, unchanged, "Should detect same metadata")
	})

	// Test bothNilOrEqual function
	t.Run("bothNilOrEqual", func(t *testing.T) {
		// Test both nil
		var nilStr1, nilStr2 *string = nil, nil
		result := bothNilOrEqual(nilStr1, nilStr2)
		assert.True(t, result, "Both nil should be equal")

		// Test first nil, second not nil
		str2 := "test"
		result = bothNilOrEqual(nilStr1, &str2)
		assert.False(t, result, "Nil and non-nil should not be equal")

		// Test first not nil, second nil
		str1 := "test"
		result = bothNilOrEqual(&str1, nilStr2)
		assert.False(t, result, "Non-nil and nil should not be equal")

		// Test both same value
		str3 := "same"
		str4 := "same"
		result = bothNilOrEqual(&str3, &str4)
		assert.True(t, result, "Same values should be equal")

		// Test different values
		str5 := "different1"
		str6 := "different2"
		result = bothNilOrEqual(&str5, &str6)
		assert.False(t, result, "Different values should not be equal")
	})
}
