package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func TestNewGoogleMetric(t *testing.T) {
	record := "test record"
	gm := NewGoogleMetric(record)

	assert.NotNil(t, gm)
	assert.Equal(t, record, gm.Record)
}

func TestNewInvalidGoogleMetricException(t *testing.T) {
	msg := "test error message"
	err := NewInvalidGoogleMetricException(msg)

	assert.NotNil(t, err)
	assert.Equal(t, msg, err.Error())
}

func TestGoogleMetric_GetType(t *testing.T) {
	tests := []struct {
		name     string
		record   interface{}
		expected GoogleType
	}{
		{
			name:     "BillingMetric type",
			record:   &datamodel.AggregatedUsage{},
			expected: BillingMetric,
		},
		{
			name:     "HydratedMetric type",
			record:   &entity.HydratedMetric{},
			expected: HydratedMetric,
		},
		{
			name:     "Unknown type",
			record:   "invalid record",
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result := gm.GetType()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetAsUsageBillingMetric(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
	}{
		{
			name:      "Valid AggregatedUsage",
			record:    &datamodel.AggregatedUsage{ID: 1},
			expectErr: false,
		},
		{
			name:      "Nil record",
			record:    nil,
			expectErr: true,
		},
		{
			name:      "Invalid type conversion",
			record:    &entity.HydratedMetric{},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetAsUsageBillingMetric()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, int64(1), result.ID)
			}
		})
	}
}

func TestGoogleMetric_GetAsHydratedMetric(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
	}{
		{
			name:      "Valid HydratedMetric",
			record:    &entity.HydratedMetric{CorrelationID: "test"},
			expectErr: false,
		},
		{
			name:      "Nil record",
			record:    nil,
			expectErr: true,
		},
		{
			name:      "Invalid type conversion",
			record:    &datamodel.AggregatedUsage{},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetAsHydratedMetric()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, "test", result.CorrelationID)
			}
		})
	}
}

func TestGoogleMetric_GetCustomerId(t *testing.T) {
	customerID := "test-customer-123"
	accountName := "test-account"

	tests := []struct {
		name        string
		record      interface{}
		expected    string
		expectErr   bool
		expectPanic bool
	}{
		{
			name: "BillingMetric with valid customer ID",
			record: &datamodel.AggregatedUsage{
				VendorCustomerID: &customerID,
			},
			expected:  customerID,
			expectErr: false,
		},
		{
			name: "HydratedMetric with valid account name",
			record: &entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					AccountName: &accountName,
				},
			},
			expected:  accountName,
			expectErr: false,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  "",
			expectErr: true,
		},
		{
			name: "BillingMetric with nil customer ID - should return error",
			record: &datamodel.AggregatedUsage{
				VendorCustomerID: nil,
			},
			expected:    "",
			expectErr:   true,
			expectPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)

			if tt.expectPanic {
				assert.Panics(t, func() {
					_, _ = gm.GetCustomerId()
				})
				return
			}

			result, err := gm.GetCustomerId()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Empty(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGoogleMetric_GetCustomerId_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "BillingMetric with GetAsUsageBillingMetric error",
			record:    "invalid record", // This will cause an error in GetAsUsageBillingMetric
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
		{
			name:      "HydratedMetric with GetAsHydratedMetric error",
			record:    "invalid record", // This will cause an error in GetAsHydratedMetric
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetCustomerId()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Empty(t, result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_GetResourceName(t *testing.T) {
	resourceDisplayName := "test-resource"

	tests := []struct {
		name      string
		record    interface{}
		expected  string
		expectErr bool
	}{
		{
			name:      "BillingMetric with nil ResourceName returns error",
			record:    &datamodel.AggregatedUsage{},
			expected:  "",
			expectErr: true,
		},
		{
			name: "HydratedMetric with valid resource display name",
			record: &entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceDisplayName: &resourceDisplayName,
				},
			},
			expected:  resourceDisplayName,
			expectErr: false,
		},
		{
			name: "HydratedMetric with nil resource display name",
			record: &entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceDisplayName: nil,
				},
			},
			expected:  "",
			expectErr: true,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetResourceName()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetResourceName_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "BillingMetric with GetAsUsageBillingMetric error",
			record:    "invalid record", // This will cause an error in GetAsUsageBillingMetric
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
		{
			name:      "HydratedMetric with GetAsHydratedMetric error",
			record:    "invalid record", // This will cause an error in GetAsHydratedMetric
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetResourceName()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Empty(t, result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_GetMeasuredType(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expected  metadata.MeasuredType
		expectErr bool
	}{
		{
			name: "BillingMetric with measured type",
			record: &datamodel.AggregatedUsage{
				MeasuredType: metadata.LogicalSize,
			},
			expected:  metadata.LogicalSize,
			expectErr: false,
		},
		{
			name: "HydratedMetric with measured type",
			record: &entity.HydratedMetric{
				MeasuredType: metadata.AllocatedSize,
			},
			expected:  metadata.AllocatedSize,
			expectErr: false,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetMeasuredType()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetMeasuredType_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Invalid record type",
			record:    "invalid record",
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetMeasuredType()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Empty(t, result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_GetQuantity(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expected  int64
		expectErr bool
	}{
		{
			name: "BillingMetric with quantity",
			record: &datamodel.AggregatedUsage{
				Quantity: 123.45,
			},
			expected:  123,
			expectErr: false,
		},
		{
			name: "HydratedMetric with quantity",
			record: &entity.HydratedMetric{
				Quantity: 456.78,
			},
			expected:  456,
			expectErr: false,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetQuantity()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetQuantity_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Invalid record type",
			record:    "invalid record",
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetQuantity()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Equal(t, int64(0), result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_GetDoubleQuantity(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expected  float64
		expectErr bool
	}{
		{
			name: "HydratedMetric with double quantity",
			record: &entity.HydratedMetric{
				Quantity: 456.78,
			},
			expected:  456.78,
			expectErr: false,
		},
		{
			name:      "BillingMetric should return error",
			record:    &datamodel.AggregatedUsage{},
			expected:  0,
			expectErr: true,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetDoubleQuantity()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetDoubleQuantity_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Invalid record type",
			record:    "invalid record",
			expectErr: true,
			errMsg:    "Only hydrated metrics have a double-valued quantity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetDoubleQuantity()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Equal(t, float64(0), result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_GetStringQuantity(t *testing.T) {
	// This method always returns an error according to the implementation
	gm := NewGoogleMetric(&datamodel.AggregatedUsage{})
	result, err := gm.GetStringQuantity()

	assert.Error(t, err)
	assert.Empty(t, result)
	assert.Contains(t, err.Error(), "Invalid GoogleMetric type")
}

func TestGoogleMetric_GetResourceType(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expected  metadata.ResourceType
		expectErr bool
	}{
		{
			name: "BillingMetric with resource type",
			record: &datamodel.AggregatedUsage{
				ResourceType: metadata.Volume,
			},
			expected:  metadata.Volume,
			expectErr: false,
		},
		{
			name: "HydratedMetric with resource type",
			record: &entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					ResourceType: metadata.VolumePool,
				},
			},
			expected:  metadata.VolumePool,
			expectErr: false,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetResourceType()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetResourceType_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Invalid record type",
			record:    "invalid record",
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetResourceType()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Empty(t, result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_GetTags(t *testing.T) {
	billingLabels := `{"tag1":"value1","tag2":"value2"}`

	tests := []struct {
		name      string
		record    interface{}
		expected  string
		expectErr bool
	}{
		{
			name: "BillingMetric with billing labels",
			record: &datamodel.AggregatedUsage{
				BillingLabels: &billingLabels,
			},
			expected:  billingLabels,
			expectErr: false,
		},
		{
			name: "BillingMetric with nil billing labels",
			record: &datamodel.AggregatedUsage{
				BillingLabels: nil,
			},
			expected:  "",
			expectErr: false,
		},
		{
			name:      "HydratedMetric returns empty string",
			record:    &entity.HydratedMetric{},
			expected:  "",
			expectErr: false,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetTags()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "Only billing metrics have tag information")
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetTags_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Invalid record type",
			record:    "invalid record",
			expectErr: true,
			errMsg:    "Only billing metrics have tag information",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetTags()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Equal(t, "", result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_GetStartTime(t *testing.T) {
	startTime := time.Now()
	unixNano := entity.UnixNano(startTime.UnixNano())

	tests := []struct {
		name      string
		record    interface{}
		expected  int64
		expectErr bool
	}{
		{
			name: "BillingMetric with start time",
			record: &datamodel.AggregatedUsage{
				AggregationStart: startTime,
			},
			expected:  startTime.Unix(),
			expectErr: false,
		},
		{
			name: "HydratedMetric with timestamp",
			record: &entity.HydratedMetric{
				Timestamp: unixNano,
			},
			expected:  startTime.Unix(),
			expectErr: false,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetStartTime()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetStartTime_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Invalid record type",
			record:    "invalid record",
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetStartTime()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Equal(t, int64(0), result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_GetEndTime(t *testing.T) {
	endTime := time.Now()
	unixNano := entity.UnixNano(endTime.UnixNano())

	tests := []struct {
		name      string
		record    interface{}
		expected  int64
		expectErr bool
	}{
		{
			name: "BillingMetric with end time",
			record: &datamodel.AggregatedUsage{
				AggregationEnd: endTime,
			},
			expected:  endTime.Unix(),
			expectErr: false,
		},
		{
			name: "HydratedMetric with timestamp",
			record: &entity.HydratedMetric{
				Timestamp: unixNano,
			},
			expected:  endTime.Unix(),
			expectErr: false,
		},
		{
			name:      "Invalid type",
			record:    "invalid",
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetEndTime()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGoogleMetric_GetEndTime_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		record    interface{}
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Invalid record type",
			record:    "invalid record",
			expectErr: true,
			errMsg:    "Invalid GoogleMetric type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result, err := gm.GetEndTime()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Equal(t, int64(0), result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGoogleMetric_Validate(t *testing.T) {
	customerID := "test-customer-123"
	accountName := "test-account"

	tests := []struct {
		name            string
		record          interface{}
		expectedMissing []string
	}{
		{
			name: "Valid BillingMetric",
			record: &datamodel.AggregatedUsage{
				VendorCustomerID: &customerID,
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
			},
			expectedMissing: []string{},
		},
		{
			name: "Valid HydratedMetric",
			record: &entity.HydratedMetric{
				Metadata: metadata.ResourceMetadata{
					AccountName: &accountName,
				},
				MeasuredType: metadata.LogicalSize,
				Quantity:     100.0,
			},
			expectedMissing: []string{},
		},
		{
			name: "BillingMetric missing measured type",
			record: &datamodel.AggregatedUsage{
				VendorCustomerID: &customerID,
				MeasuredType:     "",
				Quantity:         100.0,
			},
			expectedMissing: []string{"measuredType"},
		},
		{
			name:            "Invalid type",
			record:          "invalid",
			expectedMissing: []string{"customerId", "measuredType", "quantity", "type"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gm := NewGoogleMetric(tt.record)
			result := gm.Validate()

			assert.Equal(t, len(tt.expectedMissing), len(result))
			for _, missing := range tt.expectedMissing {
				assert.Contains(t, result, missing)
			}
		})
	}
}
