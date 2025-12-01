package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func Test_ParseBizOpsReportParams(t *testing.T) {
	tests := []struct {
		name    string
		params  BizOpsReportParams
		wantErr bool
	}{
		{
			name:    "valid UTC",
			params:  BizOpsReportParams{TimeZone: UTC},
			wantErr: false,
		},
		{
			name:    "valid PST",
			params:  BizOpsReportParams{TimeZone: PST},
			wantErr: false,
		},
		{
			name:    "invalid timezone",
			params:  BizOpsReportParams{TimeZone: "IST"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ParseBizOpsReportParams(&tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBizOpsReportParams() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateResourceMetadata(t *testing.T) {
	tests := []struct {
		name          string
		setupMetadata func() metadata.ResourceMetadata
		wantErr       bool
		expectedError string
	}{
		{
			name: "Valid metadata with all required fields",
			setupMetadata: func() metadata.ResourceMetadata {
				rm := metadata.ResourceMetadata{}
				resourceName := "test-resource"
				regionName := "us-west-1"
				deploymentName := "test-deployment"
				rm.SetResourceName(resourceName)
				rm.SetRegionName(regionName)
				rm.SetDeploymentName(deploymentName)
				return rm
			},
			wantErr: false,
		},
		{
			name: "Nil ResourceName",
			setupMetadata: func() metadata.ResourceMetadata {
				rm := metadata.ResourceMetadata{}
				regionName := "us-west-1"
				deploymentName := "test-deployment"
				rm.SetRegionName(regionName)
				rm.SetDeploymentName(deploymentName)
				// ResourceName not set (nil)
				return rm
			},
			wantErr:       true,
			expectedError: "ResourceName is nil",
		},
		{
			name: "Empty ResourceName",
			setupMetadata: func() metadata.ResourceMetadata {
				rm := metadata.ResourceMetadata{}
				regionName := "us-west-1"
				deploymentName := "test-deployment"
				rm.SetRegionName(regionName)
				rm.SetDeploymentName(deploymentName)
				return rm
			},
			wantErr:       true,
			expectedError: "ResourceName is nil",
		},
		{
			name: "Nil RegionName",
			setupMetadata: func() metadata.ResourceMetadata {
				rm := metadata.ResourceMetadata{}
				resourceName := "test-resource"
				deploymentName := "test-deployment"
				rm.SetResourceName(resourceName)
				rm.SetDeploymentName(deploymentName)
				// RegionName not set (nil)
				return rm
			},
			wantErr:       true,
			expectedError: "RegionName is nil",
		},
		{
			name: "Empty RegionName",
			setupMetadata: func() metadata.ResourceMetadata {
				rm := metadata.ResourceMetadata{}
				resourceName := "test-resource"
				deploymentName := "test-deployment"
				rm.SetResourceName(resourceName)
				rm.SetDeploymentName(deploymentName)
				return rm
			},
			wantErr:       true,
			expectedError: "RegionName is nil",
		},
		{
			name: "Nil DeploymentName",
			setupMetadata: func() metadata.ResourceMetadata {
				rm := metadata.ResourceMetadata{}
				resourceName := "test-resource"
				regionName := "us-west-1"
				rm.SetResourceName(resourceName)
				rm.SetRegionName(regionName)
				// DeploymentName not set (nil)
				return rm
			},
			wantErr:       true,
			expectedError: "DeploymentName is nil",
		},
		{
			name: "Empty DeploymentName",
			setupMetadata: func() metadata.ResourceMetadata {
				rm := metadata.ResourceMetadata{}
				resourceName := "test-resource"
				regionName := "us-west-1"
				rm.SetResourceName(resourceName)
				rm.SetRegionName(regionName)
				return rm
			},
			wantErr:       true,
			expectedError: "DeploymentName is nil",
		},
		{
			name: "Multiple nil fields",
			setupMetadata: func() metadata.ResourceMetadata {
				rm := metadata.ResourceMetadata{}
				// All fields not set (nil)
				return rm
			},
			wantErr:       true,
			expectedError: "ResourceName is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resourceMetadata := tt.setupMetadata()
			err := ValidateResourceMetadata(resourceMetadata)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateResourceMetadata() expected error but got none")
				} else if tt.expectedError != "" && err.Error() != tt.expectedError {
					t.Errorf("ValidateResourceMetadata() error = %v, want %v", err.Error(), tt.expectedError)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateResourceMetadata() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestPrepareAggregationTime(t *testing.T) {
	tests := []struct {
		name         string
		input        time.Time
		targetMinute int
		expected     time.Time
	}{
		{
			name:         "15-minute aggregation: 3:30 rounds to 3:15",
			input:        time.Date(2025, 11, 18, 3, 30, 30, 123456789, time.UTC),
			targetMinute: 15,
			expected:     time.Date(2025, 11, 18, 3, 15, 0, 0, time.UTC),
		},
		{
			name:         "15-minute aggregation: 3:45 rounds to 3:15",
			input:        time.Date(2025, 11, 18, 3, 45, 0, 0, time.UTC),
			targetMinute: 15,
			expected:     time.Date(2025, 11, 18, 3, 15, 0, 0, time.UTC),
		},
		{
			name:         "15-minute aggregation: 4:10 rounds to 3:15",
			input:        time.Date(2025, 11, 18, 4, 10, 0, 0, time.UTC),
			targetMinute: 15,
			expected:     time.Date(2025, 11, 18, 3, 15, 0, 0, time.UTC),
		},
		{
			name:         "15-minute aggregation: 2:15 stays at 2:15",
			input:        time.Date(2025, 11, 18, 2, 15, 0, 0, time.UTC),
			targetMinute: 15,
			expected:     time.Date(2025, 11, 18, 2, 15, 0, 0, time.UTC),
		},
		{
			name:         "15-minute aggregation: 00:10 rounds to previous day 23:15",
			input:        time.Date(2025, 11, 18, 0, 10, 0, 0, time.UTC),
			targetMinute: 15,
			expected:     time.Date(2025, 11, 17, 23, 15, 0, 0, time.UTC),
		},
		{
			name:         "0-minute aggregation (top of hour): 3:30 rounds to 3:00",
			input:        time.Date(2025, 11, 18, 3, 30, 30, 123456789, time.UTC),
			targetMinute: 0,
			expected:     time.Date(2025, 11, 18, 3, 0, 0, 0, time.UTC),
		},
		{
			name:         "0-minute aggregation: 00:10 rounds to 00:00",
			input:        time.Date(2025, 11, 18, 0, 10, 0, 0, time.UTC),
			targetMinute: 0,
			expected:     time.Date(2025, 11, 18, 0, 0, 0, 0, time.UTC),
		},
		{
			name:         "30-minute aggregation: 3:45 rounds to 3:30",
			input:        time.Date(2025, 11, 18, 3, 45, 0, 0, time.UTC),
			targetMinute: 30,
			expected:     time.Date(2025, 11, 18, 3, 30, 0, 0, time.UTC),
		},
		{
			name:         "30-minute aggregation: 3:15 rounds to 2:30",
			input:        time.Date(2025, 11, 18, 3, 15, 0, 0, time.UTC),
			targetMinute: 30,
			expected:     time.Date(2025, 11, 18, 2, 30, 0, 0, time.UTC),
		},
		{
			name:         "30-minute aggregation: 00:15 rounds to previous day 23:30",
			input:        time.Date(2025, 11, 18, 0, 15, 0, 0, time.UTC),
			targetMinute: 30,
			expected:     time.Date(2025, 11, 17, 23, 30, 0, 0, time.UTC),
		},
		{
			name:         "45-minute aggregation: 15:50 rounds to 15:45",
			input:        time.Date(2025, 11, 18, 15, 50, 45, 999999999, time.UTC),
			targetMinute: 45,
			expected:     time.Date(2025, 11, 18, 15, 45, 0, 0, time.UTC),
		},
		{
			name:         "45-minute aggregation: 00:44 rounds to previous day 23:45",
			input:        time.Date(2025, 11, 18, 0, 44, 45, 999999999, time.UTC),
			targetMinute: 45,
			expected:     time.Date(2025, 11, 17, 23, 45, 0, 0, time.UTC),
		},
		{
			name:         "timezone preserved - PST with 15-minute target",
			input:        time.Date(2025, 11, 18, 8, 30, 0, 0, time.FixedZone("PST", -8*3600)),
			targetMinute: 15,
			expected:     time.Date(2025, 11, 18, 8, 15, 0, 0, time.FixedZone("PST", -8*3600)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrepareAggregationTime(tt.input, tt.targetMinute)
			assert.Equal(t, tt.expected, result, "Expected %v but got %v", tt.expected, result)
		})
	}
}
