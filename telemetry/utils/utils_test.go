package utils

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"testing"
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
