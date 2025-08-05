package hyperscaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

func TestPrepareOperationID(t *testing.T) {
	tests := []struct {
		name           string
		projectNumber  string
		locationId     string
		jobId          string
		expectedResult string
	}{
		{
			name:           "Valid parameters",
			projectNumber:  "12345",
			locationId:     "us-central1",
			jobId:          "job-123",
			expectedResult: "/v1beta/projects/12345/locations/us-central1/operations/job-123",
		},
		{
			name:           "Empty project number",
			projectNumber:  "",
			locationId:     "us-central1",
			jobId:          "job-123",
			expectedResult: "",
		},
		{
			name:           "Empty location ID",
			projectNumber:  "12345",
			locationId:     "",
			jobId:          "job-123",
			expectedResult: "",
		},
		{
			name:           "Empty job ID",
			projectNumber:  "12345",
			locationId:     "us-central1",
			jobId:          "",
			expectedResult: "",
		},
		{
			name:           "All parameters empty",
			projectNumber:  "",
			locationId:     "",
			jobId:          "",
			expectedResult: "",
		},
		{
			name:           "Multiple empty parameters",
			projectNumber:  "",
			locationId:     "",
			jobId:          "job-123",
			expectedResult: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrepareOperationID(tt.projectNumber, tt.locationId, tt.jobId)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestCreateNodeForProvider(t *testing.T) {
	tests := []struct {
		name           string
		input          NodeProviderInput
		expectedResult *models.Node
	}{
		{
			name: "USER_CERTIFICATE auth type with nodes",
			input: NodeProviderInput{
				Nodes: []*datamodel.Node{
					{
						EndpointAddress: "10.0.0.1",
						HostDNSName:     "host1.example.com",
					},
					{
						EndpointAddress: "10.0.0.2",
						HostDNSName:     "host2.example.com",
					},
				},
				Password:       "password123",
				SecretID:       "secret-id",
				CertificateID:  "cert-id",
				DeploymentName: "test-deployment",
				AuthType:       env.USER_CERTIFICATE,
			},
			expectedResult: &models.Node{
				EndpointAddressesToHostNameMap: map[string]string{
					"10.0.0.1": "host1.example.com",
					"10.0.0.2": "host2.example.com",
				},
				DeploymentName: "test-deployment",
				CertificateID:  "cert-id",
				SecretID:       "secret-id",
				AuthType:       env.USER_CERTIFICATE,
			},
		},
		{
			name: "USER_CERTIFICATE auth type with empty endpoint address",
			input: NodeProviderInput{
				Nodes: []*datamodel.Node{
					{
						EndpointAddress: "",
						HostDNSName:     "host1.example.com",
					},
					{
						EndpointAddress: "10.0.0.2",
						HostDNSName:     "host2.example.com",
					},
				},
				Password:       "password123",
				SecretID:       "secret-id",
				CertificateID:  "cert-id",
				DeploymentName: "test-deployment",
				AuthType:       env.USER_CERTIFICATE,
			},
			expectedResult: &models.Node{
				EndpointAddressesToHostNameMap: map[string]string{
					"10.0.0.2": "host2.example.com",
				},
				DeploymentName: "test-deployment",
				CertificateID:  "cert-id",
				SecretID:       "secret-id",
				AuthType:       env.USER_CERTIFICATE,
			},
		},
		{
			name: "USER_CERTIFICATE auth type with no nodes",
			input: NodeProviderInput{
				Nodes:          []*datamodel.Node{},
				Password:       "password123",
				SecretID:       "secret-id",
				CertificateID:  "cert-id",
				DeploymentName: "test-deployment",
				AuthType:       env.USER_CERTIFICATE,
			},
			expectedResult: &models.Node{
				EndpointAddressesToHostNameMap: map[string]string{},
				DeploymentName:                 "test-deployment",
				CertificateID:                  "cert-id",
				SecretID:                       "secret-id",
				AuthType:                       env.USER_CERTIFICATE,
			},
		},
		{
			name: "Non-USER_CERTIFICATE auth type with nodes",
			input: NodeProviderInput{
				Nodes: []*datamodel.Node{
					{
						EndpointAddress: "10.0.0.1",
						HostDNSName:     "host1.example.com",
					},
					{
						EndpointAddress: "10.0.0.2",
						HostDNSName:     "host2.example.com",
					},
				},
				Password:       "password123",
				SecretID:       "secret-id",
				CertificateID:  "cert-id",
				DeploymentName: "test-deployment",
				AuthType:       1, // Not USER_CERTIFICATE
			},
			expectedResult: &models.Node{
				EndpointAddressesToHostNameMap: map[string]string{
					"10.0.0.1": "10.0.0.1",
					"10.0.0.2": "10.0.0.2",
				},
				Password:       "password123",
				DeploymentName: "test-deployment",
				SecretID:       "secret-id",
				AuthType:       1,
			},
		},
		{
			name: "Non-USER_CERTIFICATE auth type with empty endpoint address",
			input: NodeProviderInput{
				Nodes: []*datamodel.Node{
					{
						EndpointAddress: "",
						HostDNSName:     "host1.example.com",
					},
					{
						EndpointAddress: "10.0.0.2",
						HostDNSName:     "host2.example.com",
					},
				},
				Password:       "password123",
				SecretID:       "secret-id",
				CertificateID:  "cert-id",
				DeploymentName: "test-deployment",
				AuthType:       1, // Not USER_CERTIFICATE
			},
			expectedResult: &models.Node{
				EndpointAddressesToHostNameMap: map[string]string{
					"10.0.0.2": "10.0.0.2",
				},
				Password:       "password123",
				DeploymentName: "test-deployment",
				SecretID:       "secret-id",
				AuthType:       1,
			},
		},
		{
			name: "Non-USER_CERTIFICATE auth type with no nodes",
			input: NodeProviderInput{
				Nodes:          []*datamodel.Node{},
				Password:       "password123",
				SecretID:       "secret-id",
				CertificateID:  "cert-id",
				DeploymentName: "test-deployment",
				AuthType:       1, // Not USER_CERTIFICATE
			},
			expectedResult: &models.Node{
				EndpointAddressesToHostNameMap: map[string]string{},
				Password:                       "password123",
				DeploymentName:                 "test-deployment",
				SecretID:                       "secret-id",
				AuthType:                       1,
			},
		},
		{
			name: "Zero auth type (non-USER_CERTIFICATE)",
			input: NodeProviderInput{
				Nodes: []*datamodel.Node{
					{
						EndpointAddress: "10.0.0.1",
						HostDNSName:     "host1.example.com",
					},
				},
				Password:       "password123",
				SecretID:       "secret-id",
				CertificateID:  "cert-id",
				DeploymentName: "test-deployment",
				AuthType:       0,
			},
			expectedResult: &models.Node{
				EndpointAddressesToHostNameMap: map[string]string{
					"10.0.0.1": "10.0.0.1",
				},
				Password:       "password123",
				DeploymentName: "test-deployment",
				SecretID:       "secret-id",
				AuthType:       0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateNodeForProvider(tt.input)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestCreateJunctionPath(t *testing.T) {
	tests := []struct {
		name           string
		token          string
		expectedResult string
	}{
		{
			name:           "Normal token",
			token:          "volume123",
			expectedResult: "/volume123",
		},
		{
			name:           "Empty token",
			token:          "",
			expectedResult: "/",
		},
		{
			name:           "Token with special characters",
			token:          "vol_123-test",
			expectedResult: "/vol_123-test",
		},
		{
			name:           "Token with spaces",
			token:          "volume test",
			expectedResult: "/volume test",
		},
		{
			name:           "Token starting with slash",
			token:          "/volume123",
			expectedResult: "//volume123",
		},
		{
			name:           "Numeric token",
			token:          "123456",
			expectedResult: "/123456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := common.CreateJunctionPath(tt.token)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestCreateNodeForProviderVariable(t *testing.T) {
	// Test the exported variable points to the correct function
	input := NodeProviderInput{
		Nodes: []*datamodel.Node{
			{
				EndpointAddress: "10.0.0.1",
				HostDNSName:     "host1.example.com",
			},
		},
		Password:       "password123",
		SecretID:       "secret-id",
		CertificateID:  "cert-id",
		DeploymentName: "test-deployment",
		AuthType:       env.USER_CERTIFICATE,
	}

	result := CreateNodeForProvider(input)

	expectedResult := &models.Node{
		EndpointAddressesToHostNameMap: map[string]string{
			"10.0.0.1": "host1.example.com",
		},
		DeploymentName: "test-deployment",
		CertificateID:  "cert-id",
		SecretID:       "secret-id",
		AuthType:       env.USER_CERTIFICATE,
	}

	assert.Equal(t, expectedResult, result)
}

func TestNodeProviderInput_Struct(t *testing.T) {
	// Test that NodeProviderInput struct can be created with all fields
	input := NodeProviderInput{
		Nodes: []*datamodel.Node{
			{
				EndpointAddress: "10.0.0.1",
				HostDNSName:     "host1.example.com",
			},
		},
		Password:       "password123",
		SecretID:       "secret-id",
		CertificateID:  "cert-id",
		DeploymentName: "test-deployment",
		AuthType:       env.USER_CERTIFICATE,
	}

	assert.NotNil(t, input.Nodes)
	assert.Equal(t, "password123", input.Password)
	assert.Equal(t, "secret-id", input.SecretID)
	assert.Equal(t, "cert-id", input.CertificateID)
	assert.Equal(t, "test-deployment", input.DeploymentName)
	assert.Equal(t, env.USER_CERTIFICATE, input.AuthType)
}
