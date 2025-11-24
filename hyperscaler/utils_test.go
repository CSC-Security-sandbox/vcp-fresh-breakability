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
				DeploymentName: "test-deployment",
				OntapCredentials: &datamodel.PoolCredentials{
					Password:      "password123",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      env.USER_CERTIFICATE,
				},
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
				DeploymentName: "test-deployment",
				OntapCredentials: &datamodel.PoolCredentials{
					Password:      "password123",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      env.USER_CERTIFICATE,
				},
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
				DeploymentName: "test-deployment",
				OntapCredentials: &datamodel.PoolCredentials{
					Password:      "password123",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      env.USER_CERTIFICATE,
				},
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
				DeploymentName: "test-deployment",
				OntapCredentials: &datamodel.PoolCredentials{
					Password:      "password123",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1, // Not USER_CERTIFICATE
				},
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
				DeploymentName: "test-deployment",
				OntapCredentials: &datamodel.PoolCredentials{
					Password:      "password123",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1, // Not USER_CERTIFICATE
				},
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
				DeploymentName: "test-deployment",
				OntapCredentials: &datamodel.PoolCredentials{
					Password:      "password123",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1, // Not USER_CERTIFICATE
				},
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
				DeploymentName: "test-deployment",
				OntapCredentials: &datamodel.PoolCredentials{
					Password:      "password123",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      0,
				},
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
			// Compare basic fields
			assert.Equal(t, tt.expectedResult.EndpointAddressesToHostNameMap, result.EndpointAddressesToHostNameMap)
			assert.Equal(t, tt.expectedResult.DeploymentName, result.DeploymentName)
			assert.Equal(t, tt.expectedResult.CertificateID, result.CertificateID)
			assert.Equal(t, tt.expectedResult.SecretID, result.SecretID)
			assert.Equal(t, tt.expectedResult.Password, result.Password)
			assert.Equal(t, tt.expectedResult.AuthType, result.AuthType)
			// CaURI should be populated (either from OntapCredentials or env vars)
			// If OntapCredentials has CaURI, it should be used; otherwise env vars are used
			if tt.input.OntapCredentials != nil {
				if tt.input.OntapCredentials.CaURI != "" {
					assert.Equal(t, tt.input.OntapCredentials.CaURI, result.CaURI)
				}
				// If CaURI is not provided in OntapCredentials, it will fall back to env vars
				// We don't check those here as env vars may vary by environment
			}
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
		DeploymentName: "test-deployment",
		OntapCredentials: &datamodel.PoolCredentials{
			Password:      "password123",
			SecretID:      "secret-id",
			CertificateID: "cert-id",
			AuthType:      env.USER_CERTIFICATE,
		},
	}

	result := CreateNodeForProvider(input)

	// Verify basic fields
	assert.Equal(t, "test-deployment", result.DeploymentName)
	assert.Equal(t, "cert-id", result.CertificateID)
	assert.Equal(t, "secret-id", result.SecretID)
	assert.Equal(t, env.USER_CERTIFICATE, result.AuthType)
	assert.Equal(t, map[string]string{
		"10.0.0.1": "host1.example.com",
	}, result.EndpointAddressesToHostNameMap)
	// CA fields should be populated (from env vars since not provided in input)
	// Note: CA fields will be set from env vars, but we don't check exact values
	// as they may vary by environment. The new dedicated tests verify CA field behavior.
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
		DeploymentName: "test-deployment",
		OntapCredentials: &datamodel.PoolCredentials{
			Password:      "password123",
			SecretID:      "secret-id",
			CertificateID: "cert-id",
			AuthType:      env.USER_CERTIFICATE,
		},
	}

	assert.NotNil(t, input.Nodes)
	assert.NotNil(t, input.OntapCredentials)
	assert.Equal(t, "password123", input.OntapCredentials.Password)
	assert.Equal(t, "secret-id", input.OntapCredentials.SecretID)
	assert.Equal(t, "cert-id", input.OntapCredentials.CertificateID)
	assert.Equal(t, "test-deployment", input.DeploymentName)
	assert.Equal(t, env.USER_CERTIFICATE, input.OntapCredentials.AuthType)
}

func TestCreateNodeForProvider_CAFieldsFromOntapCredentials(t *testing.T) {
	// Test that CA fields are populated from OntapCredentials when provided
	input := NodeProviderInput{
		Nodes: []*datamodel.Node{
			{
				EndpointAddress: "10.0.0.1",
				HostDNSName:     "host1.example.com",
			},
		},
		DeploymentName: "test-deployment",
		OntapCredentials: &datamodel.PoolCredentials{
			CertificateID: "cert-id",
			SecretID:      "secret-id",
			AuthType:      env.USER_CERTIFICATE,
			CaURI:         "test-ca-pool-deployed-project-id/test-ca-pool-name/test-ca-name",
		},
	}

	result := CreateNodeForProvider(input)

	assert.NotNil(t, result)
	assert.Equal(t, "test-ca-pool-deployed-project-id/test-ca-pool-name/test-ca-name", result.CaURI)
}

func TestCreateNodeForProvider_CAFieldsFallbackToEnvVars(t *testing.T) {
	// Save original env values
	originalCaName := env.CaName
	originalCaPoolName := env.CaPoolName
	originalCaPoolDeployedProjectID := env.CaPoolDeployedProjectID

	// Set test env values
	env.CaName = "env-ca-name"
	env.CaPoolName = "env-ca-pool-name"
	env.CaPoolDeployedProjectID = "env-ca-pool-deployed-project-id"

	defer func() {
		// Restore original env values
		env.CaName = originalCaName
		env.CaPoolName = originalCaPoolName
		env.CaPoolDeployedProjectID = originalCaPoolDeployedProjectID
	}()

	// Test that CA fields fall back to environment variables when not provided in OntapCredentials
	input := NodeProviderInput{
		Nodes: []*datamodel.Node{
			{
				EndpointAddress: "10.0.0.1",
				HostDNSName:     "host1.example.com",
			},
		},
		DeploymentName: "test-deployment",
		OntapCredentials: &datamodel.PoolCredentials{
			CertificateID: "cert-id",
			SecretID:      "secret-id",
			AuthType:      env.USER_CERTIFICATE,
			// CA fields are empty, should fall back to env vars
		},
	}

	result := CreateNodeForProvider(input)

	assert.NotNil(t, result)
	// When CaURI is empty, it should fall back to env vars and build CaURI from them
	expectedCaURI := "env-ca-pool-deployed-project-id/env-ca-pool-name/env-ca-name"
	assert.Equal(t, expectedCaURI, result.CaURI)
}

func TestCreateNodeForProvider_CAFieldsPartialFallback(t *testing.T) {
	// Save original env values
	originalCaName := env.CaName
	originalCaPoolName := env.CaPoolName
	originalCaPoolDeployedProjectID := env.CaPoolDeployedProjectID

	// Set test env values
	env.CaName = "env-ca-name"
	env.CaPoolName = "env-ca-pool-name"
	env.CaPoolDeployedProjectID = "env-ca-pool-deployed-project-id"

	defer func() {
		// Restore original env values
		env.CaName = originalCaName
		env.CaPoolName = originalCaPoolName
		env.CaPoolDeployedProjectID = originalCaPoolDeployedProjectID
	}()

	// Test that CA fields use provided values when available, and fall back to env vars for missing ones
	input := NodeProviderInput{
		Nodes: []*datamodel.Node{
			{
				EndpointAddress: "10.0.0.1",
				HostDNSName:     "host1.example.com",
			},
		},
		DeploymentName: "test-deployment",
		OntapCredentials: &datamodel.PoolCredentials{
			CertificateID: "cert-id",
			SecretID:      "secret-id",
			AuthType:      env.USER_CERTIFICATE,
			// CaURI is empty, should fall back to env vars and build CaURI from them
		},
	}

	result := CreateNodeForProvider(input)

	assert.NotNil(t, result)
	// When CaURI is empty, it should fall back to env vars and build CaURI from them
	expectedCaURI := "env-ca-pool-deployed-project-id/env-ca-pool-name/env-ca-name"
	assert.Equal(t, expectedCaURI, result.CaURI)
}

func TestCreateNodeForProvider_NilOntapCredentials(t *testing.T) {
	// Test that function handles nil OntapCredentials gracefully
	input := NodeProviderInput{
		Nodes: []*datamodel.Node{
			{
				EndpointAddress: "10.0.0.1",
				HostDNSName:     "host1.example.com",
			},
		},
		DeploymentName:   "test-deployment",
		OntapCredentials: nil,
	}

	result := CreateNodeForProvider(input)

	assert.NotNil(t, result)
	assert.Equal(t, "test-deployment", result.DeploymentName)
	// Other fields should be empty/zero values
	assert.Equal(t, "", result.CertificateID)
	assert.Equal(t, "", result.Password)
	assert.Equal(t, 0, result.AuthType)
}

func TestCreateNodeForProvider_CAFieldsForNonCertificateAuth(t *testing.T) {
	// Save original env values
	originalCaName := env.CaName
	originalCaPoolName := env.CaPoolName
	originalCaPoolDeployedProjectID := env.CaPoolDeployedProjectID

	// Set test env values
	env.CaName = "env-ca-name"
	env.CaPoolName = "env-ca-pool-name"
	env.CaPoolDeployedProjectID = "env-ca-pool-deployed-project-id"

	defer func() {
		// Restore original env values
		env.CaName = originalCaName
		env.CaPoolName = originalCaPoolName
		env.CaPoolDeployedProjectID = originalCaPoolDeployedProjectID
	}()

	// Test that CA fields are populated even for non-certificate auth types
	input := NodeProviderInput{
		Nodes: []*datamodel.Node{
			{
				EndpointAddress: "10.0.0.1",
				HostDNSName:     "host1.example.com",
			},
		},
		DeploymentName: "test-deployment",
		OntapCredentials: &datamodel.PoolCredentials{
			Password:  "password123",
			SecretID:   "secret-id",
			AuthType:   env.USERNAME_PWD,
			CaURI:      "test-ca-pool-deployed-project-id/test-ca-pool-name/test-ca-name",
		},
	}

	result := CreateNodeForProvider(input)

	assert.NotNil(t, result)
	assert.Equal(t, "test-ca-pool-deployed-project-id/test-ca-pool-name/test-ca-name", result.CaURI)
	assert.Equal(t, "password123", result.Password)
	assert.Equal(t, env.USERNAME_PWD, result.AuthType)
}
