// Ensure correct package declaration
package vsa

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateSecurityLogForwarding_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateSecurityLogForwardingParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}
	response := []*ontapRest.SecurityAuditLogForward{{}}
	response[0].Address = nillable.GetStringPtr("test-address")
	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityLogForwardingCreate", &ontapRest.SecurityLogForwardingCreateParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}).Return(response, nil)

	resp, err := ontapProvider.CreateSecurityLogForwarding(params)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test-address", resp.Name)
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestCreateSecurityLogForwarding_ResponseError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateSecurityLogForwardingParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}

	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityLogForwardingCreate", &ontapRest.SecurityLogForwardingCreateParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}).Return(nil, fmt.Errorf("Rest Error"))

	resp, err := ontapProvider.CreateSecurityLogForwarding(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "Rest Error", err.Error())
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestCreateSecurityLogForwarding_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateSecurityLogForwardingParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}

	resp, err := ontapProvider.CreateSecurityLogForwarding(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestGetSecurityLogForwarding_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := GetSecurityLogForwardingParams{
		Address: "test-address",
		Port:    1009,
	}
	response := ontapRest.SecurityAuditLogForward{
		SecurityAuditLogForward: models.SecurityAuditLogForward{
			Address: nillable.GetStringPtr("test-address"),
		},
	}
	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityLogForwardingGet", &ontapRest.SecurityLogForwardingGetParams{
		Address: "test-address",
		Port:    1009,
	}).Return(&response, nil)

	err := ontapProvider.GetSecurityLogForwarding(params)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestGetSecurityLogForwarding_ResponseError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := GetSecurityLogForwardingParams{
		Address: "test-address",
		Port:    1009,
	}

	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityLogForwardingGet", &ontapRest.SecurityLogForwardingGetParams{
		Address: "test-address",
		Port:    1009,
	}).Return(nil, fmt.Errorf("Rest Error"))

	err := ontapProvider.GetSecurityLogForwarding(params)
	assert.Error(t, err)
	assert.Equal(t, "Rest Error", err.Error())
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestGetSecurityLogForwarding_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	ontapProvider := &OntapRestProvider{}
	params := GetSecurityLogForwardingParams{
		Address: "test-address",
		Port:    1009,
	}

	err := ontapProvider.GetSecurityLogForwarding(params)
	assert.Error(t, err)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestCreateEMSEventForwarding_Success(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err)
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_DestinationCreateError(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(fmt.Errorf("failed to create destination"))

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create EMS destination")
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_FilterCreateError(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(fmt.Errorf("failed to create filter"))

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create EMS filter")
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.Error(t, err)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestCreateEMSEventForwarding_FilterRuleAddError(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(fmt.Errorf("failed to add rule"))

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to add EMS filter rule")
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_DestinationModifyError(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(fmt.Errorf("failed to modify destination"))

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to link EMS filter to destination")
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_DestinationAlreadyExists(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(fmt.Errorf("destination already exists"))
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err) // Should succeed despite "already exists" error
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_DestinationError983(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(fmt.Errorf("error 983"))
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err) // Should succeed despite error 983
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_FilterAlreadyExists(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(fmt.Errorf("filter already exists"))
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err) // Should succeed despite "already exists" error
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_FilterError983(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(fmt.Errorf("error 983"))
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err) // Should succeed despite error 983
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_FilterRuleAlreadyExists(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(fmt.Errorf("rule already exists"))
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err) // Should succeed despite "already exists" error
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_FilterRuleError983(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(fmt.Errorf("error 983"))
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err) // Should succeed despite error 983
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_DestinationModifyAlreadyLinked(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(fmt.Errorf("already linked"))

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err) // Should succeed despite "already" error
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateEMSEventForwarding_DestinationModifyError983(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
	mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(fmt.Errorf("error 983"))

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
		Logger:       log.NewLogger().(*log.Slogger),
	}

	params := CreateEMSEventForwardingParams{
		DestinationName: "syslog-ems",
		DestinationIP:   "35.239.71.238",
		DestinationPort: 5140,
		Transport:       "tcp-unencrypted",
		TimestampFormat: "rfc-3164",
		MessageFormat:   "legacy-netapp",
		FilterName:      "syslog-ems",
		Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
	}

	err := provider.CreateEMSEventForwarding(params)
	assert.NoError(t, err) // Should succeed despite error 983
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetEMSEventForwarding_Success(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	destination := &ontapRest.EMSEventDestination{
		Name: "syslog-ems",
		Type: "syslog",
		Syslog: &ontapRest.EMSEventDestinationSyslog{
			Host:      "35.239.71.238",
			Port:      5140,
			Transport: "tcp",
		},
	}
	mockSupportClient.On("EMSEventDestinationGet", "syslog-ems").Return(destination, nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	result, err := provider.GetEMSEventForwarding("syslog-ems")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "syslog-ems", result.Name)
	assert.Equal(t, "syslog", result.Type)
	assert.NotNil(t, result.Syslog)
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetEMSEventForwarding_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	_, err := provider.GetEMSEventForwarding("syslog-ems")
	assert.Error(t, err)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestGetEMSEventForwarding_DestinationGetError(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationGet", "syslog-ems").Return(nil, fmt.Errorf("not found"))

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	_, err := provider.GetEMSEventForwarding("syslog-ems")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetEMSEventForwarding_NilDestination(t *testing.T) {
	// Create mock SupportClient
	mockSupportClient := new(ontapRest.MockSupportClient)
	mockSupportClient.On("EMSEventDestinationGet", "syslog-ems").Return(nil, nil)

	// Create mock RESTClient
	mockClient := new(ontapRest.MockRESTClient)
	mockClient.On("Support").Return(mockSupportClient)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	provider := &OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{},
	}

	_, err := provider.GetEMSEventForwarding("syslog-ems")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	mockSupportClient.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}
