package ontap_rest

import (
	"errors"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/support"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	vcpErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// helper function to create a pointer to support.ClientService
func supportClientServicePtr(api support.ClientService) *support.ClientService {
	return &api
}

func TestEMSEventDestinationCreate_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventDestinationCreateParams{
		Name:                  nillable.GetStringPtr("test-dest"),
		Type:                  nillable.GetStringPtr("syslog"),
		SyslogHost:            nillable.GetStringPtr("10.0.0.1"),
		SyslogPort:            nillable.GetInt64Ptr(514),
		SyslogTransport:       nillable.GetStringPtr("tcp"),
		SyslogTimestampFormat: nillable.GetStringPtr("rfc-3164"),
		SyslogMessageFormat:   nillable.GetStringPtr("legacy-netapp"),
	}

	otParams := support.NewEmsDestinationCreateParams()
	otParams.SetInfo(emsDestinationCreateParamsToONTAP(params))

	mockAPI.On("EmsDestinationCreate", otParams, mock.Anything, mock.Anything).Return(&support.EmsDestinationCreateCreated{}, nil)

	err := sc.EMSEventDestinationCreate(params)
	assert.NoError(t, err)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationCreate_AlreadyExists(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventDestinationCreateParams{
		Name: nillable.GetStringPtr("test-dest"),
	}

	otParams := support.NewEmsDestinationCreateParams()
	otParams.SetInfo(emsDestinationCreateParamsToONTAP(params))

	// Test "already exists" error (idempotent)
	mockAPI.On("EmsDestinationCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("destination already exists"))

	err := sc.EMSEventDestinationCreate(params)
	assert.NoError(t, err) // Should return nil for "already exists"
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationCreate_Error983(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventDestinationCreateParams{
		Name: nillable.GetStringPtr("test-dest"),
	}

	otParams := support.NewEmsDestinationCreateParams()
	otParams.SetInfo(emsDestinationCreateParamsToONTAP(params))

	// Test error code 983 - should NOT be treated as idempotent since it's not specific
	// ONTAP uses many 983xxx error codes for different failures, not just "already exists"
	mockAPI.On("EmsDestinationCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("error 983"))

	err := sc.EMSEventDestinationCreate(params)
	assert.Error(t, err) // Should return error since "983" alone is not specific enough
	assert.Contains(t, err.Error(), "error 983")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationCreate_HTTP409(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventDestinationCreateParams{
		Name: nillable.GetStringPtr("test-dest"),
	}

	otParams := support.NewEmsDestinationCreateParams()
	otParams.SetInfo(emsDestinationCreateParamsToONTAP(params))

	// Test HTTP 409 (Conflict) - should be treated as idempotent
	apiError := &runtime.APIError{
		Code:    409,
		Response: nil,
	}
	mockAPI.On("EmsDestinationCreate", otParams, mock.Anything, mock.Anything).Return(nil, apiError)

	err := sc.EMSEventDestinationCreate(params)
	assert.NoError(t, err) // Should return nil for HTTP 409
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationCreate_OtherError(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventDestinationCreateParams{
		Name: nillable.GetStringPtr("test-dest"),
	}

	otParams := support.NewEmsDestinationCreateParams()
	otParams.SetInfo(emsDestinationCreateParamsToONTAP(params))

	// Test other error
	mockAPI.On("EmsDestinationCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("other error"))

	err := sc.EMSEventDestinationCreate(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "other error")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationCreate_NilParams(t *testing.T) {
	sc := &supportClient{api: nil}

	err := sc.EMSEventDestinationCreate(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "destination name is required")
}

func TestEMSEventDestinationCreate_NilName(t *testing.T) {
	sc := &supportClient{api: nil}

	params := &EMSEventDestinationCreateParams{
		Name: nil,
	}

	err := sc.EMSEventDestinationCreate(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "destination name is required")
}

func TestEMSEventDestinationCreate_NilResponse(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventDestinationCreateParams{
		Name: nillable.GetStringPtr("test-dest"),
	}

	otParams := support.NewEmsDestinationCreateParams()
	otParams.SetInfo(emsDestinationCreateParamsToONTAP(params))

	mockAPI.On("EmsDestinationCreate", otParams, mock.Anything, mock.Anything).Return(nil, nil)

	err := sc.EMSEventDestinationCreate(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationGet_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	otParams := support.NewEmsDestinationGetParams()
	otParams.SetName(name)

	mockPayload := &support.EmsDestinationGetOK{
		Payload: &models.EmsDestination{
			Name: nillable.GetStringPtr("test-dest"),
			Type: nillable.GetStringPtr("syslog"),
		},
	}

	mockAPI.On("EmsDestinationGet", otParams, mock.Anything, mock.Anything).Return(mockPayload, nil)

	result, err := sc.EMSEventDestinationGet(name)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-dest", result.Name)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationGet_EmptyName(t *testing.T) {
	sc := &supportClient{api: nil}

	_, err := sc.EMSEventDestinationGet("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "destination name is required")
}

func TestEMSEventDestinationGet_Error(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	otParams := support.NewEmsDestinationGetParams()
	otParams.SetName(name)

	mockAPI.On("EmsDestinationGet", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("not found"))

	_, err := sc.EMSEventDestinationGet(name)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationGet_NilResponse(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	otParams := support.NewEmsDestinationGetParams()
	otParams.SetName(name)

	mockAPI.On("EmsDestinationGet", otParams, mock.Anything, mock.Anything).Return(nil, nil)

	_, err := sc.EMSEventDestinationGet(name)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationModify_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	params := &EMSEventDestinationModifyParams{
		Filters: []string{"filter1"},
	}

	otParams := support.NewEmsDestinationModifyParams()
	otParams.SetName(name)
	otParams.SetInfo(emsDestinationModifyParamsToONTAP(params))

	mockAPI.On("EmsDestinationModify", otParams, mock.Anything, mock.Anything).Return(&support.EmsDestinationModifyOK{}, nil)

	err := sc.EMSEventDestinationModify(name, params)
	assert.NoError(t, err)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationModify_EmptyName(t *testing.T) {
	sc := &supportClient{api: nil}

	params := &EMSEventDestinationModifyParams{}
	err := sc.EMSEventDestinationModify("", params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "destination name is required")
}

func TestEMSEventDestinationModify_AlreadyLinked(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	params := &EMSEventDestinationModifyParams{}

	otParams := support.NewEmsDestinationModifyParams()
	otParams.SetName(name)
	otParams.SetInfo(emsDestinationModifyParamsToONTAP(params))

	// Test "already" error (idempotent)
	mockAPI.On("EmsDestinationModify", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("already linked"))

	err := sc.EMSEventDestinationModify(name, params)
	assert.NoError(t, err) // Should return nil for "already" errors
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationModify_Error983(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	params := &EMSEventDestinationModifyParams{}

	otParams := support.NewEmsDestinationModifyParams()
	otParams.SetName(name)
	otParams.SetInfo(emsDestinationModifyParamsToONTAP(params))

	// Test error code 983 - should NOT be treated as idempotent since it's not specific
	mockAPI.On("EmsDestinationModify", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("error 983"))

	err := sc.EMSEventDestinationModify(name, params)
	assert.Error(t, err) // Should return error since "983" alone is not specific enough
	assert.Contains(t, err.Error(), "error 983")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationModify_HTTP409(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	params := &EMSEventDestinationModifyParams{}

	otParams := support.NewEmsDestinationModifyParams()
	otParams.SetName(name)
	otParams.SetInfo(emsDestinationModifyParamsToONTAP(params))

	// Test HTTP 409 (Conflict) - should be treated as idempotent
	apiError := &runtime.APIError{
		Code:    409,
		Response: nil,
	}
	mockAPI.On("EmsDestinationModify", otParams, mock.Anything, mock.Anything).Return(nil, apiError)

	err := sc.EMSEventDestinationModify(name, params)
	assert.NoError(t, err) // Should return nil for HTTP 409
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationModify_OtherError(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	params := &EMSEventDestinationModifyParams{}

	otParams := support.NewEmsDestinationModifyParams()
	otParams.SetName(name)
	otParams.SetInfo(emsDestinationModifyParamsToONTAP(params))

	// Test other error
	mockAPI.On("EmsDestinationModify", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("other error"))

	err := sc.EMSEventDestinationModify(name, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "other error")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationModify_NilResponse(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	params := &EMSEventDestinationModifyParams{}

	otParams := support.NewEmsDestinationModifyParams()
	otParams.SetName(name)
	otParams.SetInfo(emsDestinationModifyParamsToONTAP(params))

	mockAPI.On("EmsDestinationModify", otParams, mock.Anything, mock.Anything).Return(nil, nil)

	err := sc.EMSEventDestinationModify(name, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterCreate_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterCreateParams{
		Name: nillable.GetStringPtr("test-filter"),
	}

	otParams := support.NewEmsFilterCreateParams()
	otParams.SetInfo(emsFilterCreateParamsToONTAP(params))

	mockAPI.On("EmsFilterCreate", otParams, mock.Anything, mock.Anything).Return(&support.EmsFilterCreateCreated{}, nil)

	err := sc.EMSEventFilterCreate(params)
	assert.NoError(t, err)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterCreate_AlreadyExists(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterCreateParams{
		Name: nillable.GetStringPtr("test-filter"),
	}

	otParams := support.NewEmsFilterCreateParams()
	otParams.SetInfo(emsFilterCreateParamsToONTAP(params))

	// Test "already exists" error (idempotent)
	mockAPI.On("EmsFilterCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("filter already exists"))

	err := sc.EMSEventFilterCreate(params)
	assert.NoError(t, err) // Should return nil for "already exists"
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterCreate_Error983(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterCreateParams{
		Name: nillable.GetStringPtr("test-filter"),
	}

	otParams := support.NewEmsFilterCreateParams()
	otParams.SetInfo(emsFilterCreateParamsToONTAP(params))

	// Test error code 983 - should NOT be treated as idempotent since it's not specific
	mockAPI.On("EmsFilterCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("error 983"))

	err := sc.EMSEventFilterCreate(params)
	assert.Error(t, err) // Should return error since "983" alone is not specific enough
	assert.Contains(t, err.Error(), "error 983")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterCreate_HTTP409(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterCreateParams{
		Name: nillable.GetStringPtr("test-filter"),
	}

	otParams := support.NewEmsFilterCreateParams()
	otParams.SetInfo(emsFilterCreateParamsToONTAP(params))

	// Test HTTP 409 (Conflict) - should be treated as idempotent
	apiError := &runtime.APIError{
		Code:    409,
		Response: nil,
	}
	mockAPI.On("EmsFilterCreate", otParams, mock.Anything, mock.Anything).Return(nil, apiError)

	err := sc.EMSEventFilterCreate(params)
	assert.NoError(t, err) // Should return nil for HTTP 409
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterCreate_OtherError(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterCreateParams{
		Name: nillable.GetStringPtr("test-filter"),
	}

	otParams := support.NewEmsFilterCreateParams()
	otParams.SetInfo(emsFilterCreateParamsToONTAP(params))

	// Test other error
	mockAPI.On("EmsFilterCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("other error"))

	err := sc.EMSEventFilterCreate(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "other error")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterCreate_NilParams(t *testing.T) {
	sc := &supportClient{api: nil}

	err := sc.EMSEventFilterCreate(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter name is required")
}

func TestEMSEventFilterCreate_NilName(t *testing.T) {
	sc := &supportClient{api: nil}

	params := &EMSEventFilterCreateParams{
		Name: nil,
	}

	err := sc.EMSEventFilterCreate(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter name is required")
}

func TestEMSEventFilterCreate_NilResponse(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterCreateParams{
		Name: nillable.GetStringPtr("test-filter"),
	}

	otParams := support.NewEmsFilterCreateParams()
	otParams.SetInfo(emsFilterCreateParamsToONTAP(params))

	mockAPI.On("EmsFilterCreate", otParams, mock.Anything, mock.Anything).Return(nil, nil)

	err := sc.EMSEventFilterCreate(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterGet_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-filter"
	otParams := support.NewEmsFilterGetParams()
	otParams.SetName(name)

	mockPayload := &support.EmsFilterGetOK{
		Payload: &models.EmsFilter{
			Name: nillable.GetStringPtr("test-filter"),
		},
	}

	mockAPI.On("EmsFilterGet", otParams, mock.Anything, mock.Anything).Return(mockPayload, nil)

	result, err := sc.EMSEventFilterGet(name)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-filter", result.Name)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterGet_EmptyName(t *testing.T) {
	sc := &supportClient{api: nil}

	_, err := sc.EMSEventFilterGet("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter name is required")
}

func TestEMSEventFilterGet_Error(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-filter"
	otParams := support.NewEmsFilterGetParams()
	otParams.SetName(name)

	mockAPI.On("EmsFilterGet", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("not found"))

	_, err := sc.EMSEventFilterGet(name)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterGet_NilResponse(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-filter"
	otParams := support.NewEmsFilterGetParams()
	otParams.SetName(name)

	mockAPI.On("EmsFilterGet", otParams, mock.Anything, mock.Anything).Return(nil, nil)

	_, err := sc.EMSEventFilterGet(name)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleAdd_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterRuleAddParams{
		FilterName: nillable.GetStringPtr("test-filter"),
		Type:       nillable.GetStringPtr("include"),
		Severity:   []string{"ERROR", "WARNING"},
	}

	otParams := support.NewEmsFiltersRulesCreateParams()
	otParams.SetName(*params.FilterName)
	otParams.SetInfo(emsFilterRuleAddParamsToONTAP(params))

	mockAPI.On("EmsFiltersRulesCreate", otParams, mock.Anything, mock.Anything).Return(&support.EmsFiltersRulesCreateCreated{}, nil)

	err := sc.EMSEventFilterRuleAdd(params)
	assert.NoError(t, err)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleAdd_AlreadyExists(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterRuleAddParams{
		FilterName: nillable.GetStringPtr("test-filter"),
		Type:       nillable.GetStringPtr("include"),
		Severity:   []string{"ERROR"},
	}

	otParams := support.NewEmsFiltersRulesCreateParams()
	otParams.SetName(*params.FilterName)
	otParams.SetInfo(emsFilterRuleAddParamsToONTAP(params))

	// Test "already exists" error (idempotent)
	mockAPI.On("EmsFiltersRulesCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("rule already exists"))

	err := sc.EMSEventFilterRuleAdd(params)
	assert.NoError(t, err) // Should return nil for "already exists"
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleAdd_Error983(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterRuleAddParams{
		FilterName: nillable.GetStringPtr("test-filter"),
		Type:       nillable.GetStringPtr("include"),
		Severity:   []string{"ERROR"},
	}

	otParams := support.NewEmsFiltersRulesCreateParams()
	otParams.SetName(*params.FilterName)
	otParams.SetInfo(emsFilterRuleAddParamsToONTAP(params))

	// Test error code 983 - should NOT be treated as idempotent since it's not specific
	mockAPI.On("EmsFiltersRulesCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("error 983"))

	err := sc.EMSEventFilterRuleAdd(params)
	assert.Error(t, err) // Should return error since "983" alone is not specific enough
	assert.Contains(t, err.Error(), "error 983")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleAdd_HTTP409(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterRuleAddParams{
		FilterName: nillable.GetStringPtr("test-filter"),
		Type:       nillable.GetStringPtr("include"),
		Severity:   []string{"ERROR"},
	}

	otParams := support.NewEmsFiltersRulesCreateParams()
	otParams.SetName(*params.FilterName)
	otParams.SetInfo(emsFilterRuleAddParamsToONTAP(params))

	// Test HTTP 409 (Conflict) - should be treated as idempotent
	apiError := &runtime.APIError{
		Code:    409,
		Response: nil,
	}
	mockAPI.On("EmsFiltersRulesCreate", otParams, mock.Anything, mock.Anything).Return(nil, apiError)

	err := sc.EMSEventFilterRuleAdd(params)
	assert.NoError(t, err) // Should return nil for HTTP 409
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleAdd_OtherError(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterRuleAddParams{
		FilterName: nillable.GetStringPtr("test-filter"),
		Type:       nillable.GetStringPtr("include"),
		Severity:   []string{"ERROR"},
	}

	otParams := support.NewEmsFiltersRulesCreateParams()
	otParams.SetName(*params.FilterName)
	otParams.SetInfo(emsFilterRuleAddParamsToONTAP(params))

	// Test other error
	mockAPI.On("EmsFiltersRulesCreate", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("other error"))

	err := sc.EMSEventFilterRuleAdd(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "other error")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleAdd_NilParams(t *testing.T) {
	sc := &supportClient{api: nil}

	err := sc.EMSEventFilterRuleAdd(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter name is required")
}

func TestEMSEventFilterRuleAdd_NilFilterName(t *testing.T) {
	sc := &supportClient{api: nil}

	params := &EMSEventFilterRuleAddParams{
		FilterName: nil,
	}

	err := sc.EMSEventFilterRuleAdd(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter name is required")
}

func TestEMSEventFilterRuleAdd_NilResponse(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	params := &EMSEventFilterRuleAddParams{
		FilterName: nillable.GetStringPtr("test-filter"),
		Type:       nillable.GetStringPtr("include"),
		Severity:   []string{"ERROR"},
	}

	otParams := support.NewEmsFiltersRulesCreateParams()
	otParams.SetName(*params.FilterName)
	otParams.SetInfo(emsFilterRuleAddParamsToONTAP(params))

	mockAPI.On("EmsFiltersRulesCreate", otParams, mock.Anything, mock.Anything).Return(nil, nil)

	err := sc.EMSEventFilterRuleAdd(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationDelete_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	otParams := support.NewEmsDestinationDeleteParams()
	otParams.SetName(name)

	mockAPI.On("EmsDestinationDelete", otParams, mock.Anything, mock.Anything).Return(&support.EmsDestinationDeleteOK{}, nil)

	err := sc.EMSEventDestinationDelete(name)
	assert.NoError(t, err)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationDelete_EmptyName(t *testing.T) {
	sc := &supportClient{api: nil}

	err := sc.EMSEventDestinationDelete("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "destination name is required")
}

func TestEMSEventDestinationDelete_NotFound(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	otParams := support.NewEmsDestinationDeleteParams()
	otParams.SetName(name)

	// Test "not found" error (idempotent)
	notFoundErr := vcpErrors.NewNotFoundErr("Destination", nillable.GetStringPtr(name))
	mockAPI.On("EmsDestinationDelete", otParams, mock.Anything, mock.Anything).Return(nil, notFoundErr)

	err := sc.EMSEventDestinationDelete(name)
	assert.NoError(t, err) // Should return nil for "not found" errors
	mockAPI.AssertExpectations(t)
}

func TestEMSEventDestinationDelete_OtherError(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-dest"
	otParams := support.NewEmsDestinationDeleteParams()
	otParams.SetName(name)

	// Test other error
	mockAPI.On("EmsDestinationDelete", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("other error"))

	err := sc.EMSEventDestinationDelete(name)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "other error")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterDelete_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-filter"
	otParams := support.NewEmsFilterDeleteParams()
	otParams.SetName(name)

	mockAPI.On("EmsFilterDelete", otParams, mock.Anything, mock.Anything).Return(&support.EmsFilterDeleteOK{}, nil)

	err := sc.EMSEventFilterDelete(name)
	assert.NoError(t, err)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterDelete_EmptyName(t *testing.T) {
	sc := &supportClient{api: nil}

	err := sc.EMSEventFilterDelete("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter name is required")
}

func TestEMSEventFilterDelete_NotFound(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-filter"
	otParams := support.NewEmsFilterDeleteParams()
	otParams.SetName(name)

	// Test "not found" error (idempotent)
	notFoundErr := vcpErrors.NewNotFoundErr("Filter", nillable.GetStringPtr(name))
	mockAPI.On("EmsFilterDelete", otParams, mock.Anything, mock.Anything).Return(nil, notFoundErr)

	err := sc.EMSEventFilterDelete(name)
	assert.NoError(t, err) // Should return nil for "not found" errors
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterDelete_OtherError(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	name := "test-filter"
	otParams := support.NewEmsFilterDeleteParams()
	otParams.SetName(name)

	// Test other error
	mockAPI.On("EmsFilterDelete", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("other error"))

	err := sc.EMSEventFilterDelete(name)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "other error")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRulesGet_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	otParams := support.NewEmsFilterRuleCollectionGetParams()
	otParams.SetName(filterName)

	index1 := int64(1)
	index2 := int64(2)
	type1 := "include"
	type2 := "exclude"
	severities1 := "ERROR,WARNING"
	severities2 := "INFO"

	mockPayload := &support.EmsFilterRuleCollectionGetOK{
		Payload: &models.EmsFilterRuleResponse{
			EmsFilterRuleResponseInlineRecords: []*models.EmsFilterRuleResponseInlineRecordsInlineArrayItem{
				{
					Index: &index1,
					Type:  &type1,
					MessageCriteria: &models.EmsFilterRuleResponseInlineRecordsInlineArrayItemInlineMessageCriteria{
						Severities: &severities1,
					},
				},
				{
					Index: &index2,
					Type:  &type2,
					MessageCriteria: &models.EmsFilterRuleResponseInlineRecordsInlineArrayItemInlineMessageCriteria{
						Severities: &severities2,
					},
				},
			},
		},
	}

	mockAPI.On("EmsFilterRuleCollectionGet", otParams, mock.Anything, mock.Anything).Return(mockPayload, nil)

	rules, err := sc.EMSEventFilterRulesGet(filterName)
	assert.NoError(t, err)
	assert.NotNil(t, rules)
	assert.Len(t, rules, 2)
	assert.Equal(t, 1, rules[0].Index)
	assert.Equal(t, "include", rules[0].Type)
	assert.Equal(t, []string{"ERROR", "WARNING"}, rules[0].Severity)
	assert.Equal(t, 2, rules[1].Index)
	assert.Equal(t, "exclude", rules[1].Type)
	assert.Equal(t, []string{"INFO"}, rules[1].Severity)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRulesGet_EmptyName(t *testing.T) {
	sc := &supportClient{api: nil}

	_, err := sc.EMSEventFilterRulesGet("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter name is required")
}

func TestEMSEventFilterRulesGet_Error(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	otParams := support.NewEmsFilterRuleCollectionGetParams()
	otParams.SetName(filterName)

	mockAPI.On("EmsFilterRuleCollectionGet", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("not found"))

	_, err := sc.EMSEventFilterRulesGet(filterName)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRulesGet_NilResponse(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	otParams := support.NewEmsFilterRuleCollectionGetParams()
	otParams.SetName(filterName)

	mockAPI.On("EmsFilterRuleCollectionGet", otParams, mock.Anything, mock.Anything).Return(nil, nil)

	_, err := sc.EMSEventFilterRulesGet(filterName)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRulesGet_NilPayload(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	otParams := support.NewEmsFilterRuleCollectionGetParams()
	otParams.SetName(filterName)

	mockPayload := &support.EmsFilterRuleCollectionGetOK{
		Payload: nil,
	}

	mockAPI.On("EmsFilterRuleCollectionGet", otParams, mock.Anything, mock.Anything).Return(mockPayload, nil)

	_, err := sc.EMSEventFilterRulesGet(filterName)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response")
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRulesGet_EmptyRecords(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	otParams := support.NewEmsFilterRuleCollectionGetParams()
	otParams.SetName(filterName)

	mockPayload := &support.EmsFilterRuleCollectionGetOK{
		Payload: &models.EmsFilterRuleResponse{
			EmsFilterRuleResponseInlineRecords: []*models.EmsFilterRuleResponseInlineRecordsInlineArrayItem{},
		},
	}

	mockAPI.On("EmsFilterRuleCollectionGet", otParams, mock.Anything, mock.Anything).Return(mockPayload, nil)

	rules, err := sc.EMSEventFilterRulesGet(filterName)
	assert.NoError(t, err)
	assert.NotNil(t, rules)
	assert.Len(t, rules, 0)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRulesGet_NoSeverities(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	otParams := support.NewEmsFilterRuleCollectionGetParams()
	otParams.SetName(filterName)

	index1 := int64(1)
	type1 := "include"

	mockPayload := &support.EmsFilterRuleCollectionGetOK{
		Payload: &models.EmsFilterRuleResponse{
			EmsFilterRuleResponseInlineRecords: []*models.EmsFilterRuleResponseInlineRecordsInlineArrayItem{
				{
					Index:          &index1,
					Type:           &type1,
					MessageCriteria: nil,
				},
			},
		},
	}

	mockAPI.On("EmsFilterRuleCollectionGet", otParams, mock.Anything, mock.Anything).Return(mockPayload, nil)

	rules, err := sc.EMSEventFilterRulesGet(filterName)
	assert.NoError(t, err)
	assert.NotNil(t, rules)
	assert.Len(t, rules, 1)
	assert.Equal(t, 1, rules[0].Index)
	assert.Equal(t, "include", rules[0].Type)
	assert.Nil(t, rules[0].Severity)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleDelete_Success(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	index := 1
	otParams := support.NewEmsFilterRuleDeleteParams()
	otParams.SetName(filterName)
	otParams.SetIndex("1")

	mockAPI.On("EmsFilterRuleDelete", otParams, mock.Anything, mock.Anything).Return(&support.EmsFilterRuleDeleteOK{}, nil)

	err := sc.EMSEventFilterRuleDelete(filterName, index)
	assert.NoError(t, err)
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleDelete_EmptyName(t *testing.T) {
	sc := &supportClient{api: nil}

	err := sc.EMSEventFilterRuleDelete("", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filter name is required")
}

func TestEMSEventFilterRuleDelete_InvalidIndex(t *testing.T) {
	sc := &supportClient{api: nil}

	err := sc.EMSEventFilterRuleDelete("test-filter", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rule index must be >= 1")

	err = sc.EMSEventFilterRuleDelete("test-filter", -1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rule index must be >= 1")
}

func TestEMSEventFilterRuleDelete_NotFound(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	index := 1
	otParams := support.NewEmsFilterRuleDeleteParams()
	otParams.SetName(filterName)
	otParams.SetIndex("1")

	// Test "not found" error (idempotent)
	notFoundErr := vcpErrors.NewNotFoundErr("FilterRule", nillable.GetStringPtr("1"))
	mockAPI.On("EmsFilterRuleDelete", otParams, mock.Anything, mock.Anything).Return(nil, notFoundErr)

	err := sc.EMSEventFilterRuleDelete(filterName, index)
	assert.NoError(t, err) // Should return nil for "not found" errors
	mockAPI.AssertExpectations(t)
}

func TestEMSEventFilterRuleDelete_OtherError(t *testing.T) {
	mockAPI := new(MockSupportAPIClient)
	sc := &supportClient{api: supportClientServicePtr(mockAPI)}

	filterName := "test-filter"
	index := 1
	otParams := support.NewEmsFilterRuleDeleteParams()
	otParams.SetName(filterName)
	otParams.SetIndex("1")

	// Test other error
	mockAPI.On("EmsFilterRuleDelete", otParams, mock.Anything, mock.Anything).Return(nil, errors.New("other error"))

	err := sc.EMSEventFilterRuleDelete(filterName, index)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "other error")
	mockAPI.AssertExpectations(t)
}

// MockSupportAPIClient is a mock for the support API client
type MockSupportAPIClient struct {
	mock.Mock
}

func (m *MockSupportAPIClient) EmsDestinationCreate(params *support.EmsDestinationCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsDestinationCreateCreated, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsDestinationCreateCreated), args.Error(1)
}

func (m *MockSupportAPIClient) EmsDestinationGet(params *support.EmsDestinationGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsDestinationGetOK, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsDestinationGetOK), args.Error(1)
}

func (m *MockSupportAPIClient) EmsDestinationModify(params *support.EmsDestinationModifyParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsDestinationModifyOK, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsDestinationModifyOK), args.Error(1)
}

func (m *MockSupportAPIClient) EmsFilterCreate(params *support.EmsFilterCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsFilterCreateCreated, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsFilterCreateCreated), args.Error(1)
}

func (m *MockSupportAPIClient) EmsFilterGet(params *support.EmsFilterGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsFilterGetOK, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsFilterGetOK), args.Error(1)
}

func (m *MockSupportAPIClient) EmsFiltersRulesCreate(params *support.EmsFiltersRulesCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsFiltersRulesCreateCreated, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsFiltersRulesCreateCreated), args.Error(1)
}

func (m *MockSupportAPIClient) EmsDestinationDelete(params *support.EmsDestinationDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsDestinationDeleteOK, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsDestinationDeleteOK), args.Error(1)
}

func (m *MockSupportAPIClient) EmsFilterDelete(params *support.EmsFilterDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsFilterDeleteOK, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsFilterDeleteOK), args.Error(1)
}

func (m *MockSupportAPIClient) EmsFilterRuleCollectionGet(params *support.EmsFilterRuleCollectionGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsFilterRuleCollectionGetOK, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsFilterRuleCollectionGetOK), args.Error(1)
}

func (m *MockSupportAPIClient) EmsFilterRuleDelete(params *support.EmsFilterRuleDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsFilterRuleDeleteOK, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsFilterRuleDeleteOK), args.Error(1)
}

func (m *MockSupportAPIClient) EmsFilterRuleGet(params *support.EmsFilterRuleGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...support.ClientOption) (*support.EmsFilterRuleGetOK, error) {
	args := m.Called(params, authInfo, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*support.EmsFilterRuleGetOK), args.Error(1)
}

func (m *MockSupportAPIClient) SetTransport(transport runtime.ClientTransport) {
	// No-op for mock
}
