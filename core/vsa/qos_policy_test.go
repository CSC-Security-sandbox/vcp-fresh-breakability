// Ensure correct package declaration
package vsa

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateQoSGroupPolicy_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockStorageClient := new(ontapRest.MockStorageClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateQoSGroupPolicyParams{
		Name:          "test-policy",
		SvmName:       "svm1",
		MaxThroughput: 1000,
		MaxIOPS:       5000,
	}

	expectedQosPolicy := &ontapRest.QosPolicy{
		QosPolicy: models.QosPolicy{
			Name: nillable.GetStringPtr("test-policy"),
			UUID: nillable.GetStringPtr("uuid-123"),
			Svm: &models.QosPolicyInlineSvm{
				Name: nillable.GetStringPtr("svm1"),
			},
			Fixed: &models.QosPolicyInlineFixed{
				MaxThroughputMbps: nillable.GetInt64Ptr(1000),
				MaxThroughputIops: nillable.GetInt64Ptr(5000),
			},
		},
	}

	mockClient.On("Storage").Return(mockStorageClient)
	mockStorageClient.On("QoSPolicyGroupCreate", &ontapRest.QoSPolicyGroupCreateParams{
		Name:          "test-policy",
		SvmName:       "svm1",
		MaxThroughput: 1000,
		MaxIOPS:       5000,
	}).Return(expectedQosPolicy, nil, nil)

	resp, err := ontapProvider.CreateQoSGroupPolicy(params)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test-policy", resp.Name)
	assert.Equal(t, "uuid-123", resp.UUID)
	assert.Equal(t, "svm1", resp.SvmName)
	assert.Equal(t, int64(1000), resp.MaxThroughput)
	assert.Equal(t, int64(5000), resp.MaxIOPS)
	mockClient.AssertExpectations(t)
	mockStorageClient.AssertExpectations(t)
}

func TestCreateQoSGroupPolicy_JobAccepted(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockStorageClient := new(ontapRest.MockStorageClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateQoSGroupPolicyParams{
		Name:          "job-policy",
		SvmName:       "svm2",
		MaxThroughput: 2000,
		MaxIOPS:       6000,
	}

	mockClient.On("Storage").Return(mockStorageClient)
	mockStorageClient.On("QoSPolicyGroupCreate", &ontapRest.QoSPolicyGroupCreateParams{
		Name:          "job-policy",
		SvmName:       "svm2",
		MaxThroughput: 2000,
		MaxIOPS:       6000,
	}).Return(nil, &ontapRest.JobAccepted{JobUUID: "job-uuid"}, nil)

	mockClient.On("Poll", "job-uuid").Return(nil)

	resp, err := ontapProvider.CreateQoSGroupPolicy(params)

	// Assert that no error occurred
	assert.NoError(t, err)

	// Assert that the response is nil since no QoSPolicy is returned
	assert.Nil(t, resp)

	// Verify that the Poll method was called with the correct JobUUID
	mockClient.AssertCalled(t, "Poll", "job-uuid")

	// Verify all expectations
	mockClient.AssertExpectations(t)
	mockStorageClient.AssertExpectations(t)
}

func TestCreateQoSGroupPolicy_Error(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockStorageClient := new(ontapRest.MockStorageClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateQoSGroupPolicyParams{
		Name:          "fail-policy",
		SvmName:       "svm3",
		MaxThroughput: 3000,
		MaxIOPS:       7000,
	}

	mockClient.On("Storage").Return(mockStorageClient)
	mockStorageClient.On("QoSPolicyGroupCreate", &ontapRest.QoSPolicyGroupCreateParams{
		Name:          "fail-policy",
		SvmName:       "svm3",
		MaxThroughput: 3000,
		MaxIOPS:       7000,
	}).Return(nil, nil, fmt.Errorf("API error"))

	resp, err := ontapProvider.CreateQoSGroupPolicy(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	mockClient.AssertExpectations(t)
	mockStorageClient.AssertExpectations(t)
}

func TestCreateQoSGroupPolicy_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateQoSGroupPolicyParams{
		Name:          "any-policy",
		SvmName:       "svm4",
		MaxThroughput: 4000,
		MaxIOPS:       8000,
	}

	resp, err := ontapProvider.CreateQoSGroupPolicy(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestFindQoSGroupPolicy(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockStorageClient := new(ontapRest.MockStorageClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := FindQoSGroupPolicyParams{
		Name:    "test-policy",
		SvmName: "svm1",
	}

	expectedQosPolicy := &ontapRest.QosPolicy{
		QosPolicy: models.QosPolicy{
			Name: nillable.GetStringPtr("test-policy"),
			UUID: nillable.GetStringPtr("uuid-123"),
			Svm: &models.QosPolicyInlineSvm{
				Name: nillable.GetStringPtr("svm1"),
			},
			Fixed: &models.QosPolicyInlineFixed{
				MaxThroughputMbps: nillable.GetInt64Ptr(1000),
				MaxThroughputIops: nillable.GetInt64Ptr(5000),
			},
		},
	}

	mockClient.On("Storage").Return(mockStorageClient)
	mockStorageClient.On("QoSPolicyGroupFind", &ontapRest.QoSPolicyGroupFindParams{
		Name:    "test-policy",
		SvmName: "svm1",
	}).Return(expectedQosPolicy, nil)

	resp, err := ontapProvider.FindQoSGroupPolicy(params)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test-policy", resp.Name)
	assert.Equal(t, "uuid-123", resp.UUID)
	assert.Equal(t, "svm1", resp.SvmName)
	assert.Equal(t, int64(1000), resp.MaxThroughput)
	assert.Equal(t, int64(5000), resp.MaxIOPS)
	mockClient.AssertExpectations(t)
	mockStorageClient.AssertExpectations(t)
}

func TestFindQoSGroupPolicy_Error(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockStorageClient := new(ontapRest.MockStorageClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := FindQoSGroupPolicyParams{
		Name:    "fail-policy",
		SvmName: "svm2",
	}

	mockClient.On("Storage").Return(mockStorageClient)
	mockStorageClient.On("QoSPolicyGroupFind", &ontapRest.QoSPolicyGroupFindParams{
		Name:    "fail-policy",
		SvmName: "svm2",
	}).Return(nil, fmt.Errorf("API error"))

	resp, err := ontapProvider.FindQoSGroupPolicy(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	mockClient.AssertExpectations(t)
	mockStorageClient.AssertExpectations(t)
}

func TestUpdateQoSGroupPolicy(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockStorageClient := new(ontapRest.MockStorageClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := UpdateQoSGroupPolicyParams{
		UUID:          "uuid-123",
		Name:          "test-policy",
		SvmName:       "svm1",
		MaxThroughput: 2000,
		MaxIOPS:       6000,
	}

	mockClient.On("Storage").Return(mockStorageClient)
	mockStorageClient.On("QoSPolicyGroupUpdate", &ontapRest.QoSPolicyGroupUpdateParams{
		UUID:          "uuid-123",
		Name:          "test-policy",
		SvmName:       "svm1",
		MaxThroughput: 2000,
		MaxIOPS:       6000,
	}).Return(&ontapRest.JobAccepted{JobUUID: "job-uuid"}, nil)

	mockClient.On("Poll", "job-uuid").Return(nil)

	err := ontapProvider.UpdateQoSGroupPolicy(params)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockStorageClient.AssertExpectations(t)
}

func TestUpdateQoSGroupPolicy_Error(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockStorageClient := new(ontapRest.MockStorageClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := UpdateQoSGroupPolicyParams{
		UUID:          "uuid-123",
		Name:          "fail-policy",
		SvmName:       "svm2",
		MaxThroughput: 3000,
		MaxIOPS:       7000,
	}

	mockClient.On("Storage").Return(mockStorageClient)
	mockStorageClient.On("QoSPolicyGroupUpdate", &ontapRest.QoSPolicyGroupUpdateParams{
		UUID:          "uuid-123",
		Name:          "fail-policy",
		SvmName:       "svm2",
		MaxThroughput: 3000,
		MaxIOPS:       7000,
	}).Return(nil, fmt.Errorf("API error"))

	err := ontapProvider.UpdateQoSGroupPolicy(params)
	assert.Error(t, err)
	mockClient.AssertExpectations(t)
	mockStorageClient.AssertExpectations(t)
}

func TestUpdateQoSGroupPolicy_PollError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockStorageClient := new(ontapRest.MockStorageClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := UpdateQoSGroupPolicyParams{
		UUID:          "uuid-123",
		Name:          "poll-fail-policy",
		SvmName:       "svm3",
		MaxThroughput: 4000,
		MaxIOPS:       8000,
	}

	mockClient.On("Storage").Return(mockStorageClient)
	mockStorageClient.On("QoSPolicyGroupUpdate", &ontapRest.QoSPolicyGroupUpdateParams{
		UUID:          "uuid-123",
		Name:          "poll-fail-policy",
		SvmName:       "svm3",
		MaxThroughput: 4000,
		MaxIOPS:       8000,
	}).Return(&ontapRest.JobAccepted{JobUUID: "job-uuid"}, nil)

	mockClient.On("Poll", "job-uuid").Return(fmt.Errorf("poll error"))

	err := ontapProvider.UpdateQoSGroupPolicy(params)
	assert.Error(t, err)
	mockClient.AssertExpectations(t)
	mockStorageClient.AssertExpectations(t)
}
