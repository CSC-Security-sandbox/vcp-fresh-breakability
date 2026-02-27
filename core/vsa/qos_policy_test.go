// Ensure correct package declaration
package vsa

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateQoSGroupPolicy(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
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
		IsShared:      nil, // nil means ONTAP defaults to false
	}).Return(expectedQosPolicy, nil, nil)

	resp, err := ontapProvider.CreateQoSGroupPolicy(params)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test-policy", resp.Name)
	assert.Equal(t, "uuid-123", resp.UUID)
	assert.Equal(t, "svm1", resp.SvmName)
	assert.Equal(t, int64(1000), resp.MaxThroughput)
	assert.Equal(t, int64(5000), resp.MaxIOPS)
	assert.False(t, resp.IsShared) // Default to false when CapacityShared is nil in response
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("JobAccepted", func(t *testing.T) {
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
		IsShared:      nil, // nil means ONTAP defaults to false
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
	})

	t.Run("Error", func(t *testing.T) {
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
		IsShared:      nil, // nil means ONTAP defaults to false
	}).Return(nil, nil, fmt.Errorf("API error"))

		resp, err := ontapProvider.CreateQoSGroupPolicy(params)
		assert.Error(t, err)
		assert.Nil(t, resp)
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("GetOntapClientError", func(t *testing.T) {
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
	})
}

func TestDeleteQoSGroupPolicy(t *testing.T) {
	t.Run("Success_WithUUID", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := DeleteQoSGroupPolicyParams{
			UUID:    "uuid-123",
			SvmName: "svm1",
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QosPolicyDeleteCollection", &ontapRest.QosPolicyDeleteCollectionParams{
			UUID:    "uuid-123",
			Name:    "",
			SvmName: "svm1",
		}).Return(nil, nil)

		err := ontapProvider.DeleteQoSGroupPolicy(params)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("Success_WithName", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := DeleteQoSGroupPolicyParams{
			Name:    "test-policy",
			SvmName: "svm1",
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QosPolicyDeleteCollection", &ontapRest.QosPolicyDeleteCollectionParams{
			UUID:    "",
			Name:    "test-policy",
			SvmName: "svm1",
		}).Return(nil, nil)

		err := ontapProvider.DeleteQoSGroupPolicy(params)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("Error_MissingParams", func(t *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return new(ontapRest.MockRESTClient), nil
		}

		ontapProvider := &OntapRestProvider{}

		t.Run("MissingBothUUIDAndName", func(t *testing.T) {
			params := DeleteQoSGroupPolicyParams{
				SvmName: "svm1",
			}

			err := ontapProvider.DeleteQoSGroupPolicy(params)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Invalid input parameters provided")
		})

		t.Run("NameWithoutSvmName", func(t *testing.T) {
			mockClient := new(ontapRest.MockRESTClient)
			mockStorageClient := new(ontapRest.MockStorageClient)
			originalgetOntapClientFunc := getOntapClientFunc
			defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

			getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
				return mockClient, nil
			}

			ontapProvider := &OntapRestProvider{}
			params := DeleteQoSGroupPolicyParams{
				Name: "test-policy",
			}

			mockClient.On("Storage").Return(mockStorageClient)
			mockStorageClient.On("QosPolicyDeleteCollection", &ontapRest.QosPolicyDeleteCollectionParams{
				UUID:    "",
				Name:    "test-policy",
				SvmName: "",
			}).Return(nil, nil)

			err := ontapProvider.DeleteQoSGroupPolicy(params)
			assert.NoError(t, err)
			mockClient.AssertExpectations(t)
			mockStorageClient.AssertExpectations(t)
		})

		t.Run("UUIDWithoutSvmName", func(t *testing.T) {
			mockClient := new(ontapRest.MockRESTClient)
			mockStorageClient := new(ontapRest.MockStorageClient)
			originalgetOntapClientFunc := getOntapClientFunc
			defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

			getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
				return mockClient, nil
			}

			ontapProvider := &OntapRestProvider{}
			params := DeleteQoSGroupPolicyParams{
				UUID: "uuid-123",
			}

			mockClient.On("Storage").Return(mockStorageClient)
			mockStorageClient.On("QosPolicyDeleteCollection", &ontapRest.QosPolicyDeleteCollectionParams{
				UUID:    "uuid-123",
				Name:    "",
				SvmName: "",
			}).Return(nil, nil)

			err := ontapProvider.DeleteQoSGroupPolicy(params)
			assert.NoError(t, err)
			mockClient.AssertExpectations(t)
			mockStorageClient.AssertExpectations(t)
		})

		t.Run("BothUUIDAndNameProvided", func(t *testing.T) {
			params := DeleteQoSGroupPolicyParams{
				UUID:    "uuid-123",
				Name:    "test-policy",
				SvmName: "svm1",
			}

			err := ontapProvider.DeleteQoSGroupPolicy(params)
			assert.Error(t, err)
			// VCP error returns the mapped message, not the original error message
			assert.Contains(t, err.Error(), "Invalid input parameters provided")
		})
	})

	t.Run("Error_InUse", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := DeleteQoSGroupPolicyParams{
			UUID:    "uuid-in-use",
			SvmName: "svm1",
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QosPolicyDeleteCollection", &ontapRest.QosPolicyDeleteCollectionParams{
			UUID:    "uuid-in-use",
			Name:    "",
			SvmName: "svm1",
		}).Return(nil, errors.NewConflictErr("policy is in use by volumes"))

		err := ontapProvider.DeleteQoSGroupPolicy(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Resource is in an invalid state")
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("Error_GetOntapClientError", func(t *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, fmt.Errorf("getOntapClient error")
		}

		ontapProvider := &OntapRestProvider{}
		params := DeleteQoSGroupPolicyParams{
			UUID:    "uuid-123",
			SvmName: "svm1",
		}

		err := ontapProvider.DeleteQoSGroupPolicy(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "getOntapClient error")
	})

	t.Run("NotFound_Idempotent", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := DeleteQoSGroupPolicyParams{
			UUID:    "uuid-not-found",
			SvmName: "svm1",
		}

		mockClient.On("Storage").Return(mockStorageClient)
		notFoundErr := errors.NewNotFoundErr("qosPolicy", nillable.GetStringPtr("uuid-not-found"))
		mockStorageClient.On("QosPolicyDeleteCollection", &ontapRest.QosPolicyDeleteCollectionParams{
			UUID:    "uuid-not-found",
			Name:    "",
			SvmName: "svm1",
		}).Return(nil, notFoundErr)

		err := ontapProvider.DeleteQoSGroupPolicy(params)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("Error_GenericError", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := DeleteQoSGroupPolicyParams{
			UUID:    "uuid-error",
			SvmName: "svm1",
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QosPolicyDeleteCollection", &ontapRest.QosPolicyDeleteCollectionParams{
			UUID:    "uuid-error",
			Name:    "",
			SvmName: "svm1",
		}).Return(nil, fmt.Errorf("generic API error"))

		err := ontapProvider.DeleteQoSGroupPolicy(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "An internal error occurred")
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("Error_JobPollError", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := DeleteQoSGroupPolicyParams{
			UUID:    "uuid-with-job",
			SvmName: "svm1",
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QosPolicyDeleteCollection", &ontapRest.QosPolicyDeleteCollectionParams{
			UUID:    "uuid-with-job",
			Name:    "",
			SvmName: "svm1",
		}).Return(&ontapRest.JobAccepted{JobUUID: "job-uuid"}, nil)

		mockClient.On("Poll", "job-uuid").Return(fmt.Errorf("poll error"))

		err := ontapProvider.DeleteQoSGroupPolicy(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "An internal error occurred")
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})
}

func TestFindQoSGroupPolicy(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := FindQoSGroupPolicyParams{
			Name:    "dedicated-policy",
			SvmName: "svm1",
		}

		expectedQosPolicy := &ontapRest.QosPolicy{
			QosPolicy: models.QosPolicy{
				Name: nillable.GetStringPtr("dedicated-policy"),
				UUID: nillable.GetStringPtr("uuid-789"),
				Svm: &models.QosPolicyInlineSvm{
					Name: nillable.GetStringPtr("svm1"),
				},
				Fixed: &models.QosPolicyInlineFixed{
					MaxThroughputMbps: nillable.GetInt64Ptr(2000),
					MaxThroughputIops: nillable.GetInt64Ptr(10000),
					CapacityShared:    nillable.GetBoolPtr(false),
				},
			},
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QoSPolicyGroupFind", &ontapRest.QoSPolicyGroupFindParams{
			Name:    "dedicated-policy",
			SvmName: "svm1",
		}).Return(expectedQosPolicy, nil)

		resp, err := ontapProvider.FindQoSGroupPolicy(params)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "dedicated-policy", resp.Name)
		assert.False(t, resp.IsShared)
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("WithIsSharedTrue", func(t *testing.T) {
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
					CapacityShared:    nillable.GetBoolPtr(true),
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
		assert.True(t, resp.IsShared)
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("Error", func(t *testing.T) {
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
	})

	t.Run("WithNilCapacityShared", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := FindQoSGroupPolicyParams{
			Name:    "legacy-policy",
			SvmName: "svm1",
		}

		expectedQosPolicy := &ontapRest.QosPolicy{
			QosPolicy: models.QosPolicy{
				Name: nillable.GetStringPtr("legacy-policy"),
				UUID: nillable.GetStringPtr("uuid-legacy"),
				Svm: &models.QosPolicyInlineSvm{
					Name: nillable.GetStringPtr("svm1"),
				},
				Fixed: &models.QosPolicyInlineFixed{
					MaxThroughputMbps: nillable.GetInt64Ptr(1000),
					MaxThroughputIops: nillable.GetInt64Ptr(5000),
					CapacityShared:    nil,
				},
			},
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QoSPolicyGroupFind", &ontapRest.QoSPolicyGroupFindParams{
			Name:    "legacy-policy",
			SvmName: "svm1",
		}).Return(expectedQosPolicy, nil)

	resp, err := ontapProvider.FindQoSGroupPolicy(params)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "legacy-policy", resp.Name)
	assert.False(t, resp.IsShared) // Default to false when CapacityShared is nil
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})
}

func TestUpdateQoSGroupPolicy(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
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
	})

	t.Run("WithIsSharedFalse", func(t *testing.T) {
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
	})

	t.Run("Error", func(t *testing.T) {
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
	})

	t.Run("PollError", func(t *testing.T) {
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
	})

	t.Run("WithEmptyName_ThroughputOnlyUpdate", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := UpdateQoSGroupPolicyParams{
			UUID:          "uuid-456",
			Name:          "", // empty = do not rename; only update throughput/IOPS
			SvmName:       "svm1",
			MaxThroughput: 2000,
			MaxIOPS:       5000,
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QoSPolicyGroupUpdate", &ontapRest.QoSPolicyGroupUpdateParams{
			UUID:          "uuid-456",
			Name:          "",
			SvmName:       "svm1",
			MaxThroughput: 2000,
			MaxIOPS:       5000,
		}).Return(&ontapRest.JobAccepted{JobUUID: "job-uuid"}, nil)

		mockClient.On("Poll", "job-uuid").Return(nil)

		err := ontapProvider.UpdateQoSGroupPolicy(params)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})

	t.Run("WithNonEmptyName_RenameAndUpdate", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		ontapProvider := &OntapRestProvider{}
		params := UpdateQoSGroupPolicyParams{
			UUID:          "uuid-789",
			Name:          "renamed-policy",
			SvmName:       "svm1",
			MaxThroughput: 3000,
			MaxIOPS:       6000,
		}

		mockClient.On("Storage").Return(mockStorageClient)
		mockStorageClient.On("QoSPolicyGroupUpdate", &ontapRest.QoSPolicyGroupUpdateParams{
			UUID:          "uuid-789",
			Name:          "renamed-policy",
			SvmName:       "svm1",
			MaxThroughput: 3000,
			MaxIOPS:       6000,
		}).Return(&ontapRest.JobAccepted{JobUUID: "job-uuid"}, nil)

		mockClient.On("Poll", "job-uuid").Return(nil)

		err := ontapProvider.UpdateQoSGroupPolicy(params)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		mockStorageClient.AssertExpectations(t)
	})
}
