package vsa

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprestcluster "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/storage"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// Helper function to wrap quota rules in the proper response type for mocking
func mockQuotaRuleCollectionResponse(rules []*ontaprestmodels.QuotaRule) *storage.QuotaRuleCollectionGetOK {
	numRecords := int64(len(rules))

	return &storage.QuotaRuleCollectionGetOK{
		Payload: &ontaprestmodels.QuotaRuleResponse{
			NumRecords:                     &numRecords,
			QuotaRuleResponseInlineRecords: rules,
		},
	}
}

func TestGetQuotaStatus(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	originalVolumeGetWithContextFunc := volumeGetWithContextFunc

	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
		volumeGetWithContextFunc = originalVolumeGetWithContextFunc
	}()

	t.Run("WhenGetQuotaStatusSucceeds", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, nil // Client not needed when we mock VolumeGetWithContext
		}

		rc := &OntapRestProvider{}
		volumeUUID := "test-volume-uuid"

		expectedQuotaState := "on"
		expectedQuotaEnabled := true

		volumeGetWithContextFunc = func(ctx context.Context, restClient ontaprest.RESTClient, params *ontaprest.VolumeGetParams) (*storage.VolumeGetOK, error) {
			return &storage.VolumeGetOK{
				Payload: &ontaprestmodels.Volume{
					Quota: &ontaprestmodels.VolumeInlineQuota{
						State:   &expectedQuotaState,
						Enabled: &expectedQuotaEnabled,
					},
				},
			}, nil
		}

		result, err := rc.GetQuotaStatus(context.Background(), volumeUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedQuotaState, result.State)
		assert.Equal(tt, expectedQuotaEnabled, result.Enabled)
	})

	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("failed to get ontap client")
		}

		rc := &OntapRestProvider{}
		volumeUUID := "test-volume-uuid"

		result, err := rc.GetQuotaStatus(context.Background(), volumeUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get ontap client")
	})

	t.Run("WhenVolumeGetFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "test-volume-uuid"

		volumeGetWithContextFunc = func(ctx context.Context, restClient ontaprest.RESTClient, params *ontaprest.VolumeGetParams) (*storage.VolumeGetOK, error) {
			return nil, errors.New("volume get failed")
		}

		result, err := rc.GetQuotaStatus(context.Background(), volumeUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "volume get failed")
	})

	t.Run("WhenQuotaFieldIsNil", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "test-volume-uuid"

		volumeGetWithContextFunc = func(ctx context.Context, restClient ontaprest.RESTClient, params *ontaprest.VolumeGetParams) (*storage.VolumeGetOK, error) {
			return &storage.VolumeGetOK{
				Payload: &ontaprestmodels.Volume{
					Quota: nil,
				},
			}, nil
		}

		result, err := rc.GetQuotaStatus(context.Background(), volumeUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Unable to complete the operation")
	})

	t.Run("WhenQuotaStatusIsOff", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "test-volume-uuid"

		expectedQuotaState := "off"
		expectedQuotaEnabled := false

		volumeGetWithContextFunc = func(ctx context.Context, restClient ontaprest.RESTClient, params *ontaprest.VolumeGetParams) (*storage.VolumeGetOK, error) {
			return &storage.VolumeGetOK{
				Payload: &ontaprestmodels.Volume{
					Quota: &ontaprestmodels.VolumeInlineQuota{
						State:   &expectedQuotaState,
						Enabled: &expectedQuotaEnabled,
					},
				},
			}, nil
		}

		result, err := rc.GetQuotaStatus(context.Background(), volumeUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedQuotaState, result.State)
		assert.Equal(tt, expectedQuotaEnabled, result.Enabled)
	})
}

func TestUpdateQuotaRule(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("WhenUpdateQuotaRuleSucceeds", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := &UpdateQuotaRuleParams{
			ExternalQuotaRuleUUID: "quota-rule-uuid",
			DiskLimitInKibs:       102400, // 100 MiB
		}

		jobUUID := "job-uuid-123"
		expectedJobState := "success"
		expectedJobCode := int64(0)
		expectedJobMessage := "Quota rule updated successfully"

		mockQuotaRuleModifyResponse := &storage.QuotaRuleModifyAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleModify", mock.Anything, mock.Anything).Return(mockQuotaRuleModifyResponse, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.UpdateQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		assert.Equal(tt, expectedJobCode, result.Code)
		assert.Equal(tt, expectedJobMessage, result.Message)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("failed to get ontap client")
		}

		rc := &OntapRestProvider{}
		params := &UpdateQuotaRuleParams{
			ExternalQuotaRuleUUID: "quota-rule-uuid",
			DiskLimitInKibs:       102400,
		}

		result, err := rc.UpdateQuotaRule(context.Background(), params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get ontap client")
	})

	t.Run("WhenQuotaRuleModifyFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := &UpdateQuotaRuleParams{
			ExternalQuotaRuleUUID: "quota-rule-uuid",
			DiskLimitInKibs:       102400,
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleModify", mock.Anything, mock.Anything).Return(nil, errors.New("quota rule modify failed"))

		result, err := rc.UpdateQuotaRule(context.Background(), params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "quota rule modify failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenPollJobFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := &UpdateQuotaRuleParams{
			ExternalQuotaRuleUUID: "quota-rule-uuid",
			DiskLimitInKibs:       102400,
		}

		jobUUID := "job-uuid-123"
		mockQuotaRuleModifyResponse := &storage.QuotaRuleModifyAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleModify", mock.Anything, mock.Anything).Return(mockQuotaRuleModifyResponse, nil)
		mockClient.On("Poll", jobUUID).Return(errors.New("poll job failed"))

		result, err := rc.UpdateQuotaRule(context.Background(), params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "poll job failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenJobGetFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := &UpdateQuotaRuleParams{
			ExternalQuotaRuleUUID: "quota-rule-uuid",
			DiskLimitInKibs:       102400,
		}

		jobUUID := "job-uuid-123"
		mockQuotaRuleModifyResponse := &storage.QuotaRuleModifyAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleModify", mock.Anything, mock.Anything).Return(mockQuotaRuleModifyResponse, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(nil, errors.New("job get failed"))

		result, err := rc.UpdateQuotaRule(context.Background(), params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "job get failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenJobCompletesWithFailure", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := &UpdateQuotaRuleParams{
			ExternalQuotaRuleUUID: "quota-rule-uuid",
			DiskLimitInKibs:       102400,
		}

		jobUUID := "job-uuid-123"
		expectedJobState := "failure"
		expectedJobCode := int64(123)
		expectedJobMessage := "Quota rule update failed"

		mockQuotaRuleModifyResponse := &storage.QuotaRuleModifyAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleModify", mock.Anything, mock.Anything).Return(mockQuotaRuleModifyResponse, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.UpdateQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		assert.Equal(tt, expectedJobCode, result.Code)
		assert.Equal(tt, expectedJobMessage, result.Message)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})
}

func TestGetOntapQuotaUUIDAndType(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("WhenGetOntapQuotaUUIDAndTypeFindsUserQuota", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1001"

		expectedQuotaUUID := "quota-uuid-123"
		expectedQuotaType := "user"
		userID := "1001"

		mockQuotaRulesResponse := []*ontaprestmodels.QuotaRule{
			{
				UUID: &expectedQuotaUUID,
				Type: &expectedQuotaType,
				QuotaRuleInlineUsers: []*ontaprestmodels.QuotaRuleInlineUsersInlineArrayItem{
					{
						ID: &userID,
					},
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(mockQuotaRuleCollectionResponse(mockQuotaRulesResponse), nil)

		quotaUUID, quotaTypeResp, err := rc.GetOntapQuotaUUIDAndType(context.Background(), volumeUUID, svmName, quotaType, target)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedQuotaUUID, quotaUUID)
		assert.Equal(tt, expectedQuotaType, quotaTypeResp)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapQuotaUUIDAndTypeFindsGroupQuota", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "INDIVIDUAL_GROUP_QUOTA"
		target := "2001"

		expectedQuotaUUID := "quota-uuid-456"
		expectedQuotaType := "group"
		groupID := "2001"

		mockQuotaRulesResponse := []*ontaprestmodels.QuotaRule{
			{
				UUID: &expectedQuotaUUID,
				Type: &expectedQuotaType,
				Group: &ontaprestmodels.QuotaRuleInlineGroup{
					ID: &groupID,
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(mockQuotaRuleCollectionResponse(mockQuotaRulesResponse), nil)

		quotaUUID, quotaTypeResp, err := rc.GetOntapQuotaUUIDAndType(context.Background(), volumeUUID, svmName, quotaType, target)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedQuotaUUID, quotaUUID)
		assert.Equal(tt, expectedQuotaType, quotaTypeResp)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapQuotaUUIDAndTypeNotFound", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1001"

		// Return empty list - no matching quota found
		mockQuotaRulesResponse := []*ontaprestmodels.QuotaRule{}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(mockQuotaRuleCollectionResponse(mockQuotaRulesResponse), nil)

		quotaUUID, quotaTypeResp, err := rc.GetOntapQuotaUUIDAndType(context.Background(), volumeUUID, svmName, quotaType, target)

		assert.NoError(tt, err)
		assert.Equal(tt, "", quotaUUID)
		assert.Equal(tt, "", quotaTypeResp)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetQuotaRuleCollectionFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1001"

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(nil, errors.New("quota collection get failed"))

		quotaUUID, quotaTypeResp, err := rc.GetOntapQuotaUUIDAndType(context.Background(), volumeUUID, svmName, quotaType, target)

		assert.Error(tt, err)
		assert.Equal(tt, "", quotaUUID)
		assert.Equal(tt, "", quotaTypeResp)
		assert.Contains(tt, err.Error(), "quota collection get failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("failed to get ontap client")
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1001"

		quotaUUID, quotaTypeResp, err := rc.GetOntapQuotaUUIDAndType(context.Background(), volumeUUID, svmName, quotaType, target)

		assert.Error(tt, err)
		assert.Equal(tt, "", quotaUUID)
		assert.Equal(tt, "", quotaTypeResp)
		assert.Contains(tt, err.Error(), "failed to get ontap client")
	})

	t.Run("WhenUserQuotaMatchesByName", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "testuser"

		expectedQuotaUUID := "quota-uuid-789"
		expectedQuotaType := "user"
		userName := "testuser"

		mockQuotaRulesResponse := []*ontaprestmodels.QuotaRule{
			{
				UUID: &expectedQuotaUUID,
				Type: &expectedQuotaType,
				QuotaRuleInlineUsers: []*ontaprestmodels.QuotaRuleInlineUsersInlineArrayItem{
					{
						Name: &userName,
					},
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(mockQuotaRuleCollectionResponse(mockQuotaRulesResponse), nil)

		quotaUUID, quotaTypeResp, err := rc.GetOntapQuotaUUIDAndType(context.Background(), volumeUUID, svmName, quotaType, target)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedQuotaUUID, quotaUUID)
		assert.Equal(tt, expectedQuotaType, quotaTypeResp)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGroupQuotaMatchesByName", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "INDIVIDUAL_GROUP_QUOTA"
		target := "testgroup"

		expectedQuotaUUID := "quota-uuid-101"
		expectedQuotaType := "group"
		groupName := "testgroup"

		mockQuotaRulesResponse := []*ontaprestmodels.QuotaRule{
			{
				UUID: &expectedQuotaUUID,
				Type: &expectedQuotaType,
				Group: &ontaprestmodels.QuotaRuleInlineGroup{
					Name: &groupName,
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(mockQuotaRuleCollectionResponse(mockQuotaRulesResponse), nil)

		quotaUUID, quotaTypeResp, err := rc.GetOntapQuotaUUIDAndType(context.Background(), volumeUUID, svmName, quotaType, target)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedQuotaUUID, quotaUUID)
		assert.Equal(tt, expectedQuotaType, quotaTypeResp)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenMultipleQuotaRulesExist", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1001"

		expectedQuotaUUID := "quota-uuid-match"
		expectedQuotaType := "user"
		matchingUserID := "1001"
		nonMatchingUserID := "1002"

		mockQuotaRulesResponse := []*ontaprestmodels.QuotaRule{
			{
				UUID: nillable.ToPointer("quota-uuid-other"),
				Type: &expectedQuotaType,
				QuotaRuleInlineUsers: []*ontaprestmodels.QuotaRuleInlineUsersInlineArrayItem{
					{
						ID: &nonMatchingUserID,
					},
				},
			},
			{
				UUID: &expectedQuotaUUID,
				Type: &expectedQuotaType,
				QuotaRuleInlineUsers: []*ontaprestmodels.QuotaRuleInlineUsersInlineArrayItem{
					{
						ID: &matchingUserID,
					},
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(mockQuotaRuleCollectionResponse(mockQuotaRulesResponse), nil)

		quotaUUID, quotaTypeResp, err := rc.GetOntapQuotaUUIDAndType(context.Background(), volumeUUID, svmName, quotaType, target)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedQuotaUUID, quotaUUID)
		assert.Equal(tt, expectedQuotaType, quotaTypeResp)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})
}

func TestGetQuotaRuleCollection(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("WhenGetQuotaRuleCollectionSucceeds", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		expectedQuotaRules := []*ontaprestmodels.QuotaRule{
			{
				UUID: nillable.ToPointer("quota-uuid-1"),
				Type: nillable.ToPointer("user"),
			},
			{
				UUID: nillable.ToPointer("quota-uuid-2"),
				Type: nillable.ToPointer("group"),
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(mockQuotaRuleCollectionResponse(expectedQuotaRules), nil)

		result, err := rc.GetQuotaRuleCollection(context.Background(), volumeUUID, svmName)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 2)
		assert.Equal(tt, "quota-uuid-1", result[0].UUID)
		assert.Equal(tt, "user", result[0].QuotaType)
		assert.Equal(tt, "quota-uuid-2", result[1].UUID)
		assert.Equal(tt, "group", result[1].QuotaType)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, customerrors.New("failed to get ontap client")
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		result, err := rc.GetQuotaRuleCollection(context.Background(), volumeUUID, svmName)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get ontap client")
	})

	t.Run("WhenQuotaRuleCollectionGetFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(nil, customerrors.New("quota collection get failed"))

		result, err := rc.GetQuotaRuleCollection(context.Background(), volumeUUID, svmName)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "quota collection get failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenQuotaRuleCollectionIsEmpty", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		emptyQuotaRules := []*ontaprestmodels.QuotaRule{}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCollectionGet", mock.Anything, mock.Anything).Return(mockQuotaRuleCollectionResponse(emptyQuotaRules), nil)

		result, err := rc.GetQuotaRuleCollection(context.Background(), volumeUUID, svmName)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})
}

func TestGetQuotaTypeForOntap(t *testing.T) {
	t.Run("WhenIndividualUserQuota_ReturnsUser", func(tt *testing.T) {
		result := GetQuotaTypeForOntap(IndividualUserQuota)
		assert.Equal(tt, QuotaRuleTypeUser, result)
	})

	t.Run("WhenDefaultUserQuota_ReturnsUser", func(tt *testing.T) {
		result := GetQuotaTypeForOntap(DefaultUserQuota)
		assert.Equal(tt, QuotaRuleTypeUser, result)
	})

	t.Run("WhenIndividualGroupQuota_ReturnsGroup", func(tt *testing.T) {
		result := GetQuotaTypeForOntap(IndividualGroupQuota)
		assert.Equal(tt, QuotaRuleTypeGroup, result)
	})

	t.Run("WhenDefaultGroupQuota_ReturnsGroup", func(tt *testing.T) {
		result := GetQuotaTypeForOntap(DefaultGroupQuota)
		assert.Equal(tt, QuotaRuleTypeGroup, result)
	})

	t.Run("WhenUnknownQuotaType_ReturnsEmpty", func(tt *testing.T) {
		result := GetQuotaTypeForOntap("UNKNOWN_TYPE")
		assert.Equal(tt, "", result)
	})

	t.Run("WhenEmptyQuotaType_ReturnsEmpty", func(tt *testing.T) {
		result := GetQuotaTypeForOntap("")
		assert.Equal(tt, "", result)
	})
}

func TestGetDefaultQuotaRule(t *testing.T) {
	t.Run("WhenGetDefaultQuotaRule_ReturnsNotFound", func(tt *testing.T) {
		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "user"

		result, err := rc.GetDefaultQuotaRule(context.Background(), volumeUUID, svmName, quotaType)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, customerrors.IsNotFoundErr(err))
		assert.Contains(tt, err.Error(), "Default quota rule not found")
	})

	t.Run("WhenGetDefaultQuotaRuleWithGroupType_ReturnsNotFound", func(tt *testing.T) {
		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"
		quotaType := "group"

		result, err := rc.GetDefaultQuotaRule(context.Background(), volumeUUID, svmName, quotaType)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, customerrors.IsNotFoundErr(err))
	})
}

func TestCreateQuotaRule(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("WhenCreateQuotaRuleSucceeds_IndividualUser", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := CreateQuotaRuleParams{
			VolumeUUID:     "volume-uuid",
			SVMName:        "svm-name",
			QuotaTarget:    "1001",
			QuotaType:      IndividualUserQuota,
			DiskLimitInKib: 102400,
			RQuota:         false,
		}

		jobUUID := "job-uuid-123"
		expectedJobState := "success"
		expectedJobCode := int64(0)
		expectedJobMessage := "Quota rule created successfully"

		mockQuotaRuleCreateResponse := &storage.QuotaRuleCreateAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleCreate", mock.Anything, mock.Anything).Return(mockQuotaRuleCreateResponse, nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil).Once()
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.CreateQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		assert.Equal(tt, expectedJobCode, result.Code)
		assert.Equal(tt, expectedJobMessage, result.Message)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenCreateQuotaRuleSucceeds_DefaultUser", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := CreateQuotaRuleParams{
			VolumeUUID:     "volume-uuid",
			SVMName:        "svm-name",
			QuotaTarget:    "",
			QuotaType:      DefaultUserQuota,
			DiskLimitInKib: 204800,
			RQuota:         true,
		}

		jobUUID := "job-uuid-456"
		expectedJobState := "success"
		expectedJobCode := int64(0)
		expectedJobMessage := "Quota rule created"

		mockQuotaRuleCreateResponse := &storage.QuotaRuleCreateAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleCreate", mock.Anything, mock.Anything).Return(mockQuotaRuleCreateResponse, nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil).Once()
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.CreateQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenCreateQuotaRuleSucceeds_IndividualGroup", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := CreateQuotaRuleParams{
			VolumeUUID:     "volume-uuid",
			SVMName:        "svm-name",
			QuotaTarget:    "2001",
			QuotaType:      IndividualGroupQuota,
			DiskLimitInKib: 307200,
			RQuota:         false,
		}

		jobUUID := "job-uuid-789"
		expectedJobState := "success"
		expectedJobCode := int64(0)

		mockQuotaRuleCreateResponse := &storage.QuotaRuleCreateAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State: &expectedJobState,
				Code:  &expectedJobCode,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleCreate", mock.Anything, mock.Anything).Return(mockQuotaRuleCreateResponse, nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil).Once()
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.CreateQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("failed to get ontap client")
		}

		rc := &OntapRestProvider{}
		params := CreateQuotaRuleParams{
			VolumeUUID:     "volume-uuid",
			SVMName:        "svm-name",
			QuotaTarget:    "1001",
			QuotaType:      IndividualUserQuota,
			DiskLimitInKib: 102400,
		}

		result, err := rc.CreateQuotaRule(context.Background(), params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get ontap client")
	})

	t.Run("WhenQuotaRuleCreateFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := CreateQuotaRuleParams{
			VolumeUUID:     "volume-uuid",
			SVMName:        "svm-name",
			QuotaTarget:    "1001",
			QuotaType:      IndividualUserQuota,
			DiskLimitInKib: 102400,
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleCreate", mock.Anything, mock.Anything).Return(nil, errors.New("quota rule create failed"))

		result, err := rc.CreateQuotaRule(context.Background(), params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "quota rule create failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenFetchDetailsFromJobFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := CreateQuotaRuleParams{
			VolumeUUID:     "volume-uuid",
			SVMName:        "svm-name",
			QuotaTarget:    "1001",
			QuotaType:      IndividualUserQuota,
			DiskLimitInKib: 102400,
		}

		jobUUID := "job-uuid-123"
		mockQuotaRuleCreateResponse := &storage.QuotaRuleCreateAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleCreate", mock.Anything, mock.Anything).Return(mockQuotaRuleCreateResponse, nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(nil, errors.New("job get failed"))

		result, err := rc.CreateQuotaRule(context.Background(), params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "job get failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenJobCompletesWithFailure", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		params := CreateQuotaRuleParams{
			VolumeUUID:     "volume-uuid",
			SVMName:        "svm-name",
			QuotaTarget:    "1001",
			QuotaType:      IndividualUserQuota,
			DiskLimitInKib: 102400,
		}

		jobUUID := "job-uuid-123"
		expectedJobState := "failure"
		expectedJobCode := int64(123)
		expectedJobMessage := "Quota rule creation failed"

		mockQuotaRuleCreateResponse := &storage.QuotaRuleCreateAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		// First call returns job in progress, second call returns failure
		jobInProgress := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State: nillable.ToPointer("running"),
			},
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleCreate", mock.Anything, mock.Anything).Return(mockQuotaRuleCreateResponse, nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(jobInProgress, nil).Once()
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.CreateQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		assert.Equal(tt, expectedJobCode, result.Code)
		assert.Equal(tt, expectedJobMessage, result.Message)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})
}

func TestQuotaEnableDisable(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("WhenQuotaEnableDisableSucceeds_Enable", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		jobUUID := "job-uuid-123"
		expectedJobState := "success"
		expectedJobCode := int64(0)
		expectedJobMessage := "Quota enabled successfully"

		mockJobAccepted := &ontaprest.JobAccepted{
			JobUUID: jobUUID,
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJobAccepted, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.QuotaEnableDisable(context.Background(), volumeUUID, svmName, true)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		assert.Equal(tt, expectedJobCode, result.Code)
		assert.Equal(tt, expectedJobMessage, result.Message)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenQuotaEnableDisableSucceeds_Disable", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		jobUUID := "job-uuid-456"
		expectedJobState := "success"
		expectedJobCode := int64(0)
		expectedJobMessage := "Quota disabled successfully"

		mockJobAccepted := &ontaprest.JobAccepted{
			JobUUID: jobUUID,
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJobAccepted, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.QuotaEnableDisable(context.Background(), volumeUUID, svmName, false)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenQuotaEnableDisableSucceeds_ImmediateSuccess", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("VolumeModify", mock.Anything).Return(true, nil, nil)

		result, err := rc.QuotaEnableDisable(context.Background(), volumeUUID, svmName, true)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, JobRespSuccess, result.State)
		assert.Equal(tt, int64(http.StatusOK), result.Code)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("failed to get ontap client")
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		result, err := rc.QuotaEnableDisable(context.Background(), volumeUUID, svmName, true)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get ontap client")
	})

	t.Run("WhenVolumeModifyFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New("volume modify failed"))

		result, err := rc.QuotaEnableDisable(context.Background(), volumeUUID, svmName, true)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "volume modify failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenPollJobFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		jobUUID := "job-uuid-123"
		mockJobAccepted := &ontaprest.JobAccepted{
			JobUUID: jobUUID,
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJobAccepted, nil)
		mockClient.On("Poll", jobUUID).Return(errors.New("poll job failed"))

		result, err := rc.QuotaEnableDisable(context.Background(), volumeUUID, svmName, true)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "poll job failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenJobGetFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		jobUUID := "job-uuid-123"
		mockJobAccepted := &ontaprest.JobAccepted{
			JobUUID: jobUUID,
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJobAccepted, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(nil, errors.New("job get failed"))

		result, err := rc.QuotaEnableDisable(context.Background(), volumeUUID, svmName, true)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "job get failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenJobCompletesWithFailure", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		volumeUUID := "volume-uuid"
		svmName := "svm-name"

		jobUUID := "job-uuid-123"
		expectedJobState := "failure"
		expectedJobCode := int64(123)
		expectedJobMessage := "Quota enable failed"

		mockJobAccepted := &ontaprest.JobAccepted{
			JobUUID: jobUUID,
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJobAccepted, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.QuotaEnableDisable(context.Background(), volumeUUID, svmName, true)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		assert.Equal(tt, expectedJobCode, result.Code)
		assert.Equal(tt, expectedJobMessage, result.Message)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})
}

func TestDeleteQuotaRule(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("WhenDeleteQuotaRuleSucceeds", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		quotaUUID := "quota-rule-uuid"

		jobUUID := "job-uuid-123"
		expectedJobState := "success"
		expectedJobCode := int64(0)
		expectedJobMessage := "Quota rule deleted successfully"

		mockQuotaRuleDeleteResponse := &storage.QuotaRuleDeleteAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleDelete", mock.Anything, mock.Anything).Return(mockQuotaRuleDeleteResponse, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.DeleteQuotaRule(context.Background(), quotaUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		assert.Equal(tt, expectedJobCode, result.Code)
		assert.Equal(tt, expectedJobMessage, result.Message)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("failed to get ontap client")
		}

		rc := &OntapRestProvider{}
		quotaUUID := "quota-rule-uuid"

		result, err := rc.DeleteQuotaRule(context.Background(), quotaUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get ontap client")
	})

	t.Run("WhenQuotaRuleDeleteFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		quotaUUID := "quota-rule-uuid"

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleDelete", mock.Anything, mock.Anything).Return(nil, errors.New("quota rule delete failed"))

		result, err := rc.DeleteQuotaRule(context.Background(), quotaUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "quota rule delete failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenPollJobFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		quotaUUID := "quota-rule-uuid"

		jobUUID := "job-uuid-123"
		mockQuotaRuleDeleteResponse := &storage.QuotaRuleDeleteAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockStorage.On("QuotaRuleDelete", mock.Anything, mock.Anything).Return(mockQuotaRuleDeleteResponse, nil)
		mockClient.On("Poll", jobUUID).Return(errors.New("poll job failed"))

		result, err := rc.DeleteQuotaRule(context.Background(), quotaUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "poll job failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenJobGetFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		quotaUUID := "quota-rule-uuid"

		jobUUID := "job-uuid-123"
		mockQuotaRuleDeleteResponse := &storage.QuotaRuleDeleteAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleDelete", mock.Anything, mock.Anything).Return(mockQuotaRuleDeleteResponse, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(nil, errors.New("job get failed"))

		result, err := rc.DeleteQuotaRule(context.Background(), quotaUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "job get failed")
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})

	t.Run("WhenJobCompletesWithFailure", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}
		quotaUUID := "quota-rule-uuid"

		jobUUID := "job-uuid-123"
		expectedJobState := "failure"
		expectedJobCode := int64(123)
		expectedJobMessage := "Quota rule delete failed"

		mockQuotaRuleDeleteResponse := &storage.QuotaRuleDeleteAccepted{
			Payload: &ontaprestmodels.QuotaRuleJobLinkResponse{
				Job: &ontaprestmodels.JobLink{
					UUID: func() *strfmt.UUID { u := strfmt.UUID(jobUUID); return &u }(),
				},
			},
		}

		mockJobResponse := &ontaprestcluster.JobGetOK{
			Payload: &ontaprestmodels.Job{
				State:   &expectedJobState,
				Code:    &expectedJobCode,
				Message: &expectedJobMessage,
			},
		}

		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)
		mockStorage.On("QuotaRuleDelete", mock.Anything, mock.Anything).Return(mockQuotaRuleDeleteResponse, nil)
		mockClient.On("Poll", jobUUID).Return(nil)
		mockCluster.On("JobGet", mock.Anything, mock.Anything).Return(mockJobResponse, nil)

		result, err := rc.DeleteQuotaRule(context.Background(), quotaUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedJobState, result.State)
		assert.Equal(tt, expectedJobCode, result.Code)
		assert.Equal(tt, expectedJobMessage, result.Message)
		mockClient.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
		mockCluster.AssertExpectations(tt)
	})
}
