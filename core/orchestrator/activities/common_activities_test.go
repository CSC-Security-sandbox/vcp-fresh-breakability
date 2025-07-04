package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func TestUpdateJobStatus_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := CommonActivities{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		State:        models.JobStateSuccess,
		TrackingID:   1003,
		ErrorDetails: []byte("Requested Resource is not found"),
	}

	mockStorage.On("UpdateJob", ctx, job.UUID, job.State, job.TrackingID, job.ErrorDetails).Return(nil)

	// Act
	err := activity.UpdateJobStatus(ctx, job)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateJobStatus(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := CommonActivities{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		State:        models.JobStateSuccess,
		TrackingID:   1003,
		ErrorDetails: []byte("Requested Resource is not found"),
	}

	mockStorage.On("UpdateJob", ctx, job.UUID, job.State, job.TrackingID, job.ErrorDetails).Return(nil)

	// Act
	err := activity.UpdateJobStatus(ctx, job)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetNode(t *testing.T) {
	t.Run("GetNode_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)
		expectedNode := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-node",
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(expectedNode, nil)

		node, err := activity.GetNode(ctx, poolId)

		// Assert
		assert.NoError(tt, err)
		assert.Equal(tt, expectedNode, node)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetNode_Error", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(nil, gorm.ErrInvalidDB)

		node, err := activity.GetNode(ctx, poolId)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, node)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetNode_NotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return([]*datamodel.Node{}, nil)

		node, err := activity.GetNode(ctx, poolId)

		// Assert
		assert.Error(tt, err)
		assert.Equal(tt, "no node found for the pool", err.Error())
		assert.Nil(tt, node)
		mockStorage.AssertExpectations(tt)
	})
}

func TestCreateJob_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := CommonActivities{SE: mockStorage}
	ctx := context.Background()
	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		State:     "PROCESSING",
	}

	mockStorage.On("CreateJob", ctx, job).Return(job, nil)

	result, err := activity.CreateJob(ctx, job)

	assert.NoError(t, err)
	assert.Equal(t, job, result)
	mockStorage.AssertExpectations(t)
}

func Test_GetProviderByNode(t *testing.T) {
	origAuthType := common.AuthType
	originalGetPasswordFromCacheOrSecretManager := GetPasswordFromCacheOrSecretManager
	defer func() {
		common.AuthType = origAuthType
		GetPasswordFromCacheOrSecretManager = originalGetPasswordFromCacheOrSecretManager
	}()

	ctx := context.Background()
	node := &models2.Node{
		Username:          "user",
		Password:          "pass",
		SecretID:          "secret-id",
		EndpointAddress:   "1.2.3.4",
		EndpointAddresses: []string{},
	}

	t.Run("Password from Secret Manager", func(t *testing.T) {
		common.AuthType = common.USERNAME_PWD_SEC_MGR
		// Mock GetPasswordFromCacheOrSecretManager
		GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) string {
			return "secret-pass"
		}
		node.EndpointAddresses = []string{}
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("Password from Node", func(t *testing.T) {
		common.AuthType = common.USER_CERTIFICATE
		node.EndpointAddresses = []string{}
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("No Endpoint Address", func(t *testing.T) {
		common.AuthType = common.USER_CERTIFICATE
		node.EndpointAddresses = []string{}
		node.EndpointAddress = ""
		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Nil(t, provider)
	})

	t.Run("Already has EndpointAddresses", func(t *testing.T) {
		common.AuthType = common.USER_CERTIFICATE
		node.EndpointAddresses = []string{"5.6.7.8"}
		node.EndpointAddress = ""
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})
}

// Unit test for GetOntapJob
func TestCommonActivities_GetOntapJob(t *testing.T) {
	t.Run("Get Ontap Job", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer mockProvider.AssertExpectations(t)

		// Save and restore original GetProviderByNode
		origGetProviderByNode := GetProviderByNode
		defer func() { GetProviderByNode = origGetProviderByNode }()

		ctx := context.Background()
		jobUUID := "test-job-uuid"
		node := &models2.Node{}

		activity := CommonActivities{}

		expectedJob := &vsa.OntapJob{}

		// Success case
		GetProviderByNode = func(ctx context.Context, n *models2.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockProvider.On("JobGet", mock.Anything).Return(expectedJob, nil)

		job, err := activity.GetOntapJob(ctx, jobUUID, node)
		assert.NoError(t, err)
		assert.Equal(t, expectedJob, job)
	})
	t.Run("Get Ontap Job GetProviderByNode Error", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer mockProvider.AssertExpectations(t)

		// Save and restore original GetProviderByNode
		origGetProviderByNode := GetProviderByNode
		defer func() { GetProviderByNode = origGetProviderByNode }()

		ctx := context.Background()
		jobUUID := "test-job-uuid"
		node := &models2.Node{}

		activity := CommonActivities{}

		var job *vsa.OntapJob
		var err error

		// Error from GetProviderByNode
		GetProviderByNode = func(ctx context.Context, n *models2.Node) (vsa.Provider, error) {
			return nil, errors.New("mock error")
		}
		job, err = activity.GetOntapJob(ctx, jobUUID, node)
		assert.Error(t, err)
		assert.Nil(t, job)
	})

	t.Run("Get Ontap Job Error", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer mockProvider.AssertExpectations(t)

		// Save and restore original GetProviderByNode
		origGetProviderByNode := GetProviderByNode
		defer func() { GetProviderByNode = origGetProviderByNode }()
		node := &models2.Node{}

		activity := CommonActivities{}

		GetProviderByNode = func(ctx context.Context, n *models2.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockProvider.On("JobGet", mock.Anything).Return(nil, errors.New("jobget error"))
		job, err := activity.GetOntapJob(context.Background(), "test-job-uuid", node)
		assert.Error(t, err)
		assert.Nil(t, job)
	})
}
