package activities

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	dbUtils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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
		ErrorDetails: "Requested Resource is not found",
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
		ErrorDetails: "Requested Resource is not found",
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
		assert.Equal(tt, "Node not found for the pool", vsaerrors.ExtractCustomError(err).OriginalErr.Error())
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

// Unit test for GetOntapJob
func TestCommonActivities_GetOntapJob(t *testing.T) {
	t.Run("Get Ontap Job", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer mockProvider.AssertExpectations(t)

		// Save and restore original GetProviderByNode
		origGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = origGetProviderByNode }()

		ctx := context.Background()
		jobUUID := "test-job-uuid"
		node := &models2.Node{}

		activity := CommonActivities{}

		expectedJob := &vsa.OntapJob{}

		// Success case
		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *models2.Node) (vsa.Provider, error) {
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
		origGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = origGetProviderByNode }()

		ctx := context.Background()
		jobUUID := "test-job-uuid"
		node := &models2.Node{}

		activity := CommonActivities{}

		var job *vsa.OntapJob
		var err error

		// Error from GetProviderByNode
		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *models2.Node) (vsa.Provider, error) {
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
		origGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = origGetProviderByNode }()
		node := &models2.Node{}

		activity := CommonActivities{}

		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *models2.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockProvider.On("JobGet", mock.Anything).Return(nil, errors.New("jobget error"))
		job, err := activity.GetOntapJob(context.Background(), "test-job-uuid", node)
		assert.Error(t, err)
		assert.Nil(t, job)
	})
}

func TestGetToken(t *testing.T) {
	t.Run("WhenGenerateFailed", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := CommonActivities{SE: mockStorage}
		getSignedJwtToken = func(accountName string) (string, error) {
			return "", gorm.ErrInvalidDB
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()

		ctx := context.Background()
		token, err := activity.GetAuthJWTToken(ctx, "test-account")
		assert.Error(t, err)
		assert.NotNil(t, token)
	})
	t.Run("WhenSuccess", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := CommonActivities{SE: mockStorage}
		getSignedJwtToken = func(accountName string) (string, error) {
			return "token", nil
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()

		ctx := context.Background()
		token, err := activity.GetAuthJWTToken(ctx, "test-account")
		assert.NoError(t, err)
		assert.NotNil(t, token)
	})
}

func Test_getSubnetToBeUsed(t *testing.T) {
	ctx := context.TODO()
	mockLogger := util.GetLogger(ctx)

	customerProjectNumber := "cust-123"
	tenantProjectNumber := "tenant-456"
	snHost := "host"
	tenantProjectRegion := "region"
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 2},
		Name:      "test_account",
	}

	t.Run("ListSubnetworks error", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)

		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&[]hyperscaler_models.Subnet{}, errors.New("list error"))
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, false)
		assert.Nil(t, subnet)
		assert.Error(t, err)
		mgs.AssertExpectations(t)
	})

	t.Run("No subnets found", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		mgs := hyperscaler2.NewMockGoogleServices(t)
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&[]hyperscaler_models.Subnet{}, nil)
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, false)
		assert.Nil(t, subnet)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})

	t.Run("GetAccount error", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)

		subnets := []hyperscaler_models.Subnet{{Name: "vsa-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(&datamodel.Account{}, errors.New("account error"))
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, false)
		assert.Nil(t, subnet)
		assert.Error(t, err)
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})
	t.Run("Subnet found and reusable", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mgs := hyperscaler2.NewMockGoogleServices(t)
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)

		origIsSubnetReusable := isSubnetReusable
		defer func() { isSubnetReusable = origIsSubnetReusable }()
		isSubnetReusable = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (bool, error) {
			return true, nil
		}
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, false)

		assert.NotNil(t, subnet)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Subnet found but not reusable", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)
		origIsSubnetReusable := isSubnetReusable
		defer func() {
			isSubnetReusable = origIsSubnetReusable
		}()
		isSubnetReusable = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (bool, error) {
			return false, nil
		}
		defer func() {}()
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, false)
		assert.Nil(t, subnet)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})

	t.Run("Subnet found but _isSubnetReusable returns error", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("GetContext").Return(ctx)
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-tenant-456"}}
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)
		origIsSubnetReusable := isSubnetReusable
		defer func() {
			isSubnetReusable = origIsSubnetReusable
		}()
		isSubnetReusable = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (bool, error) {
			return false, errors.New("reuse error")
		}
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, false)
		assert.Nil(t, subnet)
		assert.Error(t, err, "reuse error")
		mgs.AssertExpectations(t)
	})

	// Test cases for large capacity pools (largeCapacity = true)
	t.Run("ListSubnetworks error - large capacity", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)

		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&[]hyperscaler_models.Subnet{}, errors.New("list error"))
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)
		assert.Nil(t, subnet)
		assert.Error(t, err)
		mgs.AssertExpectations(t)
	})

	t.Run("No subnets found - large capacity", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		mgs := hyperscaler2.NewMockGoogleServices(t)
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&[]hyperscaler_models.Subnet{}, nil)
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)
		assert.Nil(t, subnet)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})

	t.Run("GetAccount error - large capacity", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)

		subnets := []hyperscaler_models.Subnet{{Name: "vsa-lv-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(&datamodel.Account{}, errors.New("account error"))
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)
		assert.Nil(t, subnet)
		assert.Error(t, err)
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Large capacity subnet found and reusable", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mgs := hyperscaler2.NewMockGoogleServices(t)
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-lv-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)
		// Mock ListPools to return empty list (no pools associated with subnet)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

		origIsSubnetReusable := isSubnetReusable
		defer func() { isSubnetReusable = origIsSubnetReusable }()
		isSubnetReusable = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (bool, error) {
			return true, nil
		}
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)

		assert.NotNil(t, subnet)
		assert.NoError(t, err)
		assert.Equal(t, "vsa-lv-tenant-456", subnet.Name, "Should find large capacity subnet with vsa-lv- prefix")
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Large capacity subnet found but not reusable", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-lv-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)
		// Mock ListPools to return empty list (no pools associated with subnet)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
		origIsSubnetReusable := isSubnetReusable
		defer func() {
			isSubnetReusable = origIsSubnetReusable
		}()
		isSubnetReusable = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (bool, error) {
			return false, nil
		}
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)
		assert.Nil(t, subnet)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})

	t.Run("Large capacity subnet found but _isSubnetReusable returns error", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("GetContext").Return(ctx)
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-lv-tenant-456"}}
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)
		// Mock ListPools to return empty list (no pools associated with subnet)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
		origIsSubnetReusable := isSubnetReusable
		defer func() {
			isSubnetReusable = origIsSubnetReusable
		}()
		isSubnetReusable = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (bool, error) {
			return false, errors.New("reuse error")
		}
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)
		assert.Nil(t, subnet)
		assert.Error(t, err, "reuse error")
		mgs.AssertExpectations(t)
	})

	t.Run("Standard pool should not find large capacity subnet", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mgs := hyperscaler2.NewMockGoogleServices(t)
		// Only large capacity subnet available
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-lv-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)

		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, false)

		assert.Nil(t, subnet, "Standard pool should not find large capacity subnet")
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Large capacity pool should not find standard subnet", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mgs := hyperscaler2.NewMockGoogleServices(t)
		// Only standard subnet available
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)

		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)

		assert.Nil(t, subnet, "Large capacity pool should not find standard subnet")
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Mixed subnets - large capacity pool finds correct subnet", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mgs := hyperscaler2.NewMockGoogleServices(t)
		// Mix of standard and large capacity subnets
		subnets := []hyperscaler_models.Subnet{
			{Name: "vsa-tenant-456-12345"},
			{Name: "vsa-lv-tenant-456-12345"}, // Should match this one
			{Name: "vsa-tenant-456-67890"},
		}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)
		// Mock ListPools to return empty list (no pools associated with subnet)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

		origIsSubnetReusable := isSubnetReusable
		defer func() { isSubnetReusable = origIsSubnetReusable }()
		isSubnetReusable = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (bool, error) {
			return true, nil
		}
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)

		assert.NotNil(t, subnet)
		assert.NoError(t, err)
		assert.Equal(t, "vsa-lv-tenant-456-12345", subnet.Name, "Should find the large capacity subnet")
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Large capacity subnet found but has pools associated - should skip", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mgs := hyperscaler2.NewMockGoogleServices(t)
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-lv-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)
		// Mock ListPools to return pools (subnet has pools associated with it)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)

		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)

		assert.Nil(t, subnet)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Large capacity subnet found but ListPools returns error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mgs := hyperscaler2.NewMockGoogleServices(t)
		subnets := []hyperscaler_models.Subnet{{Name: "vsa-lv-tenant-456"}}
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&subnets, nil)
		mgs.On("GetContext").Return(ctx)
		mockStorage.On("GetAccount", ctx, customerProjectNumber).Return(account, nil)
		// Mock ListPools to return error
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return(nil, errors.New("list pools error"))

		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion, true)

		assert.Nil(t, subnet)
		assert.Error(t, err)
		mgs.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})
}

// Unit tests for _isSubnetReusable
func Test_isSubnetReusable(t *testing.T) {
	ctx := context.TODO()
	mockStorage := database.NewMockStorage(t)
	subnet := hyperscaler_models.Subnet{Name: "test-subnet"}
	accountId := "1"
	poolNetwork := "test-network"

	t.Run("returns true when enough IPs", func(t *testing.T) {
		origFindEmptySubnet := findEmptySubnet
		findEmptySubnet = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (int, error) {
			return totalIPPerHAPair, nil
		}
		defer func() { findEmptySubnet = origFindEmptySubnet }()

		ok, err := _isSubnetReusable(ctx, mockStorage, subnet, accountId, poolNetwork)
		assert.True(t, ok)
		assert.NoError(t, err)
	})

	t.Run("returns false when not enough IPs", func(t *testing.T) {
		origFindEmptySubnet := findEmptySubnet
		findEmptySubnet = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (int, error) {
			return totalIPPerHAPair - 1, nil
		}
		defer func() { findEmptySubnet = origFindEmptySubnet }()

		ok, err := _isSubnetReusable(ctx, mockStorage, subnet, accountId, poolNetwork)
		assert.False(t, ok)
		assert.NoError(t, err)
	})

	t.Run("returns error when _findEmptySubnet errors", func(t *testing.T) {
		origFindEmptySubnet := findEmptySubnet
		findEmptySubnet = func(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (int, error) {
			return 0, errors.New("some error")
		}
		defer func() { findEmptySubnet = origFindEmptySubnet }()

		ok, err := _isSubnetReusable(ctx, mockStorage, subnet, accountId, poolNetwork)
		assert.False(t, ok)
		assert.Error(t, err)
	})
}

func Test_findEmptySubnet(t *testing.T) {
	ctx := context.TODO()
	mockStorage := database.NewMockStorage(t)
	subnet := hyperscaler_models.Subnet{Name: "test-subnet", IpCidrRange: "10.0.0.0/29"}
	accountId := "1"
	poolNetwork := "test-network"

	t.Run("returns error when IpCidrRange is empty", func(t *testing.T) {
		subnetEmpty := hyperscaler_models.Subnet{Name: "test-subnet", IpCidrRange: ""}
		freeIPs, err := _findEmptySubnet(ctx, mockStorage, subnetEmpty, accountId, poolNetwork)
		assert.Equal(t, 0, freeIPs)
		assert.Error(t, err)
	})

	t.Run("returns error when _getPoolsBySubnetwork errors", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		origGetPoolsBySubnetwork := getPoolsBySubnetwork
		getPoolsBySubnetwork = func(ctx context.Context, se database.Storage, accountID, subnetworkName, poolNetwork string) ([]*datamodel.PoolView, error) {
			return nil, errors.New("db error")
		}
		defer func() { getPoolsBySubnetwork = origGetPoolsBySubnetwork }()
		freeIPs, err := _findEmptySubnet(ctx, mockStorage, subnet, accountId, poolNetwork)
		assert.Equal(t, 0, freeIPs)
		assert.Error(t, err)
	})

	t.Run("returns error when _getIPsInSubnet errors", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		origGetPoolsBySubnetwork := getPoolsBySubnetwork
		origGetIPsInSubnet := getIPsInSubnet
		getPoolsBySubnetwork = func(ctx context.Context, se database.Storage, accountID, subnetworkName, poolNetwork string) ([]*datamodel.PoolView, error) {
			return []*datamodel.PoolView{}, nil
		}
		getIPsInSubnet = func(ipCidrRange string) (int, error) {
			return 0, errors.New("cidr error")
		}
		defer func() {
			getPoolsBySubnetwork = origGetPoolsBySubnetwork
			getIPsInSubnet = origGetIPsInSubnet
		}()
		freeIPs, err := _findEmptySubnet(ctx, mockStorage, subnet, accountId, poolNetwork)
		assert.Equal(t, 0, freeIPs)
		assert.Error(t, err)
	})

	t.Run("returns correct free IPs", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		origGetPoolsBySubnetwork := getPoolsBySubnetwork
		origGetIPsInSubnet := getIPsInSubnet
		getPoolsBySubnetwork = func(ctx context.Context, se database.Storage, accountID, subnetworkName, poolNetwork string) ([]*datamodel.PoolView, error) {
			return []*datamodel.PoolView{{}, {}}, nil // 2 pools
		}
		getIPsInSubnet = func(ipCidrRange string) (int, error) {
			return 16, nil
		}
		defer func() {
			getPoolsBySubnetwork = origGetPoolsBySubnetwork
			getIPsInSubnet = origGetIPsInSubnet
		}()
		freeIPs, err := _findEmptySubnet(ctx, mockStorage, subnet, accountId, poolNetwork)
		expected := 16 - 4 - 2*totalIPPerHAPair
		assert.Equal(t, expected, freeIPs)
		assert.NoError(t, err)
	})
}

// Unit test for _getIPsInSubnet
func Test_getIPsInSubnet(t *testing.T) {
	tests := []struct {
		name        string
		cidr        string
		want        int
		expectError error
	}{
		{"Valid /29", "10.0.0.0/29", 8, nil},
		{"Valid /24", "192.168.1.0/24", 256, nil},
		{"Valid /32", "1.2.3.4/32", 1, nil},
		{"Valid /0", "0.0.0.0/0", 1 << 32, nil},
		{"Invalid CIDR (non-numeric)", "10.0.0.0/abc", 0, fmt.Errorf("Invalid CIDR (subnet mask cannot be string)")},
		{"Invalid CIDR (missing slash)", "10.0.0.0", 0, fmt.Errorf("Invalid CIDR (missing subnet mask)")},
		{"Invalid CIDR (too large)", "10.0.0.0/33", 0, fmt.Errorf("Invalid CIDR (too large)")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := _getIPsInSubnet(tt.cidr)
			if tt.expectError != nil {
				if err == nil {
					t.Errorf("expected error for input %q, got nil", tt.cidr)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for input %q: %v", tt.cidr, err)
				}
				if got != tt.want {
					t.Errorf("expected %d, got %d for input %q", tt.want, got, tt.cidr)
				}
			}
		})
	}
}

func Test_getPoolsBySubnetwork(t *testing.T) {
	ctx := context.TODO()
	accountID := "1"
	subnetworkName := "test-subnet"
	poolNetwork := "test-network"
	expectedPools := []*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, Name: "pool1"}},
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 2}, Name: "pool2"}},
	}

	t.Run("returns pools when found", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("ListPools", ctx, mock.Anything).Return(expectedPools, nil)
		pools, err := _getPoolsBySubnetwork(ctx, mockStorage, accountID, subnetworkName, poolNetwork)
		assert.NoError(t, err)
		assert.Equal(t, expectedPools, pools)
		mockStorage.AssertExpectations(t)
	})

	t.Run("returns error when ListPools fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("db error"))
		pools, err := _getPoolsBySubnetwork(ctx, mockStorage, accountID, subnetworkName, poolNetwork)
		assert.Error(t, err, "db error")
		assert.Nil(t, pools)
		mockStorage.AssertExpectations(t)
	})

	t.Run("handles empty poolNetwork", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("ListPools", ctx, mock.Anything).Return(expectedPools, nil)
		pools, err := _getPoolsBySubnetwork(ctx, mockStorage, accountID, subnetworkName, "")
		assert.NoError(t, err)
		assert.Equal(t, expectedPools, pools)
		mockStorage.AssertExpectations(t)
	})
}

func TestCommonActivities_GetJob(t *testing.T) {
	ctx := context.Background()
	jobUUID := "test-job-uuid"
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: jobUUID},
		State:     "PROCESSING",
	}

	t.Run("success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := CommonActivities{SE: mockStorage}
		mockStorage.On("GetJob", ctx, jobUUID).Return(expectedJob, nil)
		job, err := activity.GetJob(ctx, jobUUID)
		assert.NoError(t, err)
		assert.Equal(t, expectedJob, job)
		mockStorage.AssertExpectations(t)
	})

	t.Run("error from storage", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := CommonActivities{SE: mockStorage}
		mockStorage.On("GetJob", ctx, jobUUID).Return(nil, errors.New("db error"))
		job, err := activity.GetJob(ctx, jobUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		mockStorage.AssertExpectations(t)
	})

	t.Run("job not found", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := CommonActivities{SE: mockStorage}
		mockStorage.On("GetJob", ctx, jobUUID).Return(nil, nil)
		job, err := activity.GetJob(ctx, jobUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		mockStorage.AssertExpectations(t)
	})
}

func TestDescribeRemoteJob(t *testing.T) {
	t.Run("DescribeJob_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.CreateReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "test-location-id",
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		correlationID := "test-correlation-id"
		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.DestinationLocationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)

		err := DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.DestinationLocationID, &correlationID)

		assert.NoError(tt, err)
	})

	t.Run("DescribeJob_Error", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.CreateReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "test-location-id",
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		correlationID := "test-correlation-id"
		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.DestinationLocationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(nil, errors.New("some error"))
		err := DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.DestinationLocationID, &correlationID)

		assert.Error(tt, err)
	})

	t.Run("DescribeJob_NotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.CreateReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "test-location-id",
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		correlationID := "test-correlation-id"
		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.DestinationLocationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}
		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.DestinationLocationID, &correlationID)

		assert.Error(tt, err)
	})
	t.Run("DescribeJob_FinishedWithError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		result := &replication.CreateReplicationResult{
			JobId:            nillable.GetStringPtr("test-job-id"),
			DstProjectNumber: nillable.GetStringPtr("test-project-number"),
			Event: &replication.CreateReplicationEvent{
				DestinationLocationID: "test-location-id",
			},
			DstBasePath: nillable.GetStringPtr("base-path"),
			DstJwtToken: nillable.GetStringPtr("jwt-token"),
		}

		correlationID := "test-correlation-id"
		describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
			OperationId:    *result.JobId,
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.DestinationLocationID,
			XCorrelationID: googleproxyclient.NewOptString(correlationID),
		}

		mockClient.EXPECT().V1betaDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.OperationV1beta{Done: googleproxyclient.NewOptBool(true), Error: googleproxyclient.NewOptStatusV1Beta(googleproxyclient.StatusV1Beta{Message: googleproxyclient.NewOptString("failed")})}, nil)

		err := DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.DestinationLocationID, &correlationID)

		assert.Error(tt, err)
	})
}

// Mock encoded value for GetWorkflowLastExecutionTime tests
type mockTimeEncodedValue struct {
	err   bool
	value time.Time
}

func (m mockTimeEncodedValue) Get(valuePtr interface{}) error {
	if m.err {
		return fmt.Errorf("encoding error for value: %+v", valuePtr)
	}

	v, ok := valuePtr.(*time.Time)
	if !ok {
		return fmt.Errorf("unexpected type: %T", valuePtr)
	}

	*v = m.value
	return nil
}

func (m mockTimeEncodedValue) HasValue() bool {
	return true
}

// TestGetWorkflowLastExecutionTime tests the GetWorkflowLastExecutionTime method
func TestGetWorkflowLastExecutionTime(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	workflowID := "test-workflow-id"

	t.Run("Success - workflow completion time returned", func(t *testing.T) {
		mockClient := workflow_engine.NewMockTemporalTestClient(t)
		activity := &WFLastExecutionActivity{TemporalClient: mockClient}

		expectedTime := time.Date(2023, 10, 15, 14, 30, 0, 0, time.UTC)
		mockClient.On("QueryWorkflow", mock.Anything, workflowID, "", "status").
			Return(mockTimeEncodedValue{err: false, value: expectedTime}, nil)

		result, err := activity.GetWorkflowLastExecutionTime(ctx, workflowID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedTime, *result)
		mockClient.AssertExpectations(t)
	})

	t.Run("Success - workflow not found returns zero time", func(t *testing.T) {
		mockClient := workflow_engine.NewMockTemporalTestClient(t)
		activity := &WFLastExecutionActivity{TemporalClient: mockClient}

		expectedError := errors.New("workflow not found")
		mockClient.On("QueryWorkflow", mock.Anything, workflowID, "", "status").
			Return(nil, expectedError)

		result, err := activity.GetWorkflowLastExecutionTime(ctx, workflowID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsZero(), "Expected zero time when workflow is not found")
		mockClient.AssertExpectations(t)
	})

	t.Run("Error - decode workflow completion time fails", func(t *testing.T) {
		mockClient := workflow_engine.NewMockTemporalTestClient(t)
		activity := &WFLastExecutionActivity{TemporalClient: mockClient}

		mockClient.On("QueryWorkflow", mock.Anything, workflowID, "", "status").
			Return(mockTimeEncodedValue{err: true}, nil)

		result, err := activity.GetWorkflowLastExecutionTime(ctx, workflowID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to decode workflow completion time")
		mockClient.AssertExpectations(t)
	})

	t.Run("Success - empty workflow ID", func(t *testing.T) {
		mockClient := workflow_engine.NewMockTemporalTestClient(t)
		activity := &WFLastExecutionActivity{TemporalClient: mockClient}

		mockClient.On("QueryWorkflow", mock.Anything, "", "", "status").
			Return(nil, errors.New("invalid workflow ID"))

		result, err := activity.GetWorkflowLastExecutionTime(ctx, "")

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsZero(), "Expected zero time for empty workflow ID")
		mockClient.AssertExpectations(t)
	})

	t.Run("Success - query returns zero time value", func(t *testing.T) {
		mockClient := workflow_engine.NewMockTemporalTestClient(t)
		activity := &WFLastExecutionActivity{TemporalClient: mockClient}

		zeroTime := time.Time{}
		mockClient.On("QueryWorkflow", mock.Anything, workflowID, "", "status").
			Return(mockTimeEncodedValue{err: false, value: zeroTime}, nil)

		result, err := activity.GetWorkflowLastExecutionTime(ctx, workflowID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsZero(), "Expected zero time to be returned as is")
		mockClient.AssertExpectations(t)
	})

	t.Run("Error - temporal client query with connection error", func(t *testing.T) {
		mockClient := workflow_engine.NewMockTemporalTestClient(t)
		activity := &WFLastExecutionActivity{TemporalClient: mockClient}

		connectionError := errors.New("connection refused")
		mockClient.On("QueryWorkflow", mock.Anything, workflowID, "", "status").
			Return(nil, connectionError)

		result, err := activity.GetWorkflowLastExecutionTime(ctx, workflowID)

		assert.NoError(t, err, "Connection errors should return zero time, not propagate as error")
		assert.NotNil(t, result)
		assert.True(t, result.IsZero(), "Connection error should return zero time")
		mockClient.AssertExpectations(t)
	})

	t.Run("Success - specific time with nanoseconds", func(t *testing.T) {
		mockClient := workflow_engine.NewMockTemporalTestClient(t)
		activity := &WFLastExecutionActivity{TemporalClient: mockClient}

		expectedTime := time.Date(2023, 12, 25, 10, 15, 30, 123456789, time.UTC)
		mockClient.On("QueryWorkflow", mock.Anything, workflowID, "", "status").
			Return(mockTimeEncodedValue{err: false, value: expectedTime}, nil)

		result, err := activity.GetWorkflowLastExecutionTime(ctx, workflowID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedTime, *result)
		assert.Equal(t, 123456789, result.Nanosecond(), "Nanoseconds should be preserved")
		mockClient.AssertExpectations(t)
	})

	t.Run("Error - context timeout", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
		defer cancel()
		time.Sleep(2 * time.Nanosecond) // Ensure context is expired

		mockClient := workflow_engine.NewMockTemporalTestClient(t)
		activity := &WFLastExecutionActivity{TemporalClient: mockClient}

		contextError := context.DeadlineExceeded
		mockClient.On("QueryWorkflow", mock.Anything, workflowID, "", "status").
			Return(nil, contextError)

		result, err := activity.GetWorkflowLastExecutionTime(timeoutCtx, workflowID)

		assert.NoError(t, err, "Context timeout should not propagate as error, should return zero time")
		assert.NotNil(t, result)
		assert.True(t, result.IsZero(), "Context timeout should return zero time")
		mockClient.AssertExpectations(t)
	})
}

func TestCommonActivity_ListPoolsUUID(t *testing.T) {
	ctx := context.TODO()

	t.Run("ListPoolsUUID_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}

		expectedPools := []*database.PoolIdentifier{
			{
				Name:      "pool-1",
				AccountID: 123,
				VendorID:  "/projects/test-project/locations/us-central1/pools/pool-1",
				UUID:      "pool-uuid-1",
			},
			{
				Name:      "pool-2",
				AccountID: 124,
				VendorID:  "/projects/test-project/locations/us-west1/pools/pool-2",
				UUID:      "pool-uuid-2",
			},
		}

		mockStorage.On("ListPoolUUIDs", ctx, mock.AnythingOfType("*utils.Filter")).Return(expectedPools, nil)

		result, err := activity.ListPoolsUUID(ctx)
		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
		assert.Equal(tt, expectedPools[0].Name, result[0].Name)
		assert.Equal(tt, expectedPools[0].AccountID, result[0].AccountID)
		assert.Equal(tt, expectedPools[0].VendorID, result[0].VendorID)
		assert.Equal(tt, expectedPools[0].UUID, result[0].UUID)
		assert.Equal(tt, expectedPools[1].Name, result[1].Name)
		assert.Equal(tt, expectedPools[1].AccountID, result[1].AccountID)
		assert.Equal(tt, expectedPools[1].VendorID, result[1].VendorID)
		assert.Equal(tt, expectedPools[1].UUID, result[1].UUID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ListPoolsUUID_EmptyResult", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}

		mockStorage.On("ListPoolUUIDs", ctx, mock.AnythingOfType("*utils.Filter")).Return([]*database.PoolIdentifier{}, nil)

		result, err := activity.ListPoolsUUID(ctx)
		assert.NoError(tt, err)
		assert.Len(tt, result, 0)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ListPoolsUUID_DatabaseError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}

		mockStorage.On("ListPoolUUIDs", ctx, mock.AnythingOfType("*utils.Filter")).Return(nil, errors.New("database connection failed"))

		result, err := activity.ListPoolsUUID(ctx)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ListPoolsUUID_WithFilterConditions", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}

		expectedPools := []*database.PoolIdentifier{
			{
				Name:      "ready-pool",
				AccountID: 123,
				VendorID:  "/projects/test-project/locations/us-central1/pools/ready-pool",
				UUID:      "ready-pool-uuid",
			},
		}

		mockStorage.On("ListPoolUUIDs", ctx, mock.MatchedBy(func(filter *dbUtils.Filter) bool {
			// Verify that the filter contains the expected condition for state = "ready"
			for _, condition := range filter.Conditions {
				if condition.Field == "state" && condition.Op == "=" && condition.Value == models2.LifeCycleStateREADY {
					return true
				}
			}
			return false
		})).Return(expectedPools, nil)

		result, err := activity.ListPoolsUUID(ctx)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, expectedPools[0].Name, result[0].Name)
		mockStorage.AssertExpectations(tt)
	})
}
