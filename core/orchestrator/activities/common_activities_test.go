package activities

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
		assert.Equal(tt, "Node not present for this pool (type: GetNodeError, retryable: false): node not found for the pool", err.Error())
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
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion)
		assert.Nil(t, subnet)
		assert.Error(t, err)
		mgs.AssertExpectations(t)
	})

	t.Run("No subnets found", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		mgs := hyperscaler2.NewMockGoogleServices(t)
		mgs.On("GetLogger").Return(mockLogger)
		mgs.On("ListSubnetworks", snHost, tenantProjectRegion).Return(&[]hyperscaler_models.Subnet{}, nil)
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion)
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
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion)
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
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion)

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
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion)
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
		subnet, err := getSubnetToBeUsed(mgs, mockStorage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion)
		assert.Nil(t, subnet)
		assert.Error(t, err, "reuse error")
		mgs.AssertExpectations(t)
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
