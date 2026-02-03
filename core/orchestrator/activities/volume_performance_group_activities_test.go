package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"go.temporal.io/sdk/testsuite"
)

func TestCreateVPGInDB_Success(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreateVPGInDB)

	vpg := &datamodel.VolumePerformanceGroup{
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
		IsShared:         true,
		IsAutoGen:       false,
	}
	expectedVPG := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
		Name:      "test-vpg",
		PoolID:    1,
		ThroughputMibps: 100,
		Iops:            1000,
		IsShared:         true,
		IsAutoGen:       false,
	}

	mockStorage.On("CreateVolumePerformanceGroup", mock.Anything, vpg).Return(expectedVPG, nil)

	// Act
	var result *datamodel.VolumePerformanceGroup
	val, err := env.ExecuteActivity(activity.CreateVPGInDB, vpg)
	if err == nil {
		err = val.Get(&result)
	}

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedVPG, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateVPGInDB_Failure(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreateVPGInDB)

	vpg := &datamodel.VolumePerformanceGroup{
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
	}
	expectedError := errors.New("failed to create VPG")

	mockStorage.On("CreateVolumePerformanceGroup", mock.Anything, vpg).Return(nil, expectedError)

	// Act
	var result *datamodel.VolumePerformanceGroup
	val, err := env.ExecuteActivity(activity.CreateVPGInDB, vpg)
	if err == nil {
		err = val.Get(&result)
	}

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateQoSPolicyInONTAP_Success(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
		IsShared:         true,
	}
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("CreateQoSGroupPolicy", mock.AnythingOfType("vsa.CreateQoSGroupPolicyParams")).Return(&vsa.QoSGroupPolicyResponse{
		Name: "qos-policy-uuid",
	}, nil)

	// Act
	var result string
	val, err := env.ExecuteActivity(activity.CreateQoSPolicyInONTAP, vpg, poolView, node)
	if err == nil {
		err = val.Get(&result)
	}

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "qos-policy-uuid", result)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCreateQoSPolicyInONTAP_GetSvmError(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
	}
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	expectedError := errors.New("failed to get SVM")

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(nil, expectedError)

	// Act
	var result string
	val, err := env.ExecuteActivity(activity.CreateQoSPolicyInONTAP, vpg, poolView, node)
	if err == nil {
		err = val.Get(&result)
	}

	// Assert
	assert.Error(t, err)
	assert.Empty(t, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateQoSPolicyInONTAP_GetProviderError(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("failed to get provider")
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
	}
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

	// Act
	var result string
	val, err := env.ExecuteActivity(activity.CreateQoSPolicyInONTAP, vpg, poolView, node)
	if err == nil {
		err = val.Get(&result)
	}

	// Assert
	assert.Error(t, err)
	assert.Empty(t, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateQoSPolicyInONTAP_CreatePolicyError(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
		IsShared:         true,
	}
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}
	expectedError := errors.New("failed to create QoS policy")

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("CreateQoSGroupPolicy", mock.AnythingOfType("vsa.CreateQoSGroupPolicyParams")).Return(nil, expectedError)

	// Act
	var result string
	val, err := env.ExecuteActivity(activity.CreateQoSPolicyInONTAP, vpg, poolView, node)
	if err == nil {
		err = val.Get(&result)
	}

	// Assert
	assert.Error(t, err)
	assert.Empty(t, result)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteQoSPolicyInONTAP_Success(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeleteQoSPolicyInONTAP)

	qosPolicyID := "qos-policy-uuid"
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("DeleteQoSGroupPolicy", mock.AnythingOfType("vsa.DeleteQoSGroupPolicyParams")).Return(nil)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolView, node)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteQoSPolicyInONTAP_EmptyPolicyID(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeleteQoSPolicyInONTAP)

	qosPolicyID := ""
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolView, node)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteQoSPolicyInONTAP_GetSvmError(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeleteQoSPolicyInONTAP)

	qosPolicyID := "qos-policy-uuid"
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	expectedError := errors.New("failed to get SVM")

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(nil, expectedError)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolView, node)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteQoSPolicyInONTAP_GetProviderError(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("failed to get provider")
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeleteQoSPolicyInONTAP)

	qosPolicyID := "qos-policy-uuid"
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolView, node)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteQoSPolicyInONTAP_DeleteError(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeleteQoSPolicyInONTAP)

	qosPolicyID := "qos-policy-uuid"
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}
	expectedError := errors.New("failed to delete QoS policy")

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("DeleteQoSGroupPolicy", mock.AnythingOfType("vsa.DeleteQoSGroupPolicyParams")).Return(expectedError)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolView, node)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteQoSPolicyInONTAP_NotFoundError(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeleteQoSPolicyInONTAP)

	qosPolicyID := "qos-policy-uuid"
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
	}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}
	notFoundError := utilErrors.NewNotFoundErr("qos policy", nil)

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("DeleteQoSGroupPolicy", mock.AnythingOfType("vsa.DeleteQoSGroupPolicyParams")).Return(notFoundError)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolView, node)

	// Assert
	assert.NoError(t, err) // NotFound errors should be ignored
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}
