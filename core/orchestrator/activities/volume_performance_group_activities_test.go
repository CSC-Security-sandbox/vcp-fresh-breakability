package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"go.temporal.io/sdk/testsuite"
)

func TestGetVolumePerformanceGroupByUUID_EmptyUUID(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetVolumePerformanceGroupByUUID)

	val, err := env.ExecuteActivity(activity.GetVolumePerformanceGroupByUUID, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vpgUUID is empty")
	assert.Nil(t, val)
}

func TestGetVolumePerformanceGroupByUUID_VPGNotFound(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetVolumePerformanceGroupByUUID)

	vpgUUID := "non-existent-vpg-uuid"
	mockStorage.On("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(nil, errors.New("record not found"))

	val, err := env.ExecuteActivity(activity.GetVolumePerformanceGroupByUUID, vpgUUID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "record not found")
	assert.Nil(t, val)
	mockStorage.AssertExpectations(t)
}

func TestGetVolumePerformanceGroupByUUID_DatabaseError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetVolumePerformanceGroupByUUID)

	vpgUUID := "vpg-uuid-123"
	dbError := errors.New("database connection error")
	mockStorage.On("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(nil, dbError)

	val, err := env.ExecuteActivity(activity.GetVolumePerformanceGroupByUUID, vpgUUID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database connection error")
	assert.Nil(t, val)
	mockStorage.AssertExpectations(t)
}

func TestGetVolumePerformanceGroupByUUID_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetVolumePerformanceGroupByUUID)

	vpgUUID := "vpg-uuid-123"
	expectedVPG := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    1,
	}
	mockStorage.On("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(expectedVPG, nil)

	var result *datamodel.VolumePerformanceGroup
	val, err := env.ExecuteActivity(activity.GetVolumePerformanceGroupByUUID, vpgUUID)
	if err == nil {
		err = val.Get(&result)
	}

	assert.NoError(t, err)
	assert.Equal(t, expectedVPG, result)
	mockStorage.AssertExpectations(t)
}

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
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("CreateQoSGroupPolicy", mock.AnythingOfType("vsa.CreateQoSGroupPolicyParams")).Return(&vsa.QoSGroupPolicyResponse{
		Name: "test-vpg",
		UUID: "qos-policy-uuid",
	}, nil)

	// Act
	var result string
	val, err := env.ExecuteActivity(activity.CreateQoSPolicyInONTAP, vpg, node)
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
	node := &models.Node{ExternalUUID: "node-uuid"}
	expectedError := errors.New("failed to get SVM")

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(nil, expectedError)

	// Act
	var result string
	val, err := env.ExecuteActivity(activity.CreateQoSPolicyInONTAP, vpg, node)
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
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

	// Act
	var result string
	val, err := env.ExecuteActivity(activity.CreateQoSPolicyInONTAP, vpg, node)
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
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}
	expectedError := errors.New("failed to create QoS policy")

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("CreateQoSGroupPolicy", mock.AnythingOfType("vsa.CreateQoSGroupPolicyParams")).Return(nil, expectedError)

	// Act
	var result string
	val, err := env.ExecuteActivity(activity.CreateQoSPolicyInONTAP, vpg, node)
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
	poolID := int64(1)
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("DeleteQoSGroupPolicy", mock.AnythingOfType("vsa.DeleteQoSGroupPolicyParams")).Return(nil)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolID, node)

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
	poolID := int64(1)
	node := &models.Node{ExternalUUID: "node-uuid"}

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolID, node)

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
	poolID := int64(1)
	node := &models.Node{ExternalUUID: "node-uuid"}
	expectedError := errors.New("failed to get SVM")

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(nil, expectedError)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolID, node)

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
	poolID := int64(1)
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolID, node)

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
	poolID := int64(1)
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}
	expectedError := errors.New("failed to delete QoS policy")

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("DeleteQoSGroupPolicy", mock.AnythingOfType("vsa.DeleteQoSGroupPolicyParams")).Return(expectedError)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolID, node)

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
	poolID := int64(1)
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{
		Name: "test-svm",
	}
	notFoundError := utilErrors.NewNotFoundErr("qos policy", nil)

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("DeleteQoSGroupPolicy", mock.AnythingOfType("vsa.DeleteQoSGroupPolicyParams")).Return(notFoundError)

	// Act
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, poolID, node)

	// Assert
	assert.NoError(t, err) // NotFound errors should be ignored
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestGetPoolViewByPoolID_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetPoolViewByPoolID)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"}, AccountID: 1}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"}}}

	mockStorage.On("GetPoolByID", mock.Anything, int64(1)).Return(pool, nil)
	mockStorage.On("GetPool", mock.Anything, "pool-uuid", int64(1)).Return(poolView, nil)

	var result *datamodel.PoolView
	val, err := env.ExecuteActivity(activity.GetPoolViewByPoolID, int64(1))
	if err == nil {
		err = val.Get(&result)
	}

	assert.NoError(t, err)
	assert.Equal(t, poolView, result)
	mockStorage.AssertExpectations(t)
}

func TestGetPoolViewByPoolID_GetPoolByIDError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetPoolViewByPoolID)

	mockStorage.On("GetPoolByID", mock.Anything, int64(1)).Return(nil, errors.New("pool not found"))

	var result *datamodel.PoolView
	val, err := env.ExecuteActivity(activity.GetPoolViewByPoolID, int64(1))
	if err == nil {
		_ = val.Get(&result)
	}

	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestUpdateQoSPolicyInONTAP_Success_WithRename(t *testing.T) {
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
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: "vpg-uuid"},
		Name:            "old-name",
		OntapQosPolicyID: "old-name",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            500,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}
	qosResp := &vsa.QoSGroupPolicyResponse{UUID: "policy-uuid", Name: "old-name"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{Name: "old-name", SvmName: "svm1"}).Return(qosResp, nil)
	mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
		UUID: "policy-uuid", Name: "new-name", SvmName: "svm1", MaxThroughput: 200, MaxIOPS: 600,
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateQoSPolicyInONTAP, vpg, poolView, node, "new-name", int64(200), int64(600))
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestUpdateQoSPolicyInONTAP_Success_ThroughputOnly(t *testing.T) {
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
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: "vpg-uuid"},
		Name:            "current-name",
		OntapQosPolicyID: "current-name",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            500,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}
	qosResp := &vsa.QoSGroupPolicyResponse{UUID: "policy-uuid", Name: "current-name"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{Name: "current-name", SvmName: "svm1"}).Return(qosResp, nil)
	mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
		UUID: "policy-uuid", Name: "", SvmName: "svm1", MaxThroughput: 200, MaxIOPS: 600,
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateQoSPolicyInONTAP, vpg, poolView, node, "current-name", int64(200), int64(600))
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestUpdateQoSPolicyInONTAP_EmptyOntapQosPolicyID(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: "vpg-uuid"},
		Name:             "vpg-name",
		PoolID:           1,
		OntapQosPolicyID: "",
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}

	_, err := env.ExecuteActivity(activity.UpdateQoSPolicyInONTAP, vpg, poolView, node, "new-name", int64(100), int64(500))
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateQoSPolicyInONTAP_GetSvmError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "policy-name",
		PoolID:           1,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(nil, errors.New("svm not found"))

	_, err := env.ExecuteActivity(activity.UpdateQoSPolicyInONTAP, vpg, poolView, node, "new-name", int64(100), int64(500))
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateQoSPolicyInONTAP_GetProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider not available")
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "policy-name",
		PoolID:          1,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

	_, err := env.ExecuteActivity(activity.UpdateQoSPolicyInONTAP, vpg, poolView, node, "new-name", int64(100), int64(500))
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateQoSPolicyInONTAP_FindPolicyNotFound(t *testing.T) {
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
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "missing-policy",
		PoolID:          1,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{Name: "missing-policy", SvmName: "svm1"}).
		Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("not found")))

	_, err := env.ExecuteActivity(activity.UpdateQoSPolicyInONTAP, vpg, poolView, node, "missing-policy", int64(100), int64(500))
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestUpdateQoSPolicyInONTAP_FindPolicyNotFound_FallbackByNewName_Success(t *testing.T) {
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
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "old-db-name",
		PoolID:          1,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}
	qosResp := &vsa.QoSGroupPolicyResponse{UUID: "policy-uuid", Name: "already-renamed-in-ontap"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{Name: "old-db-name", SvmName: "svm1"}).
		Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("not found")))
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{Name: "already-renamed-in-ontap", SvmName: "svm1"}).
		Return(qosResp, nil)
	mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
		UUID: "policy-uuid", Name: "", SvmName: "svm1", MaxThroughput: 150, MaxIOPS: 700,
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateQoSPolicyInONTAP, vpg, poolView, node, "already-renamed-in-ontap", int64(150), int64(700))
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestUpdateQoSPolicyInONTAP_UpdateQoSGroupPolicyError(t *testing.T) {
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
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "policy-name",
		PoolID:          1,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}
	qosResp := &vsa.QoSGroupPolicyResponse{UUID: "policy-uuid", Name: "policy-name"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{Name: "policy-name", SvmName: "svm1"}).Return(qosResp, nil)
	mockProvider.On("UpdateQoSGroupPolicy", mock.AnythingOfType("vsa.UpdateQoSGroupPolicyParams")).Return(errors.New("ONTAP update failed"))

	_, err := env.ExecuteActivity(activity.UpdateQoSPolicyInONTAP, vpg, poolView, node, "new-name", int64(100), int64(500))
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVPGInDB_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVPGInDB)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
		Name:            "updated-name",
		PoolID:          1,
		ThroughputMibps: 200,
		Iops:            600,
	}

	mockStorage.On("UpdateVolumePerformanceGroup", mock.Anything, vpg).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVPGInDB, vpg)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVPGWithOntapID_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVPGWithOntapID)

	vpgUUID := "vpg-uuid-123"
	ontapQosPolicyID := "qos-policy-ontap-id"
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    1,
	}

	mockStorage.On("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	mockStorage.On("UpdateVolumePerformanceGroup", mock.Anything, mock.MatchedBy(func(v *datamodel.VolumePerformanceGroup) bool {
		return v.UUID == vpgUUID && v.OntapQosPolicyID == ontapQosPolicyID
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVPGWithOntapID, vpgUUID, ontapQosPolicyID)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVPGInDB_Error(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVPGInDB)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
		Name:     "updated-name",
		PoolID:   1,
	}

	mockStorage.On("UpdateVolumePerformanceGroup", mock.Anything, vpg).Return(errors.New("db update failed"))

	_, err := env.ExecuteActivity(activity.UpdateVPGInDB, vpg)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVPGWithOntapID_GetVPGError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVPGWithOntapID)

	vpgUUID := "vpg-uuid-123"
	ontapQosPolicyID := "qos-policy-ontap-id"
	dbError := errors.New("vpg not found")

	mockStorage.On("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(nil, dbError)

	_, err := env.ExecuteActivity(activity.UpdateVPGWithOntapID, vpgUUID, ontapQosPolicyID)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVPGWithOntapID_UpdateError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVPGWithOntapID)

	vpgUUID := "vpg-uuid-123"
	ontapQosPolicyID := "qos-policy-ontap-id"
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    1,
	}
	updateError := errors.New("update failed")

	mockStorage.On("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	mockStorage.On("UpdateVolumePerformanceGroup", mock.Anything, mock.Anything).Return(updateError)

	_, err := env.ExecuteActivity(activity.UpdateVPGWithOntapID, vpgUUID, ontapQosPolicyID)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}
