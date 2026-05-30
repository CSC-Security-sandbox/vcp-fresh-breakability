package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
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
		IsShared:        true,
		IsAutoGen:       false,
	}
	expectedVPG := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
		IsShared:        true,
		IsAutoGen:       false,
	}

	mockStorage.On("CreateVolumePerformanceGroupWithCap", mock.Anything, vpg, mock.AnythingOfType("int")).Return(expectedVPG, nil)

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

	mockStorage.On("CreateVolumePerformanceGroupWithCap", mock.Anything, vpg, mock.AnythingOfType("int")).Return(nil, expectedError)

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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
		IsShared:        true,
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		Name:            "test-vpg",
		PoolID:          1,
		ThroughputMibps: 100,
		Iops:            1000,
		IsShared:        true,
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, "", poolID, node)

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
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, "", poolID, node)

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
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, "", poolID, node)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteQoSPolicyInONTAP_GetProviderError(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, "", poolID, node)

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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, "", poolID, node)

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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	_, err := env.ExecuteActivity(activity.DeleteQoSPolicyInONTAP, qosPolicyID, "", poolID, node)

	// Assert
	assert.NoError(t, err) // NotFound errors should be ignored
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteQoSPolicyInONTAP_NameFallback_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	act := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(act.DeleteQoSPolicyInONTAP)

	poolID := int64(1)
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "test-svm"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("DeleteQoSGroupPolicy", vsa.DeleteQoSGroupPolicyParams{
		SvmName: "test-svm",
		Name:    "vpg-name-fallback",
	}).Return(nil)

	_, err := env.ExecuteActivity(act.DeleteQoSPolicyInONTAP, "", "vpg-name-fallback", poolID, node)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestHardDeleteVPGInDB_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(act.HardDeleteVPGInDB)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
		Name:      "test-vpg",
		PoolID:    1,
	}

	mockStorage.On("HardDeleteVolumePerformanceGroup", mock.Anything, vpg).Return(nil)

	_, err := env.ExecuteActivity(act.HardDeleteVPGInDB, vpg)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestHardDeleteVPGInDB_Error(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(act.HardDeleteVPGInDB)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-uuid"},
		Name:      "test-vpg",
		PoolID:    1,
	}

	mockStorage.On("HardDeleteVolumePerformanceGroup", mock.Anything, vpg).Return(errors.New("db delete failed"))

	_, err := env.ExecuteActivity(act.HardDeleteVPGInDB, vpg)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: "vpg-uuid"},
		Name:             "old-name",
		OntapQosPolicyID: "550e8400-e29b-41d4-a716-446655440000",
		PoolID:           1,
		ThroughputMibps:  100,
		Iops:             500,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}
	qosResp := &vsa.QoSGroupPolicyResponse{UUID: "550e8400-e29b-41d4-a716-446655440000", Name: "old-name"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{UUID: vpg.OntapQosPolicyID, SvmName: "svm1"}).Return(qosResp, nil)
	mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
		UUID: qosResp.UUID, Name: "new-name", SvmName: "svm1", MaxThroughput: 200, MaxIOPS: 600,
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: "vpg-uuid"},
		Name:             "current-name",
		OntapQosPolicyID: "550e8400-e29b-41d4-a716-446655440001",
		PoolID:           1,
		ThroughputMibps:  100,
		Iops:             500,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}
	qosResp := &vsa.QoSGroupPolicyResponse{UUID: "550e8400-e29b-41d4-a716-446655440001", Name: "current-name"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{UUID: vpg.OntapQosPolicyID, SvmName: "svm1"}).Return(qosResp, nil)
	mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
		UUID: qosResp.UUID, Name: "", SvmName: "svm1", MaxThroughput: 200, MaxIOPS: 600,
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
		BaseModel:        datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "550e8400-e29b-41d4-a716-svm-error",
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider not available")
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "550e8400-e29b-41d4-a716-provider-error",
		PoolID:           1,
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "550e8400-e29b-41d4-missing",
		PoolID:           1,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{UUID: vpg.OntapQosPolicyID, SvmName: "svm1"}).
		Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("not found")))
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "550e8400-e29b-41d4-old-db-uuid",
		PoolID:           1,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}
	qosResp := &vsa.QoSGroupPolicyResponse{UUID: "550e8400-e29b-41d4-old-db-uuid", Name: "already-renamed-in-ontap"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{UUID: vpg.OntapQosPolicyID, SvmName: "svm1"}).
		Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("not found")))
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{Name: "already-renamed-in-ontap", SvmName: "svm1"}).
		Return(qosResp, nil)
	mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
		UUID: qosResp.UUID, Name: "", SvmName: "svm1", MaxThroughput: 150, MaxIOPS: 700,
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
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateQoSPolicyInONTAP)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: "vpg-uuid"},
		OntapQosPolicyID: "550e8400-e29b-41d4-a716-446655440002",
		PoolID:           1,
	}
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}}
	node := &models.Node{ExternalUUID: "node-uuid"}
	svm := &datamodel.Svm{Name: "svm1"}
	qosResp := &vsa.QoSGroupPolicyResponse{UUID: "550e8400-e29b-41d4-a716-446655440002", Name: "policy-name"}

	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{UUID: vpg.OntapQosPolicyID, SvmName: "svm1"}).Return(qosResp, nil)
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
		Name:      "updated-name",
		PoolID:    1,
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

func TestListVolumePerformanceGroupsByPoolID_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.ListVolumePerformanceGroupsByPoolID)

	poolID := int64(1)
	vpgs := []*datamodel.VolumePerformanceGroup{
		{BaseModel: datamodel.BaseModel{UUID: "vpg-1"}, Name: "vpg-1", PoolID: poolID},
	}
	mockStorage.On("ListVolumePerformanceGroupsByPoolID", mock.Anything, poolID).Return(vpgs, nil)

	val, err := env.ExecuteActivity(activity.ListVolumePerformanceGroupsByPoolID, poolID)
	assert.NoError(t, err)
	assert.NotNil(t, val)
	var result []*datamodel.VolumePerformanceGroup
	_ = val.Get(&result)
	assert.Len(t, result, 1)
	assert.Equal(t, "vpg-1", result[0].Name)
	mockStorage.AssertExpectations(t)
}

func TestListVolumePerformanceGroupsByPoolID_StorageError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.ListVolumePerformanceGroupsByPoolID)

	poolID := int64(1)
	mockStorage.On("ListVolumePerformanceGroupsByPoolID", mock.Anything, poolID).Return(nil, errors.New("list vpgs failed"))

	val, err := env.ExecuteActivity(activity.ListVolumePerformanceGroupsByPoolID, poolID)
	assert.Error(t, err)
	assert.Nil(t, val)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVPGStateInDB_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVPGStateInDB)

	vpgUUID := "vpg-uuid-123"
	state := "READY"
	stateDetails := ""

	mockStorage.On("UpdateVolumePerformanceGroupState", mock.Anything, vpgUUID, state, stateDetails).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVPGStateInDB, vpgUUID, state, stateDetails)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVPGStateInDB_Error(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVPGStateInDB)

	vpgUUID := "vpg-uuid-123"
	state := "READY"
	stateDetails := ""

	mockStorage.On("UpdateVolumePerformanceGroupState", mock.Anything, vpgUUID, state, stateDetails).Return(errors.New("db state update failed"))

	_, err := env.ExecuteActivity(activity.UpdateVPGStateInDB, vpgUUID, state, stateDetails)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db state update failed")
	mockStorage.AssertExpectations(t)
}

func TestRestoreAutoGeneratedVPG_NilVPG_ReturnsError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.RestoreAutoGeneratedVPG)

	_, err := env.ExecuteActivity(activity.RestoreAutoGeneratedVPG, nil, []*datamodel.Volume{}, &models.Node{})
	assert.Error(t, err)
}

func TestRestoreAutoGeneratedVPG_CreateVPGInDBError_ReturnsError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.RestoreAutoGeneratedVPG)

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: "vpg-restore"}, Name: "vpg-restore", PoolID: 1,
		ThroughputMibps: 100, Iops: 200, IsShared: true,
	}
	node := &models.Node{ExternalUUID: "node-1"}

	svm := &datamodel.Svm{Name: "svm-1", SvmDetails: &datamodel.SvmDetails{ExternalUUID: "svm-uuid"}}
	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("CreateQoSGroupPolicy", mock.AnythingOfType("vsa.CreateQoSGroupPolicyParams")).Return(&vsa.QoSGroupPolicyResponse{Name: "policy-1", UUID: "policy-uuid"}, nil)
	mockStorage.On("CreateVolumePerformanceGroupWithCap", mock.Anything, mock.MatchedBy(func(v *datamodel.VolumePerformanceGroup) bool {
		return v.OntapQosPolicyID == "policy-uuid"
	}), mock.AnythingOfType("int")).Return(nil, errors.New("create vpg failed"))

	_, err := env.ExecuteActivity(activity.RestoreAutoGeneratedVPG, vpg, []*datamodel.Volume{}, node)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestRestoreAutoGeneratedVPG_Success_UsesVPGNameForVolumeQoS(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()
	vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumePerformanceGroupActivity{SE: mockStorage}
	env.RegisterActivity(activity.RestoreAutoGeneratedVPG)

	vpgName := "transition-vpg-name"
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: "vpg-uuid"}, Name: vpgName, PoolID: 1,
		ThroughputMibps: 100, Iops: 200, IsShared: true,
	}
	vol := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "vol-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "ontap-vol-uuid"},
	}
	node := &models.Node{ExternalUUID: "node-1"}

	svm := &datamodel.Svm{Name: "svm-1", SvmDetails: &datamodel.SvmDetails{ExternalUUID: "svm-uuid"}}
	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockProvider.On("CreateQoSGroupPolicy", mock.AnythingOfType("vsa.CreateQoSGroupPolicyParams")).Return(&vsa.QoSGroupPolicyResponse{Name: vpgName, UUID: "policy-uuid"}, nil)
	createdVPG := &datamodel.VolumePerformanceGroup{BaseModel: datamodel.BaseModel{ID: 42}, Name: vpgName, OntapQosPolicyID: "policy-uuid"}
	mockStorage.On("CreateVolumePerformanceGroupWithCap", mock.Anything, mock.MatchedBy(func(v *datamodel.VolumePerformanceGroup) bool {
		return v.OntapQosPolicyID == "policy-uuid" && v.Name == vpgName
	}), mock.AnythingOfType("int")).Return(createdVPG, nil)
	// ONTAP volume update must receive the policy name (vpg.Name), not the UUID.
	mockProvider.On("UpdateVolume", mock.MatchedBy(func(p vsa.UpdateVolumeParams) bool {
		return p.UUID == "ontap-vol-uuid" && p.QosPolicyName != nil && *p.QosPolicyName == vpgName
	})).Return(nil)
	mockStorage.On("UpdateVolumeFields", mock.Anything, "vol-uuid", mock.Anything).Return(nil)

	_, err := env.ExecuteActivity(activity.RestoreAutoGeneratedVPG, vpg, []*datamodel.Volume{vol}, node)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}
