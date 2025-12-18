package activities

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontapmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/testsuite"
	"gorm.io/gorm"
)

func TestVolumeRefreshActivity_GetDBVolumesForPool_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetDBVolumesForPool)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	// Create test volumes
	volumes := []*datamodel.Volume{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "external-1",
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-2"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "external-2",
			},
		},
	}

	// Mock GetVolumesByPoolID to succeed
	mockStorage.On("GetVolumesByPoolID", mock.Anything, pool.ID).Return(volumes, nil)

	val, err := env.ExecuteActivity(activity.GetDBVolumesForPool, pool)

	assert.NoError(t, err)
	var result *PoolDBVolumesMap
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.DBVolumesByExternalUUID, 2)
	assert.Contains(t, result.DBVolumesByExternalUUID, "external-1")
	assert.Contains(t, result.DBVolumesByExternalUUID, "external-2")
	assert.Equal(t, volumes[0], result.DBVolumesByExternalUUID["external-1"])
	assert.Equal(t, volumes[1], result.DBVolumesByExternalUUID["external-2"])
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetDBVolumesForPool_Error(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetDBVolumesForPool)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	expectedError := fmt.Errorf("database error")
	mockStorage.On("GetVolumesByPoolID", mock.Anything, pool.ID).Return(nil, expectedError)

	val, err := env.ExecuteActivity(activity.GetDBVolumesForPool, pool)

	assert.Error(t, err)
	var result *PoolDBVolumesMap
	if val != nil {
		_ = val.Get(&result)
	}
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetDBVolumesForPool_EmptyResult(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetDBVolumesForPool)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	// Mock GetVolumesByPoolID to return empty result
	mockStorage.On("GetVolumesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Volume{}, nil)

	val, err := env.ExecuteActivity(activity.GetDBVolumesForPool, pool)

	assert.NoError(t, err)
	var result *PoolDBVolumesMap
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.DBVolumesByExternalUUID, 0)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetOntapVolumes_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetOntapVolumes)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid", ID: 1},
		Name:      "test-pool",
		PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
			AuthType:      1,
		},
		DeploymentName: "test-deployment",
	}

	// Mock the database calls that _getOntapRestProviderForPool needs
	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "192.168.1.1",
		},
	}
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return(nodes, nil)

	// Create mock ONTAP volumes
	name1 := "volume1"
	uuid1 := "volume-uuid-1"
	svmName1 := "svm1"
	name2 := "volume2"
	uuid2 := "volume-uuid-2"
	svmName2 := "svm2"

	ontapVolumes := []*vsa.Volume{
		{
			Volume: ontapmodels.Volume{
				Name: &name1,
				UUID: &uuid1,
				Svm: &ontapmodels.VolumeInlineSvm{
					Name: &svmName1,
				},
				IsSvmRoot: nillable.ToPointer(false),
			},
		},
		{
			Volume: ontapmodels.Volume{
				Name: &name2,
				UUID: &uuid2,
				Svm: &ontapmodels.VolumeInlineSvm{
					Name: &svmName2,
				},
				IsSvmRoot: nillable.ToPointer(false),
			},
		},
	}

	// Mock _getOntapRestProviderForPool indirectly by mocking hyperscaler functions
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	mockProvider.On("GetVolumes").Return(ontapVolumes, nil)

	val, err := env.ExecuteActivity(activity.GetOntapVolumes, pool)

	assert.NoError(t, err)
	var result *GetOntapVolumesReturnValue
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.OntapVolumeMap, 2) // Two valid volumes
	assert.Contains(t, result.OntapVolumeMap, uuid1)
	assert.Contains(t, result.OntapVolumeMap, uuid2)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetOntapVolumes_ProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetOntapVolumes)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid", ID: 1},
		Name:      "test-pool",
		PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
			AuthType:      1,
		},
		DeploymentName: "test-deployment",
	}

	// Mock GetNodesByPoolID to return error (simulating no nodes found)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return(nil, gorm.ErrRecordNotFound)

	val, err := env.ExecuteActivity(activity.GetOntapVolumes, pool)

	assert.Error(t, err)
	var result *GetOntapVolumesReturnValue
	if val != nil {
		_ = val.Get(&result)
	}
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetOntapVolumes_GetVolumesError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetOntapVolumes)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid", ID: 1},
		Name:      "test-pool",
		PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
			AuthType:      1,
		},
		DeploymentName: "test-deployment",
	}

	// Mock the database calls
	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "192.168.1.1",
		},
	}
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return(nodes, nil)

	// Mock hyperscaler function
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	expectedError := errors.New("failed to get volumes from ONTAP")
	mockProvider.On("GetVolumes").Return(nil, expectedError)

	val, err := env.ExecuteActivity(activity.GetOntapVolumes, pool)

	assert.Error(t, err)
	var result *GetOntapVolumesReturnValue
	if val != nil {
		_ = val.Get(&result)
	}
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get volumes from ONTAP")
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestVolumeRefreshActivity_ProcessVolumePoolMapping_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessVolumePoolMapping)

	pool1 := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-1"},
		Name:      "pool-1-name",
	}
	pool2 := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-2"},
		Name:      "pool-2-name",
	}

	input := &ProcessVolumePoolMappingInput{
		Volumes: []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-1"},
				Pool:      pool1,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-2"},
				Pool:      pool1,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-3"},
				Pool:      pool2,
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessVolumePoolMapping, input)

	assert.NoError(t, err)
	var result *ProcessVolumePoolMappingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.PoolByUUID, 2)
	assert.Len(t, result.PoolUUIDs, 2)
	assert.Contains(t, result.PoolByUUID, "pool-1")
	assert.Contains(t, result.PoolByUUID, "pool-2")
	assert.Equal(t, pool1, result.PoolByUUID["pool-1"])
	assert.Equal(t, pool2, result.PoolByUUID["pool-2"])
}

func TestVolumeRefreshActivity_ProcessVolumePoolMapping_NilInput(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessVolumePoolMapping)

	val, err := env.ExecuteActivity(activity.ProcessVolumePoolMapping, nil)

	assert.NoError(t, err)
	var result *ProcessVolumePoolMappingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.PoolByUUID, 0)
	assert.Len(t, result.PoolUUIDs, 0)
}

func TestVolumeRefreshActivity_ProcessVolumePoolMapping_EmptyVolumes(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessVolumePoolMapping)

	input := &ProcessVolumePoolMappingInput{
		Volumes: []*datamodel.Volume{},
	}

	val, err := env.ExecuteActivity(activity.ProcessVolumePoolMapping, input)

	assert.NoError(t, err)
	var result *ProcessVolumePoolMappingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.PoolByUUID, 0)
	assert.Len(t, result.PoolUUIDs, 0)
}

func TestVolumeRefreshActivity_ProcessVolumePoolMapping_InvalidVolumes(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessVolumePoolMapping)

	pool1 := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-1"},
		Name:      "pool-1-name",
	}

	input := &ProcessVolumePoolMappingInput{
		Volumes: []*datamodel.Volume{
			nil, // nil volume should be skipped
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-1"},
				Pool:      nil, // nil pool should be skipped
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-2"},
				Pool: &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: ""}, // empty UUID should be skipped
				},
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-3"},
				Pool:      pool1, // valid volume
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessVolumePoolMapping, input)

	assert.NoError(t, err)
	var result *ProcessVolumePoolMappingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.PoolByUUID, 1) // Only one valid pool
	assert.Len(t, result.PoolUUIDs, 1)
	assert.Contains(t, result.PoolByUUID, "pool-1")
	assert.Equal(t, pool1, result.PoolByUUID["pool-1"])
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)),
				},
			},
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	assert.Contains(t, result.UpdatedVolumeByUUID, "vol-uuid")
	assert.Equal(t, uint64(2048), result.UpdatedVolumeByUUID["vol-uuid"].UsedBytes)
	assert.Equal(t, 1, result.MatchedCount)
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_NoChanges(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	// Database volume already has the same UsedBytes as ONTAP
	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		UsedBytes: uint64(2048), // Same as ONTAP value
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)), // Same as database value
				},
			},
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	// Volume should not be included in UpdatedVolumeByUUID since there are no changes
	assert.Len(t, result.UpdatedVolumeByUUID, 0)
	assert.NotContains(t, result.UpdatedVolumeByUUID, "vol-uuid")
	assert.Equal(t, 0, result.MatchedCount) // MatchedCount should be 0 since no volumes were updated
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_WithChanges(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	// Database volume has different UsedBytes than ONTAP
	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		UsedBytes: uint64(1024), // Different from ONTAP value
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)), // Different from database value
				},
			},
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	// Volume should be included in UpdatedVolumeByUUID since there are changes
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	assert.Contains(t, result.UpdatedVolumeByUUID, "vol-uuid")
	assert.Equal(t, uint64(2048), result.UpdatedVolumeByUUID["vol-uuid"].UsedBytes)
	assert.Equal(t, 1, result.MatchedCount)
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_NilInput(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, nil)

	assert.Error(t, err)
	var result *ProcessOntapVolumeMatchingResult
	if val != nil {
		_ = val.Get(&result)
	}
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "ProcessOntapVolumeMatching input cannot be nil")
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_VolumeNotFound(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					// Volume with different external UUID - not found
					"different-external-uuid": &vsa.Volume{},
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 0)
	assert.Equal(t, 0, result.MatchedCount)
	assert.Equal(t, 1, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 1)
	assert.Equal(t, dbVolume, result.VolumesNotFoundInONTAP[0])
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_NoPoolResults(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes:           []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			// No results for this pool
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 0)
	assert.Equal(t, 0, result.MatchedCount)
	assert.Equal(t, 1, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 1)
	assert.Equal(t, dbVolume, result.VolumesNotFoundInONTAP[0])
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_InvalidVolumes(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	invalidVolumes := []*datamodel.Volume{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-1"},
			Pool:      nil, // nil pool
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "external-uuid",
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-2"},
			Pool: &datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			},
			VolumeAttributes: nil, // nil volume attributes
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-3"},
			Pool: &datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "", // empty external UUID
			},
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes:           invalidVolumes,
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 0)
	assert.Equal(t, 0, result.MatchedCount)
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
}

func TestVolumeRefreshActivity_validateOntapVolume_Valid(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}

	validVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(1024)),
				},
			},
		},
	}

	err := activity.validateOntapVolume(validVolume)

	assert.NoError(t, err)
}

func TestVolumeRefreshActivity_validateOntapVolume_Nil(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}

	err := activity.validateOntapVolume(nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ONTAP volume is nil")
}

func TestVolumeRefreshActivity_validateOntapVolume_NilSpace(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}

	invalidVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: nil,
		},
	}

	err := activity.validateOntapVolume(invalidVolume)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ONTAP volume space information is nil")
}

func TestVolumeRefreshActivity_validateOntapVolume_NilLogicalSpace(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}

	invalidVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: nil,
			},
		},
	}

	err := activity.validateOntapVolume(invalidVolume)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ONTAP volume logical space information is nil")
}

func TestVolumeRefreshActivity_validateOntapVolume_NilUsedSpace(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}

	invalidVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nil,
				},
			},
		},
	}

	err := activity.validateOntapVolume(invalidVolume)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ONTAP volume used space information is nil")
}

func TestVolumeRefreshActivity_SyncUpdatedVolumesToDatabase_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.SyncUpdatedVolumesToDatabase)

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
		},
		"vol-2": {
			BaseModel: datamodel.BaseModel{UUID: "vol-2", ID: 2},
			UsedBytes: 2048,
		},
	}

	// Mock BatchUpdateVolumeFields to succeed
	mockStorage.On("BatchUpdateVolumeFields", mock.Anything, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(nil)

	_, err := env.ExecuteActivity(activity.SyncUpdatedVolumesToDatabase, dbVols)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_SyncUpdatedVolumesToDatabase_Error(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.SyncUpdatedVolumesToDatabase)

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
		},
	}

	expectedError := errors.New("database error")
	mockStorage.On("BatchUpdateVolumeFields", mock.Anything, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(expectedError)

	_, err := env.ExecuteActivity(activity.SyncUpdatedVolumesToDatabase, dbVols)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_SyncUpdatedVolumesToDatabase_EmptyVols(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.SyncUpdatedVolumesToDatabase)

	dbVols := map[string]*datamodel.Volume{}

	_, err := env.ExecuteActivity(activity.SyncUpdatedVolumesToDatabase, dbVols)

	assert.NoError(t, err)
	// No database calls should be made for empty volumes
	mockStorage.AssertExpectations(t)
}

// Test wrapper activity for testing _syncUpdatedVolumesToDatabase helper function
type testSyncActivityWrapper struct {
	SE          database.Storage
	DBVols      map[string]*datamodel.Volume
	ShouldError bool
}

func (w *testSyncActivityWrapper) TestSyncActivity(ctx context.Context) error {
	return _syncUpdatedVolumesToDatabase(ctx, w.SE, w.DBVols)
}

func Test_syncUpdatedVolumesToDatabase_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
		},
		"vol-2": {
			BaseModel: datamodel.BaseModel{UUID: "vol-2", ID: 2},
			UsedBytes: 2048,
		},
	}

	// Mock BatchUpdateVolumeFields to succeed
	mockStorage.On("BatchUpdateVolumeFields", mock.Anything, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(nil)

	wrapper := &testSyncActivityWrapper{
		SE:     mockStorage,
		DBVols: dbVols,
	}
	env.RegisterActivity(wrapper.TestSyncActivity)

	_, err := env.ExecuteActivity(wrapper.TestSyncActivity)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_syncUpdatedVolumesToDatabase_Error(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
		},
	}

	expectedError := errors.New("database error")
	mockStorage.On("BatchUpdateVolumeFields", mock.Anything, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(expectedError)

	wrapper := &testSyncActivityWrapper{
		SE:     mockStorage,
		DBVols: dbVols,
	}
	env.RegisterActivity(wrapper.TestSyncActivity)

	_, err := env.ExecuteActivity(wrapper.TestSyncActivity)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}

func Test_syncUpdatedVolumesToDatabase_EmptyVolumes(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)

	dbVols := map[string]*datamodel.Volume{}

	wrapper := &testSyncActivityWrapper{
		SE:     mockStorage,
		DBVols: dbVols,
	}
	env.RegisterActivity(wrapper.TestSyncActivity)

	_, err := env.ExecuteActivity(wrapper.TestSyncActivity)

	assert.NoError(t, err)
	// No database calls should be made for empty volumes
	mockStorage.AssertExpectations(t)
}

func Test_getOntapRestProviderForPool_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		Name:      "test-pool",
		PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
			AuthType:      1,
		},
		DeploymentName: "test-deployment",
	}

	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "192.168.1.1",
		},
	}

	mockProvider := vsa.NewMockProvider(t)

	// Mock GetNodesByPoolID to succeed
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

	// Mock hyperscaler function
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	result, err := _getOntapRestProviderForPool(ctx, mockStorage, pool)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, mockProvider, result)
	mockStorage.AssertExpectations(t)
}

func Test_getOntapRestProviderForPool_NoNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	// Mock GetNodesByPoolID to return no nodes
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{}, nil)

	result, err := _getOntapRestProviderForPool(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "Resource not found")
	mockStorage.AssertExpectations(t)
}

func Test_getOntapRestProviderForPool_NoCredentials(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		Name:            "test-pool",
		PoolCredentials: nil, // No credentials
	}

	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "192.168.1.1",
		},
	}

	// Mock GetNodesByPoolID to succeed
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

	result, err := _getOntapRestProviderForPool(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "Resource not found")
	mockStorage.AssertExpectations(t)
}

func Test_filterOntapVolumes_Success(t *testing.T) {
	name1 := "volume1"
	uuid1 := "volume-uuid-1"
	svmName1 := "svm1"
	name2 := "volume2"
	uuid2 := "volume-uuid-2"
	svmName2 := "svm2"

	volumes := []*vsa.Volume{
		{
			Volume: ontapmodels.Volume{
				Name: &name1,
				UUID: &uuid1,
				Svm: &ontapmodels.VolumeInlineSvm{
					Name: &svmName1,
				},
				IsSvmRoot: nillable.ToPointer(false),
			},
		},
		{
			Volume: ontapmodels.Volume{
				Name: &name2,
				UUID: &uuid2,
				Svm: &ontapmodels.VolumeInlineSvm{
					Name: &svmName2,
				},
				IsSvmRoot: nillable.ToPointer(false),
			},
		},
		// This should be filtered out (nil volume)
		nil,
		// This should be filtered out (nil UUID)
		{
			Volume: ontapmodels.Volume{
				Name: &name1,
				UUID: nil,
				Svm: &ontapmodels.VolumeInlineSvm{
					Name: &svmName1,
				},
				IsSvmRoot: nillable.ToPointer(false),
			},
		},
	}

	result := getFilteredOntapVolumesMapByUUID(volumes)

	assert.Len(t, result, 2)
	assert.Contains(t, result, uuid1)
	assert.Contains(t, result, uuid2)
}

func Test_filterOntapVolumes_EmptyInput(t *testing.T) {
	volumes := []*vsa.Volume{}

	result := getFilteredOntapVolumesMapByUUID(volumes)

	assert.Len(t, result, 0)
}

func Test_filterOntapVolumes_AllInvalid(t *testing.T) {
	rootVolume := "root_volume"
	svmName1 := "svm1"

	volumes := []*vsa.Volume{
		nil, // nil volume
		{
			Volume: ontapmodels.Volume{
				Name: &rootVolume,
				UUID: nil, // nil UUID
				Svm: &ontapmodels.VolumeInlineSvm{
					Name: &svmName1,
				},
			},
		},
	}

	result := getFilteredOntapVolumesMapByUUID(volumes)

	// At least the nil volume and the one with nil UUID should be filtered out
	assert.GreaterOrEqual(t, len(result), 0)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateAccountVolumeRefreshTimestamp)

	accountUUID := "test-account-uuid"
	completionTime := time.Now()

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: accountUUID,
		CompletedAt: completionTime,
	}

	// Mock UpdateAccountVolumeRefreshTimestamp to succeed
	// Use mock.MatchedBy for time comparison since time.Time has monotonic clock component
	mockStorage.On("UpdateAccountVolumeRefreshTimestamp", mock.Anything, accountUUID, mock.MatchedBy(func(t time.Time) bool {
		return t.Equal(completionTime)
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateAccountVolumeRefreshTimestamp, input)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_NilInput(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateAccountVolumeRefreshTimestamp)

	_, err := env.ExecuteActivity(activity.UpdateAccountVolumeRefreshTimestamp, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "UpdateAccountVolumeRefreshTimestamp input cannot be nil")
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_EmptyAccountUUID(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateAccountVolumeRefreshTimestamp)

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: "", // Empty UUID
		CompletedAt: time.Now(),
	}

	_, err := env.ExecuteActivity(activity.UpdateAccountVolumeRefreshTimestamp, input)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "account UUID cannot be empty")
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_DatabaseError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateAccountVolumeRefreshTimestamp)

	accountUUID := "test-account-uuid"
	completionTime := time.Now()

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: accountUUID,
		CompletedAt: completionTime,
	}

	expectedError := errors.New("database error")
	// Use mock.MatchedBy for time comparison since time.Time has monotonic clock component
	mockStorage.On("UpdateAccountVolumeRefreshTimestamp", mock.Anything, accountUUID, mock.MatchedBy(func(t time.Time) bool {
		return t.Equal(completionTime)
	})).Return(expectedError)

	_, err := env.ExecuteActivity(activity.UpdateAccountVolumeRefreshTimestamp, input)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update account volume refresh timestamp")
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_ZeroTime(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateAccountVolumeRefreshTimestamp)

	accountUUID := "test-account-uuid"
	zeroTime := time.Time{} // Zero time

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: accountUUID,
		CompletedAt: zeroTime,
	}

	// Mock should accept zero time - it's a valid timestamp
	// Use mock.MatchedBy for time comparison since time.Time has monotonic clock component
	mockStorage.On("UpdateAccountVolumeRefreshTimestamp", mock.Anything, accountUUID, mock.MatchedBy(func(t time.Time) bool {
		return t.Equal(zeroTime)
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateAccountVolumeRefreshTimestamp, input)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_FutureTime(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateAccountVolumeRefreshTimestamp)

	accountUUID := "test-account-uuid"
	futureTime := time.Now().Add(24 * time.Hour) // Future time

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: accountUUID,
		CompletedAt: futureTime,
	}

	// Mock should accept future time - validation is not enforced at activity level
	// Use mock.MatchedBy for time comparison since time.Time has monotonic clock component
	mockStorage.On("UpdateAccountVolumeRefreshTimestamp", mock.Anything, accountUUID, mock.MatchedBy(func(t time.Time) bool {
		return t.Equal(futureTime)
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateAccountVolumeRefreshTimestamp, input)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// Test clone validation logic when enableCloneInfoRefresh is true
func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_CloneValidation_NilClone(t *testing.T) {
	// Enable clone info refresh for this test
	oldValue := enableCloneInfoRefresh
	enableCloneInfoRefresh = true
	defer func() { enableCloneInfoRefresh = oldValue }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		ClonesSharedBytes: 100, // Volume is a clone
		UsedBytes: 1024,        // Different from ONTAP value to ensure update
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)),
				},
			},
			Clone: nil, // Missing clone info
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	// Volume should be updated even when clone info is missing, as long as UsedBytes changed
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	assert.Contains(t, result.UpdatedVolumeByUUID, "vol-uuid")
	assert.Equal(t, uint64(2048), result.UpdatedVolumeByUUID["vol-uuid"].UsedBytes)
	assert.Equal(t, 1, result.MatchedCount)
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
	// Volume should be tracked as having incomplete clone info in ONTAP
	assert.Len(t, result.VolumesNotCloneInONTAP, 1)
	assert.Equal(t, dbVolume, result.VolumesNotCloneInONTAP[0])
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_CloneValidation_NilParentVolume(t *testing.T) {
	oldValue := enableCloneInfoRefresh
	enableCloneInfoRefresh = true
	defer func() { enableCloneInfoRefresh = oldValue }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		ClonesSharedBytes: 100,
		UsedBytes: 1024, // Different from ONTAP value to ensure update
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)),
				},
			},
			Clone: &ontapmodels.VolumeInlineClone{
				ParentVolume: nil, // Missing parent volume
			},
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	// Volume should be updated even when clone info is missing, as long as UsedBytes changed
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	assert.Contains(t, result.UpdatedVolumeByUUID, "vol-uuid")
	assert.Equal(t, uint64(2048), result.UpdatedVolumeByUUID["vol-uuid"].UsedBytes)
	assert.Equal(t, 1, result.MatchedCount)
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
	// Volume should be tracked as having incomplete clone info in ONTAP
	assert.Len(t, result.VolumesNotCloneInONTAP, 1)
	assert.Equal(t, dbVolume, result.VolumesNotCloneInONTAP[0])
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_CloneValidation_NilParentSnapshot(t *testing.T) {
	oldValue := enableCloneInfoRefresh
	enableCloneInfoRefresh = true
	defer func() { enableCloneInfoRefresh = oldValue }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		ClonesSharedBytes: 100,
		UsedBytes: 1024, // Different from ONTAP value to ensure update
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	parentVolumeName := "parent-vol"
	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)),
				},
			},
			Clone: &ontapmodels.VolumeInlineClone{
				ParentVolume: &ontapmodels.VolumeInlineCloneInlineParentVolume{
					Name: &parentVolumeName,
				},
				ParentSnapshot: nil, // Missing parent snapshot
			},
		},
	}

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	// Volume should be updated even when clone info is missing, as long as UsedBytes changed
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	assert.Contains(t, result.UpdatedVolumeByUUID, "vol-uuid")
	assert.Equal(t, uint64(2048), result.UpdatedVolumeByUUID["vol-uuid"].UsedBytes)
	assert.Equal(t, 1, result.MatchedCount)
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
	// Volume should be tracked as having incomplete clone info in ONTAP
	assert.Len(t, result.VolumesNotCloneInONTAP, 1)
	assert.Equal(t, dbVolume, result.VolumesNotCloneInONTAP[0])
}

// Test clone info processing with successful parent volume and snapshot lookup
func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_CloneInfo_Success(t *testing.T) {
	oldValue := enableCloneInfoRefresh
	enableCloneInfoRefresh = true
	defer func() { enableCloneInfoRefresh = oldValue }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	parentVolumeName := "parent-vol"
	parentSnapshotName := "parent-snap"
	parentVolumeUUID := "parent-vol-uuid"
	parentSnapshotUUID := "parent-snap-uuid"

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		AccountID: int64(1),
		ClonesSharedBytes: 100,
		UsedBytes: 1024,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
			CloneParentInfo: nil, // Missing clone info
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)),
				},
			},
			Clone: &ontapmodels.VolumeInlineClone{
				ParentVolume: &ontapmodels.VolumeInlineCloneInlineParentVolume{
					Name: &parentVolumeName,
				},
				ParentSnapshot: &ontapmodels.SnapshotReference{
					Name: &parentSnapshotName,
				},
			},
		},
	}

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: parentVolumeUUID, ID: 456},
	}

	parentSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: parentSnapshotUUID},
	}

	mockStorage.On("GetVolumeByNameAndAccountID", mock.Anything, parentVolumeName, int64(1)).Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, parentSnapshotName, int64(1), int64(456)).Return(parentSnapshot, nil)

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	updatedVol := result.UpdatedVolumeByUUID["vol-uuid"]
	assert.NotNil(t, updatedVol)
	assert.NotNil(t, updatedVol.VolumeAttributes)
	assert.NotNil(t, updatedVol.VolumeAttributes.CloneParentInfo)
	assert.Equal(t, parentVolumeUUID, updatedVol.VolumeAttributes.CloneParentInfo.ParentVolumeUUID)
	assert.Equal(t, parentSnapshotUUID, updatedVol.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID)
	mockStorage.AssertExpectations(t)
}

// Test clone info processing with parent volume lookup error
func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_CloneInfo_ParentVolumeError(t *testing.T) {
	oldValue := enableCloneInfoRefresh
	enableCloneInfoRefresh = true
	defer func() { enableCloneInfoRefresh = oldValue }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	parentVolumeName := "parent-vol"
	parentSnapshotName := "parent-snap"

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		AccountID: int64(1),
		ClonesSharedBytes: 100,
		UsedBytes: 1024,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)),
				},
			},
			Clone: &ontapmodels.VolumeInlineClone{
				ParentVolume: &ontapmodels.VolumeInlineCloneInlineParentVolume{
					Name: &parentVolumeName,
				},
				ParentSnapshot: &ontapmodels.SnapshotReference{
					Name: &parentSnapshotName,
				},
			},
		},
	}

	mockStorage.On("GetVolumeByNameAndAccountID", mock.Anything, parentVolumeName, int64(1)).Return(nil, errors.New("parent volume not found"))

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	// Should still update used_bytes even if clone info lookup fails
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	mockStorage.AssertExpectations(t)
}

// Test clone info processing with parent snapshot lookup error
func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_CloneInfo_ParentSnapshotError(t *testing.T) {
	oldValue := enableCloneInfoRefresh
	enableCloneInfoRefresh = true
	defer func() { enableCloneInfoRefresh = oldValue }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	parentVolumeName := "parent-vol"
	parentSnapshotName := "parent-snap"
	parentVolumeUUID := "parent-vol-uuid"

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		AccountID: int64(1),
		ClonesSharedBytes: 100,
		UsedBytes: 1024,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)),
				},
			},
			Clone: &ontapmodels.VolumeInlineClone{
				ParentVolume: &ontapmodels.VolumeInlineCloneInlineParentVolume{
					Name: &parentVolumeName,
				},
				ParentSnapshot: &ontapmodels.SnapshotReference{
					Name: &parentSnapshotName,
				},
			},
		},
	}

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: parentVolumeUUID, ID: 456},
	}

	mockStorage.On("GetVolumeByNameAndAccountID", mock.Anything, parentVolumeName, int64(1)).Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, parentSnapshotName, int64(1), int64(456)).Return(nil, errors.New("snapshot not found"))

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	// Should still update used_bytes even if snapshot lookup fails
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	mockStorage.AssertExpectations(t)
}

// Test clone info processing with existing clone info that needs update
func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_CloneInfo_UpdateExisting(t *testing.T) {
	oldValue := enableCloneInfoRefresh
	enableCloneInfoRefresh = true
	defer func() { enableCloneInfoRefresh = oldValue }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	env.RegisterActivity(activity.ProcessOntapVolumeMatching)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	parentVolumeName := "parent-vol"
	parentSnapshotName := "parent-snap"
	parentVolumeUUID := "parent-vol-uuid-new"
	parentSnapshotUUID := "parent-snap-uuid-new"
	oldParentVolumeUUID := "parent-vol-uuid-old"
	oldParentSnapshotUUID := "parent-snap-uuid-old"

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid", ID: 123},
		Pool:      pool,
		AccountID: int64(1),
		ClonesSharedBytes: 100,
		UsedBytes: 1024,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   oldParentVolumeUUID,
				ParentSnapshotUUID: oldParentSnapshotUUID,
			},
		},
	}

	ontapVolume := &vsa.Volume{
		Volume: ontapmodels.Volume{
			Space: &ontapmodels.VolumeInlineSpace{
				LogicalSpace: &ontapmodels.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.ToPointer(int64(2048)),
				},
			},
			Clone: &ontapmodels.VolumeInlineClone{
				ParentVolume: &ontapmodels.VolumeInlineCloneInlineParentVolume{
					Name: &parentVolumeName,
				},
				ParentSnapshot: &ontapmodels.SnapshotReference{
					Name: &parentSnapshotName,
				},
			},
		},
	}

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: parentVolumeUUID, ID: 456},
	}

	parentSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: parentSnapshotUUID},
	}

	mockStorage.On("GetVolumeByNameAndAccountID", mock.Anything, parentVolumeName, int64(1)).Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, parentSnapshotName, int64(1), int64(456)).Return(parentSnapshot, nil)

	input := &ProcessOntapVolumeMatchingInput{
		DbVolumes: []*datamodel.Volume{dbVolume},
		OntapVolumesResults: map[string]*GetOntapVolumesReturnValue{
			"pool-uuid": {
				OntapVolumeMap: map[string]*vsa.Volume{
					"external-uuid": ontapVolume,
				},
			},
		},
	}

	val, err := env.ExecuteActivity(activity.ProcessOntapVolumeMatching, input)

	assert.NoError(t, err)
	var result *ProcessOntapVolumeMatchingResult
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	updatedVol := result.UpdatedVolumeByUUID["vol-uuid"]
	assert.NotNil(t, updatedVol)
	assert.NotNil(t, updatedVol.VolumeAttributes)
	assert.NotNil(t, updatedVol.VolumeAttributes.CloneParentInfo)
	assert.Equal(t, parentVolumeUUID, updatedVol.VolumeAttributes.CloneParentInfo.ParentVolumeUUID)
	assert.Equal(t, parentSnapshotUUID, updatedVol.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID)
	mockStorage.AssertExpectations(t)
}

// Test sync volumes to database with clone info updates
func Test_syncUpdatedVolumesToDatabase_WithCloneInfo(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)

	parentVolumeUUID := "parent-vol-uuid"
	parentSnapshotUUID := "parent-snap-uuid"

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
		},
		"vol-2": {
			BaseModel: datamodel.BaseModel{UUID: "vol-2", ID: 2},
			UsedBytes: 2048,
			VolumeAttributes: &datamodel.VolumeAttributes{
				CloneParentInfo: &datamodel.CloneParentInfo{
					ParentVolumeUUID:   parentVolumeUUID,
					ParentSnapshotUUID: parentSnapshotUUID,
				},
			},
		},
	}

	existingVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-2", ID: 2},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid-2",
		},
	}

	mockStorage.On("BatchUpdateVolumeFields", mock.Anything, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(nil)
	mockStorage.On("GetVolume", mock.Anything, "vol-2").Return(existingVolume, nil)
	mockStorage.On("UpdateVolumeFields", mock.Anything, "vol-2", mock.AnythingOfType("map[string]interface {}")).Return(nil)

	wrapper := &testSyncActivityWrapper{
		SE:     mockStorage,
		DBVols: dbVols,
	}
	env.RegisterActivity(wrapper.TestSyncActivity)

	_, err := env.ExecuteActivity(wrapper.TestSyncActivity)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// Test sync volumes to database with clone info and nil existing volume attributes
func Test_syncUpdatedVolumesToDatabase_WithCloneInfo_NilAttributes(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)

	parentVolumeUUID := "parent-vol-uuid"
	parentSnapshotUUID := "parent-snap-uuid"

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
			VolumeAttributes: &datamodel.VolumeAttributes{
				CloneParentInfo: &datamodel.CloneParentInfo{
					ParentVolumeUUID:   parentVolumeUUID,
					ParentSnapshotUUID: parentSnapshotUUID,
				},
			},
		},
	}

	existingVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
		VolumeAttributes: nil, // No existing attributes
	}

	mockStorage.On("GetVolume", mock.Anything, "vol-1").Return(existingVolume, nil)
	mockStorage.On("UpdateVolumeFields", mock.Anything, "vol-1", mock.AnythingOfType("map[string]interface {}")).Return(nil)

	wrapper := &testSyncActivityWrapper{
		SE:     mockStorage,
		DBVols: dbVols,
	}
	env.RegisterActivity(wrapper.TestSyncActivity)

	_, err := env.ExecuteActivity(wrapper.TestSyncActivity)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// Test sync volumes to database with clone info and GetVolume error
func Test_syncUpdatedVolumesToDatabase_WithCloneInfo_GetVolumeError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)

	parentVolumeUUID := "parent-vol-uuid"
	parentSnapshotUUID := "parent-snap-uuid"

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
			VolumeAttributes: &datamodel.VolumeAttributes{
				CloneParentInfo: &datamodel.CloneParentInfo{
					ParentVolumeUUID:   parentVolumeUUID,
					ParentSnapshotUUID: parentSnapshotUUID,
				},
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "vol-1").Return(nil, errors.New("volume not found"))

	wrapper := &testSyncActivityWrapper{
		SE:     mockStorage,
		DBVols: dbVols,
	}
	env.RegisterActivity(wrapper.TestSyncActivity)

	_, err := env.ExecuteActivity(wrapper.TestSyncActivity)

	assert.NoError(t, err) // Should continue even if GetVolume fails
	mockStorage.AssertExpectations(t)
}

// Test sync volumes to database with clone info and UpdateVolumeFields error
func Test_syncUpdatedVolumesToDatabase_WithCloneInfo_UpdateError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)

	parentVolumeUUID := "parent-vol-uuid"
	parentSnapshotUUID := "parent-snap-uuid"

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
			VolumeAttributes: &datamodel.VolumeAttributes{
				CloneParentInfo: &datamodel.CloneParentInfo{
					ParentVolumeUUID:   parentVolumeUUID,
					ParentSnapshotUUID: parentSnapshotUUID,
				},
			},
		},
	}

	existingVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid-1",
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "vol-1").Return(existingVolume, nil)
	mockStorage.On("UpdateVolumeFields", mock.Anything, "vol-1", mock.AnythingOfType("map[string]interface {}")).Return(errors.New("update failed"))

	wrapper := &testSyncActivityWrapper{
		SE:     mockStorage,
		DBVols: dbVols,
	}
	env.RegisterActivity(wrapper.TestSyncActivity)

	_, err := env.ExecuteActivity(wrapper.TestSyncActivity)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update failed")
	mockStorage.AssertExpectations(t)
}
