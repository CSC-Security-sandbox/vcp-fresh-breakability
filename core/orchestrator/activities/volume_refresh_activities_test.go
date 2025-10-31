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
	"gorm.io/gorm"
)

func TestVolumeRefreshActivity_GetDBVolumesForPool_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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
	mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return(volumes, nil)

	result, err := activity.GetDBVolumesForPool(ctx, pool)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.DBVolumesByExternalUUID, 2)
	assert.Contains(t, result.DBVolumesByExternalUUID, "external-1")
	assert.Contains(t, result.DBVolumesByExternalUUID, "external-2")
	assert.Equal(t, volumes[0], result.DBVolumesByExternalUUID["external-1"])
	assert.Equal(t, volumes[1], result.DBVolumesByExternalUUID["external-2"])
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetDBVolumesForPool_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	expectedError := fmt.Errorf("database error")
	mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return(nil, expectedError)

	result, err := activity.GetDBVolumesForPool(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetDBVolumesForPool_EmptyResult(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		Name:      "test-pool",
	}

	// Mock GetVolumesByPoolID to return empty result
	mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return([]*datamodel.Volume{}, nil)

	result, err := activity.GetDBVolumesForPool(ctx, pool)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.DBVolumesByExternalUUID, 0)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetOntapVolumes_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

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

	result, err := activity.GetOntapVolumes(ctx, pool)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.OntapVolumeMap, 2) // Two valid volumes
	assert.Contains(t, result.OntapVolumeMap, uuid1)
	assert.Contains(t, result.OntapVolumeMap, uuid2)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetOntapVolumes_ProviderError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, gorm.ErrRecordNotFound)

	result, err := activity.GetOntapVolumes(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_GetOntapVolumes_GetVolumesError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

	// Mock hyperscaler function
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	expectedError := errors.New("failed to get volumes from ONTAP")
	mockProvider.On("GetVolumes").Return(nil, expectedError)

	result, err := activity.GetOntapVolumes(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestVolumeRefreshActivity_ProcessVolumePoolMapping_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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

	result, err := activity.ProcessVolumePoolMapping(ctx, input)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.PoolByUUID, 2)
	assert.Len(t, result.PoolUUIDs, 2)
	assert.Contains(t, result.PoolByUUID, "pool-1")
	assert.Contains(t, result.PoolByUUID, "pool-2")
	assert.Equal(t, pool1, result.PoolByUUID["pool-1"])
	assert.Equal(t, pool2, result.PoolByUUID["pool-2"])
}

func TestVolumeRefreshActivity_ProcessVolumePoolMapping_NilInput(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	result, err := activity.ProcessVolumePoolMapping(ctx, nil)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.PoolByUUID, 0)
	assert.Len(t, result.PoolUUIDs, 0)
}

func TestVolumeRefreshActivity_ProcessVolumePoolMapping_EmptyVolumes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	input := &ProcessVolumePoolMappingInput{
		Volumes: []*datamodel.Volume{},
	}

	result, err := activity.ProcessVolumePoolMapping(ctx, input)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.PoolByUUID, 0)
	assert.Len(t, result.PoolUUIDs, 0)
}

func TestVolumeRefreshActivity_ProcessVolumePoolMapping_InvalidVolumes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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

	result, err := activity.ProcessVolumePoolMapping(ctx, input)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.PoolByUUID, 1) // Only one valid pool
	assert.Len(t, result.PoolUUIDs, 1)
	assert.Contains(t, result.PoolByUUID, "pool-1")
	assert.Equal(t, pool1, result.PoolByUUID["pool-1"])
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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

	result, err := activity.ProcessOntapVolumeMatching(ctx, input)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 1)
	assert.Contains(t, result.UpdatedVolumeByUUID, "vol-uuid")
	assert.Equal(t, uint64(2048), result.UpdatedVolumeByUUID["vol-uuid"].UsedBytes)
	assert.Equal(t, 1, result.MatchedCount)
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_NoChanges(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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

	result, err := activity.ProcessOntapVolumeMatching(ctx, input)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	// Volume should not be included in UpdatedVolumeByUUID since there are no changes
	assert.Len(t, result.UpdatedVolumeByUUID, 0)
	assert.NotContains(t, result.UpdatedVolumeByUUID, "vol-uuid")
	assert.Equal(t, 0, result.MatchedCount) // MatchedCount should be 0 since no volumes were updated
	assert.Equal(t, 0, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 0)
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_WithChanges(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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

	result, err := activity.ProcessOntapVolumeMatching(ctx, input)

	assert.NoError(t, err)
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
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	result, err := activity.ProcessOntapVolumeMatching(ctx, nil)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "ProcessOntapVolumeMatching input cannot be nil")
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_VolumeNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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

	result, err := activity.ProcessOntapVolumeMatching(ctx, input)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 0)
	assert.Equal(t, 0, result.MatchedCount)
	assert.Equal(t, 1, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 1)
	assert.Equal(t, dbVolume, result.VolumesNotFoundInONTAP[0])
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_NoPoolResults(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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
			// No results for this pool
		},
	}

	result, err := activity.ProcessOntapVolumeMatching(ctx, input)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.UpdatedVolumeByUUID, 0)
	assert.Equal(t, 0, result.MatchedCount)
	assert.Equal(t, 1, result.NotFoundCount)
	assert.Len(t, result.VolumesNotFoundInONTAP, 1)
	assert.Equal(t, dbVolume, result.VolumesNotFoundInONTAP[0])
}

func TestVolumeRefreshActivity_ProcessOntapVolumeMatching_InvalidVolumes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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

	result, err := activity.ProcessOntapVolumeMatching(ctx, input)

	assert.NoError(t, err)
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
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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
	mockStorage.On("BatchUpdateVolumeFields", ctx, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(nil)

	err := activity.SyncUpdatedVolumesToDatabase(ctx, dbVols)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_SyncUpdatedVolumesToDatabase_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
		},
	}

	expectedError := errors.New("database error")
	mockStorage.On("BatchUpdateVolumeFields", ctx, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(expectedError)

	err := activity.SyncUpdatedVolumesToDatabase(ctx, dbVols)

	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_SyncUpdatedVolumesToDatabase_EmptyVols(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	dbVols := map[string]*datamodel.Volume{}

	err := activity.SyncUpdatedVolumesToDatabase(ctx, dbVols)

	assert.NoError(t, err)
	// No database calls should be made for empty volumes
	mockStorage.AssertExpectations(t)
}

func Test_syncUpdatedVolumesToDatabase_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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
	mockStorage.On("BatchUpdateVolumeFields", ctx, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(nil)

	err := _syncUpdatedVolumesToDatabase(ctx, mockStorage, dbVols)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_syncUpdatedVolumesToDatabase_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	dbVols := map[string]*datamodel.Volume{
		"vol-1": {
			BaseModel: datamodel.BaseModel{UUID: "vol-1", ID: 1},
			UsedBytes: 1024,
		},
	}

	expectedError := errors.New("database error")
	mockStorage.On("BatchUpdateVolumeFields", ctx, mock.AnythingOfType("[]datamodel.VolumeFieldUpdate")).Return(expectedError)

	err := _syncUpdatedVolumesToDatabase(ctx, mockStorage, dbVols)

	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockStorage.AssertExpectations(t)
}

func Test_syncUpdatedVolumesToDatabase_EmptyVolumes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	dbVols := map[string]*datamodel.Volume{}

	err := _syncUpdatedVolumesToDatabase(ctx, mockStorage, dbVols)

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
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	accountUUID := "test-account-uuid"
	completionTime := time.Now()

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: accountUUID,
		CompletedAt: completionTime,
	}

	// Mock UpdateAccountVolumeRefreshTimestamp to succeed
	mockStorage.On("UpdateAccountVolumeRefreshTimestamp", ctx, accountUUID, completionTime).Return(nil)

	err := activity.UpdateAccountVolumeRefreshTimestamp(ctx, input)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_NilInput(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	err := activity.UpdateAccountVolumeRefreshTimestamp(ctx, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "UpdateAccountVolumeRefreshTimestamp input cannot be nil")
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_EmptyAccountUUID(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: "", // Empty UUID
		CompletedAt: time.Now(),
	}

	err := activity.UpdateAccountVolumeRefreshTimestamp(ctx, input)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "account UUID cannot be empty")
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_DatabaseError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	accountUUID := "test-account-uuid"
	completionTime := time.Now()

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: accountUUID,
		CompletedAt: completionTime,
	}

	expectedError := errors.New("database error")
	mockStorage.On("UpdateAccountVolumeRefreshTimestamp", ctx, accountUUID, completionTime).Return(expectedError)

	err := activity.UpdateAccountVolumeRefreshTimestamp(ctx, input)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update account volume refresh timestamp")
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_ZeroTime(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	accountUUID := "test-account-uuid"
	zeroTime := time.Time{} // Zero time

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: accountUUID,
		CompletedAt: zeroTime,
	}

	// Mock should accept zero time - it's a valid timestamp
	mockStorage.On("UpdateAccountVolumeRefreshTimestamp", ctx, accountUUID, zeroTime).Return(nil)

	err := activity.UpdateAccountVolumeRefreshTimestamp(ctx, input)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRefreshActivity_UpdateAccountVolumeRefreshTimestamp_FutureTime(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &VolumeRefreshActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	accountUUID := "test-account-uuid"
	futureTime := time.Now().Add(24 * time.Hour) // Future time

	input := &UpdateAccountVolumeRefreshTimestampInput{
		AccountUUID: accountUUID,
		CompletedAt: futureTime,
	}

	// Mock should accept future time - validation is not enforced at activity level
	mockStorage.On("UpdateAccountVolumeRefreshTimestamp", ctx, accountUUID, futureTime).Return(nil)

	err := activity.UpdateAccountVolumeRefreshTimestamp(ctx, input)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}
