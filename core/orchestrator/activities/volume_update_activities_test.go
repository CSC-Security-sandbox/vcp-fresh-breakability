package activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/testsuite"
)

func TestUpdateVolumeInONTAP_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateVolumeInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "default-snapshot-policy",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 1024,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        "auto",
			RetrievalPolicy:      "default",
			CoolingThresholdDays: 10,
		},
	}
	node := &models.Node{}

	mockProvider.On("UpdateVolume", vsa.UpdateVolumeParams{
		UUID: volume.VolumeAttributes.ExternalUUID,
		Size: params.QuotaInBytes,
		TieringPolicy: &vsa.TieringPolicy{
			CoolnessPeriod:            int64(params.AutoTieringPolicy.CoolingThresholdDays),
			CoolAccessRetrievalPolicy: params.AutoTieringPolicy.RetrievalPolicy,
			CoolAccessTieringPolicy:   params.AutoTieringPolicy.TieringPolicy,
		},
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInONTAP, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInONTAP_WithUnixPermissions(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateVolumeInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2048,
		FileProperties: &models.FileProperties{
			UnixPermissions: "0755",
		},
	}
	node := &models.Node{}

	mockProvider.On("UpdateVolume", mock.MatchedBy(func(p vsa.UpdateVolumeParams) bool {
		return p.UnixPermissions != nil && *p.UnixPermissions == "0755" && p.Size == params.QuotaInBytes
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInONTAP, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInONTAPWithSnapshotPolicy_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateVolumeInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "default-snapshot-policy",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 1024,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        "auto",
			RetrievalPolicy:      "default",
			CoolingThresholdDays: 10,
		},
		SnapshotPolicy: &models.SnapshotPolicy{
			Name: "default-snapshot-policy",
		},
	}
	node := &models.Node{}

	mockProvider.On("UpdateVolume", vsa.UpdateVolumeParams{
		UUID:               volume.VolumeAttributes.ExternalUUID,
		Size:               params.QuotaInBytes,
		SnapshotPolicyName: "default-snapshot-policy",
		TieringPolicy: &vsa.TieringPolicy{
			CoolnessPeriod:            int64(params.AutoTieringPolicy.CoolingThresholdDays),
			CoolAccessRetrievalPolicy: params.AutoTieringPolicy.RetrievalPolicy,
			CoolAccessTieringPolicy:   params.AutoTieringPolicy.TieringPolicy,
		},
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInONTAP, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInONTAP_DataProtectionVolume_Snapshot_Skip(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateVolumeInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:     "uuid-123",
			IsDataProtection: true,
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 1024,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        "auto",
			RetrievalPolicy:      "default",
			CoolingThresholdDays: 10,
		},
	}
	node := &models.Node{}

	mockProvider.On("UpdateVolume", vsa.UpdateVolumeParams{
		UUID: volume.VolumeAttributes.ExternalUUID,
		Size: params.QuotaInBytes,
		TieringPolicy: &vsa.TieringPolicy{
			CoolnessPeriod:            int64(params.AutoTieringPolicy.CoolingThresholdDays),
			CoolAccessRetrievalPolicy: params.AutoTieringPolicy.RetrievalPolicy,
			CoolAccessTieringPolicy:   params.AutoTieringPolicy.TieringPolicy,
		},
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInONTAP, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInONTAP_Failure(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateVolumeInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2048,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        "auto",
			RetrievalPolicy:      "default",
			CoolingThresholdDays: 5,
		},
	}
	node := &models.Node{}
	expectedErr := errors.New("update failed")

	mockProvider.On("UpdateVolume", vsa.UpdateVolumeParams{
		UUID: volume.VolumeAttributes.ExternalUUID,
		Size: int64(params.QuotaInBytes),
		TieringPolicy: &vsa.TieringPolicy{
			CoolnessPeriod:            int64(params.AutoTieringPolicy.CoolingThresholdDays),
			CoolAccessRetrievalPolicy: params.AutoTieringPolicy.RetrievalPolicy,
			CoolAccessTieringPolicy:   params.AutoTieringPolicy.TieringPolicy,
		},
	}).Return(expectedErr)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInONTAP, volume, params, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateLun_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateLun)

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
				LunName: "lun_test-volume",
			},
		},
	}
	node := &models.Node{}

	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       1073729479, // Match the actual value used in the code
	}).Return(nil)

	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(&vsa.LunResponse{}, nil)
	ontapRes := &vsa.VolumeResponse{
		Size:         BytesPerGB,
		AFSSize:      BytesPerGB,
		MetadataSize: 12345,
	}

	val, err := env.ExecuteActivity(activity.UpdateLun, volume, ontapRes, node)
	assert.NoError(t, err)
	var result *vsa.LunResponse
	_ = val.Get(&result)
	mockProvider.AssertExpectations(t)
}

func TestUpdateLunWithBD_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateLun)

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{LunUUID: "lun-uuid-123", Name: "lun_test-volume"},
			},
		},
	}
	node := &models.Node{}

	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       1073729479,
	}).Return(nil)

	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(&vsa.LunResponse{}, nil)
	ontapRes := &vsa.VolumeResponse{
		Size:         BytesPerGB,
		AFSSize:      BytesPerGB,
		MetadataSize: 12345,
	}

	val, err := env.ExecuteActivity(activity.UpdateLun, volume, ontapRes, node)
	assert.NoError(t, err)
	var result *vsa.LunResponse
	_ = val.Get(&result)
	mockProvider.AssertExpectations(t)
}

func TestUpdateLun_Failure(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateLun)

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
				LunName: "lun_test-volume",
			},
		},
	}
	node := &models.Node{}
	expectedErr := errors.New("lun update failed")

	// Mock LunGet call that happens before LunUpdate
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(&vsa.LunResponse{
		Size: 1024, // Some existing size
	}, nil)

	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       1073729479,
	}).Return(expectedErr)
	ontapRes := &vsa.VolumeResponse{
		Size:         BytesPerGB,
		AFSSize:      BytesPerGB,
		MetadataSize: 12345,
	}
	_, err := env.ExecuteActivity(activity.UpdateLun, volume, ontapRes, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error updating volume - Cannot update the volume with the specified size. Please increase the volume size")
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInDB_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVolumeInDB)

	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 4096,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        "auto",
			CoolingThresholdDays: 7,
			RetrievalPolicy:      "default",
		},
	}
	updatedFields := map[string]interface{}{
		"size":          int64(4096),
		"state_details": models.LifeCycleStateAvailableDetails,
	}

	prepareFieldsForUpdate = func(ctx context.Context, se database.Storage, volume *datamodel.Volume, params *common.UpdateVolumeParams) (map[string]interface{}, error) {
		return updatedFields, nil
	}
	defer func() { prepareFieldsForUpdate = getUpdatedFieldsFromParams }()

	mockStorage.On("UpdateVolumeFields", mock.Anything, volume.UUID, updatedFields).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInDB, volume, params)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeInDB_FailureWithPrepareField(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVolumeInDB)

	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
	params := &common.UpdateVolumeParams{QuotaInBytes: 4096}

	prepareFieldsForUpdate = func(ctx context.Context, se database.Storage, volume *datamodel.Volume, params *common.UpdateVolumeParams) (map[string]interface{}, error) {
		return nil, errors.New("failed to prepare fields for update")
	}
	defer func() { prepareFieldsForUpdate = getUpdatedFieldsFromParams }()

	_, err := env.ExecuteActivity(activity.UpdateVolumeInDB, volume, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare fields for update")
	mockStorage.AssertExpectations(t)
}

func TestUpdateLun_ConflictError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateLun)

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
				LunName: "lun_test-volume",
			},
		},
	}

	node := &models.Node{}

	conflictErr := errors.NewConflictErr("conflict")

	// Mock LunGet call that happens before LunUpdate
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(&vsa.LunResponse{
		Size: 1024, // Some existing size
	}, nil)

	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       1073729479,
	}).Return(conflictErr)
	ontapRes := &vsa.VolumeResponse{
		Size:         BytesPerGB,
		AFSSize:      BytesPerGB,
		MetadataSize: 12345,
	}
	_, err := env.ExecuteActivity(activity.UpdateLun, volume, ontapRes, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInDB_Failure(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVolumeInDB)

	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
	params := &common.UpdateVolumeParams{QuotaInBytes: 4096}
	updatedFields := map[string]interface{}{"size": int64(4096), "state_details": models.LifeCycleStateAvailableDetails}
	expectedErr := errors.New("update failed")

	// Patch prepareFieldsForUpdate to return our expected map
	originalGetUpdatedFields := prepareFieldsForUpdate
	prepareFieldsForUpdate = func(ctx context.Context, se database.Storage, volume *datamodel.Volume, params *common.UpdateVolumeParams) (map[string]interface{}, error) {
		return updatedFields, nil
	}
	defer func() { prepareFieldsForUpdate = originalGetUpdatedFields }()

	mockStorage.On("UpdateVolumeFields", mock.Anything, volume.UUID, updatedFields).Return(expectedErr)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInDB, volume, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockStorage.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams_WithBlockDevices_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name:   "existing-lun",
					OSType: "LINUX",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hg-uuid-1",
							HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
						},
					},
				},
			},
		},
	}

	params := &common.UpdateVolumeParams{
		BlockDevices: []*common.BlockDevice{
			{
				Name:        "updated-lun",
				OSType:      "WINDOWS",
				HostGroups:  []string{"hg-uuid-2", "hg-uuid-3"},
				SizeInBytes: 107374182400, // 100 GiB
			},
		},
	}

	expectedHostGroups := []*datamodel.HostGroup{
		{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid-2"},
			Name:      "hg2",
			State:     "READY",
			Hosts:     datamodel.Hosts{Hosts: []string{"iqn.1998-01.com.vmware:host2"}},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid-3"},
			Name:      "hg3",
			State:     "READY",
			Hosts:     datamodel.Hosts{Hosts: []string{"iqn.1998-01.com.vmware:host3"}},
		},
	}

	mockStorage.On("GetHostGroup", ctx, "hg-uuid-2", int64(1)).Return(expectedHostGroups[0], nil)
	mockStorage.On("GetHostGroup", ctx, "hg-uuid-3", int64(1)).Return(expectedHostGroups[1], nil)

	// Act
	result, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Contains(t, result, "volume_attributes")

	// Verify BlockDevices were updated
	assert.NotNil(t, volume.VolumeAttributes.BlockDevices)
	assert.Len(t, *volume.VolumeAttributes.BlockDevices, 1)

	blockDevice := (*volume.VolumeAttributes.BlockDevices)[0]
	assert.Equal(t, "updated-lun", blockDevice.Name)
	assert.Equal(t, "WINDOWS", blockDevice.OSType)
	assert.Equal(t, int64(107374182400), blockDevice.Size)
	assert.Len(t, blockDevice.HostGroupDetails, 2)

	// Verify host group details
	assert.Equal(t, "hg-uuid-2", blockDevice.HostGroupDetails[0].HostGroupUUID)
	assert.Equal(t, []string{"iqn.1998-01.com.vmware:host2"}, blockDevice.HostGroupDetails[0].HostQNs)
	assert.Equal(t, "hg-uuid-3", blockDevice.HostGroupDetails[1].HostGroupUUID)
	assert.Equal(t, []string{"iqn.1998-01.com.vmware:host3"}, blockDevice.HostGroupDetails[1].HostQNs)

	mockStorage.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams_WithBlockDevices_GetHostGroupError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name:   "existing-lun",
					OSType: "LINUX",
				},
			},
		},
	}

	params := &common.UpdateVolumeParams{
		BlockDevices: []*common.BlockDevice{
			{
				Name:        "updated-lun",
				OSType:      "WINDOWS",
				HostGroups:  []string{"hg-uuid-2"},
				SizeInBytes: 107374182400,
			},
		},
	}

	expectedError := errors.New("host group not found")

	mockStorage.On("GetHostGroup", ctx, "hg-uuid-2", int64(1)).Return(nil, expectedError)

	// Act
	result, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
	mockStorage.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams_WithBlockDevices_EmptyHostGroups(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name:   "existing-lun",
					OSType: "LINUX",
				},
			},
		},
	}

	params := &common.UpdateVolumeParams{
		BlockDevices: []*common.BlockDevice{
			{
				Name:        "updated-lun",
				OSType:      "WINDOWS",
				HostGroups:  []string{}, // Empty host groups
				SizeInBytes: 107374182400,
			},
		},
	}

	// Act
	result, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Contains(t, result, "volume_attributes")

	// Verify BlockDevices were updated
	assert.NotNil(t, volume.VolumeAttributes.BlockDevices)
	assert.Len(t, *volume.VolumeAttributes.BlockDevices, 1)

	blockDevice := (*volume.VolumeAttributes.BlockDevices)[0]
	assert.Equal(t, "updated-lun", blockDevice.Name)
	assert.Equal(t, "WINDOWS", blockDevice.OSType)
	assert.Equal(t, int64(107374182400), blockDevice.Size)
	assert.Len(t, blockDevice.HostGroupDetails, 0) // Should be empty

	mockStorage.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams_WithBlockProperties_Fallback(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				OSType: "LINUX",
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "hg-uuid-1",
						HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
					},
				},
			},
		},
	}

	params := &common.UpdateVolumeParams{
		BlockDevices: []*common.BlockDevice{}, // Empty BlockDevices
		BlockProperties: &common.BlockPropertiesRequest{
			OSType:         "WINDOWS",
			HostGroupUUIDs: []string{"hg-uuid-2", "hg-uuid-3"},
		},
	}

	expectedHostGroups := []*datamodel.HostGroup{
		{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid-2"},
			Name:      "hg2",
			State:     "READY",
			Hosts:     datamodel.Hosts{Hosts: []string{"iqn.1998-01.com.vmware:host2"}},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid-3"},
			Name:      "hg3",
			State:     "READY",
			Hosts:     datamodel.Hosts{Hosts: []string{"iqn.1998-01.com.vmware:host3"}},
		},
	}

	mockStorage.On("GetHostGroup", ctx, "hg-uuid-2", int64(1)).Return(expectedHostGroups[0], nil)
	mockStorage.On("GetHostGroup", ctx, "hg-uuid-3", int64(1)).Return(expectedHostGroups[1], nil)

	// Act
	result, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Contains(t, result, "volume_attributes")

	// Verify BlockProperties were updated (fallback)
	assert.NotNil(t, volume.VolumeAttributes.BlockProperties)
	// Note: OSType is not updated in the fallback logic, only HostGroupDetails
	assert.Len(t, volume.VolumeAttributes.BlockProperties.HostGroupDetails, 2)

	// Verify host group details
	assert.Equal(t, "hg-uuid-2", volume.VolumeAttributes.BlockProperties.HostGroupDetails[0].HostGroupUUID)
	assert.Equal(t, []string{"iqn.1998-01.com.vmware:host2"}, volume.VolumeAttributes.BlockProperties.HostGroupDetails[0].HostQNs)
	assert.Equal(t, "hg-uuid-3", volume.VolumeAttributes.BlockProperties.HostGroupDetails[1].HostGroupUUID)
	assert.Equal(t, []string{"iqn.1998-01.com.vmware:host3"}, volume.VolumeAttributes.BlockProperties.HostGroupDetails[1].HostQNs)

	mockStorage.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams_WithBlockProperties_NilBlockProperties(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: nil, // Nil BlockProperties
		},
	}

	params := &common.UpdateVolumeParams{
		BlockDevices: []*common.BlockDevice{}, // Empty BlockDevices
		BlockProperties: &common.BlockPropertiesRequest{
			OSType:         "WINDOWS",
			HostGroupUUIDs: []string{"hg-uuid-2"},
		},
	}

	expectedHostGroup := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hg-uuid-2"},
		Name:      "hg2",
		State:     "READY",
		Hosts:     datamodel.Hosts{Hosts: []string{"iqn.1998-01.com.vmware:host2"}},
	}

	mockStorage.On("GetHostGroup", ctx, "hg-uuid-2", int64(1)).Return(expectedHostGroup, nil)

	// Act
	result, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Contains(t, result, "volume_attributes")

	// Verify BlockProperties were created and updated
	assert.NotNil(t, volume.VolumeAttributes.BlockProperties)
	// Note: OSType is not set in the fallback logic when creating new BlockProperties
	assert.Len(t, volume.VolumeAttributes.BlockProperties.HostGroupDetails, 1)

	// Verify host group details
	assert.Equal(t, "hg-uuid-2", volume.VolumeAttributes.BlockProperties.HostGroupDetails[0].HostGroupUUID)
	assert.Equal(t, []string{"iqn.1998-01.com.vmware:host2"}, volume.VolumeAttributes.BlockProperties.HostGroupDetails[0].HostQNs)

	mockStorage.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams(t *testing.T) {
	backupVaultId := "vault-123"
	backupPolicyId := "policy-uuid"
	policyEnabled := true
	tests := []struct {
		name           string
		ctx            context.Context
		volume         *datamodel.Volume
		se             database.Storage
		params         *common.UpdateVolumeParams
		dbCallRequired bool
		check          func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume, se database.Storage)
	}{
		{
			name: "AllFields",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},

			params: &common.UpdateVolumeParams{
				Description:  "desc",
				QuotaInBytes: 12345,
				Labels:       &datamodel.JSONB{"env": "prod", "team": "devops"},
				DataProtection: &models.UpdateDataProtection{
					BackupVaultID:          &backupVaultId,
					BackupPolicyId:         &backupPolicyId,
					ScheduledBackupEnabled: &policyEnabled,
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume, se database.Storage) {
				assert.Equal(t, "desc", fields["description"])
				assert.Equal(t, int64(12345), fields["size_in_bytes"])
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				va, ok := fields["volume_attributes"].(*datamodel.VolumeAttributes)
				assert.True(t, ok)
				assert.NotNil(t, va.Labels)
				assert.Equal(t, "prod", (*va.Labels)["env"])
				assert.Equal(t, "devops", (*va.Labels)["team"])
			},
			dbCallRequired: false,
		},
		{
			name: "OnlyDescription",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				Description:    "desc",
				SnapshotPolicy: &models.SnapshotPolicy{Name: "test-policy"},
				DataProtection: &models.UpdateDataProtection{
					BackupVaultID: &backupVaultId,
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, se database.Storage) {
				assert.Equal(t, "desc", fields["description"])
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				_, hasSize := fields["size_in_bytes"]
				assert.False(t, hasSize)
				_, hasVA := fields["volume_attributes"]
				assert.True(t, hasVA)
			},
			dbCallRequired: false,
		},
		{
			name: "OnlyQuota",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 999,
				DataProtection: &models.UpdateDataProtection{
					BackupVaultID: &backupVaultId,
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, se database.Storage) {
				assert.Equal(t, int64(999), fields["size_in_bytes"])
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				_, hasDesc := fields["description"]
				assert.False(t, hasDesc)
				_, hasVA := fields["volume_attributes"]
				assert.True(t, hasVA)
			},
			dbCallRequired: false,
		},
		{
			name: "OnlyLabels",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				Labels: &datamodel.JSONB{"foo": "bar"},
				DataProtection: &models.UpdateDataProtection{
					BackupVaultID: &backupVaultId,
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume, se database.Storage) {
				va, ok := fields["volume_attributes"].(*datamodel.VolumeAttributes)
				assert.True(t, ok)
				assert.NotNil(t, va.Labels)
				assert.Equal(t, "bar", (*va.Labels)["foo"])
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				_, hasDesc := fields["description"]
				assert.False(t, hasDesc)
				_, hasSize := fields["size_in_bytes"]
				assert.False(t, hasSize)
			},
			dbCallRequired: false,
		},
		{
			name: "EmptyParams",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				DataProtection: &models.UpdateDataProtection{},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, se database.Storage) {
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				_, hasDesc := fields["description"]
				assert.False(t, hasDesc)
				_, hasSize := fields["size_in_bytes"]
				assert.False(t, hasSize)
				_, hasVA := fields["volume_attributes"]
				assert.True(t, hasVA)
			},
			dbCallRequired: false,
		},
		{
			name: "LabelsWithNilVolumeAttributes",
			volume: &datamodel.Volume{
				VolumeAttributes: nil,
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				Labels: &datamodel.JSONB{"foo": "bar"},
				DataProtection: &models.UpdateDataProtection{
					BackupVaultID: &backupVaultId,
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume, se database.Storage) {
				va, ok := fields["volume_attributes"].(*datamodel.VolumeAttributes)
				assert.True(t, ok)
				assert.NotNil(t, va.Labels)
				assert.Equal(t, "bar", (*va.Labels)["foo"])
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				// Ensure VolumeAttributes was initialized
				assert.NotNil(t, volume.VolumeAttributes)
			},
			dbCallRequired: false,
		},
		{
			name: "WhenBlockProperties",
			volume: &datamodel.Volume{
				VolumeAttributes: nil,
				AccountID:        1,
			},
			params: &common.UpdateVolumeParams{
				BlockProperties: &common.BlockPropertiesRequest{
					HostGroupUUIDs: []string{"abcd", "xyz"},
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume, se database.Storage) {
				va, ok := fields["volume_attributes"].(*datamodel.VolumeAttributes)
				assert.True(t, ok)
				assert.Equal(t, va.BlockProperties.HostGroupDetails[0].HostGroupUUID, "abcd")
				assert.Equal(t, va.BlockProperties.HostGroupDetails[1].HostGroupUUID, "xyz")
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				assert.NotNil(t, volume.VolumeAttributes)
			},
			dbCallRequired: true,
		},
		{
			name: "WithAutoTieringPolicy",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					TieringPolicy:        "auto",
					CoolingThresholdDays: 7,
					RetrievalPolicy:      "default",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, _ database.Storage) {
				assert.Equal(t, true, fields["auto_tiering_enabled"])
				autoTieringPolicy, _ := fields["auto_tiering_policy"].(*datamodel.AutoTieringPolicy)
				assert.Equal(t, "auto", autoTieringPolicy.TieringPolicy)
				assert.Equal(t, int32(7), autoTieringPolicy.CoolingThresholdDays)
				assert.Equal(t, "default", autoTieringPolicy.RetrievalPolicy)
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
			},
			dbCallRequired: false,
		},
		{
			name: "WithAutoTieringPolicy_AutoTieringIsTrue_CoolnessPeriodChanged",
			volume: &datamodel.Volume{
				VolumeAttributes:   &datamodel.VolumeAttributes{},
				DataProtection:     &datamodel.DataProtection{},
				AutoTieringEnabled: false,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 5,
				},
			},
			params: &common.UpdateVolumeParams{
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					TieringPolicy:        "auto",
					CoolingThresholdDays: 7,
					RetrievalPolicy:      "default",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, _ database.Storage) {
				assert.Equal(t, true, fields["auto_tiering_enabled"])
				autoTieringPolicy, _ := fields["auto_tiering_policy"].(*datamodel.AutoTieringPolicy)
				assert.Equal(t, "auto", autoTieringPolicy.TieringPolicy)
				assert.Equal(t, int32(7), autoTieringPolicy.CoolingThresholdDays)
				assert.Equal(t, "default", autoTieringPolicy.RetrievalPolicy)
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
			},
			dbCallRequired: false,
		},
		{
			name: "WithAutoTieringPolicy_AutoTieringIsTrue_CoolnessPeriodSame",
			volume: &datamodel.Volume{
				VolumeAttributes:   &datamodel.VolumeAttributes{},
				DataProtection:     &datamodel.DataProtection{},
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 7,
				},
			},
			params: &common.UpdateVolumeParams{
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					TieringPolicy:        "auto",
					CoolingThresholdDays: 7,
					RetrievalPolicy:      "default",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, _ database.Storage) {
				// Should not update coolness_period since it's the same
				assert.NotContains(t, fields, "coolness_period")
				assert.NotContains(t, fields, "auto_tiering_enabled")
				assert.NotContains(t, fields, "cool_access_tiering_policy")
				assert.NotContains(t, fields, "cool_access_retrieval_policy")
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
			},
			dbCallRequired: false,
		},
		{
			name: "WithAutoTieringPolicy_AutoTieringIsFalse",
			volume: &datamodel.Volume{
				VolumeAttributes:   &datamodel.VolumeAttributes{},
				DataProtection:     &datamodel.DataProtection{},
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 7,
				},
			},
			params: &common.UpdateVolumeParams{
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   false,
					TieringPolicy:        "none",
					CoolingThresholdDays: 0,
					RetrievalPolicy:      "",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, _ database.Storage) {
				assert.Equal(t, false, fields["auto_tiering_enabled"])
				autoTieringPolicy, _ := fields["auto_tiering_policy"].(*datamodel.AutoTieringPolicy)
				assert.Equal(t, "none", autoTieringPolicy.TieringPolicy)
				assert.Equal(t, autoTieringPolicy.CoolingThresholdDays, int32(0))
				assert.Empty(t, autoTieringPolicy.RetrievalPolicy)
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
			},
			dbCallRequired: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := database.NewMockStorage(t)
			if tt.dbCallRequired {
				hg := &datamodel.HostGroup{
					Name: "abcd",
					Hosts: datamodel.Hosts{
						Hosts: []string{"iqn.1994-05.com.redhat:abcd", "iqn.1994-05.com.redhat:xyz"},
					},
				}
				getHostGroup = func(se database.Storage, ctx context.Context, uuid string, accountId int64) (*datamodel.HostGroup, error) {
					return hg, nil
				}
				defer func() { getHostGroup = _getHostGroup }()
			}
			fields, _ := getUpdatedFieldsFromParams(tt.ctx, tt.se, tt.volume, tt.params)

			tt.check(t, fields, tt.volume, mockStorage)
		})
	}
}

func TestGetUpdatedFieldsFromParams_FlexCacheUpdate(t *testing.T) {
	writebackEnabled := true

	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{},
		CacheParameters: &datamodel.CacheParameters{
			PeerClusterName: "old-peer-cluster",
			PeerSvmName:     "old-peer-svm",
			PeerVolumeName:  "old-peer-volume",
			CacheConfig: &datamodel.CacheConfig{
				WritebackEnabled: &writebackEnabled,
			},
		},
	}
	params := &common.UpdateVolumeParams{}

	applyFlexCacheParameters = func(volume *datamodel.Volume, params *common.UpdateVolumeParams) bool {
		return true
	}
	defer func() { applyFlexCacheParameters = _applyFlexCacheParameters }()
	fields, err := getUpdatedFieldsFromParams(context.Background(), nil, volume, params)
	assert.NoError(t, err)
	assert.NotNil(t, fields)
	assert.Contains(t, fields, "cache_parameters")

	cacheParams, ok := fields["cache_parameters"].(*datamodel.CacheParameters)
	assert.True(t, ok)
	assert.Equal(t, "old-peer-cluster", cacheParams.PeerClusterName)
	assert.Equal(t, "old-peer-svm", cacheParams.PeerSvmName)
	assert.Equal(t, "old-peer-volume", cacheParams.PeerVolumeName)
	assert.NotNil(t, cacheParams.CacheConfig)
	assert.NotNil(t, cacheParams.CacheConfig.WritebackEnabled)
	assert.Equal(t, writebackEnabled, *cacheParams.CacheConfig.WritebackEnabled)
}

func TestApplyFlexCacheParameters(t *testing.T) {
	tests := []struct {
		name           string
		volume         *datamodel.Volume
		params         *common.UpdateVolumeParams
		expectModified bool
	}{
		{
			name: "NoCacheParameters_NoModification",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
			},
			params:         &common.UpdateVolumeParams{},
			expectModified: false,
		},
		{
			name: "CacheParameters_NoPeerInfo_NoModification",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				CacheParameters:  &datamodel.CacheParameters{},
			},
			params:         &common.UpdateVolumeParams{},
			expectModified: false,
		},
		{
			name: "CacheParameters_WithPeerInfo_Modification",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				CacheParameters: &datamodel.CacheParameters{
					PeerClusterName: "peer-cluster",
					PeerSvmName:     "peer-svm",
					PeerVolumeName:  "peer-volume",
					CacheConfig: &datamodel.CacheConfig{
						WritebackEnabled: nil,
					},
				},
			},
			params: &common.UpdateVolumeParams{
				CacheParameters: &models.CacheParameters{
					PeerClusterName: "peer-cluster",
					PeerSvmName:     "peer-svm",
					PeerVolumeName:  "peer-volume",
					CacheConfig: &models.CacheConfig{
						WritebackEnabled:        func(b bool) *bool { return &b }(true),
						AtimeScrubEnabled:       func(b bool) *bool { return &b }(true),
						AtimeScrubDays:          func(b int16) *int16 { return &b }(5),
						CifsChangeNotifyEnabled: func(b bool) *bool { return &b }(true),
						CachePrePopulate: &models.CachePrePopulate{
							Recursion:       func(b bool) *bool { return &b }(true),
							PathList:        []string{"/vol1/folder1", "/vol1/folder2"},
							ExcludePathList: []string{"/vol1/folder1", "/vol1/folder2"},
						},
					},
				},
			},
			expectModified: true,
		},
		{
			name: "CacheParameters_WithPeerInfo_CacheConfigNilWriteback_NoModification",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				CacheParameters: &datamodel.CacheParameters{
					PeerClusterName: "peer-cluster",
					PeerSvmName:     "peer-svm",
					PeerVolumeName:  "peer-volume",
					CacheConfig: &datamodel.CacheConfig{
						WritebackEnabled: nil,
					},
				},
			},
			params: &common.UpdateVolumeParams{
				CacheParameters: &models.CacheParameters{
					PeerClusterName: "peer-cluster",
					PeerSvmName:     "peer-svm",
					PeerVolumeName:  "peer-volume",
					CacheConfig: &models.CacheConfig{
						WritebackEnabled: nil,
					},
				},
			},
			expectModified: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalVolume := *tt.volume // Make a copy of the original volume
			modified := applyFlexCacheParameters(tt.volume, tt.params)
			assert.Equal(t, tt.expectModified, modified)
			if !tt.expectModified {
				assert.Equal(t, originalVolume, *tt.volume, "Volume should not be modified")
			}
		})
	}
}

func TestDeleteLunIGroupMap(t *testing.T) {
	tests := []struct {
		name          string
		volume        *datamodel.Volume
		iGroupUUIDs   []string
		mockSetup     func(mockStorage *database.MockStorage, mockProvider *vsa.MockProvider)
		expectedError bool
	}{
		{
			name: "SuccessfulDeletion",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					BlockProperties: &datamodel.BlockProperties{
						LunUUID: "lun-uuid",
					},
				},
				AccountID: 1,
				Svm:       &datamodel.Svm{Name: "svm-name"},
			},
			iGroupUUIDs: []string{"igroup-uuid-1", "igroup-uuid-2"},
			mockSetup: func(mockStorage *database.MockStorage, mockProvider *vsa.MockProvider) {
				mockStorage.On("GetHostGroup", mock.Anything, mock.Anything, int64(1)).Return(&datamodel.HostGroup{Name: "igroup-1"}, nil).Times(2)

				mockProvider.On("IgroupExists", "igroup-1", mock.Anything).Return(true, &ontap_rest.Igroup{Igroup: ontapModels.Igroup{
					UUID: nillable.GetStringPtr("ontap-uuid-1"),
				}}, nil).Times(2)

				mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
					LunUUID:    "lun-uuid",
					IGroupUUID: "ontap-uuid-1",
				}).Return(nil).Times(2)

				mockStorage.On("GetAllVolumesForHG", mock.Anything, mock.Anything, int64(1)).Return([]*datamodel.Volume{}, nil).Times(2)

				mockProvider.On("IgroupDelete", "ontap-uuid-1").Return(nil).Times(2)
			},
			expectedError: false,
		},
		{
			name: "HostGroupNotFound",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					BlockProperties: &datamodel.BlockProperties{
						LunUUID: "lun-uuid",
					},
				},
				AccountID: 1,
				Svm:       &datamodel.Svm{Name: "svm-name"},
			},
			iGroupUUIDs: []string{"invalid-uuid"},
			mockSetup: func(mockStorage *database.MockStorage, mockProvider *vsa.MockProvider) {
				mockStorage.On("GetHostGroup", mock.Anything, "invalid-uuid", int64(1)).Return(nil, errors.New("host group not found"))
			},
			expectedError: true,
		},
		{
			name: "LunMapDeleteFails",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					BlockProperties: &datamodel.BlockProperties{
						LunUUID: "lun-uuid",
					},
				},
				AccountID: 1,
				Svm:       &datamodel.Svm{Name: "svm-name"},
			},
			iGroupUUIDs: []string{"igroup-uuid"},
			mockSetup: func(mockStorage *database.MockStorage, mockProvider *vsa.MockProvider) {
				mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid", int64(1)).Return(&datamodel.HostGroup{Name: "igroup"}, nil)

				mockProvider.On("IgroupExists", "igroup", mock.Anything).Return(true, &ontap_rest.Igroup{Igroup: ontapModels.Igroup{
					UUID: nillable.GetStringPtr("ontap-uuid"),
				}}, nil)
				mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
					LunUUID:    "lun-uuid",
					IGroupUUID: "ontap-uuid",
				}).Return(errors.New("some error occurred"))
			},
			expectedError: true,
		},
		{
			name: "WhenIgroupGetFails",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					BlockProperties: &datamodel.BlockProperties{
						LunUUID: "lun-uuid",
					},
				},
				AccountID: 1,
				Svm:       &datamodel.Svm{Name: "svm-name"},
			},
			iGroupUUIDs: []string{"igroup-uuid"},
			mockSetup: func(mockStorage *database.MockStorage, mockProvider *vsa.MockProvider) {
				mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid", int64(1)).Return(&datamodel.HostGroup{Name: "igroup"}, nil)

				mockProvider.On("IgroupExists", "igroup", mock.Anything).Return(false, nil, errors.New("some error"))
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestActivityEnvironment()

			mockStorage := database.NewMockStorage(t)
			mockProvider := vsa.NewMockProvider(t)

			originalGetProviderByNode := hyperscaler.GetProviderByNode
			defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

			hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
				return mockProvider, nil
			}
			tt.mockSetup(mockStorage, mockProvider)

			activity := &VolumeUpdateActivity{
				SE: mockStorage,
			}
			env.RegisterActivity(activity.UnmapHostGroupFromDisk)

			_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, tt.volume, tt.iGroupUUIDs, &models.Node{})
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
			mockProvider.AssertExpectations(t)
		})
	}
}

// TestEnsureIGroupsExistsAndMapLun tests the EnsureHostGroupsExistsAndMapLun function
func TestEnsureIGroupsExistsAndMapLun(t *testing.T) {
	mockNode := &models.Node{}
	volume := &datamodel.Volume{
		Name:      "test-volume",
		AccountID: 12345,
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
	}
	iGroups := []string{"igroup1", "igroup2"}
	hostGroups := []*datamodel.HostGroup{
		{
			Name:   "igroup1",
			OSType: "linux",
			Hosts: datamodel.Hosts{
				Hosts: []string{"iqn.1993-08.org.debian:01:123456789"},
			},
		},
		{
			Name:   "igroup2",
			OSType: "windows",
			Hosts: datamodel.Hosts{
				Hosts: []string{"iqn.1993-08.org.debian:01:987654321"},
			},
		},
	}

	t.Run("successfully ensures iGroups and maps LUN", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)
		mockProvider.On("IgroupCreate", vsa.IgroupCreateParams{
			IgroupName: "igroup1",
			SvmName:    "test-svm",
			OsType:     "linux",
			Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
		}).Return("", nil)
		mockProvider.On("IgroupExists", "igroup2", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)

		mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
			LunName:    "/vol/" + volume.Name + "/" + utils.GetLunName(volume.Name),
			SvmName:    "test-svm",
			IGroupName: []string{"igroup1", "igroup2"},
		}).Return(nil)

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})
	t.Run("successfully all igroups are created prev", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)
		mockProvider.On("IgroupExists", "igroup2", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)

		mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
			LunName:    "/vol/" + volume.Name + "/" + utils.GetLunName(volume.Name),
			SvmName:    "test-svm",
			IGroupName: []string{"igroup1", "igroup2"},
		}).Return(nil)

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})
	t.Run("returns error when GetMultipleHostGroups fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := vsa.NewMockProvider(t)
		mockStorage := database.NewMockStorage(t)
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(nil, errors.New("failed to fetch host groups"))

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch host groups")
		mockStorage.AssertExpectations(t)
	})

	t.Run("returns error when IgroupExists fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, errors.New("failed to check igroup existence"))

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check igroup existence")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when IgroupCreate fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)
		mockProvider.On("IgroupCreate", vsa.IgroupCreateParams{
			IgroupName: "igroup1",
			SvmName:    "test-svm",
			OsType:     "linux",
			Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
		}).Return("", errors.New("failed to create igroup"))

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create igroup")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})
	t.Run("returns error when LunMapCreate fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)
		mockProvider.On("IgroupExists", "igroup2", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)
		mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
			LunName:    "/vol/" + volume.Name + "/" + utils.GetLunName(volume.Name),
			SvmName:    "test-svm",
			IGroupName: []string{"igroup1", "igroup2"},
		}).Return(errors.New("failed to map lun"))

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to map lun")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})
	t.Run("returns error when LunMapCreate fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return([]*datamodel.HostGroup{}, nil)

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("successfully ensures iGroups and maps LUN with BlockDevices", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		// Create volume with BlockDevices to test line 173
		volumeWithBlockDevices := &datamodel.Volume{
			Name:      "test-volume",
			AccountID: 12345,
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices: &[]datamodel.BlockDevice{
					{
						Name: "custom-lun-name",
					},
				},
			},
		}

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volumeWithBlockDevices.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)
		mockProvider.On("IgroupCreate", vsa.IgroupCreateParams{
			IgroupName: "igroup1",
			SvmName:    "test-svm",
			OsType:     "linux",
			Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
		}).Return("", nil)
		mockProvider.On("IgroupExists", "igroup2", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)

		// Test that the custom lun name is used (line 173)
		mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
			LunName:    "/vol/" + volumeWithBlockDevices.Name + "/custom-lun-name",
			SvmName:    "test-svm",
			IGroupName: []string{"igroup1", "igroup2"},
		}).Return(nil)

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volumeWithBlockDevices, iGroups, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when LunMapCreate fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)
		mockProvider.On("IgroupCreate", vsa.IgroupCreateParams{
			IgroupName: "igroup1",
			SvmName:    "test-svm",
			OsType:     "linux",
			Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
		}).Return("", nil)
		mockProvider.On("IgroupExists", "igroup2", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)

		expectedError := errors.New("failed to create lun map")
		mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
			LunName:    "/vol/" + volume.Name + "/" + utils.GetLunName(volume.Name),
			SvmName:    "test-svm",
			IGroupName: []string{"igroup1", "igroup2"},
		}).Return(expectedError)

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create lun map")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when IgroupCreate fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)
		expectedError := errors.New("failed to create igroup")
		mockProvider.On("IgroupCreate", vsa.IgroupCreateParams{
			IgroupName: "igroup1",
			SvmName:    "test-svm",
			OsType:     "linux",
			Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
		}).Return("", expectedError)

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create igroup")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when IgroupExists fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		mockStorage.On("GetMultipleHostGroups", mock.Anything, iGroups, volume.AccountID).Return(hostGroups, nil)
		expectedError := errors.New("failed to check igroup existence")
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, expectedError)

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check igroup existence")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when GetProviderByNode fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		expectedError := errors.New("failed to get provider")
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.EnsureHostGroupsExistsAndMapDisk)

		_, err := env.ExecuteActivity(activity.EnsureHostGroupsExistsAndMapDisk, volume, iGroups, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get provider")
	})
}

// TestGetHostGroup tests the GetHostGroup function
func TestGetHostGroup(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		mockStorage.On("GetHostGroup", ctx, "test-uuid", int64(1)).Return(&datamodel.HostGroup{Name: "1"}, nil)
		hg, err := getHostGroup(mockStorage, ctx, "test-uuid", 1)
		assert.Equal(t, hg.Name, "1")
		assert.NoError(t, err)
	})
	t.Run("Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		mockStorage.On("GetHostGroup", ctx, "test-uuid", int64(1)).Return(nil, errors.New("host group not found"))
		hg, err := getHostGroup(mockStorage, ctx, "test-uuid", 1)
		assert.Nil(t, hg)
		assert.EqualError(t, err, "host group not found")
	})
}

func TestGetVolumeFromONTAP_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.GetVolumeFromONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	node := &models.Node{}
	expectedRes := &vsa.VolumeResponse{Size: 12345}

	mockProvider.On("GetVolume", vsa.GetVolumeParams{
		UUID:       volume.VolumeAttributes.ExternalUUID,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
		IsRestore:  false,
	}).Return(expectedRes, nil)

	val, err := env.ExecuteActivity(activity.GetVolumeFromONTAP, volume, node)
	assert.NoError(t, err)
	var res *vsa.VolumeResponse
	_ = val.Get(&res)
	assert.Equal(t, expectedRes, res)
	mockProvider.AssertExpectations(t)
}

func TestGetVolumeFromONTAP_Error(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.GetVolumeFromONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	node := &models.Node{}
	expectedErr := errors.New("get failed")

	mockProvider.On("GetVolume", vsa.GetVolumeParams{
		UUID:       volume.VolumeAttributes.ExternalUUID,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
		IsRestore:  false,
	}).Return(nil, expectedErr)

	_, err := env.ExecuteActivity(activity.GetVolumeFromONTAP, volume, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateSnapshotPolicyInOntap_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := &VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateSnapshotPolicyInOntap)
	node := &models.Node{}

	currentPolicy := &datamodel.SnapshotPolicy{
		Name:      "policy1",
		IsEnabled: true,
		Schedules: []*datamodel.SnapshotPolicySchedule{
			{Count: 2},
		},
	}
	updatingPolicy := &datamodel.SnapshotPolicy{
		Name:      "policy1",
		IsEnabled: false,
		Schedules: []*datamodel.SnapshotPolicySchedule{
			{Count: 2},
		},
	}

	mockProvider.On("UpdateSnapshotPolicy", mock.Anything, &vsa.UpdateSnapshotPolicyParams{
		CurrentSnapshotPolicy: &vsa.SnapshotPolicy{
			Name:      currentPolicy.Name,
			IsEnabled: currentPolicy.IsEnabled,
			Schedules: ConvertToVSASnapshotPolicySchedules(currentPolicy.Schedules),
		},
		UpdatingSnapshotPolicy: &vsa.SnapshotPolicy{
			Name:      currentPolicy.Name,
			IsEnabled: updatingPolicy.IsEnabled,
			Schedules: ConvertToVSASnapshotPolicySchedules(updatingPolicy.Schedules),
		},
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateSnapshotPolicyInOntap, node, currentPolicy, updatingPolicy)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateSnapshotPolicyInOntap_Error(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := &VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateSnapshotPolicyInOntap)
	node := &models.Node{}

	currentPolicy := &datamodel.SnapshotPolicy{
		Name:      "policy1",
		IsEnabled: true,
		Schedules: []*datamodel.SnapshotPolicySchedule{
			{Count: 2},
		},
	}
	updatingPolicy := &datamodel.SnapshotPolicy{
		Name:      "policy1",
		IsEnabled: false,
		Schedules: []*datamodel.SnapshotPolicySchedule{
			{Count: 2},
		},
	}
	expectedErr := errors.New("update failed")
	mockProvider.On("UpdateSnapshotPolicy", mock.Anything, &vsa.UpdateSnapshotPolicyParams{
		CurrentSnapshotPolicy: &vsa.SnapshotPolicy{
			Name:      currentPolicy.Name,
			IsEnabled: currentPolicy.IsEnabled,
			Schedules: ConvertToVSASnapshotPolicySchedules(currentPolicy.Schedules),
		},
		UpdatingSnapshotPolicy: &vsa.SnapshotPolicy{
			Name:      currentPolicy.Name,
			IsEnabled: updatingPolicy.IsEnabled,
			Schedules: ConvertToVSASnapshotPolicySchedules(updatingPolicy.Schedules),
		},
	}).Return(expectedErr)

	_, err := env.ExecuteActivity(activity.UpdateSnapshotPolicyInOntap, node, currentPolicy, updatingPolicy)
	assert.Error(t, err)
	mockProvider.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams_SnapReserve(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{},
	}
	snapReserve := int64(25)
	params := &common.UpdateVolumeParams{
		SnapReserve: &snapReserve,
	}
	fields, err := getUpdatedFieldsFromParams(context.Background(), mockStorage, volume, params)
	assert.NoError(t, err)
	va, ok := fields["volume_attributes"].(*datamodel.VolumeAttributes)
	assert.True(t, ok)
	assert.Equal(t, snapReserve, va.SnapReserve)
}

func TestGetVolumeFromONTAP_NilVolumeAttributes(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetVolumeFromONTAP)

	volume := &datamodel.Volume{
		Name:             "test-volume",
		Svm:              &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: nil, // This will cause ExternalUUID to be empty
	}
	node := &models.Node{}

	// Act: ExecuteActivity - this will call the activity function
	_, err := env.ExecuteActivity(activity.GetVolumeFromONTAP, volume, node)

	// Assert: Temporal converts panics to errors
	assert.Error(t, err)
}

func TestUpdateVolumeUsedBytes_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volumeUUID := "test-volume-uuid"
	volResponse := &vsa.VolumeResponse{
		UsedBytes: int64(1024),
	}

	expectedFields := map[string]interface{}{
		"used_bytes": uint64(volResponse.UsedBytes),
	}

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, expectedFields).Return(nil)

	// Act
	err := activity.RefreshVolumeFieldsInDB(ctx, volumeUUID, volResponse)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeUsedBytes_UpdateError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volumeUUID := "test-volume-uuid"
	volResponse := &vsa.VolumeResponse{
		UsedBytes: int64(1024),
	}

	expectedFields := map[string]interface{}{
		"used_bytes": uint64(volResponse.UsedBytes),
	}

	expectedError := errors.New("database update error")

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, expectedFields).Return(expectedError)

	// Act
	err := activity.RefreshVolumeFieldsInDB(ctx, volumeUUID, volResponse)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestVerifyIfBackupPolicyExistsInVCP(t *testing.T) {
	ctx := context.Background()
	backupPolicyUUID := "test-uuid"
	accountId := int64(123)

	t.Run("ReturnsTrueIfBackupPolicyExists", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeUpdateActivity{SE: mockStorage}
		mockBackupPolicy := &datamodel.BackupPolicy{}
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountId).Return(mockBackupPolicy, nil)
		ok, err := activity.VerifyIfBackupPolicyExistsInVCP(ctx, backupPolicyUUID, accountId)
		assert.NoError(t, err)
		assert.True(t, ok)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ReturnsFalseIfBackupPolicyNotFound", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeUpdateActivity{SE: mockStorage}
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountId).
			Return(nil, errors.NewNotFoundErr("backup policy", &backupPolicyUUID))
		ok, err := activity.VerifyIfBackupPolicyExistsInVCP(ctx, backupPolicyUUID, accountId)
		assert.NoError(t, err)
		assert.False(t, ok)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ReturnsErrorIfOtherError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeUpdateActivity{SE: mockStorage}
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountId).
			Return(nil, errors.New("db error"))
		ok, err := activity.VerifyIfBackupPolicyExistsInVCP(ctx, backupPolicyUUID, accountId)
		assert.Error(t, err)
		assert.False(t, ok)
		mockStorage.AssertExpectations(t)
	})
}

func TestFetchAndCreateBackupPolicyFromSDESucceeds(t *testing.T) {
	ctx := context.Background()
	region := "us-central1"
	backupPolicyUUID := "test-backup-policy-uuid"
	accountName := "test-account"
	accountID := int64(123)

	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID},
		Account:        &datamodel.Account{Name: accountName},
		AccountID:      accountID,
	}

	t.Run("FetchAndCreateBackupPolicyFromSDESucceeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeUpdateActivity{SE: mockStorage}
		mockClient := backup_policy.NewMockClientService(t)

		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := CvpCreateClient
		defer func() { CvpCreateClient = originalCreateClient }()
		CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockBackupPolicy := &cvpModels.BackupPolicyDetailsV1beta{
			BackupPolicyV1beta: cvpModels.BackupPolicyV1beta{
				BackupPolicyID: backupPolicyUUID,
				State:          models.LifeCycleStateREADY,
			},
		}
		mockClient.On("V1betaDescribeBackupPolicy", mock.Anything).Return(&backup_policy.V1betaDescribeBackupPolicyOK{
			Payload: mockBackupPolicy,
		}, nil)
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: backupPolicyUUID}, AccountID: accountID, LifeCycleState: models.LifeCycleStateREADY, LifeCycleStateDetails: models.LifeCycleStateAvailableDetails}, nil)

		res, err := activity.FetchAndCreateBackupPolicyFromSDE(ctx, volume, region)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, backupPolicyUUID, res.UUID)
		assert.Equal(t, accountID, res.AccountID)
	})

	t.Run("FetchAndCreateBackupPolicyFromSDEFailsWhenCVPReturnsError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeUpdateActivity{SE: mockStorage}
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := CvpCreateClient
		defer func() { CvpCreateClient = originalCreateClient }()
		CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockClient.On("V1betaDescribeBackupPolicy", mock.Anything).Return(nil, errors.New("cvp error"))
		res, err := activity.FetchAndCreateBackupPolicyFromSDE(ctx, volume, region)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("FetchAndCreateBackupPolicyFromSDEFailsWhenCVPReturnsNil", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeUpdateActivity{SE: mockStorage}
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := CvpCreateClient
		defer func() { CvpCreateClient = originalCreateClient }()
		CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockClient.On("V1betaDescribeBackupPolicy", mock.Anything).Return(&backup_policy.V1betaDescribeBackupPolicyOK{
			Payload: nil,
		}, nil)

		res, err := activity.FetchAndCreateBackupPolicyFromSDE(ctx, volume, region)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("FetchAndCreateBackupPolicyFromSDEFailsWithDBError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := VolumeUpdateActivity{SE: mockStorage}
		mockClient := backup_policy.NewMockClientService(t)

		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := CvpCreateClient
		defer func() { CvpCreateClient = originalCreateClient }()
		CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockBackupPolicy := &cvpModels.BackupPolicyDetailsV1beta{
			BackupPolicyV1beta: cvpModels.BackupPolicyV1beta{
				BackupPolicyID: backupPolicyUUID,
				State:          models.LifeCycleStateREADY,
			},
		}
		mockClient.On("V1betaDescribeBackupPolicy", mock.Anything).Return(&backup_policy.V1betaDescribeBackupPolicyOK{
			Payload: mockBackupPolicy,
		}, nil)
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(nil, errors.New("db error"))

		res, err := activity.FetchAndCreateBackupPolicyFromSDE(ctx, volume, region)
		assert.Error(t, err)
		assert.Nil(t, res)
	})
}

// TestUnmapHostGroupFromDisk tests the UnmapHostGroupFromDisk function
func TestUnmapHostGroupFromDisk(t *testing.T) {
	mockNode := &models.Node{}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:      "test-volume",
		AccountID: 12345,
		PoolID:    1,
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "test-lun-uuid",
			},
		},
	}

	t.Run("successfully unmaps host group with BlockProperties", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		ontapIgroup := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("ontap-igroup-uuid"),
			},
		}

		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(true, ontapIgroup, nil)
		mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
			LunUUID:    "test-lun-uuid",
			IGroupUUID: "ontap-igroup-uuid",
		}).Return(nil)
		mockStorage.On("GetAllVolumesForHG", mock.Anything, "igroup-uuid-1", volume.AccountID).Return([]*datamodel.Volume{}, nil)
		mockProvider.On("IgroupDelete", "ontap-igroup-uuid").Return(nil)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("successfully unmaps host group with BlockDevices", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		// Create volume with BlockDevices to test line 199
		volumeWithBlockDevices := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test-volume",
			AccountID: 12345,
			PoolID:    1,
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices: &[]datamodel.BlockDevice{
					{
						LunUUID: "block-device-lun-uuid",
					},
				},
			},
		}

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		ontapIgroup := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("ontap-igroup-uuid"),
			},
		}

		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volumeWithBlockDevices.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(true, ontapIgroup, nil)
		// Test that the block device lun uuid is used (line 199)
		mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
			LunUUID:    "block-device-lun-uuid",
			IGroupUUID: "ontap-igroup-uuid",
		}).Return(nil)
		mockStorage.On("GetAllVolumesForHG", mock.Anything, "igroup-uuid-1", volumeWithBlockDevices.AccountID).Return([]*datamodel.Volume{}, nil)
		mockProvider.On("IgroupDelete", "ontap-igroup-uuid").Return(nil)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volumeWithBlockDevices, iGroupUUIDs, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("skips when igroup does not exist", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when IgroupExists fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		expectedError := errors.New("failed to check igroup existence")
		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(false, nil, expectedError)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check igroup existence")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when LunMapDelete fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		ontapIgroup := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("ontap-igroup-uuid"),
			},
		}

		expectedError := errors.New("failed to delete lun map")
		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(true, ontapIgroup, nil)
		mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
			LunUUID:    "test-lun-uuid",
			IGroupUUID: "ontap-igroup-uuid",
		}).Return(expectedError)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete lun map")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("skips igroup deletion when other volumes use same host group in same pool", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		ontapIgroup := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("ontap-igroup-uuid"),
			},
		}

		// Create another volume that uses the same host group in the same pool
		otherVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "other-volume-uuid"},
			PoolID:    1, // Same pool as the current volume
		}

		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(true, ontapIgroup, nil)
		mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
			LunUUID:    "test-lun-uuid",
			IGroupUUID: "ontap-igroup-uuid",
		}).Return(nil)
		// Test lines 236-238: other volume uses same host group in same pool
		mockStorage.On("GetAllVolumesForHG", mock.Anything, "igroup-uuid-1", volume.AccountID).Return([]*datamodel.Volume{volume, otherVolume}, nil)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("deletes igroup when no other volumes use same host group in same pool", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		ontapIgroup := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("ontap-igroup-uuid"),
			},
		}

		// Create another volume that uses the same host group but in a different pool
		otherVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "other-volume-uuid"},
			PoolID:    2, // Different pool
		}

		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(true, ontapIgroup, nil)
		mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
			LunUUID:    "test-lun-uuid",
			IGroupUUID: "ontap-igroup-uuid",
		}).Return(nil)
		mockStorage.On("GetAllVolumesForHG", mock.Anything, "igroup-uuid-1", volume.AccountID).Return([]*datamodel.Volume{volume, otherVolume}, nil)
		mockProvider.On("IgroupDelete", "ontap-igroup-uuid").Return(nil)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when IgroupDelete fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		ontapIgroup := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("ontap-igroup-uuid"),
			},
		}

		expectedError := errors.New("failed to delete igroup")
		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(true, ontapIgroup, nil)
		mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
			LunUUID:    "test-lun-uuid",
			IGroupUUID: "ontap-igroup-uuid",
		}).Return(nil)
		mockStorage.On("GetAllVolumesForHG", mock.Anything, "igroup-uuid-1", volume.AccountID).Return([]*datamodel.Volume{}, nil)
		mockProvider.On("IgroupDelete", "ontap-igroup-uuid").Return(expectedError)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete igroup")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when GetHostGroup fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		expectedError := errors.New("failed to get host group")
		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(nil, expectedError)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get host group")
		mockStorage.AssertExpectations(t)
	})

	t.Run("returns error when GetAllVolumesForHG fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		hostGroup := &datamodel.HostGroup{
			Name: "test-igroup",
		}

		ontapIgroup := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("ontap-igroup-uuid"),
			},
		}

		expectedError := errors.New("failed to get volumes for host group")
		mockStorage.On("GetHostGroup", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(hostGroup, nil)
		mockProvider.On("IgroupExists", "test-igroup", nillable.GetStringPtr("test-svm")).Return(true, ontapIgroup, nil)
		mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
			LunUUID:    "test-lun-uuid",
			IGroupUUID: "ontap-igroup-uuid",
		}).Return(nil)
		mockStorage.On("GetAllVolumesForHG", mock.Anything, "igroup-uuid-1", volume.AccountID).Return(nil, expectedError)

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get volumes for host group")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when GetProviderByNode fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		oldProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = oldProviderByNode }()

		expectedError := errors.New("failed to get provider")
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		env.RegisterActivity(activity.UnmapHostGroupFromDisk)

		iGroupUUIDs := []string{"igroup-uuid-1"}

		_, err := env.ExecuteActivity(activity.UnmapHostGroupFromDisk, volume, iGroupUUIDs, mockNode)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get provider")
	})
}

// TestGetUpdatedFieldsFromParams_HotTierBypassModeComparisonLogic tests the specific comparison logic:
// params.AutoTieringPolicy.HotTierBypassModeEnabled != volume.AutoTieringPolicy.HotTierBypassModeEnabled
func TestGetUpdatedFieldsFromParams_HotTierBypassModeComparisonLogic(t *testing.T) {
	t.Run("HotTierBypassMode_TrueToFalse_ShouldTriggerUpdate", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Original volume with HotTierBypassModeEnabled = true
		volume := &datamodel.Volume{
			Name:               "test-volume",
			AutoTieringEnabled: true,
			AutoTieringPolicy: &datamodel.AutoTieringPolicy{
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAll,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     10,
				HotTierBypassModeEnabled: true, // Original value
			},
		}

		// Params to change HotTierBypassModeEnabled from true to false
		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAuto,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     10,    // Same value
				HotTierBypassModeEnabled: false, // Changed from true to false
			},
		}

		fields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

		assert.NoError(tt, err)
		assert.Contains(tt, fields, "auto_tiering_enabled")
		assert.Contains(tt, fields, "auto_tiering_policy")

		autoTieringPolicy, ok := fields["auto_tiering_policy"].(*datamodel.AutoTieringPolicy)
		assert.True(tt, ok)
		assert.False(tt, autoTieringPolicy.HotTierBypassModeEnabled)
		assert.Equal(tt, ontapModels.VolumeInlineTieringPolicyAuto, autoTieringPolicy.TieringPolicy)
	})

	t.Run("HotTierBypassMode_FalseToTrue_ShouldTriggerUpdate", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Original volume with HotTierBypassModeEnabled = false
		volume := &datamodel.Volume{
			Name:               "test-volume",
			AutoTieringEnabled: true,
			AutoTieringPolicy: &datamodel.AutoTieringPolicy{
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAuto,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     10,
				HotTierBypassModeEnabled: false, // Original value
			},
		}

		// Params to change HotTierBypassModeEnabled from false to true
		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAll,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     10,   // Same value
				HotTierBypassModeEnabled: true, // Changed from false to true
			},
		}

		fields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

		assert.NoError(tt, err)
		assert.Contains(tt, fields, "auto_tiering_enabled")
		assert.Contains(tt, fields, "auto_tiering_policy")

		autoTieringPolicy, ok := fields["auto_tiering_policy"].(*datamodel.AutoTieringPolicy)
		assert.True(tt, ok)
		assert.True(tt, autoTieringPolicy.HotTierBypassModeEnabled)
		assert.Equal(tt, ontapModels.VolumeInlineTieringPolicyAll, autoTieringPolicy.TieringPolicy)
	})

	t.Run("HotTierBypassMode_SameValue_ShouldNotTriggerUpdate", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Original volume with HotTierBypassModeEnabled = true
		volume := &datamodel.Volume{
			Name:               "test-volume",
			AutoTieringEnabled: true,
			AutoTieringPolicy: &datamodel.AutoTieringPolicy{
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAll,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     10,
				HotTierBypassModeEnabled: true, // Original value
			},
		}

		// Params with same HotTierBypassModeEnabled value
		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAll,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     10,   // Same value
				HotTierBypassModeEnabled: true, // Same value
			},
		}

		fields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

		assert.NoError(tt, err)
		// Should not contain auto_tiering_policy since no changes detected
		assert.NotContains(tt, fields, "auto_tiering_policy")
		assert.NotContains(tt, fields, "auto_tiering_enabled")
	})

	t.Run("HotTierBypassMode_OnlyHotTierBypassModeChanged_ShouldTriggerUpdate", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Original volume with HotTierBypassModeEnabled = false
		volume := &datamodel.Volume{
			Name:               "test-volume",
			AutoTieringEnabled: true,
			AutoTieringPolicy: &datamodel.AutoTieringPolicy{
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAuto,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     15,
				HotTierBypassModeEnabled: false, // Original value
			},
		}

		// Params with only HotTierBypassModeEnabled changed, all other values same
		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,                                          // Same
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAuto,     // Same
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault, // Same
				CoolingThresholdDays:     15,                                            // Same
				HotTierBypassModeEnabled: true,                                          // Only this changed
			},
		}

		fields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

		assert.NoError(tt, err)
		assert.Contains(tt, fields, "auto_tiering_enabled")
		assert.Contains(tt, fields, "auto_tiering_policy")

		autoTieringPolicy, ok := fields["auto_tiering_policy"].(*datamodel.AutoTieringPolicy)
		assert.True(tt, ok)
		assert.True(tt, autoTieringPolicy.HotTierBypassModeEnabled)
		assert.Equal(tt, int32(15), autoTieringPolicy.CoolingThresholdDays)
	})

	t.Run("HotTierBypassMode_WithNilVolumeAutoTieringPolicy_ShouldHandleGracefully", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Original volume with nil AutoTieringPolicy
		volume := &datamodel.Volume{
			Name:               "test-volume",
			AutoTieringEnabled: false,
			AutoTieringPolicy:  nil, // Nil policy
		}

		// Params to enable auto tiering with HotTierBypassModeEnabled
		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAll,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     10,
				HotTierBypassModeEnabled: true,
			},
		}

		fields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

		assert.NoError(tt, err)
		assert.Contains(tt, fields, "auto_tiering_enabled")
		assert.Contains(tt, fields, "auto_tiering_policy")

		autoTieringPolicy, ok := fields["auto_tiering_policy"].(*datamodel.AutoTieringPolicy)
		assert.True(tt, ok)
		assert.True(tt, autoTieringPolicy.HotTierBypassModeEnabled)
		assert.Equal(tt, ontapModels.VolumeInlineTieringPolicyAll, autoTieringPolicy.TieringPolicy)
	})

	t.Run("HotTierBypassMode_AutoTieringDisabled_ShouldNotEvaluateComparison", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Original volume with auto tiering enabled
		volume := &datamodel.Volume{
			Name:               "test-volume",
			AutoTieringEnabled: true,
			AutoTieringPolicy: &datamodel.AutoTieringPolicy{
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyAll,
				RetrievalPolicy:          ontapModels.VolumeCloudRetrievalPolicyDefault,
				CoolingThresholdDays:     10,
				HotTierBypassModeEnabled: true,
			},
		}

		// Params to disable auto tiering (should not evaluate HotTierBypassModeEnabled comparison)
		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       false, // Disabled
				TieringPolicy:            ontapModels.VolumeInlineTieringPolicyNone,
				HotTierBypassModeEnabled: false, // Different value, but should not matter
			},
		}

		fields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

		assert.NoError(tt, err)
		assert.Contains(tt, fields, "auto_tiering_enabled")
		assert.Contains(tt, fields, "auto_tiering_policy")

		autoTieringPolicy, ok := fields["auto_tiering_policy"].(*datamodel.AutoTieringPolicy)
		assert.True(tt, ok)
		assert.False(tt, autoTieringPolicy.HotTierBypassModeEnabled)
		assert.Equal(tt, ontapModels.VolumeInlineTieringPolicyNone, autoTieringPolicy.TieringPolicy)
	})
}

func TestUpdateJunctionPathInONTAP_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateJunctionPathInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	junctionPath := "/new/path"
	node := &models.Node{}

	mockProvider.On("UnmountVolume", volume.VolumeAttributes.ExternalUUID).Return(&vsa.OntapAsyncResponse{}, nil)
	mockProvider.On("MountVolume", vsa.MountVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: junctionPath,
	}).Return(&vsa.OntapAsyncResponse{}, nil)

	_, err := env.ExecuteActivity(activity.UpdateJunctionPathInONTAP, volume, junctionPath, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateJunctionPathInONTAP_GetProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateJunctionPathInONTAP)

	volume := &datamodel.Volume{}
	junctionPath := "/new/path"
	node := &models.Node{}

	_, err := env.ExecuteActivity(activity.UpdateJunctionPathInONTAP, volume, junctionPath, node)
	assert.Error(t, err)
}

func TestUpdateJunctionPathInONTAP_UnmountError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateJunctionPathInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	junctionPath := "/new/path"
	node := &models.Node{}

	expectedError := errors.New("unmount failed")
	mockProvider.On("UnmountVolume", volume.VolumeAttributes.ExternalUUID).Return(nil, expectedError)

	_, err := env.ExecuteActivity(activity.UpdateJunctionPathInONTAP, volume, junctionPath, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateJunctionPathInONTAP_MountError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateJunctionPathInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	junctionPath := "/new/path"
	node := &models.Node{}

	expectedError := errors.New("mount failed")
	mockProvider.On("UnmountVolume", volume.VolumeAttributes.ExternalUUID).Return(&vsa.OntapAsyncResponse{}, nil)
	mockProvider.On("MountVolume", vsa.MountVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: junctionPath,
	}).Return(nil, expectedError)

	_, err := env.ExecuteActivity(activity.UpdateJunctionPathInONTAP, volume, junctionPath, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateExportPolicyRulesInONTAP_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateExportPolicyRulesInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
	}

	exportPolicy := &models.ExportPolicy{
		ExportPolicyName: "test-policy",
		ExportRules: []*models.ExportRule{
			{
				AllowedClients: "192.168.1.0/24",
				AccessType:     "ro",
				AnonymousUser:  "65534",
			},
		},
	}

	node := &models.Node{}

	mockProvider.On("UpdateExportPolicyRules", mock.MatchedBy(func(params vsa.UpdateExportPolicyRulesParams) bool {
		return params.VolumeName == volume.Name &&
			params.SvmName == volume.Svm.Name &&
			params.ExportPolicy.ExportPolicyName == exportPolicy.ExportPolicyName
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateExportPolicyRulesInONTAP, volume, exportPolicy, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateExportPolicyRulesInONTAP_GetProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateExportPolicyRulesInONTAP)

	volume := &datamodel.Volume{}
	exportPolicy := &models.ExportPolicy{}
	node := &models.Node{}

	_, err := env.ExecuteActivity(activity.UpdateExportPolicyRulesInONTAP, volume, exportPolicy, node)
	assert.Error(t, err)
}

func TestUpdateExportPolicyRulesInONTAP_UpdateError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateExportPolicyRulesInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
	}

	exportPolicy := &models.ExportPolicy{
		ExportPolicyName: "test-policy",
		ExportRules: []*models.ExportRule{
			{
				AllowedClients: "192.168.1.0/24",
				AccessType:     "ro",
				AnonymousUser:  "65534",
			},
		},
	}

	node := &models.Node{}

	expectedError := errors.New("update export policy rules failed")
	mockProvider.On("UpdateExportPolicyRules", mock.Anything).Return(expectedError)

	_, err := env.ExecuteActivity(activity.UpdateExportPolicyRulesInONTAP, volume, exportPolicy, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateExportPolicyRulesInONTAP_EmptyRules(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateExportPolicyRulesInONTAP)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
	}

	exportPolicy := &models.ExportPolicy{
		ExportPolicyName: "test-policy",
		ExportRules:      []*models.ExportRule{}, // Empty rules
	}

	node := &models.Node{}

	mockProvider.On("UpdateExportPolicyRules", mock.MatchedBy(func(params vsa.UpdateExportPolicyRulesParams) bool {
		return params.VolumeName == volume.Name &&
			params.SvmName == volume.Svm.Name &&
			params.ExportPolicy.ExportPolicyName == exportPolicy.ExportPolicyName &&
			len(params.ExportPolicy.ExportRules) == 0
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateExportPolicyRulesInONTAP, volume, exportPolicy, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateLun_LunGetError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	// This test covers the case where LunGet returns an error (lines 126-127)
	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateLun)

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "test-external-uuid",
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
				LunName: "lun_test-volume",
			},
		},
	}

	node := &models.Node{}

	// Mock the provider
	mockProvider := new(vsa.MockProvider)
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(nil, errors.New("lun not found"))

	// Mock hyperscaler.GetProviderByNode
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
	}()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	ontapRes := &vsa.VolumeResponse{
		Size:         BytesPerGB,
		AFSSize:      BytesPerGB,
		MetadataSize: 12345,
	}

	// Act
	_, err := env.ExecuteActivity(activity.UpdateLun, volume, ontapRes, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lun not found")
	mockProvider.AssertExpectations(t)
}

func TestUpdateLun_WithSnapReserveLogic(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateLun)

	volume := &datamodel.Volume{
		Name:        "test-volume",
		SizeInBytes: 10737418240, // 10GB
		Svm:         &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "lun-uuid-123",
			SnapReserve:  10, // 10%
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
				LunName: "lun_test-volume",
			},
		},
	}

	node := &models.Node{}
	mockProvider := new(vsa.MockProvider)

	// First LunGet call
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(&vsa.LunResponse{
		Size: 900,
	}, nil).Once()

	// The actual implementation uses volResponse.AFSSize - volResponse.MetadataSize
	// We'll calculate this after ontapRes is defined

	// We'll set up the LunUpdate mock after ontapRes is defined

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ontapRes := &vsa.VolumeResponse{
		Size:         volume.SizeInBytes,
		AFSSize:      volume.SizeInBytes,
		MetadataSize: 12345,
	}

	// Calculate expected LUN size based on actual implementation
	expectedLunSize := ontapRes.AFSSize - ontapRes.MetadataSize

	// Set up LunUpdate mock with correct expected size
	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       expectedLunSize,
	}).Return(nil).Once()

	// Second LunGet call
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(&vsa.LunResponse{
		Size: expectedLunSize,
	}, nil).Once()

	val, err := env.ExecuteActivity(activity.UpdateLun, volume, ontapRes, node)

	assert.NoError(t, err)
	var result *vsa.LunResponse
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Equal(t, expectedLunSize, result.Size)
	mockProvider.AssertExpectations(t)
}

func TestUpdateLun_WithSnapReserveLogic_SizeUnchanged(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	// This test covers the case where updatedLunSpace == currentLunSpace (line 136)
	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateLun)

	volume := &datamodel.Volume{
		Name:        "test-volume",
		SizeInBytes: BytesPerGB, // 1GB
		Svm:         &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "test-external-uuid",
			SnapReserve:  10, // 10% snap reserve
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
				LunName: "lun_test-volume",
			},
		},
	}

	node := &models.Node{}

	// Mock the provider
	mockProvider := new(vsa.MockProvider)
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(&vsa.LunResponse{
		Size: 900, // Current LUN size (1000 - 10% = 900)
	}, nil)

	// Calculate expected size based on actual implementation
	// sizeToUpdate = 1000 - 200 = 800, lun.Size = 900
	// Since sizeToUpdate (800) < lun.Size (900), it should use lun.Size
	expectedSize := int64(900) // Should use lun.Size when sizeToUpdate < lun.Size

	// Mock LunUpdate with calculated size
	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       expectedSize,
	}).Return(nil)

	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "lun_test-volume",
	}).Return(&vsa.LunResponse{
		Size: expectedSize,
	}, nil)

	// Mock hyperscaler.GetProviderByNode
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
	}()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	// Act - Set up ontapRes so that sizeToUpdate < lun.Size
	// sizeToUpdate = AFSSize - MetadataSize
	// For sizeToUpdate < lun.Size (900), we need AFSSize - MetadataSize < 900
	// So let's set AFSSize = 1000 and MetadataSize = 200, giving sizeToUpdate = 800
	ontapRes := &vsa.VolumeResponse{
		Size:         1000,
		AFSSize:      1000,
		MetadataSize: 200,
	}
	val, err := env.ExecuteActivity(activity.UpdateLun, volume, ontapRes, node)

	// Assert
	assert.NoError(t, err)
	var result *vsa.LunResponse
	_ = val.Get(&result)
	assert.NotNil(t, result)
	assert.Equal(t, expectedSize, result.Size)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInONTAP_WithSnapshotDirectoryAccess_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(activity.UpdateVolumeInONTAP)

	// Create test volume
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "default-snapshot-policy",
		},
	}

	// Create test params with SnapshotDirectoryAccess set
	snapshotDirectoryAccess := true
	params := &common.UpdateVolumeParams{
		QuotaInBytes:            1024,
		SnapshotDirectoryAccess: &snapshotDirectoryAccess,
		// Add SnapshotPolicy to params - this is what the code actually checks
		SnapshotPolicy: &models.SnapshotPolicy{
			Name: "default-snapshot-policy",
		},
	}

	node := &models.Node{}

	// Mock the UpdateVolume call with the correct parameters
	mockProvider.On("UpdateVolume", vsa.UpdateVolumeParams{
		UUID:                    volume.VolumeAttributes.ExternalUUID,
		Size:                    params.QuotaInBytes,
		SnapshotPolicyName:      params.SnapshotPolicy.Name, // Use params.SnapshotPolicy.Name
		SnapshotDirectoryAccess: params.SnapshotDirectoryAccess,
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInONTAP, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeJunctionpath_SANProtocols_Success(t *testing.T) {
	// Test case: When volume has SAN protocols, function should return early without calling provider
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
			Protocols:    []string{utils.ProtocolISCSI}, // SAN protocol
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction/path",
			},
		},
	}
	node := &models.Node{}

	// Should return nil immediately without calling any provider methods
	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.NoError(t, err)
}

func TestUpdateVolumeJunctionpath_NASProtocols_Success(t *testing.T) {
	// Test case: When volume has NAS protocols, function should call provider to update junction path
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
			Protocols:    []string{utils.ProtocolNFSv3}, // NAS protocol
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction/path",
			},
		},
	}
	node := &models.Node{}

	// Mock the UpdateVolume call
	expectedParams := vsa.UpdateVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: &volume.VolumeAttributes.FileProperties.JunctionPath,
	}
	mockProvider.On("UpdateVolume", expectedParams).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeJunctionpath_MixedProtocols_Success(t *testing.T) {
	// Test case: When volume has mixed protocols (not all SAN), function should call provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
			Protocols:    []string{utils.ProtocolNFSv3, utils.ProtocolISCSI}, // Mixed protocols
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction/path",
			},
		},
	}
	node := &models.Node{}

	// Mock the UpdateVolume call
	expectedParams := vsa.UpdateVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: &volume.VolumeAttributes.FileProperties.JunctionPath,
	}
	mockProvider.On("UpdateVolume", expectedParams).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeJunctionpath_EmptyProtocols_Success(t *testing.T) {
	// Test case: When volume has empty protocols, function should call provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
			Protocols:    []string{}, // Empty protocols
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction/path",
			},
		},
	}
	node := &models.Node{}

	// Mock the UpdateVolume call
	expectedParams := vsa.UpdateVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: &volume.VolumeAttributes.FileProperties.JunctionPath,
	}
	mockProvider.On("UpdateVolume", expectedParams).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeJunctionpath_GetProviderByNodeError(t *testing.T) {
	// Test case: When GetProviderByNode returns an error, function should return wrapped error
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("provider error")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
			Protocols:    []string{utils.ProtocolNFSv3},
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction/path",
			},
		},
	}
	node := &models.Node{}

	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "provider error")
}

func TestUpdateVolumeJunctionpath_UpdateVolumeError(t *testing.T) {
	// Test case: When UpdateVolume returns an error, function should return the error
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
			Protocols:    []string{utils.ProtocolNFSv3},
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction/path",
			},
		},
	}
	node := &models.Node{}

	expectedError := errors.New("update volume error")
	expectedParams := vsa.UpdateVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: &volume.VolumeAttributes.FileProperties.JunctionPath,
	}
	mockProvider.On("UpdateVolume", expectedParams).Return(expectedError)

	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update volume error")
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeJunctionpath_NilVolumeAttributes(t *testing.T) {
	// Test case: When volume has nil VolumeAttributes, function should handle gracefully
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name:             "test-volume",
		VolumeAttributes: nil, // Nil VolumeAttributes
	}
	node := &models.Node{}

	// The function will panic when trying to access volume.VolumeAttributes.Protocols
	// Temporal will catch the panic and return it as an error
	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.Error(t, err)
	// The error should indicate a nil pointer dereference
	assert.Contains(t, err.Error(), "nil pointer dereference")
}

func TestUpdateVolumeJunctionpath_NilFileProperties(t *testing.T) {
	// Test case: When volume has nil FileProperties, function should handle gracefully
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "uuid-123",
			Protocols:      []string{utils.ProtocolNFSv3},
			FileProperties: nil, // Nil FileProperties
		},
	}
	node := &models.Node{}

	// The function will panic when trying to access volume.VolumeAttributes.FileProperties.JunctionPath
	// Temporal will catch the panic and return it as an error
	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.Error(t, err)
	// The error should indicate a nil pointer dereference
	assert.Contains(t, err.Error(), "nil pointer")
}

func TestUpdateVolumeJunctionpath_AllNASProtocols_Success(t *testing.T) {
	// Test case: When volume has all NAS protocols, function should call provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
			Protocols:    []string{utils.ProtocolNFS, utils.ProtocolNFSv3, utils.ProtocolNFSv4, utils.ProtocolSMB}, // All NAS protocols
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction/path",
			},
		},
	}
	node := &models.Node{}

	// Mock the UpdateVolume call
	expectedParams := vsa.UpdateVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: &volume.VolumeAttributes.FileProperties.JunctionPath,
	}
	mockProvider.On("UpdateVolume", expectedParams).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeJunctionpath_InvalidProtocols_Success(t *testing.T) {
	// Test case: When volume has invalid protocols, function should call provider (since IsSanProtocols returns false for invalid protocols)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	env.RegisterActivity(&activity)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
			Protocols:    []string{"INVALID_PROTOCOL"}, // Invalid protocol
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction/path",
			},
		},
	}
	node := &models.Node{}

	// Mock the UpdateVolume call
	expectedParams := vsa.UpdateVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: &volume.VolumeAttributes.FileProperties.JunctionPath,
	}
	mockProvider.On("UpdateVolume", expectedParams).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeJunctionpath, volume, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

// TestUpdateAutoTieringParams tests the updateAutoTieringParams function
func TestUpdateAutoTieringParams_WithAutoPolicy_TieringNotPaused(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	falseVal := false
	params := &common.UpdateVolumeParams{
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:    true,
			TieringPolicy:         ontapModels.VolumeInlineTieringPolicyAuto,
			RetrievalPolicy:       ontapModels.VolumeCloudRetrievalPolicyDefault,
			CoolingThresholdDays:  10,
			CloudWriteModeEnabled: &falseVal,
		},
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3}, // File volume
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		},
		AccountID: 123,
	}

	result, err := updateAutoTieringParams(ctx, params, updateVolumeParams, volume, mockStorage)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, ontapModels.VolumeInlineTieringPolicyAuto, result.CoolAccessTieringPolicy)
	assert.Equal(t, ontapModels.VolumeCloudRetrievalPolicyDefault, result.CoolAccessRetrievalPolicy)
	assert.Equal(t, int64(10), result.CoolnessPeriod)
	assert.NotNil(t, result.CloudWriteModeEnabled)
	assert.False(t, *result.CloudWriteModeEnabled)
}

func TestUpdateAutoTieringParams_WithAllPolicy_TieringNotPaused(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	trueVal := true
	params := &common.UpdateVolumeParams{
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:    true,
			TieringPolicy:         ontapModels.VolumeInlineTieringPolicyAll,
			RetrievalPolicy:       ontapModels.VolumeCloudRetrievalPolicyDefault,
			CoolingThresholdDays:  15,
			CloudWriteModeEnabled: &trueVal,
		},
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3}, // File volume
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		},
		AccountID: 123,
	}

	pool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				TieringStatus: datamodel.TieringStatusResumed,
			},
		},
	}

	mockStorage.On("GetPool", ctx, "pool-uuid", int64(123)).Return(pool, nil)

	result, err := updateAutoTieringParams(ctx, params, updateVolumeParams, volume, mockStorage)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, ontapModels.VolumeInlineTieringPolicyAll, result.CoolAccessTieringPolicy)
	assert.Equal(t, ontapModels.VolumeCloudRetrievalPolicyDefault, result.CoolAccessRetrievalPolicy)
	assert.Equal(t, int64(15), result.CoolnessPeriod)
	assert.NotNil(t, result.CloudWriteModeEnabled)
	assert.True(t, *result.CloudWriteModeEnabled)
	mockStorage.AssertExpectations(t)
}

func TestUpdateAutoTieringParams_WithAllPolicy_TieringStatusPaused(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	trueVal := true
	params := &common.UpdateVolumeParams{
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:    true,
			TieringPolicy:         ontapModels.VolumeInlineTieringPolicyAll,
			RetrievalPolicy:       ontapModels.VolumeCloudRetrievalPolicyDefault,
			CoolingThresholdDays:  20,
			CloudWriteModeEnabled: &trueVal,
		},
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3}, // File volume
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		},
		AccountID: 123,
	}

	pool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				TieringStatus: datamodel.TieringStatusPaused,
			},
		},
	}

	mockStorage.On("GetPool", ctx, "pool-uuid", int64(123)).Return(pool, nil)

	result, err := updateAutoTieringParams(ctx, params, updateVolumeParams, volume, mockStorage)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	// When tiering is paused, it should set to 'none' and CloudWriteModeEnabled to false
	assert.Equal(t, ontapModels.VolumeInlineTieringPolicyNone, result.CoolAccessTieringPolicy)
	assert.NotNil(t, result.CloudWriteModeEnabled)
	assert.False(t, *result.CloudWriteModeEnabled)
	mockStorage.AssertExpectations(t)
}

func TestUpdateAutoTieringParams_WithAllPolicy_GetPoolError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	params := &common.UpdateVolumeParams{
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        ontapModels.VolumeInlineTieringPolicyAll,
			RetrievalPolicy:      ontapModels.VolumeCloudRetrievalPolicyDefault,
			CoolingThresholdDays: 10,
		},
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3}, // File volume
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		},
		AccountID: 123,
	}

	mockStorage.On("GetPool", ctx, "pool-uuid", int64(123)).Return(nil, errors.New("db error"))

	result, err := updateAutoTieringParams(ctx, params, updateVolumeParams, volume, mockStorage)

	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestUpdateAutoTieringParams_AutoTieringDisabled(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	falseVal := false
	params := &common.UpdateVolumeParams{
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:    false,
			TieringPolicy:         ontapModels.VolumeInlineTieringPolicyNone,
			RetrievalPolicy:       "",
			CoolingThresholdDays:  0,
			CloudWriteModeEnabled: &falseVal,
		},
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3},
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		},
		AccountID: 123,
	}

	result, err := updateAutoTieringParams(ctx, params, updateVolumeParams, volume, mockStorage)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, ontapModels.VolumeInlineTieringPolicyNone, result.CoolAccessTieringPolicy)
	assert.NotNil(t, result.CloudWriteModeEnabled)
	assert.False(t, *result.CloudWriteModeEnabled)
}

func TestUpdateAutoTieringParams_WithSnapshotOnlyPolicy(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	falseVal := false
	params := &common.UpdateVolumeParams{
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:    true,
			TieringPolicy:         ontapModels.VolumeInlineTieringPolicySnapshotOnly,
			RetrievalPolicy:       ontapModels.VolumeCloudRetrievalPolicyDefault,
			CoolingThresholdDays:  5,
			CloudWriteModeEnabled: &falseVal,
		},
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolISCSI}, // Block volume
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		},
		AccountID: 123,
	}

	result, err := updateAutoTieringParams(ctx, params, updateVolumeParams, volume, mockStorage)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, ontapModels.VolumeInlineTieringPolicySnapshotOnly, result.CoolAccessTieringPolicy)
	assert.Equal(t, ontapModels.VolumeCloudRetrievalPolicyDefault, result.CoolAccessRetrievalPolicy)
	assert.Equal(t, int64(5), result.CoolnessPeriod)
	assert.NotNil(t, result.CloudWriteModeEnabled)
	assert.False(t, *result.CloudWriteModeEnabled)
}

func TestUpdateAutoTieringParams_WithAutoTieringPolicySetForFileVolume(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	params := &common.UpdateVolumeParams{
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        ontapModels.VolumeInlineTieringPolicyAuto, // Explicitly set
			RetrievalPolicy:      ontapModels.VolumeCloudRetrievalPolicyDefault,
			CoolingThresholdDays: 10,
		},
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFS}, // File protocol
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		},
		AccountID: 123,
	}

	result, err := updateAutoTieringParams(ctx, params, updateVolumeParams, volume, mockStorage)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, ontapModels.VolumeInlineTieringPolicyAuto, result.CoolAccessTieringPolicy)
	assert.Equal(t, ontapModels.VolumeCloudRetrievalPolicyDefault, result.CoolAccessRetrievalPolicy)
	assert.Equal(t, int64(10), result.CoolnessPeriod)
}

func TestUpdateAutoTieringParams_WithSnapshotOnlyPolicySetForBlockVolume(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	params := &common.UpdateVolumeParams{
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        ontapModels.VolumeInlineTieringPolicySnapshotOnly, // Explicitly set
			RetrievalPolicy:      ontapModels.VolumeCloudRetrievalPolicyDefault,
			CoolingThresholdDays: 10,
		},
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolISCSI}, // Block protocol
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		},
		AccountID: 123,
	}

	result, err := updateAutoTieringParams(ctx, params, updateVolumeParams, volume, mockStorage)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, ontapModels.VolumeInlineTieringPolicySnapshotOnly, result.CoolAccessTieringPolicy)
	assert.Equal(t, ontapModels.VolumeCloudRetrievalPolicyDefault, result.CoolAccessRetrievalPolicy)
	assert.Equal(t, int64(10), result.CoolnessPeriod)
}

func TestUpdateSMBShareSettings_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable", "encrypt_data"},
	}

	node := &models.Node{}

	// Mock CifsShareCollectionGet to return share without continuously_available
	mockProvider.On("CifsShareCollectionGet", "test-svm-uuid", "test-share", []string{"continuously_available"}).
		Return([]string{"browsable"}, nil)

	// Mock UpdateCIFSServer
	mockProvider.On("UpdateCIFSServer", "test-svm-uuid", "test-share", []string{"browsable", "encrypt_data"}).
		Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateSMBShareSettings_EmptyJunctionPath(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "",
			},
		},
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable"},
	}

	node := &models.Node{}

	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, params, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	mockProvider.AssertExpectations(t)
}

func TestUpdateSMBShareSettings_GetProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable"},
	}

	node := &models.Node{}

	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, params, node)
	assert.Error(t, err)
}

func TestUpdateSMBShareSettings_ShareNotFound(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable"},
	}

	node := &models.Node{}

	// Mock CifsShareCollectionGet to return not found error
	notFoundErr := errors.NewNotFoundErr("share", nil)
	mockProvider.On("CifsShareCollectionGet", "test-svm-uuid", "test-share", []string{"continuously_available"}).
		Return(nil, notFoundErr)

	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, params, node)
	assert.NoError(t, err) // Should not return error when share not found
	mockProvider.AssertExpectations(t)
}

func TestUpdateSMBShareSettings_ContinuouslyAvailableNotAllowed(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable", "encrypt_data"},
	}

	node := &models.Node{}

	// Mock CifsShareCollectionGet to return share with continuously_available
	mockProvider.On("CifsShareCollectionGet", "test-svm-uuid", "test-share", []string{"continuously_available"}).
		Return([]string{"browsable", "continuously_available"}, nil)

	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, params, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continuously_available share property cannot be modified")
	mockProvider.AssertExpectations(t)
}

func TestUpdateSMBShareSettings_NoChangesDetected(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable", "encrypt_data"},
	}

	node := &models.Node{}

	// Mock CifsShareCollectionGet to return all requested settings already present
	mockProvider.On("CifsShareCollectionGet", "test-svm-uuid", "test-share", []string{"continuously_available"}).
		Return([]string{"browsable", "encrypt_data", "oplocks"}, nil)

	// UpdateCIFSServer should NOT be called since no changes are needed
	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateSMBShareSettings_CifsShareCollectionGetError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable"},
	}

	node := &models.Node{}

	// Mock CifsShareCollectionGet to return generic error
	expectedErr := errors.New("connection error")
	mockProvider.On("CifsShareCollectionGet", "test-svm-uuid", "test-share", []string{"continuously_available"}).
		Return(nil, expectedErr)

	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, params, node)
	assert.Error(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateSMBShareSettings_UpdateCIFSServerError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable", "encrypt_data"},
	}

	node := &models.Node{}

	// Mock CifsShareCollectionGet to succeed (returns only "browsable", so "encrypt_data" is new)
	mockProvider.On("CifsShareCollectionGet", "test-svm-uuid", "test-share", []string{"continuously_available"}).
		Return([]string{"browsable"}, nil)

	// Mock UpdateCIFSServer to fail
	expectedErr := errors.New("update failed")
	mockProvider.On("UpdateCIFSServer", "test-svm-uuid", "test-share", []string{"browsable", "encrypt_data"}).
		Return(expectedErr)

	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, params, node)
	assert.Error(t, err)
	mockProvider.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams_WithSMBSettings(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"ENCRYPT_DATA", "BROWSABLE", "ACCESS_BASED_ENUMERATION"},
	}

	updatedFields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	assert.NoError(t, err)
	assert.NotNil(t, updatedFields)
	assert.Contains(t, updatedFields, "volume_attributes")

	volumeAttrs, ok := updatedFields["volume_attributes"].(*datamodel.VolumeAttributes)
	assert.True(t, ok)
	assert.NotNil(t, volumeAttrs.FileProperties)
	assert.Equal(t, []string{"ENCRYPT_DATA", "BROWSABLE", "ACCESS_BASED_ENUMERATION"}, volumeAttrs.FileProperties.SMBShareSettings)
}

func TestGetUpdatedFieldsFromParams_WithSMBSettings_InitializesVolumeAttributes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:             "test-volume",
		VolumeAttributes: nil, // Testing nil VolumeAttributes
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"ENCRYPT_DATA"},
	}

	updatedFields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	assert.NoError(t, err)
	assert.NotNil(t, updatedFields)
	assert.Contains(t, updatedFields, "volume_attributes")

	volumeAttrs, ok := updatedFields["volume_attributes"].(*datamodel.VolumeAttributes)
	assert.True(t, ok)
	assert.NotNil(t, volumeAttrs)
	assert.NotNil(t, volumeAttrs.FileProperties)
	assert.Equal(t, []string{"ENCRYPT_DATA"}, volumeAttrs.FileProperties.SMBShareSettings)
}

func TestUpdateSMBShareSettings_NilParams(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	activity := VolumeUpdateActivity{}
	env.RegisterActivity(activity.UpdateSMBShareSettings)

	volume := &datamodel.Volume{
		Name: "test-volume",
	}

	// Test with nil params
	_, err := env.ExecuteActivity(activity.UpdateSMBShareSettings, volume, nil, &models.Node{})
	assert.NoError(t, err) // Should return nil without error

	// Test with nil volume
	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"browsable"},
	}
	_, err = env.ExecuteActivity(activity.UpdateSMBShareSettings, nil, params, &models.Node{})
	assert.NoError(t, err) // Should return nil without error
}

func TestGetUpdatedFieldsFromParams_WithSMBSettings_InitializesFileProperties(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: nil, // Testing nil FileProperties
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"BROWSABLE", "ACCESS_BASED_ENUMERATION"},
	}

	updatedFields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	assert.NoError(t, err)
	assert.NotNil(t, updatedFields)
	assert.Contains(t, updatedFields, "volume_attributes")

	volumeAttrs, ok := updatedFields["volume_attributes"].(*datamodel.VolumeAttributes)
	assert.True(t, ok)
	assert.NotNil(t, volumeAttrs.FileProperties)
	assert.Equal(t, []string{"BROWSABLE", "ACCESS_BASED_ENUMERATION"}, volumeAttrs.FileProperties.SMBShareSettings)
}

func TestGetUpdatedFieldsFromParams_EmptySMBSettings(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath:     "/test-share",
				SMBShareSettings: []string{"EXISTING_SETTING"},
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{}, // Empty SMB settings
	}

	updatedFields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	assert.NoError(t, err)
	assert.NotNil(t, updatedFields)

	// When SMBShareSettings is empty, existing settings should be preserved
	volumeAttrs, ok := updatedFields["volume_attributes"].(*datamodel.VolumeAttributes)
	assert.True(t, ok)
	assert.Equal(t, []string{"EXISTING_SETTING"}, volumeAttrs.FileProperties.SMBShareSettings)
}

func TestUpdateVolumeInDB_WithSMBSettings_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVolumeInDB)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
	}

	params := &common.UpdateVolumeParams{
		SMBShareSettings: []string{"ENCRYPT_DATA", "BROWSABLE"},
	}

	// Mock the UpdateVolumeFields call
	mockStorage.On("UpdateVolumeFields", mock.Anything, volume.UUID, mock.MatchedBy(func(fields map[string]interface{}) bool {
		// Verify that volume_attributes contains the SMB settings
		volumeAttrs, ok := fields["volume_attributes"].(*datamodel.VolumeAttributes)
		if !ok {
			return false
		}
		if volumeAttrs.FileProperties == nil {
			return false
		}
		expectedSettings := []string{"ENCRYPT_DATA", "BROWSABLE"}
		if len(volumeAttrs.FileProperties.SMBShareSettings) != len(expectedSettings) {
			return false
		}
		for i, setting := range expectedSettings {
			if volumeAttrs.FileProperties.SMBShareSettings[i] != setting {
				return false
			}
		}
		return true
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInDB, volume, params)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeInDB_WithSMBSettingsAndOtherFields_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateVolumeInDB)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test-share",
			},
		},
	}

	snapReserve := int64(10)
	params := &common.UpdateVolumeParams{
		QuotaInBytes:     5368709120, // 5GB
		Description:      "Updated volume",
		SMBShareSettings: []string{"ACCESS_BASED_ENUMERATION"},
		SnapReserve:      &snapReserve,
	}

	// Mock the UpdateVolumeFields call
	mockStorage.On("UpdateVolumeFields", mock.Anything, volume.UUID, mock.MatchedBy(func(fields map[string]interface{}) bool {
		// Verify that multiple fields are updated correctly
		if fields["size_in_bytes"] != int64(5368709120) {
			return false
		}
		if fields["description"] != "Updated volume" {
			return false
		}
		volumeAttrs, ok := fields["volume_attributes"].(*datamodel.VolumeAttributes)
		if !ok {
			return false
		}
		if volumeAttrs.SnapReserve != int64(10) {
			return false
		}
		if volumeAttrs.FileProperties == nil {
			return false
		}
		expectedSettings := []string{"ACCESS_BASED_ENUMERATION"}
		if len(volumeAttrs.FileProperties.SMBShareSettings) != len(expectedSettings) {
			return false
		}
		for i, setting := range expectedSettings {
			if volumeAttrs.FileProperties.SMBShareSettings[i] != setting {
				return false
			}
		}
		return fields["state"] == models.LifeCycleStateREADY
	})).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateVolumeInDB, volume, params)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams_WithKmsGrant(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	kmsGrant := "projects/test-project/locations/us-west1/keyRings/test-keyring/cryptoKeys/test-key/cryptoKeyVersions/1"
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:      "test-volume",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "vault-123",
		},
	}

	params := &common.UpdateVolumeParams{
		DataProtection: &models.UpdateDataProtection{
			KmsGrant: &kmsGrant,
		},
	}

	updatedFields, err := getUpdatedFieldsFromParams(ctx, mockStorage, volume, params)

	assert.NoError(t, err)
	assert.NotNil(t, updatedFields)
	assert.Contains(t, updatedFields, "data_protection")

	dataProtection, ok := updatedFields["data_protection"].(*datamodel.DataProtection)
	assert.True(t, ok)
	assert.NotNil(t, dataProtection)
	assert.Equal(t, kmsGrant, *dataProtection.KmsGrant)
	assert.Equal(t, "vault-123", dataProtection.BackupVaultID)
}

func TestGenerateResourceNamesForBackupVault_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
	}
	tenancyDetails := &common.TenancyInfo{RegionalTenantProject: "test-project"}
	gcpRegion := "us-central1"
	originalGetResourceNamesForBackup := GetResourceNamesForBackup
	defer func() { GetResourceNamesForBackup = originalGetResourceNamesForBackup }()

	GetResourceNamesForBackup = func(region, location, project, vaultID string) (string, string, string, error) {
		return "test-email", "test-bucket", "test-service-account", nil
	}

	resourceNames, err := activity.GenerateResourceNamesForBackupVault(ctx, volume, tenancyDetails, gcpRegion)

	assert.NoError(t, err)
	assert.NotNil(t, resourceNames)
	assert.Equal(t, "test-email", resourceNames.Email)
	assert.Equal(t, "test-bucket", resourceNames.BucketName)
	assert.Equal(t, "test-service-account", resourceNames.ServiceAccountId)
}
