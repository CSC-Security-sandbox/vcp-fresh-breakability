package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	error "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestUpdateVolumeInONTAP_Success(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
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
	}
	node := &models.Node{}

	mockProvider.On("UpdateVolume", vsa.UpdateVolumeParams{
		UUID:               volume.VolumeAttributes.ExternalUUID,
		Size:               int64(params.QuotaInBytes),
		SnapshotPolicyName: "default-snapshot-policy",
	}).Return(nil)

	err := activity.UpdateVolumeInONTAP(ctx, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInONTAP_Failure(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2048,
	}
	node := &models.Node{}
	expectedErr := errors.New("update failed")

	mockProvider.On("UpdateVolume", vsa.UpdateVolumeParams{
		UUID:               volume.VolumeAttributes.ExternalUUID,
		Size:               int64(params.QuotaInBytes),
		SnapshotPolicyName: SnapshotPolicyNone,
	}).Return(expectedErr)

	err := activity.UpdateVolumeInONTAP(ctx, volume, params, node)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErr.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateLun_Success(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
			},
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 4096,
	}
	node := &models.Node{}

	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       int64(params.QuotaInBytes),
	}).Return(nil)

	err := activity.UpdateLun(ctx, volume, params.QuotaInBytes, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateLun_Failure(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
			},
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 8192,
	}
	node := &models.Node{}
	expectedErr := errors.New("lun update failed")

	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       int64(params.QuotaInBytes),
	}).Return(expectedErr)

	err := activity.UpdateLun(ctx, volume, params.QuotaInBytes, node)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErr.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInDB_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
	params := &common.UpdateVolumeParams{QuotaInBytes: 4096}
	updatedFields := map[string]interface{}{"size": int64(4096), "state_details": models.LifeCycleStateAvailableDetails}

	// Patch prepareFieldsForUpdate to return our expected map
	originalGetUpdatedFields := prepareFieldsForUpdate
	prepareFieldsForUpdate = func(volume *datamodel.Volume, p *common.UpdateVolumeParams) map[string]interface{} {
		return updatedFields
	}
	defer func() { prepareFieldsForUpdate = originalGetUpdatedFields }()

	mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, updatedFields).Return(nil)

	err := activity.UpdateVolumeInDB(ctx, volume, params)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateLun_ConflictError(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				LunUUID: "lun-uuid-123",
			},
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 4096,
	}
	node := &models.Node{}

	conflictErr := error.NewConflictErr("conflict")

	mockProvider.On("LunUpdate", vsa.LunUpdateParams{
		UUID:       "lun-uuid-123",
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		Size:       int64(params.QuotaInBytes),
	}).Return(conflictErr)

	err := activity.UpdateLun(ctx, volume, params.QuotaInBytes, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInDB_Failure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
	params := &common.UpdateVolumeParams{QuotaInBytes: 4096}
	updatedFields := map[string]interface{}{"size": int64(4096), "state_details": models.LifeCycleStateAvailableDetails}
	expectedErr := errors.New("update failed")

	// Patch prepareFieldsForUpdate to return our expected map
	originalGetUpdatedFields := prepareFieldsForUpdate
	prepareFieldsForUpdate = func(volume *datamodel.Volume, p *common.UpdateVolumeParams) map[string]interface{} {
		return updatedFields
	}
	defer func() { prepareFieldsForUpdate = originalGetUpdatedFields }()

	mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, updatedFields).Return(expectedErr)

	err := activity.UpdateVolumeInDB(ctx, volume, params)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErr.Error())
	mockStorage.AssertExpectations(t)
}

func TestGetUpdatedFieldsFromParams(t *testing.T) {
	tests := []struct {
		name   string
		volume *datamodel.Volume
		params *common.UpdateVolumeParams
		check  func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume)
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
				Labels:       map[string]string{"env": "prod", "team": "devops"},
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume) {
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
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume) {
				assert.Equal(t, "desc", fields["description"])
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				_, hasSize := fields["size_in_bytes"]
				assert.False(t, hasSize)
				_, hasVA := fields["volume_attributes"]
				assert.False(t, hasVA)
			},
		},
		{
			name: "OnlyQuota",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 999,
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume) {
				assert.Equal(t, int64(999), fields["size_in_bytes"])
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				_, hasDesc := fields["description"]
				assert.False(t, hasDesc)
				_, hasVA := fields["volume_attributes"]
				assert.False(t, hasVA)
			},
		},
		{
			name: "OnlyLabels",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				Labels: map[string]string{"foo": "bar"},
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume) {
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
		},
		{
			name: "EmptyParams",
			volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{},
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				DataProtection: &models.DataProtection{
					BackupVaultID: "",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume) {
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				_, hasDesc := fields["description"]
				assert.False(t, hasDesc)
				_, hasSize := fields["size_in_bytes"]
				assert.False(t, hasSize)
				_, hasVA := fields["volume_attributes"]
				assert.False(t, hasVA)
			},
		},
		{
			name: "LabelsWithNilVolumeAttributes",
			volume: &datamodel.Volume{
				VolumeAttributes: nil,
				DataProtection:   &datamodel.DataProtection{},
			},
			params: &common.UpdateVolumeParams{
				Labels: map[string]string{"foo": "bar"},
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, volume *datamodel.Volume) {
				va, ok := fields["volume_attributes"].(*datamodel.VolumeAttributes)
				assert.True(t, ok)
				assert.NotNil(t, va.Labels)
				assert.Equal(t, "bar", (*va.Labels)["foo"])
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
				// Ensure VolumeAttributes was initialized
				assert.NotNil(t, volume.VolumeAttributes)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := getUpdatedFieldsFromParams(tt.volume, tt.params)
			tt.check(t, fields, tt.volume)
		})
	}
}

func TestGetVolumeFromONTAP_Success(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()
	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
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
	}).Return(expectedRes, nil)

	res, err := activity.GetVolumeFromONTAP(ctx, volume, node)
	assert.NoError(t, err)
	assert.Equal(t, expectedRes, res)
	mockProvider.AssertExpectations(t)
}

func TestGetVolumeFromONTAP_Error(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()
	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
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
	}).Return(nil, expectedErr)

	res, err := activity.GetVolumeFromONTAP(ctx, volume, node)
	assert.Error(t, err)
	assert.Nil(t, res)
	assert.EqualError(t, err, expectedErr.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateSnapshotPolicyInOntap_Success(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
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

	mockProvider.On("UpdateSnapshotPolicy", ctx, &vsa.UpdateSnapshotPolicyParams{
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

	err := activity.UpdateSnapshotPolicyInOntap(ctx, node, currentPolicy, updatingPolicy)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateSnapshotPolicyInOntap_Error(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeUpdateActivity{SE: database.NewMockStorage(t)}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
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
	mockProvider.On("UpdateSnapshotPolicy", ctx, &vsa.UpdateSnapshotPolicyParams{
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

	err := activity.UpdateSnapshotPolicyInOntap(ctx, node, currentPolicy, updatingPolicy)
	assert.Error(t, err)
	assert.EqualError(t, err, expectedErr.Error())
	mockProvider.AssertExpectations(t)
}
