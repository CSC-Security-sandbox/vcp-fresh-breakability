package activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestUpdateVolumeInONTAP_Success(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
		AutoTieringPolicy: &common.AutoTieringPolicy{
			CoolAccessEnabled:    true,
			TieringPolicy:        "auto",
			RetrievalPolicy:      "default",
			CoolingThresholdDays: 10,
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

	err := activity.UpdateVolumeInONTAP(ctx, volume, params, node)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeInONTAP_Failure(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
		AutoTieringPolicy: &common.AutoTieringPolicy{
			CoolAccessEnabled:    true,
			TieringPolicy:        "auto",
			RetrievalPolicy:      "default",
			CoolingThresholdDays: 5,
		},
	}
	node := &models.Node{}
	expectedErr := errors.New("update failed")

	mockProvider.On("UpdateVolume", vsa.UpdateVolumeParams{
		UUID:               volume.VolumeAttributes.ExternalUUID,
		Size:               int64(params.QuotaInBytes),
		SnapshotPolicyName: SnapshotPolicyNone,
		TieringPolicy: &vsa.TieringPolicy{
			CoolnessPeriod:            int64(params.AutoTieringPolicy.CoolingThresholdDays),
			CoolAccessRetrievalPolicy: params.AutoTieringPolicy.RetrievalPolicy,
			CoolAccessTieringPolicy:   params.AutoTieringPolicy.TieringPolicy,
		},
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

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	params := &common.UpdateVolumeParams{QuotaInBytes: 4096,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			CoolAccessEnabled:    true,
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

	mockStorage.On("UpdateVolumeFields", ctx, volume.UUID, updatedFields).Return(nil)

	err := activity.UpdateVolumeInDB(ctx, volume, params)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeInDB_FailureWithPrepareField(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeUpdateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
	params := &common.UpdateVolumeParams{QuotaInBytes: 4096}

	prepareFieldsForUpdate = func(ctx context.Context, se database.Storage, volume *datamodel.Volume, params *common.UpdateVolumeParams) (map[string]interface{}, error) {
		return nil, errors.New("failed to prepare fields for update")
	}
	defer func() { prepareFieldsForUpdate = getUpdatedFieldsFromParams }()

	err := activity.UpdateVolumeInDB(ctx, volume, params)
	assert.EqualError(t, err, "failed to prepare fields for update")
	mockStorage.AssertExpectations(t)
}

func TestUpdateLun_ConflictError(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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

	conflictErr := errors.NewConflictErr("conflict")

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
	prepareFieldsForUpdate = func(ctx context.Context, se database.Storage, volume *datamodel.Volume, params *common.UpdateVolumeParams) (map[string]interface{}, error) {
		return updatedFields, nil
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
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
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
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
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
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
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
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
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
				DataProtection: &models.DataProtection{
					BackupVaultID: "",
				},
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
				DataProtection: &models.DataProtection{
					BackupVaultID: "vault-123",
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
					CoolAccessEnabled:    true,
					TieringPolicy:        "auto",
					CoolingThresholdDays: 7,
					RetrievalPolicy:      "default",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, _ database.Storage) {
				assert.Equal(t, true, fields["cool_access_enabled"])
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
			name: "WithAutoTieringPolicy_CoolAccessTrue_CoolnessPeriodChanged",
			volume: &datamodel.Volume{
				VolumeAttributes:  &datamodel.VolumeAttributes{},
				DataProtection:    &datamodel.DataProtection{},
				CoolAccessEnabled: false,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 5,
				},
			},
			params: &common.UpdateVolumeParams{
				AutoTieringPolicy: &common.AutoTieringPolicy{
					CoolAccessEnabled:    true,
					TieringPolicy:        "auto",
					CoolingThresholdDays: 7,
					RetrievalPolicy:      "default",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, _ database.Storage) {
				assert.Equal(t, true, fields["cool_access_enabled"])
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
			name: "WithAutoTieringPolicy_CoolAccessTrue_CoolnessPeriodSame",
			volume: &datamodel.Volume{
				VolumeAttributes:  &datamodel.VolumeAttributes{},
				DataProtection:    &datamodel.DataProtection{},
				CoolAccessEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 7,
				},
			},
			params: &common.UpdateVolumeParams{
				AutoTieringPolicy: &common.AutoTieringPolicy{
					CoolAccessEnabled:    true,
					TieringPolicy:        "auto",
					CoolingThresholdDays: 7,
					RetrievalPolicy:      "default",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, _ database.Storage) {
				// Should not update coolness_period since it's the same
				assert.NotContains(t, fields, "coolness_period")
				assert.NotContains(t, fields, "cool_access")
				assert.NotContains(t, fields, "cool_access_tiering_policy")
				assert.NotContains(t, fields, "cool_access_retrieval_policy")
				assert.Equal(t, models.LifeCycleStateREADY, fields["state"])
				assert.Equal(t, models.LifeCycleStateAvailableDetails, fields["state_details"])
			},
			dbCallRequired: false,
		},
		{
			name: "WithAutoTieringPolicy_CoolAccessFalse",
			volume: &datamodel.Volume{
				VolumeAttributes:  &datamodel.VolumeAttributes{},
				DataProtection:    &datamodel.DataProtection{},
				CoolAccessEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 7,
				},
			},
			params: &common.UpdateVolumeParams{
				AutoTieringPolicy: &common.AutoTieringPolicy{
					CoolAccessEnabled:    false,
					TieringPolicy:        "none",
					CoolingThresholdDays: 0,
					RetrievalPolicy:      "default",
				},
			},
			check: func(t *testing.T, fields map[string]interface{}, _ *datamodel.Volume, _ database.Storage) {
				assert.Equal(t, false, fields["cool_access_enabled"])
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

				mockProvider.On("IgroupGet", mock.Anything, mock.Anything).Return(&ontap_rest.Igroup{Igroup: ontapModels.Igroup{
					UUID: nillable.GetStringPtr("ontap-uuid-1")}}, nil).Times(2)

				mockProvider.On("LunMapDelete", vsa.LunMapDeleteParams{
					LunUUID:    "lun-uuid",
					IGroupUUID: "ontap-uuid-1",
				}).Return(nil).Times(2)
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

				mockProvider.On("IgroupGet", mock.Anything, mock.Anything).Return(&ontap_rest.Igroup{Igroup: ontapModels.Igroup{
					UUID: nillable.GetStringPtr("ontap-uuid")}}, nil)
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

				mockProvider.On("IgroupGet", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := database.NewMockStorage(t)
			mockProvider := vsa.NewMockProvider(t)

			originalGetProviderByNode := GetProviderByNode
			defer func() { GetProviderByNode = originalGetProviderByNode }()

			GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
				return mockProvider, nil
			}
			tt.mockSetup(mockStorage, mockProvider)

			activity := &VolumeUpdateActivity{
				SE: mockStorage,
			}

			err := activity.UnmapHostGroupFromDisk(context.Background(), tt.volume, tt.iGroupUUIDs, &models.Node{})
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
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		defer func() { GetProviderByNode = _getProviderByNode }()

		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}

		mockStorage.On("GetMultipleHostGroups", ctx, iGroups, volume.AccountID).Return(hostGroups, nil)
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

		err := activity.EnsureHostGroupsExistsAndMapDisk(ctx, volume, iGroups, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})
	t.Run("successfully all igroups are created prev", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		defer func() { GetProviderByNode = _getProviderByNode }()
		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}

		mockStorage.On("GetMultipleHostGroups", ctx, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)
		mockProvider.On("IgroupExists", "igroup2", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)

		mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
			LunName:    "/vol/" + volume.Name + "/" + utils.GetLunName(volume.Name),
			SvmName:    "test-svm",
			IGroupName: []string{"igroup1", "igroup2"},
		}).Return(nil)

		err := activity.EnsureHostGroupsExistsAndMapDisk(ctx, volume, iGroups, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})
	t.Run("returns error when GetMultipleHostGroups fails", func(t *testing.T) {
		mockProvider := vsa.NewMockProvider(t)
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockStorage.On("GetMultipleHostGroups", ctx, iGroups, volume.AccountID).Return(nil, errors.New("failed to fetch host groups"))

		err := activity.EnsureHostGroupsExistsAndMapDisk(ctx, volume, iGroups, mockNode)
		assert.EqualError(t, err, "failed to fetch host groups")
		mockStorage.AssertExpectations(t)
	})

	t.Run("returns error when IgroupExists fails", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		defer func() { GetProviderByNode = _getProviderByNode }()
		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		mockStorage.On("GetMultipleHostGroups", ctx, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, errors.New("failed to check igroup existence"))

		err := activity.EnsureHostGroupsExistsAndMapDisk(ctx, volume, iGroups, mockNode)
		assert.EqualError(t, err, "failed to check igroup existence")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error when IgroupCreate fails", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		defer func() { GetProviderByNode = _getProviderByNode }()
		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		mockStorage.On("GetMultipleHostGroups", ctx, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)
		mockProvider.On("IgroupCreate", vsa.IgroupCreateParams{
			IgroupName: "igroup1",
			SvmName:    "test-svm",
			OsType:     "linux",
			Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
		}).Return("", errors.New("failed to create igroup"))

		err := activity.EnsureHostGroupsExistsAndMapDisk(ctx, volume, iGroups, mockNode)
		assert.Error(t, err, "failed to create igroup")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})
	t.Run("returns error when LunMapCreate fails", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		defer func() { GetProviderByNode = _getProviderByNode }()
		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		mockStorage.On("GetMultipleHostGroups", ctx, iGroups, volume.AccountID).Return(hostGroups, nil)
		mockProvider.On("IgroupExists", "igroup1", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)
		mockProvider.On("IgroupExists", "igroup2", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)
		mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
			LunName:    "/vol/" + volume.Name + "/" + utils.GetLunName(volume.Name),
			SvmName:    "test-svm",
			IGroupName: []string{"igroup1", "igroup2"},
		}).Return(errors.New("failed to map lun"))

		err := activity.EnsureHostGroupsExistsAndMapDisk(ctx, volume, iGroups, mockNode)
		assert.EqualError(t, err, "failed to map lun")
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})
	t.Run("returns error when LunMapCreate fails", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)
		defer func() { GetProviderByNode = _getProviderByNode }()
		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activity := &VolumeUpdateActivity{
			SE: mockStorage,
		}
		mockStorage.On("GetMultipleHostGroups", ctx, iGroups, volume.AccountID).Return([]*datamodel.HostGroup{}, nil)

		err := activity.EnsureHostGroupsExistsAndMapDisk(ctx, volume, iGroups, mockNode)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
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
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
