package activities

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
)

func TestMergeRestrictedActionsFromExisting(t *testing.T) {
	t.Run("copies when incoming empty and existing has DELETE", func(t *testing.T) {
		incoming := &datamodel.VolumeAttributes{ExternalUUID: "ext-new"}
		existing := &datamodel.VolumeAttributes{RestrictedActions: []string{RestrictedActionDelete}}
		mergeRestrictedActionsFromExisting(incoming, existing)
		assert.Equal(t, []string{RestrictedActionDelete}, incoming.RestrictedActions)
		assert.Equal(t, "ext-new", incoming.ExternalUUID)
	})

	t.Run("does not overwrite when incoming has actions", func(t *testing.T) {
		incoming := &datamodel.VolumeAttributes{RestrictedActions: []string{"OTHER"}}
		existing := &datamodel.VolumeAttributes{RestrictedActions: []string{RestrictedActionDelete}}
		mergeRestrictedActionsFromExisting(incoming, existing)
		assert.Equal(t, []string{"OTHER"}, incoming.RestrictedActions)
	})

	t.Run("nil attrs no-op", func(t *testing.T) {
		mergeRestrictedActionsFromExisting(nil, &datamodel.VolumeAttributes{RestrictedActions: []string{RestrictedActionDelete}})
	})
}

func TestHasVolumeDeleteRestriction(t *testing.T) {
	tests := []struct {
		name  string
		attrs *datamodel.VolumeAttributes
		want  bool
	}{
		{"nil attributes", nil, false},
		{"no restricted actions", &datamodel.VolumeAttributes{}, false},
		{"DELETE present", &datamodel.VolumeAttributes{RestrictedActions: []string{RestrictedActionDelete}}, true},
		{"other action only", &datamodel.VolumeAttributes{RestrictedActions: []string{"OTHER"}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, HasVolumeDeleteRestriction(tc.attrs))
		})
	}
}

func TestCheckDeleteProtection_NoVolumeAttributes(t *testing.T) {
	assert.NoError(t, CheckDeleteProtection(context.Background(), &datamodel.Volume{Name: "vol"}, nil, nil))
}

func TestCheckDeleteProtection_NoDeleteRestriction_Skips(t *testing.T) {
	tests := []struct {
		name  string
		volume *datamodel.Volume
	}{
		{
			name: "NFS without DELETE restriction",
			volume: &datamodel.Volume{
				Name: "nfs-vol",
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{utils.ProtocolNFSv3},
				},
			},
		},
		{
			name: "SMB without DELETE restriction",
			volume: &datamodel.Volume{
				Name: "smb-vol",
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{utils.ProtocolSMB},
				},
			},
		},
		{
			name: "SAN without DELETE restriction and no host group",
			volume: &datamodel.Volume{
				Name: "san-vol",
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{utils.ProtocolISCSI},
					BlockProperties: &datamodel.BlockProperties{
						HostGroupDetails: []datamodel.HostGroupDetail{{HostGroupUUID: ""}},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.NoError(t, CheckDeleteProtection(context.Background(), tc.volume, nil, nil))
		})
	}
}

func TestCheckDeleteProtection_SANHostGroupWithoutDeleteRestriction_Denies(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "san-vol",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolISCSI},
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{{HostGroupUUID: "hg-1"}},
			},
		},
	}
	assertDeleteProtectionDenied(
		t,
		CheckDeleteProtection(context.Background(), volume, nil, nil),
		vsaerrors.ErrDeleteVolumeRestrictedAction,
		"host group",
	)
}

func TestCheckDeleteProtection_AtAPI_SANWithoutHostGroupDoesNotRequirePool(t *testing.T) {
	ctx := context.Background()
	volume := &datamodel.Volume{
		Name: "san-vol",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{utils.ProtocolISCSI},
			RestrictedActions: []string{RestrictedActionDelete},
		},
	}
	se := database.NewMockStorage(t)
	assert.NoError(t, CheckDeleteProtection(ctx, volume, nil, se))
}

func TestCheckDeleteProtection_AtAPI_NFSMissingPoolReturnsValidationError(t *testing.T) {
	ctx := context.Background()
	volume := &datamodel.Volume{
		Name: "nfs-vol",
		Svm:  &datamodel.Svm{Name: "svm-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{utils.ProtocolNFSv3},
			RestrictedActions: []string{RestrictedActionDelete},
			ExternalUUID:      "ext-uuid",
		},
	}
	err := CheckDeleteProtection(ctx, volume, nil, database.NewMockStorage(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool is required")
}

func TestCheckDeleteProtection_NFSWithoutRestriction_Allows(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "nfs-vol",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3},
		},
	}
	assert.NoError(t, CheckDeleteProtection(context.Background(), volume, nil, nil))
}

func TestCheckDeleteProtection_SANHostGroup(t *testing.T) {
	tests := []struct {
		name  string
		attrs *datamodel.VolumeAttributes
	}{
		{
			name: "host group on block properties",
			attrs: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolISCSI},
				RestrictedActions: []string{RestrictedActionDelete},
				BlockProperties: &datamodel.BlockProperties{
					HostGroupDetails: []datamodel.HostGroupDetail{{HostGroupUUID: "hg-1"}},
				},
			},
		},
		{
			name: "host group on block device",
			attrs: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolISCSI},
				RestrictedActions: []string{RestrictedActionDelete},
				BlockDevices: &[]datamodel.BlockDevice{
					{HostGroupDetails: []datamodel.HostGroupDetail{{HostGroupUUID: "hg-2"}}},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			volume := &datamodel.Volume{Name: "san-vol", VolumeAttributes: tc.attrs}
			assertDeleteProtectionDenied(t, CheckDeleteProtection(context.Background(), volume, nil, nil), vsaerrors.ErrDeleteVolumeRestrictedAction, "host group")
		})
	}
}

func TestCheckDeleteProtection_SANNoHostGroup_Allows(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "san-vol",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{utils.ProtocolISCSI},
			RestrictedActions: []string{RestrictedActionDelete},
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{{HostGroupUUID: ""}},
			},
		},
	}
	assert.NoError(t, CheckDeleteProtection(context.Background(), volume, nil, nil))
}

func TestCheckDeleteProtection_SMB_Denies(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "smb-vol",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{utils.ProtocolSMB},
			RestrictedActions: []string{RestrictedActionDelete},
		},
	}
	assertDeleteProtectionDenied(t, CheckDeleteProtection(context.Background(), volume, nil, nil), vsaerrors.ErrDeleteVolumeRestrictedAction, "SMB")
}

func TestCheckDeleteProtection_DualNfsSmb_WithConnectedClients_Denies(t *testing.T) {
	volume := nfsVolumeWithDeleteRestriction("ext-uuid", "svm-1")
	volume.VolumeAttributes.Protocols = []string{utils.ProtocolNFSv3, utils.ProtocolSMB}
	node := &models.Node{Name: "node-1"}

	mockNAS := ontap_rest.NewMockNASClient(t)
	mockNAS.On("NfsClientsGet", mock.Anything).Return([]*ontap_rest.NfsClients{{}}, nil).Once()
	mockREST := ontap_rest.NewMockRESTClient(t)
	mockREST.EXPECT().NAS().Return(mockNAS).Once()

	restore := patchDeleteProtectionProvider(t, mockREST)
	defer restore()

	assertDeleteProtectionDenied(t, CheckDeleteProtection(context.Background(), volume, node, nil), vsaerrors.ErrDeleteVolumeRestrictedAction, "52 hours")
}

func TestCheckDeleteProtection_DualNfsSmb_NoClients_Allows(t *testing.T) {
	volume := nfsVolumeWithDeleteRestriction("ext-uuid", "svm-1")
	volume.VolumeAttributes.Protocols = []string{utils.ProtocolNFSv3, utils.ProtocolSMB}
	node := &models.Node{Name: "node-1"}

	mockNAS := ontap_rest.NewMockNASClient(t)
	mockNAS.On("NfsClientsGet", mock.MatchedBy(func(p *ontap_rest.NfsClientsGetParams) bool {
		return p != nil && p.VolumeUUID != nil && *p.VolumeUUID == "ext-uuid" && p.SvmName != nil && *p.SvmName == "svm-1"
	})).Return([]*ontap_rest.NfsClients{}, nil).Once()
	mockREST := ontap_rest.NewMockRESTClient(t)
	mockREST.EXPECT().NAS().Return(mockNAS).Once()

	restore := patchDeleteProtectionProvider(t, mockREST)
	defer restore()

	assert.NoError(t, CheckDeleteProtection(context.Background(), volume, node, nil))
}

func TestCheckDeleteProtection_NFSNotProvisioned_ReturnsInternalError(t *testing.T) {
	tests := []struct {
		name            string
		volume          *datamodel.Volume
		expectedDetails string
	}{
		{
			name: "missing SVM",
			volume: &datamodel.Volume{
				Name: "nfs-vol",
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols:         []string{utils.ProtocolNFSv3},
					RestrictedActions: []string{RestrictedActionDelete},
					ExternalUUID:      "ext-uuid",
				},
			},
			expectedDetails: "volume SVM is not set",
		},
		{
			name: "missing external UUID",
			volume: &datamodel.Volume{
				Name: "nfs-vol",
				Svm:  &datamodel.Svm{Name: "svm-1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols:         []string{utils.ProtocolNFSv3},
					RestrictedActions: []string{RestrictedActionDelete},
				},
			},
			expectedDetails: "volume external UUID is not set",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckDeleteProtection(context.Background(), tc.volume, &models.Node{}, nil)
			require.Error(t, err)
			var customErr *vsaerrors.CustomError
			require.True(t, errors.As(err, &customErr))
			assert.Equal(t, vsaerrors.ErrInternalServerError, customErr.TrackingID)
			assert.Contains(t, customErr.Unwrap().Error(), tc.expectedDetails)
		})
	}
}

func TestCheckDeleteProtection_NFSNotProvisioned_WithProvider_ReturnsInternalError(t *testing.T) {
	orig := hyperscaler.GetProviderByNode
	t.Cleanup(func() { hyperscaler.GetProviderByNode = orig })

	fakeProvider := &deleteProtectionRESTProvider{
		MockProvider: vsa.NewMockProvider(t),
		restClient:   nil,
	}
	hyperscaler.GetProviderByNode = func(context.Context, *models.Node) (vsa.Provider, error) {
		return fakeProvider, nil
	}

	volume := &datamodel.Volume{
		Name: "nfs-vol",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{utils.ProtocolNFSv3},
			RestrictedActions: []string{RestrictedActionDelete},
			ExternalUUID:      "ext-uuid",
		},
	}
	err := CheckDeleteProtection(context.Background(), volume, &models.Node{Name: "node-1"}, nil)
	require.Error(t, err)
	var customErr *vsaerrors.CustomError
	require.True(t, errors.As(err, &customErr))
	assert.Equal(t, vsaerrors.ErrInternalServerError, customErr.TrackingID)
	assert.Contains(t, customErr.Unwrap().Error(), "volume SVM is not set")
}

func TestCheckDeleteProtection_NFSProviderError_ReturnsInternalError(t *testing.T) {
	volume := nfsVolumeWithDeleteRestriction("ext-uuid", "svm-1")
	node := &models.Node{Name: "node-1"}

	orig := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(context.Context, *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider unavailable")
	}
	t.Cleanup(func() { hyperscaler.GetProviderByNode = orig })

	var deny *vsaerrors.CustomError
	err := CheckDeleteProtection(context.Background(), volume, node, nil)
	require.Error(t, err)
	require.True(t, errors.As(err, &deny))
	assert.Equal(t, vsaerrors.ErrInternalServerError, deny.TrackingID)
}

func TestCheckDeleteProtection_NFSWithConnectedClients_Denies(t *testing.T) {
	volume := nfsVolumeWithDeleteRestriction("ext-uuid", "svm-1")
	node := &models.Node{Name: "node-1"}

	mockNAS := ontap_rest.NewMockNASClient(t)
	mockNAS.On("NfsClientsGet", mock.Anything).Return([]*ontap_rest.NfsClients{{}}, nil).Once()
	mockREST := ontap_rest.NewMockRESTClient(t)
	mockREST.EXPECT().NAS().Return(mockNAS).Once()

	restore := patchDeleteProtectionProvider(t, mockREST)
	defer restore()

	assertDeleteProtectionDenied(t, CheckDeleteProtection(context.Background(), volume, node, nil), vsaerrors.ErrDeleteVolumeRestrictedAction, "52 hours")
}

func TestCheckDeleteProtection_NFSNoClients_Allows(t *testing.T) {
	volume := nfsVolumeWithDeleteRestriction("ext-uuid", "svm-1")
	node := &models.Node{Name: "node-1"}

	mockNAS := ontap_rest.NewMockNASClient(t)
	mockNAS.On("NfsClientsGet", mock.MatchedBy(func(p *ontap_rest.NfsClientsGetParams) bool {
		return p != nil && p.VolumeUUID != nil && *p.VolumeUUID == "ext-uuid" && p.SvmName != nil && *p.SvmName == "svm-1"
	})).Return([]*ontap_rest.NfsClients{}, nil).Once()
	mockREST := ontap_rest.NewMockRESTClient(t)
	mockREST.EXPECT().NAS().Return(mockNAS).Once()

	restore := patchDeleteProtectionProvider(t, mockREST)
	defer restore()

	assert.NoError(t, CheckDeleteProtection(context.Background(), volume, node, nil))
}

func TestCheckDeleteProtection_AtAPI_NFSWithConnectedClients_Denies(t *testing.T) {
	ctx := context.Background()
	se := database.NewMockStorage(t)
	poolID := int64(7)
	volume := &datamodel.Volume{
		Name: "nfs-vol",
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: poolID, UUID: "pool-uuid"},
			DeploymentName: "deploy",
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pw",
			},
		},
		Svm: &datamodel.Svm{Name: "svm-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{utils.ProtocolNFSv3},
			RestrictedActions: []string{RestrictedActionDelete},
			ExternalUUID:      "ext-uuid",
		},
	}
	se.On("GetNodesByPoolID", ctx, poolID).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{UUID: "node-uuid"},
		EndpointAddress: "10.0.0.1",
	}}, nil)

	mockNAS := ontap_rest.NewMockNASClient(t)
	mockNAS.On("NfsClientsGet", mock.Anything).Return([]*ontap_rest.NfsClients{{}}, nil).Once()
	mockREST := ontap_rest.NewMockRESTClient(t)
	mockREST.EXPECT().NAS().Return(mockNAS).Once()

	restore := patchDeleteProtectionProvider(t, mockREST)
	defer restore()

	assertDeleteProtectionDenied(t, CheckDeleteProtection(ctx, volume, nil, se), vsaerrors.ErrDeleteVolumeRestrictedAction, "52 hours")
	se.AssertExpectations(t)
}

func TestCheckDeleteProtection_UnknownProtocol_Allows(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "unknown-vol",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{"unknown-protocol"},
			RestrictedActions: []string{RestrictedActionDelete},
		},
	}
	assert.NoError(t, CheckDeleteProtection(context.Background(), volume, nil, nil))
}

func TestCheckDeleteProtection_SMBWithoutRestriction_Allows(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "smb-vol",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolSMB},
		},
	}
	assert.NoError(t, CheckDeleteProtection(context.Background(), volume, nil, nil))
}

func assertDeleteProtectionDenied(t *testing.T, err error, trackingID int, msgContains string) {
	t.Helper()
	require.Error(t, err)
	var deny *vsaerrors.CustomError
	require.True(t, errors.As(err, &deny))
	assert.Equal(t, trackingID, deny.TrackingID)
	if msgContains != "" {
		assert.Contains(t, deny.GetMessage(), msgContains)
	}
}

func assertDeleteProtectionInternalError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var deny *vsaerrors.CustomError
	require.True(t, errors.As(err, &deny))
	assert.Equal(t, vsaerrors.ErrInternalServerError, deny.TrackingID)
}

func nfsVolumeWithDeleteRestriction(externalUUID, svmName string) *datamodel.Volume {
	return &datamodel.Volume{
		Name: "nfs-vol",
		Svm:  &datamodel.Svm{Name: svmName},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{utils.ProtocolNFSv3},
			RestrictedActions: []string{RestrictedActionDelete},
			ExternalUUID:      externalUUID,
		},
	}
}

func patchDeleteProtectionProvider(t *testing.T, restClient ontap_rest.RESTClient) func() {
	t.Helper()
	fakeProvider := &deleteProtectionRESTProvider{
		MockProvider: vsa.NewMockProvider(t),
		restClient:   restClient,
	}
	orig := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(context.Context, *models.Node) (vsa.Provider, error) {
		return fakeProvider, nil
	}
	return func() { hyperscaler.GetProviderByNode = orig }
}

type deleteProtectionRESTProvider struct {
	*vsa.MockProvider
	restClient ontap_rest.RESTClient
	createErr  error
}

func (p *deleteProtectionRESTProvider) CreateRESTClient() (ontap_rest.RESTClient, error) {
	if p.createErr != nil {
		return nil, p.createErr
	}
	if p.restClient == nil {
		return nil, fmt.Errorf("rest client not configured")
	}
	return p.restClient, nil
}
