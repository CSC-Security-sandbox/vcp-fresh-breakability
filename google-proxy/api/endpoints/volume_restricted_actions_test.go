package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

const deleteProtectionNotSupportedSMBMessage = "Delete protection is not supported for SMB volumes"

func TestSetRestrictedActionsFromRequest(t *testing.T) {
	t.Run("rejects DELETE for SMB when protocols are set", func(t *testing.T) {
		var actions []string
		err := setRestrictedActionsFromRequest([]gcpgenserver.RestrictedActionsV1betaItem{
			gcpgenserver.RestrictedActionsV1betaItemDELETE,
		}, &actions, []string{utils.ProtocolSMB})
		require.Error(t, err)
		assert.True(t, errors.IsUserInputValidationErr(err))
		assert.Equal(t, deleteProtectionNotSupportedSMBMessage, err.Error())
	})

	t.Run("allows DELETE for NFS", func(t *testing.T) {
		var actions []string
		err := setRestrictedActionsFromRequest([]gcpgenserver.RestrictedActionsV1betaItem{
			gcpgenserver.RestrictedActionsV1betaItemDELETE,
		}, &actions, []string{utils.ProtocolNFSv3})
		require.NoError(t, err)
		assert.Equal(t, []string{"DELETE"}, actions)
	})

	t.Run("allows DELETE when protocols empty", func(t *testing.T) {
		var actions []string
		err := setRestrictedActionsFromRequest([]gcpgenserver.RestrictedActionsV1betaItem{
			gcpgenserver.RestrictedActionsV1betaItemDELETE,
		}, &actions, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"DELETE"}, actions)
	})

	t.Run("drops RESTRICTED_ACTION_UNSPECIFIED", func(t *testing.T) {
		var actions []string
		err := setRestrictedActionsFromRequest([]gcpgenserver.RestrictedActionsV1betaItem{
			gcpgenserver.RestrictedActionsV1betaItemRESTRICTEDACTIONUNSPECIFIED,
		}, &actions, []string{utils.ProtocolNFSv3})
		require.NoError(t, err)
		assert.Nil(t, actions)
	})

	t.Run("allows DELETE for iSCSI", func(t *testing.T) {
		var actions []string
		err := setRestrictedActionsFromRequest([]gcpgenserver.RestrictedActionsV1betaItem{
			gcpgenserver.RestrictedActionsV1betaItemDELETE,
		}, &actions, []string{utils.ProtocolISCSI})
		require.NoError(t, err)
		assert.Equal(t, []string{"DELETE"}, actions)
	})

	t.Run("allows DELETE for dual NFS and SMB", func(t *testing.T) {
		var actions []string
		err := setRestrictedActionsFromRequest([]gcpgenserver.RestrictedActionsV1betaItem{
			gcpgenserver.RestrictedActionsV1betaItemDELETE,
		}, &actions, []string{utils.ProtocolNFSv3, utils.ProtocolSMB})
		require.NoError(t, err)
		assert.Equal(t, []string{"DELETE"}, actions)
	})

	t.Run("maps multiple actions preserving order", func(t *testing.T) {
		var actions []string
		err := setRestrictedActionsFromRequest([]gcpgenserver.RestrictedActionsV1betaItem{
			gcpgenserver.RestrictedActionsV1betaItemDELETE,
			gcpgenserver.RestrictedActionsV1betaItemRESTRICTEDACTIONUNSPECIFIED,
		}, &actions, []string{utils.ProtocolNFSv3})
		require.NoError(t, err)
		assert.Equal(t, []string{"DELETE"}, actions)
	})
}

func TestParseRestrictedActionsFromRequest(t *testing.T) {
	t.Run("uses volume protocols for SMB validation on update", func(t *testing.T) {
		_, err := parseRestrictedActionsFromRequest(
			[]gcpgenserver.RestrictedActionsV1betaItem{gcpgenserver.RestrictedActionsV1betaItemDELETE},
			[]string{utils.ProtocolSMB},
		)
		require.Error(t, err)
		assert.True(t, errors.IsUserInputValidationErr(err))
		assert.Equal(t, deleteProtectionNotSupportedSMBMessage, err.Error())
	})

	t.Run("clear when only UNSPECIFIED", func(t *testing.T) {
		actions, err := parseRestrictedActionsFromRequest(
			[]gcpgenserver.RestrictedActionsV1betaItem{gcpgenserver.RestrictedActionsV1betaItemRESTRICTEDACTIONUNSPECIFIED},
			[]string{utils.ProtocolNFSv3},
		)
		require.NoError(t, err)
		require.NotNil(t, actions)
		assert.Empty(t, *actions)
	})

	t.Run("clear when empty array", func(t *testing.T) {
		actions, err := parseRestrictedActionsFromRequest(
			[]gcpgenserver.RestrictedActionsV1betaItem{},
			[]string{utils.ProtocolNFSv3},
		)
		require.NoError(t, err)
		require.NotNil(t, actions)
		assert.Empty(t, *actions)
	})

	t.Run("replace with DELETE", func(t *testing.T) {
		actions, err := parseRestrictedActionsFromRequest(
			[]gcpgenserver.RestrictedActionsV1betaItem{gcpgenserver.RestrictedActionsV1betaItemDELETE},
			[]string{utils.ProtocolNFSv3},
		)
		require.NoError(t, err)
		require.NotNil(t, actions)
		assert.Equal(t, []string{"DELETE"}, *actions)
	})
}

func TestPrepareUpdateVolumeParams_RestrictedActions(t *testing.T) {
	dbVolume := &models.Volume{
		ProtocolTypes: []string{utils.ProtocolNFSv3},
	}
	params := gcpgenserver.V1betaUpdateVolumeParams{
		ProjectNumber: "123456789",
		VolumeId:      "vol-uuid",
	}

	t.Run("omitted field leaves RestrictedActions nil", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{}
		param, err := prepareUpdateVolumeParams(req, params, "us-central1", dbVolume)
		require.NoError(t, err)
		require.NotNil(t, param)
		assert.Nil(t, param.RestrictedActions)
	})

	t.Run("DELETE replaces restrictions", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			RestrictedActions: []gcpgenserver.RestrictedActionsV1betaItem{
				gcpgenserver.RestrictedActionsV1betaItemDELETE,
			},
		}
		param, err := prepareUpdateVolumeParams(req, params, "us-central1", dbVolume)
		require.NoError(t, err)
		require.NotNil(t, param.RestrictedActions)
		assert.Equal(t, []string{"DELETE"}, *param.RestrictedActions)
	})

	t.Run("UNSPECIFIED only clears restrictions", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			RestrictedActions: []gcpgenserver.RestrictedActionsV1betaItem{
				gcpgenserver.RestrictedActionsV1betaItemRESTRICTEDACTIONUNSPECIFIED,
			},
		}
		param, err := prepareUpdateVolumeParams(req, params, "us-central1", dbVolume)
		require.NoError(t, err)
		require.NotNil(t, param.RestrictedActions)
		assert.Empty(t, *param.RestrictedActions)
	})

	t.Run("empty array clears restrictions", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			RestrictedActions: []gcpgenserver.RestrictedActionsV1betaItem{},
		}
		param, err := prepareUpdateVolumeParams(req, params, "us-central1", dbVolume)
		require.NoError(t, err)
		require.NotNil(t, param.RestrictedActions)
		assert.Empty(t, *param.RestrictedActions)
	})
}
