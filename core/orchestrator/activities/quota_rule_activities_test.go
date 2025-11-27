package activities

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormWrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

// Test_ValidateQuotaTargetByProtocol tests the ValidateQuotaTargetByProtocol helper function
// following the pattern from Test_validateCreateReplicationParams
func Test_ValidateQuotaTargetByProtocol(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := QuotaRuleCreateActivity{SE: mockStorage}

	t.Run("SMBProtocol_ValidIndividualUserQuota_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "S-1-5-21-123456789-123456789-123456789-1000",
		}
		protocolTypes := []string{utils.ProtocolSMB}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("SMBProtocol_ValidDefaultUserQuota_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "DEFAULT_USER_QUOTA",
			QuotaTarget: "S-1-5-21-123456789-123456789-123456789-1000",
		}
		protocolTypes := []string{utils.ProtocolSMB}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("SMBProtocol_EmptyQuotaTarget_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "DEFAULT_USER_QUOTA",
			QuotaTarget: "",
		}
		protocolTypes := []string{utils.ProtocolSMB}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("SMBProtocol_GroupQuotaNotAllowed_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   QuotaIndividualGroup,
			QuotaTarget: "S-1-5-21-123456789-123456789-123456789-1000",
		}
		protocolTypes := []string{utils.ProtocolSMB}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Group Quota cannot be specified for a SMB and Dual Protocol volume")
	})

	t.Run("SMBProtocol_DefaultGroupQuotaNotAllowed_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   QuotaDefaultGroup,
			QuotaTarget: "S-1-5-21-123456789-123456789-123456789-1000",
		}
		protocolTypes := []string{utils.ProtocolSMB}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Group Quota cannot be specified for a SMB and Dual Protocol volume")
	})

	t.Run("SMBProtocol_InvalidSIDFormat_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "invalid-sid-format",
		}
		protocolTypes := []string{utils.ProtocolSMB}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass valid SID in quotaTarget for SMB volume")
	})

	t.Run("SMBProtocol_InvalidSIDPrefix_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "S-1-6-21-123456789-123456789-123456789-1000", // Invalid authority (should be 0-5)
		}
		protocolTypes := []string{utils.ProtocolSMB}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass valid SID in quotaTarget for SMB volume")
	})

	t.Run("SMBProtocol_SIDWithInvalidRID_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "S-1-5-21-123456789-123456789-123456789-999", // RID should be >= 1000
		}
		protocolTypes := []string{utils.ProtocolSMB}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass valid SID in quotaTarget for SMB volume")
	})

	t.Run("NFSv3Protocol_ValidNumericTarget_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "1000",
		}
		protocolTypes := []string{utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("NFSv3Protocol_ValidZeroTarget_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "0",
		}
		protocolTypes := []string{utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("NFSv3Protocol_ValidMaxTarget_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "4294967295", // Max value for 32-bit unsigned int
		}
		protocolTypes := []string{utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("NFSv3Protocol_EmptyTarget_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "DEFAULT_USER_QUOTA",
			QuotaTarget: "",
		}
		protocolTypes := []string{utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("NFSv3Protocol_NonNumericTarget_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "username",
		}
		protocolTypes := []string{utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass numeric value for quotaTarget in range [0, 4294967295] for NFS volumes")
	})

	t.Run("NFSv3Protocol_NegativeNumber_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "-1",
		}
		protocolTypes := []string{utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass numeric value for quotaTarget in range [0, 4294967295] for NFS volumes")
	})

	t.Run("NFSv3Protocol_OutOfRangeHigh_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "4294967296", // One more than max 32-bit unsigned int
		}
		protocolTypes := []string{utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass numeric value for quotaTarget in range [0, 4294967295] for NFS volumes")
	})

	t.Run("NFSv4Protocol_ValidNumericTarget_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "2000",
		}
		protocolTypes := []string{utils.ProtocolNFSv4}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("NFSv4Protocol_NonNumericTarget_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "user123",
		}
		protocolTypes := []string{utils.ProtocolNFSv4}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass numeric value for quotaTarget in range [0, 4294967295] for NFS volumes")
	})

	t.Run("DualProtocol_ValidSID_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "S-1-5-21-123456789-123456789-123456789-1000",
		}
		protocolTypes := []string{utils.ProtocolSMB, utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("DualProtocol_ValidNumeric_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "1000",
		}
		protocolTypes := []string{utils.ProtocolSMB, utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("DualProtocol_GroupQuotaNotAllowed_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   QuotaIndividualGroup,
			QuotaTarget: "1000",
		}
		protocolTypes := []string{utils.ProtocolSMB, utils.ProtocolNFSv4}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Group Quota cannot be specified for a SMB and Dual Protocol volume")
	})

	t.Run("DualProtocol_InvalidFormat_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "invalid-format",
		}
		protocolTypes := []string{utils.ProtocolSMB, utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass numeric value in range [0, 4294967295] or SID for quotaTarget for dual protocol volumes")
	})

	t.Run("DualProtocol_EmptyTarget_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "DEFAULT_USER_QUOTA",
			QuotaTarget: "",
		}
		protocolTypes := []string{utils.ProtocolSMB, utils.ProtocolNFSv4}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("MultipleNFSProtocols_ValidTarget_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "1000",
		}
		protocolTypes := []string{utils.ProtocolNFSv3, utils.ProtocolNFSv4}

		// With multiple NFS protocols but no SMB, it should pass validation
		// since the validation only applies to single protocol volumes
		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("EdgeCase_ConversionError_Failure", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "99999999999999999999", // Number too large for int
		}
		protocolTypes := []string{utils.ProtocolNFSv3}

		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.Error(t, err)
		// The exact error message depends on strconv.Atoi behavior
	})

	t.Run("EdgeCase_BoundaryValidSIDs_Success", func(t *testing.T) {
		testCases := []struct {
			name string
			sid  string
		}{
			{"ValidS10", "S-1-0-21-123456789-123456789-123456789-1000"},
			{"ValidS11", "S-1-1-21-123456789-123456789-123456789-1000"},
			{"ValidS12", "S-1-2-21-123456789-123456789-123456789-1000"},
			{"ValidS13", "S-1-3-21-123456789-123456789-123456789-1000"},
			{"ValidS14", "S-1-4-21-123456789-123456789-123456789-1000"},
			{"ValidS15", "S-1-5-21-123456789-123456789-123456789-1000"},
			{"ValidS19", "S-1-9-21-123456789-123456789-123456789-1000"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				quotaRule := &datamodel.QuotaRule{
					QuotaType:   "INDIVIDUAL_USER_QUOTA",
					QuotaTarget: tc.sid,
				}
				protocolTypes := []string{utils.ProtocolSMB}

				err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
				assert.NoError(t, err)
			})
		}
	})

	t.Run("EdgeCase_BoundaryInvalidSIDs_Failure", func(t *testing.T) {
		testCases := []struct {
			name string
			sid  string
		}{
			{"InvalidS16", "S-1-6-21-123456789-123456789-123456789-1000"},
			{"InvalidS17", "S-1-7-21-123456789-123456789-123456789-1000"},
			{"InvalidS18", "S-1-8-21-123456789-123456789-123456789-1000"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				quotaRule := &datamodel.QuotaRule{
					QuotaType:   "INDIVIDUAL_USER_QUOTA",
					QuotaTarget: tc.sid,
				}
				protocolTypes := []string{utils.ProtocolSMB}

				err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "quotaTarget is invalid. Please pass valid SID in quotaTarget for SMB volume")
			})
		}
	})

	t.Run("EdgeCase_EmptyProtocolTypes_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "1000",
		}
		protocolTypes := []string{}

		// With no protocols, validation should pass
		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("EdgeCase_NilProtocolTypes_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "1000",
		}
		var protocolTypes []string // nil slice

		// With nil protocols, validation should pass
		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})

	t.Run("EdgeCase_ISCSIProtocol_Success", func(t *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			QuotaTarget: "anything", // Should not be validated for iSCSI
		}
		protocolTypes := []string{utils.ProtocolISCSI}

		// iSCSI should not trigger any quota target validation
		err := activity.ValidateQuotaTargetByProtocol(ctx, quotaRule, protocolTypes)
		assert.NoError(t, err)
	})
}

// Test_GetVolumeForQuotaRule tests the GetVolumeForQuotaRule activity
// following the pattern from existing activity tests
func Test_GetVolumeForQuotaRule(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetVolumeForQuotaRule_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeUUID := "test-volume-uuid"
		expectedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: volumeUUID,
			},
			Name:        "test-volume",
			Description: "Test volume for quota rules",
			State:       "AVAILABLE",
			SizeInBytes: 1073741824, // 1GB
		}

		mockStorage.On("GetVolume", ctx, volumeUUID).Return(expectedVolume, nil)

		result, err := activity.GetVolumeForQuotaRule(ctx, volumeUUID)

		assert.NoError(t, err)
		assert.Equal(t, expectedVolume, result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeForQuotaRule_VolumeNotFound_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeUUID := "non-existent-volume-uuid"
		expectedError := errors.New("volume not found")

		mockStorage.On("GetVolume", ctx, volumeUUID).Return(nil, expectedError)

		result, err := activity.GetVolumeForQuotaRule(ctx, volumeUUID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "volume not found")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeForQuotaRule_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeUUID := "test-volume-uuid"
		expectedError := errors.New("database connection failed")

		mockStorage.On("GetVolume", ctx, volumeUUID).Return(nil, expectedError)

		result, err := activity.GetVolumeForQuotaRule(ctx, volumeUUID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "database connection failed")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeForQuotaRule_EmptyVolumeUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeUUID := ""
		expectedError := errors.New("invalid volume UUID")

		mockStorage.On("GetVolume", ctx, volumeUUID).Return(nil, expectedError)

		result, err := activity.GetVolumeForQuotaRule(ctx, volumeUUID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "invalid volume UUID")
		mockStorage.AssertExpectations(t)
	})
}

// Test_GetVolumeByID tests the GetVolumeByID activity
// following the pattern from existing activity tests
func Test_GetVolumeByID(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetVolumeByID_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(123)
		expectedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   volumeID,
				UUID: "test-volume-uuid",
			},
			Name:        "test-volume",
			Description: "Test volume for quota rules",
			State:       "AVAILABLE",
			SizeInBytes: 2147483648, // 2GB
		}
		expectedConditions := [][]interface{}{{"id = ?", volumeID}}
		expectedVolumes := []*datamodel.Volume{expectedVolume}

		mockStorage.On("ListVolumes", ctx, expectedConditions).Return(expectedVolumes, nil)

		result, err := activity.GetVolumeByID(ctx, volumeID)

		assert.NoError(t, err)
		assert.Equal(t, expectedVolume, result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_VolumeNotFound_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(999)
		expectedConditions := [][]interface{}{{"id = ?", volumeID}}
		emptyVolumes := []*datamodel.Volume{} // No volumes returned

		mockStorage.On("ListVolumes", ctx, expectedConditions).Return(emptyVolumes, nil)

		result, err := activity.GetVolumeByID(ctx, volumeID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "volume not found for ID 999")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(456)
		expectedConditions := [][]interface{}{{"id = ?", volumeID}}
		expectedError := errors.New("database query failed")

		mockStorage.On("ListVolumes", ctx, expectedConditions).Return(nil, expectedError)

		result, err := activity.GetVolumeByID(ctx, volumeID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "database query failed")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_NegativeID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(-1)
		expectedConditions := [][]interface{}{{"id = ?", volumeID}}
		emptyVolumes := []*datamodel.Volume{}

		mockStorage.On("ListVolumes", ctx, expectedConditions).Return(emptyVolumes, nil)

		result, err := activity.GetVolumeByID(ctx, volumeID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "volume not found for ID -1")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_ZeroID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(0)
		expectedConditions := [][]interface{}{{"id = ?", volumeID}}
		emptyVolumes := []*datamodel.Volume{}

		mockStorage.On("ListVolumes", ctx, expectedConditions).Return(emptyVolumes, nil)

		result, err := activity.GetVolumeByID(ctx, volumeID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "volume not found for ID 0")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_MultipleVolumesReturned_Success", func(t *testing.T) {
		// Edge case: ListVolumes returns multiple volumes, but we take the first one
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(789)
		firstVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   volumeID,
				UUID: "first-volume-uuid",
			},
			Name: "first-volume",
		}
		secondVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   volumeID,
				UUID: "second-volume-uuid",
			},
			Name: "second-volume",
		}
		expectedConditions := [][]interface{}{{"id = ?", volumeID}}
		multipleVolumes := []*datamodel.Volume{firstVolume, secondVolume}

		mockStorage.On("ListVolumes", ctx, expectedConditions).Return(multipleVolumes, nil)

		result, err := activity.GetVolumeByID(ctx, volumeID)

		// Should return the first volume without error
		assert.NoError(t, err)
		assert.Equal(t, firstVolume, result)
		assert.Equal(t, "first-volume", result.Name)
		mockStorage.AssertExpectations(t)
	})
}

// Test_CreateQuotaRuleForDataProtectionVolume tests the CreateQuotaRuleForDataProtectionVolume activity
// following the pattern from existing activity tests
func Test_CreateQuotaRuleForDataProtectionVolume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("CreateQuotaRuleForDataProtectionVolume_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			Description:    "Test quota rule for DP volume",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576, // 1GB in KiB
			State:          "CREATING",
			VolumeID:       123,
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			Description:    "Test quota rule for DP volume",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			State:          "AVAILABLE", // Updated state
			VolumeID:       123,
		}

		mockStorage.On("UpdateQuotaRule", ctx, quotaRule).Return(updatedQuotaRule, nil)

		err := activity.CreateQuotaRuleForDataProtectionVolume(ctx, quotaRule)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleForDataProtectionVolume_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			Description:    "Test quota rule for DP volume",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			State:          "CREATING",
			VolumeID:       123,
		}
		expectedError := errors.New("database update failed")

		mockStorage.On("UpdateQuotaRule", ctx, quotaRule).Return(nil, expectedError)

		err := activity.CreateQuotaRuleForDataProtectionVolume(ctx, quotaRule)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database update failed")
		mockStorage.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleForDataProtectionVolume_NilQuotaRule_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		var quotaRule *datamodel.QuotaRule = nil
		expectedError := errors.New("quota rule is nil")

		mockStorage.On("UpdateQuotaRule", ctx, quotaRule).Return(nil, expectedError)

		err := activity.CreateQuotaRuleForDataProtectionVolume(ctx, quotaRule)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quota rule is nil")
		mockStorage.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleForDataProtectionVolume_GroupQuotaType_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "group-quota-rule-uuid",
			},
			Name:           "test-group-quota-rule",
			Description:    "Test group quota rule for DP volume",
			QuotaType:      "INDIVIDUAL_GROUP_QUOTA",
			QuotaTarget:    "staff",
			DiskLimitInKib: 5242880, // 5GB in KiB
			State:          "CREATING",
			VolumeID:       456,
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "group-quota-rule-uuid",
			},
			Name:           "test-group-quota-rule",
			Description:    "Test group quota rule for DP volume",
			QuotaType:      "INDIVIDUAL_GROUP_QUOTA",
			QuotaTarget:    "staff",
			DiskLimitInKib: 5242880,
			State:          "AVAILABLE",
			VolumeID:       456,
		}

		mockStorage.On("UpdateQuotaRule", ctx, quotaRule).Return(updatedQuotaRule, nil)

		err := activity.CreateQuotaRuleForDataProtectionVolume(ctx, quotaRule)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleForDataProtectionVolume_DefaultQuotaType_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "default-quota-rule-uuid",
			},
			Name:           "test-default-quota-rule",
			Description:    "Test default quota rule for DP volume",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "",      // Empty target for default quota
			DiskLimitInKib: 2097152, // 2GB in KiB
			State:          "CREATING",
			VolumeID:       789,
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "default-quota-rule-uuid",
			},
			Name:           "test-default-quota-rule",
			Description:    "Test default quota rule for DP volume",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "",
			DiskLimitInKib: 2097152,
			State:          "AVAILABLE",
			VolumeID:       789,
		}

		mockStorage.On("UpdateQuotaRule", ctx, quotaRule).Return(updatedQuotaRule, nil)

		err := activity.CreateQuotaRuleForDataProtectionVolume(ctx, quotaRule)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleForDataProtectionVolume_WithRQuotaEnabled_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "rquota-rule-uuid",
			},
			Name:           "test-rquota-rule",
			Description:    "Test quota rule with RQuota enabled",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "2000",
			DiskLimitInKib: 10485760, // 10GB in KiB
			RQuota:         true,     // RQuota enabled
			State:          "CREATING",
			VolumeID:       321,
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "rquota-rule-uuid",
			},
			Name:           "test-rquota-rule",
			Description:    "Test quota rule with RQuota enabled",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "2000",
			DiskLimitInKib: 10485760,
			RQuota:         true,
			State:          "AVAILABLE",
			VolumeID:       321,
		}

		mockStorage.On("UpdateQuotaRule", ctx, quotaRule).Return(updatedQuotaRule, nil)

		err := activity.CreateQuotaRuleForDataProtectionVolume(ctx, quotaRule)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleForDataProtectionVolume_WithAttributes_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "attrs-quota-rule-uuid",
			},
			Name:           "test-attrs-quota-rule",
			Description:    "Test quota rule with attributes",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "3000",
			DiskLimitInKib: 3145728, // 3GB in KiB
			State:          "CREATING",
			VolumeID:       654,
			QuotaRuleAttributes: &datamodel.QuotaRuleAttributes{
				ExternalUUID: "external-quota-uuid",
			},
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "attrs-quota-rule-uuid",
			},
			Name:           "test-attrs-quota-rule",
			Description:    "Test quota rule with attributes",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "3000",
			DiskLimitInKib: 3145728,
			State:          "AVAILABLE",
			VolumeID:       654,
			QuotaRuleAttributes: &datamodel.QuotaRuleAttributes{
				ExternalUUID: "external-quota-uuid",
			},
		}

		mockStorage.On("UpdateQuotaRule", ctx, quotaRule).Return(updatedQuotaRule, nil)

		err := activity.CreateQuotaRuleForDataProtectionVolume(ctx, quotaRule)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
}

// Test_UpdateQuotaRuleDetails tests the UpdateQuotaRuleDetails activity
// following the pattern from existing activity tests
func Test_UpdateQuotaRuleDetails(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("UpdateQuotaRuleDetails_WithResponse_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:                "test-quota-rule",
			QuotaType:           "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:         "1000",
			DiskLimitInKib:      1048576,
			State:               models.LifeCycleStateCreating,
			QuotaRuleAttributes: &datamodel.QuotaRuleAttributes{},
		}
		quotaRuleCreateResponse := &vsa.QuotaRuleProviderResponse{
			ExternalUUID: "external-quota-uuid-123",
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			State:          models.LifeCycleStateAvailable,
			StateDetails:   models.LifeCycleStateAvailableDetails,
			QuotaRuleAttributes: &datamodel.QuotaRuleAttributes{
				ExternalUUID: "external-quota-uuid-123",
			},
		}

		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateAvailableDetails &&
				qr.QuotaRuleAttributes.ExternalUUID == "external-quota-uuid-123"
		})).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleDetails(ctx, dbQuotaRule, quotaRuleCreateResponse)

		assert.NoError(t, err)
		assert.Equal(t, models.LifeCycleStateAvailable, dbQuotaRule.State)
		assert.Equal(t, models.LifeCycleStateAvailableDetails, dbQuotaRule.StateDetails)
		assert.Equal(t, "external-quota-uuid-123", dbQuotaRule.QuotaRuleAttributes.ExternalUUID)
		mockStorage.AssertExpectations(t)
	})
	t.Run("UpdateQuotaRuleDetails_NilResponse_DefaultQuota_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "default-quota-rule-uuid",
			},
			Name:           "default-quota-rule",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "", // Empty target for default quota
			DiskLimitInKib: 1048576,
			State:          models.LifeCycleStateCreating,
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "default-quota-rule-uuid",
			},
			Name:           "default-quota-rule",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "",
			DiskLimitInKib: 1048576,
			State:          models.LifeCycleStateAvailable,
			StateDetails:   models.LifeCycleStateAvailableDetails,
		}

		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateAvailableDetails &&
				qr.QuotaTarget == ""
		})).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleDetails(ctx, dbQuotaRule, nil)

		assert.NoError(t, err)
		assert.Equal(t, models.LifeCycleStateAvailable, dbQuotaRule.State)
		assert.Equal(t, models.LifeCycleStateAvailableDetails, dbQuotaRule.StateDetails)
		mockStorage.AssertExpectations(t)
	})
	t.Run("UpdateQuotaRuleDetails_NilResponse_NonDefaultQuota_Error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000", // Non-empty target
			DiskLimitInKib: 1048576,
			State:          models.LifeCycleStateCreating,
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID:      "quota-rule-uuid",
				DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			State:          models.LifeCycleStateError,
			StateDetails:   models.LifeCycleStateCreationErrorDetails,
		}

		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.State == models.LifeCycleStateError &&
				qr.StateDetails == models.LifeCycleStateCreationErrorDetails &&
				qr.DeletedAt != nil &&
				qr.DeletedAt.Valid == true
		})).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleDetails(ctx, dbQuotaRule, nil)

		assert.NoError(t, err)
		assert.Equal(t, models.LifeCycleStateError, dbQuotaRule.State)
		assert.Equal(t, models.LifeCycleStateCreationErrorDetails, dbQuotaRule.StateDetails)
		assert.NotNil(t, dbQuotaRule.DeletedAt)
		assert.True(t, dbQuotaRule.DeletedAt.Valid)
		mockStorage.AssertExpectations(t)
	})
	t.Run("UpdateQuotaRuleDetails_NilResponse_NonCreatingState_Error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "", // Empty but state is not CREATING
			DiskLimitInKib: 1048576,
			State:          models.LifeCycleStateAvailable, // Not CREATING
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID:      "quota-rule-uuid",
				DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
			},
			Name:           "test-quota-rule",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "",
			DiskLimitInKib: 1048576,
			State:          models.LifeCycleStateError,
			StateDetails:   models.LifeCycleStateCreationErrorDetails,
		}

		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.State == models.LifeCycleStateError &&
				qr.DeletedAt != nil &&
				qr.DeletedAt.Valid == true
		})).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleDetails(ctx, dbQuotaRule, nil)

		assert.NoError(t, err)
		assert.Equal(t, models.LifeCycleStateError, dbQuotaRule.State)
		assert.NotNil(t, dbQuotaRule.DeletedAt)
		mockStorage.AssertExpectations(t)
	})
	t.Run("UpdateQuotaRuleDetails_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:                "test-quota-rule",
			QuotaType:           "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:         "1000",
			DiskLimitInKib:      1048576,
			State:               models.LifeCycleStateCreating,
			QuotaRuleAttributes: &datamodel.QuotaRuleAttributes{},
		}
		quotaRuleCreateResponse := &vsa.QuotaRuleProviderResponse{
			ExternalUUID: "external-quota-uuid-123",
		}
		expectedError := errors.New("database update failed")

		mockStorage.On("UpdateQuotaRule", ctx, mock.Anything).Return(nil, expectedError)

		err := activity.UpdateQuotaRuleDetails(ctx, dbQuotaRule, quotaRuleCreateResponse)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database update failed")
		mockStorage.AssertExpectations(t)
	})
}

// Test_GetVolumeReplication tests the GetVolumeReplication activity
// following the pattern from existing activity tests
func Test_GetVolumeReplication(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetVolumeReplication_Success_WithReplications", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(123)

		expectedReplications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "replication-uuid-1",
				},
				Name:     "replication-1",
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1-a",
					DestinationLocation: "us-east1-a",
				},
			},
			{
				BaseModel: datamodel.BaseModel{
					UUID: "replication-uuid-2",
				},
				Name:     "replication-2",
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1-a",
					DestinationLocation: "us-west1-a",
				},
			},
		}

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything, database.QueryDepthZero).Return(expectedReplications, nil)

		result, err := activity.GetVolumeReplication(ctx, volumeID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result, 2)
		assert.Equal(t, "replication-uuid-1", result[0].UUID)
		assert.Equal(t, "replication-uuid-2", result[1].UUID)
		assert.Equal(t, volumeID, result[0].VolumeID)
		assert.Equal(t, volumeID, result[1].VolumeID)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetVolumeReplication_Success_EmptyList", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(456)

		emptyReplications := []*datamodel.VolumeReplication{}

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything, database.QueryDepthZero).Return(emptyReplications, nil)

		result, err := activity.GetVolumeReplication(ctx, volumeID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result, 0)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetVolumeReplication_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(789)
		expectedError := errors.New("database query failed")

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything, database.QueryDepthZero).Return(nil, expectedError)

		result, err := activity.GetVolumeReplication(ctx, volumeID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "database query failed")
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetVolumeReplication_SingleReplication_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(999)

		singleReplication := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "single-replication-uuid",
				},
				Name:     "single-replication",
				VolumeID: volumeID,
				State:    models.LifeCycleStateAvailable,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:        "us-central1-a",
					DestinationLocation:   "us-east1-a",
					SourceVolumeUUID:      "source-vol-uuid",
					DestinationVolumeUUID: "dest-vol-uuid",
				},
			},
		}

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything, database.QueryDepthZero).Return(singleReplication, nil)

		result, err := activity.GetVolumeReplication(ctx, volumeID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result, 1)
		assert.Equal(t, "single-replication-uuid", result[0].UUID)
		assert.Equal(t, volumeID, result[0].VolumeID)
		assert.Equal(t, models.LifeCycleStateAvailable, result[0].State)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetVolumeReplication_ZeroVolumeID_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeID := int64(0)

		emptyReplications := []*datamodel.VolumeReplication{}

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything, database.QueryDepthZero).Return(emptyReplications, nil)

		result, err := activity.GetVolumeReplication(ctx, volumeID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result, 0)
		mockStorage.AssertExpectations(t)
	})
}

// Test_VerifyReplicationState tests the VerifyReplicationState activity
// Note: This function has many external dependencies, so we focus on testable paths
func Test_VerifyReplicationState(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("VerifyReplicationState_EmptyReplications_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		locationId := "us-central1-a"

		result, err := activity.VerifyReplicationState(ctx, []*datamodel.VolumeReplication{}, locationId)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result, 0)
	})
	t.Run("VerifyReplicationState_NilReplications_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		locationId := "us-central1-a"

		result, err := activity.VerifyReplicationState(ctx, nil, locationId)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result, 0)
	})
	t.Run("VerifyReplicationState_InvalidLocationId_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		invalidLocationId := "invalid-location"

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "replication-uuid-1",
				},
			},
		}

		result, err := activity.VerifyReplicationState(ctx, replications, invalidLocationId)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to parse region from locationId")
	})
	t.Run("VerifyReplicationState_NilReplicationAttributes_Skips", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "replication-uuid-1",
				},
				ReplicationAttributes: nil, // Nil attributes should be skipped
			},
		}

		result, err := activity.VerifyReplicationState(ctx, replications, locationId)

		assert.NoError(t, err)
		// When all replications are skipped, result can be nil (uninitialized slice)
		// or empty slice, both are acceptable
		if result != nil {
			assert.Len(t, result, 0)
		}
	})
	t.Run("VerifyReplicationState_SameRegion_Skips", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		locationId := "us-central1-a" // Same region as destination

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "replication-uuid-1",
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-central1-b", // Same region, different zone
				},
			},
		}

		result, err := activity.VerifyReplicationState(ctx, replications, locationId)

		assert.NoError(t, err)
		// When on destination side (same region), it should skip and return nil or empty list
		// Note: This depends on ParseRegionAndZone working correctly
		if result != nil {
			assert.Len(t, result, 0)
		}
	})
	t.Run("VerifyReplicationState_InvalidDestinationLocation_Skips", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "replication-uuid-1",
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "invalid-destination-location", // Invalid location
				},
			},
		}

		result, err := activity.VerifyReplicationState(ctx, replications, locationId)

		assert.NoError(t, err)
		// Should skip with warning when destination location parsing fails
		// Result can be nil (uninitialized) or empty slice
		if result != nil {
			assert.Len(t, result, 0)
		}
	})
}

// Test_UpdateRQuotaOnSvm tests the UpdateRQuotaOnSvm activity
func Test_UpdateRQuotaOnSvm(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("UpdateRQuotaOnSvm_EmptySVMUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{}

		err := activity.UpdateRQuotaOnSvm(ctx, "", node, true)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SVM ExternalUUID is required")
	})
	t.Run("UpdateRQuotaOnSvm_EnableRQuota_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		svmExternalUUID := "svm-external-uuid-123"
		node := &models.Node{
			Name: "test-node",
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ModifyRquota", ctx, svmExternalUUID, true).Return(nil)

		err := activity.UpdateRQuotaOnSvm(ctx, svmExternalUUID, node, true)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("UpdateRQuotaOnSvm_DisableRQuota_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		svmExternalUUID := "svm-external-uuid-456"
		node := &models.Node{
			Name: "test-node-2",
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ModifyRquota", ctx, svmExternalUUID, false).Return(nil)

		err := activity.UpdateRQuotaOnSvm(ctx, svmExternalUUID, node, false)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("UpdateRQuotaOnSvm_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		svmExternalUUID := "svm-external-uuid-789"
		node := &models.Node{}
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		err := activity.UpdateRQuotaOnSvm(ctx, svmExternalUUID, node, true)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create provider")
	})
	t.Run("UpdateRQuotaOnSvm_ModifyRquotaError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		svmExternalUUID := "svm-external-uuid-999"
		node := &models.Node{
			Name: "test-node-3",
		}
		mockProvider := new(vsa.MockProvider)
		expectedError := errors.New("failed to modify rquota")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ModifyRquota", ctx, svmExternalUUID, true).Return(expectedError)

		err := activity.UpdateRQuotaOnSvm(ctx, svmExternalUUID, node, true)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to modify rquota")
		mockProvider.AssertExpectations(t)
	})
}

// Test_HandleDefaultQuotaRuleUpdate tests the HandleDefaultQuotaRuleUpdate activity
func Test_HandleDefaultQuotaRuleUpdate(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("HandleDefaultQuotaRuleUpdate_QuotaNotFound_ReturnsNotFound", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Quota not found - returns empty UUID
		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, "").Return("", "", nil)

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Default Quota")
		mockProvider.AssertExpectations(t)
	})
	t.Run("HandleDefaultQuotaRuleUpdate_QuotaFound_SameType_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)
		quotaUUID := "quota-uuid-123"
		ontapQuotaType := "user" // Same as converted type

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, "").Return(quotaUUID, ontapQuotaType, nil)

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota updated successfully",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(params *vsa.UpdateQuotaRuleParams) bool {
			return params.ExternalQuotaRuleUUID == quotaUUID && params.DiskLimitInKibs == diskLimitInKibs
		})).Return(updateResponse, nil)

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("HandleDefaultQuotaRuleUpdate_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: nil, // No VolumeAttributes
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})
	t.Run("HandleDefaultQuotaRuleUpdate_NoSVMDetails_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: nil, // No SVM
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Volume has no SVM details")
	})
	t.Run("HandleDefaultQuotaRuleUpdate_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create provider")
	})
	t.Run("HandleDefaultQuotaRuleUpdate_GetQuotaUUIDError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)
		expectedError := errors.New("failed to get quota UUID")

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, "").Return("", "", expectedError)

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get quota UUID")
		mockProvider.AssertExpectations(t)
	})
	t.Run("HandleDefaultQuotaRuleUpdate_UpdateQuotaRuleError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)
		quotaUUID := "quota-uuid-456"
		ontapQuotaType := "user"
		expectedError := errors.New("failed to update quota")

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, "").Return(quotaUUID, ontapQuotaType, nil)
		mockProvider.On("UpdateQuotaRule", ctx, mock.Anything).Return(nil, expectedError)

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update quota")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleDefaultQuotaRuleUpdate_UpdateQuotaRuleFailure_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)
		quotaUUID := "quota-uuid-789"
		ontapQuotaType := "user"

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, "").Return(quotaUUID, ontapQuotaType, nil)

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Quota update failed",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.Anything).Return(updateResponse, nil)

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Quota update failed")
		mockProvider.AssertExpectations(t)
	})
}

// Test_CreateQuotaRuleOnONTAP tests the CreateQuotaRuleOnONTAP activity
func Test_CreateQuotaRuleOnONTAP(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("CreateQuotaRuleOnONTAP_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		quotaRule := &datamodel.QuotaRule{
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			RQuota:         false,
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		jobStatus := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}
		mockProvider.On("CreateQuotaRule", ctx, mock.MatchedBy(func(params vsa.CreateQuotaRuleParams) bool {
			return params.VolumeUUID == "volume-external-uuid" &&
				params.SVMName == "test-svm" &&
				params.QuotaTarget == "1000" &&
				params.QuotaType == "INDIVIDUAL_USER_QUOTA" &&
				params.DiskLimitInKib == 1048576 &&
				params.RQuota == false
		})).Return(jobStatus, nil)

		result, err := activity.CreateQuotaRuleOnONTAP(ctx, node, volumeDetails, quotaRule)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, vsa.JobRespSuccess, result.State)
		assert.Equal(t, "Quota rule created successfully", result.Message)
		mockProvider.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnONTAP_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}
		quotaRule := &datamodel.QuotaRule{}
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		result, err := activity.CreateQuotaRuleOnONTAP(ctx, node, volumeDetails, quotaRule)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("CreateQuotaRuleOnONTAP_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: nil,
		}
		quotaRule := &datamodel.QuotaRule{}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.CreateQuotaRuleOnONTAP(ctx, node, volumeDetails, quotaRule)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})

	t.Run("CreateQuotaRuleOnONTAP_NoSVMDetails_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: nil,
		}
		quotaRule := &datamodel.QuotaRule{}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.CreateQuotaRuleOnONTAP(ctx, node, volumeDetails, quotaRule)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Volume has no SVM details")
	})

	t.Run("CreateQuotaRuleOnONTAP_CreateQuotaRuleError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		quotaRule := &datamodel.QuotaRule{
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
		}
		mockProvider := new(vsa.MockProvider)
		expectedError := errors.New("failed to create quota rule")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateQuotaRule", ctx, mock.Anything).Return(nil, expectedError)

		result, err := activity.CreateQuotaRuleOnONTAP(ctx, node, volumeDetails, quotaRule)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to create quota rule")
		mockProvider.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnONTAP_WithRQuota_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		quotaRule := &datamodel.QuotaRule{
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "",
			DiskLimitInKib: 2097152,
			RQuota:         true,
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		jobStatus := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created",
		}
		mockProvider.On("CreateQuotaRule", ctx, mock.MatchedBy(func(params vsa.CreateQuotaRuleParams) bool {
			return params.RQuota == true
		})).Return(jobStatus, nil)

		result, err := activity.CreateQuotaRuleOnONTAP(ctx, node, volumeDetails, quotaRule)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, vsa.JobRespSuccess, result.State)
		mockProvider.AssertExpectations(t)
	})
}

// Test_GetQuotaStatus tests the GetQuotaStatus activity
func Test_GetQuotaStatus(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetQuotaStatus_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		quotaStatus := &vsa.QuotaStatus{
			Enabled: true,
			State:   vsa.QuotaStateOn,
		}
		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)

		result, err := activity.GetQuotaStatus(ctx, node, volumeDetails)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Enabled)
		assert.Equal(t, vsa.QuotaStateOn, result.State)
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetQuotaStatus_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		result, err := activity.GetQuotaStatus(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("GetQuotaStatus_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: nil,
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.GetQuotaStatus(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})

	t.Run("GetQuotaStatus_GetQuotaStatusError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
		}
		mockProvider := new(vsa.MockProvider)
		expectedError := errors.New("failed to get quota status")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(nil, expectedError)

		result, err := activity.GetQuotaStatus(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get quota status")
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetQuotaStatus_QuotaDisabled_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		quotaStatus := &vsa.QuotaStatus{
			Enabled: false,
			State:   vsa.QuotaStateOff,
		}
		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)

		result, err := activity.GetQuotaStatus(ctx, node, volumeDetails)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.False(t, result.Enabled)
		assert.Equal(t, vsa.QuotaStateOff, result.State)
		mockProvider.AssertExpectations(t)
	})
}

// Test_QuotaReinitialization tests the QuotaReinitialization activity
func Test_QuotaReinitialization(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("QuotaReinitialization_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota disabled successfully",
		}
		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled successfully",
		}
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("QuotaReinitialization_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		err := activity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("QuotaReinitialization_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: nil,
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		err := activity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})

	t.Run("QuotaReinitialization_DisableFailure_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Failed to disable quota",
		}
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)

		err := activity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to disable quota")
		mockProvider.AssertExpectations(t)
	})

	t.Run("QuotaReinitialization_EnableFailure_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota disabled",
		}
		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Failed to enable quota",
		}
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to enable quota")
		mockProvider.AssertExpectations(t)
	})
}

// Test_HandleQuotaEnableDisable tests the HandleQuotaEnableDisable activity
func Test_HandleQuotaEnableDisable(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("HandleQuotaEnableDisable_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		jobStatus := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled successfully",
		}
		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(jobStatus, nil)

		result, err := activity.HandleQuotaEnableDisable(ctx, node, volumeDetails)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, vsa.JobRespSuccess, result.State)
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnableDisable_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		result, err := activity.HandleQuotaEnableDisable(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("HandleQuotaEnableDisable_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: nil,
		}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.HandleQuotaEnableDisable(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})

	t.Run("HandleQuotaEnableDisable_QuotaEnableDisableError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		node := &models.Node{Name: "test-node"}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)
		expectedError := errors.New("failed to enable quota")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(nil, expectedError)

		result, err := activity.HandleQuotaEnableDisable(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to enable quota")
		mockProvider.AssertExpectations(t)
	})
}

// Test_QuotaEnableDisable tests the QuotaEnableDisable activity
func Test_QuotaEnableDisable(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("QuotaEnableDisable_Enable_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		jobStatus := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled",
		}
		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(jobStatus, nil)

		result, err := activity.QuotaEnableDisable(ctx, mockProvider, volumeDetails, true)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, vsa.JobRespSuccess, result.State)
		assert.Equal(t, "Quota enabled", result.Message)
		mockProvider.AssertExpectations(t)
	})

	t.Run("QuotaEnableDisable_Disable_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		jobStatus := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota disabled",
		}
		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", false).Return(jobStatus, nil)

		result, err := activity.QuotaEnableDisable(ctx, mockProvider, volumeDetails, false)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, vsa.JobRespSuccess, result.State)
		assert.Equal(t, "Quota disabled", result.Message)
		mockProvider.AssertExpectations(t)
	})

	t.Run("QuotaEnableDisable_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: nil,
		}
		mockProvider := new(vsa.MockProvider)

		result, err := activity.QuotaEnableDisable(ctx, mockProvider, volumeDetails, true)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})

	t.Run("QuotaEnableDisable_QuotaEnableDisableError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)
		expectedError := errors.New("failed to enable quota")

		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(nil, expectedError)

		result, err := activity.QuotaEnableDisable(ctx, mockProvider, volumeDetails, true)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to enable quota")
		mockProvider.AssertExpectations(t)
	})
}

// Test_CheckQuotaRuleCreationFailure tests the CheckQuotaRuleCreationFailure activity
func Test_CheckQuotaRuleCreationFailure(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("CheckQuotaRuleCreationFailure_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "",
		}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}
		mockProvider := new(vsa.MockProvider)

		err := activity.CheckQuotaRuleCreationFailure(ctx, quotaRuleResponse, mockProvider, volumeDetails)

		assert.NoError(t, err)
	})

	t.Run("CheckQuotaRuleCreationFailure_ResizeFailure_Reinitializes", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespFailure,
			Message: vsa.ResizeOperationFailed,
		}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota disabled",
		}
		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled",
		}
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.CheckQuotaRuleCreationFailure(ctx, quotaRuleResponse, mockProvider, volumeDetails)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("CheckQuotaRuleCreationFailure_ActivationFailure_Reinitializes", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespFailure,
			Message: vsa.ActivationOperationFailed,
		}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		mockProvider := new(vsa.MockProvider)

		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota disabled",
		}
		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled",
		}
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.CheckQuotaRuleCreationFailure(ctx, quotaRuleResponse, mockProvider, volumeDetails)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("CheckQuotaRuleCreationFailure_OtherFailure_ReturnsError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespFailure,
			Message: "Some other error occurred",
		}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}
		mockProvider := new(vsa.MockProvider)

		err := activity.CheckQuotaRuleCreationFailure(ctx, quotaRuleResponse, mockProvider, volumeDetails)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Some other error occurred")
	})
}

// Test_UpdateQuotaRuleState tests the UpdateQuotaRuleState activity
func Test_UpdateQuotaRuleState(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("UpdateQuotaRuleState_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}

		mockStorage.On("UpdateQuotaRule", ctx, &quotaRule).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleState_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name: "test-quota-rule",
		}
		expectedError := errors.New("database update failed")

		mockStorage.On("UpdateQuotaRule", ctx, &quotaRule).Return(nil, expectedError)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database update failed")
		mockStorage.AssertExpectations(t)
	})
}

// Test_GetDestinationVolume tests the GetDestinationVolume activity
func Test_GetDestinationVolume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetDestinationVolume_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		expectedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: destinationVolumeUUID,
			},
			Name:        "destination-volume",
			Description: "Destination volume for quota sync",
		}

		mockStorage.On("GetVolume", ctx, destinationVolumeUUID).Return(expectedVolume, nil)

		result, err := activity.GetDestinationVolume(ctx, destinationVolumeUUID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, destinationVolumeUUID, result.UUID)
		assert.Equal(t, "destination-volume", result.Name)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetDestinationVolume_VolumeNotFound_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "non-existent-uuid"
		expectedError := errors.New("volume not found")

		mockStorage.On("GetVolume", ctx, destinationVolumeUUID).Return(nil, expectedError)

		result, err := activity.GetDestinationVolume(ctx, destinationVolumeUUID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "volume not found")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetDestinationVolume_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		expectedError := errors.New("database connection failed")

		mockStorage.On("GetVolume", ctx, destinationVolumeUUID).Return(nil, expectedError)

		result, err := activity.GetDestinationVolume(ctx, destinationVolumeUUID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "database connection failed")
		mockStorage.AssertExpectations(t)
	})
}

// Test_hasProtocol tests the hasProtocol helper function
func Test_hasProtocol(t *testing.T) {
	t.Run("hasProtocol_ProtocolExists_ReturnsTrue", func(t *testing.T) {
		protocolTypes := []string{"NFSV3", "NFSV4", "SMB"}
		result := hasProtocol("NFSV3", protocolTypes)
		assert.True(t, result)
	})

	t.Run("hasProtocol_ProtocolDoesNotExist_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{"NFSV3", "NFSV4", "SMB"}
		result := hasProtocol("ISCSI", protocolTypes)
		assert.False(t, result)
	})

	t.Run("hasProtocol_EmptyProtocolTypes_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{}
		result := hasProtocol("NFSV3", protocolTypes)
		assert.False(t, result)
	})

	t.Run("hasProtocol_NilProtocolTypes_ReturnsFalse", func(t *testing.T) {
		var protocolTypes []string
		result := hasProtocol("NFSV3", protocolTypes)
		assert.False(t, result)
	})
}

// Test_HasCIFS tests the HasCIFS helper function
func Test_HasCIFS(t *testing.T) {
	t.Run("HasCIFS_ProtocolExists_ReturnsTrue", func(t *testing.T) {
		protocolTypes := []string{"SMB", "NFSV3"}
		result := HasCIFS(protocolTypes)
		assert.True(t, result)
	})

	t.Run("HasCIFS_ProtocolDoesNotExist_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{"NFSV3", "NFSV4"}
		result := HasCIFS(protocolTypes)
		assert.False(t, result)
	})

	t.Run("HasCIFS_EmptyProtocolTypes_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{}
		result := HasCIFS(protocolTypes)
		assert.False(t, result)
	})
}

// Test_HasNFSv3 tests the HasNFSv3 helper function
func Test_HasNFSv3(t *testing.T) {
	t.Run("HasNFSv3_ProtocolExists_ReturnsTrue", func(t *testing.T) {
		protocolTypes := []string{"NFSV3", "SMB"}
		result := HasNFSv3(protocolTypes)
		assert.True(t, result)
	})

	t.Run("HasNFSv3_ProtocolDoesNotExist_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{"NFSV4", "SMB"}
		result := HasNFSv3(protocolTypes)
		assert.False(t, result)
	})

	t.Run("HasNFSv3_EmptyProtocolTypes_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{}
		result := HasNFSv3(protocolTypes)
		assert.False(t, result)
	})
}

// Test_HasNFSv4 tests the HasNFSv4 helper function
func Test_HasNFSv4(t *testing.T) {
	t.Run("HasNFSv4_ProtocolExists_ReturnsTrue", func(t *testing.T) {
		protocolTypes := []string{"NFSV4", "SMB"}
		result := HasNFSv4(protocolTypes)
		assert.True(t, result)
	})

	t.Run("HasNFSv4_ProtocolDoesNotExist_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{"NFSV3", "SMB"}
		result := HasNFSv4(protocolTypes)
		assert.False(t, result)
	})

	t.Run("HasNFSv4_EmptyProtocolTypes_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{}
		result := HasNFSv4(protocolTypes)
		assert.False(t, result)
	})
}

// Test_HasDualProtocolForUserMapping tests the HasDualProtocolForUserMapping helper function
func Test_HasDualProtocolForUserMapping(t *testing.T) {
	t.Run("HasDualProtocolForUserMapping_SMBAndNFSv3_ReturnsTrue", func(t *testing.T) {
		protocolTypes := []string{"SMB", "NFSV3"}
		result := HasDualProtocolForUserMapping(protocolTypes)
		assert.True(t, result)
	})

	t.Run("HasDualProtocolForUserMapping_SMBAndNFSv4_ReturnsTrue", func(t *testing.T) {
		protocolTypes := []string{"SMB", "NFSV4"}
		result := HasDualProtocolForUserMapping(protocolTypes)
		assert.True(t, result)
	})

	t.Run("HasDualProtocolForUserMapping_OnlySMB_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{"SMB"}
		result := HasDualProtocolForUserMapping(protocolTypes)
		assert.False(t, result)
	})

	t.Run("HasDualProtocolForUserMapping_OnlyNFS_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{"NFSV3", "NFSV4"}
		result := HasDualProtocolForUserMapping(protocolTypes)
		assert.False(t, result)
	})

	t.Run("HasDualProtocolForUserMapping_EmptyProtocols_ReturnsFalse", func(t *testing.T) {
		protocolTypes := []string{}
		result := HasDualProtocolForUserMapping(protocolTypes)
		assert.False(t, result)
	})

	t.Run("HasDualProtocolForUserMapping_SMBAndBothNFS_ReturnsTrue", func(t *testing.T) {
		protocolTypes := []string{"SMB", "NFSV3", "NFSV4"}
		result := HasDualProtocolForUserMapping(protocolTypes)
		assert.True(t, result)
	})
}

// Test_ConvertGoogleProxyReplicationToModel tests the ConvertGoogleProxyReplicationToModel function
func Test_ConvertGoogleProxyReplicationToModel(t *testing.T) {
	t.Run("ConvertGoogleProxyReplicationToModel_NilResponse_ReturnsNil", func(t *testing.T) {
		result := ConvertGoogleProxyReplicationToModel(nil)
		assert.Nil(t, result)
	})

	t.Run("ConvertGoogleProxyReplicationToModel_EmptyReplications_ReturnsNil", func(t *testing.T) {
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{},
		}
		result := ConvertGoogleProxyReplicationToModel(response)
		assert.Nil(t, result)
	})

	t.Run("ConvertGoogleProxyReplicationToModel_NilReplications_ReturnsNil", func(t *testing.T) {
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: nil,
		}
		result := ConvertGoogleProxyReplicationToModel(response)
		assert.Nil(t, result)
	})

	t.Run("ConvertGoogleProxyReplicationToModel_WithMirrorState_ReturnsModel", func(t *testing.T) {
		mirrorState := googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					MirrorState: googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(mirrorState),
				},
			},
		}
		result := ConvertGoogleProxyReplicationToModel(response)
		assert.NotNil(t, result)
		assert.NotNil(t, result.MirrorState)
		assert.Equal(t, string(mirrorState), *result.MirrorState)
	})

	t.Run("ConvertGoogleProxyReplicationToModel_WithRelationshipStatus_ReturnsModel", func(t *testing.T) {
		relationshipStatus := googleproxyclient.VolumeReplicationInternalV1betaRelationshipStatusSuccess
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					RelationshipStatus: googleproxyclient.NewOptVolumeReplicationInternalV1betaRelationshipStatus(relationshipStatus),
				},
			},
		}
		result := ConvertGoogleProxyReplicationToModel(response)
		assert.NotNil(t, result)
		assert.NotNil(t, result.RelationshipStatus)
		assert.Equal(t, string(relationshipStatus), *result.RelationshipStatus)
	})

	t.Run("ConvertGoogleProxyReplicationToModel_WithBothFields_ReturnsModel", func(t *testing.T) {
		mirrorState := googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED
		relationshipStatus := googleproxyclient.VolumeReplicationInternalV1betaRelationshipStatusSuccess
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					MirrorState:        googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(mirrorState),
					RelationshipStatus: googleproxyclient.NewOptVolumeReplicationInternalV1betaRelationshipStatus(relationshipStatus),
				},
			},
		}
		result := ConvertGoogleProxyReplicationToModel(response)
		assert.NotNil(t, result)
		assert.NotNil(t, result.MirrorState)
		assert.NotNil(t, result.RelationshipStatus)
		assert.Equal(t, string(mirrorState), *result.MirrorState)
		assert.Equal(t, string(relationshipStatus), *result.RelationshipStatus)
	})

	t.Run("ConvertGoogleProxyReplicationToModel_WithoutOptionalFields_ReturnsModel", func(t *testing.T) {
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{},
			},
		}
		result := ConvertGoogleProxyReplicationToModel(response)
		assert.NotNil(t, result)
		assert.Nil(t, result.MirrorState)
		assert.Nil(t, result.RelationshipStatus)
	})
}

// Test_GetReplicationDetails tests the GetReplicationDetails function
func Test_GetReplicationDetails(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetReplicationDetails_Success", func(t *testing.T) {
		basePath := "https://test-base-path.com"
		projectNumber := "123456789"
		locationID := "us-central1-a"
		volumeReplicationID := "replication-uuid-123"
		jwt := "test-jwt-token"

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		mirrorState := googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED
		expectedResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					MirrorState: googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(mirrorState),
				},
			},
		}

		mockInvoker.On("V1betaGetMultipleReplicationsInternal", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := GetReplicationDetails(ctx, basePath, projectNumber, locationID, volumeReplicationID, jwt)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.MirrorState)
		assert.Equal(t, string(mirrorState), *result.MirrorState)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetReplicationDetails_APIError_ReturnsError", func(t *testing.T) {
		basePath := "https://test-base-path.com"
		projectNumber := "123456789"
		locationID := "us-central1-a"
		volumeReplicationID := "replication-uuid-123"
		jwt := "test-jwt-token"

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		expectedError := errors.New("API call failed")
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", ctx, mock.Anything, mock.Anything).Return(nil, expectedError)

		result, err := GetReplicationDetails(ctx, basePath, projectNumber, locationID, volumeReplicationID, jwt)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetReplicationDetails_InvalidResponseType_ReturnsError", func(t *testing.T) {
		basePath := "https://test-base-path.com"
		projectNumber := "123456789"
		locationID := "us-central1-a"
		volumeReplicationID := "replication-uuid-123"
		jwt := "test-jwt-token"

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		// Return a different response type
		invalidResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest{}
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", ctx, mock.Anything, mock.Anything).Return(invalidResponse, nil)

		result, err := GetReplicationDetails(ctx, basePath, projectNumber, locationID, volumeReplicationID, jwt)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetReplicationDetails_EmptyReplications_ReturnsError", func(t *testing.T) {
		basePath := "https://test-base-path.com"
		projectNumber := "123456789"
		locationID := "us-central1-a"
		volumeReplicationID := "replication-uuid-123"
		jwt := "test-jwt-token"

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{},
		}
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", ctx, mock.Anything, mock.Anything).Return(response, nil)

		result, err := GetReplicationDetails(ctx, basePath, projectNumber, locationID, volumeReplicationID, jwt)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})
}

// Test_FinishQuotaRuleJob tests the FinishQuotaRuleJob activity
func Test_FinishQuotaRuleJob(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("FinishQuotaRuleJob_Success", func(t *testing.T) {
		db, err := database.SetupTestDB()
		assert.NoError(t, err)

		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRuleUUID := "quota-rule-uuid-123"
		jobUUID := "job-uuid-456"
		externalUUID := "external-uuid-789"

		// Create test quota rule and job in database
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: quotaRuleUUID,
			},
			Name:                "test-quota-rule",
			QuotaRuleAttributes: &datamodel.QuotaRuleAttributes{},
		}
		err = db.Create(quotaRule).Error
		assert.NoError(t, err)

		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: jobUUID,
			},
			Type:  "CREATE_QUOTA_RULE",
			State: string(models.JobsStatePROCESSING),
		}
		err = db.Create(job).Error
		assert.NoError(t, err)

		// Mock WithTransaction to use real DB
		mockStorage.On("WithTransaction", ctx, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(dbutils.Transaction) error)
			tx := db.Begin()
			defer tx.Rollback()
			wrapper := gormWrapper.New(tx)
			err := txFunc(wrapper)
			if err == nil {
				tx.Commit()
			}
		})

		err = activity.FinishQuotaRuleJob(ctx, quotaRuleUUID, jobUUID, externalUUID, "")
		assert.NoError(t, err)

		// Verify quota rule was updated
		var updatedQuotaRule datamodel.QuotaRule
		err = db.Where("uuid = ?", quotaRuleUUID).First(&updatedQuotaRule).Error
		assert.NoError(t, err)
		assert.Equal(t, models.LifeCycleStateREADY, updatedQuotaRule.State)
		assert.Equal(t, models.LifeCycleStateReadyDetails, updatedQuotaRule.StateDetails)
		assert.Equal(t, externalUUID, updatedQuotaRule.QuotaRuleAttributes.ExternalUUID)

		// Verify job was updated
		var updatedJob datamodel.Job
		err = db.Where("uuid = ?", jobUUID).First(&updatedJob).Error
		assert.NoError(t, err)
		assert.Equal(t, string(models.JobsStateDONE), updatedJob.State)

		mockStorage.AssertExpectations(t)
	})

	t.Run("FinishQuotaRuleJob_QuotaRuleNotFound_ReturnsError", func(t *testing.T) {
		db, err := database.SetupTestDB()
		assert.NoError(t, err)

		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRuleUUID := "non-existent-uuid"
		jobUUID := "job-uuid-456"

		mockStorage.On("WithTransaction", ctx, mock.Anything).Return(errors.New("not found")).Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(dbutils.Transaction) error)
			tx := db.Begin()
			defer tx.Rollback()
			wrapper := gormWrapper.New(tx)
			err := txFunc(wrapper)
			if err != nil {
				// Return the error from the transaction function
				return
			}
		})

		err = activity.FinishQuotaRuleJob(ctx, quotaRuleUUID, jobUUID, "", "")
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("FinishQuotaRuleJob_EmptyExternalUUID_Success", func(t *testing.T) {
		db, err := database.SetupTestDB()
		assert.NoError(t, err)

		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRuleUUID := "quota-rule-uuid-456"
		jobUUID := "job-uuid-789"

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: quotaRuleUUID,
			},
			Name: "test-quota-rule-2",
		}
		err = db.Create(quotaRule).Error
		assert.NoError(t, err)

		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: jobUUID,
			},
			Type:  "CREATE_QUOTA_RULE",
			State: string(models.JobsStatePROCESSING),
		}
		err = db.Create(job).Error
		assert.NoError(t, err)

		mockStorage.On("WithTransaction", ctx, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(dbutils.Transaction) error)
			tx := db.Begin()
			defer tx.Rollback()
			wrapper := gormWrapper.New(tx)
			err := txFunc(wrapper)
			if err == nil {
				tx.Commit()
			}
		})

		err = activity.FinishQuotaRuleJob(ctx, quotaRuleUUID, jobUUID, "", "")
		assert.NoError(t, err)

		var updatedQuotaRule datamodel.QuotaRule
		err = db.Where("uuid = ?", quotaRuleUUID).First(&updatedQuotaRule).Error
		assert.NoError(t, err)
		assert.Equal(t, models.LifeCycleStateREADY, updatedQuotaRule.State)

		mockStorage.AssertExpectations(t)
	})

	t.Run("FinishQuotaRuleJob_WithExternalUUIDAndNilAttributes_CreatesAttributes", func(t *testing.T) {
		db, err := database.SetupTestDB()
		assert.NoError(t, err)

		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		quotaRuleUUID := "quota-rule-uuid-789"
		jobUUID := "job-uuid-999"
		externalUUID := "external-uuid-111"

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: quotaRuleUUID,
			},
			Name:                "test-quota-rule-3",
			QuotaRuleAttributes: nil, // Nil attributes
		}
		err = db.Create(quotaRule).Error
		assert.NoError(t, err)

		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: jobUUID,
			},
			Type:  "CREATE_QUOTA_RULE",
			State: string(models.JobsStatePROCESSING),
		}
		err = db.Create(job).Error
		assert.NoError(t, err)

		mockStorage.On("WithTransaction", ctx, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(dbutils.Transaction) error)
			tx := db.Begin()
			defer tx.Rollback()
			wrapper := gormWrapper.New(tx)
			err := txFunc(wrapper)
			if err == nil {
				tx.Commit()
			}
		})

		err = activity.FinishQuotaRuleJob(ctx, quotaRuleUUID, jobUUID, externalUUID, "")
		assert.NoError(t, err)

		var updatedQuotaRule datamodel.QuotaRule
		err = db.Where("uuid = ?", quotaRuleUUID).First(&updatedQuotaRule).Error
		assert.NoError(t, err)
		assert.NotNil(t, updatedQuotaRule.QuotaRuleAttributes)
		assert.Equal(t, externalUUID, updatedQuotaRule.QuotaRuleAttributes.ExternalUUID)

		mockStorage.AssertExpectations(t)
	})
}

// Test_HandleDefaultQuotaRuleUpdate_QuotaTypeMismatch tests the quota type mismatch scenario
func Test_HandleDefaultQuotaRuleUpdate_QuotaTypeMismatch(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("HandleDefaultQuotaRuleUpdate_QuotaTypeMismatch_TriggersReinitialization", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)
		quotaUUID := "quota-uuid-123"
		ontapQuotaType := "group" // Different from converted type "user"

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, "").Return(quotaUUID, ontapQuotaType, nil)

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota updated successfully",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.Anything).Return(updateResponse, nil)

		// Mock reinitialization calls
		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota disabled",
		}
		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled",
		}
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)
		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleDefaultQuotaRuleUpdate_ReinitializationError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "volume-external-uuid",
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "DEFAULT_USER_QUOTA"
		diskLimitInKibs := int64(1048576)
		quotaUUID := "quota-uuid-123"
		ontapQuotaType := "group" // Different type triggers reinitialization

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, "").Return(quotaUUID, ontapQuotaType, nil)

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota updated successfully",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.Anything).Return(updateResponse, nil)

		// Mock reinitialization failure
		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Failed to disable quota",
		}
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)

		err := activity.HandleDefaultQuotaRuleUpdate(ctx, volumeDetails, node, quotaType, diskLimitInKibs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to disable quota")
		mockProvider.AssertExpectations(t)
	})
}

// Test_CreateQuotaRuleOnDestination tests the CreateQuotaRuleOnDestination activity
func Test_CreateQuotaRuleOnDestination(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("CreateQuotaRuleOnDestination_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576, // 1GB in KiB
			Description:    "Test description",
		}
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		// Mock replication.InternalUtilGetPairedRegionURI to mock GetBasePath indirectly
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		// Mock auth.GetSignedJwtToken
		originalGetSignedJwtToken := auth.GetSignedJwtToken
		defer func() {
			auth.GetSignedJwtToken = originalGetSignedJwtToken
		}()
		jwtToken := "test-jwt-token"
		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return jwtToken, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		// Mock successful response
		quotaID := "quota-id-123"
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(quotaID),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateCREATING),
		}

		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber)

		assert.NoError(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnDestination_GetBasePathError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 1048576,
		}
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("failed to get base path")
		}

		err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get destination base path")
	})

	t.Run("CreateQuotaRuleOnDestination_GetJWTTokenError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 1048576,
		}
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		originalGetSignedJwtToken := auth.GetSignedJwtToken
		defer func() {
			auth.GetSignedJwtToken = originalGetSignedJwtToken
		}()
		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return "", errors.New("failed to get JWT token")
		}

		err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get JWT token")
	})

	t.Run("CreateQuotaRuleOnDestination_APIError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 1048576,
		}
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		originalGetSignedJwtToken := auth.GetSignedJwtToken
		defer func() {
			auth.GetSignedJwtToken = originalGetSignedJwtToken
		}()
		jwtToken := "test-jwt-token"
		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return jwtToken, nil
		}

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		expectedError := errors.New("API call failed")
		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(nil, expectedError)

		err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create quota rule on destination")
		mockInvoker.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnDestination_BadRequest_ReturnsError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 1048576,
		}
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		originalGetSignedJwtToken := auth.GetSignedJwtToken
		defer func() {
			auth.GetSignedJwtToken = originalGetSignedJwtToken
		}()
		jwtToken := "test-jwt-token"
		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return jwtToken, nil
		}

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		badRequestResponse := &googleproxyclient.V1betaCreateQuotaRuleVCPBadRequest{
			Message: "Invalid request",
		}
		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(badRequestResponse, nil)

		err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnDestination_NotFound_ReturnsError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 1048576,
		}
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		originalGetSignedJwtToken := auth.GetSignedJwtToken
		defer func() {
			auth.GetSignedJwtToken = originalGetSignedJwtToken
		}()
		jwtToken := "test-jwt-token"
		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return jwtToken, nil
		}

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		notFoundResponse := &googleproxyclient.V1betaCreateQuotaRuleVCPNotFound{
			Message: "Volume not found",
		}
		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(notFoundResponse, nil)

		err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnDestination_Conflict_ReturnsError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 1048576,
		}
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		originalGetSignedJwtToken := auth.GetSignedJwtToken
		defer func() {
			auth.GetSignedJwtToken = originalGetSignedJwtToken
		}()
		jwtToken := "test-jwt-token"
		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return jwtToken, nil
		}

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		conflictResponse := &googleproxyclient.V1betaCreateQuotaRuleVCPConflict{
			Message: "Quota rule already exists",
		}
		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(conflictResponse, nil)

		err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnDestination_UnexpectedResponseType_ReturnsError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 1048576,
		}
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		originalGetSignedJwtToken := auth.GetSignedJwtToken
		defer func() {
			auth.GetSignedJwtToken = originalGetSignedJwtToken
		}()
		jwtToken := "test-jwt-token"
		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return jwtToken, nil
		}

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		// Return an unexpected response type (using a different response type)
		unexpectedResponse := &googleproxyclient.V1betaCreateQuotaRuleVCPInternalServerError{
			Message: "Internal server error",
		}
		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(unexpectedResponse, nil)

		err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})
}
