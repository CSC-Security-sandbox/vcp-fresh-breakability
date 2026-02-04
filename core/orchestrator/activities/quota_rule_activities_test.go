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
	coreerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

// Test_GetVolumeByID tests the GetVolumeByID activity
// following the pattern from existing activity tests
func Test_GetVolumeByID(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetVolumeByID_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeID := int64(123)
		accountID := int64(1)
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

		mockStorage.On("GetVolumeByIDAndAccountID", ctx, volumeID, accountID).Return(expectedVolume, nil)

		result, err := activity.GetVolumeByID(ctx, volumeID, accountID)

		assert.NoError(t, err)
		assert.Equal(t, expectedVolume, result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_VolumeNotFound_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeID := int64(999)
		accountID := int64(1)
		expectedError := customerrors.NewNotFoundErr("volume not found for ID 999", nil)

		mockStorage.On("GetVolumeByIDAndAccountID", ctx, volumeID, accountID).Return(nil, expectedError)

		result, err := activity.GetVolumeByID(ctx, volumeID, accountID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "volume not found for ID 999")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeID := int64(456)
		accountID := int64(1)
		expectedError := errors.New("database query failed")

		mockStorage.On("GetVolumeByIDAndAccountID", ctx, volumeID, accountID).Return(nil, expectedError)

		result, err := activity.GetVolumeByID(ctx, volumeID, accountID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "database query failed")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_NegativeID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeID := int64(-1)
		accountID := int64(1)
		expectedError := customerrors.NewNotFoundErr("volume not found for ID -1", nil)

		mockStorage.On("GetVolumeByIDAndAccountID", ctx, volumeID, accountID).Return(nil, expectedError)

		result, err := activity.GetVolumeByID(ctx, volumeID, accountID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "volume not found for ID -1")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_ZeroID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeID := int64(0)
		accountID := int64(1)
		expectedError := customerrors.NewNotFoundErr("volume not found for ID 0", nil)

		mockStorage.On("GetVolumeByIDAndAccountID", ctx, volumeID, accountID).Return(nil, expectedError)

		result, err := activity.GetVolumeByID(ctx, volumeID, accountID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "volume not found for ID 0")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVolumeByID_MultipleVolumesReturned_Success", func(t *testing.T) {
		// GetVolumeByIDAndAccountID returns a single volume
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeID := int64(789)
		accountID := int64(1)
		expectedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   volumeID,
				UUID: "volume-uuid",
			},
			Name: "test-volume",
		}

		mockStorage.On("GetVolumeByIDAndAccountID", ctx, volumeID, accountID).Return(expectedVolume, nil)

		result, err := activity.GetVolumeByID(ctx, volumeID, accountID)

		assert.NoError(t, err)
		assert.Equal(t, expectedVolume, result)
		assert.Equal(t, "test-volume", result.Name)
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

// Test_GetVolumeReplication tests the GetVolumeReplication activity
// following the pattern from existing activity tests
func Test_GetVolumeReplication(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetVolumeReplication_Success_WithReplications", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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

	t.Run("VerifyReplicationState_NilReplication_ReturnsFalse", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		result, err := activity.VerifyReplicationState(ctx, nil, locationId)

		assert.NoError(t, err)
		assert.False(t, result)
	})
	t.Run("VerifyReplicationState_ReplicationWithoutAttributes_ReturnsFalse", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: nil, // Nil attributes
		}

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.NoError(t, err)
		assert.False(t, result)
	})
	t.Run("VerifyReplicationState_InvalidLocationId_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		invalidLocationId := "invalid-location"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation: "us-east1-a",
			},
		}

		result, err := activity.VerifyReplicationState(ctx, replication, invalidLocationId)

		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "failed to parse region from locationId")
	})
	t.Run("VerifyReplicationState_NilReplicationAttributes_ReturnsFalse", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: nil, // Nil attributes should return false
		}

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.NoError(t, err)
		assert.False(t, result)
	})
	t.Run("VerifyReplicationState_SameRegion_ReturnsFalse", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a" // Same region as destination

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation: "us-central1-b", // Same region, different zone
			},
		}

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.NoError(t, err)
		// When on destination side (same region), it should return false
		assert.False(t, result)
	})
	t.Run("VerifyReplicationState_InvalidDestinationLocation_ReturnsFalse", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation: "invalid-destination-location", // Invalid location
			},
		}

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.NoError(t, err)
		// Should return false when destination location parsing fails
		assert.False(t, result)
	})

	t.Run("VerifyReplicationState_ParseProjectNumberError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation: "us-east1-a",
			},
			RemoteUri: "invalid-uri-without-project-number",
		}

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			// Parse correctly based on input location
			if locationID == "us-central1-a" {
				return "us-central1", "us-central1-a", nil
			} else if locationID == "us-east1-a" {
				return "us-east1", "us-east1-a", nil
			}
			return "", "", errors.New("invalid location")
		}

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", errors.New("failed to parse project number")
		}

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "failed to parse destination project number")
	})

	t.Run("VerifyReplicationState_GetPairedRegionURIError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation: "us-east1-a",
			},
			RemoteUri: "https://us-east1.example.com/projects/123456789/locations/us-east1/volumes/vol-1",
		}

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		originalGetPairedRegionURI := utils.GetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
			utils.GetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			if locationID == "us-central1-a" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		utils.GetPairedRegionURI = func(region string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "failed to get destination base path")
	})

	t.Run("VerifyReplicationState_GetSignedJwtTokenError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:        "us-east1-a",
				DestinationReplicationUUID: "dest-replication-uuid",
				DestinationVolumeUUID:      "dest-volume-uuid",
			},
			RemoteUri: "https://us-east1.example.com/projects/123456789/locations/us-east1/volumes/vol-1",
		}

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		originalGetPairedRegionURI := utils.GetPairedRegionURI
		originalGetSignedJwtToken := auth.GetSignedJwtToken
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
			utils.GetPairedRegionURI = originalGetPairedRegionURI
			auth.GetSignedJwtToken = originalGetSignedJwtToken
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			if locationID == "us-central1-a" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		utils.GetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.example.com", nil
		}

		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return "", errors.New("failed to get signed token")
		}

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "failed to get signed token")
	})

	t.Run("VerifyReplicationState_GetReplicationDetailsError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:        "us-east1-a",
				DestinationReplicationUUID: "dest-replication-uuid",
				DestinationVolumeUUID:      "dest-volume-uuid",
			},
			RemoteUri: "https://us-east1.example.com/projects/123456789/locations/us-east1/volumes/vol-1",
		}

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		originalGetPairedRegionURI := utils.GetPairedRegionURI
		originalGetSignedJwtToken := auth.GetSignedJwtToken
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
			utils.GetPairedRegionURI = originalGetPairedRegionURI
			auth.GetSignedJwtToken = originalGetSignedJwtToken
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			if locationID == "us-central1-a" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		utils.GetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.example.com", nil
		}

		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		expectedError := errors.New("failed to fetch destination replication")
		mockInvoker.On("V1betaGetMultipleReplicationsInternal", ctx, mock.Anything, mock.Anything).Return(nil, expectedError)

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "failed to fetch destination replication")
		mockInvoker.AssertExpectations(t)
	})

	t.Run("VerifyReplicationState_MirrorStateMirrored_ReturnsTrue", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:        "us-east1-a",
				DestinationReplicationUUID: "dest-replication-uuid",
				DestinationVolumeUUID:      "dest-volume-uuid",
			},
			RemoteUri: "https://us-east1.example.com/projects/123456789/locations/us-east1/volumes/vol-1",
		}

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		originalGetPairedRegionURI := utils.GetPairedRegionURI
		originalGetSignedJwtToken := auth.GetSignedJwtToken
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
			utils.GetPairedRegionURI = originalGetPairedRegionURI
			auth.GetSignedJwtToken = originalGetSignedJwtToken
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			if locationID == "us-central1-a" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		utils.GetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.example.com", nil
		}

		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		expectedResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					MirrorState: googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED),
				},
			},
		}

		mockInvoker.On("V1betaGetMultipleReplicationsInternal", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.NoError(t, err)
		assert.True(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("VerifyReplicationState_MirrorStateUninitialized_ReturnsTrue", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		locationId := "us-central1-a"

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid-1",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:        "us-east1-a",
				DestinationReplicationUUID: "dest-replication-uuid",
				DestinationVolumeUUID:      "dest-volume-uuid",
			},
			RemoteUri: "https://us-east1.example.com/projects/123456789/locations/us-east1/volumes/vol-1",
		}

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		originalGetPairedRegionURI := utils.GetPairedRegionURI
		originalGetSignedJwtToken := auth.GetSignedJwtToken
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
			utils.GetPairedRegionURI = originalGetPairedRegionURI
			auth.GetSignedJwtToken = originalGetSignedJwtToken
			googleproxyclient.GetGProxyClient = originalGetGProxyClient
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			if locationID == "us-central1-a" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		utils.GetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.example.com", nil
		}

		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}

		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockProxyClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}

		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockProxyClient
		}

		expectedResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					MirrorState: googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
				},
			},
		}

		mockInvoker.On("V1betaGetMultipleReplicationsInternal", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.VerifyReplicationState(ctx, replication, locationId)

		assert.NoError(t, err)
		assert.True(t, result)
		mockInvoker.AssertExpectations(t)
	})
}

// Test_UpdateRQuotaOnSvm tests the UpdateRQuotaOnSvm activity
func Test_UpdateRQuotaOnSvm(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("UpdateRQuotaOnSvm_EmptySVMUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
		node := &models.Node{}
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ModifyRquota", ctx, "", true).Return(errors.New("SVM UUID cannot be empty"))

		err := commonActivity.UpdateRQuotaOnSvm(ctx, "", node, true)

		assert.Error(t, err)
	})
	t.Run("UpdateRQuotaOnSvm_EnableRQuota_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.UpdateRQuotaOnSvm(ctx, svmExternalUUID, node, true)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("UpdateRQuotaOnSvm_DisableRQuota_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.UpdateRQuotaOnSvm(ctx, svmExternalUUID, node, false)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("UpdateRQuotaOnSvm_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.UpdateRQuotaOnSvm(ctx, svmExternalUUID, node, true)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create provider")
	})
	t.Run("UpdateRQuotaOnSvm_ModifyRquotaError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.UpdateRQuotaOnSvm(ctx, svmExternalUUID, node, true)

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
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("QuotaReinitialization_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("QuotaReinitialization_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})

	t.Run("QuotaReinitialization_DisableFailure_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.QuotaReinitialization(ctx, node, volumeDetails)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to disable quota")
		mockProvider.AssertExpectations(t)
	})

	t.Run("QuotaReinitialization_EnableFailure_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := commonActivity.QuotaReinitialization(ctx, node, volumeDetails)

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
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		result, err := commonActivity.HandleQuotaEnableDisable(ctx, node, volumeDetails, true)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, vsa.JobRespSuccess, result.State)
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnableDisable_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		result, err := commonActivity.HandleQuotaEnableDisable(ctx, node, volumeDetails, true)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("HandleQuotaEnableDisable_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		result, err := commonActivity.HandleQuotaEnableDisable(ctx, node, volumeDetails, true)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})

	t.Run("HandleQuotaEnableDisable_QuotaEnableDisableError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		commonActivity := QuotaRuleCommonActivity{SE: mockStorage}
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

		result, err := commonActivity.HandleQuotaEnableDisable(ctx, node, volumeDetails, true)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to enable quota")
		mockProvider.AssertExpectations(t)
	})
}

// Test_UpdateQuotaRuleState tests the UpdateQuotaRuleState activity
func Test_UpdateQuotaRuleState(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("UpdateQuotaRuleState_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}

		// Mock GetQuotaRuleByUUID to return current state (CREATING)
		currentQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
		}
		mockStorage.On("GetQuotaRuleByUUID", ctx, "quota-rule-uuid", int64(0)).Return(currentQuotaRule, nil)

		// Mock UpdateQuotaRule with the updated state (transitions to READY)
		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == "quota-rule-uuid" &&
				qr.State == models.LifeCycleStateREADY &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(currentQuotaRule, nil)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule, false)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleState_DatabaseError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name: "test-quota-rule",
		}

		// Mock GetQuotaRuleByUUID to return current state
		currentQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
		}
		mockStorage.On("GetQuotaRuleByUUID", ctx, "quota-rule-uuid", int64(0)).Return(currentQuotaRule, nil)

		// Mock UpdateQuotaRule to return error
		expectedError := errors.New("database update failed")
		mockStorage.On("UpdateQuotaRule", ctx, mock.Anything).Return(nil, expectedError)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule, false)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database update failed")
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleState_GetQuotaRuleByUUIDError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}

		expectedError := errors.New("failed to fetch quota rule")
		mockStorage.On("GetQuotaRuleByUUID", ctx, "quota-rule-uuid", int64(1)).Return(nil, expectedError)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule, false)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch quota rule")
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleState_QuotaRuleStateError_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateError,
			StateDetails: models.LifeCycleStateCreationErrorDetails,
		}

		currentQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
		}

		mockStorage.On("GetQuotaRuleByUUID", ctx, "quota-rule-uuid", int64(1)).Return(currentQuotaRule, nil)

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateError,
			StateDetails: models.LifeCycleStateCreationErrorDetails,
		}

		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.State == models.LifeCycleStateError &&
				qr.StateDetails == models.LifeCycleStateCreationErrorDetails
		})).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule, false)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleState_StateUpdating_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			AccountID:      int64(1),
			DiskLimitInKib: 2097152,
			Description:    "updated description",
			State:          models.LifeCycleStateAvailable,
			StateDetails:   models.LifeCycleStateAvailableDetails,
		}

		currentQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			AccountID:      int64(1),
			DiskLimitInKib: 1048576,
			Description:    "old description",
			State:          models.LifeCycleStateUpdating,
			StateDetails:   models.LifeCycleStateUpdatingDetails,
		}

		mockStorage.On("GetQuotaRuleByUUID", ctx, "quota-rule-uuid", int64(1)).Return(currentQuotaRule, nil)

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			AccountID:      int64(1),
			DiskLimitInKib: 2097152,
			Description:    "updated description",
			State:          models.LifeCycleStateREADY,
			StateDetails:   models.LifeCycleStateReadyDetails,
		}

		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.State == models.LifeCycleStateREADY &&
				qr.StateDetails == models.LifeCycleStateReadyDetails &&
				qr.DiskLimitInKib == 2097152 &&
				qr.Description == "updated description"
		})).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule, false)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleState_StateDeleting_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}

		currentQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		mockStorage.On("GetQuotaRuleByUUID", ctx, "quota-rule-uuid", int64(1)).Return(currentQuotaRule, nil)

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID:      "quota-rule-uuid",
				DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateDeleted,
			StateDetails: models.LifeCycleStateDeletedDetails,
		}

		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.State == models.LifeCycleStateDeleted &&
				qr.StateDetails == models.LifeCycleStateDeletedDetails &&
				qr.DeletedAt != nil &&
				qr.DeletedAt.Valid == true
		})).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule, false)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleState_CleanupDelete_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		quotaRule := datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
		}

		currentQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
		}

		mockStorage.On("GetQuotaRuleByUUID", ctx, "quota-rule-uuid", int64(1)).Return(currentQuotaRule, nil)

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID:      "quota-rule-uuid",
				DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
			},
			Name:         "test-quota-rule",
			AccountID:    int64(1),
			State:        models.LifeCycleStateDeleted,
			StateDetails: models.LifeCycleStateDeletedDetails,
		}

		mockStorage.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.State == models.LifeCycleStateDeleted &&
				qr.StateDetails == models.LifeCycleStateDeletedDetails &&
				qr.DeletedAt != nil &&
				qr.DeletedAt.Valid == true
		})).Return(updatedQuotaRule, nil)

		err := activity.UpdateQuotaRuleState(ctx, quotaRule, true)

		assert.NoError(t, err)
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

	t.Run("GetReplicationDetails_EmptyVolumeReplicationID_ReturnsError", func(t *testing.T) {
		basePath := "https://test-base-path.com"
		projectNumber := "123456789"
		locationID := "us-central1-a"
		volumeReplicationID := "" // Empty UUID
		jwt := "test-jwt-token"

		result, err := GetReplicationDetails(ctx, basePath, projectNumber, locationID, volumeReplicationID, jwt)

		assert.Error(t, err)
		assert.Nil(t, result)
		// Verify it's an input validation error
		var customErr *coreerrors.CustomError
		assert.ErrorAs(t, err, &customErr, "Error should be a CustomError")
		if customErr != nil {
			assert.Equal(t, coreerrors.ErrInputValidationError, customErr.TrackingID)
			// Verify the original error message contains the expected text
			if customErr.OriginalErr != nil {
				assert.Contains(t, customErr.OriginalErr.Error(), "volumeReplicationID cannot be empty")
			}
		}
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

		// Mock successful response with READY state (synchronous completion)
		quotaID := "quota-id-123"
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(quotaID),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateREADY),
		}

		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsDone)
		assert.Empty(t, result.OperationName)
		assert.NotNil(t, result.QuotaRule, "destination quota rule should be returned for hydration")
		assert.Equal(t, quotaID, result.QuotaRule.UUID)
		assert.Equal(t, "test-quota-rule", result.QuotaRule.Name)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnDestination_CreatingStateWithJobId_ReturnsJobIdForPolling", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			Description:    "Test description",
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

		// Mock response with CREATING state and Jobs array containing JobId
		quotaID := "quota-id-123"
		jobID := "job-uuid-456"
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(quotaID),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateCREATING),
			Jobs: []googleproxyclient.JobV1beta{
				{
					JobId: googleproxyclient.NewOptString(jobID),
				},
			},
		}

		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.False(t, result.IsDone)
		assert.Equal(t, jobID, result.OperationName)
		assert.NotNil(t, result.QuotaRule)
		assert.Equal(t, quotaID, result.QuotaRule.UUID)
		assert.Equal(t, "test-quota-rule", result.QuotaRule.Name)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("CreateQuotaRuleOnDestination_CreatingStateWithoutJobId_ReturnsSuccess", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCreateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			Description:    "Test description",
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

		// Mock response with CREATING state but no Jobs array (edge case - assume success)
		quotaID := "quota-id-123"
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(quotaID),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateCREATING),
			Jobs:       []googleproxyclient.JobV1beta{}, // Empty Jobs array
		}

		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsDone) // Assume success when no JobId available
		assert.Empty(t, result.OperationName)
		assert.NotNil(t, result.QuotaRule)
		assert.Equal(t, quotaID, result.QuotaRule.UUID)
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

		jwtToken := "test-jwt-token"
		_, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get destination base path")
	})

	t.Run("CreateQuotaRuleOnDestination_EmptyJWTTokenError_Failure", func(t *testing.T) {
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

		// Test with empty JWT token
		emptyJwtToken := ""
		_, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &emptyJwtToken)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "JWT token is required")
	})

	t.Run("CreateQuotaRuleOnDestination_NilJWTTokenError_Failure", func(t *testing.T) {
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

		// Test with nil JWT token
		_, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "JWT token is required")
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

		_, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

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

		_, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

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

		_, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

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

		_, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

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

		_, err := activity.CreateQuotaRuleOnDestination(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})
}

// Test_GetOntapQuotaUUID tests the GetOntapQuotaUUID activity
// following the pattern from existing activity tests
func Test_GetOntapQuotaUUID(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetOntapQuotaUUID_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1000"
		expectedQuotaUUID := "quota-uuid-123"

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, target).
			Return(expectedQuotaUUID, "user", nil)

		result, err := activity.GetOntapQuotaUUID(ctx, volumeDetails, node, quotaType, target, "create")

		assert.NoError(t, err)
		assert.Equal(t, expectedQuotaUUID, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetOntapQuotaUUID_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1000"
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		result, err := activity.GetOntapQuotaUUID(ctx, volumeDetails, node, quotaType, target, "create")

		assert.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("GetOntapQuotaUUID_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeDetails := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			VolumeAttributes: nil, // No VolumeAttributes
		}
		node := &models.Node{Name: "test-node"}
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1000"

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.GetOntapQuotaUUID(ctx, volumeDetails, node, quotaType, target, "create")

		assert.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
	})

	t.Run("GetOntapQuotaUUID_NoSVMDetails_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1000"

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		result, err := activity.GetOntapQuotaUUID(ctx, volumeDetails, node, quotaType, target, "create")

		assert.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "Volume has no SVM details")
	})

	t.Run("GetOntapQuotaUUID_GetQuotaUUIDError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1000"
		expectedError := errors.New("failed to get quota UUID")

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, target).
			Return("", "", expectedError)

		result, err := activity.GetOntapQuotaUUID(ctx, volumeDetails, node, quotaType, target, "create")

		assert.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "failed to get quota UUID")
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetOntapQuotaUUID_QuotaNotFound_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaType := "INDIVIDUAL_USER_QUOTA"
		target := "1000"

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Return empty UUID (quota not found)
		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, target).
			Return("", "", nil)

		result, err := activity.GetOntapQuotaUUID(ctx, volumeDetails, node, quotaType, target, "create")

		assert.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "Quota")
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetOntapQuotaUUID_DefaultQuotaType_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		target := "" // Empty target for default quota
		expectedQuotaUUID := "default-quota-uuid-456"

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetOntapQuotaUUIDAndType", ctx, "volume-external-uuid", "test-svm", quotaType, target).
			Return(expectedQuotaUUID, "user", nil)

		result, err := activity.GetOntapQuotaUUID(ctx, volumeDetails, node, quotaType, target, "create")

		assert.NoError(t, err)
		assert.Equal(t, expectedQuotaUUID, result)
		mockProvider.AssertExpectations(t)
	})
}

// Test_UpdateQuotaRulesOnOntap tests the UpdateQuotaRulesOnOntap activity
// following the pattern from existing activity tests
func Test_UpdateQuotaRulesOnOntap(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("UpdateQuotaRulesOnOntap_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		diskLimitInKibs := int64(2097152) // 2GB in KiB

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule updated successfully",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(params *vsa.UpdateQuotaRuleParams) bool {
			return params.ExternalQuotaRuleUUID == externalQuotaUUID && params.DiskLimitInKibs == diskLimitInKibs
		})).Return(updateResponse, nil)

		err := activity.UpdateQuotaRulesOnOntap(ctx, externalQuotaUUID, node, diskLimitInKibs)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRulesOnOntap_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		diskLimitInKibs := int64(2097152)
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		err := activity.UpdateQuotaRulesOnOntap(ctx, externalQuotaUUID, node, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("UpdateQuotaRulesOnOntap_UpdateQuotaRuleError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		diskLimitInKibs := int64(2097152)
		expectedError := errors.New("failed to update quota rule")

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("UpdateQuotaRule", ctx, mock.Anything).Return(nil, expectedError)

		err := activity.UpdateQuotaRulesOnOntap(ctx, externalQuotaUUID, node, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update quota rule")
		mockProvider.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRulesOnOntap_UpdateQuotaRuleFailure_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		diskLimitInKibs := int64(2097152)

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Quota update failed",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.Anything).Return(updateResponse, nil)

		err := activity.UpdateQuotaRulesOnOntap(ctx, externalQuotaUUID, node, diskLimitInKibs)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Quota update failed")
		mockProvider.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRulesOnOntap_ZeroDiskLimit_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		diskLimitInKibs := int64(0) // Zero disk limit (should still work)

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule updated successfully",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(params *vsa.UpdateQuotaRuleParams) bool {
			return params.ExternalQuotaRuleUUID == externalQuotaUUID && params.DiskLimitInKibs == diskLimitInKibs
		})).Return(updateResponse, nil)

		err := activity.UpdateQuotaRulesOnOntap(ctx, externalQuotaUUID, node, diskLimitInKibs)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
}

// Test_RevertQuotaRulesOnSource tests the RevertQuotaRulesOnSource activity
// following the pattern from existing activity tests
func Test_RevertQuotaRulesOnSource(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("RevertQuotaRulesOnSource_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		originalDiskLimitInKib := int64(1048576) // 1GB in KiB (original value)

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule reverted successfully",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.MatchedBy(func(params *vsa.UpdateQuotaRuleParams) bool {
			return params.ExternalQuotaRuleUUID == externalQuotaUUID && params.DiskLimitInKibs == originalDiskLimitInKib
		})).Return(updateResponse, nil)

		err := activity.RevertQuotaRulesOnSource(ctx, externalQuotaUUID, node, originalDiskLimitInKib)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("RevertQuotaRulesOnSource_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		originalDiskLimitInKib := int64(1048576)
		expectedError := errors.New("failed to create provider")

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		err := activity.RevertQuotaRulesOnSource(ctx, externalQuotaUUID, node, originalDiskLimitInKib)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("RevertQuotaRulesOnSource_UpdateQuotaRuleError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		originalDiskLimitInKib := int64(1048576)
		expectedError := errors.New("failed to revert quota rule")

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("UpdateQuotaRule", ctx, mock.Anything).Return(nil, expectedError)

		err := activity.RevertQuotaRulesOnSource(ctx, externalQuotaUUID, node, originalDiskLimitInKib)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to revert quota rule")
		mockProvider.AssertExpectations(t)
	})

	t.Run("RevertQuotaRulesOnSource_UpdateQuotaRuleFailure_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		externalQuotaUUID := "quota-uuid-123"
		node := &models.Node{Name: "test-node"}
		originalDiskLimitInKib := int64(1048576)

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		updateResponse := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Failed to revert quota rule",
		}
		mockProvider.On("UpdateQuotaRule", ctx, mock.Anything).Return(updateResponse, nil)

		err := activity.RevertQuotaRulesOnSource(ctx, externalQuotaUUID, node, originalDiskLimitInKib)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to revert quota rule")
		mockProvider.AssertExpectations(t)
	})
}

// Test_GetMatchingQuotaRuleOnDestination tests the GetMatchingQuotaRuleOnDestination activity
// following the pattern from existing activity tests
func Test_GetMatchingQuotaRuleOnDestination(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("GetMatchingQuotaRuleOnDestination_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"
		expectedQuotaRuleId := "destination-quota-id-123"

		// Mock getBasePathForQuotaRule (via ParseRegionAndZone and InternalUtilGetPairedRegionURI)
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Mock successful response with matching quota rule
		expectedResponse := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId: quotaRuleName,
					QuotaId:    googleproxyclient.NewOptString(expectedQuotaRuleId),
				},
			},
		}

		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedQuotaRuleId, *result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_GetBasePathError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "invalid-region"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "", "", errors.New("failed to parse region")
		}

		jwtToken := "test-jwt-token"
		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get destination base path")
	})

	t.Run("GetMatchingQuotaRuleOnDestination_EmptyJWTTokenError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Test with empty JWT token
		emptyJwtToken := ""
		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &emptyJwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "JWT token is required")
	})

	t.Run("GetMatchingQuotaRuleOnDestination_NilJWTTokenError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Test with nil JWT token
		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, nil)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "JWT token is required")
	})

	t.Run("GetMatchingQuotaRuleOnDestination_APIError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(nil, expectedError)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to list quota rules on destination")
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_NoMatchingQuotaRule_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "non-existent-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Response with different quota rule name
		response := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId: "different-quota-rule",
					QuotaId:    googleproxyclient.NewOptString("different-quota-id"),
				},
			},
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(response, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Quota rule")
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_QuotaRuleWithoutQuotaId_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Response with matching quota rule but no QuotaId
		response := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{
				{
					ResourceId: quotaRuleName,
					QuotaId:    googleproxyclient.OptString{}, // Empty QuotaId
				},
			},
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(response, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Quota rule")
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_BadRequest_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		badRequestResponse := &googleproxyclient.V1betaListAllQuotaRulesBadRequest{
			Message: "Invalid request",
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(badRequestResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_NotFound_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		notFoundResponse := &googleproxyclient.V1betaListAllQuotaRulesNotFound{
			Message: "Volume not found",
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(notFoundResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_EmptyQuotaRulesList_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Response with empty quota rules list
		response := &googleproxyclient.V1betaListAllQuotaRulesOK{
			QuotaRules: []googleproxyclient.QuotaRulesV1beta{},
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(response, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Quota rule")
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_Unauthorized_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		unauthorizedResponse := &googleproxyclient.V1betaListAllQuotaRulesUnauthorized{
			Message: "Unauthorized",
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(unauthorizedResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_Forbidden_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		forbiddenResponse := &googleproxyclient.V1betaListAllQuotaRulesForbidden{
			Message: "Forbidden",
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(forbiddenResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_Conflict_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		conflictResponse := &googleproxyclient.V1betaListAllQuotaRulesConflict{
			Message: "Conflict",
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(conflictResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_UnprocessableEntity_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		unprocessableEntityResponse := &googleproxyclient.V1betaListAllQuotaRulesUnprocessableEntity{
			Message: "Unprocessable Entity",
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(unprocessableEntityResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_TooManyRequests_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		tooManyRequestsResponse := &googleproxyclient.V1betaListAllQuotaRulesTooManyRequests{
			Message: "Too Many Requests",
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(tooManyRequestsResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_InternalServerError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		internalServerErrorResponse := &googleproxyclient.V1betaListAllQuotaRulesInternalServerError{
			Message: "Internal Server Error",
		}
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(internalServerErrorResponse, nil)

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetMatchingQuotaRuleOnDestination_UnexpectedResponseType_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationRegion := "us-east1"
		projectNumber := "123456789"
		quotaRuleName := "test-quota-rule"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Return an error to simulate unexpected response handling
		mockInvoker.On("V1betaListAllQuotaRules", ctx, mock.Anything).Return(nil, errors.New("unexpected response type error"))

		result, err := activity.GetMatchingQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationRegion, projectNumber, quotaRuleName, &jwtToken)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unexpected response type error")
		mockInvoker.AssertExpectations(t)
	})
}

// Test_UpdateQuotaRuleOnDestination tests the UpdateQuotaRuleOnDestination activity
// following the pattern from existing activity tests
func Test_UpdateQuotaRuleOnDestination(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("UpdateQuotaRuleOnDestination_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152) // 2GB in KiB
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		// Mock getBasePathForQuotaRule
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Mock successful response with READY state (synchronous completion)
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(destinationQuotaRuleId),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateREADY),
		}

		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsDone)
		assert.Empty(t, result.OperationName)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_UpdatingStateWithJobId_ReturnsJobIdForPolling", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Mock response with UPDATING state and Jobs array containing JobId
		jobID := "job-uuid-789"
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(destinationQuotaRuleId),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateUPDATING),
			Jobs: []googleproxyclient.JobV1beta{
				{
					JobId: googleproxyclient.NewOptString(jobID),
				},
			},
		}

		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.False(t, result.IsDone)
		assert.Equal(t, jobID, result.OperationName)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_UpdatingStateWithoutJobId_ReturnsSuccess", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Mock response with UPDATING state but no Jobs array (edge case - assume success)
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(destinationQuotaRuleId),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateUPDATING),
			Jobs:       []googleproxyclient.JobV1beta{}, // Empty Jobs array
		}

		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.IsDone) // Assume success when no JobId available
		assert.Empty(t, result.OperationName)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_GetBasePathError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "invalid-region"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "", "", errors.New("failed to parse region")
		}

		jwtToken := "test-jwt-token"
		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get destination base path")
	})

	t.Run("UpdateQuotaRuleOnDestination_EmptyJWTTokenError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Test with empty JWT token
		emptyJwtToken := ""
		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &emptyJwtToken)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "JWT token is required")
	})

	t.Run("UpdateQuotaRuleOnDestination_NilJWTTokenError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Test with nil JWT token
		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "JWT token is required")
	})

	t.Run("UpdateQuotaRuleOnDestination_APIError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(nil, expectedError)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update quota rule on destination")
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_BadRequest_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		badRequestResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPBadRequest{
			Message: "Invalid request",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(badRequestResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_NotFound_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		notFoundResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPNotFound{
			Message: "Quota rule not found",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(notFoundResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_Conflict_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		conflictResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPConflict{
			Message: "Quota rule is in transition state",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(conflictResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_OperationResponse_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Operation response (async operation)
		operationResponse := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString("/v1beta/projects/123456789/locations/us-east1/operations/op-123"),
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(operationResponse, nil)

		result, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.False(t, result.IsDone)
		assert.Equal(t, "/v1beta/projects/123456789/locations/us-east1/operations/op-123", result.OperationName)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_ZeroDiskLimit_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(0) // Zero disk limit (only description update)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(destinationQuotaRuleId),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateUPDATING),
		}

		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_UnexpectedResponseType_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		// Return an unexpected response type
		unexpectedResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPInternalServerError{
			Message: "Internal server error",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(unexpectedResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_Unauthorized_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		unauthorizedResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPUnauthorized{
			Message: "Unauthorized",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(unauthorizedResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_Forbidden_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		forbiddenResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPForbidden{
			Message: "Forbidden",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(forbiddenResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_MethodNotAllowed_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		methodNotAllowedResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPMethodNotAllowed{
			Message: "Method Not Allowed",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(methodNotAllowedResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_RequestTimeout_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		requestTimeoutResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPRequestTimeout{
			Message: "Request Timeout",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(requestTimeoutResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_UnprocessableEntity_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		unprocessableEntityResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPUnprocessableEntity{
			Message: "Unprocessable Entity",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(unprocessableEntityResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_TooManyRequests_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		tooManyRequestsResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPTooManyRequests{
			Message: "Too Many Requests",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(tooManyRequestsResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UpdateQuotaRuleOnDestination_ServiceUnavailable_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleUpdateActivity{SE: mockStorage}
		destinationVolumeUUID := "destination-volume-uuid"
		destinationQuotaRuleId := "destination-quota-id-123"
		diskLimitInKib := int64(2097152)
		destinationRegion := "us-east1"
		projectNumber := "123456789"

		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
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

		serviceUnavailableResponse := &googleproxyclient.V1betaUpdateQuotaRuleVCPServiceUnavailable{
			Message: "Service Unavailable",
		}
		mockInvoker.On("V1betaUpdateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(serviceUnavailableResponse, nil)

		_, err := activity.UpdateQuotaRuleOnDestination(ctx, destinationVolumeUUID, destinationQuotaRuleId, diskLimitInKib, destinationRegion, projectNumber, &jwtToken)

		assert.Error(t, err)
		mockInvoker.AssertExpectations(t)
	})
}

func TestMapQuotaRuleToHydrateObject(t *testing.T) {
	t.Run("WhenQuotaRuleHasUUID", func(tt *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{UUID: "quota-uuid-123"},
			Name:      "quota-rule-1",
		}

		result := mapQuotaRuleToHydrateObject(quotaRule)

		assert.Equal(tt, "quota-rule-1", result.ResourceId)
		assert.Equal(tt, "quota-uuid-123", result.QuotaRuleId)
	})

	t.Run("WhenQuotaRuleHasEmptyUUID", func(tt *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			Name: "quota-rule-2",
		}

		result := mapQuotaRuleToHydrateObject(quotaRule)

		assert.Equal(tt, "quota-rule-2", result.ResourceId)
		assert.Equal(tt, "", result.QuotaRuleId)
	})

	t.Run("WhenQuotaRuleHasNameOnly", func(tt *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			Name: "quota-rule-3",
		}

		result := mapQuotaRuleToHydrateObject(quotaRule)

		assert.Equal(tt, "quota-rule-3", result.ResourceId)
		assert.Equal(tt, "", result.QuotaRuleId)
	})
}

func TestConvertQuotaRulesVCPV1betaToDatamodel(t *testing.T) {
	t.Run("WhenInputIsNil", func(tt *testing.T) {
		result := convertQuotaRulesVCPV1betaToDatamodel(nil)
		assert.Nil(tt, result)
	})

	t.Run("WhenAllOptionalFieldsSet", func(tt *testing.T) {
		createdAt := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
		updatedAt := time.Date(2024, 1, 16, 11, 0, 0, 0, time.UTC)
		r := &googleproxyclient.QuotaRulesVCPV1beta{
			ResourceId:     "test-quota-rule",
			QuotaType:      googleproxyclient.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 100,
			QuotaId:        googleproxyclient.NewOptString("quota-uuid-123"),
			QuotaTarget:    googleproxyclient.NewOptString("user:1001"),
			State:          googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateREADY),
			StateDetails:   googleproxyclient.NewOptString("Ready state details"),
			Description:    googleproxyclient.NewOptString("Test description"),
			CreatedAt:      googleproxyclient.NewOptDateTime(createdAt),
			UpdatedAt:      googleproxyclient.NewOptDateTime(updatedAt),
		}

		result := convertQuotaRulesVCPV1betaToDatamodel(r)

		assert.NotNil(tt, result)
		assert.Equal(tt, "test-quota-rule", result.Name)
		assert.Equal(tt, int64(100*1024), result.DiskLimitInKib)
		assert.Equal(tt, "quota-uuid-123", result.UUID)
		assert.Equal(tt, "user:1001", result.QuotaTarget)
		assert.Equal(tt, "READY", result.State)
		assert.Equal(tt, "Ready state details", result.StateDetails)
		assert.Equal(tt, "Test description", result.Description)
		assert.Equal(tt, createdAt, result.CreatedAt)
		assert.Equal(tt, updatedAt, result.UpdatedAt)
	})
}

func TestHydrateQuotaRuleDelete(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := QuotaRuleCommonActivity{SE: mockStorage}
	quotaRuleId := "quota-rule-uuid-123"
	volumeId := "volume-uuid-456"
	region := "us-central1"
	projectId := "project-789"

	t.Run("WhenHydrateQuotaRuleDeleteSucceeds", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRuleDelete := common.HydrateQuotaRuleDelete
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			common.HydrateQuotaRuleDelete = originalHydrateQuotaRuleDelete
		}()

		callbackToken := "test-callback-token"
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return callbackToken, nil
		}

		common.HydrateQuotaRuleDelete = func(ctx context.Context, logger log.Logger, qrId string, volId string, reg string, projId string, token string) error {
			assert.Equal(tt, quotaRuleId, qrId)
			assert.Equal(tt, volumeId, volId)
			assert.Equal(tt, region, reg)
			assert.Equal(tt, projectId, projId)
			assert.Equal(tt, callbackToken, token)
			return nil
		}

		err := activity.HydrateQuotaRuleDelete(ctx, quotaRuleId, volumeId, region, projectId)

		assert.NoError(tt, err)
	})

	t.Run("WhenGenerateCallbackTokenFails", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		expectedError := errors.New("failed to generate callback token")
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", expectedError
		}

		err := activity.HydrateQuotaRuleDelete(ctx, quotaRuleId, volumeId, region, projectId)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to generate callback token")
	})

	t.Run("WhenHydrateQuotaRuleDeleteFails", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRuleDelete := common.HydrateQuotaRuleDelete
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			common.HydrateQuotaRuleDelete = originalHydrateQuotaRuleDelete
		}()

		callbackToken := "test-callback-token"
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return callbackToken, nil
		}

		expectedError := errors.New("failed to hydrate")
		common.HydrateQuotaRuleDelete = func(ctx context.Context, logger log.Logger, qrId string, volId string, reg string, projId string, token string) error {
			return expectedError
		}

		err := activity.HydrateQuotaRuleDelete(ctx, quotaRuleId, volumeId, region, projectId)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to hydrate quota rule delete")
	})
}

func TestHydrateQuotaRuleCreate(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := QuotaRuleCommonActivity{SE: mockStorage}
	// QuotaRuleId in hydrate object comes from quota rule UUID (BaseModel.UUID), not ExternalUUID
	quotaRule := &datamodel.QuotaRule{
		BaseModel: datamodel.BaseModel{UUID: "cp-quota-rule-uuid-123"},
		Name:      "quota-rule-1",
	}
	volumeId := "volume-uuid-456"
	region := "us-central1"
	projectId := "project-789"

	t.Run("WhenHydrateQuotaRuleCreateSucceeds", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRuleCreate := common.HydrateQuotaRuleCreate
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			common.HydrateQuotaRuleCreate = originalHydrateQuotaRuleCreate
		}()

		callbackToken := "test-callback-token"
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return callbackToken, nil
		}

		common.HydrateQuotaRuleCreate = func(ctx context.Context, logger log.Logger, qr models.QuotaRuleHydrateObject, volResourceID string, loc string, projId string, token string) error {
			assert.Equal(tt, "quota-rule-1", qr.ResourceId)
			assert.Equal(tt, "cp-quota-rule-uuid-123", qr.QuotaRuleId, "QuotaRuleId should be quota rule UUID")
			assert.Equal(tt, volumeId, volResourceID)
			assert.Equal(tt, region, loc)
			assert.Equal(tt, projectId, projId)
			assert.Equal(tt, callbackToken, token)
			return nil
		}

		err := activity.HydrateQuotaRuleCreate(ctx, quotaRule, volumeId, region, projectId)

		assert.NoError(tt, err)
	})

	t.Run("WhenGenerateCallbackTokenFails", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
		}()

		expectedError := errors.New("failed to generate callback token")
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", expectedError
		}

		err := activity.HydrateQuotaRuleCreate(ctx, quotaRule, volumeId, region, projectId)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to generate callback token")
	})

	t.Run("WhenHydrateQuotaRuleCreateFails", func(tt *testing.T) {
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		originalHydrateQuotaRuleCreate := common.HydrateQuotaRuleCreate
		defer func() {
			auth.GenerateCallbackToken = originalGenerateCallbackToken
			common.HydrateQuotaRuleCreate = originalHydrateQuotaRuleCreate
		}()

		callbackToken := "test-callback-token"
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return callbackToken, nil
		}

		expectedError := errors.New("failed to hydrate")
		common.HydrateQuotaRuleCreate = func(ctx context.Context, logger log.Logger, qr models.QuotaRuleHydrateObject, volResourceID string, loc string, projId string, token string) error {
			return expectedError
		}

		err := activity.HydrateQuotaRuleCreate(ctx, quotaRule, volumeId, region, projectId)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to hydrate quota rule create")
	})
}

func TestDeleteQuotaRuleOnOntap(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := QuotaRuleDeleteActivity{SE: mockStorage}
	externalQuotaUUID := "quota-uuid-123"
	node := &models.Node{
		Name: "test-node",
	}

	t.Run("WhenDeleteQuotaRuleOnOntapSucceeds", func(tt *testing.T) {
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
			Message: "Quota rule deleted successfully",
		}
		mockProvider.On("DeleteQuotaRule", ctx, externalQuotaUUID).Return(jobStatus, nil)

		result, err := activity.DeleteQuotaRuleOnOntap(ctx, externalQuotaUUID, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, vsa.JobRespSuccess, result.State)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		expectedError := errors.New("failed to create provider")
		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		_, err := activity.DeleteQuotaRuleOnOntap(ctx, externalQuotaUUID, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to create provider")
	})

	t.Run("WhenDeleteQuotaRuleFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		expectedError := errors.New("failed to delete quota rule")
		mockProvider.On("DeleteQuotaRule", ctx, externalQuotaUUID).Return(nil, expectedError)

		_, err := activity.DeleteQuotaRuleOnOntap(ctx, externalQuotaUUID, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to delete quota rule")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenDeleteQuotaRuleReturnsFailureStatus", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		jobStatus := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Quota rule deletion failed",
		}
		mockProvider.On("DeleteQuotaRule", ctx, externalQuotaUUID).Return(jobStatus, nil)

		result, err := activity.DeleteQuotaRuleOnOntap(ctx, externalQuotaUUID, node)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, vsa.JobRespFailure, result.State)
		assert.Equal(tt, "Quota rule deletion failed", result.Message)
		mockProvider.AssertExpectations(tt)
	})
}

func TestRevertQuotaRuleOnDestinationForDelete(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := QuotaRuleDeleteActivity{SE: mockStorage}
	destinationVolumeUUID := "destination-volume-uuid"
	quotaRule := &datamodel.QuotaRule{
		Name:           "test-quota-rule",
		QuotaType:      "INDIVIDUAL_USER_QUOTA",
		QuotaTarget:    "user:alice",
		DiskLimitInKib: 1048576,
		Description:    "Test description",
	}
	destinationRegion := "us-east1"
	projectNumber := "123456789"
	jwtToken := "test-jwt-token"

	t.Run("WhenRevertQuotaRuleOnDestinationForDeleteSucceeds", func(tt *testing.T) {
		// Mock replication.InternalUtilGetPairedRegionURI
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
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

		// Mock successful response with READY state (synchronous completion)
		quotaID := "quota-id-123"
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString(quotaID),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateREADY),
		}

		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.RevertQuotaRuleOnDestinationForDelete(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.NotNil(tt, result.QuotaRule)
		assert.Equal(tt, "quota-id-123", result.QuotaRule.UUID, "destination quota rule ID from API")
		assert.Equal(tt, "test-quota-rule", result.QuotaRule.Name)
		assert.NotNil(tt, result.OperationResult)
		assert.True(tt, result.OperationResult.IsDone)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenRevertQuotaRuleOnDestinationForDeleteSucceedsWithAsyncOperation", func(tt *testing.T) {
		// Mock replication.InternalUtilGetPairedRegionURI
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
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

		// Mock response with CREATING state and job ID (async operation)
		jobID := "operation-123"
		expectedResponse := &googleproxyclient.QuotaRulesVCPV1beta{
			QuotaId:    googleproxyclient.NewOptString("quota-id-456"),
			ResourceId: "test-quota-rule",
			State:      googleproxyclient.NewOptQuotaRulesVCPV1betaState(googleproxyclient.QuotaRulesVCPV1betaStateCREATING),
			Jobs: []googleproxyclient.JobV1beta{
				{
					JobId: googleproxyclient.NewOptString(jobID),
				},
			},
		}

		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(expectedResponse, nil)

		result, err := activity.RevertQuotaRuleOnDestinationForDelete(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.NotNil(tt, result.QuotaRule)
		assert.Equal(tt, "quota-id-456", result.QuotaRule.UUID, "destination quota rule ID from API")
		assert.Equal(tt, "test-quota-rule", result.QuotaRule.Name)
		assert.NotNil(tt, result.OperationResult)
		assert.False(tt, result.OperationResult.IsDone)
		assert.Equal(tt, jobID, result.OperationResult.OperationName)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenJWTTokenIsNil", func(tt *testing.T) {
		_, err := activity.RevertQuotaRuleOnDestinationForDelete(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, nil)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "JWT token is required")
	})

	t.Run("WhenJWTTokenIsEmpty", func(tt *testing.T) {
		emptyToken := ""
		_, err := activity.RevertQuotaRuleOnDestinationForDelete(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &emptyToken)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "JWT token is required")
	})

	t.Run("WhenCreateQuotaRuleOnDestinationFails", func(tt *testing.T) {
		// Mock replication.InternalUtilGetPairedRegionURI
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return basePath, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
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

		// Mock error response
		expectedError := errors.New("failed to create quota rule")
		mockInvoker.On("V1betaCreateQuotaRuleVCP", ctx, mock.Anything, mock.Anything).Return(nil, expectedError)

		_, err := activity.RevertQuotaRuleOnDestinationForDelete(ctx, destinationVolumeUUID, quotaRule, destinationRegion, projectNumber, &jwtToken)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to create quota rule")
		mockInvoker.AssertExpectations(tt)
	})
}

func TestDescribeQuotaRuleRemoteJob(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := QuotaRuleCommonActivity{SE: mockStorage}
	operationName := "operation-123"
	destinationRegion := "us-east1"
	projectNumber := "123456789"
	jwtToken := "test-jwt-token"

	t.Run("WhenOperationNameIsEmpty", func(tt *testing.T) {
		err := activity.DescribeQuotaRuleRemoteJob(ctx, "", destinationRegion, projectNumber, &jwtToken)

		assert.NoError(tt, err)
	})

	t.Run("WhenDescribeQuotaRuleRemoteJobSucceeds", func(tt *testing.T) {
		// Mock getBasePathForQuotaRule
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
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

		// Mock successful operation response (operation completed)
		expectedResponse := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString(operationName),
			Done: googleproxyclient.NewOptBool(true),
		}

		mockInvoker.On("V1betaDescribeOperation", ctx, mock.Anything).Return(expectedResponse, nil)

		err := activity.DescribeQuotaRuleRemoteJob(ctx, operationName, destinationRegion, projectNumber, &jwtToken)

		assert.NoError(tt, err)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenOperationIsStillInProgress", func(tt *testing.T) {
		// Mock getBasePathForQuotaRule
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
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

		// Mock operation still in progress (not done)
		expectedResponse := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString(operationName),
			Done: googleproxyclient.NewOptBool(false),
		}

		mockInvoker.On("V1betaDescribeOperation", ctx, mock.Anything).Return(expectedResponse, nil)

		err := activity.DescribeQuotaRuleRemoteJob(ctx, operationName, destinationRegion, projectNumber, &jwtToken)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Job not finished")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenOperationFails", func(tt *testing.T) {
		// Mock getBasePathForQuotaRule
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
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

		// Mock operation failed (done with error)
		errorMsg := "Operation failed"
		expectedResponse := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString(operationName),
			Done: googleproxyclient.NewOptBool(true),
			Error: googleproxyclient.NewOptStatusV1Beta(googleproxyclient.StatusV1Beta{
				Message: googleproxyclient.NewOptString(errorMsg),
			}),
		}

		mockInvoker.On("V1betaDescribeOperation", ctx, mock.Anything).Return(expectedResponse, nil)

		err := activity.DescribeQuotaRuleRemoteJob(ctx, operationName, destinationRegion, projectNumber, &jwtToken)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Internal job failed")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenJWTTokenIsNil", func(tt *testing.T) {
		// Mock getBasePathForQuotaRule
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		err := activity.DescribeQuotaRuleRemoteJob(ctx, operationName, destinationRegion, projectNumber, nil)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "JWT token is required")
	})

	t.Run("WhenJWTTokenIsEmpty", func(tt *testing.T) {
		// Mock getBasePathForQuotaRule
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		emptyToken := ""
		err := activity.DescribeQuotaRuleRemoteJob(ctx, operationName, destinationRegion, projectNumber, &emptyToken)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "JWT token is required")
	})

	t.Run("WhenGetBasePathFails", func(tt *testing.T) {
		originalParseRegionAndZone := utils.ParseRegionAndZone
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
		}()

		expectedError := errors.New("failed to parse region")
		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "", "", expectedError
		}

		err := activity.DescribeQuotaRuleRemoteJob(ctx, operationName, destinationRegion, projectNumber, &jwtToken)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get destination base path")
	})

	t.Run("WhenV1betaDescribeOperationFails", func(tt *testing.T) {
		// Mock getBasePathForQuotaRule
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
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

		// Mock error from V1betaDescribeOperation
		expectedError := errors.New("failed to describe operation")
		mockInvoker.On("V1betaDescribeOperation", ctx, mock.Anything).Return(nil, expectedError)

		err := activity.DescribeQuotaRuleRemoteJob(ctx, operationName, destinationRegion, projectNumber, &jwtToken)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to describe job")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("WhenUnexpectedResponseType", func(tt *testing.T) {
		// Mock getBasePathForQuotaRule
		originalParseRegionAndZone := utils.ParseRegionAndZone
		originalGetPairedRegionURI := replication.InternalUtilGetPairedRegionURI
		defer func() {
			utils.ParseRegionAndZone = originalParseRegionAndZone
			replication.InternalUtilGetPairedRegionURI = originalGetPairedRegionURI
		}()

		utils.ParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}
		basePath := "https://us-east1.example.com"
		replication.InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return basePath, nil
		}

		// Mock Google Proxy client
		mockInvoker := googleproxyclient.NewMockInvoker(tt)
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

		// Mock unexpected response type
		unexpectedResponse := &googleproxyclient.V1betaDescribeOperationBadRequest{
			Message: "Bad request",
		}

		mockInvoker.On("V1betaDescribeOperation", ctx, mock.Anything).Return(unexpectedResponse, nil)

		err := activity.DescribeQuotaRuleRemoteJob(ctx, operationName, destinationRegion, projectNumber, &jwtToken)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to describe job")
		mockInvoker.AssertExpectations(tt)
	})
}

// Test_ListQuotaRuleForVolume tests the ListQuotaRuleForVolume activity
func Test_ListQuotaRuleForVolume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("ListQuotaRuleForVolume_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeUUID := "volume-uuid-123"

		expectedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   123,
				UUID: volumeUUID,
			},
		}

		replication := &datamodel.VolumeReplication{
			Volume: expectedVolume,
		}

		expectedQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
				VolumeID:       123,
			},
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-2",
				},
				Name:           "quota-rule-2",
				QuotaType:      "DEFAULT_USER_QUOTA",
				QuotaTarget:    "",
				DiskLimitInKib: 2097152,
				VolumeID:       123,
			},
		}

		mockStorage.On("GetQuotaRulesByVolumeID", ctx, int64(123)).Return(expectedQuotaRules, nil)

		result, err := activity.ListQuotaRuleForVolume(ctx, replication)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 2, len(result))
		assert.Equal(t, "quota-rule-uuid-1", result[0].UUID)
		assert.Equal(t, "quota-rule-uuid-2", result[1].UUID)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ListQuotaRuleForVolume_EmptyList_Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeUUID := "volume-uuid-123"

		expectedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   123,
				UUID: volumeUUID,
			},
		}

		replication := &datamodel.VolumeReplication{
			Volume: expectedVolume,
		}

		mockStorage.On("GetQuotaRulesByVolumeID", ctx, int64(123)).Return([]*datamodel.QuotaRule{}, nil)

		result, err := activity.ListQuotaRuleForVolume(ctx, replication)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
		mockStorage.AssertExpectations(t)
	})

	t.Run("ListQuotaRuleForVolume_NilReplication_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}

		result, err := activity.ListQuotaRuleForVolume(ctx, nil)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ListQuotaRuleForVolume_GetQuotaRulesError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
		volumeUUID := "volume-uuid-123"

		expectedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   123,
				UUID: volumeUUID,
			},
		}

		replication := &datamodel.VolumeReplication{
			Volume: expectedVolume,
		}

		expectedError := errors.New("failed to get quota rules")

		mockStorage.On("GetQuotaRulesByVolumeID", ctx, int64(123)).Return(nil, expectedError)

		result, err := activity.ListQuotaRuleForVolume(ctx, replication)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})
}

// Test_HandleQuotaEnablementAndReinitialization tests the HandleQuotaEnablementAndReinitialization activity
func Test_HandleQuotaEnablementAndReinitialization(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOff_EnableSuccess", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
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

		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled successfully",
		}

		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)
		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOff_EnableFailure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
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

		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Failed to enable quota",
		}

		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)
		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to enable quota")
		assert.Contains(t, err.Error(), "please delete the quota rule and try again")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOff_EnableFailure_WithQuotaStatusFailed", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
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

		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Another quota operation is currently in progress for volume",
		}

		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)
		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Another quota operation is currently in progress for volume")
		assert.NotContains(t, err.Error(), "please delete the quota rule and try again")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOff_EnableFailure_WithSVMName", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
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

		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "Error in test-svm quota operation",
		}

		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)
		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Error in test-svm quota operation")
		assert.NotContains(t, err.Error(), "please delete the quota rule and try again")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOn_NoReinitializationNeeded", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
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

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOn_ResizeFailure_Reinitialization", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespFailure,
			Message: vsa.ResizeOperationFailed,
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

		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota disabled successfully",
		}
		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled successfully",
		}

		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOn_ActivationFailure_Reinitialization", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespFailure,
			Message: vsa.ActivationOperationFailed,
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

		disableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota disabled successfully",
		}
		enableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "Quota enabled successfully",
		}

		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(disableResp, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", true).Return(enableResp, nil)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOn_OtherFailure_Error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespFailure,
			Message: "Some other error occurred",
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

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Some other error occurred")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOn_NilResponse_NoReinitialization", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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

		quotaStatus := &vsa.QuotaStatus{
			Enabled: true,
			State:   vsa.QuotaStateOn,
		}

		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, nil)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_GetProviderByNodeError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		expectedError := errors.New("failed to create provider")
		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return nil, expectedError
		}

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create provider")
	})

	t.Run("HandleQuotaEnablementAndReinitialization_NoExternalUUID_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Volume has no ExternalUUID")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_NoSVMDetails_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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

		mockProvider := new(vsa.MockProvider)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Volume has no SVM details")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_GetQuotaStatusError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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

		expectedError := errors.New("failed to get quota status")
		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(nil, expectedError)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get quota status")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_QuotaOff_EnableError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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

		quotaStatus := &vsa.QuotaStatus{
			Enabled: false,
			State:   vsa.QuotaStateOff,
		}

		expectedError := errors.New("failed to enable quota")
		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)
		mockProvider.On("QuotaEnableDisable", ctx, "volume-external-uuid", "test-svm", true).Return(nil, expectedError)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to enable quota")
		mockProvider.AssertExpectations(t)
	})

	t.Run("HandleQuotaEnablementAndReinitialization_ReinitializationError_Failure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := QuotaRuleCommonActivity{SE: mockStorage}
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
		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespFailure,
			Message: vsa.ResizeOperationFailed,
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

		expectedError := errors.New("reinitialization failed")
		mockProvider.On("GetQuotaStatus", ctx, "volume-external-uuid").Return(quotaStatus, nil)
		mockProvider.On("QuotaEnableDisable", mock.Anything, "volume-external-uuid", "test-svm", false).Return(nil, expectedError)

		err := activity.HandleQuotaEnablementAndReinitialization(ctx, node, volumeDetails, quotaRuleResponse)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reinitialization failed")
		mockProvider.AssertExpectations(t)
	})
}

// Test_ParseQuotaRuleErrors tests the ParseQuotaRuleErrors activity
