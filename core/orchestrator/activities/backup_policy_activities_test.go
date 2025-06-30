package activities

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
)

func TestConvertsValidBackupPolicyV1betaToDataModel(tt *testing.T) {
	tt.Run("ConvertsValidBackupPolicyV1betaToDataModel", func(t *testing.T) {
		createdAt := strfmt.DateTime(time.Now())
		name := "test-backup-policy"
		dailyLimit := int64(5)
		weeklyLimit := int64(3)
		monthlyLimit := int64(2)
		description := "This is a test backup policy"
		policyEnabled := true
		backupPolicyUUID := "uuid-123"
		volumeCount := int64(0)
		backupPolicy := &cvpModels.BackupPolicyDetailsV1beta{
			BackupPolicyV1beta: cvpModels.BackupPolicyV1beta{
				ResourceID:  &name,
				CreatedAt:   &createdAt,
				Description: &description,
				BackupPolicyScheduleV1beta: cvpModels.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyLimit,
					WeeklyBackupLimit:  &weeklyLimit,
					MonthlyBackupLimit: &monthlyLimit,
				},
				Enabled:        &policyEnabled,
				BackupPolicyID: backupPolicyUUID,
				State:          "READY",
				VolumeCount:    &volumeCount,
			},
		}
		expected := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID:      backupPolicyUUID,
				CreatedAt: time.Time(createdAt),
			},
			Name:                  name,
			Description:           &description,
			DailyBackupsToKeep:    dailyLimit,
			WeeklyBackupsToKeep:   weeklyLimit,
			MonthlyBackupsToKeep:  monthlyLimit,
			PolicyEnabled:         policyEnabled,
			LifeCycleState:        "READY",
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		res := convertToBackupPolicyDataModel(backupPolicy)
		assert.Equal(t, res, expected)
	})
	tt.Run("ConvertBackupPolicyWithNilFieldsToDataModel", func(t *testing.T) {
		backupPolicy := &cvpModels.BackupPolicyDetailsV1beta{}
		expected := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID:      "",
				CreatedAt: time.Time{},
				UpdatedAt: time.Time{},
				DeletedAt: nil,
			},
			Name:                  "",
			Description:           nil,
			DailyBackupsToKeep:    0,
			WeeklyBackupsToKeep:   0,
			MonthlyBackupsToKeep:  0,
			PolicyEnabled:         false,
			LifeCycleState:        "",
			LifeCycleStateDetails: "",
		}
		res := convertToBackupPolicyDataModel(backupPolicy)
		assert.Equal(t, res, expected)
	})
}
