package activities

import (
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	cvpModel "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
)

var (
	CvpCreateClient                = cvp.CreateClient
	convertToBackupPolicyDataModel = _convertToBackupPolicyDataModel
)

func _convertToBackupPolicyDataModel(backupPolicy *cvpModel.BackupPolicyDetailsV1beta) *datamodel.BackupPolicy {
	var createdTime strfmt.DateTime
	if backupPolicy.CreatedAt != nil {
		createdTime = *backupPolicy.CreatedAt
	}
	var resourceID string
	if backupPolicy.ResourceID != nil {
		resourceID = *backupPolicy.ResourceID
	}
	var policyEnabled bool
	if backupPolicy.Enabled != nil {
		policyEnabled = *backupPolicy.Enabled
	}
	var dailyLimit, monthlyLimit, weeklyLimit int64
	if backupPolicy.DailyBackupLimit != nil {
		dailyLimit = *backupPolicy.DailyBackupLimit
	}
	if backupPolicy.WeeklyBackupLimit != nil {
		weeklyLimit = *backupPolicy.WeeklyBackupLimit
	}
	if backupPolicy.MonthlyBackupLimit != nil {
		monthlyLimit = *backupPolicy.MonthlyBackupLimit
	}
	var lifeCycleStateDetails string
	if backupPolicy.State == models.LifeCycleStateREADY {
		lifeCycleStateDetails = models.LifeCycleStateAvailableDetails
	}
	return &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			UUID:      backupPolicy.BackupPolicyID,
			CreatedAt: time.Time(createdTime),
		},
		Name:                  resourceID,
		Description:           backupPolicy.Description,
		DailyBackupsToKeep:    dailyLimit,
		WeeklyBackupsToKeep:   weeklyLimit,
		MonthlyBackupsToKeep:  monthlyLimit,
		PolicyEnabled:         policyEnabled,
		LifeCycleState:        backupPolicy.State,
		LifeCycleStateDetails: lifeCycleStateDetails,
	}
}
