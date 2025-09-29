package common

import (
	"fmt"
	"regexp"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

const (
	LocalEnv                = "local"
	DaysInWeekForRetention  = 7
	DaysInMonthForRetention = 30
	ScheduleTagDaily        = "daily"
	ScheduleTagWeekly       = "weekly"
	ScheduleTagMonthly      = "monthly"
)

var (
	SnapmirrorSnapshotPrefix             = regexp.MustCompile("^snapmirror.*$")
	ImmutablePeriodInDaysMax             = env.GetInt("IMMUTABLE_PERIOD_IN_DAYS_MAX", 5475)
	ImmutablePeriodInDaysMaxDailyEnabled = env.GetInt("IMMUTABLE_PERIOD_IN_DAYS_MAX_DAILY_ENABLED", 1000)
	SleepFn                              = time.Sleep
	MaxRetries                           = 3
	RetryDelay                           = 5 * time.Second
)

func CreateJunctionPath(token string) string {
	junctionPath := "/" + token
	return junctionPath
}

func ValidateBackupPolicyRetentionLimits(backupPolicyParams *BackupPolicyParams, retentionPolicyParams *BackupRetentionPolicyParams) error {
	// Check if any backup type is immutable
	isDailyImmutable := retentionPolicyParams.IsDailyBackupImmutable != nil && *retentionPolicyParams.IsDailyBackupImmutable
	isWeeklyImmutable := retentionPolicyParams.IsWeeklyBackupImmutable != nil && *retentionPolicyParams.IsWeeklyBackupImmutable
	isMonthlyImmutable := retentionPolicyParams.IsMonthlyBackupImmutable != nil && *retentionPolicyParams.IsMonthlyBackupImmutable

	if isDailyImmutable || isWeeklyImmutable || isMonthlyImmutable {
		// Get the minimum retention period in days
		var immutablePeriodDays int64 = 0
		if retentionPolicyParams.BackupMinimumEnforcedRetentionDuration != nil {
			immutablePeriodDays = *retentionPolicyParams.BackupMinimumEnforcedRetentionDuration
		}

		// Validate daily backup retention
		if isDailyImmutable && backupPolicyParams.DailyBackupsToKeep >= 2 {
			if backupPolicyParams.DailyBackupsToKeep > int64(ImmutablePeriodInDaysMaxDailyEnabled) {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("daily backup limit (%d) exceeds maximum allowed (%d) for immutable backup policy", backupPolicyParams.DailyBackupsToKeep, ImmutablePeriodInDaysMaxDailyEnabled))
			}
			if backupPolicyParams.DailyBackupsToKeep < immutablePeriodDays {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("daily backup retention (%d days) is less than backup vault immutable period (%d days)", backupPolicyParams.DailyBackupsToKeep, immutablePeriodDays))
			}
		}

		// Validate weekly backup retention (weeks to days conversion)
		if isWeeklyImmutable && backupPolicyParams.WeeklyBackupsToKeep > 0 {
			weeklyRetentionDays := backupPolicyParams.WeeklyBackupsToKeep * DaysInWeekForRetention
			if weeklyRetentionDays < immutablePeriodDays {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("weekly backup retention (%d days) is less than backup vault immutable period (%d days)", weeklyRetentionDays, immutablePeriodDays))
			}
		}

		// Validate monthly backup retention (months to days conversion, using 30 days per month)
		if isMonthlyImmutable && backupPolicyParams.MonthlyBackupsToKeep > 0 {
			monthlyRetentionDays := backupPolicyParams.MonthlyBackupsToKeep * DaysInMonthForRetention
			if monthlyRetentionDays < immutablePeriodDays {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("monthly backup retention (%d days) is less than backup vault immutable period (%d days)", monthlyRetentionDays, immutablePeriodDays))
			}
		}
	}

	return nil
}

func CheckIfBackupIsImmutable(backup *datamodel.Backup) bool {
	// Check if backup vault or immutable attributes are nil
	if backup.BackupVault == nil || backup.BackupVault.ImmutableAttributes == nil {
		return false
	}

	switch backup.Type {
	case utils.BackupTypeSCHEDULED:
		// Check if schedule tag is nil
		if backup.ScheduleTag == nil {
			return true // Default to immutable when schedule tag is nil
		}

		if *backup.ScheduleTag == ScheduleTagDaily && !backup.BackupVault.ImmutableAttributes.IsDailyBackupImmutable {
			break
		} else if *backup.ScheduleTag == ScheduleTagWeekly && !backup.BackupVault.ImmutableAttributes.IsWeeklyBackupImmutable {
			break
		} else if *backup.ScheduleTag == ScheduleTagMonthly && !backup.BackupVault.ImmutableAttributes.IsMonthlyBackupImmutable {
			break
		}
		return true
	case utils.BackupTypeMANUAL:
		if backup.BackupVault.ImmutableAttributes.IsAdhocBackupImmutable {
			return true
		}
	}
	return false
}
