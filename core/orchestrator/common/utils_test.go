package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestValidateBackupPolicyRetentionLimits(t *testing.T) {
	// Helper function to create pointer values
	int64Ptr := func(v int64) *int64 { return &v }
	boolPtr := func(v bool) *bool { return &v }

	t.Run("Success Cases", func(t *testing.T) {
		t.Run("NoImmutableBackups_ShouldPass", func(t *testing.T) {
			// Test when no backup types are immutable
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(60),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("NilImmutableFlags_ShouldPass", func(t *testing.T) {
			// Test when immutable flags are nil (default to false)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(60),
				IsDailyBackupImmutable:                 nil,
				IsWeeklyBackupImmutable:                nil,
				IsMonthlyBackupImmutable:               nil,
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ValidDailyImmutableBackup_ShouldPass", func(t *testing.T) {
			// Test valid daily immutable backup with retention period within limits
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   100,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(50),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ValidDailyImmutableAtMaxLimit_ShouldPass", func(t *testing.T) {
			// Test daily backup at maximum limit for daily immutable (1000 days)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   1000,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(500),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ValidWeeklyImmutableBackup_ShouldPass", func(t *testing.T) {
			// Test valid weekly immutable backup (4 weeks * 7 days = 28 days >= 20 days retention)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(20),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ValidMonthlyImmutableBackup_ShouldPass", func(t *testing.T) {
			// Test valid monthly immutable backup (12 months * 30 days = 360 days >= 300 days retention)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(300),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ValidAllImmutableBackups_ShouldPass", func(t *testing.T) {
			// Test when all backup types are immutable with valid retention periods
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   100,
				WeeklyBackupsToKeep:  20, // 20 * 7 = 140 days
				MonthlyBackupsToKeep: 6,  // 6 * 30 = 180 days
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(90),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("NilRetentionDuration_ShouldPass", func(t *testing.T) {
			// Test when retention duration is nil (defaults to 0)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   100,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: nil,
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("DailyBackupsLessThan2_ShouldSkipValidation", func(t *testing.T) {
			// Test when daily backups are less than 2 (validation should be skipped)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   1,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ZeroWeeklyBackups_ShouldSkipValidation", func(t *testing.T) {
			// Test when weekly backups are 0 (validation should be skipped)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  0,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ZeroMonthlyBackups_ShouldSkipValidation", func(t *testing.T) {
			// Test when monthly backups are 0 (validation should be skipped)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 0,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})
	})

	t.Run("Failure Cases - Daily Backup Validation", func(t *testing.T) {
		t.Run("DailyBackupExceedsMaxLimit_ShouldFail", func(t *testing.T) {
			// Test when daily backup limit exceeds maximum allowed for immutable backup (1000 days)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   1001,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(500),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "daily backup limit (1001) exceeds maximum allowed (1000)")
		})

		t.Run("DailyBackupBelowRetentionPeriod_ShouldFail", func(t *testing.T) {
			// Test when daily backup retention is less than backup vault immutable period
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   50,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "daily backup retention (50 days) is less than backup vault immutable period (100 days)")
		})

		t.Run("DailyBackupWayAboveMaxLimit_ShouldFail", func(t *testing.T) {
			// Test edge case with extremely high daily backup limit
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   5000,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "daily backup limit (5000) exceeds maximum allowed (1000)")
		})

		t.Run("DailyBackupAtMinThreshold_ShouldFail", func(t *testing.T) {
			// Test daily backup exactly at minimum threshold but below retention period
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   2,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(10),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "daily backup retention (2 days) is less than backup vault immutable period (10 days)")
		})
	})

	t.Run("Failure Cases - Weekly Backup Validation", func(t *testing.T) {
		t.Run("WeeklyBackupBelowRetentionPeriod_ShouldFail", func(t *testing.T) {
			// Test when weekly backup retention is less than backup vault immutable period
			// 2 weeks * 7 days = 14 days < 20 days retention
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  2,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(20),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "weekly backup retention (14 days) is less than backup vault immutable period (20 days)")
		})

		t.Run("WeeklyBackupSingleWeek_ShouldFail", func(t *testing.T) {
			// Test single weekly backup vs higher retention period
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  1,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(14),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "weekly backup retention (7 days) is less than backup vault immutable period (14 days)")
		})

		t.Run("WeeklyBackupHighRetention_ShouldFail", func(t *testing.T) {
			// Test weekly backup with high retention requirement
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  10, // 10 * 7 = 70 days
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "weekly backup retention (70 days) is less than backup vault immutable period (100 days)")
		})
	})

	t.Run("Failure Cases - Monthly Backup Validation", func(t *testing.T) {
		t.Run("MonthlyBackupBelowRetentionPeriod_ShouldFail", func(t *testing.T) {
			// Test when monthly backup retention is less than backup vault immutable period
			// 6 months * 30 days = 180 days < 200 days retention
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 6,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(200),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "monthly backup retention (180 days) is less than backup vault immutable period (200 days)")
		})

		t.Run("MonthlyBackupSingleMonth_ShouldFail", func(t *testing.T) {
			// Test single monthly backup vs higher retention period
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 1,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(60),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "monthly backup retention (30 days) is less than backup vault immutable period (60 days)")
		})

		t.Run("MonthlyBackupExtremeRetention_ShouldFail", func(t *testing.T) {
			// Test monthly backup with extremely high retention requirement
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12, // 12 * 30 = 360 days
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(400),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "monthly backup retention (360 days) is less than backup vault immutable period (400 days)")
		})
	})

	t.Run("Complex Multi-Backup Type Failure Cases", func(t *testing.T) {
		t.Run("MultipleImmutableBackups_DailyFails_ShouldFail", func(t *testing.T) {
			// Test multiple immutable backup types where daily validation fails
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   1200, // Exceeds limit
				WeeklyBackupsToKeep:  20,   // Valid: 20 * 7 = 140 days
				MonthlyBackupsToKeep: 12,   // Valid: 12 * 30 = 360 days
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "daily backup limit (1200) exceeds maximum allowed (1000)")
		})

		t.Run("MultipleImmutableBackups_WeeklyFails_ShouldFail", func(t *testing.T) {
			// Test multiple immutable backup types where weekly validation fails
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   100, // Valid
				WeeklyBackupsToKeep:  5,   // 5 * 7 = 35 days < 50 days retention
				MonthlyBackupsToKeep: 12,  // Valid: 12 * 30 = 360 days
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(50),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "weekly backup retention (35 days) is less than backup vault immutable period (50 days)")
		})

		t.Run("MultipleImmutableBackups_MonthlyFails_ShouldFail", func(t *testing.T) {
			// Test multiple immutable backup types where monthly validation fails
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   100, // Valid
				WeeklyBackupsToKeep:  20,  // Valid: 20 * 7 = 140 days
				MonthlyBackupsToKeep: 3,   // 3 * 30 = 90 days < 100 days retention
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "monthly backup retention (90 days) is less than backup vault immutable period (100 days)")
		})
	})

	t.Run("Edge Cases and Boundary Conditions", func(t *testing.T) {
		t.Run("MaximumRetentionPeriod_ShouldPass", func(t *testing.T) {
			// Test with maximum retention period (5475 days as per constants)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   100, // Not daily immutable to avoid limit check
				WeeklyBackupsToKeep:  783, // 783 * 7 = 5481 days > 5475 days
				MonthlyBackupsToKeep: 183, // 183 * 30 = 5490 days > 5475 days
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(5475),
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ZeroRetentionPeriod_ShouldPass", func(t *testing.T) {
			// Test with zero retention period
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   100,
				WeeklyBackupsToKeep:  4,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(0),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("NegativeBackupCounts_ShouldPass", func(t *testing.T) {
			// Test with negative backup counts (should skip validation)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   -1,
				WeeklyBackupsToKeep:  -5,
				MonthlyBackupsToKeep: -10,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(100),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ExactBoundaryValues_ShouldPass", func(t *testing.T) {
			// Test exact boundary values where backup retention equals immutable period
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   50, // Exactly 50 days
				WeeklyBackupsToKeep:  7,  // 7 * 7 = 49 days (will pass since weekly > 0 but retention check will fail)
				MonthlyBackupsToKeep: 2,  // 2 * 30 = 60 days
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(50),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err) // Daily: 50 >= 50 (pass), Monthly: 60 >= 50 (pass)
		})

		t.Run("LargeRetentionPeriod_ShouldHandleGracefully", func(t *testing.T) {
			// Test with extremely large retention period
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   999,  // Within daily limit
				WeeklyBackupsToKeep:  1000, // Large weekly backup count
				MonthlyBackupsToKeep: 500,  // Large monthly backup count
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(999999),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err) // Should fail on daily backup retention check
			assert.Contains(t, err.Error(), "daily backup retention (999 days) is less than backup vault immutable period (999999 days)")
		})
	})

	t.Run("Customer Practical Scenarios", func(t *testing.T) {
		t.Run("TypicalEnterpriseDaily_ShouldPass", func(t *testing.T) {
			// Typical enterprise scenario: daily backups for 3 months with 1 month immutable period
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   90,
				WeeklyBackupsToKeep:  12,
				MonthlyBackupsToKeep: 12,
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(30),
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(false),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("ComplianceRequirement7Years_ShouldPass", func(t *testing.T) {
			// Compliance scenario: 7-year retention with monthly backups
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   30, // Not immutable
				WeeklyBackupsToKeep:  52, // Not immutable
				MonthlyBackupsToKeep: 84, // 84 * 30 = 2520 days (~7 years)
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(2555), // ~7 years in days
				IsDailyBackupImmutable:                 boolPtr(false),
				IsWeeklyBackupImmutable:                boolPtr(false),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err) // Should fail: 2520 < 2555
			assert.Contains(t, err.Error(), "monthly backup retention (2520 days) is less than backup vault immutable period (2555 days)")
		})

		t.Run("DisasterRecoveryScenario_ShouldFail", func(t *testing.T) {
			// Disaster recovery: mix of backup types with different immutable requirements
			// This should fail because daily backup retention (14 days) is less than immutable period (90 days)
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   14, // 2 weeks daily
				WeeklyBackupsToKeep:  26, // 6 months weekly (26 * 7 = 182 days)
				MonthlyBackupsToKeep: 24, // 2 years monthly (24 * 30 = 720 days)
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(90), // 3 months
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			assert.IsType(t, &customerrors.UserInputValidationErr{}, err)
			assert.Contains(t, err.Error(), "daily backup retention (14 days) is less than backup vault immutable period (90 days)")
		})

		t.Run("DisasterRecoveryScenario_ValidRetention_ShouldPass", func(t *testing.T) {
			// Disaster recovery scenario with valid retention periods for all backup types
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   100, // 100 days daily (> 90 days immutable period)
				WeeklyBackupsToKeep:  26,  // 6 months weekly (26 * 7 = 182 days)
				MonthlyBackupsToKeep: 24,  // 2 years monthly (24 * 30 = 720 days)
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(90), // 3 months
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.NoError(t, err)
		})

		t.Run("FinancialInstitution_ShouldFail", func(t *testing.T) {
			// Financial institution with strict requirements but insufficient retention
			backupPolicyParams := &BackupPolicyParams{
				DailyBackupsToKeep:   365, // 1 year daily
				WeeklyBackupsToKeep:  104, // 2 years weekly (104 * 7 = 728 days)
				MonthlyBackupsToKeep: 60,  // 5 years monthly (60 * 30 = 1800 days)
			}
			retentionPolicyParams := &BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: int64Ptr(2555), // 7 years requirement
				IsDailyBackupImmutable:                 boolPtr(true),
				IsWeeklyBackupImmutable:                boolPtr(true),
				IsMonthlyBackupImmutable:               boolPtr(true),
			}

			err := ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
			assert.Error(t, err)
			// Should fail on daily backup retention: 365 < 2555
			assert.Contains(t, err.Error(), "daily backup retention (365 days) is less than backup vault immutable period (2555 days)")
		})
	})
}

func TestCheckIfBackupIsImmutable(t *testing.T) {
	// Helper function to create string pointers
	stringPtr := func(s string) *string { return &s }

	t.Run("WhenBackupTypeIsScheduled", func(t *testing.T) {
		t.Run("DailyBackupImmutableEnabled", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: stringPtr(ScheduleTagDaily),
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsDailyBackupImmutable: true,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.True(t, result)
		})

		t.Run("DailyBackupImmutableDisabled", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: stringPtr(ScheduleTagDaily),
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsDailyBackupImmutable: false,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.False(t, result)
		})

		t.Run("WeeklyBackupImmutableEnabled", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: stringPtr(ScheduleTagWeekly),
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsWeeklyBackupImmutable: true,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.True(t, result)
		})

		t.Run("WeeklyBackupImmutableDisabled", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: stringPtr(ScheduleTagWeekly),
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsWeeklyBackupImmutable: false,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.False(t, result)
		})

		t.Run("MonthlyBackupImmutableEnabled", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: stringPtr(ScheduleTagMonthly),
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsMonthlyBackupImmutable: true,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.True(t, result)
		})

		t.Run("MonthlyBackupImmutableDisabled", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: stringPtr(ScheduleTagMonthly),
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsMonthlyBackupImmutable: false,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.False(t, result)
		})

		t.Run("UnknownScheduleTag", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: stringPtr("unknown"),
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsDailyBackupImmutable:   false,
						IsWeeklyBackupImmutable:  false,
						IsMonthlyBackupImmutable: false,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.True(t, result) // Should return true for unknown schedule tags
		})
	})

	t.Run("WhenBackupTypeIsManual", func(t *testing.T) {
		t.Run("AdhocBackupImmutableEnabled", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type: utils.BackupTypeMANUAL,
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsAdhocBackupImmutable: true,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.True(t, result)
		})

		t.Run("AdhocBackupImmutableDisabled", func(t *testing.T) {
			backup := &datamodel.Backup{
				Type: utils.BackupTypeMANUAL,
				BackupVault: &datamodel.BackupVault{
					ImmutableAttributes: &datamodel.ImmutableAttributes{
						IsAdhocBackupImmutable: false,
					},
				},
			}

			result := CheckIfBackupIsImmutable(backup)
			assert.False(t, result)
		})
	})

	t.Run("WhenBackupVaultIsNil", func(t *testing.T) {
		backup := &datamodel.Backup{
			Type: utils.BackupTypeMANUAL,
		}

		result := CheckIfBackupIsImmutable(backup)
		assert.False(t, result)
	})

	t.Run("WhenImmutableAttributesIsNil", func(t *testing.T) {
		backup := &datamodel.Backup{
			Type:        utils.BackupTypeMANUAL,
			BackupVault: &datamodel.BackupVault{},
		}

		result := CheckIfBackupIsImmutable(backup)
		assert.False(t, result)
	})

	t.Run("WhenBackupTypeIsUnknown", func(t *testing.T) {
		backup := &datamodel.Backup{
			Type: "UNKNOWN_TYPE",
			BackupVault: &datamodel.BackupVault{
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					IsAdhocBackupImmutable: true,
				},
			},
		}

		result := CheckIfBackupIsImmutable(backup)
		assert.False(t, result)
	})

	t.Run("WhenScheduleTagIsNil", func(t *testing.T) {
		backup := &datamodel.Backup{
			Type:        utils.BackupTypeSCHEDULED,
			ScheduleTag: nil,
			BackupVault: &datamodel.BackupVault{
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					IsDailyBackupImmutable: true,
				},
			},
		}

		result := CheckIfBackupIsImmutable(backup)
		assert.True(t, result) // Should return true when schedule tag is nil
	})
}
