package adminbackgroundjobs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"os"
	"testing"
)

const trialSyncJobJSON = `{"TRIAL_ACCOUNT_SYNC": {"jobType": "TRIAL_ACCOUNT_SYNC", "cronExpression": "0 * * * *", "state": "CREATING"}}`

func resetTrialSyncJobSpecs(t *testing.T) {
	t.Helper()
	adminJobSpecs = make(map[string]*datamodel.AdminJobSpec)
}

func loadTrialSyncJobSpecs(t *testing.T) {
	t.Helper()
	data = []byte(trialSyncJobJSON)
	require.NoError(t, LoadJobSpecs())
}

func withTrialAccountSyncEnabled(t *testing.T) {
	t.Helper()
	require.NoError(t, os.Setenv("TRIAL_ACCOUNT_SYNC_ENABLED", "true"))
	t.Cleanup(func() { _ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_ENABLED") })
}

func TestLoadJobSpecs_TrialAccountSync_CronExpression(t *testing.T) {
	t.Run("WhenTRIAL_ACCOUNT_SYNC_CRON_EXPRESSION_IsSet", func(tt *testing.T) {
		withTrialAccountSyncEnabled(tt)
		resetTrialSyncJobSpecs(tt)
		require.NoError(tt, os.Setenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION", "0 */2 * * *"))
		tt.Cleanup(func() { _ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION") })

		loadTrialSyncJobSpecs(tt)

		spec, exists := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
		assert.True(tt, exists)
		assert.Equal(tt, "0 */2 * * *", spec.CronExpression)
	})

	t.Run("WhenTRIAL_ACCOUNT_SYNC_CRON_EXPRESSION_IsNotSet", func(tt *testing.T) {
		withTrialAccountSyncEnabled(tt)
		resetTrialSyncJobSpecs(tt)
		_ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION")

		loadTrialSyncJobSpecs(tt)

		spec, exists := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
		assert.True(tt, exists)
		assert.Equal(tt, "0 * * * *", spec.CronExpression)
	})

	t.Run("WhenTRIAL_ACCOUNT_SYNC_CRON_EXPRESSION_IsEmpty", func(tt *testing.T) {
		withTrialAccountSyncEnabled(tt)
		resetTrialSyncJobSpecs(tt)
		require.NoError(tt, os.Setenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION", ""))
		tt.Cleanup(func() { _ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION") })

		loadTrialSyncJobSpecs(tt)

		spec, exists := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
		assert.True(tt, exists)
		assert.Equal(tt, "0 * * * *", spec.CronExpression)
	})

	t.Run("WhenTRIAL_ACCOUNT_SYNC_CRON_EXPRESSION_IsWhitespaceOnly", func(tt *testing.T) {
		withTrialAccountSyncEnabled(tt)
		resetTrialSyncJobSpecs(tt)
		require.NoError(tt, os.Setenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION", "   "))
		tt.Cleanup(func() { _ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION") })

		loadTrialSyncJobSpecs(tt)

		spec, exists := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
		assert.True(tt, exists)
		assert.Equal(tt, "0 * * * *", spec.CronExpression)
	})
}

func TestLoadJobSpecs_TrialAccountSync_Enabled(t *testing.T) {
	t.Run("WhenTRIAL_ACCOUNT_SYNC_ENABLED_IsNotSet", func(tt *testing.T) {
		resetTrialSyncJobSpecs(tt)
		_ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_ENABLED")

		loadTrialSyncJobSpecs(tt)

		_, exists := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
		assert.False(tt, exists, "defaults to disabled when env unset")
	})

	t.Run("WhenTRIAL_ACCOUNT_SYNC_ENABLED_IsTrue", func(tt *testing.T) {
		resetTrialSyncJobSpecs(tt)
		require.NoError(tt, os.Setenv("TRIAL_ACCOUNT_SYNC_ENABLED", "true"))
		tt.Cleanup(func() { _ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_ENABLED") })

		loadTrialSyncJobSpecs(tt)

		_, exists := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
		assert.True(tt, exists)
	})

	t.Run("WhenTRIAL_ACCOUNT_SYNC_ENABLED_IsFalse", func(tt *testing.T) {
		resetTrialSyncJobSpecs(tt)
		require.NoError(tt, os.Setenv("TRIAL_ACCOUNT_SYNC_ENABLED", "false"))
		tt.Cleanup(func() { _ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_ENABLED") })

		loadTrialSyncJobSpecs(tt)

		_, exists := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
		assert.False(tt, exists)
	})

	t.Run("WhenTRIAL_ACCOUNT_SYNC_ENABLED_IsFalse_CronExpressionIsIgnored", func(tt *testing.T) {
		resetTrialSyncJobSpecs(tt)
		require.NoError(tt, os.Setenv("TRIAL_ACCOUNT_SYNC_ENABLED", "false"))
		require.NoError(tt, os.Setenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION", "0 */2 * * *"))
		tt.Cleanup(func() {
			_ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_ENABLED")
			_ = os.Unsetenv("TRIAL_ACCOUNT_SYNC_CRON_EXPRESSION")
		})

		loadTrialSyncJobSpecs(tt)

		_, exists := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
		assert.False(tt, exists)
	})
}

func TestLoadJobSpecs_TrialAccountSync(t *testing.T) {
	withTrialAccountSyncEnabled(t)
	resetTrialSyncJobSpecs(t)
	jsonBytes, err := os.ReadFile("admin_background_jobs.json")
	require.NoError(t, err)
	data = jsonBytes

	require.NoError(t, LoadJobSpecs())
	spec, ok := adminJobSpecs["TRIAL_ACCOUNT_SYNC"]
	require.True(t, ok)
	assert.Equal(t, "TRIAL_ACCOUNT_SYNC", spec.JobType)
	assert.Equal(t, "0 * * * *", spec.CronExpression)
}
