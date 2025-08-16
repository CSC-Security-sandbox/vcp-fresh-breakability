package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobTypeRotateKmsConfig(t *testing.T) {
	// Test that the new job type constant is defined correctly
	assert.Equal(t, JobType("ROTATE_KMS_CONFIG"), JobTypeRotateKmsConfig)
	assert.NotEmpty(t, string(JobTypeRotateKmsConfig))
}

func TestJobTypeConstants(t *testing.T) {
	// Test that all job types are unique strings
	jobTypes := []JobType{
		JobTypeCreatePool,
		JobTypeUpdatePool,
		JobTypeDeletePool,
		JobTypeCreateVolume,
		JobTypeUpdateVolume,
		JobTypeDeleteVolume,
		JobTypeCreateSnapshot,
		JobTypeDeleteSnapshot,
		JobTypeCreateBackup,
		JobTypeDeleteBackup,
		JobTypeCreateBackupVault,
		JobTypeDeleteBackupVault,
		JobTypeCreateKmsConfig,
		JobTypeUpdateKmsConfig,
		JobTypeDeleteKmsConfig,
		JobTypeMigrateKmsConfig,
		JobTypeRotateKmsConfig, // New job type
	}

	// Check uniqueness
	seen := make(map[string]bool)
	for _, jobType := range jobTypes {
		strJobType := string(jobType)
		assert.False(t, seen[strJobType], "Duplicate job type found: %s", strJobType)
		seen[strJobType] = true
	}

	// Verify the new job type is in the list
	found := false
	for _, jobType := range jobTypes {
		if jobType == JobTypeRotateKmsConfig {
			found = true
			break
		}
	}
	assert.True(t, found, "JobTypeRotateKmsConfig should be in the job types list")
}
