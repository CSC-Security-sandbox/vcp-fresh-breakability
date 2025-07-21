package backgroundactivities

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestCreateScheduledBackup(t *testing.T) {
	t.Run("CreateScheduledBackupSuccess", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := ScheduledBackupActivity{SE: mockStorage}

		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid"}}
		backupVault := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{ID: 123}}
		timestamp := "20240610"
		scheduleTag := "test-schedule-tag"

		expectedBackup := &datamodel.Backup{
			Name:          mock.Anything,
			Type:          "SCHEDULED",
			ScheduleTag:   scheduleTag,
			VolumeUUID:    volume.UUID,
			BackupVaultID: backupVault.ID,
			BackupVault:   backupVault,
			State:         models.LifeCycleStateCreating,
			StateDetails:  models.LifeCycleStateCreatingDetails,
		}

		mockStorage.On("CreateBackup", mock.Anything, mock.Anything).Return(expectedBackup, nil)

		backup, err := activity.CreateScheduledBackup(context.Background(), volume, backupVault, timestamp, scheduleTag)
		assert.NoError(t, err)
		assert.NotNil(t, backup)
		assert.Equal(t, expectedBackup, backup)
		mockStorage.AssertExpectations(t)
	})
	t.Run("CreateScheduledBackupFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := ScheduledBackupActivity{SE: mockStorage}

		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid"}}
		backupVault := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{ID: 123}}
		timestamp := "20240610"
		scheduleTag := "test-schedule-tag"

		mockStorage.On("CreateBackup", mock.Anything, mock.Anything).Return(&datamodel.Backup{}, errors.New("db error"))

		backup, err := activity.CreateScheduledBackup(context.Background(), volume, backupVault, timestamp, scheduleTag)
		assert.Error(t, err)
		assert.Nil(t, backup)
		mockStorage.AssertExpectations(t)
	})
}

func TestGenerateScheduledSnapshotName(t *testing.T) {
	activity := &ScheduledBackupActivity{}

	t.Run("ReturnsNameWithCorrectFormat", func(t *testing.T) {
		timestamp := "20240610"
		ctx := context.Background()
		name, err := activity.GenerateScheduledSnapshotName(ctx, timestamp)
		parts := strings.Split(name, "-")

		assert.NoError(t, err)
		assert.True(t, strings.HasPrefix(name, "scheduled-snapshot-"))
		assert.True(t, strings.HasSuffix(name, "-"+timestamp))

		assert.Equal(t, 4, len(parts))
		assert.Equal(t, "scheduled", parts[0])
		assert.Equal(t, "snapshot", parts[1])
		assert.Equal(t, timestamp, parts[3])
	})

	t.Run("ReturnsDifferentNames", func(t *testing.T) {
		timestamp := "20240610"
		ctx := context.Background()
		name1, _ := activity.GenerateScheduledSnapshotName(ctx, timestamp)
		name2, _ := activity.GenerateScheduledSnapshotName(ctx, timestamp)
		assert.NotEqual(t, name1, name2)
	})
}

func TestGetVolumesByBackupPolicyUUID(t *testing.T) {
	t.Run("GetVolumesByBackupPolicyUUIDSuccess", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := ScheduledBackupActivity{SE: mockStorage}

		ctx := context.Background()
		backupPolicyUUID := "policy-uuid"
		policyEnabled := true
		accountID := int64(42)

		expectedVolumes := []*datamodel.Volume{
			{BaseModel: datamodel.BaseModel{UUID: "vol-1"}, DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID, ScheduledBackupEnabled: &policyEnabled}},
			{BaseModel: datamodel.BaseModel{UUID: "vol-2"}, DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID, ScheduledBackupEnabled: &policyEnabled}},
		}

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
			{"data_protection->>'scheduled_backup_enabled' = true"},
		}
		mockStorage.On("ListVolumes", ctx, conditions).Return(expectedVolumes, nil).Once()

		volumes, err := activity.GetVolumesByBackupPolicyUUID(ctx, backupPolicyUUID, accountID)
		assert.NoError(t, err)
		assert.Equal(t, expectedVolumes, volumes)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetVolumesByBackupPolicyUUIDFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := ScheduledBackupActivity{SE: mockStorage}

		ctx := context.Background()
		backupPolicyUUID := "policy-uuid"
		accountID := int64(42)

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
			{"data_protection->>'scheduled_backup_enabled' = true"},
		}

		mockStorage.On("ListVolumes", ctx, conditions).Return(nil, errors.New("db error")).Once()

		volumes, err := activity.GetVolumesByBackupPolicyUUID(ctx, backupPolicyUUID, accountID)
		assert.Error(t, err)
		assert.Nil(t, volumes)
		mockStorage.AssertExpectations(t)
	})
}

func TestConvertToGCPHydrateCreateRequests(t *testing.T) {
	backups := []*datamodel.Backup{
		{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "uuid1",
			},
			Name:        "backup1",
			SizeInBytes: 12345,
		},
		{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "uuid2",
			},
			Name:        "backup2",
			SizeInBytes: 67890,
		},
	}
	result := convertToGCPHydrateCreateRequests(backups)
	require := assert.New(t)
	require.Len(result, 2)
	require.Equal("backup1", result[0].Backup.ResourceId)
	require.Equal("uuid1", result[0].Backup.BackupId)
	require.NotNil(result[0].Backup.VolumeUsageBytes)
	require.Equal(uint64(12345), *result[0].Backup.VolumeUsageBytes)

	require.Equal("backup2", result[1].Backup.ResourceId)
	require.Equal("uuid2", result[1].Backup.BackupId)
	require.NotNil(result[1].Backup.VolumeUsageBytes)
	require.Equal(uint64(67890), *result[1].Backup.VolumeUsageBytes)

	result = convertToGCPHydrateCreateRequests([]*datamodel.Backup{})
	require.Empty(result)

	result = convertToGCPHydrateCreateRequests(nil)
	require.Empty(result)
}

func TestConvertToGCPHydrateDeleteRequests(t *testing.T) {
	backups := []*datamodel.Backup{
		{Name: "backup1"},
		{Name: "backup2"},
		{Name: "backup3"},
	}
	expected := []string{"backup1", "backup2", "backup3"}
	result := convertToGCPHydrateDeleteRequests(backups)
	assert.Equal(t, expected, result)

	// Test with empty slice
	result = convertToGCPHydrateDeleteRequests([]*datamodel.Backup{})
	assert.Empty(t, result)

	// Test with nil input
	result = convertToGCPHydrateDeleteRequests(nil)
	assert.Empty(t, result)
}

func TestRandomString(t *testing.T) {
	t.Run("ReturnsStringOfCorrectLength", func(t *testing.T) {
		for _, n := range []int{0, 1, 5, 10, 20} {
			s := RandomString(n)
			if len(s) != n {
				t.Errorf("expected length %d, got %d", n, len(s))
			}
		}
	})

	t.Run("ReturnsOnlyLetters", func(t *testing.T) {
		s := RandomString(100)
		for _, c := range s {
			if c < 'a' || c > 'z' {
				t.Errorf("unexpected character: %c", c)
			}
		}
	})

	t.Run("ReturnsDifferentStrings", func(t *testing.T) {
		s1 := RandomString(10)
		s2 := RandomString(10)
		if s1 == s2 {
			t.Errorf("expected different strings, got %s and %s", s1, s2)
		}
	})
}

func TestFetchScheduledBackupForDeletion(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	ctx := context.Background()
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid"}}
	backupPolicy := &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "policy-uuid"}}

	expectedBackups := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "backup-1"}},
		{BaseModel: datamodel.BaseModel{UUID: "backup-2"}},
	}

	t.Run("FetchScheduledBackupForDeletionSuccess", func(t *testing.T) {
		mockStorage.On("FetchScheduledBackupsForDeletion", ctx, volume, backupPolicy).Return(expectedBackups, nil).Once()
		backups, err := activity.FetchScheduledBackupForDeletion(ctx, volume, backupPolicy)
		assert.NoError(t, err)
		assert.Equal(t, expectedBackups, backups)
		mockStorage.AssertExpectations(t)
	})

	t.Run("FetchScheduledBackupForDeletionFails", func(t *testing.T) {
		mockStorage.On("FetchScheduledBackupsForDeletion", ctx, volume, backupPolicy).Return(nil, errors.New("db error")).Once()
		backups, err := activity.FetchScheduledBackupForDeletion(ctx, volume, backupPolicy)
		assert.Error(t, err)
		assert.Nil(t, backups)
		mockStorage.AssertExpectations(t)
	})
}

func TestHydrateCreatedBackupsToCCFE(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	originalGenerateCallbackToken := auth.GenerateCallbackToken
	originalHydrateCreatedScheduledBackups := common.HydrateCreatedScheduledBackups
	originalHydrateDeletedScheduledBackups := common.HydrateDeletedScheduledBackups
	defer func() {
		auth.GenerateCallbackToken = originalGenerateCallbackToken
		common.HydrateCreatedScheduledBackups = originalHydrateCreatedScheduledBackups
		common.HydrateDeletedScheduledBackups = originalHydrateDeletedScheduledBackups
	}()

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
		Account: &datamodel.Account{
			Name: "account-1",
		},
	}
	backups := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"}, Name: "backup1", SizeInBytes: 1024000, BackupVault: &datamodel.BackupVault{Name: "backup-vault-1", RegionName: "us-east1"}},
		{BaseModel: datamodel.BaseModel{UUID: "backup-uuid-2"}, Name: "backup2", SizeInBytes: 1083532, BackupVault: &datamodel.BackupVault{Name: "backup-vault-1", RegionName: "us-east1"}},
	}

	t.Run("HydrateCreatedBackupsToCcfeSuccess", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		common.HydrateCreatedScheduledBackups = func(ctx context.Context, logger log.Logger, resources []models.Request, backupVaultName string, location string, projectId string, token string) error {
			return nil
		}

		err := activity.HydrateCreatedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenNoBackupsAreToBeHydrated", func(t *testing.T) {
		err := activity.HydrateCreatedBackupsToCCFE(ctx, volume, nil, "backup-vault-1")
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)

		err = activity.HydrateCreatedBackupsToCCFE(ctx, volume, []*datamodel.Backup{}, "backup-vault-1")
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenGenerationOfCallbackTokenFails", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("could not generate CCFE auth token")
		}

		err := activity.HydrateCreatedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.Error(t, err)
		assert.Equal(t, "could not generate CCFE auth token", err.Error())
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenHydrationToCcfeFails", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		common.HydrateCreatedScheduledBackups = func(ctx context.Context, logger log.Logger, resources []models.Request, backupVaultName string, location string, projectId string, token string) error {
			return errors.New("could not hydrate backups to CCFE")
		}

		err := activity.HydrateCreatedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.Error(t, err)
		assert.Equal(t, "could not hydrate backups to CCFE", err.Error())
		mockStorage.AssertExpectations(t)
	})
}

func TestHydrateDeletedBackupsToCCFE(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	originalGenerateCallbackToken := auth.GenerateCallbackToken
	originalHydrateCreatedScheduledBackups := common.HydrateCreatedScheduledBackups
	originalHydrateDeletedScheduledBackups := common.HydrateDeletedScheduledBackups
	defer func() {
		auth.GenerateCallbackToken = originalGenerateCallbackToken
		common.HydrateCreatedScheduledBackups = originalHydrateCreatedScheduledBackups
		common.HydrateDeletedScheduledBackups = originalHydrateDeletedScheduledBackups
	}()

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
		Account: &datamodel.Account{
			Name: "account-1",
		},
	}
	backups := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"}, Name: "backup1", SizeInBytes: 1024000, BackupVault: &datamodel.BackupVault{Name: "backup-vault-1", RegionName: "us-east1"}},
		{BaseModel: datamodel.BaseModel{UUID: "backup-uuid-2"}, Name: "backup2", SizeInBytes: 1083532, BackupVault: &datamodel.BackupVault{Name: "backup-vault-1", RegionName: "us-east1"}},
	}

	t.Run("WhenNoBackupsAreToBeHydrated", func(t *testing.T) {
		err := activity.HydrateDeletedBackupsToCCFE(ctx, volume, nil, "backup-vault-1")
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)

		err = activity.HydrateDeletedBackupsToCCFE(ctx, volume, []*datamodel.Backup{}, "backup-vault-1")
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("HydrateDeletedBackupsToCcfeSuccess", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		common.HydrateDeletedScheduledBackups = func(ctx context.Context, logger log.Logger, names []string, backupVaultName string, location string, projectId string, token string) error {
			return nil
		}

		err := activity.HydrateDeletedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenGenerationOfCallbackTokenFails", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("could not generate CCFE auth token")
		}

		err := activity.HydrateDeletedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.Error(t, err)
		assert.Equal(t, "could not generate CCFE auth token", err.Error())
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenHydrationToCcfeFails", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		common.HydrateDeletedScheduledBackups = func(ctx context.Context, logger log.Logger, names []string, backupVaultName string, location string, projectId string, token string) error {
			return errors.New("could not hydrate backups to CCFE")
		}

		err := activity.HydrateDeletedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.Error(t, err)
		assert.Equal(t, "could not hydrate backups to CCFE", err.Error())
		mockStorage.AssertExpectations(t)
	})
}
