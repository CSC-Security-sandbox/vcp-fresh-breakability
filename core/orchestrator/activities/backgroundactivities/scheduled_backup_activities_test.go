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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
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
			ScheduleTag:   &scheduleTag,
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
		assert.Nil(t, backup)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "db error")

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
		limit := 20
		offset := 0

		expectedVolumes := []*datamodel.Volume{
			{BaseModel: datamodel.BaseModel{UUID: "vol-1"}, DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID, ScheduledBackupEnabled: &policyEnabled}},
			{BaseModel: datamodel.BaseModel{UUID: "vol-2"}, DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID, ScheduledBackupEnabled: &policyEnabled}},
		}

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
			{"data_protection->>'scheduled_backup_enabled' = 'true'"},
		}
		mockStorage.On("ListVolumesWithPagination", ctx, conditions, mock.Anything).Return(expectedVolumes, nil).Once()

		volumes, err := activity.GetVolumesByBackupPolicyUUID(ctx, backupPolicyUUID, accountID, limit, offset)
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
		limit := 20
		offset := 0

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
			{"data_protection->>'scheduled_backup_enabled' = 'true'"},
		}

		mockStorage.On("ListVolumesWithPagination", ctx, conditions, mock.Anything).Return(nil, errors.New("db error")).Once()

		volumes, err := activity.GetVolumesByBackupPolicyUUID(ctx, backupPolicyUUID, accountID, limit, offset)
		assert.Nil(t, volumes)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "db error")

		mockStorage.AssertExpectations(t)
	})
	t.Run("GetVolumesByBackupPolicyUUIDWithDifferentPaginationParams", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := ScheduledBackupActivity{SE: mockStorage}

		ctx := context.Background()
		backupPolicyUUID := "policy-uuid"
		policyEnabled := true
		accountID := int64(42)
		limit := 50
		offset := 100

		expectedVolumes := []*datamodel.Volume{
			{BaseModel: datamodel.BaseModel{UUID: "vol-101"}, DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID, ScheduledBackupEnabled: &policyEnabled}},
		}

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
			{"data_protection->>'scheduled_backup_enabled' = 'true'"},
		}
		mockStorage.On("ListVolumesWithPagination", ctx, conditions, mock.Anything).Return(expectedVolumes, nil).Once()

		volumes, err := activity.GetVolumesByBackupPolicyUUID(ctx, backupPolicyUUID, accountID, limit, offset)
		assert.NoError(t, err)
		assert.Equal(t, expectedVolumes, volumes)
		assert.Len(t, volumes, 1)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetVolumesByBackupPolicyUUIDReturnsEmptyList", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := ScheduledBackupActivity{SE: mockStorage}

		ctx := context.Background()
		backupPolicyUUID := "policy-uuid"
		accountID := int64(42)
		limit := 20
		offset := 0

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
			{"data_protection->>'scheduled_backup_enabled' = 'true'"},
		}
		mockStorage.On("ListVolumesWithPagination", ctx, conditions, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

		volumes, err := activity.GetVolumesByBackupPolicyUUID(ctx, backupPolicyUUID, accountID, limit, offset)
		assert.NoError(t, err)
		assert.NotNil(t, volumes)
		assert.Len(t, volumes, 0)
		mockStorage.AssertExpectations(t)
	})
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
		assert.Equal(t, err, errors.New("db error"))
		mockStorage.AssertExpectations(t)
	})
}

func TestHydrateCreatedBackupsToCCFE(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	originalGenerateCallbackToken := auth.GenerateCallbackToken
	originalHydrateCreatedScheduledBackups := common.HydrateCreatedBackups
	originalHydrateDeletedScheduledBackups := common.HydrateDeletedBackups
	defer func() {
		auth.GenerateCallbackToken = originalGenerateCallbackToken
		common.HydrateCreatedBackups = originalHydrateCreatedScheduledBackups
		common.HydrateDeletedBackups = originalHydrateDeletedScheduledBackups
	}()

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
		Account: &datamodel.Account{
			Name: "account-1",
		},
	}
	backups := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"}, Name: "backup1", SizeInBytes: 1024000, BackupVault: &datamodel.BackupVault{Name: "backup-vault-1", RegionName: "us-east1", SourceRegionName: nillable.ToPointer("us-east1")}},
		{BaseModel: datamodel.BaseModel{UUID: "backup-uuid-2"}, Name: "backup2", SizeInBytes: 1083532, BackupVault: &datamodel.BackupVault{Name: "backup-vault-1", RegionName: "us-east1"}, Attributes: &datamodel.BackupAttributes{SourceVolumeZone: "us-east1-b"}},
	}

	t.Run("HydrateCreatedBackupsToCcfeSuccess", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		common.HydrateCreatedBackups = func(ctx context.Context, logger log.Logger, resources []models.Request, backupVaultName string, location string, projectId string, token string) error {
			return nil
		}
		utils.GetBackupRegion = func(*datamodel.Volume) (string, error) {
			return "mock-region", nil
		}
		utils.GetSourceVolumePathFromBackup = func(*datamodel.Backup) string {
			return "mock-source-volume-path"
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

	t.Run("WhenRegionCouldNotBeFetched", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		common.HydrateCreatedBackups = func(ctx context.Context, logger log.Logger, resources []models.Request, backupVaultName string, location string, projectId string, token string) error {
			return errors.New("could not hydrate backups to CCFE")
		}
		utils.GetBackupRegion = func(*datamodel.Volume) (string, error) {
			return "", errors.New("could not get backup region")
		}

		err := activity.HydrateCreatedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.Error(t, err)
		assert.Equal(t, "could not get backup region", err.Error())
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenHydrationToCcfeFails", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		common.HydrateCreatedBackups = func(ctx context.Context, logger log.Logger, resources []models.Request, backupVaultName string, location string, projectId string, token string) error {
			return errors.New("could not hydrate backups to CCFE")
		}
		utils.GetBackupRegion = func(*datamodel.Volume) (string, error) {
			return "mock-region", nil
		}
		utils.GetSourceVolumePathFromBackup = func(*datamodel.Backup) string {
			return "mock-source-volume-path"
		}

		err := activity.HydrateCreatedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.Error(t, err)
		assert.Equal(t, "could not hydrate backups to CCFE", err.Error())
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenHydrationIsDisabled", func(t *testing.T) {
		// Store original value
		originalHydrationEnabled := hydrationEnabled
		defer func() {
			hydrationEnabled = originalHydrationEnabled
		}()

		// Disable hydration
		hydrationEnabled = false

		// The method should return early without calling any external functions
		err := activity.HydrateCreatedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
}

func TestHydrateDeletedBackupsToCCFE(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	originalGenerateCallbackToken := auth.GenerateCallbackToken
	originalHydrateCreatedScheduledBackups := common.HydrateCreatedBackups
	originalHydrateDeletedScheduledBackups := common.HydrateDeletedBackups
	originalGetBackupRegion := utils.GetBackupRegion
	defer func() {
		auth.GenerateCallbackToken = originalGenerateCallbackToken
		common.HydrateCreatedBackups = originalHydrateCreatedScheduledBackups
		common.HydrateDeletedBackups = originalHydrateDeletedScheduledBackups
		utils.GetBackupRegion = originalGetBackupRegion
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
		common.HydrateDeletedBackups = func(ctx context.Context, logger log.Logger, names []string, backupVaultName string, location string, projectId string, token string) error {
			return nil
		}
		utils.GetBackupRegion = func(*datamodel.Volume) (string, error) {
			return "mock-region", nil
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

	t.Run("WhenRegionCouldNotBeFetched", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		utils.GetBackupRegion = func(*datamodel.Volume) (string, error) {
			return "", errors.New("could not get backup region")
		}

		err := activity.HydrateDeletedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.Error(t, err)
		assert.Equal(t, "could not get backup region", err.Error())
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenHydrationToCcfeFails", func(t *testing.T) {
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}
		common.HydrateDeletedBackups = func(ctx context.Context, logger log.Logger, names []string, backupVaultName string, location string, projectId string, token string) error {
			return errors.New("could not hydrate backups to CCFE")
		}
		utils.GetBackupRegion = func(*datamodel.Volume) (string, error) {
			return "mock-region", nil
		}

		err := activity.HydrateDeletedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.Error(t, err)
		assert.Equal(t, "could not hydrate backups to CCFE", err.Error())
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenHydrationIsDisabled", func(t *testing.T) {
		// Store original value
		originalHydrationEnabled := hydrationEnabled
		defer func() {
			hydrationEnabled = originalHydrationEnabled
		}()

		// Disable hydration
		hydrationEnabled = false

		// The method should return early without calling any external functions
		err := activity.HydrateDeletedBackupsToCCFE(ctx, volume, backups, "backup-vault-1")
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
}

func TestCreateBackupSnapshotInDB(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 123, UUID: "volume-uuid"},
		AccountID: 456,
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}
	snapshotName := "test-snapshot-name"

	t.Run("CreateBackupSnapshotInDBSuccess", func(t *testing.T) {
		expectedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{ID: 789, UUID: "snapshot-uuid"},
			Name:      snapshotName,
		}

		mockStorage.On("CreatingSnapshot", ctx, mock.Anything).Return(expectedSnapshot, nil).Once()

		snapshot, err := activity.CreateBackupSnapshotInDB(ctx, volume, snapshotName)
		assert.NoError(t, err)
		assert.Equal(t, expectedSnapshot, snapshot)

		// Verify the snapshot object passed to CreatingSnapshot
		callArgs := mockStorage.Calls[0].Arguments
		passedSnapshot := callArgs[1].(*datamodel.Snapshot)
		assert.Equal(t, snapshotName, passedSnapshot.Name)
		assert.Equal(t, activities.BackupComment, passedSnapshot.Description)
		assert.Equal(t, volume.ID, passedSnapshot.VolumeID)
		assert.Equal(t, volume.AccountID, passedSnapshot.AccountID)
		assert.Equal(t, volume, passedSnapshot.Volume)
		assert.Equal(t, volume.Account, passedSnapshot.Account)
		assert.False(t, passedSnapshot.IsAppConsistent)
		assert.Equal(t, SnapshotTypeBackup, passedSnapshot.Type)
		assert.NotNil(t, passedSnapshot.SnapshotAttributes)

		mockStorage.AssertExpectations(t)
	})

	t.Run("CreateBackupSnapshotInDBFails", func(t *testing.T) {
		mockStorage.On("CreatingSnapshot", ctx, mock.Anything).Return(nil, errors.New("database error")).Once()

		snapshot, err := activity.CreateBackupSnapshotInDB(ctx, volume, snapshotName)
		assert.Error(t, err)
		assert.Nil(t, snapshot)
		// CreateBackupSnapshotInDB wraps plain errors with WrapAsTemporalApplicationError,
		// but since it's not a CustomError, it returns the original error unchanged
		assert.Equal(t, "database error", err.Error())

		mockStorage.AssertExpectations(t)
	})
}

func TestUpdateBackupSnapshotInDB(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	dbSnapshot := &datamodel.Snapshot{
		BaseModel:          datamodel.BaseModel{ID: 789, UUID: "snapshot-uuid"},
		Name:               "test-snapshot",
		SnapshotAttributes: &datamodel.SnapshotAttributes{},
	}

	ontapSnapshot := &vsa.SnapshotProviderResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "ontap-external-uuid",
		},
		SizeInBytes:        1024000,
		LogicalSizeInBytes: 512000,
	}

	t.Run("UpdateBackupSnapshotInDBSuccess", func(t *testing.T) {
		updatedSnapshot := &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{ID: 789, UUID: "snapshot-uuid"},
			Name:         "test-snapshot",
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            1024000,
				ExternalUUID:           "ontap-external-uuid",
				LogicalSizeUsedInBytes: 512000,
			},
		}

		mockStorage.On("UpdateSnapshot", ctx, mock.Anything).Return(updatedSnapshot, nil).Once()

		result, err := activity.UpdateBackupSnapshotInDB(ctx, dbSnapshot, ontapSnapshot)
		assert.NoError(t, err)
		assert.Equal(t, updatedSnapshot, result)

		// Verify the snapshot object was updated correctly
		assert.Equal(t, models.LifeCycleStateREADY, dbSnapshot.State)
		assert.Equal(t, models.LifeCycleStateAvailableDetails, dbSnapshot.StateDetails)
		assert.Equal(t, ontapSnapshot.SizeInBytes, dbSnapshot.SnapshotAttributes.SizeInBytes)
		assert.Equal(t, ontapSnapshot.ExternalUUID, dbSnapshot.SnapshotAttributes.ExternalUUID)
		assert.Equal(t, ontapSnapshot.LogicalSizeInBytes, dbSnapshot.SnapshotAttributes.LogicalSizeUsedInBytes)

		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupSnapshotInDBFails", func(t *testing.T) {
		mockStorage.On("UpdateSnapshot", ctx, mock.Anything).Return(nil, errors.New("database error")).Once()

		result, err := activity.UpdateBackupSnapshotInDB(ctx, dbSnapshot, ontapSnapshot)
		assert.Error(t, err)
		assert.Nil(t, result)
		// UpdateBackupSnapshotInDB returns the raw error, not wrapped as ApplicationError
		assert.Equal(t, "database error", err.Error())

		mockStorage.AssertExpectations(t)
	})
}

func TestDeleteBackupSnapshotInDB(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	snapshotName := "test-snapshot-name"

	t.Run("DeleteBackupSnapshotInDBSuccess", func(t *testing.T) {
		mockStorage.On("DeleteSnapshot", ctx, snapshotName).Return(&datamodel.Snapshot{}, nil).Once()

		err := activity.DeleteBackupSnapshotInDB(ctx, snapshotName)
		assert.NoError(t, err)

		mockStorage.AssertExpectations(t)
	})

	t.Run("DeleteBackupSnapshotInDBFails", func(t *testing.T) {
		mockStorage.On("DeleteSnapshot", ctx, snapshotName).Return(nil, errors.New("database error")).Once()

		err := activity.DeleteBackupSnapshotInDB(ctx, snapshotName)
		assert.Error(t, err)
		// DeleteBackupSnapshotInDB wraps plain errors with WrapAsTemporalApplicationError,
		// but since it's not a CustomError, it returns the original error unchanged
		assert.Equal(t, "database error", err.Error())

		mockStorage.AssertExpectations(t)
	})
}

func TestUpdateBackupSize(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	t.Run("UpdateBackupSizeSuccessWithNonZeroSize", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			VolumeUUID:              "volume-uuid",
			LatestLogicalBackupSize: 1024000,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			DataProtection: &datamodel.DataProtection{},
		}

		// Mock UpdateBackup call
		mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil).Once()

		// Mock UpdateBackupLatestLogicalBackupSizeByVolume call (should be called when LatestLogicalBackupSize != 0)
		mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, "volume-uuid", "backup-uuid").Return(nil).Once()
		mockStorage.On("UpdateVolumeFields", ctx, "volume-uuid", mock.Anything).Return(nil).Once()

		err := activity.UpdateBackupSize(ctx, backup, volume)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupSizeSuccessWithZeroSize", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			VolumeUUID:              "volume-uuid",
			LatestLogicalBackupSize: 0,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			DataProtection: &datamodel.DataProtection{},
		}

		// Mock UpdateBackup call
		mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil).Once()

		// UpdateBackupLatestLogicalBackupSizeByVolume should NOT be called when LatestLogicalBackupSize == 0
		mockStorage.On("UpdateVolumeFields", ctx, "volume-uuid", mock.Anything).Return(nil).Once()

		err := activity.UpdateBackupSize(ctx, backup, volume)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupSizeFailsOnUpdateBackup", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			VolumeUUID:              "volume-uuid",
			LatestLogicalBackupSize: 1024000,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}

		// Mock UpdateBackup call to fail
		mockStorage.On("FinishBackup", ctx, backup).Return(nil, errors.New("database error")).Once()

		err := activity.UpdateBackupSize(ctx, backup, volume)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database error")
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupSizeFailsOnResetPreviousBackups", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			VolumeUUID:              "volume-uuid",
			LatestLogicalBackupSize: 1024000,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
		}

		// Mock UpdateBackup call to succeed
		mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil).Once()

		// Mock UpdateBackupLatestLogicalBackupSizeByVolume call to fail
		mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, "volume-uuid", "backup-uuid").Return(errors.New("reset error")).Once()

		err := activity.UpdateBackupSize(ctx, backup, volume)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reset error")
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupSizeFailsOnUpdateVolumeFields", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			VolumeUUID:              "volume-uuid",
			LatestLogicalBackupSize: 1024000,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			DataProtection: &datamodel.DataProtection{},
		}

		// Mock UpdateBackup call to succeed
		mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil).Once()

		// Mock UpdateBackupLatestLogicalBackupSizeByVolume call to succeed
		mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, "volume-uuid", "backup-uuid").Return(nil).Once()

		mockStorage.On("UpdateVolumeFields", ctx, "volume-uuid", mock.Anything).Return(errors.New("volume update error")).Once()

		err := activity.UpdateBackupSize(ctx, backup, volume)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "volume update error")
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupSizeFailsOnUpdateVolumeFieldsWithZeroSize", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			VolumeUUID:              "volume-uuid",
			LatestLogicalBackupSize: 0,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid",
			},
			DataProtection: &datamodel.DataProtection{},
		}

		// Mock UpdateBackup call to succeed
		mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil).Once()

		// UpdateBackupLatestLogicalBackupSizeByVolume should NOT be called when LatestLogicalBackupSize == 0
		mockStorage.On("UpdateVolumeFields", ctx, "volume-uuid", mock.Anything).Return(errors.New("volume update error")).Once()

		err := activity.UpdateBackupSize(ctx, backup, volume)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "volume update error")
		mockStorage.AssertExpectations(t)
	})
}

func TestGetSnapshotByNameAndVolumeID(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	snapshotName := "test-snapshot-name"
	accountID := int64(123)
	volumeID := int64(456)

	t.Run("GetSnapshotByNameAndVolumeIDSuccess", func(t *testing.T) {
		expectedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				ID:   789,
				UUID: "snapshot-uuid",
			},
			Name:      snapshotName,
			AccountID: accountID,
			VolumeID:  volumeID,
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					ID:   volumeID,
					UUID: "volume-uuid",
				},
				AccountID: accountID,
			},
		}

		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, snapshotName, accountID, volumeID).Return(expectedSnapshot, nil).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, snapshotName, accountID, volumeID)
		assert.NoError(t, err)
		assert.Equal(t, expectedSnapshot, snapshot)
		assert.Equal(t, snapshotName, snapshot.Name)
		assert.Equal(t, accountID, snapshot.AccountID)
		assert.Equal(t, volumeID, snapshot.VolumeID)

		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnapshotByNameAndVolumeIDDatabaseError", func(t *testing.T) {
		dbError := errors.New("database connection failed")
		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, snapshotName, accountID, volumeID).Return(nil, dbError).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, snapshotName, accountID, volumeID)
		assert.Error(t, err)
		assert.Nil(t, snapshot)
		assert.Contains(t, err.Error(), "database connection failed")

		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnapshotByNameAndVolumeIDNotFound", func(t *testing.T) {
		notFoundError := errors.New("snapshot not found")
		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, snapshotName, accountID, volumeID).Return(nil, notFoundError).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, snapshotName, accountID, volumeID)
		assert.Error(t, err)
		assert.Nil(t, snapshot)
		assert.Contains(t, err.Error(), "snapshot not found")

		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnapshotByNameAndVolumeIDEmptySnapshotName", func(t *testing.T) {
		emptySnapshotName := ""
		expectedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				ID:   789,
				UUID: "snapshot-uuid",
			},
			Name:      emptySnapshotName,
			AccountID: accountID,
			VolumeID:  volumeID,
		}

		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, emptySnapshotName, accountID, volumeID).Return(expectedSnapshot, nil).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, emptySnapshotName, accountID, volumeID)
		assert.NoError(t, err)
		assert.Equal(t, expectedSnapshot, snapshot)
		assert.Equal(t, emptySnapshotName, snapshot.Name)

		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnapshotByNameAndVolumeIDZeroIDs", func(t *testing.T) {
		zeroAccountID := int64(0)
		zeroVolumeID := int64(0)
		expectedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				ID:   789,
				UUID: "snapshot-uuid",
			},
			Name:      snapshotName,
			AccountID: zeroAccountID,
			VolumeID:  zeroVolumeID,
		}

		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, snapshotName, zeroAccountID, zeroVolumeID).Return(expectedSnapshot, nil).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, snapshotName, zeroAccountID, zeroVolumeID)
		assert.NoError(t, err)
		assert.Equal(t, expectedSnapshot, snapshot)
		assert.Equal(t, zeroAccountID, snapshot.AccountID)
		assert.Equal(t, zeroVolumeID, snapshot.VolumeID)

		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnapshotByNameAndVolumeIDNegativeIDs", func(t *testing.T) {
		negativeAccountID := int64(-1)
		negativeVolumeID := int64(-1)
		dbError := errors.New("invalid ID")
		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, snapshotName, negativeAccountID, negativeVolumeID).Return(nil, dbError).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, snapshotName, negativeAccountID, negativeVolumeID)
		assert.Error(t, err)
		assert.Nil(t, snapshot)
		assert.Contains(t, err.Error(), "invalid ID")

		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnapshotByNameAndVolumeIDLargeIDs", func(t *testing.T) {
		largeAccountID := int64(9223372036854775807) // Max int64
		largeVolumeID := int64(9223372036854775807)  // Max int64
		expectedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				ID:   789,
				UUID: "snapshot-uuid",
			},
			Name:      snapshotName,
			AccountID: largeAccountID,
			VolumeID:  largeVolumeID,
		}

		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, snapshotName, largeAccountID, largeVolumeID).Return(expectedSnapshot, nil).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, snapshotName, largeAccountID, largeVolumeID)
		assert.NoError(t, err)
		assert.Equal(t, expectedSnapshot, snapshot)
		assert.Equal(t, largeAccountID, snapshot.AccountID)
		assert.Equal(t, largeVolumeID, snapshot.VolumeID)

		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnapshotByNameAndVolumeIDSpecialCharactersInName", func(t *testing.T) {
		specialSnapshotName := "test-snapshot_@#$%^&*()_+-=[]{}|;':\",./<>?"
		expectedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				ID:   789,
				UUID: "snapshot-uuid",
			},
			Name:      specialSnapshotName,
			AccountID: accountID,
			VolumeID:  volumeID,
		}

		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, specialSnapshotName, accountID, volumeID).Return(expectedSnapshot, nil).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, specialSnapshotName, accountID, volumeID)
		assert.NoError(t, err)
		assert.Equal(t, expectedSnapshot, snapshot)
		assert.Equal(t, specialSnapshotName, snapshot.Name)

		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnapshotByNameAndVolumeIDLongSnapshotName", func(t *testing.T) {
		longSnapshotName := strings.Repeat("a", 1000) // 1000 character long name
		expectedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				ID:   789,
				UUID: "snapshot-uuid",
			},
			Name:      longSnapshotName,
			AccountID: accountID,
			VolumeID:  volumeID,
		}

		mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, longSnapshotName, accountID, volumeID).Return(expectedSnapshot, nil).Once()

		snapshot, err := activity.GetSnapshotByNameAndVolumeID(ctx, longSnapshotName, accountID, volumeID)
		assert.NoError(t, err)
		assert.Equal(t, expectedSnapshot, snapshot)
		assert.Equal(t, longSnapshotName, snapshot.Name)

		mockStorage.AssertExpectations(t)
	})
}

func TestUpdateBackupState(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := ScheduledBackupActivity{SE: mockStorage}

	t.Run("UpdateBackupStateSuccess", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "backup-uuid-1",
			},
			Name:  "test-backup",
			State: models.LifeCycleStateREADY,
		}

		expectedUpdatedBackup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "backup-uuid-1",
			},
			Name:  "test-backup",
			State: models.LifeCycleStateREADY,
		}

		mockStorage.On("UpdateBackupState", ctx, backup).Return(expectedUpdatedBackup, nil).Once()

		updatedBackup, err := activity.UpdateBackupState(ctx, backup)
		assert.NoError(t, err)
		assert.NotNil(t, updatedBackup)
		assert.Equal(t, expectedUpdatedBackup, updatedBackup)
		assert.Equal(t, models.LifeCycleStateREADY, updatedBackup.State)

		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupStateWithNilBackup", func(t *testing.T) {
		mockStorage.On("UpdateBackupState", ctx, (*datamodel.Backup)(nil)).Return(nil, errors.New("backup cannot be nil")).Once()

		updatedBackup, err := activity.UpdateBackupState(ctx, nil)
		assert.Error(t, err)
		assert.Nil(t, updatedBackup)
		assert.Equal(t, "backup cannot be nil", err.Error())

		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupStateDatabaseError", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "backup-uuid-1",
			},
			Name:  "test-backup",
			State: models.LifeCycleStateCreating,
		}

		mockStorage.On("UpdateBackupState", ctx, backup).Return(nil, errors.New("database connection failed")).Once()

		updatedBackup, err := activity.UpdateBackupState(ctx, backup)
		assert.Error(t, err)
		assert.Nil(t, updatedBackup)
		assert.Equal(t, "database connection failed", err.Error())

		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdateBackupStateWithDifferentStates", func(t *testing.T) {
		testCases := []struct {
			name          string
			initialState  string
			expectedState string
		}{
			{
				name:          "CreatingToReady",
				initialState:  models.LifeCycleStateCreating,
				expectedState: models.LifeCycleStateREADY,
			},
			{
				name:          "ReadyToError",
				initialState:  models.LifeCycleStateREADY,
				expectedState: models.LifeCycleStateError,
			},
			{
				name:          "CreatingToError",
				initialState:  models.LifeCycleStateCreating,
				expectedState: models.LifeCycleStateError,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				backup := &datamodel.Backup{
					BaseModel: datamodel.BaseModel{
						ID:   1,
						UUID: "backup-uuid-1",
					},
					Name:  "test-backup",
					State: tc.initialState,
				}

				expectedUpdatedBackup := &datamodel.Backup{
					BaseModel: datamodel.BaseModel{
						ID:   1,
						UUID: "backup-uuid-1",
					},
					Name:  "test-backup",
					State: tc.expectedState,
				}

				mockStorage.On("UpdateBackupState", ctx, backup).Return(expectedUpdatedBackup, nil).Once()

				updatedBackup, err := activity.UpdateBackupState(ctx, backup)
				assert.NoError(t, err)
				assert.NotNil(t, updatedBackup)
				assert.Equal(t, tc.expectedState, updatedBackup.State)

				mockStorage.AssertExpectations(t)
			})
		}
	})
}
