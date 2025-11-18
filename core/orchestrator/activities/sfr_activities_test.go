package activities_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestPopulateSfrMetadataActivity(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/file1.txt": {
				Inode: "12345",
				Size:  1024,
			},
			"/file2.txt": {
				Inode: "67890",
				Size:  2048,
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}

		jobID := int64(100)

		expectedSfrMetadata := &datamodel.SfrMetadata{
			FilesSize:  3072, // 1024 + 2048
			FileCount:  2,
			VolumeName: "test-volume",
			VolumeUUID: "volume-uuid",
			BackupUUID: "backup-uuid",
			AccountID:  sql.NullInt64{Int64: 1, Valid: true},
			JobID:      sql.NullInt64{Int64: 100, Valid: true},
		}

		mockSE.On("CreateSfrMetadata", ctx, mock.MatchedBy(func(metadata *datamodel.SfrMetadata) bool {
			return metadata.FilesSize == expectedSfrMetadata.FilesSize &&
				metadata.FileCount == expectedSfrMetadata.FileCount &&
				metadata.VolumeName == expectedSfrMetadata.VolumeName &&
				metadata.VolumeUUID == expectedSfrMetadata.VolumeUUID &&
				metadata.BackupUUID == expectedSfrMetadata.BackupUUID &&
				metadata.AccountID.Valid == expectedSfrMetadata.AccountID.Valid &&
				metadata.AccountID.Int64 == expectedSfrMetadata.AccountID.Int64 &&
				metadata.JobID.Valid == expectedSfrMetadata.JobID.Valid &&
				metadata.JobID.Int64 == expectedSfrMetadata.JobID.Int64
		})).Return(expectedSfrMetadata, nil)

		err := activity.PopulateSfrMetadataActivity(ctx, fileInodeSizeMap, volume, backup, &jobID)
		assert.NoError(t, err)
		mockSE.AssertExpectations(t)
	})

	t.Run("SuccessWithNilFileInfo", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/file1.txt": {
				Inode: "12345",
				Size:  1024,
			},
			"/file2.txt": nil, // nil file info
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}

		jobID := int64(100)

		mockSE.On("CreateSfrMetadata", ctx, mock.MatchedBy(func(metadata *datamodel.SfrMetadata) bool {
			return metadata.FilesSize == 1024 && // Only file1.txt size
				metadata.FileCount == 2 // Count includes nil entry
		})).Return(&datamodel.SfrMetadata{}, nil)

		err := activity.PopulateSfrMetadataActivity(ctx, fileInodeSizeMap, volume, backup, &jobID)
		assert.NoError(t, err)
		mockSE.AssertExpectations(t)
	})

	t.Run("SuccessWithZeroAccountID", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/file1.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 0, // Zero account ID
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}

		jobID := int64(100)

		mockSE.On("CreateSfrMetadata", ctx, mock.MatchedBy(func(metadata *datamodel.SfrMetadata) bool {
			return !metadata.AccountID.Valid // Should be invalid for zero account ID
		})).Return(&datamodel.SfrMetadata{}, nil)

		err := activity.PopulateSfrMetadataActivity(ctx, fileInodeSizeMap, volume, backup, &jobID)
		assert.NoError(t, err)
		mockSE.AssertExpectations(t)
	})

	t.Run("SuccessWithNilJobID", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/file1.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}

		mockSE.On("CreateSfrMetadata", ctx, mock.MatchedBy(func(metadata *datamodel.SfrMetadata) bool {
			return !metadata.JobID.Valid // Should be invalid for nil job ID
		})).Return(&datamodel.SfrMetadata{}, nil)

		err := activity.PopulateSfrMetadataActivity(ctx, fileInodeSizeMap, volume, backup, nil)
		assert.NoError(t, err)
		mockSE.AssertExpectations(t)
	})

	t.Run("SuccessWithZeroJobID", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/file1.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}

		jobID := int64(0)

		mockSE.On("CreateSfrMetadata", ctx, mock.MatchedBy(func(metadata *datamodel.SfrMetadata) bool {
			return !metadata.JobID.Valid // Should be invalid for zero job ID
		})).Return(&datamodel.SfrMetadata{}, nil)

		err := activity.PopulateSfrMetadataActivity(ctx, fileInodeSizeMap, volume, backup, &jobID)
		assert.NoError(t, err)
		mockSE.AssertExpectations(t)
	})

	t.Run("EmptyFileMap", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}

		jobID := int64(100)

		// Should not call CreateSfrMetadata for empty map
		err := activity.PopulateSfrMetadataActivity(ctx, fileInodeSizeMap, volume, backup, &jobID)
		assert.NoError(t, err)
		mockSE.AssertExpectations(t)
	})

	t.Run("NilFileMap", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}

		jobID := int64(100)

		// Should not call CreateSfrMetadata for nil map
		err := activity.PopulateSfrMetadataActivity(ctx, nil, volume, backup, &jobID)
		assert.NoError(t, err)
		mockSE.AssertExpectations(t)
	})

	t.Run("CreateSfrMetadataError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/file1.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}

		jobID := int64(100)

		mockSE.On("CreateSfrMetadata", ctx, mock.Anything).Return(nil, errors.New("database error"))

		err := activity.PopulateSfrMetadataActivity(ctx, fileInodeSizeMap, volume, backup, &jobID)
		assert.Error(t, err)
		mockSE.AssertExpectations(t)
	})
}

func TestSnapmirrorTransferWithFiles(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{
			{
				SourcePath:      "12345",
				DestinationPath: "/restore/file1.txt",
			},
		}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		defer func() {
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		smcLicense := "test-license"
		token := "test-token"
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return smcLicense, nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			return &token, nil
		}

		mockProvider.On("SnapmirrorRelationshipTransferCreateWithFiles", snapmirrorUUID, snapshotName, &token, files).Return(nil)

		err := activity.SnapmirrorTransferWithFiles(ctx, node, snapmirrorUUID, snapshotName, files)
		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetProviderByNodeError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		err := activity.SnapmirrorTransferWithFiles(ctx, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
	})

	t.Run("GetSmcLicenseFromCloudError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		defer func() {
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		}()

		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "", errors.New("failed to get SMC license")
		}

		err := activity.SnapmirrorTransferWithFiles(ctx, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get SMC license from cloud")
	})

	t.Run("GenerateTokenForNodeError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		defer func() {
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		smcLicense := "test-license"
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return smcLicense, nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			return nil, errors.New("failed to generate token")
		}

		err := activity.SnapmirrorTransferWithFiles(ctx, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to generate SMC token for node")
	})

	t.Run("NilToken", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		defer func() {
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		smcLicense := "test-license"
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return smcLicense, nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			return nil, nil // Return nil token
		}

		err := activity.SnapmirrorTransferWithFiles(ctx, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SMC token is empty or nil")
	})

	t.Run("EmptyToken", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		defer func() {
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		smcLicense := "test-license"
		emptyToken := ""
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return smcLicense, nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			return &emptyToken, nil // Return empty token
		}

		err := activity.SnapmirrorTransferWithFiles(ctx, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SMC token is empty or nil")
	})

	t.Run("SnapmirrorRelationshipTransferCreateWithFilesError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{
			{
				SourcePath:      "12345",
				DestinationPath: "/restore/file1.txt",
			},
		}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		defer func() {
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		smcLicense := "test-license"
		token := "test-token"
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return smcLicense, nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			return &token, nil
		}

		mockProvider.On("SnapmirrorRelationshipTransferCreateWithFiles", snapmirrorUUID, snapshotName, &token, files).Return(errors.New("transfer failed"))

		err := activity.SnapmirrorTransferWithFiles(ctx, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		mockProvider.AssertExpectations(t)
	})
}
