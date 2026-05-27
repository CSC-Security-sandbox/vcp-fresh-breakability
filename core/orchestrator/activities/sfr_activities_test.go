package activities_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/testsuite"
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
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}
		env.RegisterActivity(&activity)

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
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() {
			vsa.GetProviderByNode = originalGetProviderByNode
		}()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		_, err := env.ExecuteActivity(activity.SnapmirrorTransferWithFiles, node, snapmirrorUUID, snapshotName, files)
		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetProviderByNodeError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}
		env.RegisterActivity(&activity)

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() {
			vsa.GetProviderByNode = originalGetProviderByNode
		}()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		_, err := env.ExecuteActivity(activity.SnapmirrorTransferWithFiles, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get provider")
	})

	t.Run("GetSmcLicenseFromCloudError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}
		env.RegisterActivity(&activity)

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() {
			vsa.GetProviderByNode = originalGetProviderByNode
		}()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		defer func() {
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		}()

		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "", errors.New("failed to get SMC license")
		}

		_, err := env.ExecuteActivity(activity.SnapmirrorTransferWithFiles, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get SMC license from cloud")
	})

	t.Run("GenerateTokenForNodeError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}
		env.RegisterActivity(&activity)

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() {
			vsa.GetProviderByNode = originalGetProviderByNode
		}()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		_, err := env.ExecuteActivity(activity.SnapmirrorTransferWithFiles, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to generate SMC token for node")
	})

	t.Run("NilToken", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}
		env.RegisterActivity(&activity)

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() {
			vsa.GetProviderByNode = originalGetProviderByNode
		}()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		_, err := env.ExecuteActivity(activity.SnapmirrorTransferWithFiles, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SMC token is empty or nil")
	})

	t.Run("EmptyToken", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}
		env.RegisterActivity(&activity)

		node := &models.Node{
			Name: "test-node",
		}

		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		files := []*commonparams.SnapmirrorTransferFile{}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() {
			vsa.GetProviderByNode = originalGetProviderByNode
		}()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		_, err := env.ExecuteActivity(activity.SnapmirrorTransferWithFiles, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SMC token is empty or nil")
	})

	t.Run("SnapmirrorRelationshipTransferCreateWithFilesError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(t)
		activity := activities.SFRActivity{SE: mockSE}
		env.RegisterActivity(&activity)

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
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() {
			vsa.GetProviderByNode = originalGetProviderByNode
		}()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		_, err := env.ExecuteActivity(activity.SnapmirrorTransferWithFiles, node, snapmirrorUUID, snapshotName, files)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transfer failed")
		mockProvider.AssertExpectations(t)
	})
}

func TestValidateAndDeduplicateFileList(t *testing.T) {
	tests := []struct {
		name                string
		fileList            []string
		expectedUniqueFiles []string
		expectError         bool
	}{
		{
			name:                "Empty file list",
			fileList:            []string{},
			expectedUniqueFiles: []string{},
			expectError:         false,
		},
		{
			name:                "No duplicates",
			fileList:            []string{"file1.txt", "file2.txt", "file3.txt"},
			expectedUniqueFiles: []string{"file1.txt", "file2.txt", "file3.txt"},
			expectError:         false,
		},
		{
			name:                "With duplicates",
			fileList:            []string{"file1.txt", "file2.txt", "file1.txt", "file3.txt"},
			expectedUniqueFiles: []string{"file1.txt", "file2.txt", "file3.txt"},
			expectError:         false,
		},
		{
			name:                "Multiple duplicates of same file",
			fileList:            []string{"file1.txt", "file1.txt", "file1.txt", "file2.txt"},
			expectedUniqueFiles: []string{"file1.txt", "file2.txt"},
			expectError:         false,
		},
		{
			name:                "All files are duplicates",
			fileList:            []string{"file1.txt", "file1.txt", "file1.txt"},
			expectedUniqueFiles: []string{"file1.txt"},
			expectError:         false,
		},
		{
			name:                "Multiple different duplicates",
			fileList:            []string{"file1.txt", "file2.txt", "file1.txt", "file3.txt", "file2.txt", "file4.txt"},
			expectedUniqueFiles: []string{"file1.txt", "file2.txt", "file3.txt", "file4.txt"},
			expectError:         false,
		},
		{
			name:                "Single file",
			fileList:            []string{"file1.txt"},
			expectedUniqueFiles: []string{"file1.txt"},
			expectError:         false,
		},
		{
			name:                "Files with paths",
			fileList:            []string{"/path/to/file1.txt", "/path/to/file2.txt", "/path/to/file1.txt", "path2/to/file1.txt"},
			expectedUniqueFiles: []string{"/path/to/file1.txt", "/path/to/file2.txt", "path2/to/file1.txt"},
			expectError:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			var ts testsuite.WorkflowTestSuite
			env := ts.NewTestActivityEnvironment()

			activity := activities.SFRActivity{}
			env.RegisterActivity(&activity)

			// Act
			encodedValue, err := env.ExecuteActivity(activity.ValidateAndDeduplicateFileList, tt.fileList)

			// Assert
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				var result []string
				err = encodedValue.Get(&result)
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedUniqueFiles, result, "Unique files mismatch")
			}
		})
	}
}
