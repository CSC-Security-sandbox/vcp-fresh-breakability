package activities

import (
	"context"
	"database/sql"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

type SFRActivity struct {
	SE database.Storage
}

// SnapmirrorTransferWithFiles performs snapmirror transfer with a list of files (using inode numbers)
// This activity is specific to Single File Restore (SFR) operations
func (a SFRActivity) SnapmirrorTransferWithFiles(ctx context.Context, node *models.Node, snapmirrorUUID, snapshotName string, files []*commonparams.SnapmirrorTransferFile) error {
	activity.RecordHeartbeat(ctx, "SnapmirrorTransferWithFiles started")
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Remove this once we start using cache to store token
	smcLicense, err := GetSmcLicenseFromCloud(ctx)
	if err != nil {
		logger.Errorf("Failed to get SMC license from cloud: %v", err)
		return errors.New("failed to get SMC license from cloud")
	}
	token, err := GenerateTokenForNode(ctx, node, &smcLicense)
	if err != nil {
		logger.Errorf("Failed to generate SMC token for node %s: %v", node.Name, err)
		return errors.New("failed to generate SMC token for node")
	}
	if token == nil || *token == "" {
		logger.Error("SMC token is empty or nil")
		return errors.New("SMC token is empty or nil")
	}
	err = provider.SnapmirrorRelationshipTransferCreateWithFiles(snapmirrorUUID, snapshotName, token, files)
	activity.RecordHeartbeat(ctx, "SnapmirrorTransferWithFiles finished")
	return err
}

// FileInodeAndSize represents inode number and file size for a file
// This type should match the one in adc_activities.go
type FileInodeAndSize struct {
	Inode string
	Size  int64
}

// PopulateSfrMetadataActivity populates SfrMetadata table with file count and total size
func (a SFRActivity) PopulateSfrMetadataActivity(ctx context.Context, fileInodeSizeMap map[string]*FileInodeAndSize, volume *datamodel.Volume, backup *datamodel.Backup, jobID *int64) error {
	logger := util.GetLogger(ctx)

	if len(fileInodeSizeMap) == 0 {
		logger.Warn("No file information provided, skipping SfrMetadata creation")
		return nil
	}

	// Calculate total file size and file count
	var totalSize int64
	fileCount := len(fileInodeSizeMap)
	for _, fileInfo := range fileInodeSizeMap {
		if fileInfo != nil {
			totalSize += fileInfo.Size
		}
	}

	// Create SfrMetadata entry
	var jobIDValue int64
	var jobIDValid bool
	if jobID != nil && *jobID > 0 {
		jobIDValue = *jobID
		jobIDValid = true
	}
	sfrMetadata := &datamodel.SfrMetadata{
		FilesSize:  totalSize,
		FileCount:  fileCount,
		VolumeName: volume.Name,
		VolumeUUID: volume.UUID,
		BackupUUID: backup.UUID,
		AccountID:  sql.NullInt64{Int64: volume.AccountID, Valid: volume.AccountID > 0},
		JobID:      sql.NullInt64{Int64: jobIDValue, Valid: jobIDValid},
	}

	_, err := a.SE.CreateSfrMetadata(ctx, sfrMetadata)
	if err != nil {
		logger.Errorf("Failed to create SfrMetadata: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully created SfrMetadata: fileCount=%d, totalSize=%d bytes for volume %s, backup %s", fileCount, totalSize, volume.UUID, backup.UUID)
	return nil
}

// ValidateAndDeduplicateFileList validates and removes duplicate files from a file list
func (a SFRActivity) ValidateAndDeduplicateFileList(ctx context.Context, fileList []string) ([]string, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "ValidateAndDeduplicateFileList started")

	seen := make(map[string]bool)
	uniqueFiles := make([]string, 0, len(fileList))

	for _, file := range fileList {
		if !seen[file] {
			seen[file] = true
			uniqueFiles = append(uniqueFiles, file)
		}
	}

	logger.Infof("Processing %d unique files, removed %d duplicate(s)", len(uniqueFiles), len(fileList)-len(uniqueFiles))

	activity.RecordHeartbeat(ctx, "ValidateAndDeduplicateFileList completed")
	return uniqueFiles, nil
}
