package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

// GetQuotaRulesByVolumeID fetches all quota rules for a given volume
// This implements GetQuotaRulesForVolume from the specification
func (d *DataStoreRepository) GetQuotaRulesByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	var quotaRules []*datamodel.QuotaRule

	// Query quota rules for the specified volume
	err := db.Where("volume_id = ?", volumeID).
		Find(&quotaRules).Error

	if err != nil {
		logger.Error("Failed to fetch quota rules for volume", "volumeID", volumeID, "error", err)
		return nil, err
	}

	return quotaRules, nil
}

// CreatingQuotaRule creates a new quota rule entry in the database with CREATING state
// This is used during the async quota rule creation workflow
func (d *DataStoreRepository) CreatingQuotaRule(ctx context.Context, quotaRule *datamodel.QuotaRule) (*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	// Start transaction for consistency
	tx, err := startTransaction(db)
	if err != nil {
		logger.Error("Failed to start transaction for quota rule creation", "error", err)
		return nil, err
	}
	defer commitOrRollbackOnError(logger, tx, &err)

	// Check for duplicate quota rule (same type and target on same volume)
	var existingRule datamodel.QuotaRule
	checkErr := tx.Where("volume_id = ? AND quota_type = ? AND quota_target = ? AND deleted_at IS NULL",
		quotaRule.VolumeID, quotaRule.QuotaType, quotaRule.QuotaTarget).
		First(&existingRule).Error

	if checkErr == nil {
		// Found existing quota rule with same type and target
		logger.Errorf("Duplicate quota rule detected: type=%s, target=%s, volumeID=%d",
			quotaRule.QuotaType, quotaRule.QuotaTarget, quotaRule.VolumeID)
		return nil, customerrors.NewConflictErr("quota rule with same type and target already exists for this volume")
	} else if checkErr != gorm.ErrRecordNotFound {
		// Unexpected error during duplicate check
		logger.Error("Error checking for duplicate quota rule", "error", checkErr)
		return nil, checkErr
	}

	// Generate UUID if not provided
	if quotaRule.UUID == "" {
		quotaRule.UUID = utils.RandomUUID()
	}

	// Set timestamps
	now := time.Now()
	quotaRule.CreatedAt = now
	quotaRule.UpdatedAt = now

	// Create the quota rule entry
	if err := tx.Create(quotaRule).Error; err != nil {
		logger.Error("Failed to create quota rule in database", "error", err)
		return nil, err
	}

	// Preload Volume details for the quota rule
	if err := tx.Preload("Volume").First(quotaRule, quotaRule.ID).Error; err != nil {
		logger.Error("Failed to preload volume details for quota rule", "error", err)
		return nil, err
	}

	return quotaRule, nil
}

// UpdateQuotaRule updates an existing quota rule in the database
func (d *DataStoreRepository) UpdateQuotaRule(ctx context.Context, quotaRule *datamodel.QuotaRule) (*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	// Start transaction
	tx, err := startTransaction(db)
	if err != nil {
		logger.Error("Failed to start transaction for quota rule update", "error", err)
		return nil, err
	}
	defer commitOrRollbackOnError(logger, tx, &err)

	// Verify quota rule exists before updating
	// Use UUID, VolumeID, QuotaTarget, and QuotaType for verification
	_, err = _getQuotaRule(tx, &datamodel.QuotaRule{
		BaseModel:   datamodel.BaseModel{UUID: quotaRule.UUID},
		VolumeID:    quotaRule.VolumeID,
		QuotaTarget: quotaRule.QuotaTarget,
		QuotaType:   quotaRule.QuotaType,
	})
	if err != nil {
		logger.Error("Quota rule not found for update", "uuid", quotaRule.UUID, "volumeID", quotaRule.VolumeID, "quotaTarget", quotaRule.QuotaTarget, "quotaType", quotaRule.QuotaType, "error", err)
		return nil, err
	}

	// For DP volumes, set the quota rule directly to AVAILABLE state
	// No ONTAP operations are needed since quotas are managed by the source volume
	// Update timestamp
	quotaRule.State = models.LifeCycleStateREADY
	quotaRule.StateDetails = models.LifeCycleStateReadyDetails
	quotaRule.UpdatedAt = time.Now()

	// Update the quota rule
	err = tx.Updates(quotaRule).Error
	if err != nil {
		logger.Error("Failed to update quota rule in database", "error", err)
		return nil, err
	}

	return quotaRule, nil
}

// GetQuotaRuleByUUID retrieves a specific quota rule by UUID with ownership validation
func (d *DataStoreRepository) GetQuotaRuleByUUID(ctx context.Context, uuid string, accountID int64, volumeID int64) (*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	var quotaRule datamodel.QuotaRule

	// Build query with ownership constraints
	query := db.Where("uuid = ?", uuid)

	if accountID > 0 {
		query = query.Where("account_id = ?", accountID)
	}

	if volumeID > 0 {
		query = query.Where("volume_id = ?", volumeID)
	}

	err := query.Preload("Volume").Preload("Volume.Pool").First(&quotaRule).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			logger.Warnf("Quota rule not found: UUID=%s", uuid)
			return nil, customerrors.NewNotFoundErr("quota rule", &uuid)
		}
		logger.Error("Failed to fetch quota rule by UUID", "uuid", uuid, "error", err)
		return nil, err
	}

	logger.Infof("Successfully fetched quota rule: UUID=%s, Name=%s", quotaRule.UUID, quotaRule.Name)
	return &quotaRule, nil
}

// DeleteQuotaRule performs soft delete on a quota rule
func (d *DataStoreRepository) DeleteQuotaRule(ctx context.Context, uuid string) (*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	// Start transaction
	tx, err := startTransaction(db)
	if err != nil {
		logger.Error("Failed to start transaction for quota rule deletion", "error", err)
		return nil, err
	}
	defer commitOrRollbackOnError(logger, tx, &err)

	// Fetch the quota rule
	var quotaRule datamodel.QuotaRule
	if err := tx.Where("uuid = ?", uuid).First(&quotaRule).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			logger.Warnf("Quota rule not found for deletion: UUID=%s", uuid)
			return nil, customerrors.NewNotFoundErr("quota rule", &uuid)
		}
		logger.Error("Failed to fetch quota rule for deletion", "uuid", uuid, "error", err)
		return nil, err
	}

	// Perform soft delete
	now := time.Now()
	quotaRule.DeletedAt = &gorm.DeletedAt{Time: now, Valid: true}
	quotaRule.State = models.LifeCycleStateDeleted
	quotaRule.StateDetails = models.LifeCycleStateDeletedDetails
	quotaRule.UpdatedAt = now

	if err := tx.Save(&quotaRule).Error; err != nil {
		logger.Error("Failed to soft delete quota rule", "uuid", uuid, "error", err)
		return nil, err
	}

	return nil, err
}

// GetQuotaRuleCountBySvmID returns the count of quota rules associated with a specific SVM
// This is used to determine if quota (RQuota) should be enabled on the SVM
// According to spec: "Counts quota rules across all NFS volumes in an SVM"
func (d *DataStoreRepository) GetQuotaRuleCountBySvmID(ctx context.Context, svmID int64) (int64, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	// Step 1: Get all volumes for this SVM that have NFS protocols
	var nfsVolumes []datamodel.Volume
	err := db.Where("svm_id = ?", svmID).
		Where("deleted_at IS NULL").
		Find(&nfsVolumes).Error

	if err != nil {
		logger.Error("Failed to get volumes for SVM", "svmID", svmID, "error", err)
		return 0, err
	}

	// Step 2: Filter volumes to only NFS volumes using protocol constants
	var nfsVolumeIDs []int64
	for _, volume := range nfsVolumes {
		if volume.VolumeAttributes != nil && volume.VolumeAttributes.Protocols != nil {
			protocols := volume.VolumeAttributes.Protocols
			// Check if volume has NFSv3 or NFSv4 protocol
			if hasNFSProtocol(protocols) {
				nfsVolumeIDs = append(nfsVolumeIDs, volume.ID)
			}
		}
	}

	// Step 3: Count quota rules for these NFS volumes
	var count int64
	err = db.Model(&datamodel.QuotaRule{}).
		Where("volume_id IN ?", nfsVolumeIDs).
		Where("deleted_at IS NULL").
		Count(&count).Error

	if err != nil {
		logger.Error("Failed to count quota rules for NFS volumes", "svmID", svmID, "error", err)
		return 0, err
	}

	return count, nil
}

// _getQuotaRule retrieves a quota rule from the database using the provided query
func _getQuotaRule(db *gorm.DB, query *datamodel.QuotaRule) (*datamodel.QuotaRule, error) {
	quotaRule := &datamodel.QuotaRule{}
	err := db.First(quotaRule, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "quota rule", nil)
	}
	return quotaRule, nil
}

// hasNFSProtocol checks if the protocol list contains NFSv3 or NFSv4
func hasNFSProtocol(protocols []string) bool {
	for _, protocol := range protocols {
		if protocol == utils.ProtocolNFSv3 || protocol == utils.ProtocolNFSv4 {
			return true
		}
	}
	return false
}
