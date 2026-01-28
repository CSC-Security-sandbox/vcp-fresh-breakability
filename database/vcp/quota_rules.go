package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	getQuotaRule = _getQuotaRule
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

	// Generate UUID if not provided
	if quotaRule.UUID == "" {
		quotaRule.UUID = utils.RandomUUID()
	}

	// Set timestamps
	now := time.Now()
	quotaRule.CreatedAt = now
	quotaRule.UpdatedAt = now

	// Create the quota rule entry
	err = tx.Create(quotaRule).Error
	if err != nil {
		logger.Error("Failed to create quota rule in database", "error", err)
		return nil, err
	}

	return quotaRule, nil
}

func (d *DataStoreRepository) UpdatingQuotaRule(ctx context.Context, quotaRule *datamodel.QuotaRule) (*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	// Start transaction
	tx, err := startTransaction(db)
	if err != nil {
		logger.Error("Failed to start transaction for quota rule update", "error", err)
		return nil, err
	}
	defer commitOrRollbackOnError(logger, tx, &err)

	// Update timestamp
	updateTime := time.Now()
	quotaRule.UpdatedAt = updateTime

	// Save the updated quota rule
	if err = tx.Updates(quotaRule).Error; err != nil {
		logger.Error("Failed to update quota rule in database", "error", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	// Reload to get the updated quota rule
	updatedQuotaRule, err := getQuotaRule(tx, &datamodel.QuotaRule{
		BaseModel: datamodel.BaseModel{UUID: quotaRule.UUID},
	})
	if err != nil {
		logger.Error("Failed to reload updated quota rule", "error", err)
		return nil, err
	}

	return updatedQuotaRule, nil
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
	_, err = getQuotaRule(tx, &datamodel.QuotaRule{
		BaseModel:   datamodel.BaseModel{UUID: quotaRule.UUID},
		VolumeID:    quotaRule.VolumeID,
		QuotaTarget: quotaRule.QuotaTarget,
		QuotaType:   quotaRule.QuotaType,
	})
	if err != nil {
		logger.Error("Quota rule not found for update", "uuid", quotaRule.UUID, "volumeID", quotaRule.VolumeID, "quotaTarget", quotaRule.QuotaTarget, "quotaType", quotaRule.QuotaType, "error", err)
		return nil, err
	}

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
func (d *DataStoreRepository) GetQuotaRuleByUUID(ctx context.Context, uuid string, accountID int64) (*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	var quotaRule datamodel.QuotaRule

	// Build query with ownership constraints
	query := db.Where("uuid = ?", uuid)

	if accountID > 0 {
		query = query.Where("account_id = ?", accountID)
	}

	err := query.First(&quotaRule).Error

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

// GetQuotaRulesWithCondition fetches quota rules based on filter conditions
func (d *DataStoreRepository) GetQuotaRulesWithCondition(ctx context.Context, filter utils2.Filter) ([]*datamodel.QuotaRule, error) {
	db := d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
	var quotaRules []*datamodel.QuotaRule
	err := db.Find(&quotaRules).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return quotaRules, nil
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
	err = tx.Where("uuid = ?", uuid).First(&quotaRule).Error
	if err != nil {
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

	err = tx.Save(&quotaRule).Error
	if err != nil {
		logger.Error("Failed to soft delete quota rule", "uuid", uuid, "error", err)
		return nil, err
	}

	return nil, err
}

// ReplaceDstQuotaRulesWithSrc deletes destination quota rules and adds source quota rules in a single transaction.
// Returns the created quota rules.
func (d *DataStoreRepository) ReplaceDstQuotaRulesWithSrc(ctx context.Context, volumeID int64, accountID int64, dstQuotaRuleUUIDs []string, srcQuotaRules []*datamodel.QuotaRule) ([]*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	tx, err := startTransaction(db)
	if err != nil {
		logger.Error("Failed to start transaction for replacing destination quota rules", "error", err)
		return nil, err
	}
	defer commitOrRollbackOnError(logger, tx, &err)

	// Delete destination quota rules if provided
	if len(dstQuotaRuleUUIDs) > 0 {
		// Fetch quota rules by volumeID and UUIDs
		var quotaRules []datamodel.QuotaRule
		err = tx.Where("volume_id = ? AND uuid IN ? AND deleted_at IS NULL", volumeID, dstQuotaRuleUUIDs).
			Find(&quotaRules).Error
		if err != nil {
			logger.Error("Failed to fetch quota rules for deletion", "volumeID", volumeID, "error", err)
			return nil, err
		}

		// Check if all requested quota rules were found
		if len(quotaRules) != len(dstQuotaRuleUUIDs) {
			foundUUIDs := make(map[string]bool)
			for _, rule := range quotaRules {
				foundUUIDs[rule.UUID] = true
			}
			var missingUUIDs []string
			for _, uuid := range dstQuotaRuleUUIDs {
				if !foundUUIDs[uuid] {
					missingUUIDs = append(missingUUIDs, uuid)
				}
			}
			logger.Warnf("Some quota rules not found for deletion: volumeID=%d, missingUUIDs=%v", volumeID, missingUUIDs)
			return nil, customerrors.NewNotFoundErr("quota rule", &missingUUIDs[0])
		}

		// Perform soft delete on all quota rules
		now := time.Now()
		for i := range quotaRules {
			quotaRules[i].DeletedAt = &gorm.DeletedAt{Time: now, Valid: true}
			quotaRules[i].State = models.LifeCycleStateDeleted
			quotaRules[i].StateDetails = models.LifeCycleStateDeletedDetails
			quotaRules[i].UpdatedAt = now
		}

		err = tx.Save(&quotaRules).Error
		if err != nil {
			logger.Error("Failed to soft delete destination quota rules", "volumeID", volumeID, "error", err)
			return nil, err
		}

		logger.Infof("Deleted %d destination quota rules for volumeID=%d", len(quotaRules), volumeID)
	}

	// Add source quota rules if provided
	createdQuotaRules := make([]*datamodel.QuotaRule, 0, len(srcQuotaRules))
	if len(srcQuotaRules) > 0 {
		now := time.Now()
		for _, quotaRule := range srcQuotaRules {
			// Set volume and account IDs
			quotaRule.VolumeID = volumeID
			quotaRule.AccountID = accountID

			// Always generate new UUID for destination quota rules
			quotaRule.UUID = utils.RandomUUID()

			// Set timestamps
			quotaRule.CreatedAt = now
			quotaRule.UpdatedAt = now

			// Set state to READY
			quotaRule.State = models.LifeCycleStateREADY
			quotaRule.StateDetails = models.LifeCycleStateReadyDetails

			// Create the quota rule entry
			err = tx.Create(quotaRule).Error
			if err != nil {
				logger.Error("Failed to create source quota rule in database", "error", err, "quotaRuleUUID", quotaRule.UUID)
				return nil, err
			}

			// Add to created quota rules list
			createdQuotaRules = append(createdQuotaRules, quotaRule)
		}

		logger.Infof("Added %d source quota rules for volumeID=%d", len(srcQuotaRules), volumeID)
	}

	logger.Infof("Replaced destination quota rules with %d source rules for volumeID=%d", len(srcQuotaRules), volumeID)
	return createdQuotaRules, nil
}

// GetQuotaRuleCountBySvmID returns the count of quota rules associated with a specific SVM
// This is used to determine if quota (RQuota) should be enabled on the SVM
// According to spec: "Counts quota rules across all NFS volumes in an SVM"
func (d *DataStoreRepository) GetQuotaRuleCountBySvmID(ctx context.Context, svmID int64) (int64, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)

	var nfsVolumes []datamodel.Volume
	err := db.Where("svm_id = ?", svmID).
		Where("deleted_at IS NULL").
		Find(&nfsVolumes).Error

	if err != nil {
		logger.Error("Failed to get volumes for SVM", "svmID", svmID, "error", err)
		return 0, err
	}

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
