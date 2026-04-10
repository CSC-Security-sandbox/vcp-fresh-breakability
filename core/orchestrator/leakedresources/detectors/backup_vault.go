package detectors

import (
	"context"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// CCFEBackupVaultLister lists backup vault resource identifiers from CCFE for a given project and location.
// Implemented by leakedresources/ccfe.Client; injectable for tests.
type CCFEBackupVaultLister interface {
	ListBackupVaults(ctx context.Context, projectID, location string) ([]string, error)
}

// BackupVaultDetector detects backup vault leaks by comparing VCP DB with CCFE per (project, location).
type BackupVaultDetector struct {
	ccfe CCFEBackupVaultLister
}

// NewBackupVaultDetector returns a detector that compares VCP backup vaults with CCFE ListBackupVaults
// for each derived (project id, region) scope.
func NewBackupVaultDetector(ccfe CCFEBackupVaultLister) *BackupVaultDetector {
	return &BackupVaultDetector{ccfe: ccfe}
}

// Name implements model.Detector.
func (d *BackupVaultDetector) Name() string {
	return "backup_vault"
}

// ccfeLocationForBackupVault returns the GCP region (location id) to use for CCFE
// .../locations/{location}/backupVaults. Matches backup vault creation: IN_REGION uses source/location;
// prefer SourceRegionName, then BackupRegionName when source is unset.
// fromField is "source_region", "backup_region", or "" when ok is false (for logging).
func ccfeLocationForBackupVault(bv *datamodel.BackupVault) (location string, fromField string, ok bool) {
	if bv.SourceRegionName != nil {
		if s := strings.TrimSpace(*bv.SourceRegionName); s != "" {
			return s, "source_region", true
		}
	}
	if bv.BackupRegionName != nil {
		if s := strings.TrimSpace(*bv.BackupRegionName); s != "" {
			return s, "backup_region", true
		}
	}
	return "", "", false
}

// Detect implements model.Detector. It loads all non-deleted backup vaults from VCP (with Account),
// groups by (projectID, CCFE location), and diffs against ccfe.ListBackupVaults for each group.
func (d *BackupVaultDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	logger := util.GetLogger(ctx)
	var records []model.LeakRecord

	logger.Infof("backup vault detector: Detect started")
	vaults, err := storage.GetMultipleBackupVaults(ctx, nil)
	if err != nil {
		logger.Infof("backup vault detector: GetMultipleBackupVaults failed: %v", err)
		return nil, err
	}
	logger.Infof("backup vault detector: GetMultipleBackupVaults returned %d vault(s)", len(vaults))
	if len(vaults) == 0 {
		logger.Infof("backup vault detector: no VCP backup vaults, exiting")
		return records, nil
	}

	type projectLocation struct {
		projectID string
		location  string
	}
	groups := make(map[projectLocation][]*datamodel.BackupVault)
	for _, bv := range vaults {
		if bv == nil {
			logger.Infof("backup vault detector: skip nil backup vault entry in slice")
			continue
		}
		logger.Infof("backup vault detector: considering vault uuid=%s name=%q", bv.UUID, bv.Name)
		if bv.Account == nil {
			logger.Infof("backup vault detector: skip vault uuid=%s, account is nil", bv.UUID)
			continue
		}
		if bv.Account.Name == "" {
			logger.Infof("backup vault detector: skip vault uuid=%s, account name is empty", bv.UUID)
			continue
		}
		loc, fromField, ok := ccfeLocationForBackupVault(bv)
		if !ok {
			logger.Infof("backup vault %s: skip, no source/backup region for CCFE scope", bv.UUID)
			continue
		}
		logger.Infof("backup vault %s: CCFE location=%q derived from %s", bv.UUID, loc, fromField)
		if strings.TrimSpace(bv.Name) == "" {
			logger.Infof("backup vault detector: skip vault uuid=%s, name is empty", bv.UUID)
			continue
		}
		key := projectLocation{projectID: bv.Account.Name, location: loc}
		groups[key] = append(groups[key], bv)
		logger.Infof("backup vault detector: grouped vault uuid=%s name=%q under project=%s location=%s", bv.UUID, bv.Name, key.projectID, key.location)
	}

	logger.Infof("backup vault detector: built %d (project,location) group(s) for CCFE compare", len(groups))
	for key, vcpVaults := range groups {
		logger.Infof("backup vault detector: processing group project=%s location=%s with %d VCP vault(s)", key.projectID, key.location, len(vcpVaults))
		vcpByName := make(map[string]*datamodel.BackupVault)
		for _, bv := range vcpVaults {
			vcpByName[bv.Name] = bv
			logger.Infof("backup vault detector: VCP map entry name=%q uuid=%s", bv.Name, bv.UUID)
		}
		logger.Infof("backup vault detector: calling CCFE ListBackupVaults project=%s location=%s", key.projectID, key.location)

		ccfeNames, err := d.ccfe.ListBackupVaults(ctx, key.projectID, key.location)
		if err != nil {
			logger.Warnf("backup vault detector: CCFE list failed for project=%s location=%s: %v", key.projectID, key.location, err)
			logger.Infof("backup vault detector: skipping group project=%s location=%s after CCFE error", key.projectID, key.location)
			continue
		}
		if ccfeNames == nil {
			logger.Infof("backup vault detector: CCFE ListBackupVaults returned nil (disabled); skip group project=%s location=%s", key.projectID, key.location)
			continue
		}
		logger.Infof("backup vault detector: CCFE returned %d backup vault id(s) for project=%s location=%s", len(ccfeNames), key.projectID, key.location)
		ccfeSet := make(map[string]struct{}, len(ccfeNames))
		for _, n := range ccfeNames {
			ccfeSet[n] = struct{}{}
			logger.Infof("backup vault detector: CCFE set entry id=%q", n)
		}

		for _, n := range ccfeNames {
			if _, inVCP := vcpByName[n]; !inVCP {
				logger.Infof("backup vault detector: leak in_ccfe_not_in_vcp id=%q project=%s location=%s", n, key.projectID, key.location)
				records = append(records, model.LeakRecord{
					ResourceType: model.ResourceTypeBackupVault,
					ResourceID:   n,
					ResourceName: n,
					ProjectID:    key.projectID,
					Region:       key.location,
					Reason:       ReasonInCCFENotInVCP,
				})
			}
		}
		for name, bv := range vcpByName {
			if _, inCCFE := ccfeSet[name]; !inCCFE {
				logger.Infof("backup vault detector: leak in_vcp_not_in_ccfe name=%q uuid=%s project=%s location=%s", name, bv.UUID, key.projectID, key.location)
				records = append(records, model.LeakRecord{
					ResourceType: model.ResourceTypeBackupVault,
					ResourceID:   bv.UUID,
					ResourceName: bv.Name,
					ProjectID:    key.projectID,
					Region:       key.location,
					Reason:       ReasonInVCPNotInCCFE,
					Extra:        map[string]string{"uuid": bv.UUID},
				})
			}
		}
		logger.Infof("backup vault detector: finished diff for group project=%s location=%s", key.projectID, key.location)
	}

	logger.Infof("backup vault detector: Detect finished, %d leak record(s)", len(records))
	return records, nil
}
