package detectors

import (
	"context"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// BackupVaultDetector detects backup vault leaks by diffing VCP backup
// vaults against the live CCFE snapshot fetched per project. Pairs are
// enumerated from the shared ProjectLocationLister (accounts × (region +
// zones) via EnumerateProjectLocationKeys), then collapsed to unique
// (project, region) since backup vaults are regional. This ensures coverage
// even when VCP has zero vault rows. CCFE data is fetched via a Temporal
// workflow on the background worker pod (which has the IAM permissions for
// the hydration token).
type BackupVaultDetector struct {
	fetcher CCFEBackupVaultFetcher
	lister  ProjectLocationLister
}

// NewBackupVaultDetector returns a detector that compares VCP backup
// vaults against the CCFE snapshot fetched live per project. The lister
// is the shared ProjectLocationLister; the detector deduplicates
// zone-level pairs to their parent region because CCFE backup vaults are
// regional resources.
func NewBackupVaultDetector(fetcher CCFEBackupVaultFetcher, lister ProjectLocationLister) *BackupVaultDetector {
	return &BackupVaultDetector{fetcher: fetcher, lister: lister}
}

// Name implements model.Detector.
func (d *BackupVaultDetector) Name() string {
	return "backup_vault"
}

// regionOnlyPairs deduplicates a set of (projectID, location) pairs to
// unique (projectID, region) pairs. Zones are collapsed to their parent
// region via utils.ParseRegionAndZone; entries that fail to parse are
// skipped.
func regionOnlyPairs(pairs []resourcescope.ProjectLocation) []resourcescope.ProjectLocation {
	type projectRegionKey struct {
		projectID string
		region    string
	}
	seen := make(map[projectRegionKey]struct{}, len(pairs))
	result := make([]resourcescope.ProjectLocation, 0, len(pairs))
	for _, p := range pairs {
		region, _, err := utils.ParseRegionAndZone(p.Location)
		if err != nil || region == "" {
			continue
		}
		key := projectRegionKey{projectID: p.ProjectID, region: region}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, resourcescope.ProjectLocation{ProjectID: p.ProjectID, Location: region})
	}
	return result
}

// Detect implements model.Detector. It enumerates (projectID, location)
// pairs from the shared ProjectLocationLister, collapses them to unique
// (projectID, region), loads VCP backup vaults and groups them by
// (projectID, region) keyed on UUID, then fires one
// FetchCCFEBackupVaultsWorkflow per project. The workflow fans
// per-location CCFE list calls out as parallel activities. The detector
// then diffs each location against the VCP-side groups by UUID and emits
// in_ccfe_not_in_vcp / in_vcp_not_in_ccfe records.
//
// UUID is the comparison key (not name): CCFE returns it as "netappUuid"
// which equals VCP's BackupVault.UUID, so the diff is unambiguous even
// when a vault is recreated under the same name.
//
// Because pairs come from the accounts table (not from VCP vault rows),
// the detector still queries CCFE even when VCP has zero backup vaults
// — so CCFE-only leaks are always surfaced.
func (d *BackupVaultDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	logger := util.GetLogger(ctx)
	var records []model.LeakRecord

	logger.Infof("backup vault detector: Detect started")

	// 1. Load VCP backup vaults and group by (projectID, region) keyed on UUID.
	vaults, err := storage.GetMultipleBackupVaults(ctx, nil)
	if err != nil {
		logger.Warnf("backup vault detector: GetMultipleBackupVaults failed: %v", err)
		return nil, err
	}
	logger.Infof("backup vault detector: loaded %d VCP vault(s)", len(vaults))

	type projectRegion struct {
		projectID string
		region    string
	}
	vcpGroups := make(map[projectRegion][]*datamodel.BackupVault)
	for _, bv := range vaults {
		if bv == nil || bv.Account == nil || bv.Account.Name == "" || strings.TrimSpace(bv.UUID) == "" {
			continue
		}
		loc, _, ok := ccfeLocationForBackupVault(bv)
		if !ok {
			continue
		}
		key := projectRegion{projectID: bv.Account.Name, region: loc}
		vcpGroups[key] = append(vcpGroups[key], bv)
	}

	// 2. Enumerate (projectID, location) pairs from the shared lister,
	//    then collapse to unique (project, region).
	allPairs, err := d.lister.ListProjectLocations(ctx)
	if err != nil {
		return nil, err
	}
	pairs := regionOnlyPairs(allPairs)
	if len(pairs) == 0 {
		logger.Infof("backup vault detector: no account-region pairs to check, exiting")
		return records, nil
	}

	// Group by project so we fire one workflow per project.
	locationsByProject, projectOrder := groupByProject(pairs)

	failedProjects := 0
	skippedLocations := 0
	totalProjects := len(projectOrder)
	lastDecile := 0
	for i, projectID := range projectOrder {
		locations := locationsByProject[projectID]

		vaultsByLocation, err := d.fetcher.FetchCCFEBackupVaults(ctx, projectID, locations)
		if err != nil {
			logger.Warnf("backup vault detector: FetchCCFEBackupVaults failed project=%s: %v", projectID, err)
			failedProjects++
		} else {
			for _, location := range locations {
				key := projectRegion{projectID: projectID, region: location}

				ccfeVaults, present := vaultsByLocation[location]
				if !present || ccfeVaults == nil {
					skippedLocations++
					continue
				}

				// Build VCP-side index keyed on UUID — names can be reused
				// across recreations, UUIDs cannot, so UUID is the stable
				// identity for the diff.
				vcpByUUID := make(map[string]*datamodel.BackupVault, len(vcpGroups[key]))
				for _, bv := range vcpGroups[key] {
					if bv.UUID != "" {
						vcpByUUID[bv.UUID] = bv
					}
				}

				ccfeByUUID := make(map[string]resourcescope.CachedBackupVault, len(ccfeVaults))
				for _, cv := range ccfeVaults {
					if cv.UUID != "" {
						ccfeByUUID[cv.UUID] = cv
					}
				}

				// In CCFE but not in VCP.
				for uuid, cv := range ccfeByUUID {
					if _, inVCP := vcpByUUID[uuid]; !inVCP {
						records = append(records, model.LeakRecord{
							ResourceType: model.ResourceTypeBackupVault,
							ResourceID:   uuid,
							ResourceName: cv.Name,
							ProjectID:    projectID,
							Region:       location,
							Reason:       ReasonInCCFENotInVCP,
							Extra:        map[string]string{"uuid": uuid},
						})
					}
				}

				// In VCP but not in CCFE.
				for uuid, bv := range vcpByUUID {
					if _, inCCFE := ccfeByUUID[uuid]; !inCCFE {
						records = append(records, model.LeakRecord{
							ResourceType: model.ResourceTypeBackupVault,
							ResourceID:   uuid,
							ResourceName: bv.Name,
							ProjectID:    projectID,
							Region:       location,
							Reason:       ReasonInVCPNotInCCFE,
							Extra:        map[string]string{"uuid": uuid},
						})
					}
				}
			}
		}

		logDetectorProgress(logger, "backup vault detector", i+1, totalProjects, &lastDecile)
	}

	logger.Infof("backup vault detector: completed vcp_vaults=%d pairs=%d projects=%d failed_projects=%d skipped_locations=%d leaks=%d",
		len(vaults), len(pairs), len(projectOrder), failedProjects, skippedLocations, len(records))
	return records, nil
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
