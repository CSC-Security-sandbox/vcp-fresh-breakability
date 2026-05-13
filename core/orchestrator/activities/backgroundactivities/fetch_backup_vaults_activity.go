package backgroundactivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ccfe"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// CCFEBackupVaultLister is the subset of ccfe.Client used to fetch the
// backup vault list for a (project, location) pair. Defined here so tests
// can substitute a fake.
type CCFEBackupVaultLister interface {
	ListBackupVaults(ctx context.Context, projectID, location string) ([]resourcescope.CachedBackupVault, error)
}

// FetchBackupVaultsActivity is registered on the background worker and
// invoked synchronously by FetchCCFEBackupVaultsWorkflow. The leaked-
// resources backup vault detector kicks off that workflow once per project
// and waits for the result, so CCFE data is read live on every detector
// tick. Running on the worker pod ensures the service account has the
// iam.serviceAccounts.implicitDelegation permission required for the
// hydration token.
type FetchBackupVaultsActivity struct {
	CCFE CCFEBackupVaultLister
}

// FetchBackupVaults returns the CCFE backup vault list for one
// (projectID, location) pair. A nil slice means "CCFE disabled"
// (e.g. GCP_HYDRATE_BASE_URL empty) and the detector treats it the same
// as a transient miss — skip the diff instead of false-flagging.
func (a *FetchBackupVaultsActivity) FetchBackupVaults(ctx context.Context, projectID, location string) ([]resourcescope.CachedBackupVault, error) {
	logger := util.GetLogger(ctx)
	vaults, err := a.CCFE.ListBackupVaults(ctx, projectID, location)
	if err != nil {
		logger.Warnf("FetchBackupVaultsActivity.FetchBackupVaults: CCFE list failed project=%s location=%s: %v", projectID, location, err)
		return nil, err
	}
	if vaults == nil {
		logger.Infof("FetchBackupVaultsActivity.FetchBackupVaults: CCFE returned nil (disabled) project=%s location=%s", projectID, location)
		return nil, nil
	}
	logger.Infof("FetchBackupVaultsActivity.FetchBackupVaults: project=%s location=%s vault_count=%d", projectID, location, len(vaults))
	return vaults, nil
}

// Compile-time assertion that *ccfe.Client satisfies CCFEBackupVaultLister.
var _ CCFEBackupVaultLister = (*ccfe.Client)(nil)
