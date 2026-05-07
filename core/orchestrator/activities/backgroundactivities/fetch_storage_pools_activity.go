package backgroundactivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ccfe"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/poolpairs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// CCFEPoolLister is the subset of ccfe.Client used to fetch the pool list for
// a (project, location) pair. Defined here so tests can substitute a fake.
// The slice element type lives in poolpairs so this activity and the
// leaked-resources pool detector share a single struct definition.
type CCFEPoolLister interface {
	ListStoragePools(ctx context.Context, projectID, location string) ([]poolpairs.CachedPool, error)
}

// FetchStoragePoolsActivity is registered on the background worker and
// invoked synchronously by FetchStoragePoolsWorkflow. The leaked-resources
// pool detector kicks off that workflow once per (project, location) group
// it derives from VCP pools and waits for the result, so CCFE data is read
// live on every detector tick (no DB cache).
type FetchStoragePoolsActivity struct {
	CCFE CCFEPoolLister
}

// FetchStoragePools returns the CCFE pool list for one (projectID, location)
// pair. A nil slice means "CCFE disabled" (e.g. GCP_HYDRATE_BASE_URL empty)
// and the detector treats it the same as a transient miss — skip the diff
// instead of false-flagging every VCP pool as a leak.
func (a *FetchStoragePoolsActivity) FetchStoragePools(ctx context.Context, projectID, location string) ([]poolpairs.CachedPool, error) {
	logger := util.GetLogger(ctx)
	pools, err := a.CCFE.ListStoragePools(ctx, projectID, location)
	if err != nil {
		logger.Warnf("FetchStoragePoolsActivity.FetchStoragePools: CCFE list failed project=%s location=%s: %v", projectID, location, err)
		return nil, err
	}
	if pools == nil {
		logger.Infof("FetchStoragePoolsActivity.FetchStoragePools: CCFE returned nil (disabled) project=%s location=%s", projectID, location)
		return nil, nil
	}
	logger.Infof("FetchStoragePoolsActivity.FetchStoragePools: project=%s location=%s pool_count=%d", projectID, location, len(pools))
	return pools, nil
}

// Compile-time assertion that *ccfe.Client satisfies CCFEPoolLister.
var _ CCFEPoolLister = (*ccfe.Client)(nil)
