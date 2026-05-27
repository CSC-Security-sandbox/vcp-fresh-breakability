package backgroundactivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ipscan"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// newRegionalAddressListerForActivity is an assignable constructor so unit
// tests can swap in a fake without touching the activity body.
var newRegionalAddressListerForActivity = func(ctx context.Context) (hyperscalerleakedresources.RegionalAddressLister, error) {
	return hyperscalerleakedresources.NewRegionalAddressLister(ctx)
}

// ScanRegionalAddressesActivity is the Temporal activity that runs on the
// vcp-background-worker pod. It enumerates regional reserved addresses for
// each (project, region) pair via the GCE Compute API (using the worker
// pod's GSA, which is the only SA in the system that holds compute
// permissions in production).
type ScanRegionalAddressesActivity struct{}

// ScanRegionalAddresses performs the cross-(project,region) scan. Failures
// on individual pairs are recorded in PartialFailures and do not abort the
// whole activity, so a single bad pair does not prevent the detector from
// receiving results for the others.
func (a *ScanRegionalAddressesActivity) ScanRegionalAddresses(ctx context.Context, in ipscan.ScanInput) (*ipscan.ScanOutput, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("ScanRegionalAddresses activity started: targets=%d", len(in.Targets))

	out := &ipscan.ScanOutput{}
	if len(in.Targets) == 0 {
		logger.Info("ScanRegionalAddresses activity: no targets supplied, returning empty result")
		return out, nil
	}

	lister, err := newRegionalAddressListerForActivity(ctx)
	if err != nil {
		logger.Warnf("ScanRegionalAddresses activity: failed to initialize address lister: %v", err)
		return nil, err
	}

	for _, t := range in.Targets {
		select {
		case <-ctx.Done():
			logger.Warnf("ScanRegionalAddresses activity: context cancelled at project=%s region=%s: %v", t.Project, t.Region, ctx.Err())
			return out, ctx.Err()
		default:
		}

		addrs, err := lister.ListRegionalAddresses(ctx, t.Project, t.Region)
		if err != nil {
			logger.Warnf("ScanRegionalAddresses activity: list addresses failed project=%s region=%s: %v", t.Project, t.Region, err)
			out.PartialFailures = append(out.PartialFailures, ipscan.ProjectRegionFailure{
				Project: t.Project,
				Region:  t.Region,
				Error:   err.Error(),
			})
			continue
		}
		out.Results = append(out.Results, ipscan.ScanResult{
			Project:   t.Project,
			Region:    t.Region,
			Addresses: addrs,
		})
		logger.Infof("ScanRegionalAddresses activity: project=%s region=%s address_count=%d", t.Project, t.Region, len(addrs))
	}

	logger.Infof("ScanRegionalAddresses activity finished: result_groups=%d partial_failures=%d",
		len(out.Results), len(out.PartialFailures))
	return out, nil
}
