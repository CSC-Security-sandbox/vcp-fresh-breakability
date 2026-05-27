package backgroundactivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/diskscan"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// newDiskListerForActivity is an assignable constructor so unit tests
// can swap in a fake without touching the activity body.
var newDiskListerForActivity = func(ctx context.Context) (hyperscalerleakedresources.DiskLister, error) {
	return hyperscalerleakedresources.NewDiskLister(ctx)
}

// ScanGCEDisksActivity is the Temporal activity that runs on the
// vcp-background-worker pod. It enumerates disks in the supplied tenant
// projects via the GCE Compute API (using the worker pod's GSA, which is
// the only SA in the system that holds compute permissions in production).
type ScanGCEDisksActivity struct{}

// ScanGCEDisks performs the cross-project scan. Failures on individual
// projects are recorded in the output's PartialFailures and do not abort the
// whole activity, so a single bad project does not prevent the detector from
// receiving results for the others.
func (a *ScanGCEDisksActivity) ScanGCEDisks(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("ScanGCEDisks activity started: projects=%d", len(in.ProjectIDs))

	out := &diskscan.ScanOutput{}
	if len(in.ProjectIDs) == 0 {
		logger.Info("ScanGCEDisks activity: no projects supplied, returning empty result")
		return out, nil
	}

	lister, err := newDiskListerForActivity(ctx)
	if err != nil {
		logger.Warnf("ScanGCEDisks activity: failed to initialize disk lister: %v", err)
		return nil, err
	}

	for _, project := range in.ProjectIDs {
		select {
		case <-ctx.Done():
			logger.Warnf("ScanGCEDisks activity: context cancelled at project=%s: %v", project, ctx.Err())
			return out, ctx.Err()
		default:
		}

		hsItems, err := lister.ListDisks(ctx, project)
		if err != nil {
			logger.Warnf("ScanGCEDisks activity: list disks failed project=%s: %v", project, err)
			out.PartialFailures = append(out.PartialFailures, diskscan.ProjectFailure{
				Project: project,
				Error:   err.Error(),
			})
			out.Items = append(out.Items, toDiskscanItems(hsItems)...)
			continue
		}
		items := toDiskscanItems(hsItems)
		out.Items = append(out.Items, items...)
		logger.Infof("ScanGCEDisks activity: project=%s disk_count=%d", project, len(items))
	}

	logger.Infof("ScanGCEDisks activity finished: total_disks=%d partial_failures=%d",
		len(out.Items), len(out.PartialFailures))
	return out, nil
}

// toDiskscanItems converts the hyperscaler-leaf GCEDisk shape into the
// workflow wire shape diskscan.GCEDiskItem. See toVmscanItems for rationale.
func toDiskscanItems(in []hyperscalermodels.GCEDisk) []diskscan.GCEDiskItem {
	if len(in) == 0 {
		return nil
	}
	out := make([]diskscan.GCEDiskItem, len(in))
	for i, d := range in {
		out[i] = diskscan.GCEDiskItem{
			Project:           d.Project,
			Zone:              d.Zone,
			Name:              d.Name,
			SelfLink:          d.SelfLink,
			Status:            d.Status,
			SizeGB:            d.SizeGB,
			Type:              d.Type,
			Labels:            d.Labels,
			CreationTimestamp: d.CreationTimestamp,
		}
	}
	return out
}
