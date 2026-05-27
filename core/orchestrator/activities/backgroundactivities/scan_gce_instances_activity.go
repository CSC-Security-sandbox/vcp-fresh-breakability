package backgroundactivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/vmscan"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// newInstanceListerForActivity is an assignable constructor so unit tests
// can swap in a fake without touching the activity body.
var newInstanceListerForActivity = func(ctx context.Context) (hyperscalerleakedresources.InstanceLister, error) {
	return hyperscalerleakedresources.NewInstanceLister(ctx)
}

// ScanGCEInstancesActivity is the Temporal activity that runs on the
// vcp-background-worker pod. It enumerates VMs in the supplied tenant
// projects via the GCE Compute API (using the worker pod's GSA, which is
// the only SA in the system that holds compute permissions in production).
type ScanGCEInstancesActivity struct{}

// ScanGCEInstances performs the cross-project scan. Failures on individual
// projects are recorded in the output's PartialFailures and do not abort the
// whole activity, so a single bad project does not prevent the detector from
// receiving results for the others.
func (a *ScanGCEInstancesActivity) ScanGCEInstances(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("ScanGCEInstances activity started: projects=%d", len(in.ProjectIDs))

	out := &vmscan.ScanOutput{}
	if len(in.ProjectIDs) == 0 {
		logger.Info("ScanGCEInstances activity: no projects supplied, returning empty result")
		return out, nil
	}

	lister, err := newInstanceListerForActivity(ctx)
	if err != nil {
		logger.Warnf("ScanGCEInstances activity: failed to initialize instance lister: %v", err)
		return nil, err
	}

	for _, project := range in.ProjectIDs {
		select {
		case <-ctx.Done():
			logger.Warnf("ScanGCEInstances activity: context cancelled at project=%s: %v", project, ctx.Err())
			return out, ctx.Err()
		default:
		}
		hsItems, err := lister.ListInstances(ctx, project)
		if err != nil {
			logger.Warnf("ScanGCEInstances activity: list instances failed project=%s: %v", project, err)
			out.PartialFailures = append(out.PartialFailures, vmscan.ProjectFailure{
				Project: project,
				Error:   err.Error(),
			})
			out.Items = append(out.Items, toVmscanItems(hsItems)...)
			continue
		}
		items := toVmscanItems(hsItems)
		out.Items = append(out.Items, items...)
		logger.Infof("ScanGCEInstances activity: project=%s instance_count=%d", project, len(items))
	}

	logger.Infof("ScanGCEInstances activity finished: total_instances=%d partial_failures=%d",
		len(out.Items), len(out.PartialFailures))
	return out, nil
}

// toVmscanItems converts the hyperscaler-leaf GCEInstance shape into the
// workflow wire shape vmscan.GCEInstanceItem. The translation is a 1:1 field
// copy by design: keeping the two types decoupled is what lets hyperscaler/
// stay a leaf w.r.t. core/ while the Temporal payload contract (vmscan)
// remains the source of truth for cross-pod traffic.
func toVmscanItems(in []hyperscalermodels.GCEInstance) []vmscan.GCEInstanceItem {
	if len(in) == 0 {
		return nil
	}
	out := make([]vmscan.GCEInstanceItem, len(in))
	for i, v := range in {
		out[i] = vmscan.GCEInstanceItem{
			Project:           v.Project,
			Zone:              v.Zone,
			Name:              v.Name,
			SelfLink:          v.SelfLink,
			Status:            v.Status,
			MachineType:       v.MachineType,
			Labels:            v.Labels,
			CreationTimestamp: v.CreationTimestamp,
		}
	}
	return out
}
