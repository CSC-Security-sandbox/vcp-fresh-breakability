package leakedresources

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	googlehyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"google.golang.org/api/compute/v1"
)

// DiskLister lists GCE disks across every zone in a project using the
// Compute API's aggregated list endpoint. It runs inside the worker pod
// activity (whose GSA holds compute permissions).
type DiskLister interface {
	ListDisks(ctx context.Context, projectID string) ([]hyperscalermodels.GCEDisk, error)
}

// buildGcpServiceForDiskLister mirrors the address-lister factory pattern
// (assignable function variable so tests can swap it).
var buildGcpServiceForDiskLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
	gcpService := hyperscaler.NewGcpServices(ctx)
	if err := gcpService.InitializeClients(); err != nil {
		return nil, err
	}
	return gcpService, nil
}

// NewDiskLister returns a GCP-backed DiskLister. It is intended to be called
// from the worker pod activity, not from the core pod.
func NewDiskLister(ctx context.Context) (DiskLister, error) {
	gcpService, err := buildGcpServiceForDiskLister(ctx)
	if err != nil {
		return nil, fmt.Errorf("init gcp service for disk lister: %w", err)
	}
	return &gcpDiskLister{svc: gcpService}, nil
}

type gcpDiskLister struct {
	svc computeServiceGetter
}

func (l *gcpDiskLister) ListDisks(ctx context.Context, projectID string) ([]hyperscalermodels.GCEDisk, error) {
	if l == nil || l.svc == nil {
		return nil, fmt.Errorf("disk lister not initialized")
	}
	computeSvc, err := l.svc.GetComputeService(ctx)
	if err != nil {
		return nil, err
	}

	var out []hyperscalermodels.GCEDisk
	err = computeSvc.Disks.AggregatedList(projectID).Pages(ctx, func(resp *compute.DiskAggregatedList) error {
		for scopeKey, scopedList := range resp.Items {
			// zoneFromScopeKey is shared in this package (instance_lister.go).
			zone := zoneFromScopeKey(scopeKey)
			for _, d := range scopedList.Disks {
				if d == nil {
					continue
				}
				out = append(out, hyperscalermodels.GCEDisk{
					Project:           projectID,
					Zone:              zone,
					Name:              d.Name,
					SelfLink:          d.SelfLink,
					Status:            d.Status,
					SizeGB:            d.SizeGb,
					Type:              d.Type,
					Labels:            d.Labels,
					CreationTimestamp: d.CreationTimestamp,
				})
			}
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	return out, nil
}
