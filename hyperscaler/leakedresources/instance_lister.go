package leakedresources

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/vmscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	googlehyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	"google.golang.org/api/compute/v1"
	"strings"
)

// InstanceLister lists GCE instances across every zone in a project using the
// Compute API's aggregated list endpoint. It runs inside the worker pod
// activity (whose GSA holds compute permissions).
type InstanceLister interface {
	ListInstances(ctx context.Context, projectID string) ([]vmscan.GCEInstanceItem, error)
}

// buildGcpServiceForInstanceLister mirrors the address-lister factory pattern
// (assignable function variable so tests can swap it).
var buildGcpServiceForInstanceLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
	gcpService := hyperscaler.NewGcpServices(ctx)
	if err := gcpService.InitializeClients(); err != nil {
		return nil, err
	}
	return gcpService, nil
}

// NewInstanceLister returns a GCP-backed InstanceLister. It is intended to be
// called from the worker pod activity, not from the core pod.
//
// When COMPUTE_API_ENDPOINT is set we route every Compute API call through a
// local proxy (see cmd/local-compute-proxy) instead of compute.googleapis.com.
// This lets developers exercise the worker pod end-to-end with no GCP
// credentials. Production leaves the env var unset, so this branch is dead.
func NewInstanceLister(ctx context.Context) (InstanceLister, error) {
	if ep := getEnvString("COMPUTE_API_ENDPOINT", ""); ep != "" {
		return &gcpInstanceLister{svc: newLocalComputeServiceGetter(ep)}, nil
	}
	gcpService, err := buildGcpServiceForInstanceLister(ctx)
	if err != nil {
		return nil, fmt.Errorf("init gcp service for instance lister: %w", err)
	}
	return &gcpInstanceLister{svc: gcpService}, nil
}

type gcpInstanceLister struct {
	svc computeServiceGetter
}

func (l *gcpInstanceLister) ListInstances(ctx context.Context, projectID string) ([]vmscan.GCEInstanceItem, error) {
	if l == nil || l.svc == nil {
		return nil, fmt.Errorf("instance lister not initialized")
	}
	computeSvc, err := l.svc.GetComputeService(ctx)
	if err != nil {
		return nil, err
	}

	var out []vmscan.GCEInstanceItem
	err = computeSvc.Instances.AggregatedList(projectID).Pages(ctx, func(resp *compute.InstanceAggregatedList) error {
		for scopeKey, scopedList := range resp.Items {
			zone := zoneFromScopeKey(scopeKey)
			for _, vm := range scopedList.Instances {
				if vm == nil {
					continue
				}
				out = append(out, vmscan.GCEInstanceItem{
					Project:           projectID,
					Zone:              zone,
					Name:              vm.Name,
					SelfLink:          vm.SelfLink,
					Status:            vm.Status,
					MachineType:       machineTypeNameFromURL(vm.MachineType),
					Labels:            vm.Labels,
					CreationTimestamp: vm.CreationTimestamp,
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

// machineTypeNameFromURL returns the last path segment of a machine-type URL
// ("…/machineTypes/n2-standard-8" -> "n2-standard-8"). The bare value is
// returned untouched if it is not URL-shaped.
func machineTypeNameFromURL(mtURL string) string {
	if mtURL == "" {
		return ""
	}
	if i := strings.LastIndex(mtURL, "/"); i >= 0 {
		return mtURL[i+1:]
	}
	return mtURL
}

// zoneFromScopeKey extracts the zone name from an aggregated list scope key
// (e.g. "zones/us-central1-a" -> "us-central1-a").
//
// NOTE: the disk-lister PR (#3634) introduces an identical helper in
// disk_lister.go. Whichever PR lands second should delete its copy and rely
// on the surviving one (same package, no rename needed).
func zoneFromScopeKey(key string) string {
	const prefix = "zones/"
	if strings.HasPrefix(key, prefix) {
		return key[len(prefix):]
	}
	return key
}
