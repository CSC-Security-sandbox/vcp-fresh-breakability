package leakedresources

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	googlehyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"google.golang.org/api/compute/v1"
	"strings"
	"time"
)

var (
	getEnvString                            = env.GetString
	buildGcpServiceForRegionalAddressLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
		gcpService := hyperscaler.NewGcpServices(ctx)
		if err := gcpService.InitializeClients(); err != nil {
			return nil, err
		}
		return gcpService, nil
	}
)

// RegionalAddress is a subset of compute.Address fields needed for internal capacity leak detection.
type RegionalAddress struct {
	ResourceName       string
	Name               string
	IP                 string
	Subnetwork         string
	AddressType        string
	Status             string
	Users              []string
	CreationTimestamp  string // RFC3339 from API; used for min-age without persisting state elsewhere
	CreationTime       time.Time
	CreationTimeParsed bool
}

// RegionalAddressLister lists regional compute addresses for a project/region.
type RegionalAddressLister interface {
	ListRegionalAddresses(ctx context.Context, projectID, region string) ([]RegionalAddress, error)
}

type computeServiceGetter interface {
	GetComputeService(ctx context.Context) (*compute.Service, error)
}

// NewRegionalAddressLister creates a GCP-backed lister through hyperscaler abstraction.
//
// COMPUTE_API_ENDPOINT takes precedence: when set we route Compute API calls
// through a local proxy (see cmd/local-compute-proxy) and skip both the
// ENV=local guard and the real GCP client init. That lets the leaked-resources
// pipeline run end-to-end on a developer laptop without GCP credentials.
func NewRegionalAddressLister(ctx context.Context) (RegionalAddressLister, error) {
	if ep := getEnvString("COMPUTE_API_ENDPOINT", ""); ep != "" {
		return &gcpRegionalAddressLister{svc: newLocalComputeServiceGetter(ep)}, nil
	}
	if getEnvString("ENV", "") == "local" {
		return nil, fmt.Errorf("regional address lister disabled when ENV=local")
	}
	gcpService, err := buildGcpServiceForRegionalAddressLister(ctx)
	if err != nil {
		return nil, err
	}
	return &gcpRegionalAddressLister{svc: gcpService}, nil
}

type gcpRegionalAddressLister struct {
	svc computeServiceGetter
}

func (l *gcpRegionalAddressLister) ListRegionalAddresses(ctx context.Context, projectID, region string) ([]RegionalAddress, error) {
	if l == nil || l.svc == nil {
		return nil, fmt.Errorf("regional address lister not initialized")
	}
	computeSvc, err := l.svc.GetComputeService(ctx)
	if err != nil {
		return nil, err
	}

	var out []RegionalAddress
	err = computeSvc.Addresses.List(projectID, region).Pages(ctx, func(resp *compute.AddressList) error {
		for _, a := range resp.Items {
			if a == nil {
				continue
			}
			key := a.SelfLink
			if key == "" {
				key = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/addresses/%s", projectID, region, a.Name)
			}
			addr := RegionalAddress{
				ResourceName:      key,
				Name:              a.Name,
				IP:                a.Address,
				Subnetwork:        a.Subnetwork,
				AddressType:       a.AddressType,
				Status:            a.Status,
				Users:             a.Users,
				CreationTimestamp: a.CreationTimestamp,
			}
			if a.CreationTimestamp != "" {
				if t, err := time.Parse(time.RFC3339, a.CreationTimestamp); err == nil {
					addr.CreationTime = t
					addr.CreationTimeParsed = true
				} else if t, err := time.Parse("2006-01-02T15:04:05.000-07:00", a.CreationTimestamp); err == nil {
					addr.CreationTime = t
					addr.CreationTimeParsed = true
				}
			}
			out = append(out, addr)
		}
		return nil
	})
	return out, err
}

// SubnetworkBaseName returns the subnetwork resource name (last segment) from a full subnetwork URL.
func SubnetworkBaseName(subnetworkURL string) string {
	const sep = "/subnetworks/"
	i := strings.LastIndex(subnetworkURL, sep)
	if i < 0 {
		return ""
	}
	return subnetworkURL[i+len(sep):]
}
