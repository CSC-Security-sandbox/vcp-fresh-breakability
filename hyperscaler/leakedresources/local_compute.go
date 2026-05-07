package leakedresources

import (
	"context"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"strings"
)

// localComputeServiceGetter constructs a *compute.Service that talks to a
// local proxy compute API instead of compute.googleapis.com. It satisfies
// the package-internal computeServiceGetter interface used by
// gcpInstanceLister and gcpRegionalAddressLister, so the rest of the lister
// code (pagination, projection) is shared between the production and local
// paths.
//
// Activated when the COMPUTE_API_ENDPOINT env var is set (non-empty) on the
// worker process — see instance_lister.go / address_lister.go. Unset =
// production behavior, fully unchanged.
type localComputeServiceGetter struct {
	endpoint string
}

func newLocalComputeServiceGetter(endpoint string) *localComputeServiceGetter {
	return &localComputeServiceGetter{endpoint: endpoint}
}

// GetComputeService implements computeServiceGetter.
//
// option.WithoutAuthentication is required because the local proxy does
// not validate OAuth tokens; option.WithEndpoint redirects every API call
// the *compute.Service would normally send to compute.googleapis.com.
//
// The Compute SDK's default base path is "https://compute.googleapis.com/compute/v1/"
// — the "/compute/v1/" segment is part of the base, not part of the
// per-method path. When option.WithEndpoint replaces the base, that segment
// must be preserved or every request will resolve to /projects/... and
// 404 from any sane proxy. We accept any of these COMPUTE_API_ENDPOINT shapes:
//
//	http://localhost:9099
//	http://localhost:9099/
//	http://localhost:9099/compute/v1
//	http://localhost:9099/compute/v1/
//
// and normalize to "<host>/compute/v1/".
func (g *localComputeServiceGetter) GetComputeService(ctx context.Context) (*compute.Service, error) {
	ep := strings.TrimRight(g.endpoint, "/")
	if !strings.HasSuffix(ep, "/compute/v1") {
		ep += "/compute/v1"
	}
	ep += "/"
	return compute.NewService(ctx,
		option.WithEndpoint(ep),
		option.WithoutAuthentication(),
	)
}
