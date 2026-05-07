package leakedresources

import (
	"context"
	"strings"
	"testing"
)

// TestLocalComputeServiceGetter_GetComputeService verifies that the local
// proxy getter both: (a) builds a usable *compute.Service, and (b) normalizes
// the COMPUTE_API_ENDPOINT value to the canonical "<host>/compute/v1/" base
// path the Compute SDK expects. If this normalization breaks, every Compute
// API call against the local proxy 404s — see local_compute.go for the full
// rationale.
func TestLocalComputeServiceGetter_GetComputeService(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		wantBase string
	}{
		{name: "host_only", endpoint: "http://localhost:9099", wantBase: "http://localhost:9099/compute/v1/"},
		{name: "trailing_slash", endpoint: "http://localhost:9099/", wantBase: "http://localhost:9099/compute/v1/"},
		{name: "with_compute_v1", endpoint: "http://localhost:9099/compute/v1", wantBase: "http://localhost:9099/compute/v1/"},
		{name: "with_compute_v1_slash", endpoint: "http://localhost:9099/compute/v1/", wantBase: "http://localhost:9099/compute/v1/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := newLocalComputeServiceGetter(tc.endpoint)
			if g == nil {
				t.Fatalf("newLocalComputeServiceGetter returned nil")
			}
			svc, err := g.GetComputeService(context.Background())
			if err != nil {
				t.Fatalf("GetComputeService returned error: %v", err)
			}
			if svc == nil {
				t.Fatalf("GetComputeService returned nil service")
			}
			if !strings.HasSuffix(svc.BasePath, "/compute/v1/") {
				t.Errorf("BasePath = %q, want suffix %q", svc.BasePath, "/compute/v1/")
			}
			if svc.BasePath != tc.wantBase {
				t.Errorf("BasePath = %q, want %q", svc.BasePath, tc.wantBase)
			}
		})
	}
}

// TestNewListers_LocalProxyBranch covers the COMPUTE_API_ENDPOINT branch in
// both NewInstanceLister and NewRegionalAddressLister. These constructors
// are exercised in production by the worker pod activity and the local
// branch is what we use for credential-free local runs (see
// cmd/local-compute-proxy). Without this test the new branches show up as
// uncovered diff lines.
func TestNewListers_LocalProxyBranch(t *testing.T) {
	origGetEnv := getEnvString
	t.Cleanup(func() { getEnvString = origGetEnv })

	getEnvString = func(key, fallback string) string {
		if key == "COMPUTE_API_ENDPOINT" {
			return "http://localhost:9099"
		}
		return fallback
	}

	t.Run("instance_lister", func(t *testing.T) {
		l, err := NewInstanceLister(context.Background())
		if err != nil {
			t.Fatalf("NewInstanceLister returned error: %v", err)
		}
		if l == nil {
			t.Fatalf("NewInstanceLister returned nil lister")
		}
		impl, ok := l.(*gcpInstanceLister)
		if !ok {
			t.Fatalf("NewInstanceLister returned %T, want *gcpInstanceLister", l)
		}
		if _, ok := impl.svc.(*localComputeServiceGetter); !ok {
			t.Errorf("instance lister svc = %T, want *localComputeServiceGetter", impl.svc)
		}
	})

	t.Run("regional_address_lister", func(t *testing.T) {
		l, err := NewRegionalAddressLister(context.Background())
		if err != nil {
			t.Fatalf("NewRegionalAddressLister returned error: %v", err)
		}
		if l == nil {
			t.Fatalf("NewRegionalAddressLister returned nil lister")
		}
		impl, ok := l.(*gcpRegionalAddressLister)
		if !ok {
			t.Fatalf("NewRegionalAddressLister returned %T, want *gcpRegionalAddressLister", l)
		}
		if _, ok := impl.svc.(*localComputeServiceGetter); !ok {
			t.Errorf("address lister svc = %T, want *localComputeServiceGetter", impl.svc)
		}
	})
}
