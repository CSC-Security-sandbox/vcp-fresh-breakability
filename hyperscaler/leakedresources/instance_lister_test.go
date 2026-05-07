package leakedresources

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	googlehyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMachineTypeNameFromURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want string
	}{
		{"https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/machineTypes/n2-standard-8", "n2-standard-8"},
		{"projects/p/zones/us-central1-a/machineTypes/c2-standard-30", "c2-standard-30"},
		{"n2-standard-8", "n2-standard-8"},
		{"", ""},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, machineTypeNameFromURL(tc.in), "in=%q", tc.in)
	}
}

func TestNewInstanceLister_BuilderError(t *testing.T) {
	orig := buildGcpServiceForInstanceLister
	t.Cleanup(func() { buildGcpServiceForInstanceLister = orig })
	buildGcpServiceForInstanceLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
		return nil, errors.New("init failed")
	}

	l, err := NewInstanceLister(context.Background())
	assert.Error(t, err)
	assert.Nil(t, l)
}

func TestNewInstanceLister_Success(t *testing.T) {
	orig := buildGcpServiceForInstanceLister
	t.Cleanup(func() { buildGcpServiceForInstanceLister = orig })
	buildGcpServiceForInstanceLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
		return &googlehyperscaler.GcpServices{}, nil
	}

	l, err := NewInstanceLister(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, l)
}

func TestListInstances_NilLister(t *testing.T) {
	var l *gcpInstanceLister
	_, err := l.ListInstances(context.Background(), "p")
	assert.Error(t, err)
}

func TestListInstances_GetComputeServiceError(t *testing.T) {
	l := &gcpInstanceLister{svc: &fakeComputeServiceGetter{err: errors.New("compute svc error")}}
	_, err := l.ListInstances(context.Background(), "p")
	assert.Error(t, err)
}

func TestListInstances_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"items": {
				"zones/us-central1-a": {
					"instances": [
						{
							"name": "gcnv-abc-01",
							"selfLink": "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/instances/gcnv-abc-01",
							"status": "RUNNING",
							"machineType": "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/machineTypes/n2-standard-8",
							"labels": {
								"pool_uuid": "b9af7f04-4029-6839-6925-595c7d11aef6",
								"deployment_id": "gcnv-abc"
							},
							"creationTimestamp": "2026-05-01T10:00:00Z"
						}
					]
				},
				"zones/us-central1-c": {
					"instances": [
						{
							"name": "gcnv-abc-02",
							"status": "RUNNING",
							"labels": {
								"pool_uuid": "b9af7f04-4029-6839-6925-595c7d11aef6"
							}
						}
					]
				}
			}
		}`))
	}))
	defer srv.Close()

	ctx := context.Background()
	computeSvc, err := compute.NewService(
		ctx,
		option.WithEndpoint(srv.URL+"/"),
		option.WithHTTPClient(srv.Client()),
		option.WithoutAuthentication(),
	)
	assert.NoError(t, err)

	l := &gcpInstanceLister{svc: &fakeComputeServiceGetter{svc: computeSvc}}
	got, err := l.ListInstances(ctx, "p")
	assert.NoError(t, err)
	assert.Len(t, got, 2)

	byName := map[string]struct {
		zone        string
		status      string
		machineType string
		labels      map[string]string
	}{}
	for _, vm := range got {
		byName[vm.Name] = struct {
			zone        string
			status      string
			machineType string
			labels      map[string]string
		}{vm.Zone, vm.Status, vm.MachineType, vm.Labels}
	}

	first := byName["gcnv-abc-01"]
	assert.Equal(t, "us-central1-a", first.zone)
	assert.Equal(t, "RUNNING", first.status)
	assert.Equal(t, "n2-standard-8", first.machineType)
	assert.Equal(t, "b9af7f04-4029-6839-6925-595c7d11aef6", first.labels["pool_uuid"])
	assert.Equal(t, "gcnv-abc", first.labels["deployment_id"])

	second := byName["gcnv-abc-02"]
	assert.Equal(t, "us-central1-c", second.zone)
	assert.Equal(t, "b9af7f04-4029-6839-6925-595c7d11aef6", second.labels["pool_uuid"])
}

func TestListInstances_EmptyProject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items": {}}`))
	}))
	defer srv.Close()

	ctx := context.Background()
	computeSvc, err := compute.NewService(
		ctx,
		option.WithEndpoint(srv.URL+"/"),
		option.WithHTTPClient(srv.Client()),
		option.WithoutAuthentication(),
	)
	assert.NoError(t, err)

	l := &gcpInstanceLister{svc: &fakeComputeServiceGetter{svc: computeSvc}}
	got, err := l.ListInstances(ctx, "p")
	assert.NoError(t, err)
	assert.Empty(t, got)
}
