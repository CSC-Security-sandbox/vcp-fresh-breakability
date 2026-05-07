package leakedresources

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	googlehyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
)

func TestZoneFromScopeKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		key  string
		want string
	}{
		{"zones/us-central1-a", "us-central1-a"},
		{"zones/europe-west1-b", "europe-west1-b"},
		{"regions/us-central1", "regions/us-central1"},
		{"", ""},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, zoneFromScopeKey(tc.key), "key=%q", tc.key)
	}
}

func TestNewDiskLister_BuilderError(t *testing.T) {
	orig := buildGcpServiceForDiskLister
	t.Cleanup(func() { buildGcpServiceForDiskLister = orig })
	buildGcpServiceForDiskLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
		return nil, errors.New("init failed")
	}

	l, err := NewDiskLister(context.Background())
	assert.Error(t, err)
	assert.Nil(t, l)
}

func TestNewDiskLister_Success(t *testing.T) {
	orig := buildGcpServiceForDiskLister
	t.Cleanup(func() { buildGcpServiceForDiskLister = orig })
	buildGcpServiceForDiskLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
		return &googlehyperscaler.GcpServices{}, nil
	}

	l, err := NewDiskLister(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, l)
}

func TestListDisks_NilLister(t *testing.T) {
	var l *gcpDiskLister
	_, err := l.ListDisks(context.Background(), "p")
	assert.Error(t, err)
}

func TestListDisks_GetComputeServiceError(t *testing.T) {
	l := &gcpDiskLister{svc: &fakeComputeServiceGetter{err: errors.New("compute svc error")}}
	_, err := l.ListDisks(context.Background(), "p")
	assert.Error(t, err)
}

func TestListDisks_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"items": {
				"zones/us-central1-a": {
					"disks": [
						{
							"name": "gcnv-abc-01-disk-boot",
							"selfLink": "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/disks/gcnv-abc-01-disk-boot",
							"status": "READY",
							"sizeGb": "10",
							"type": "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/diskTypes/hyperdisk-balanced",
							"users": ["https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/instances/gcnv-abc-01"],
							"labels": {
								"pool_uuid": "b9af7f04-4029-6839-6925-595c7d11aef6",
								"deployment_id": "gcnv-abc"
							},
							"creationTimestamp": "2026-05-01T10:00:00Z"
						}
					]
				},
				"zones/us-central1-c": {
					"disks": [
						{
							"name": "gcnv-abc-02-disk-data-0",
							"status": "READY",
							"sizeGb": "154",
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

	l := &gcpDiskLister{svc: &fakeComputeServiceGetter{svc: computeSvc}}
	got, err := l.ListDisks(ctx, "p")
	assert.NoError(t, err)
	assert.Len(t, got, 2)

	byName := map[string]struct {
		zone   string
		status string
		sizeGB int64
		labels map[string]string
	}{}
	for _, d := range got {
		byName[d.Name] = struct {
			zone   string
			status string
			sizeGB int64
			labels map[string]string
		}{d.Zone, d.Status, d.SizeGB, d.Labels}
	}

	boot := byName["gcnv-abc-01-disk-boot"]
	assert.Equal(t, "us-central1-a", boot.zone)
	assert.Equal(t, "READY", boot.status)
	assert.Equal(t, int64(10), boot.sizeGB)
	assert.Equal(t, "b9af7f04-4029-6839-6925-595c7d11aef6", boot.labels["pool_uuid"])
	assert.Equal(t, "gcnv-abc", boot.labels["deployment_id"])

	data := byName["gcnv-abc-02-disk-data-0"]
	assert.Equal(t, "us-central1-c", data.zone)
	assert.Equal(t, int64(154), data.sizeGB)
}

func TestListDisks_EmptyProject(t *testing.T) {
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

	l := &gcpDiskLister{svc: &fakeComputeServiceGetter{svc: computeSvc}}
	got, err := l.ListDisks(ctx, "p")
	assert.NoError(t, err)
	assert.Empty(t, got)
}
