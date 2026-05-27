package leakedresources

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	googlehyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
)

func TestSubnetworkBaseName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		url  string
		want string
	}{
		{"", ""},
		{"https://www.googleapis.com/compute/v1/projects/p/regions/r/subnetworks/sn-1", "sn-1"},
		{"projects/p/regions/r/subnetworks/my-subnet", "my-subnet"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, SubnetworkBaseName(tc.url), "url=%q", tc.url)
	}
}

type fakeComputeServiceGetter struct {
	svc *compute.Service
	err error
}

func (f *fakeComputeServiceGetter) GetComputeService(ctx context.Context) (*compute.Service, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.svc, nil
}

func TestNewRegionalAddressLister_LocalEnv(t *testing.T) {
	origGetEnvString := getEnvString
	t.Cleanup(func() { getEnvString = origGetEnvString })
	getEnvString = func(key, defaultValue string) string {
		if key == "ENV" {
			return "local"
		}
		return defaultValue
	}

	l, err := NewRegionalAddressLister(context.Background())
	assert.Error(t, err)
	assert.Nil(t, l)
}

func TestNewRegionalAddressLister_BuilderError(t *testing.T) {
	origGetEnvString := getEnvString
	origBuild := buildGcpServiceForRegionalAddressLister
	t.Cleanup(func() {
		getEnvString = origGetEnvString
		buildGcpServiceForRegionalAddressLister = origBuild
	})
	getEnvString = func(key, defaultValue string) string { return defaultValue }
	buildGcpServiceForRegionalAddressLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
		return nil, errors.New("init failed")
	}

	l, err := NewRegionalAddressLister(context.Background())
	assert.Error(t, err)
	assert.Nil(t, l)
}

func TestNewRegionalAddressLister_Success(t *testing.T) {
	origGetEnvString := getEnvString
	origBuild := buildGcpServiceForRegionalAddressLister
	t.Cleanup(func() {
		getEnvString = origGetEnvString
		buildGcpServiceForRegionalAddressLister = origBuild
	})
	getEnvString = func(key, defaultValue string) string { return defaultValue }
	buildGcpServiceForRegionalAddressLister = func(ctx context.Context) (*googlehyperscaler.GcpServices, error) {
		return &googlehyperscaler.GcpServices{}, nil
	}

	l, err := NewRegionalAddressLister(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, l)
}

func TestListRegionalAddresses_NilLister(t *testing.T) {
	var l *gcpRegionalAddressLister
	_, err := l.ListRegionalAddresses(context.Background(), "p", "r")
	assert.Error(t, err)
}

func TestListRegionalAddresses_GetComputeServiceError(t *testing.T) {
	l := &gcpRegionalAddressLister{svc: &fakeComputeServiceGetter{err: errors.New("compute svc error")}}
	_, err := l.ListRegionalAddresses(context.Background(), "p", "r")
	assert.Error(t, err)
}

func TestListRegionalAddresses_SuccessAndTimestampParsing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"items": [
				{
					"name": "a1",
					"address": "10.0.0.1",
					"subnetwork": "https://www.googleapis.com/compute/v1/projects/p/regions/r/subnetworks/sn-1",
					"addressType": "INTERNAL",
					"status": "RESERVED",
					"creationTimestamp": "2026-03-25T10:00:00Z"
				},
				{
					"name": "a2",
					"address": "10.0.0.2",
					"selfLink": "",
					"subnetwork": "https://www.googleapis.com/compute/v1/projects/p/regions/r/subnetworks/sn-2",
					"addressType": "INTERNAL",
					"status": "RESERVED",
					"creationTimestamp": "2026-03-25T10:00:00.000+00:00"
				},
				null
			]
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

	l := &gcpRegionalAddressLister{svc: &fakeComputeServiceGetter{svc: computeSvc}}
	got, err := l.ListRegionalAddresses(ctx, "p", "r")
	assert.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, "a1", got[0].Name)
	assert.True(t, got[0].CreationTimeParsed)
	assert.Equal(t, "a2", got[1].Name)
	assert.True(t, got[1].CreationTimeParsed)
	assert.NotEmpty(t, got[1].ResourceName)
	assert.WithinDuration(t, time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC), got[0].CreationTime.UTC(), time.Second)
}
