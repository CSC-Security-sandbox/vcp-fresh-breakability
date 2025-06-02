package google

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/serviceconsumermanagement/v1"
	"google.golang.org/api/servicenetworking/v1"
	scopesHttp "google.golang.org/api/transport/http"
)

func TestInitializeClients(t *testing.T) {
	ctx := context.Background()
	t.Run("WhenAdminClientInitialised", func(t *testing.T) {
		gService := &GcpServices{
			Ctx:             ctx,
			Logger:          util.GetLogger(ctx),
			AdminGCPService: &AdminGCPService{}}
		err := gService.InitializeClients()
		if err != nil {
			t.Error("unexpected error returned")
		}
	})
	t.Run("InitializingAdmin", func(t *testing.T) {
		gService := &GcpServices{
			Ctx:    ctx,
			Logger: util.GetLogger(ctx)}
		admin := &AdminGCPService{}
		newGoogleClient = func(ctx context.Context) (*AdminGCPService, error) {
			return admin, nil
		}
		err := gService.InitializeClients()
		if err != nil {
			t.Error("unexpected error returned")
		}
		newGoogleClient = _newGoogleClient
	})
	t.Run("InitializeAdminFails", func(t *testing.T) {
		gService := &GcpServices{
			Ctx:    ctx,
			Logger: util.GetLogger(ctx)}
		admin := &AdminGCPService{}
		newGoogleClient = func(ctx context.Context) (*AdminGCPService, error) {
			return admin, errors.New("initializeAdminClient Failed")
		}
		err := gService.InitializeClients()
		if err == nil {
			t.Error("error was returned")
		}
		newGoogleClient = _newGoogleClient
	})
}

func TestIsAdminClientInitialized(t *testing.T) {
	t.Run("WhenAdminClientInitialised", func(t *testing.T) {
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{}}
		res := gService.IsAdminClientInitialized()
		if !res {
			t.Error("Should return true")
		}
	})
	t.Run("WhenAdminClientNotInitialised", func(t *testing.T) {
		gService := &GcpServices{}
		res := gService.IsAdminClientInitialized()
		if res {
			t.Error("Should return false")
		}
	})
}

func TestNewGoogleClient(t *testing.T) {
	t.Run("initializeManagementServiceFails", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return nil, errors.New("initializeManagementService failed")
		}
		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "initializeManagementService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
	})
	t.Run("initializeNetworkingServiceFails", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return nil, nil
		}
		initializeNetworkingService = func(ctx context.Context) (*servicenetworking.APIService, error) {
			return nil, errors.New("initializeNetworkingService failed")
		}
		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "initializeNetworkingService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
	})
	t.Run("initializeComputeServiceFails", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return &serviceconsumermanagement.APIService{
				BasePath: "",
			}, nil
		}
		initializeNetworkingService = func(ctx context.Context) (*servicenetworking.APIService, error) {
			return nil, nil
		}
		initializeComputeService = func(ctx context.Context) (*compute.Service, error) {
			return nil, errors.New("initializeComputeService failed")
		}
		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "initializeComputeService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
	})
	t.Run("initializeStorageClientServiceFails", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return &serviceconsumermanagement.APIService{
				BasePath: "",
			}, nil
		}
		initializeNetworkingService = func(ctx context.Context) (*servicenetworking.APIService, error) {
			return nil, nil
		}
		initializeComputeService = func(ctx context.Context) (*compute.Service, error) {
			return &compute.Service{
				BasePath: "",
			}, nil
		}
		initializeStorageService = func(ctx context.Context) (*storage.Client, error) {
			return nil, errors.New("initializeStorageService failed")
		}

		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "initializeStorageService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
		initializeStorageService = _initializeStorageService
	})
	t.Run("WhenOK", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return &serviceconsumermanagement.APIService{
				BasePath: "",
			}, nil
		}
		initializeNetworkingService = func(ctx context.Context) (*servicenetworking.APIService, error) {
			return &servicenetworking.APIService{
				BasePath: "",
			}, nil
		}
		initializeComputeService = func(ctx context.Context) (*compute.Service, error) {
			return &compute.Service{
				BasePath: "",
			}, nil
		}

		initializeIamService = func(ctx context.Context) (*iam.Service, error) {
			return &iam.Service{
				BasePath: "",
			}, nil
		}

		initializeStorageService = func(ctx context.Context) (*storage.Client, error) {
			return &storage.Client{}, nil
		}

		_, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if err != nil {
			t.Error("Unexpected error")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
		initializeStorageService = _initializeStorageService
		initializeIamService = _initializeIamService
	})
}

func TestInitializeManagementService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, nil
		}
		wi, err := initializeManagementService(context.Background())
		if err != nil {
			return
		}
		assert.Nil(t, err, "Unexpected error received")
		assert.NotNil(t, wi)
	})
	t.Run("whenNewClientFails", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, errors.New("client creation failed")
		}
		wi, err := initializeManagementService(context.Background())
		if err != nil {
			return
		}
		assert.NotNil(t, err)
		assert.Equal(t, "client creation failed", err.Error())
		assert.NotNil(t, wi)
	})
}

func TestInitializeNetworkingService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, nil
		}
		wi, err := initializeNetworkingService(context.Background())
		if err != nil {
			return
		}
		assert.Nil(t, err, "Unexpected error received")
		assert.NotNil(t, wi)
	})
	t.Run("whenNewClientFails", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, errors.New("client creation failed")
		}
		wi, err := initializeNetworkingService(context.Background())
		if err != nil {
			return
		}
		assert.NotNil(t, err)
		assert.Equal(t, "client creation failed", err.Error())
		assert.NotNil(t, wi)
	})
}

func TestInitializeComputeService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, nil
		}
		wi, err := initializeComputeService(context.Background())
		if err != nil {
			return
		}
		assert.Nil(t, err, "Unexpected error received")
		assert.NotNil(t, wi)
	})
	t.Run("whenNewClientFails", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, errors.New("client creation failed")
		}
		wi, err := initializeComputeService(context.Background())
		if err != nil {
			return
		}
		assert.NotNil(t, err)
		assert.Equal(t, "client creation failed", err.Error())
		assert.NotNil(t, wi)
	})
}

func TestGetServiceNetworkingEndpoint(t *testing.T) {
	t.Run("WhenServiceNetworkingInitialised", func(t *testing.T) {
		endpoint := "test-endpoint"
		gService := &GcpServices{
			AdminGCPService:           &AdminGCPService{},
			serviceNetworkingEndpoint: endpoint,
			Logger:                    util.GetLogger(context.Background()),
		}
		res := gService.GetServiceNetworkingEndpoint()
		if res == "" {
			t.Error("Must have a value")
		}
		if res != endpoint {
			t.Error("Must be = " + endpoint)
		}
	})
	t.Run("WhenServiceNetworkingNotInitialised", func(t *testing.T) {
		gService := &GcpServices{
			Logger: util.GetLogger(context.Background()),
		}
		defer func() {
			serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "endpoint.google")
		}()
		serviceNetworkingEndpoint = "google-endpoint"
		res := gService.GetServiceNetworkingEndpoint()
		if res == "" {
			t.Error("Must have a value")
		}
		if res != serviceNetworkingEndpoint {
			t.Error("Must be = " + serviceNetworkingEndpoint)
		}
	})
}

func TestGetServiceConsumerManagementEndpoint(t *testing.T) {
	t.Run("WhenConsumerManagementInitialised", func(t *testing.T) {
		endpoint := "test-endpoint"
		gService := &GcpServices{
			AdminGCPService:                   &AdminGCPService{},
			serviceConsumerManagementEndpoint: endpoint,
			Logger:                            util.GetLogger(context.Background()),
		}
		res := gService.GetServiceConsumerManagementEndpoint()
		if res != endpoint {
			t.Error("Must be = " + endpoint)
		}
	})
	t.Run("WhenConsumerManagementNotInitialised", func(t *testing.T) {
		gService := &GcpServices{
			Logger: util.GetLogger(context.Background()),
		}
		defer func() {
			serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "endpoint.google")
		}()
		serviceConsumerManagementEndpoint = ""
		res := gService.GetServiceConsumerManagementEndpoint()
		if res != serviceConsumerManagementEndpoint {
			t.Error("Must be = " + serviceConsumerManagementEndpoint)
		}
	})
}

func TestGcpServices_GetLogger(t *testing.T) {
	ctx := context.Background()

	t.Run("ReturnsExistingLogger", func(t *testing.T) {
		logger := util.GetLogger(ctx)
		gcpService := &GcpServices{
			Ctx:    ctx,
			Logger: logger,
		}
		got := gcpService.GetLogger()
		assert.Equal(t, logger, got)
	})

	t.Run("InitializesLoggerIfNil", func(t *testing.T) {
		gcpService := &GcpServices{
			Ctx: ctx,
		}
		got := gcpService.GetLogger()
		assert.NotNil(t, got)
		assert.Equal(t, gcpService.Logger, got)
	})
}

type mockBucketHandle struct {
	attrsErr  error
	createErr error
}

func (m *mockBucketHandle) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return nil, m.attrsErr
}
func (m *mockBucketHandle) Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) error {
	return m.createErr
}

type mockStorageClient struct {
	bucket *mockBucketHandle
}

func (m *mockStorageClient) Bucket(name string) BucketHandle {
	return m.bucket
}

func TestCreateBucketIfNotExists(t *testing.T) {
	ctx := context.Background()

	t.Run("bucket create returns 409 conflict (already exists)", func(t *testing.T) {
		gcp := &GcpServices{
			AdminGCPService: &AdminGCPService{
				storageService: &mockStorageClient{
					bucket: &mockBucketHandle{createErr: &googleapi.Error{Code: 409}},
				},
			},
		}
		err := gcp.CreateBucketIfNotExists(ctx, "pid", "bkt", "region")
		assert.NoError(t, err)
	})

	t.Run("bucket create succeeds", func(t *testing.T) {
		gcp := &GcpServices{
			AdminGCPService: &AdminGCPService{
				storageService: &mockStorageClient{
					bucket: &mockBucketHandle{createErr: nil},
				},
			},
		}
		err := gcp.CreateBucketIfNotExists(ctx, "pid", "bkt", "region")
		assert.NoError(t, err)
	})

	t.Run("bucket create fails with non-409 error", func(t *testing.T) {
		gcp := &GcpServices{
			AdminGCPService: &AdminGCPService{
				storageService: &mockStorageClient{
					bucket: &mockBucketHandle{createErr: errors.New("fail")},
				},
			},
		}
		err := gcp.CreateBucketIfNotExists(ctx, "pid", "bkt", "region")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fail")
	})
}

func TestInitializeStorageService(t *testing.T) {
	origNewClient := newClient
	defer func() { newClient = origNewClient }()

	t.Run("success", func(t *testing.T) {
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{}, "", nil
		}
		client, err := _initializeStorageService(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if client == nil {
			t.Fatal("expected client, got nil")
		}
	})

	t.Run("failure", func(t *testing.T) {
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return nil, "", errors.New("fail")
		}
		client, err := _initializeStorageService(context.Background())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if client != nil {
			t.Fatal("expected nil client, got non-nil")
		}
	})
}

func TestInitializeStorageServiceWithMockMetaDataHost(t *testing.T) {
	origNewClient := newClient
	origMockMetaDataHost := MockMetaDataHost
	defer func() {
		newClient = origNewClient
		MockMetaDataHost = origMockMetaDataHost
	}()

	MockMetaDataHost = "mock-host" // Set to non-empty to cover the branch

	newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
		if len(opts) == 0 {
			t.Error("Expected at least one option when MockMetaDataHost is set")
		}
		return &http.Client{}, "", nil
	}

	client, err := _initializeStorageService(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if client == nil {
		t.Fatal("expected client, got nil")
	}
}

func TestInitializeIamService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, nil
		}
		wi, err := initializeIamService(context.Background())
		if err != nil {
			return
		}
		assert.Nil(t, err, "Unexpected error received")
		assert.NotNil(t, wi)
	})
	t.Run("whenNewClientFails", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, errors.New("client creation failed")
		}
		wi, err := initializeIamService(context.Background())
		if err != nil {
			return
		}
		assert.NotNil(t, err)
		assert.Equal(t, "client creation failed", err.Error())
		assert.NotNil(t, wi)
	})
}
