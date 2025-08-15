package google

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/privateca/v1"
	cloudrun "google.golang.org/api/run/v2"
	"google.golang.org/api/secretmanager/v1"
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

		initializeCloudProjectsService = func(ctx context.Context) (*cloudresourcemanager.Service, error) {
			return nil, errors.New("initializeCloudProjectsService failed")
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
		initializeCloudProjectsService = _initializeCloudProjectsService
	})
	t.Run("WhenInitializeIamServiceFails", func(t *testing.T) {
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
			return &storage.Client{}, nil
		}

		initializeIamService = func(ctx context.Context) (*iam.Service, error) {
			return nil, errors.New("initializeIamService failed")
		}

		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "initializeIamService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
		initializeStorageService = _initializeStorageService
		initializeIamService = _initializeIamService
	})
	t.Run("WhenInitializeCloudProjectsServiceFails", func(t *testing.T) {
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
			return &storage.Client{}, nil
		}

		initializeIamService = func(ctx context.Context) (*iam.Service, error) {
			return &iam.Service{
				BasePath: "",
			}, nil
		}

		initializeCloudProjectsService = func(ctx context.Context) (*cloudresourcemanager.Service, error) {
			return nil, errors.New("initializeCloudProjectsService failed")
		}

		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "initializeCloudProjectsService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
		initializeStorageService = _initializeStorageService
		initializeIamService = _initializeIamService
		initializeCloudProjectsService = _initializeCloudProjectsService
	})
	t.Run("initializePrivateCaServiceFails", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return &serviceconsumermanagement.APIService{
				BasePath: "",
			}, nil
		}
		initializeNetworkingService = func(ctx context.Context) (*servicenetworking.APIService, error) {
			return nil, nil
		}
		initializeComputeService = func(ctx context.Context) (*compute.Service, error) {
			return nil, nil
		}
		initializeStorageService = func(ctx context.Context) (*storage.Client, error) {
			return nil, nil
		}
		initializeIamService = func(ctx context.Context) (*iam.Service, error) {
			return &iam.Service{
				BasePath: "",
			}, nil
		}
		initializeCloudProjectsService = func(ctx context.Context) (*cloudresourcemanager.Service, error) { return nil, nil }
		initializePrivateCaService = func(ctx context.Context) (*privateca.Service, error) {
			return nil, fmt.Errorf("initializePrivateCaService failed")
		}
		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}

		if err.Error() != "initializePrivateCaService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
		initializeStorageService = _initializeStorageService
		initializeIamService = _initializeIamService
		initializeCloudProjectsService = _initializeCloudProjectsService
		initializePrivateCaService = _initializePrivateCaService
	})
	t.Run("initializeSecretManagerServiceFails", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return &serviceconsumermanagement.APIService{
				BasePath: "",
			}, nil
		}
		initializeNetworkingService = func(ctx context.Context) (*servicenetworking.APIService, error) {
			return nil, nil
		}
		initializeComputeService = func(ctx context.Context) (*compute.Service, error) {
			return nil, nil
		}
		initializeStorageService = func(ctx context.Context) (*storage.Client, error) {
			return nil, nil
		}
		initializeIamService = func(ctx context.Context) (*iam.Service, error) { return nil, nil }
		initializeCloudProjectsService = func(ctx context.Context) (*cloudresourcemanager.Service, error) { return nil, nil }
		initializePrivateCaService = func(ctx context.Context) (*privateca.Service, error) {
			return nil, nil
		}
		initializeSecretManagerService = func(ctx context.Context) (*secretmanager.Service, error) {
			return nil, errors.New("initializeSecretManagerService failed")
		}
		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "initializeSecretManagerService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
		initializeStorageService = _initializeStorageService
		initializeIamService = _initializeIamService
		initializeCloudProjectsService = _initializeCloudProjectsService
		initializePrivateCaService = _initializePrivateCaService
		initializeSecretManagerService = _initializeSecretManagerService
	})
	t.Run("initializeCloudDnsServiceFails", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return &serviceconsumermanagement.APIService{
				BasePath: "",
			}, nil
		}
		initializeNetworkingService = func(ctx context.Context) (*servicenetworking.APIService, error) {
			return nil, nil
		}
		initializeComputeService = func(ctx context.Context) (*compute.Service, error) {
			return nil, nil
		}
		initializeStorageService = func(ctx context.Context) (*storage.Client, error) {
			return nil, nil
		}
		initializeCloudProjectsService = func(ctx context.Context) (*cloudresourcemanager.Service, error) { return nil, nil }
		initializePrivateCaService = func(ctx context.Context) (*privateca.Service, error) {
			return nil, nil
		}
		initializeSecretManagerService = func(ctx context.Context) (*secretmanager.Service, error) { return nil, nil }
		initializeIamService = func(ctx context.Context) (*iam.Service, error) { return nil, nil }
		initializeCloudRunService = func(ctx context.Context) (*cloudrun.Service, error) { return nil, nil }
		initializeCloudDnsService = func(ctx context.Context) (*dns.Service, error) {
			return nil, errors.New("initializeCloudDnsService failed")
		}
		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "initializeCloudDnsService failed" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
		initializeStorageService = _initializeStorageService
		initializeCloudProjectsService = _initializeCloudProjectsService
		initializePrivateCaService = _initializePrivateCaService
		initializeSecretManagerService = _initializeSecretManagerService
		initializeCloudDnsService = _initializeCloudDnsService
		initializeIamService = _initializeIamService
		initializeCloudRunService = _initializeCloudRunService
	})
	t.Run("initializeCloudRunServiceFails", func(t *testing.T) {
		initializeManagementService = func(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
			return &serviceconsumermanagement.APIService{
				BasePath: "",
			}, nil
		}
		initializeNetworkingService = func(ctx context.Context) (*servicenetworking.APIService, error) {
			return nil, nil
		}
		initializeComputeService = func(ctx context.Context) (*compute.Service, error) {
			return nil, nil
		}
		initializeStorageService = func(ctx context.Context) (*storage.Client, error) {
			return nil, nil
		}
		initializeCloudProjectsService = func(ctx context.Context) (*cloudresourcemanager.Service, error) { return nil, nil }
		initializePrivateCaService = func(ctx context.Context) (*privateca.Service, error) {
			return nil, nil
		}
		initializeSecretManagerService = func(ctx context.Context) (*secretmanager.Service, error) { return nil, nil }
		initializeIamService = func(ctx context.Context) (*iam.Service, error) { return nil, nil }
		initializeCloudRunService = func(ctx context.Context) (*cloudrun.Service, error) {
			return nil, errors.New("error initializing CloudRun Service")
		}
		res, err := _newGoogleClient(context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}))
		if res != nil {
			t.Error("unexpected result returned")
		}
		if err == nil {
			t.Error("error was expected")
		}
		if err.Error() != "error initializing CloudRun Service" {
			t.Error("Incorrect error response")
		}
		initializeManagementService = _initializeManagementService
		initializeNetworkingService = _initializeNetworkingService
		initializeComputeService = _initializeComputeService
		initializeStorageService = _initializeStorageService
		initializeCloudProjectsService = _initializeCloudProjectsService
		initializePrivateCaService = _initializePrivateCaService
		initializeSecretManagerService = _initializeSecretManagerService
		initializeIamService = _initializeIamService
		initializeCloudRunService = _initializeCloudRunService
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
		initializePrivateCaService = func(ctx context.Context) (*privateca.Service, error) {
			return &privateca.Service{
				BasePath: "",
			}, nil
		}
		initializeSecretManagerService = func(ctx context.Context) (*secretmanager.Service, error) {
			return &secretmanager.Service{
				BasePath: "",
			}, nil
		}
		initializeCloudDnsService = func(ctx context.Context) (*dns.Service, error) {
			return &dns.Service{
				BasePath: "",
			}, nil
		}

		initializeCloudRunService = func(ctx context.Context) (*cloudrun.Service, error) {
			return &cloudrun.Service{
				BasePath: "",
			}, nil
		}

		initializeIamService = func(ctx context.Context) (*iam.Service, error) {
			return &iam.Service{
				BasePath: "",
			}, nil
		}

		initializeCloudProjectsService = func(ctx context.Context) (*cloudresourcemanager.Service, error) {
			return &cloudresourcemanager.Service{
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
		initializePrivateCaService = _initializePrivateCaService
		initializeSecretManagerService = _initializeSecretManagerService
		initializeStorageService = _initializeStorageService
		initializeIamService = _initializeIamService
		initializeCloudProjectsService = _initializeCloudProjectsService
		initializeCloudDnsService = _initializeCloudDnsService
		initializeCloudRunService = _initializeCloudRunService
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

func TestInitializeMockManagementService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
			VSAMockPath = env.GetString("GOOGLE_EMULATOR_PATH", "")
		}()
		MockMetaDataHost = "sample-server.com"
		VSAMockPath = "emulator-path"
		wi, err := initializeMockManagementService(context.Background())
		if err != nil {
			return
		}
		assert.Nil(t, err, "Unexpected error received")
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

func TestInitializeMockNetworkingService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
			VSAMockPath = env.GetString("GOOGLE_EMULATOR_PATH", "")
		}()
		MockMetaDataHost = "sample-server.com"
		VSAMockPath = "emulator-path"
		wi, err := initializeMockNetworkingService(context.Background())
		if err != nil {
			return
		}
		assert.Nil(t, err, "Unexpected error received")
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

func TestInitializeMockComputeService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
			VSAMockPath = env.GetString("GOOGLE_EMULATOR_PATH", "")
		}()
		MockMetaDataHost = "sample-server.com"
		VSAMockPath = "emulator-path"
		wi, err := initializeMockComputeService(context.Background())
		if err != nil {
			return
		}
		assert.Nil(t, err, "Unexpected error received")
		assert.NotNil(t, wi)
	})
}

func TestInitializePrivateCaService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, nil
		}
		wi, err := initializePrivateCaService(context.Background())
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
		wi, err := initializePrivateCaService(context.Background())
		if err != nil {
			return
		}
		assert.NotNil(t, err)
		assert.Equal(t, "client creation failed", err.Error())
		assert.NotNil(t, wi)
	})
}

func TestInitializeSecretManagerService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, nil
		}
		wi, err := initializeSecretManagerService(context.Background())
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
		wi, err := initializeSecretManagerService(context.Background())
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

func TestCreateBucketIfNotExists(t *testing.T) {
	tests := []struct {
		name           string
		statusCode     int
		expectError    bool
		expectedErrMsg string
	}{
		{
			name:        "Success",
			statusCode:  http.StatusOK,
			expectError: false,
		},
		{
			name:        "BucketAlreadyExists",
			statusCode:  http.StatusConflict,
			expectError: false,
		},
		{
			name:        "forbidden",
			statusCode:  http.StatusForbidden,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var requestCount int

			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				requestCount++

				if req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/b") {
					rw.Header().Set("Content-Type", "application/json")
					rw.WriteHeader(tc.statusCode)

					switch tc.statusCode {
					case http.StatusOK:
						_, _ = rw.Write([]byte(`{
							"name": "test-bucket",
							"location": "us-central1"
						}`))
					case http.StatusConflict:
						_, _ = rw.Write([]byte(`{
							"error": {
								"code": 409,
								"message": "Bucket already exists.",
								"errors": [{
									"message": "Bucket already exists.",
									"reason": "conflict"
								}]
							}
						}`))
					case http.StatusInternalServerError:
						// GCS-style structured error response
						_, _ = rw.Write([]byte(`{
							"error": {
								"code": 500,
								"message": "Internal Server Error",
								"errors": [{
									"message": "Internal Server Error",
									"reason": "backendError"
								}]
							}
						}`))
					}
					return
				}

				// fallback
				http.NotFound(rw, req)
			}))
			defer server.Close()

			ctx := context.Background()

			// Disable retries
			httpClient := &http.Client{
				Transport: &http.Transport{},
			}

			storageClient, err := storage.NewClient(ctx,
				option.WithEndpoint(server.URL+"/storage/v1/"),
				option.WithHTTPClient(httpClient),
				option.WithoutAuthentication(),
			)
			if err != nil {
				t.Fatalf("failed to create storage client: %v", err)
			}

			gcp := &GcpServices{
				Ctx: ctx,
				AdminGCPService: &AdminGCPService{
					storageService: storageClient,
				},
				Logger: util.GetLogger(ctx), // use nop logger
			}

			err = gcp.CreateBucketIfNotExists(ctx, "test-project", "test-bucket", "us-central1")

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error but got nil")
				} else if tc.expectedErrMsg != "" && !strings.Contains(err.Error(), tc.expectedErrMsg) {
					t.Errorf("expected error message to contain %q, got %v", tc.expectedErrMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got: %v", err)
				}
			}
		})
	}
}

func TestDeleteBucket(t *testing.T) {
	tests := []struct {
		name           string
		statusCode     int
		expectError    bool
		expectedErrMsg string
	}{
		{
			name:        "Success",
			statusCode:  http.StatusNoContent, // 204 No Content on successful delete
			expectError: false,
		},
		{
			name:        "BucketNotFound",
			statusCode:  http.StatusNotFound,
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodDelete && strings.Contains(req.URL.Path, "/b/") {
					rw.Header().Set("Content-Type", "application/json")
					rw.WriteHeader(tc.statusCode)

					if tc.statusCode == http.StatusInternalServerError {
						_, _ = rw.Write([]byte(`{
							"error": {
								"code": 500,
								"message": "Internal Server Error",
								"errors": [{
									"message": "Internal Server Error",
									"reason": "backendError"
								}]
							}
						}`))
					}
					return
				}
				http.NotFound(rw, req)
			}))
			defer server.Close()

			ctx := context.Background()

			httpClient := &http.Client{
				Transport: &http.Transport{},
			}

			storageClient, err := storage.NewClient(ctx,
				option.WithEndpoint(server.URL+"/storage/v1/"),
				option.WithHTTPClient(httpClient),
				option.WithoutAuthentication(),
			)
			if err != nil {
				t.Fatalf("failed to create storage client: %v", err)
			}

			gcp := &GcpServices{
				Ctx: ctx,
				AdminGCPService: &AdminGCPService{
					storageService: storageClient,
				},
				Logger: util.GetLogger(ctx),
			}

			err = gcp.DeleteBucket(ctx, "test-bucket")

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error but got nil")
				} else if !strings.Contains(err.Error(), tc.expectedErrMsg) {
					t.Errorf("expected error message to contain %q, got %v", tc.expectedErrMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got: %v", err)
				}
			}
		})
	}
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

func TestInitializeCloudProjectsService(t *testing.T) {
	t.Run("whenOk", func(t *testing.T) {
		defer func() {
			newClient = scopesHttp.NewClient
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		MockMetaDataHost = "sample-server.com"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{Timeout: time.Second}, MockMetaDataHost, nil
		}
		wi, err := initializeCloudProjectsService(context.Background())
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
		wi, err := initializeCloudProjectsService(context.Background())
		if err != nil {
			return
		}
		assert.NotNil(t, err)
		assert.Equal(t, "client creation failed", err.Error())
		assert.NotNil(t, wi)
	})
}

func Test_GetServiceAccount(t *testing.T) {
	projectName := "1079058383248"
	url := "/v1/projects/" + projectName + "/serviceAccounts"
	t.Run("WhenGetServiceAccountNotFound", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		resp := &iam.ServiceAccount{Email: "abc"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.GetServiceAccount(projectName, "abc")
		assert.Nil(tt, out)
		assert.NotNil(tt, err, "Expected error but got nil")
	})
	t.Run("WhenGetServiceAccountFound", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		r := &iam.ListServiceAccountsResponse{
			Accounts: []*iam.ServiceAccount{
				{
					Email: "abc@google.com",
				},
			},
		}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(r)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.GetServiceAccount(projectName, "abc@google.com")
		assert.NotNil(tt, out)
		assert.Nil(tt, err, "Expected no error but got one")
	})
	t.Run("WhenGetServiceAccountFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.GetServiceAccount(projectName, "abc@google.com")
		assert.Nil(tt, out)
		assert.NotNil(tt, err, "Expected no error but got one")
	})
}

func Test_IsServiceAccountCreated(t *testing.T) {
	projectName := "1079058383248"
	url := "/v1/projects/" + projectName + "/serviceAccounts"
	t.Run("WhenIsServiceAccountCreatedSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		resp := &iam.ServiceAccount{Email: "abc@google.com"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, isCreated, err := gService.IsServiceAccountCreated("abc.google.com")
		assert.Nil(tt, out)
		assert.NotNil(tt, err, "Expected error but got nil")
		assert.False(tt, isCreated, "Expected isCreated to be false")
	})
}

func TestCreateServiceAccount(t *testing.T) {
	t.Run("WhenCreateServiceAccountSuccess", func(tt *testing.T) {
		defer testReset(tt)
		url := "/v1/projects/test-proj/serviceAccounts"
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		resp := &models.ServiceAccount{Email: "abc@google.com"}
		createRequest := &models.CreateServiceAccountRequest{
			AccountId:      "abc",
			ServiceAccount: resp,
		}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.CreateServiceAccount(createRequest, "test-proj", "abc@google.com")
		assert.NotNil(tt, out)
		assert.Nil(tt, err)
	})
	t.Run("WhenCreateServiceAccountConflict", func(tt *testing.T) {
		defer testReset(tt)
		url := "/v1/projects/test-proj/serviceAccounts"
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		resp := &models.ServiceAccount{Email: "abc@google.com"}
		createRequest := &models.CreateServiceAccountRequest{
			AccountId:      "abc",
			ServiceAccount: resp,
		}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusConflict)
				return
			}
			rw.WriteHeader(http.StatusConflict)
		}))

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.CreateServiceAccount(createRequest, "test-proj", "abc@google.com")
		assert.Nil(tt, out)
		assert.NotNil(tt, err)
	})
}

func TestAddMissingRoles(t *testing.T) {
	t.Run("DoesNotAddExistingRoles", func(t *testing.T) {
		projectIAMPolicyBindings := []*cloudresourcemanager.Binding{
			{
				Role:    "roles/viewer",
				Members: []string{"serviceAccount:existing@example.com"},
			},
		}
		requiredRolesMap := map[string]bool{
			"roles/viewer": true,
		}
		currentSvcAccountMember := "serviceAccount:existing@example.com"

		gcpService := &GcpServices{}
		result := gcpService.addMissingRoles(projectIAMPolicyBindings, requiredRolesMap, currentSvcAccountMember)

		assert.Equal(t, 1, len(result))
		assert.Equal(t, "roles/viewer", result[0].Role)
		assert.Contains(t, result[0].Members, currentSvcAccountMember)
	})

	t.Run("AddsMissingRole", func(t *testing.T) {
		projectIAMPolicyBindings := []*cloudresourcemanager.Binding{}
		requiredRolesMap := map[string]bool{
			"roles/editor": false,
		}
		currentSvcAccountMember := "serviceAccount:new@example.com"

		gcpService := &GcpServices{}
		result := gcpService.addMissingRoles(projectIAMPolicyBindings, requiredRolesMap, currentSvcAccountMember)

		assert.Equal(t, 1, len(result))
		assert.Equal(t, "roles/editor", result[0].Role)
		assert.Contains(t, result[0].Members, currentSvcAccountMember)
	})

	t.Run("HandlesEmptyRolesMap", func(t *testing.T) {
		projectIAMPolicyBindings := []*cloudresourcemanager.Binding{}
		requiredRolesMap := map[string]bool{}
		currentSvcAccountMember := "serviceAccount:new@example.com"

		gcpService := &GcpServices{}
		result := gcpService.addMissingRoles(projectIAMPolicyBindings, requiredRolesMap, currentSvcAccountMember)

		assert.Equal(t, 0, len(result))
	})

	t.Run("CaseInsensitiveMemberCheck", func(t *testing.T) {
		projectIAMPolicyBindings := []*cloudresourcemanager.Binding{
			{
				Role:    "roles/editor",
				Members: []string{"serviceAccount:EXISTING@example.com"},
			},
		}
		requiredRolesMap := map[string]bool{
			"roles/editor": false,
		}
		currentSvcAccountMember := "serviceAccount:existing@example.com"

		gcpService := &GcpServices{}
		result := gcpService.addMissingRoles(projectIAMPolicyBindings, requiredRolesMap, currentSvcAccountMember)

		assert.Equal(t, 2, len(result)) // Expect 2 bindings
		roles := []string{result[0].Role, result[1].Role}
		assert.Contains(t, roles, "roles/editor")
		assert.Contains(t, result[0].Members, "serviceAccount:EXISTING@example.com")
	})

	t.Run("AddsMultipleMissingRoles", func(t *testing.T) {
		projectIAMPolicyBindings := []*cloudresourcemanager.Binding{}
		requiredRolesMap := map[string]bool{
			"roles/editor": false,
			"roles/viewer": false,
		}
		currentSvcAccountMember := "serviceAccount:new@example.com"

		gcpService := &GcpServices{}
		result := gcpService.addMissingRoles(projectIAMPolicyBindings, requiredRolesMap, currentSvcAccountMember)

		assert.Equal(t, 2, len(result))
		roles := []string{result[0].Role, result[1].Role}
		assert.Contains(t, roles, "roles/editor")
		assert.Contains(t, roles, "roles/viewer")
	})
}

func TestPolicyBindings(t *testing.T) {
	t.Run("WhenPolicyBindingsUpdatedWithExistingRole", func(tt *testing.T) {
		policyBindings := []*cloudresourcemanager.Binding{
			{
				Role:    "roles/editor",
				Members: []string{"serviceAccount:existing@example.com"},
			},
		}
		requiredRolesMap := map[string]bool{
			"roles/editor": false,
		}
		currentSvcAccountMember := "serviceAccount:existing@example.com"

		gcpService := &GcpServices{}
		updatedBindings := gcpService.updatePolicyBindings(policyBindings, requiredRolesMap, currentSvcAccountMember)

		assert.Equal(tt, 1, len(updatedBindings))
		assert.Equal(tt, "roles/editor", updatedBindings[0].Role)
		assert.Contains(tt, updatedBindings[0].Members, "serviceAccount:existing@example.com")
	})
	t.Run("WhenPolicyBindingsUpdatedWithCaseInsensitiveMemberCheck", func(tt *testing.T) {
		policyBindings := []*cloudresourcemanager.Binding{
			{
				Role:    "roles/editor",
				Members: []string{"serviceAccount:EXISTING@example.com"},
			},
		}
		requiredRolesMap := map[string]bool{
			"roles/editor": false,
		}
		currentSvcAccountMember := "serviceAccount:existing@example.com"

		gcpService := &GcpServices{}
		updatedBindings := gcpService.updatePolicyBindings(policyBindings, requiredRolesMap, currentSvcAccountMember)

		assert.Equal(tt, 1, len(updatedBindings))
		assert.Equal(tt, "roles/editor", updatedBindings[0].Role)
		assert.Contains(tt, updatedBindings[0].Members, "serviceAccount:EXISTING@example.com")
	})
	t.Run("WhenPolicyBindingsUpdatedWithEmptyRolesMap", func(t *testing.T) {
		policyBindings := []*cloudresourcemanager.Binding{
			{
				Role:    "roles/viewer",
				Members: []string{"serviceAccount:existing@example.com"},
			},
		}
		requiredRolesMap := map[string]bool{}
		currentSvcAccountMember := "serviceAccount:new@example.com"

		gcpService := &GcpServices{}
		updatedBindings := gcpService.updatePolicyBindings(policyBindings, requiredRolesMap, currentSvcAccountMember)

		assert.Equal(t, 1, len(updatedBindings))
		assert.Equal(t, "roles/viewer", updatedBindings[0].Role)
		assert.Contains(t, updatedBindings[0].Members, "serviceAccount:existing@example.com")
	})
}

func TestInitializeRequiredRolesMap(t *testing.T) {
	t.Run("WhenRequiredRolesMapInitializedWithValidRoles", func(t *testing.T) {
		roles := []string{"roles/viewer", "roles/editor"}
		gcpService := &GcpServices{}
		requiredRolesMap := gcpService.initializeRequiredRolesMap(roles)

		assert.Equal(t, 2, len(requiredRolesMap))
		assert.False(t, requiredRolesMap["roles/viewer"])
		assert.False(t, requiredRolesMap["roles/editor"])
	})
	t.Run("WhenRequiredRolesMapInitializedWithEmptyRoles", func(t *testing.T) {
		roles := []string{}
		gcpService := &GcpServices{}
		requiredRolesMap := gcpService.initializeRequiredRolesMap(roles)

		assert.Equal(t, 0, len(requiredRolesMap))
	})
	t.Run("WhenRequiredRolesMapHandlesDuplicateRoles", func(t *testing.T) {
		roles := []string{"roles/viewer", "roles/viewer", "roles/editor"}
		gcpService := &GcpServices{}
		requiredRolesMap := gcpService.initializeRequiredRolesMap(roles)

		assert.Equal(t, 2, len(requiredRolesMap))
		assert.False(t, requiredRolesMap["roles/viewer"])
		assert.False(t, requiredRolesMap["roles/editor"])
	})
}

func Test_GetProjectIamPolicyd(t *testing.T) {
	projectName := "1079058383248"
	url := "/v1/projects/" + projectName + ":getIamPolicy"
	t.Run("WhenGetProjectIamPolicySuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		resp := &cloudresourcemanager.Policy{
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role:    "roles/viewer",
					Members: []string{"serviceAccount:existing@example.com"},
				},
			},
		}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.getProjectIamPolicy(projectName)
		assert.NotNil(tt, out)
		assert.Nil(tt, err)
	})
	t.Run("WhenGetProjectIamPolicyFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.getProjectIamPolicy(projectName)
		assert.Nil(tt, out)
		assert.NotNil(tt, err)
	})
}

func Test_SetProjectIamPolicyd(t *testing.T) {
	projectName := "1079058383248"
	url := "/v1/projects/" + projectName + ":setIamPolicy"
	t.Run("WhenSetProjectIamPolicySuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		resp := &cloudresourcemanager.Policy{
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role:    "roles/viewer",
					Members: []string{"serviceAccount:existing@example.com"},
				},
			},
		}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}
		projectIAMPolicyBindings := []*cloudresourcemanager.Binding{
			{
				Role:    "roles/viewer",
				Members: []string{"serviceAccount:existing@example.com"},
			},
		}
		etag := "etag"

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		err = gService.setProjectIamPolicy(projectName, etag, projectIAMPolicyBindings)
		assert.Nil(tt, err)
	})
	t.Run("WhenSetProjectIamPolicyError", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}
		projectIAMPolicyBindings := []*cloudresourcemanager.Binding{
			{
				Role:    "roles/viewer",
				Members: []string{"serviceAccount:existing@example.com"},
			},
		}
		etag := "etag"

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		err = gService.setProjectIamPolicy(projectName, etag, projectIAMPolicyBindings)
		assert.NotNil(tt, err)
	})
}

func TestGcpServices_DeleteServiceAccount(t *testing.T) {
	tests := []struct {
		name         string
		statusCode   int
		responseBody string
		wantErr      bool
		errorType    int
	}{
		{
			name:       "service account not found",
			statusCode: http.StatusNotFound,
			wantErr:    false,
		},
		{
			name:       "delete fails with retriable error",
			statusCode: http.StatusTooManyRequests,
			wantErr:    true,
			errorType:  vsaerrors.ErrGCPServiceAccountDeletionError,
		},
		{
			name:       "delete fails with non-retriable error",
			statusCode: http.StatusForbidden,
			wantErr:    true,
			errorType:  vsaerrors.ErrGCPServiceAccountDeletionNonRetriableError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projectID := "test-project"
			projectNumber := "534737369447"
			saEmail := "test-sa@test-project.iam.gserviceaccount.com"

			// Mock both compute and IAM services
			urlPathCompute := "/projects/" + projectNumber
			urlPathIAM := "/v1/projects/" + projectID + "/serviceAccounts/" + saEmail

			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				if req.URL.Path == urlPathCompute {
					// Mock compute service response for project ID resolution
					project := map[string]interface{}{
						"name": projectID,
					}
					response, _ := json.Marshal(project)
					rw.Header().Set("Content-Type", "application/json")
					rw.WriteHeader(http.StatusOK)
					_, _ = rw.Write(response)
					return
				}
				if req.URL.Path == urlPathIAM {
					// Mock IAM service response
					rw.WriteHeader(tt.statusCode)
					if tt.responseBody != "" {
						_, _ = rw.Write([]byte(tt.responseBody))
					}
					return
				}
				rw.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()

			ctx := context.Background()

			// Create compute and IAM services with mock server
			computeSvc, err := compute.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
			assert.NoError(t, err)

			iamSvc, err := iam.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
			assert.NoError(t, err)

			gcp := &GcpServices{
				Ctx: ctx,
				AdminGCPService: &AdminGCPService{
					iamService:     iamSvc,
					computeService: computeSvc,
				},
			}

			err = gcp.DeleteServiceAccount(projectNumber, saEmail)
			if tt.wantErr {
				assert.Error(t, err)
				customErr := err.(*vsaerrors.CustomError)
				assert.Equal(t, tt.errorType, customErr.TrackingID)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
func TestInitializeCloudDnsService(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		origNewClient := newClient
		origMockMetaDataHost := MockMetaDataHost
		defer func() {
			newClient = origNewClient
			MockMetaDataHost = origMockMetaDataHost
		}()
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return &http.Client{}, "custom-endpoint", nil
		}
		client, err := _initializeCloudDnsService(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if client == nil {
			t.Fatal("expected client, got nil")
		}
	})

	t.Run("failure", func(t *testing.T) {
		origNewClient := newClient
		origMockMetaDataHost := MockMetaDataHost
		defer func() {
			newClient = origNewClient
			MockMetaDataHost = origMockMetaDataHost
		}()
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return nil, "", errors.New("fail")
		}
		client, err := _initializeCloudDnsService(context.Background())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if client != nil {
			t.Fatal("expected nil client, got non-nil")
		}
	})

	t.Run("with MockMetaDataHost", func(t *testing.T) {
		origNewClient := newClient
		origMockMetaDataHost := MockMetaDataHost
		defer func() {
			newClient = origNewClient
			MockMetaDataHost = origMockMetaDataHost
		}()
		MockMetaDataHost = "mock-host"
		newClient = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			if len(opts) == 0 {
				t.Error("Expected at least one option when MockMetaDataHost is set")
			}
			return &http.Client{}, "", nil
		}
		client, err := _initializeCloudDnsService(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if client == nil {
			t.Fatal("expected client, got nil")
		}
	})
}

func TestCreateHmacKey(t *testing.T) {
	t.Run("WhenCreateHmacKeySuccess", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/storage/v1/projects/project1/hmacKeys") {
				rw.Header().Set("Content-Type", "application/json")
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write([]byte(`{
                    "metadata": {
                        "accessId": "test-access-id",
                        "secret": "test-secret",
                        "timeCreated": "2025-06-24T14:38:52Z"
                    }
                }`))
				return
			}
			http.NotFound(rw, req)
		}))
		defer server.Close()

		ctx := context.Background()
		logger := util.GetLogger(ctx)

		storageClient, err := storage.NewClient(ctx,
			option.WithEndpoint(server.URL+"/storage/v1/"),
			option.WithHTTPClient(&http.Client{}),
			option.WithoutAuthentication(),
		)
		if err != nil {
			tt.Fatalf("failed to create storage client: %v", err)
		}

		gcp := &GcpServices{
			Ctx: ctx,
			AdminGCPService: &AdminGCPService{
				storageService: storageClient,
			},
			Logger: logger,
		}

		accessKey, secretKey, err := gcp.CreateHmacKey("project1", "serviceAccount1")
		assert.Nil(tt, err, "Expected no error but got one")
		assert.NotNil(tt, accessKey)
		assert.NotNil(tt, secretKey)
	})

	t.Run("WhenCreateHmacKeyFails", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		ctx := context.Background()
		logger := util.GetLogger(ctx)

		storageClient, err := storage.NewClient(ctx,
			option.WithEndpoint(server.URL+"/storage/v1/"),
			option.WithHTTPClient(&http.Client{}),
			option.WithoutAuthentication(),
		)
		if err != nil {
			tt.Fatalf("failed to create storage client: %v", err)
		}

		gcp := &GcpServices{
			Ctx: ctx,
			AdminGCPService: &AdminGCPService{
				storageService: storageClient,
			},
			Logger: logger,
		}

		accessKey, secretKey, err := gcp.CreateHmacKey("project1", "serviceAccount1")
		assert.NotNil(tt, err)
		assert.Nil(tt, accessKey)
		assert.Nil(tt, secretKey)
	})
}

func TestDeleteHmacKey(t *testing.T) {
	t.Run("WhenDeleteHmacKeySuccess", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPut && strings.Contains(req.URL.Path, "/storage/v1/projects/project1/hmacKeys/test-access-id") {
				rw.Header().Set("Content-Type", "application/json")
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write([]byte(`{
					"accessId": "test-access-id",
					"state": "INACTIVE",
					"timeCreated": "2025-06-24T14:38:52Z",
					"updated": "2025-06-24T14:38:52Z"
				}`))
				return
			}
			if req.Method == http.MethodDelete && strings.Contains(req.URL.Path, "/storage/v1/projects/project1/hmacKeys/test-access-id") {
				rw.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(rw, req)
		}))
		defer server.Close()

		ctx := context.Background()
		logger := util.GetLogger(ctx)

		storageClient, err := storage.NewClient(ctx,
			option.WithEndpoint(server.URL+"/storage/v1/"),
			option.WithHTTPClient(&http.Client{}),
			option.WithoutAuthentication(),
		)
		if err != nil {
			tt.Fatalf("failed to create storage client: %v", err)
		}

		gcp := &GcpServices{
			Ctx: ctx,
			AdminGCPService: &AdminGCPService{
				storageService: storageClient,
			},
			Logger: logger,
		}

		err = gcp.DeleteHmacKey("project1", "test-access-id", "serviceAccount1")
		assert.Nil(tt, err, "Expected no error but got one")
	})

	t.Run("WhenUpdateHmacKeyFails", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPatch && strings.Contains(req.URL.Path, "/storage/v1/projects/project1/hmacKeys/test-access-id") {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			http.NotFound(rw, req)
		}))
		defer server.Close()

		ctx := context.Background()
		logger := util.GetLogger(ctx)

		storageClient, err := storage.NewClient(ctx,
			option.WithEndpoint(server.URL+"/storage/v1/"),
			option.WithHTTPClient(&http.Client{}),
			option.WithoutAuthentication(),
		)
		if err != nil {
			tt.Fatalf("failed to create storage client: %v", err)
		}

		gcp := &GcpServices{
			Ctx: ctx,
			AdminGCPService: &AdminGCPService{
				storageService: storageClient,
			},
			Logger: logger,
		}

		err = gcp.DeleteHmacKey("project1", "test-access-id", "serviceAccount1")
		assert.NotNil(tt, err, "Expected an error but got none")
		assert.Contains(tt, err.Error(), "failed to update HMAC key state to INACTIVE", "Expected error message to match")
	})

	t.Run("WhenDeleteHmacKeyFails", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPatch && strings.Contains(req.URL.Path, "/storage/v1/projects/project1/hmacKeys/test-access-id") {
				rw.WriteHeader(http.StatusOK)
				return
			}
			if req.Method == http.MethodDelete && strings.Contains(req.URL.Path, "/storage/v1/projects/project1/hmacKeys/test-access-id") {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			http.NotFound(rw, req)
		}))
		defer server.Close()

		ctx := context.Background()
		logger := util.GetLogger(ctx)

		storageClient, err := storage.NewClient(ctx,
			option.WithEndpoint(server.URL+"/storage/v1/"),
			option.WithHTTPClient(&http.Client{}),
			option.WithoutAuthentication(),
		)
		if err != nil {
			tt.Fatalf("failed to create storage client: %v", err)
		}

		gcp := &GcpServices{
			Ctx: ctx,
			AdminGCPService: &AdminGCPService{
				storageService: storageClient,
			},
			Logger: logger,
		}

		err = gcp.DeleteHmacKey("project1", "test-access-id", "serviceAccount1")
		assert.NotNil(tt, err, "Expected an error but got none")
	})
}

func TestAttachOrUpdateRolesForServiceAccounts(t *testing.T) {
	projectID := "test-project"
	serviceAccountEmail := "test-sa@test-project.iam.gserviceaccount.com"

	t.Run("WhenAttachOrUpdateRolesSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock policy response for getIamPolicy
		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role:    "roles/viewer",
					Members: []string{"serviceAccount:existing@example.com"},
				},
			},
		}

		// Mock policy response for setIamPolicy
		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role:    "roles/viewer",
					Members: []string{"serviceAccount:existing@example.com"},
				},
				{
					Role:    "roles/editor",
					Members: []string{"serviceAccount:" + serviceAccountEmail},
				},
			},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/editor"}
		err = gService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})

	t.Run("WhenGetProjectIamPolicyFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/editor"}
		err = gService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.NotNil(tt, err)
		assert.ErrorContains(t, err.(*vsaerrors.CustomError).OriginalErr, "An internal error occurred.")
	})

	t.Run("WhenSetProjectIamPolicyFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag:     "test-etag",
			Bindings: []*cloudresourcemanager.Binding{},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/editor"}
		err = gService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.NotNil(tt, err)
		assert.ErrorContains(t, err.(*vsaerrors.CustomError).OriginalErr, "googleapi: got HTTP response code 500 with body: ")
	})

	t.Run("WhenServiceAccountAlreadyHasRoles", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role:    "roles/editor",
					Members: []string{"serviceAccount:" + serviceAccountEmail},
				},
			},
		}

		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role:    "roles/editor",
					Members: []string{"serviceAccount:" + serviceAccountEmail},
				},
			},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/editor"}
		err = gService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})

	t.Run("WhenMultipleRolesAssigned", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role:    "roles/viewer",
					Members: []string{"serviceAccount:other@example.com"},
				},
			},
		}

		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/editor", "roles/storage.admin", "roles/compute.admin"}
		err = gService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})

	t.Run("WhenEmptyRolesProvided", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag:     "test-etag",
			Bindings: []*cloudresourcemanager.Binding{},
		}

		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{}
		err = gService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})
}

func TestRemoveRolesFromServiceAccounts(t *testing.T) {
	projectID := "test-project"
	serviceAccountEmail := "test-sa@test-project.iam.gserviceaccount.com"

	t.Run("WhenRemoveRolesFromServiceAccountsSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock policy response for getIamPolicy
		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail,
						"serviceAccount:other@test.com",
					},
				},
				{
					Role: "roles/storage.objectAdmin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail,
					},
				},
				{
					Role: "roles/storage.viewer",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		// Mock policy response for setIamPolicy
		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
				{
					Role: "roles/storage.viewer",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/storage.admin", "roles/storage.objectAdmin"}
		err = gService.RemoveRolesFromServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})

	t.Run("WhenGetProjectIamPolicyFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/storage.admin"}
		err = gService.RemoveRolesFromServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.NotNil(tt, err)
		assert.ErrorContains(t, err.(*vsaerrors.CustomError).OriginalErr, "googleapi: got HTTP response code 500 with body: ")
	})

	t.Run("WhenSetProjectIamPolicyFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail,
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/storage.admin"}
		err = gService.RemoveRolesFromServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.NotNil(tt, err)
		assert.ErrorContains(t, err.(*vsaerrors.CustomError).OriginalErr, "googleapi: got HTTP response code 500 with body: ")
	})

	t.Run("WhenServiceAccountNotInRole", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/storage.admin"}
		err = gService.RemoveRolesFromServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})

	t.Run("WhenCaseInsensitiveMatching", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:test-sa@test-project.iam.gserviceaccount.com", // Lower case
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		// Use different case for service account email
		differentCaseEmail := "TEST-SA@TEST-PROJECT.IAM.GSERVICEACCOUNT.COM"
		roles := []string{"roles/storage.admin"}
		err = gService.RemoveRolesFromServiceAccounts(roles, differentCaseEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})

	t.Run("WhenEmptyRolesList", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail,
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail,
						"serviceAccount:other@test.com",
					},
				},
			},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{} // Empty roles list
		err = gService.RemoveRolesFromServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})

	t.Run("WhenRemoveAllMembersFromRole", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail, // Only member
					},
				},
			},
		}

		setPolicyResp := &cloudresourcemanager.Policy{
			Etag:     "new-etag",
			Bindings: []*cloudresourcemanager.Binding{}, // Empty bindings since role was completely removed
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/storage.admin"}
		err = gService.RemoveRolesFromServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})

	t.Run("WhenMultipleRolesWithMixedMembers", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		getPolicyResp := &cloudresourcemanager.Policy{
			Etag: "test-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail,
						"serviceAccount:other@test.com",
					},
				},
				{
					Role: "roles/storage.objectAdmin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail,
					},
				},
				{
					Role: "roles/storage.viewer",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
				{
					Role: "roles/compute.admin",
					Members: []string{
						"serviceAccount:" + serviceAccountEmail,
						"serviceAccount:another@test.com",
					},
				},
			},
		}

		setPolicyResp := &cloudresourcemanager.Policy{
			Etag: "new-etag",
			Bindings: []*cloudresourcemanager.Binding{
				{
					Role: "roles/storage.admin",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
				{
					Role: "roles/storage.viewer",
					Members: []string{
						"serviceAccount:other@test.com",
					},
				},
				{
					Role: "roles/compute.admin",
					Members: []string{
						"serviceAccount:another@test.com",
					},
				},
			},
		}

		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":getIamPolicy") {
				response, err := json.Marshal(getPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			if req.Method == http.MethodPost && strings.Contains(req.URL.Path, ":setIamPolicy") {
				response, err := json.Marshal(setPolicyResp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		pjSvc, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudProjectsService: pjSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		roles := []string{"roles/storage.admin", "roles/storage.objectAdmin", "roles/compute.admin"}
		err = gService.RemoveRolesFromServiceAccounts(roles, serviceAccountEmail, projectID)
		assert.Nil(tt, err)
		assert.Equal(tt, 2, callCount, "Expected 2 API calls (getIamPolicy and setIamPolicy)")
	})
}

func TestCreateServiceAccount_StatusConflict_ServiceAccountNotFound(t *testing.T) {
	t.Run("WhenCreateServiceAccountConflictAndServiceAccountNotFound", func(tt *testing.T) {
		defer testReset(tt)
		url := "/v1/projects/test-proj/serviceAccounts"
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock server that returns conflict on create, then not found on get
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == url {
				// First call to create returns conflict
				rw.WriteHeader(http.StatusConflict)
				return
			}
			if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/projects/-/serviceAccounts/") {
				// Second call to get service account returns not found
				rw.WriteHeader(http.StatusNotFound)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		createRequest := &models.CreateServiceAccountRequest{
			AccountId: "abc",
			ServiceAccount: &models.ServiceAccount{
				DisplayName: "test-account",
			},
		}

		out, err := gService.CreateServiceAccount(createRequest, "test-proj", "abc@google.com")
		assert.Nil(tt, out)
		assert.NotNil(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
	})
}

func TestCreateServiceAccount_StatusConflict_ServiceAccountFound(t *testing.T) {
	t.Run("WhenCreateServiceAccountConflictAndServiceAccountFound", func(tt *testing.T) {
		defer testReset(tt)
		url := "/v1/projects/test-proj/serviceAccounts"
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock server that returns conflict on create, then success on get
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == url {
				// First call to create returns conflict
				rw.WriteHeader(http.StatusConflict)
				return
			}
			if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/projects/-/serviceAccounts/") {
				// Second call to get service account returns success
				resp := &iam.ServiceAccount{
					Email: "abc@google.com",
					Name:  "projects/test-proj/serviceAccounts/abc@google.com",
				}
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		createRequest := &models.CreateServiceAccountRequest{
			AccountId: "abc",
			ServiceAccount: &models.ServiceAccount{
				DisplayName: "test-account",
			},
		}

		out, err := gService.CreateServiceAccount(createRequest, "test-proj", "abc@google.com")
		assert.NotNil(tt, out)
		assert.Nil(tt, err)
		assert.Equal(tt, "abc@google.com", out.Email)
	})
}

func TestCreateServiceAccount_NonConflictError(t *testing.T) {
	t.Run("WhenCreateServiceAccountNonConflictError", func(tt *testing.T) {
		defer testReset(tt)
		url := "/v1/projects/test-proj/serviceAccounts"
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock server that returns internal server error
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		createRequest := &models.CreateServiceAccountRequest{
			AccountId: "abc",
			ServiceAccount: &models.ServiceAccount{
				DisplayName: "test-account",
			},
		}

		out, err := gService.CreateServiceAccount(createRequest, "test-proj", "abc@google.com")
		assert.Nil(tt, out)
		assert.NotNil(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
	})
}

func TestCreateServiceAccount_Success(t *testing.T) {
	t.Run("WhenCreateServiceAccountSuccess", func(tt *testing.T) {
		defer testReset(tt)
		url := "/v1/projects/test-proj/serviceAccounts"
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock server that returns success
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == url {
				resp := &iam.ServiceAccount{
					Email: "abc@google.com",
					Name:  "projects/test-proj/serviceAccounts/abc@google.com",
				}
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		createRequest := &models.CreateServiceAccountRequest{
			AccountId: "abc",
			ServiceAccount: &models.ServiceAccount{
				DisplayName: "test-account",
			},
		}

		out, err := gService.CreateServiceAccount(createRequest, "test-proj", "abc@google.com")
		assert.NotNil(tt, out)
		assert.Nil(tt, err)
		assert.Equal(tt, "abc@google.com", out.Email)
	})
}

func TestGetServiceAccountByEmail(t *testing.T) {
	t.Run("WhenGetServiceAccountByEmailSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock server that returns success
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/projects/-/serviceAccounts/") {
				resp := &iam.ServiceAccount{
					Email: "abc@google.com",
					Name:  "projects/test-proj/serviceAccounts/abc@google.com",
				}
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		out, err := gService.GetServiceAccountByEmail("abc@google.com")
		assert.NotNil(tt, out)
		assert.Nil(tt, err)
		assert.Equal(tt, "abc@google.com", out.Email)
	})

	t.Run("WhenGetServiceAccountByEmailFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock server that returns error
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/projects/-/serviceAccounts/") {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		out, err := gService.GetServiceAccountByEmail("abc@google.com")
		assert.Nil(tt, out)
		assert.NotNil(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
	})
}

func TestConvertServiceAccountToHyperscalerServiceAccount(t *testing.T) {
	t.Run("WhenConvertServiceAccountToHyperscalerServiceAccount", func(tt *testing.T) {
		iamSA := &iam.ServiceAccount{
			Name:        "projects/test-proj/serviceAccounts/abc@google.com",
			Description: "Test Description",
			Email:       "abc@google.com",
			ProjectId:   "test-proj",
			UniqueId:    "123456789",
			Disabled:    false,
			DisplayName: "Test Account",
		}

		result := convertServiceAccountToHyperscalerServiceAccount(iamSA)

		assert.Equal(tt, iamSA.Name, result.Name)
		assert.Equal(tt, iamSA.Description, result.Description)
		assert.Equal(tt, iamSA.Email, result.Email)
		assert.Equal(tt, iamSA.ProjectId, result.ProjectId)
		assert.Equal(tt, iamSA.UniqueId, result.UniqueId)
		assert.Equal(tt, iamSA.Disabled, result.Disabled)
		assert.Equal(tt, iamSA.DisplayName, result.DisplayName)
	})
}

func TestConvertServiceAccounttoGcpServiceAccount(t *testing.T) {
	t.Run("WhenConvertServiceAccounttoGcpServiceAccount", func(tt *testing.T) {
		hyperscalerSA := &models.ServiceAccount{
			Name:        "projects/test-proj/serviceAccounts/abc@google.com",
			Description: "Test Description",
			Email:       "abc@google.com",
			ProjectId:   "test-proj",
			UniqueId:    "123456789",
			Disabled:    false,
			DisplayName: "Test Account",
		}

		result := convertServiceAccounttoGcpServiceAccount(hyperscalerSA)

		assert.Equal(tt, hyperscalerSA.Name, result.Name)
		assert.Equal(tt, hyperscalerSA.Description, result.Description)
		assert.Equal(tt, hyperscalerSA.Email, result.Email)
		assert.Equal(tt, hyperscalerSA.ProjectId, result.ProjectId)
		assert.Equal(tt, hyperscalerSA.UniqueId, result.UniqueId)
		assert.Equal(tt, hyperscalerSA.Disabled, result.Disabled)
		assert.Equal(tt, hyperscalerSA.DisplayName, result.DisplayName)
	})
}

func TestConvertCreateServiceAccountRequestToGcpCreateRequest(t *testing.T) {
	t.Run("WhenConvertCreateServiceAccountRequestToGcpCreateRequest", func(tt *testing.T) {
		createRequest := &models.CreateServiceAccountRequest{
			AccountId: "abc",
			ServiceAccount: &models.ServiceAccount{
				DisplayName: "Test Account",
			},
		}

		result := convertCreateServiceAccountRequestToGcpCreateRequest(createRequest)

		assert.Equal(tt, createRequest.AccountId, result.AccountId)
		assert.Equal(tt, createRequest.ServiceAccount.DisplayName, result.ServiceAccount.DisplayName)
	})
}
