package google

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	logger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/serviceconsumermanagement/v1"
	"google.golang.org/api/servicenetworking/v1"
	scopesHttp "google.golang.org/api/transport/http"
	"netapp.com/vsa/lifecycle-manager/pkg/log"
)

var (

	// serviceNetworkingEndpoint is the endpoint for the Service Networking API.
	serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "")
	// serviceConsumerManagementEndpoint is the endpoint for the Service Consumer Management API.
	serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "")
	// MockMetaDataHost is the endpoint for the metadata server.
	MockMetaDataHost = env.GetString("GCE_METADATA_HOST", "")

	newClient = _newClient

	newGoogleClient             = _newGoogleClient
	initializeManagementService = _initializeManagementService
	initializeNetworkingService = _initializeNetworkingService
	initializeComputeService    = _initializeComputeService
)

type GcpServices struct {
	Ctx    context.Context
	Logger logger.Logger
	Retry  RetryStrategy

	serviceConsumerManagementEndpoint string
	serviceNetworkingEndpoint         string

	AdminGCPService *AdminGCPService
}

type AdminGCPService struct {
	managementService *serviceconsumermanagement.APIService
	networkingService *servicenetworking.APIService
	computeService    *compute.Service
}

// _newClient redirects to third party library HTTP NewClient for networking, while it helps to mock the function for init_test
func _newClient(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
	return scopesHttp.NewClient(ctx, opts...)
}

// InitializeClients Initialize the nvc clients & admin clients
func (gcpService *GcpServices) InitializeClients() error {
	var adminServiceClient *AdminGCPService
	var err error
	gcpService.Logger.Debug("Initializing GCP clients")
	if !gcpService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Admin Client isn't initialised. Initialising now. Creating new GCP client")
		adminServiceClient, err = newGoogleClient(gcpService.Ctx)
		if err != nil {
			return err
		}
		gcpService.AdminGCPService = adminServiceClient
	}
	gcpService.Logger.Debug("Admin Client is initialised")
	return nil
}

// IsAdminClientInitialized checks the admin initialisation
func (gcpService *GcpServices) IsAdminClientInitialized() bool {
	return gcpService.AdminGCPService != nil
}

// GetLogger returns the logger instance for gcpService if exists, else creates a new one
func (gcpService *GcpServices) GetLogger() logger.Logger {
	if gcpService.Logger == nil {
		gcpService.Logger = util.GetLogger(gcpService.Ctx)
	}
	return gcpService.Logger
}

// _initializeAdminClient creates a new googleService object using Workload identity and Initializes the services
func _newGoogleClient(ctx context.Context) (*AdminGCPService, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("Calling initializeManagementService")
	managementService, err := initializeManagementService(ctx)
	if err != nil {
		log.Error("Error initializeManagementService", err)
		return nil, err
	}

	log.Debug("Calling initializeNetworkingService")
	networkingService, err := initializeNetworkingService(ctx)
	if err != nil {
		log.Error("Error initializeNetworkingService", err)
		return nil, err
	}

	log.Debug("Calling initializeComputeService")
	computeService, err := initializeComputeService(ctx)
	if err != nil {
		log.Error("Error initializeComputeService", err)
		return nil, err
	}

	gServices := AdminGCPService{
		networkingService: networkingService,
		managementService: managementService,
		computeService:    computeService,
	}
	return &gServices, nil
}

// _initializeManagementService initializes the service consumer management API service
func _initializeManagementService(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
	logger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(serviceconsumermanagement.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}

	logger.Debug(fmt.Sprintf("opts: %#v", opts))
	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", serviceconsumermanagement.CloudPlatformScope)))
	}
	logger.Debug("creating newClient")
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Error("error while creating new client for _initializeManagementService", err)
		return nil, err
	}
	client.Timeout = waitTimeoutMinutes
	svc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Error("serviceconsumermanagement.NewService error", err)
		return nil, err
	}
	if endpoint != "" {
		svc.BasePath = endpoint
	}
	return svc, nil
}

// _initializeNetworkingService initializes the service networking API service
func _initializeNetworkingService(ctx context.Context) (*servicenetworking.APIService, error) {
	logger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(servicenetworking.CloudPlatformScope, servicenetworking.ServiceManagementScope)
	opts := []option.ClientOption{scopesOption}
	logger.Debug(fmt.Sprintf("opts: %#v", opts))
	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", servicenetworking.CloudPlatformScope, servicenetworking.ServiceManagementScope)))
	}
	logger.Debug("creating newClient")
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Error("error while creating new client for _initializeNetworkingService", err)
		return nil, err
	}
	client.Timeout = defaultSleepTime
	svc, err := servicenetworking.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Error("servicenetworking.NewService error", err)
		return nil, err
	}
	if endpoint != "" {
		svc.BasePath = endpoint
	}
	return svc, nil
}

// _initializeComputeService initializes the compute API service in GCP
func _initializeComputeService(ctx context.Context) (*compute.Service, error) {
	logger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(compute.ComputeReadonlyScope, compute.ComputeScope, compute.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}
	logger.Debug(fmt.Sprintf("opts: %#v", opts))

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", compute.ComputeReadonlyScope, compute.ComputeScope, compute.CloudPlatformScope)))
	}
	logger.Debug("creating newClient")
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Error("error while creating new client for _initializeComputeService", err)
		return nil, err
	}
	client.Timeout = defaultSleepTime

	svc, err := compute.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Error("compute.NewService error", err)
		return nil, err
	}

	if endpoint != "" {
		svc.BasePath = endpoint
	}

	return svc, nil
}

// GetServiceNetworkingEndpoint returns the service consumer management endpoint
func (gcpService *GcpServices) GetServiceNetworkingEndpoint() string {
	if gcpService.serviceNetworkingEndpoint == "" {
		gcpService.serviceNetworkingEndpoint = serviceNetworkingEndpoint
	}
	gcpService.Logger.Debug("GetServiceNetworkingEndpoint : gcpService.serviceNetworkingEndpoint = ", gcpService.serviceNetworkingEndpoint)
	return gcpService.serviceNetworkingEndpoint
}

// GetServiceConsumerManagementEndpoint returns the service consumer management endpoint
func (gcpService *GcpServices) GetServiceConsumerManagementEndpoint() string {
	if gcpService.serviceConsumerManagementEndpoint == "" {
		gcpService.serviceConsumerManagementEndpoint = serviceConsumerManagementEndpoint
	}
	gcpService.Logger.Debug("GetServiceConsumerManagementEndpoint : gcpService.serviceConsumerManagementEndpoint = ", gcpService.serviceConsumerManagementEndpoint)
	return gcpService.serviceConsumerManagementEndpoint
}
