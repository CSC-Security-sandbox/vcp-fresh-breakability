package google

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	logger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	retryutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/retry"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/cloudkms/v1"
	projectsManagement "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/privateca/v1"
	cloudrun "google.golang.org/api/run/v2"
	"google.golang.org/api/secretmanager/v1"
	"google.golang.org/api/serviceconsumermanagement/v1"
	"google.golang.org/api/servicenetworking/v1"
	storagev1 "google.golang.org/api/storage/v1"
	scopesHttp "google.golang.org/api/transport/http"
)

const INACTIVE = "INACTIVE"

func init() {
	// Override initialization functions if mock path is set
	if VSAMockPath != "" {
		initializeManagementService = _initializeMockManagementService
		initializeNetworkingService = _initializeMockNetworkingService
		initializeComputeService = _initializeMockComputeService
	}
}

var (
	// serviceNetworkingEndpoint is the endpoint for the Service Networking API.
	serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "")
	// serviceConsumerManagementEndpoint is the endpoint for the Service Consumer Management API.
	serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "")
	// MockMetaDataHost is the endpoint for the metadata server.
	MockMetaDataHost = env.GetString("GCE_METADATA_HOST", "")
	VSAMockPath      = env.GetString("GOOGLE_EMULATOR_PATH", "")

	newClient       = _newClient
	newClientScopes = scopesHttp.NewClient

	newGoogleClient                 = _newGoogleClient
	initializeManagementService     = _initializeManagementService
	initializeNetworkingService     = _initializeNetworkingService
	initializeComputeService        = _initializeComputeService
	initializeStorageService        = _initializeStorageService
	initializeIamService            = _initializeIamService
	initializeCloudProjectsService  = _initializeCloudProjectsService
	initializePrivateCaService      = _initializePrivateCaService
	initializeSecretManagerService  = _initializeSecretManagerService
	initializeCloudDnsService       = _initializeCloudDnsService
	initializeStorageV1Service      = _initializeStorageV1Service
	initializeCloudRunService       = _initializeCloudRunService
	initializeMockManagementService = _initializeMockManagementService
	initializeMockNetworkingService = _initializeMockNetworkingService
	initializeMockComputeService    = _initializeMockComputeService
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
	managementService    *serviceconsumermanagement.APIService
	networkingService    *servicenetworking.APIService
	computeService       *compute.Service
	storageService       *storage.Client
	storageV1Service     *storagev1.Service
	iamService           *iam.Service
	privateCaService     *privateca.Service
	secretManagerService *secretmanager.Service
	cloudProjectsService *projectsManagement.Service
	cloudDnsService      *dns.Service
	cloudRunService      *cloudrun.Service
}

// _newClient redirects to third party library HTTP NewClient for networking, while it helps to mock the function for init_test
func _newClient(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
	return newClientScopes(ctx, opts...)
}

// InitializeClients Initialize the nvc clients & admin clients
func (gcpService *GcpServices) InitializeClients() error {
	var adminServiceClient *AdminGCPService
	var err error
	if !gcpService.IsAdminClientInitialized() {
		adminServiceClient, err = newGoogleClient(gcpService.Ctx)
		if err != nil {
			return err
		}
		gcpService.AdminGCPService = adminServiceClient
	}
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

// GetLogger returns the logger instance for gcpService if exists, else creates a new one
func (gcpService *GcpServices) GetContext() context.Context {
	if gcpService.Ctx == nil {
		gcpService.Ctx = context.Background()
	}
	return gcpService.Ctx
}

// _initializeAdminClient creates a new googleService object using Workload identity and Initializes the services
func _newGoogleClient(ctx context.Context) (*AdminGCPService, error) {
	log := util.GetLogger(ctx)
	managementService, err := initializeManagementService(ctx)
	if err != nil {
		log.Errorf("Error initializeManagementService : %s", err.Error())
		return nil, err
	}

	networkingService, err := initializeNetworkingService(ctx)
	if err != nil {
		log.Errorf("Error initializeNetworkingService : %s", err.Error())
		return nil, err
	}

	computeService, err := initializeComputeService(ctx)
	if err != nil {
		log.Errorf("Error initializeComputeService : %s", err.Error())
		return nil, err
	}

	storageService, err := initializeStorageService(ctx)
	if err != nil {
		log.Errorf("Error initializeStorageService :%s", err.Error())
		return nil, err
	}

	iamService, err := initializeIamService(ctx)
	if err != nil {
		log.Errorf("Error initializeIamService :%s", err.Error())
		return nil, err
	}

	cloudProjectService, err := initializeCloudProjectsService(ctx)
	if err != nil {
		log.Error("Error initializeCloudProjectsService", err)
		return nil, err
	}

	privateCaService, err := initializePrivateCaService(ctx)
	if err != nil {
		log.Errorf("Error initializePrivateCaService :%s", err.Error())
		return nil, err
	}

	secretManagerService, err := initializeSecretManagerService(ctx)
	if err != nil {
		log.Errorf("Error initializeSecretManagerService :%s", err.Error())
		return nil, err
	}

	cloudRunService, err := initializeCloudRunService(ctx)
	if err != nil {
		log.Errorf("error initializing CloudRun Service: %s", err.Error())
		return nil, err
	}

	cloudDnsService, err := initializeCloudDnsService(ctx)
	if err != nil {
		log.Errorf("Error initializeCloudDnsService :%s", err.Error())
		return nil, err
	}

	storageV1Service, err := initializeStorageV1Service(ctx)
	if err != nil {
		log.Errorf("Error initializeStorageV1Service :%s", err.Error())
		return nil, err
	}

	gServices := AdminGCPService{
		networkingService:    networkingService,
		managementService:    managementService,
		computeService:       computeService,
		storageService:       storageService,
		storageV1Service:     storageV1Service,
		iamService:           iamService,
		secretManagerService: secretManagerService,
		cloudProjectsService: cloudProjectService,
		cloudRunService:      cloudRunService,
		privateCaService:     privateCaService,
		cloudDnsService:      cloudDnsService,
	}
	return &gServices, nil
}

// _initializeManagementService initializes the service consumer management API service
func _initializeManagementService(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
	logger := util.GetLogger(ctx)

	scopesOption := option.WithScopes(serviceconsumermanagement.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", serviceconsumermanagement.CloudPlatformScope)))
	}
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Errorf("error while creating new client for _initializeManagementService : %v", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = waitTimeoutMinutes
	svc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Errorf("serviceconsumermanagement.NewService error : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	if endpoint != "" {
		svc.BasePath = endpoint
	}
	return svc, nil
}

func _initializeMockManagementService(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
	logger := util.GetLogger(ctx)
	client := &http.Client{Timeout: time.Second * 3}
	logger.Info("#1 Using mock path for serviceconsumermanagement API: ", VSAMockPath)
	// default path -> https://serviceconsumermanagement.googleapis.com/
	svc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	logger.Info("#2 Using mock path for serviceconsumermanagement API: ", VSAMockPath)
	svc.BasePath = fmt.Sprintf("http://%s/", VSAMockPath)
	return svc, nil
}

// _initializeNetworkingService initializes the service networking API service
func _initializeNetworkingService(ctx context.Context) (*servicenetworking.APIService, error) {
	logger := util.GetLogger(ctx)

	scopesOption := option.WithScopes(servicenetworking.CloudPlatformScope, servicenetworking.ServiceManagementScope)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", servicenetworking.CloudPlatformScope, servicenetworking.ServiceManagementScope)))
	}
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Errorf("error while creating new client for _initializeNetworkingService : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = waitTimeoutMinutes
	svc, err := servicenetworking.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Errorf("servicenetworking.NewService error : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	if endpoint != "" {
		svc.BasePath = endpoint
	}
	return svc, nil
}

func _initializeMockNetworkingService(ctx context.Context) (*servicenetworking.APIService, error) {
	logger := util.GetLogger(ctx)

	client := &http.Client{Timeout: time.Second * 3}
	logger.Info("#1 Using mock path for servicenetworking API: ", VSAMockPath)
	svc, err := servicenetworking.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	logger.Info("#2 Using mock path for servicenetworking API: ", VSAMockPath)
	svc.BasePath = fmt.Sprintf("http://%s/", VSAMockPath)
	return svc, nil
}

// _initializeIamService initializes the IAM API service
func _initializeIamService(ctx context.Context) (*iam.Service, error) {
	slogger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(iam.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", iam.CloudPlatformScope)))
	}
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		slogger.Error("error while creating new client for _initializeIamService", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = defaultSleepTime

	svc, err := iam.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		slogger.Error("compute.NewService error", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}

	if endpoint != "" {
		svc.BasePath = endpoint
	}

	return svc, nil
}

func _initializeCloudProjectsService(ctx context.Context) (*projectsManagement.Service, error) {
	scopesOption := option.WithScopes(projectsManagement.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}
	slogger := util.GetLogger(ctx)

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", projectsManagement.CloudPlatformScope)))
	}

	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		slogger.Error("error while creating new client for _initializeCloudProjectsService", err)
		return nil, err
	}
	client.Timeout = waitTimeoutMinutes
	svc, err := projectsManagement.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		slogger.Error("projectsManagement.NewService error", err)
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

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", compute.ComputeReadonlyScope, compute.ComputeScope, compute.CloudPlatformScope)))
	}
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Errorf("error while creating new client for _initializeComputeService : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = waitTimeoutMinutes

	svc, err := compute.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Errorf("compute.NewService error : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}

	if endpoint != "" {
		svc.BasePath = endpoint
	}

	return svc, nil
}

func _initializeMockComputeService(ctx context.Context) (*compute.Service, error) {
	logger := util.GetLogger(ctx)
	client := &http.Client{Timeout: time.Second * 3}
	logger.Info("#1 Using mock path for compute API: ", VSAMockPath)
	// default path -> https://serviceconsumermanagement.googleapis.com/
	svc, err := compute.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	logger.Info("#2 Using mock path for compute API: ", VSAMockPath)
	svc.BasePath = fmt.Sprintf("http://%s/", VSAMockPath)
	return svc, nil
}

// _initializePrivateCaService initializes the private CA API service in GCP
func _initializePrivateCaService(ctx context.Context) (*privateca.Service, error) {
	logger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(privateca.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", privateca.CloudPlatformScope)))
	}
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Errorf("error while creating new client for _initializePrivateCaService : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = waitTimeoutMinutes

	svc, err := privateca.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Errorf("privateca.NewService error : %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}

	if endpoint != "" {
		svc.BasePath = endpoint
	}

	return svc, nil
}

// _initializeSecretManagerService initializes the Secret Manager API service in GCP
func _initializeSecretManagerService(ctx context.Context) (*secretmanager.Service, error) {
	logger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(secretmanager.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", secretmanager.CloudPlatformScope)))
	}
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Errorf("error while creating new client for _initializeSecretManagerService : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = waitTimeoutMinutes

	svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Errorf("secretmanager.NewService error : %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}

	if endpoint != "" {
		svc.BasePath = endpoint
	}
	return svc, nil
}

// _initializeCloudDnsService initializes the Cloud DNS API service in GCP
func _initializeCloudDnsService(ctx context.Context) (*dns.Service, error) {
	logger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(dns.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", dns.CloudPlatformScope)))
	}
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Errorf("error while creating new client for _initializeCloudDnsService : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = waitTimeoutMinutes

	svc, err := dns.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Errorf("dns.NewService error : %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}

	if endpoint != "" {
		svc.BasePath = endpoint
	}
	return svc, nil
}

// _initializeStorageV1Service initializes the Storage v1 API service in GCP
func _initializeStorageV1Service(ctx context.Context) (*storagev1.Service, error) {
	logger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(storagev1.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", storagev1.CloudPlatformScope)))
	}
	client, endpoint, err := newClient(ctx, opts...)
	if err != nil {
		logger.Errorf("error while creating new client for _initializeStorageV1Service : %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = waitTimeoutMinutes

	svc, err := storagev1.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Errorf("storagev1.NewService error : %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}

	if endpoint != "" {
		svc.BasePath = endpoint
	}
	return svc, nil
}

func _initializeStorageService(ctx context.Context) (*storage.Client, error) {
	scopesOption := option.WithScopes(storage.ScopeFullControl)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(
			google.ComputeTokenSource("", storage.ScopeFullControl),
		))
	}

	client, _, err := newClient(ctx, opts...)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}

	return storage.NewClient(ctx, option.WithHTTPClient(client))
}

// _initializeCloudRunService initializes the Cloud Run API service in GCP
func _initializeCloudRunService(ctx context.Context) (*cloudrun.Service, error) {
	// Use the correct Cloud Run scope
	scopesOption := option.WithScopes("https://www.googleapis.com/auth/cloud-platform")
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(
			google.ComputeTokenSource("", "https://www.googleapis.com/auth/cloud-platform"),
		))
	}

	client, err := cloudrun.NewService(ctx, opts...)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}

	return client, nil
}

// Enhanced CreateBucketIfNotExists with better error handling
func (gcpService *GcpServices) CreateBucketIfNotExists(ctx context.Context, projectID, bucketName, region string) error {
	logger := util.GetLogger(ctx)
	err := gcpService.AdminGCPService.storageService.Bucket(bucketName).Create(ctx, projectID, &storage.BucketAttrs{
		Location: region,
	})
	if err != nil {
		var gErr *googleapi.Error
		if errors.As(err, &gErr) {
			switch gErr.Code {
			case 409: // Already exists
				logger.Infof("bucket %s already exists", bucketName)
				return nil
			default:
				return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
			}
		}
		return err
	}
	logger.Infof("created bucket %s in region %s", bucketName, region)
	return nil
}

func (gcpService *GcpServices) DeleteBucket(ctx context.Context, bucketName string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("Deleting bucket: %s", bucketName)

	err := gcpService.AdminGCPService.storageService.Bucket(bucketName).Delete(ctx)
	if err != nil {
		var gErr *googleapi.Error
		if errors.As(err, &gErr) && gErr.Code == http.StatusNotFound {
			// Bucket does not exist, treat as success
			return nil
		}
		return fmt.Errorf("Buckets.Delete: %v", err)
	}
	return nil
}

// GetBucket retrieves bucket details from GCP Storage API
func (gcpService *GcpServices) GetBucket(ctx context.Context, bucketName string) (*hyperscalermodels.BucketDetails, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Getting bucket details for: %s", bucketName)

	// Get bucket attributes from GCP Storage API using the high-level client
	bucketAttrs, err := gcpService.AdminGCPService.storageService.Bucket(bucketName).Attrs(ctx)
	if err != nil {
		var gErr *googleapi.Error
		if errors.As(err, &gErr) && gErr.Code == http.StatusNotFound {
			logger.Errorf("Bucket %s not found", bucketName)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, fmt.Errorf("bucket %s not found", bucketName))
		}
		logger.Errorf("Failed to get bucket attributes for %s: %v", bucketName, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}

	logger.Debugf("Successfully retrieved bucket attributes for: %s", bucketName)

	// Get bucket details from Storage v1 API to access satisfiesPzi and satisfiesPzs fields
	bucketV1, err := gcpService.AdminGCPService.storageV1Service.Buckets.Get(bucketName).Do()
	if err != nil {
		logger.Errorf("Failed to get bucket v1 details for %s: %v", bucketName, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}

	// Extract PZI/PZS information from the v1 API response
	satisfiesPzi := bucketV1.SatisfiesPZI
	satisfiesPzs := bucketV1.SatisfiesPZS

	// Create and return bucket details
	bucketDetails := &hyperscalermodels.BucketDetails{
		Name:          bucketAttrs.Name,
		Location:      bucketAttrs.Location,
		LocationType:  bucketAttrs.LocationType,
		StorageClass:  bucketAttrs.StorageClass,
		SatisfiesPzi:  satisfiesPzi,
		SatisfiesPzs:  satisfiesPzs,
		ProjectNumber: "", // Will be populated by the caller if needed
		Region:        "", // Will be populated by the caller if needed
		Created:       bucketAttrs.Created.Format("2006-01-02T15:04:05Z"),
		Updated:       bucketAttrs.Updated.Format("2006-01-02T15:04:05Z"),
	}

	logger.Infof("Bucket %s - PZI: %t, PZS: %t", bucketName, satisfiesPzi, satisfiesPzs)
	return bucketDetails, nil
}

func (gcpService *GcpServices) EmptyBucket(ctx context.Context, bucketName string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("Emptying bucket: %s", bucketName)

	bucket := gcpService.AdminGCPService.storageService.Bucket(bucketName)

	// List all objects in the bucket
	it := bucket.Objects(ctx, nil)
	objectCount := 0
	batchSize := 100 // Process objects in batches
	objectNames := make([]string, 0, batchSize)
	maxObjects := 10000 // Safety limit to prevent infinite loops
	iterationCount := 0

	for {
		iterationCount++

		// Safety check to prevent infinite loops
		if iterationCount > maxObjects {
			return fmt.Errorf("safety limit reached: processed %d objects, stopping to prevent infinite loop", maxObjects)
		}

		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects in bucket %s: %v", bucketName, err)
		}

		objectNames = append(objectNames, obj.Name)

		// Process batch when it reaches batchSize or when we're done
		if len(objectNames) >= batchSize {
			err = gcpService.deleteObjectBatch(ctx, bucket, objectNames, bucketName)
			if err != nil {
				return err
			}
			objectCount += len(objectNames)
			objectNames = objectNames[:0] // Reset slice but keep capacity
		}
	}

	// Process remaining objects in the final batch
	if len(objectNames) > 0 {
		err := gcpService.deleteObjectBatch(ctx, bucket, objectNames, bucketName)
		if err != nil {
			return err
		}
		objectCount += len(objectNames)
	}

	logger.Infof("Successfully emptied bucket: %s (deleted %d objects)", bucketName, objectCount)
	return nil
}

// deleteObjectBatch deletes a batch of objects from the bucket
func (gcpService *GcpServices) deleteObjectBatch(ctx context.Context, bucket *storage.BucketHandle, objectNames []string, bucketName string) error {
	logger := util.GetLogger(ctx)

	// Delete objects in parallel using goroutines
	type deleteResult struct {
		objectName string
		err        error
	}

	resultChan := make(chan deleteResult, len(objectNames))

	// Launch goroutines to delete objects in parallel
	for _, objectName := range objectNames {
		go func(name string) {
			logger.Debugf("Deleting object: %s", name)
			err := bucket.Object(name).Delete(ctx)
			resultChan <- deleteResult{objectName: name, err: err}
		}(objectName)
	}

	// Collect results
	var errors []error
	for i := 0; i < len(objectNames); i++ {
		result := <-resultChan
		if result.err != nil {
			errors = append(errors, fmt.Errorf("failed to delete object %s: %v", result.objectName, result.err))
		}
	}

	// Return error if any deletions failed
	if len(errors) > 0 {
		return fmt.Errorf("failed to delete %d objects from bucket %s: %v", len(errors), bucketName, errors)
	}

	return nil
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

func (gcpService *GcpServices) GetServiceAccount(projectID, email string) (*hyperscalermodels.ServiceAccount, error) {
	response, err := gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.List("projects/" + projectID).Do()
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	for _, account := range response.Accounts {
		if account.Email == email {
			return convertServiceAccountToHyperscalerServiceAccount(account), nil
		}
	}
	return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("service account not found"))
}

func (gcpService *GcpServices) AttachOrUpdateRolesForServiceAccounts(roles []string, serviceAccountEmail, projectID string) error {
	policy, err := gcpService.getProjectIamPolicy(projectID)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}

	currentSvcAccountMember := "serviceAccount:" + serviceAccountEmail
	requiredRolesMap := gcpService.initializeRequiredRolesMap(roles)

	projectIAMPolicyBindings := gcpService.updatePolicyBindings(policy.Bindings, requiredRolesMap, currentSvcAccountMember)

	projectIAMPolicyBindings = gcpService.addMissingRoles(projectIAMPolicyBindings, requiredRolesMap, currentSvcAccountMember)

	return gcpService.setProjectIamPolicy(projectID, policy.Etag, projectIAMPolicyBindings)
}

// RemoveRolesFromServiceAccounts removes specified roles from a service account
func (gcpService *GcpServices) RemoveRolesFromServiceAccounts(roles []string, serviceAccountEmail, projectID string) error {
	policy, err := gcpService.getProjectIamPolicy(projectID)
	if err != nil {
		return err
	}

	currentSvcAccountMember := "serviceAccount:" + serviceAccountEmail
	rolesToRemove := make(map[string]bool)
	for _, role := range roles {
		rolesToRemove[role] = true
	}

	// Remove the service account from the specified roles
	var updatedBindings []*projectsManagement.Binding
	for _, binding := range policy.Bindings {
		if rolesToRemove[binding.Role] {
			// Remove the service account from this role's members
			var updatedMembers []string
			for _, member := range binding.Members {
				if !strings.EqualFold(strings.ToLower(member), strings.ToLower(currentSvcAccountMember)) {
					updatedMembers = append(updatedMembers, member)
				}
			}

			// Only keep the binding if there are still members
			if len(updatedMembers) > 0 {
				updatedBindings = append(updatedBindings, &projectsManagement.Binding{
					Role:    binding.Role,
					Members: updatedMembers,
				})
			}
			// If no members left, we don't add the binding (effectively removing the role)
		} else {
			// Keep other bindings unchanged
			updatedBindings = append(updatedBindings, binding)
		}
	}

	return gcpService.setProjectIamPolicy(projectID, policy.Etag, updatedBindings)
}

func (gcpService *GcpServices) getProjectIamPolicy(projectID string) (*projectsManagement.Policy, error) {
	getPolicyRequest := &projectsManagement.GetIamPolicyRequest{}
	iamPolicy, err := gcpService.AdminGCPService.cloudProjectsService.Projects.GetIamPolicy(projectID, getPolicyRequest).Do()
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	return iamPolicy, nil
}

func (gcpService *GcpServices) initializeRequiredRolesMap(roles []string) map[string]bool {
	requiredRolesMap := make(map[string]bool)
	for _, role := range roles {
		requiredRolesMap[role] = false
	}
	return requiredRolesMap
}

func (gcpService *GcpServices) updatePolicyBindings(policyBindings []*projectsManagement.Binding, requiredRolesMap map[string]bool, currentSvcAccountMember string) []*projectsManagement.Binding {
	var updatedBindings []*projectsManagement.Binding
	for _, policyBinding := range policyBindings {
		svcAccountPreExists := false
		if roleProcessed, ok := requiredRolesMap[policyBinding.Role]; ok && !roleProcessed {
			for _, member := range policyBinding.Members {
				if strings.EqualFold(strings.ToLower(member), strings.ToLower(currentSvcAccountMember)) {
					svcAccountPreExists = true
					break
				}
			}
			if !svcAccountPreExists {
				policyBinding.Members = append(policyBinding.Members, currentSvcAccountMember)
			}
			requiredRolesMap[policyBinding.Role] = true
		}
		updatedBindings = append(updatedBindings, &projectsManagement.Binding{
			Role:    policyBinding.Role,
			Members: policyBinding.Members,
		})
	}
	return updatedBindings
}

func (gcpService *GcpServices) addMissingRoles(projectIAMPolicyBindings []*projectsManagement.Binding, requiredRolesMap map[string]bool, currentSvcAccountMember string) []*projectsManagement.Binding {
	for role, isProcessed := range requiredRolesMap {
		if !isProcessed {
			projectIAMPolicyBindings = append(projectIAMPolicyBindings, &projectsManagement.Binding{
				Role: role,
				Members: []string{
					currentSvcAccountMember,
				},
			})
		}
	}
	return projectIAMPolicyBindings
}

func (gcpService *GcpServices) setProjectIamPolicy(projectID string, etag string, projectIAMPolicyBindings []*projectsManagement.Binding) error {
	policyRequest := &projectsManagement.SetIamPolicyRequest{
		Policy: &projectsManagement.Policy{
			Bindings: projectIAMPolicyBindings,
			Etag:     etag,
		},
	}
	_, err := gcpService.AdminGCPService.cloudProjectsService.Projects.SetIamPolicy(projectID, policyRequest).Do()
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	return nil
}

func convertServiceAccounttoGcpServiceAccount(sa *hyperscalermodels.ServiceAccount) *iam.ServiceAccount {
	return &iam.ServiceAccount{
		Name:        sa.Name,
		Description: sa.Description,
		Email:       sa.Email,
		ProjectId:   sa.ProjectId,
		UniqueId:    sa.UniqueId,
		Disabled:    sa.Disabled,
		DisplayName: sa.DisplayName,
	}
}

func convertCreateServiceAccountRequestToGcpCreateRequest(createRequest *hyperscalermodels.CreateServiceAccountRequest) *iam.CreateServiceAccountRequest {
	return &iam.CreateServiceAccountRequest{
		AccountId:      createRequest.AccountId,
		ServiceAccount: convertServiceAccounttoGcpServiceAccount(createRequest.ServiceAccount),
	}
}

func (gcpService *GcpServices) CreateServiceAccount(createRequest *hyperscalermodels.CreateServiceAccountRequest, projectNumber, email string) (*hyperscalermodels.ServiceAccount, error) {
	account, err := gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.Create("projects/"+projectNumber, convertCreateServiceAccountRequestToGcpCreateRequest(createRequest)).Do()
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		switch gerr.Code {
		case http.StatusConflict:
			serviceAccount, isSACreated, err := gcpService.IsServiceAccountCreated(email)
			if err != nil || !isSACreated {
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
			}
			account = convertServiceAccounttoGcpServiceAccount(serviceAccount)
			return convertServiceAccountToHyperscalerServiceAccount(account), nil
		}
	}

	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	return convertServiceAccountToHyperscalerServiceAccount(account), nil
}

func (gcpService *GcpServices) IsServiceAccountCreated(email string) (account *hyperscalermodels.ServiceAccount, isSACreated bool, err error) {
	acc, err := gcpService.GetServiceAccountByEmail(email)
	if err != nil {
		return nil, false, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	return acc, true, nil
}

func convertServiceAccountToHyperscalerServiceAccount(sa *iam.ServiceAccount) *hyperscalermodels.ServiceAccount {
	return &hyperscalermodels.ServiceAccount{
		Name:        sa.Name,
		Description: sa.Description,
		Email:       sa.Email,
		ProjectId:   sa.ProjectId,
		UniqueId:    sa.UniqueId,
		Disabled:    sa.Disabled,
		DisplayName: sa.DisplayName,
	}
}
func (gcpService *GcpServices) GetServiceAccountByEmail(email string) (*hyperscalermodels.ServiceAccount, error) {
	account, err := gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.Get("projects/-/serviceAccounts/" + email).Do()
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	return convertServiceAccountToHyperscalerServiceAccount(account), nil
}

func (gcpService *GcpServices) DeleteServiceAccount(projectNumber string, saEmail string) error {
	// Convert project number to project ID for the IAM API call
	projectID, err := getProjectIDFromNumber(gcpService, projectNumber)
	if err != nil {
		gcpService.GetLogger().Errorf("Failed to get project ID from project number %s: %v", projectNumber, err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, fmt.Errorf("failed to resolve project number %s to project ID: %v", projectNumber, err))
	}

	if strings.TrimSpace(projectID) == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, fmt.Errorf("resolved project ID is empty for project number: %s", projectNumber))
	}

	gcpService.GetLogger().Debugf("Resolved project number %s to project ID: %s", projectNumber, projectID)
	name := "projects/" + projectID + "/serviceAccounts/" + saEmail
	_, err = gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.Delete(name).Do()
	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == http.StatusNotFound {
			// Service account does not exist, treat as success
			return nil
		}
		return gcpService.determineIsRetryableError(saEmail, err)
	}
	return nil
}

func (gcpService *GcpServices) determineIsRetryableError(saEmail string, err error) error {
	if retryutils.ShouldRetry(err) {
		gcpService.GetLogger().Debugf("Service account %s deletion failed with retriable error, returning for Temporal retry", saEmail)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPServiceAccountDeletionError,
			fmt.Errorf("Projects.ServiceAccounts.Delete: %v", err))
	}

	gcpService.GetLogger().Debugf("Service account %s deletion failed with non-retriable error, returning non-retriable error", saEmail)
	return vsaerrors.NewVCPError(vsaerrors.ErrGCPServiceAccountDeletionNonRetriableError,
		fmt.Errorf("Projects.ServiceAccounts.Delete: %v", err))
}

func (gcpService *GcpServices) CreateHmacKey(projectID string, serviceAccount string) (accessKey *string, secretKey *string, err error) {
	// Create the HMAC key
	key, err := gcpService.AdminGCPService.storageService.CreateHMACKey(gcpService.Ctx, projectID, serviceAccount, storage.ForHMACKeyServiceAccountEmail(serviceAccount))
	if err != nil {
		return nil, nil, err
	}

	// Extract the access key and secret key from the response
	accessKey = &key.AccessID
	secretKey = &key.Secret

	return accessKey, secretKey, nil
}

func (gcpService *GcpServices) DeleteHmacKey(projectID string, accessKey string, ServiceAccount string) error {
	// Delete the HMAC key
	_, err := gcpService.AdminGCPService.storageService.HMACKeyHandle(projectID, accessKey).Update(gcpService.Ctx, storage.HMACKeyAttrsToUpdate{State: INACTIVE}, storage.ForHMACKeyServiceAccountEmail(ServiceAccount))
	if err != nil {
		return fmt.Errorf("failed to update HMAC key state to INACTIVE: %v", err)
	}
	err = gcpService.AdminGCPService.storageService.HMACKeyHandle(projectID, accessKey).Delete(gcpService.Ctx)
	if err != nil {
		return err
	}

	return nil
}

func GetImpersonatedKmsService(ctx context.Context, targetEmail string, scopeCreds *google.Credentials) (*cloudkms.Service, error) {
	// Set up the impersonation token source using the sde service account email from the KMS config
	// Use the VSA service account key to impersonate the SDE service account
	// Note:- SDE service account should have roles/iam.serviceAccountTokenCreator and VSA service account should be the member of the project
	logger := util.GetLogger(ctx)
	scopes := []string{cloudkms.CloudPlatformScope}
	tokenSource, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
		TargetPrincipal: targetEmail,
		Scopes:          scopes,
	}, option.WithCredentials(scopeCreds))
	if err != nil {
		logger.Errorf("Failed to create impersonated token source: %v. TargetPrincipal: %s, Scopes: %v", err, targetEmail, scopes)
		return nil, err
	}

	// Use the impersonated client to interact with Google Cloud KMS
	kmsService, err := cloudkms.NewService(ctx, option.WithTokenSource(tokenSource))
	if err != nil {
		return nil, fmt.Errorf("failed to create KMS service: %w", err)
	}
	return kmsService, nil
}
