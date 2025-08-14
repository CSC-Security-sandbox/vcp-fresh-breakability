package google

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

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
	"google.golang.org/api/option"
	"google.golang.org/api/privateca/v1"
	cloudrun "google.golang.org/api/run/v2"
	"google.golang.org/api/secretmanager/v1"
	"google.golang.org/api/serviceconsumermanagement/v1"
	"google.golang.org/api/servicenetworking/v1"
	scopesHttp "google.golang.org/api/transport/http"
)

const INACTIVE = "INACTIVE"

var (
	// serviceNetworkingEndpoint is the endpoint for the Service Networking API.
	serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "")
	// serviceConsumerManagementEndpoint is the endpoint for the Service Consumer Management API.
	serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "")
	// MockMetaDataHost is the endpoint for the metadata server.
	MockMetaDataHost = env.GetString("GCE_METADATA_HOST", "")

	newClient       = _newClient
	newClientScopes = scopesHttp.NewClient

	newGoogleClient                = _newGoogleClient
	initializeManagementService    = _initializeManagementService
	initializeNetworkingService    = _initializeNetworkingService
	initializeComputeService       = _initializeComputeService
	initializeStorageService       = _initializeStorageService
	initializeIamService           = _initializeIamService
	initializeCloudProjectsService = _initializeCloudProjectsService
	initializePrivateCaService     = _initializePrivateCaService
	initializeSecretManagerService = _initializeSecretManagerService
	initializeCloudDnsService      = _initializeCloudDnsService
	initializeCloudRunService      = _initializeCloudRunService
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
	log.Debug("Calling initializeManagementService")
	managementService, err := initializeManagementService(ctx)
	if err != nil {
		log.Errorf("Error initializeManagementService : %s", err.Error())
		return nil, err
	}

	log.Debug("Calling initializeNetworkingService")
	networkingService, err := initializeNetworkingService(ctx)
	if err != nil {
		log.Errorf("Error initializeNetworkingService : %s", err.Error())
		return nil, err
	}

	log.Debug("Calling initializeComputeService")
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

	log.Debug("Calling initializeIamService")
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
	log.Debug("Calling initializePrivateCaService")
	privateCaService, err := initializePrivateCaService(ctx)
	if err != nil {
		log.Errorf("Error initializePrivateCaService :%s", err.Error())
		return nil, err
	}
	log.Debug("Calling initializeSecretManagerService")
	secretManagerService, err := initializeSecretManagerService(ctx)
	if err != nil {
		log.Errorf("Error initializeSecretManagerService :%s", err.Error())
		return nil, err
	}

	log.Debug("Calling initializeCloudRunService")
	cloudRunService, err := initializeCloudRunService(ctx)
	if err != nil {
		log.Errorf("error initializing CloudRun Service: %s", err.Error())
		return nil, err
	}

	log.Debug("Calling initializeCloudDnsService")
	cloudDnsService, err := initializeCloudDnsService(ctx)
	if err != nil {
		log.Errorf("Error initializeCloudDnsService :%s", err.Error())
		return nil, err
	}

	gServices := AdminGCPService{
		networkingService:    networkingService,
		managementService:    managementService,
		computeService:       computeService,
		storageService:       storageService,
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

	logger.Debug(fmt.Sprintf("opts: %#v", opts))
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

// _initializeIamService initializes the IAM API service
func _initializeIamService(ctx context.Context) (*iam.Service, error) {
	slogger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(iam.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}
	slogger.Debug(fmt.Sprintf("opts: %#v", opts))

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", iam.CloudPlatformScope)))
	}
	slogger.Debug("creating newClient")
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

	slogger.Debug(fmt.Sprintf("opts: %#v", opts))
	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", projectsManagement.CloudPlatformScope)))
	}
	slogger.Debug("creating newClient")
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
	logger.Debug(fmt.Sprintf("opts: %#v", opts))

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", compute.ComputeReadonlyScope, compute.ComputeScope, compute.CloudPlatformScope)))
	}
	logger.Debug("creating newClient")
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

// _initializePrivateCaService initializes the private CA API service in GCP
func _initializePrivateCaService(ctx context.Context) (*privateca.Service, error) {
	logger := util.GetLogger(ctx)
	scopesOption := option.WithScopes(privateca.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", privateca.CloudPlatformScope)))
	}
	logger.Debug("creating newClient")
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
	logger.Debug("creating newClient")
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
	logger.Debug("creating newClient")
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
