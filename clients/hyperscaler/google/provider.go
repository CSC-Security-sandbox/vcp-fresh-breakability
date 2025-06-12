package google

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	logger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"golang.org/x/oauth2/google"
	projectsManagement "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/serviceconsumermanagement/v1"
	"google.golang.org/api/servicenetworking/v1"
	scopesHttp "google.golang.org/api/transport/http"
)

var (

	// serviceNetworkingEndpoint is the endpoint for the Service Networking API.
	serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "")
	// serviceConsumerManagementEndpoint is the endpoint for the Service Consumer Management API.
	serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "")
	// MockMetaDataHost is the endpoint for the metadata server.
	MockMetaDataHost = env.GetString("GCE_METADATA_HOST", "")

	newClient = _newClient

	newGoogleClient                = _newGoogleClient
	initializeManagementService    = _initializeManagementService
	initializeNetworkingService    = _initializeNetworkingService
	initializeComputeService       = _initializeComputeService
	initializeStorageService       = _initializeStorageService
	initializeIamService           = _initializeIamService
	initializeCloudProjectsService = _initializeCloudProjectsService
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
	storageService       StorageClient
	cloudProjectsService *projectsManagement.Service
	iamService           *iam.Service
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
	log := util.GetLogger(ctx)
	log.Debug("Calling initializeManagementService")
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

	storageService, err := initializeStorageService(ctx)
	if err != nil {
		log.Error("Error initializeStorageService", err)
		return nil, err
	}

	cloudProjectservice, err := initializeCloudProjectsService(ctx)
	if err != nil {
		log.Error("Error initializeCloudProjectsService", err)
		return nil, err
	}

	iamService, err := initializeIamService(ctx)
	if err != nil {
		log.Error("Error initializeIamService", err)
		return nil, err
	}
	gServices := AdminGCPService{
		networkingService:    networkingService,
		managementService:    managementService,
		computeService:       computeService,
		storageService:       &storageClient{client: storageService},
		iamService:           iamService,
		cloudProjectsService: cloudProjectservice,
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
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = waitTimeoutMinutes
	svc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Error("serviceconsumermanagement.NewService error", err)
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
		logger.Error("error while creating new client for _initializeNetworkingService", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = defaultSleepTime
	svc, err := servicenetworking.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Error("servicenetworking.NewService error", err)
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
		logger.Error("error while creating new client for _initializeComputeService", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err)
	}
	client.Timeout = defaultSleepTime

	svc, err := compute.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		logger.Error("compute.NewService error", err)
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

func (gcpService *GcpServices) CreateBucketIfNotExists(ctx context.Context, projectID, bucketName, region string) error {
	logger := util.GetLogger(ctx)
	err := gcpService.AdminGCPService.storageService.Bucket(bucketName).Create(ctx, projectID, &storage.BucketAttrs{
		Location: region,
	})
	if err != nil {
		// Ignore error if bucket already exists
		var gErr *googleapi.Error
		if errors.As(err, &gErr) && gErr.Code == 409 {
			logger.Infof("bucket %s already exists", bucketName)
			return nil
		}
		return err
	}
	logger.Infof("created bucket %s in region %s", bucketName, region)
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

func (gcpService *GcpServices) GetServiceAccount(projectID, email string) (*iam.ServiceAccount, error) {
	response, err := gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.List("projects/" + projectID).Do()
	if err != nil {
		return nil, fmt.Errorf("Projects.ServiceAccounts.List: %v", err)
	}
	for _, account := range response.Accounts {
		if account.Email == email {
			return account, nil
		}
	}
	return nil, fmt.Errorf("service account not found")
}

func (gcpService *GcpServices) AttachOrUpdateRolesForServiceAccounts(roles []string, serviceAccountEmail, projectID string) error {
	policy, err := gcpService.getProjectIamPolicy(projectID)
	if err != nil {
		return err
	}

	currentSvcAccountMember := "serviceAccount:" + serviceAccountEmail
	requiredRolesMap := gcpService.initializeRequiredRolesMap(roles)

	projectIAMPolicyBindings := gcpService.updatePolicyBindings(policy.Bindings, requiredRolesMap, currentSvcAccountMember)

	gcpService.addMissingRoles(projectIAMPolicyBindings, requiredRolesMap, currentSvcAccountMember)

	return gcpService.setProjectIamPolicy(projectID, policy.Etag, projectIAMPolicyBindings)
}

func (gcpService *GcpServices) getProjectIamPolicy(projectID string) (*projectsManagement.Policy, error) {
	getPolicyRequest := &projectsManagement.GetIamPolicyRequest{}
	iamPolicy, err := gcpService.AdminGCPService.cloudProjectsService.Projects.GetIamPolicy(projectID, getPolicyRequest).Do()
	if err != nil {
		return nil, fmt.Errorf("Projects.GetIamPolicy: %v", err)
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

func (gcpService *GcpServices) addMissingRoles(projectIAMPolicyBindings []*projectsManagement.Binding, requiredRolesMap map[string]bool, currentSvcAccountMember string) {
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
		return fmt.Errorf("Projects.SetIamPolicy: %v", err)
	}
	return err
}

func (gcpService *GcpServices) CreateServiceAccount(createRequest *iam.CreateServiceAccountRequest, projectNumber, email string) (account *iam.ServiceAccount, err error) {
	account, err = gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.Create("projects/"+projectNumber, createRequest).Do()
	if gerr, ok := err.(*googleapi.Error); ok {
		switch gerr.Code {
		case http.StatusConflict:
			serviceAccount, isSACreated, err := gcpService.IsServiceAccountCreated(email)
			if err != nil || !isSACreated {
				return nil, err
			}
			account = serviceAccount
			return account, err
		}
	}

	if err != nil {
		return nil, err
	}
	return account, nil
}

func (gcpService *GcpServices) IsServiceAccountCreated(email string) (account *iam.ServiceAccount, isSACreated bool, err error) {
	acc, err := gcpService.GetServiceAccountByEmail(email)
	if err != nil {
		return nil, false, fmt.Errorf("Projects.ServiceAccounts.Get: %v", err)
	}
	return acc, true, nil
}

func (gcpService *GcpServices) GetServiceAccountByEmail(email string) (*iam.ServiceAccount, error) {
	account, err := gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.Get("projects/-/serviceAccounts/" + email).Do()
	if err != nil {
		return nil, fmt.Errorf("Projects.ServiceAccounts.Get: %v", err)
	}
	return account, nil
}
