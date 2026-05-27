package oci

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/secrets"
	"github.com/oracle/oci-go-sdk/v65/vault"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	logger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// OciServices is the top-level struct for OCI interactions, mirroring GcpServices.
// It holds the request context, logger, and the admin-level OCI clients.
type OciServices struct {
	Ctx    context.Context
	Logger logger.Logger
	Retry  RetryStrategy

	AdminOCIService *AdminOCIService
}

// AdminOCIService groups the OCI SDK clients needed for secret management
// and object storage pre-authenticated request (PAR) generation.
//
// OCI splits secret operations across two services:
//   - vault.VaultsClient  — CRUD on secret metadata + creating new versions (write API)
//   - secrets.SecretsClient — reading secret content / bundles (read API)
//
// Object Storage exposes a single client used to mint PARs for the VSA image
// hand-off to VLM, mirroring what GcpServices does with V4 signed URLs on GCS.
//
// Docs:
//   - Vault (write):       https://docs.oracle.com/en-us/iaas/api/#/en/secretmgmt/20180608/
//   - Secrets (read):      https://docs.oracle.com/en-us/iaas/api/#/en/secretretrieval/20190301/
//   - Object Storage:      https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/20160918/
type AdminOCIService struct {
	vaultClient         vault.VaultsClient
	secretsClient       secrets.SecretsClient
	objectStorageClient objectstorage.ObjectStorageClient
}

var (
	initializeVaultClient         = _initializeVaultClient
	initializeSecretsClient       = _initializeSecretsClient
	initializeObjectStorageClient = _initializeObjectStorageClient
)

// InitializeClients Creates the OCI SDK clients using the auth method
// configured via OCI_AUTH_TYPE (defaults to oke_workload_identity)
// (~/.oci/config or instance principal, depending on environment).
func (ociService *OciServices) InitializeClients() error {
	if ociService.IsAdminClientInitialized() {
		return nil
	}
	adminService, err := newOCIClients(ociService.Ctx)
	if err != nil {
		return err
	}
	ociService.AdminOCIService = adminService
	return nil
}

// IsAdminClientInitialized returns true when the admin clients are ready.
func (ociService *OciServices) IsAdminClientInitialized() bool {
	return ociService.AdminOCIService != nil
}

// GetLogger returns the logger, creating one from context if necessary.
func (ociService *OciServices) GetLogger() logger.Logger {
	if ociService.Logger == nil {
		ociService.Logger = util.GetLogger(ociService.Ctx)
	}
	return ociService.Logger
}

// GetContext returns the context, falling back to context.Background().
func (ociService *OciServices) GetContext() context.Context {
	if ociService.Ctx == nil {
		ociService.Ctx = context.Background()
	}
	return ociService.Ctx
}

func newOCIClients(ctx context.Context) (*AdminOCIService, error) {
	log := util.GetLogger(ctx)

	vaultCl, err := initializeVaultClient()
	if err != nil {
		log.Errorf("Error initializing OCI Vault client: %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIClientInitializationError, err)
	}

	secretsCl, err := initializeSecretsClient()
	if err != nil {
		log.Errorf("Error initializing OCI Secrets client: %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIClientInitializationError, err)
	}

	objectStorageCl, err := initializeObjectStorageClient()
	if err != nil {
		log.Errorf("Error initializing OCI Object Storage client: %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIClientInitializationError, err)
	}

	return &AdminOCIService{
		vaultClient:         *vaultCl,
		secretsClient:       *secretsCl,
		objectStorageClient: *objectStorageCl,
	}, nil
}

// ociConfigProvider returns the appropriate OCI ConfigurationProvider based on
// the OCI_AUTH_TYPE environment variable:
//   - "instance_principal":    for workloads on OCI compute instances
//   - "oke_workload_identity": for pods on OKE with workload identity federation
//   - "config_file":           reads from ~/.oci/config (local dev outside containers)
//   - "env":                   reads credentials from OCI_TENANCY, OCI_USER,
//     OCI_REGION, OCI_FINGERPRINT, OCI_PRIVATE_KEY env vars
//     (best for Kubernetes where secrets are injected as env vars)
func ociConfigProvider() (common.ConfigurationProvider, error) {
	switch env.OCIAuthType {
	case "config_file":
		return common.DefaultConfigProvider(), nil
	case "instance_principal":
		return auth.InstancePrincipalConfigurationProvider()
	case "oke_workload_identity":
		return auth.OkeWorkloadIdentityConfigurationProvider()
	case "env":
		return ociConfigProviderFromEnv()
	default:
		return nil, fmt.Errorf("unsupported OCI_AUTH_TYPE %q; expected one of: instance_principal, oke_workload_identity, config_file, env", env.OCIAuthType)
	}
}

// ociConfigProviderFromEnv builds a ConfigurationProvider from individual
// environment variables. This is the preferred method for Kubernetes
// deployments where credentials are injected via ConfigMap or Secret.
//
// Required env vars: OCI_TENANCY, OCI_USER, OCI_REGION, OCI_FINGERPRINT, OCI_PRIVATE_KEY
// Optional env var:  OCI_PASSPHRASE (private key passphrase)
func ociConfigProviderFromEnv() (common.ConfigurationProvider, error) {
	const (
		envTenancy     = "OCI_TENANCY"
		envUser        = "OCI_USER"
		envRegion      = "OCI_REGION"
		envFingerprint = "OCI_FINGERPRINT"
		envPrivateKey  = "OCI_PRIVATE_KEY"
		envPassphrase  = "OCI_PASSPHRASE"
	)

	tenancyID := os.Getenv(envTenancy)
	userID := os.Getenv(envUser)
	region := os.Getenv(envRegion)
	fingerprint := os.Getenv(envFingerprint)
	privateKey := os.Getenv(envPrivateKey)
	passphrase := os.Getenv(envPassphrase)

	var missing []string
	for _, kv := range []struct{ name, val string }{
		{envTenancy, tenancyID},
		{envUser, userID},
		{envRegion, region},
		{envFingerprint, fingerprint},
		{envPrivateKey, privateKey},
	} {
		if kv.val == "" {
			missing = append(missing, kv.name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("OCI_AUTH_TYPE=env but missing required env vars: %s", strings.Join(missing, ", "))
	}

	var pp *string
	if passphrase != "" {
		pp = &passphrase
	}
	return common.NewRawConfigurationProvider(tenancyID, userID, region, fingerprint, privateKey, pp), nil
}

// _initializeVaultClient creates the Vault (write) client.
// Docs: https://docs.oracle.com/en-us/iaas/tools/go/65.108.3/vault/index.html
func _initializeVaultClient() (*vault.VaultsClient, error) {
	configProvider, err := ociConfigProvider()
	if err != nil {
		return nil, fmt.Errorf("ociConfigProvider: %w", err)
	}
	client, err := vault.NewVaultsClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, fmt.Errorf("vault.NewVaultsClientWithConfigurationProvider: %w", err)
	}
	return &client, nil
}

// _initializeSecretsClient creates the Secrets (read) client.
// Docs: https://docs.oracle.com/en-us/iaas/tools/go/65.108.3/secrets/index.html
func _initializeSecretsClient() (*secrets.SecretsClient, error) {
	configProvider, err := ociConfigProvider()
	if err != nil {
		return nil, fmt.Errorf("ociConfigProvider: %w", err)
	}
	client, err := secrets.NewSecretsClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, fmt.Errorf("secrets.NewSecretsClientWithConfigurationProvider: %w", err)
	}
	return &client, nil
}

// _initializeObjectStorageClient creates the Object Storage client used for
// pre-authenticated request (PAR) generation and namespace lookup.
// Docs: https://docs.oracle.com/en-us/iaas/tools/go/65.108.3/objectstorage/index.html
func _initializeObjectStorageClient() (*objectstorage.ObjectStorageClient, error) {
	configProvider, err := ociConfigProvider()
	if err != nil {
		return nil, fmt.Errorf("ociConfigProvider: %w", err)
	}
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, fmt.Errorf("objectstorage.NewObjectStorageClientWithConfigurationProvider: %w", err)
	}
	return &client, nil
}

// NewAdminOCIService creates an AdminOCIService from pre-built SDK clients.
// This allows callers (including test code in other packages) to construct
// an OciServices backed by controlled or mock-backed clients without going
// through the full InitializeClients authentication flow.
func NewAdminOCIService(vaultClient vault.VaultsClient, secretsClient secrets.SecretsClient) *AdminOCIService {
	return &AdminOCIService{vaultClient: vaultClient, secretsClient: secretsClient}
}

// ociResourceNotFoundCheck mirrors googleResourceNotFoundCheck: it returns nil
// when the error is an HTTP 404 (resource not found), allowing callers to
// distinguish "not found" from real failures.
func ociResourceNotFoundCheck(err error) error {
	if serviceErr, ok := common.IsServiceError(err); ok && serviceErr.GetHTTPStatusCode() == http.StatusNotFound {
		return nil
	}
	return err
}
