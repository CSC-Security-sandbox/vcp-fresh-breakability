package activities

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"strconv"
	"strings"
	"time"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type CommonActivities struct {
	SE database.Storage
}

var (
	GetGCPService        = _getGCPService
	GetProviderByNode    = _getProviderByNode
	MakeSubnetName       = _makeSubnetName
	isSubnetReusable     = _isSubnetReusable
	findEmptySubnet      = _findEmptySubnet
	getPoolsBySubnetwork = _getPoolsBySubnetwork
	getIPsInSubnet       = _getIPsInSubnet
	getSignedJwtToken    = auth.GetSignedJwtToken
)

func (ca CommonActivities) CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	se := ca.SE
	logger.Infof("creating job: %s with status: %s", job.UUID, job.State)
	return se.CreateJob(ctx, job)
}

// UpdateJobStatus updates the status of a job in the database.
func (ca CommonActivities) UpdateJobStatus(ctx context.Context, job *datamodel.Job) error {
	logger := util.GetLogger(ctx)
	se := ca.SE
	logger.Infof("updating job: %s with status: %s", job.UUID, job.State)
	return se.UpdateJob(ctx, job.UUID, job.State, job.TrackingID, job.ErrorDetails)
}

// DescribeJob gives the status of a job in the database.
func DescribeJob(ctx context.Context, jobId, basepath, jwtToken, projectNumber, location *string) error {
	if jobId == nil {
		return nil
	}
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*basepath, *jwtToken, logger)

	describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
		OperationId:   *jobId,
		ProjectNumber: *projectNumber,
		LocationId:    *location,
	}

	res, err := googleProxyClient.Invoker.V1betaDescribeOperation(ctx, describeOperationParams)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDescribingJob, err)
	}
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if ok {
		if operation.Done.Value {
			return nil
		}
	}
	return vsaerrors.NewVCPError(vsaerrors.ErrJobNotFinished, errors.New("job not finished"))
}

// GetNode retrieves the node associated with the given pool ID.
func (ca CommonActivities) GetNode(ctx context.Context, poolId int64) ([]*datamodel.Node, error) {
	se := ca.SE

	nodes, err := se.GetNodesByPoolID(ctx, poolId)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, errors.New("no node found for the pool")
	}

	return nodes, nil
}

// _getProviderByNode creates a new vsa.Provider instance using the details from the provided node.
func _getProviderByNode(ctx context.Context, node *models.Node) (vsa.Provider, error) {
	var password string
	if common.AuthType == common.USERNAME_PWD_SEC_MGR {
		password = GetPasswordFromCacheOrSecretManager(ctx, node.SecretID)
	} else {
		password = node.Password
	}

	// if ipAddress in empty, populate it with the node's endpoint address
	if len(node.EndpointAddresses) == 0 {
		if node.EndpointAddress == "" {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterNodeIPAddressNotFound, errors.New("node endpoint address is empty"))
		}
		node.EndpointAddresses = []string{node.EndpointAddress}
	}

	return vsa.NewProvider(ctx, vsa.ProviderDetails{
		IPAddresses: node.EndpointAddresses,
		UserName:    node.Username,
		Password:    password,
		// TODO : need to fix once we have certs
		InsecureSkipVerify: true,
	}), nil
}

func (j CommonActivities) GetOntapJob(ctx context.Context, jobUUID string, node *models.Node) (*vsa.OntapJob, error) {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	job, err := provider.JobGet(jobUUID)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (j CommonActivities) GetAuthJWTToken(ctx context.Context, accountName string) (string, error) {
	logger := util.GetLogger(ctx)
	jwtToken, err := getSignedJwtToken(accountName)
	if err != nil {
		logger.Errorf("failed to get token for account %s: %v", accountName, err)
		return "", errors.New("failed to get token")
	}
	return jwtToken, nil
}

// _getGCPService initializes and returns a GcpServices instance.
func _getGCPService(ctx context.Context) (*google.GcpServices, error) {
	gcpService := _newGcpServices(ctx)

	gcpService.Logger.Debug("gcpService initialized")

	gcpService.Logger.Debug("Calling InitializeClients")
	err := gcpService.InitializeClients()
	if err != nil || !gcpService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, errors.New("initialisation of Google GCP service failed"))
	}
	return gcpService, nil
}

// _newGcpServices creates a new instance of GcpServices with the provided context
func _newGcpServices(ctx context.Context) *google.GcpServices {
	return &google.GcpServices{
		Ctx:    ctx,
		Logger: util.GetLogger(ctx),
		Retry:  google.NewExponentialRetryStrategy(time.Second, uint(maxRetries)),
	}
}

// makeSubnetName generates a subnet name based on the project number, region and timestamp
func _makeSubnetName(projectNumber string) string {
	timeNow := strconv.Itoa(int(time.Now().Unix()))
	return fmt.Sprintf("vsa-%s-%s", projectNumber, timeNow)
}

// getSubnetToBeUsed examines existing subnets or identifies if existing subnet can be used for creation of new pool
func getSubnetToBeUsed(service hyperscaler.GoogleServices, se database.Storage, customerProjectNumber, tenantProjectNumber, snHost, tenantProjectRegion string) (*hyperscaler_models.Subnet, error) {
	logger := service.GetLogger()
	subnetsReceived, err := service.ListSubnetworks(snHost, tenantProjectRegion)
	if err != nil {
		logger.Errorf("Error listing subnetwork for tenant project: %s, SN host : %s, region %s. Error : %s", tenantProjectNumber, snHost, tenantProjectRegion, err.Error())
		return nil, err
	}
	if subnetsReceived == nil || len(*subnetsReceived) == 0 {
		return nil, nil
	}
	ctx := service.GetContext()
	account, err := se.GetAccount(ctx, customerProjectNumber)
	if err != nil {
		return nil, err
	}
	subnetPrefix := fmt.Sprintf("vsa-%s", tenantProjectNumber)
	for _, subnet := range *subnetsReceived {
		if strings.HasPrefix(subnet.Name, subnetPrefix) {
			// get number of free IPs in the subnet
			reuseSubnet, err := isSubnetReusable(ctx, se, subnet, strconv.Itoa(int(account.ID)), "")
			if err != nil {
				logger.Errorf("Error finding empty IP's subnet: tenant project: %s, SN host : %s, region %s. Error : %s", tenantProjectNumber, snHost, tenantProjectRegion, err.Error())
				return nil, err
			}
			if reuseSubnet {
				logger.Debug(fmt.Sprintf("Subnetwork %s already exists in tenant project %s and region %s. Reusing the subnet", subnet.Name, tenantProjectNumber, tenantProjectRegion))
				return &subnet, err
			}
		}
	}
	// either no subnet was found or no subnet was found with enough free IPs sufficient to create a HA pair for pool
	return nil, nil
}

func _isSubnetReusable(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (bool, error) {
	availableIPs, err := findEmptySubnet(ctx, se, subnet, accountId, "")
	if err != nil {
		return false, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	if availableIPs >= totalIPPerHAPair {
		return true, nil
	}
	return false, nil
}

// _findEmptySubnet calculates the number of free IPs in the subnet
func _findEmptySubnet(ctx context.Context, se database.Storage, subnet hyperscaler_models.Subnet, accountId, poolNetwork string) (int, error) {
	// Check if the subnetwork has an IP range
	if subnet.IpCidrRange == "" {
		return 0, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, errors.New("subnetwork does not have an IP range"))
	}

	// Check the number of pools using this subnetwork
	pools, err := getPoolsBySubnetwork(ctx, se, accountId, subnet.Name, poolNetwork)
	if err != nil {
		return 0, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	totalIPs, err := getIPsInSubnet(subnet.IpCidrRange)
	if err != nil {
		return 0, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}

	// 4 IPs are reserved for the network, gateway, broadcast, and subnet address
	freeIPs := totalIPs - 4 - len(pools)*totalIPPerHAPair // Assuming each pool uses 6 IPs
	return freeIPs, nil
}

// _getPoolsBySubnetwork retrieves all pools that are using the specified subnetwork
func _getPoolsBySubnetwork(ctx context.Context, se database.Storage, accountID, subnetworkName, poolNetwork string) ([]*datamodel.PoolView, error) {
	filter := utils.CreateFilterWithConditions(
		utils.NewFilterCondition("account_id", "=", accountID),
		utils.NewFilterCondition("cluster_details->>'subnet_names'", "LIKE", "%"+subnetworkName+"%"),
	)
	if poolNetwork != "" {
		filter.Conditions = append(filter.Conditions, utils.NewFilterCondition("network", "=", poolNetwork))
	}
	filter.SetIncludeDeleted(false)

	pools, err := se.ListPools(ctx, filter)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return pools, nil
}

// _getIPsInSubnet calculates the number of IPs for a given CIDR range
func _getIPsInSubnet(ipCidrRange string) (int, error) {
	// split string by '/' to get the CIDR notation and get last part of the string which is the CIDR notation
	parts := strings.Split(ipCidrRange, "/")
	// get the number of IPs in the subnet
	cidr, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return 0, err
	}
	if cidr < 0 || cidr > 32 {
		return 0, fmt.Errorf("IPCR range must be between 1 and 32. CIDR notation found : %s", ipCidrRange)
	}
	return 1 << (32 - cidr), nil
}
