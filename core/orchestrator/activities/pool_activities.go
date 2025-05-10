package activities

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

type PoolActivity struct {
	SE *database.Storage
}

const (
	aggregateName  = "aggr1"
	defaultSvmName = "gcnv-default-svm"
	lifNameFormat  = "san_lif_%s"
	enableIscsi    = true
)

var (
	pollInterval          = env.GetUint64("VSA_DEPLOYMENT_POLL_INTERVAL_SEC", 30)
	waitTimeVSADeployment = env.GetUint64("VSA_DEPLOYMENT_TIMEOUT_MIN", 20)
	homePort              = env.GetString("VSA_NODE_HOME_PORT", "e0e")
	region                = env.GetString("REGION", "")
)

func (j *PoolActivity) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := *j.SE
	return se.CreatingPool(ctx, pool)
}

func (j *PoolActivity) FailedPool(ctx context.Context, pool *datamodel.Pool, errMessage string) error {
	se := *j.SE
	pool.State = models.LifeCycleStateError
	pool.StateDetails = errMessage
	return se.UpdatePool(ctx, pool)
}

func (j *PoolActivity) CreatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := *j.SE
	return se.CreatedPool(ctx, pool)
}

func (j *PoolActivity) CreateTenancy(ctx context.Context, params commonparams.CreatePoolParams) (*commonparams.TenancyInfo, error) {
	tenancy, err := FindTenancyAndGetSubnetwork(ctx, params.VendorSubNetID, params.AccountName, &params.Region)
	if err != nil {
		return nil, err
	}
	return tenancy, nil
}

// FindTenancyAndGetSubnetwork finds the tenancy unit and creates a subnetwork for the tenant project
func FindTenancyAndGetSubnetwork(ctx context.Context, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*commonparams.TenancyInfo, error) {
	// need to pass tenantProjectRegion only in case of CBR where region != the regional region as set from env variable
	var gService hyperscaler.GoogleServices
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: log.NewLogger(),
	}
	gService = gcpService

	gcpService.Logger.Debug("gcpService initialized")

	if tenantProjectRegion == nil {
		tenantProjectRegion = &region
	}
	gcpService.Logger.Debug("Calling InitializeClients")
	err := gService.InitializeClients()
	if err != nil || !gService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return nil, errors.New("initialisation of service failed")
	}

	tenantProjectNumber, err := gService.GetTenantProject(consumerVPC, customerProjectNumber, *tenantProjectRegion)
	if err != nil {
		gcpService.Logger.Errorf("Error finding tenancy unit: %v", err)
		return nil, err
	}
	subnet, err := gService.CreateSubnetwork(consumerVPC, *tenantProjectRegion, tenantProjectNumber)
	if err != nil {
		gcpService.Logger.Errorf("Error adding subnetwork: %v", err)
		return nil, err
	}
	snHostProject, network, err := utils.ParseProjectId(subnet.Network)
	if err != nil {
		return nil, err
	}
	subnetwork, err := gcpService.GetSubnetwork(snHostProject, *tenantProjectRegion, subnet.Name)
	if err != nil {
		return nil, err
	}
	gcpService.Logger.Errorf("FindTenancyAndGetSubnetwork: tenantProjectNumber :  %s subnet  :  %s   ", &tenantProjectNumber, subnet)

	return &commonparams.TenancyInfo{
		RegionalTenantProject: tenantProjectNumber,
		Network:               network,
		SubnetworkName:        subnet.Name,
		SnHostProject:         snHostProject,
		Gateway:               subnetwork.GatewayAddress,
	}, nil
}

func (j *PoolActivity) DeployDeploymentManager(ctx context.Context, deploymentName, region, zone, network, subnet, projectId, snHostProject string, size int) (*[]map[string]string, error) {
	return common.DeploymentsInsert(ctx, deploymentName, region, zone, network, subnet, projectId, snHostProject, size)
}

func (j *PoolActivity) SavePoolWithClusterDetails(ctx context.Context, dbPool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	se := *j.SE
	return se.SavePoolWithVsaClusterDetails(ctx, dbPool, cluster)
}

func (j *PoolActivity) SaveNodeDetails(ctx context.Context, pool *datamodel.Pool, cluster *[]map[string]string) error {
	if len(*cluster) == 0 {
		return errors.New("no cluster details provided")
	}
	for _, details := range *cluster {
		node := PrepareNodeFromVsaClusterDetails(details, pool)
		provider := GetProviderByNode(node)

		vsaNode, err := provider.GetNodeByName(node.Name)
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", node.Name, err)
		}

		rec := &datamodel.Node{
			Name:            node.Name,
			EndpointAddress: node.EndpointAddress,
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
			StateDetails:    models.LifeCycleStateAvailableDetails,
			NodeAttributes:  &datamodel.NodeDetails{ExternalUUID: vsaNode.ExternalUUID, InstanceType: node.InstanceType},
			ZoneName:        node.Zone,
			AccountID:       pool.AccountID,
		}
		se := *j.SE
		if _, err = se.CreateNode(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

// PrepareNodeFromVsaClusterDetails builds a Node model from the provided cluster details.
func PrepareNodeFromVsaClusterDetails(details map[string]string, pool *datamodel.Pool) *models.Node {
	return &models.Node{
		Name:            details["Name"],
		EndpointAddress: details["NodeIp"],
		Username:        pool.Username,
		Password:        pool.Password,
		Zone:            details["Zone"],
		InstanceType:    details["MachineType"],
	}
}

func GetProviderByNode(node *models.Node) *vsa.OntapRestProvider {
	// as we don't have any other provider, we can directly return the ontap_rest provider
	return vsa.NewProvider(vsa.ProviderDetails{
		IPAddress: node.EndpointAddress,
		UserName:  node.Username,
		Password:  node.Password,
		// TODO : need to fix once we have certs
		InsecureSkipVerify: true,
	})
}

func (j *PoolActivity) WaitForNodes(ctx context.Context, node *models.Node) error {
	provider := GetProviderByNode(node)
	logger := log.NewLogger()
	startTime := time.Now()
	attempt := 0
	pollIntervalDuration := time.Duration(pollInterval) * time.Second
	timeoutDuration := time.Duration(waitTimeVSADeployment) * time.Minute
	logMsg := "nodes"

	// Create a context that automatically cancels after the timeout.
	timeoutCtx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()
	ticker := time.NewTicker(pollIntervalDuration)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			cancel()
			ticker.Stop()
			return fmt.Errorf("timeout waiting for %s after %v", logMsg, time.Since(startTime))
		case <-ticker.C:
			attempt++
			elapsed := time.Since(startTime)
			logger.Infof("Attempt %d after %v: checking %s...", attempt, elapsed, logMsg)

			ready, err := provider.AreAllNodeUpAndRunning()
			if err != nil {
				logger.Errorf("Error checking %s: %v", logMsg, err)
			}
			if ready {
				logger.Infof("%s is available after %v on attempt %d.", logMsg, elapsed, attempt)
				return nil
			}
		}
	}
}

func (j *PoolActivity) WaitForAggr(ctx context.Context, node *models.Node) error {
	provider := GetProviderByNode(node)
	logger := log.NewLogger()
	startTime := time.Now()
	attempt := 0
	pollInterval := time.Duration(pollInterval) * time.Second
	timeout := time.Duration(waitTimeVSADeployment) * time.Minute
	logMsg := "aggregate " + aggregateName

	// Create a context that automatically cancels after the timeout.
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for %s after %v", logMsg, time.Since(startTime))
		case <-ticker.C:
			attempt++
			elapsed := time.Since(startTime)
			logger.Infof("Attempt %d after %v: checking %s...", attempt, elapsed, logMsg)

			ready, err := provider.IsAggregateOnline(aggregateName)
			if err != nil {
				logger.Errorf("Error checking %s: %v", logMsg, err)
			}
			if ready {
				logger.Infof("%s is available after %v on attempt %d.", logMsg, elapsed, attempt)
				return nil
			}
		}
	}
}

func (j *PoolActivity) GetOntapVersion(ctx context.Context, node *models.Node) (*string, error) {
	provider := GetProviderByNode(node)
	return provider.GetONTAPVersion()
}

func (j *PoolActivity) CreateSvmForPool(ctx context.Context, pool *datamodel.Pool, node *models.Node) (*datamodel.Svm, error) {
	provider := GetProviderByNode(node)
	se := *j.SE
	svmResponse, err := provider.CreateSVM(vsa.CreateSvmParams{Name: defaultSvmName, Protocols: vsa.Protocols{EnableIscsi: enableIscsi}})
	if err != nil {
		return nil, err
	}

	svmRec := &datamodel.Svm{
		Name:      svmResponse.Name,
		AccountID: pool.AccountID,
		PoolID:    pool.ID,
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: svmResponse.ExternalUUID,
			IPSpace:      "Default",
		},
	}
	if _, err = se.CreateSVM(ctx, svmRec); err != nil {
		return nil, err
	}
	return svmRec, nil
}

func (j *PoolActivity) EnableIscsiServiceForSVM(ctx context.Context, node *models.Node, svmUUID string) error {
	provider := GetProviderByNode(node)
	err := provider.IscsiServiceCreate(svmUUID)
	if err != nil {
		return err
	}
	return nil
}

func (j *PoolActivity) CreateLifForSvm(ctx context.Context, node *models.Node, cluster []map[string]string, pool *datamodel.Pool, svm *datamodel.Svm) error {
	provider := GetProviderByNode(node)
	se := *j.SE
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return err
	}
	if len(nodes) < 2 {
		return errors.New("not enough nodes in the cluster to create LIFs for SVM " + svm.Name)
	}

	for i, node := range nodes {
		dataLif, ok := cluster[i]["dataLif"]
		if !ok {
			return fmt.Errorf("missing dataLif in cluster details for node index %d", i)
		}
		ip := strings.Split(dataLif, "/")[0]
		lifName := fmt.Sprintf(lifNameFormat, node.Name)
		lifResponse, err := provider.CreateDataLIF(vsa.CreateLifParams{Name: lifName, SvmName: svm.Name, IpAddress: ip, NodeName: node.Name, HomePort: homePort})
		if err != nil {
			return err
		}
		lifRec := &datamodel.Lif{
			Name:       lifResponse.Name,
			AccountID:  pool.AccountID,
			NodeID:     node.ID,
			LifDetails: &datamodel.LifDetails{ExternalUUID: lifResponse.ExternalUUID},
			IPAddress:  lifResponse.IPAddress,
			SubnetMask: lifResponse.SubnetMask,
		}
		if _, err = se.CreateLif(ctx, lifRec); err != nil {
			return err
		}
	}
	return nil
}

func (j *PoolActivity) GetProxyIP(ctx context.Context, dataLif string) (string, error) {
	ip := strings.Split(dataLif, "/")[0]
	octets := strings.Split(ip, ".")
	if len(octets) != 4 {
		return "", fmt.Errorf("invalid IP address format: %s", ip)
	}
	octets[3] = "1"
	return strings.Join(octets, "."), nil
}

func (j *PoolActivity) CreateNetworkIpRoute(ctx context.Context, node *models.Node, svmName string, gateway string) error {
	provider := GetProviderByNode(node)
	return provider.CreateNetworkIpRoute(vsa.CreateNetworkIPRouteParams{SvmName: svmName, Gateway: gateway})
}

func (j *PoolActivity) GetPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := *j.SE
	return se.GetPool(ctx, pool.UUID, 0)
}

func (j *PoolActivity) DeletingPoolResources(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := *j.SE
	// Update SVM, and Pool States to Deleting
	if err := deletingSVMs(ctx, se, pool); err != nil {
		return nil, err
	}

	if err := deletingNodes(ctx, se, pool); err != nil {
		return nil, err
	}

	if err := se.DeletingPool(ctx, pool); err != nil {
		return nil, fmt.Errorf("failed to delete pool record %s: %w", pool.Name, err)
	}
	return pool, nil
}

func (j *PoolActivity) DeleteDeployment(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	if pool.ClusterDetails.ExternalName == "" {
		return nil, errors.New("pool cannot be deleted with active clusters")
	}
	clusterName := pool.ClusterDetails.ExternalName
	err := common.DeleteDeployment(ctx, pool.Account.Name, clusterName)
	if err != nil {
		return nil, errors.New("failed to delete cluster" + clusterName + " " + err.Error())
	}
	return pool, nil
}

func (j *PoolActivity) ReleaseSubnet(ctx context.Context, pool *datamodel.Pool) error {
	se := *j.SE
	logger := log.NewLogger()
	conditions := [][]interface{}{{"account_id = ?", pool.AccountID}}
	conditions = append(conditions, []interface{}{"network = ?", pool.Network})
	pools, err := se.ListPools(ctx, conditions)
	if err != nil {
		return err
	}
	if len(pools) > 1 {
		logger.Info("Skipping release subnetwork as there are other pools in the same region for the account")
		return nil
	}
	var gService hyperscaler.GoogleServices
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: logger,
	}
	gService = gcpService

	gcpService.Logger.Debug("gcpService initialized")

	gcpService.Logger.Debug("Calling InitializeClients")
	err = gService.InitializeClients()
	if err != nil || !gService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return errors.New("initialisation of service failed")
	}

	consumerVpc := pool.Network
	accountName := pool.Account.Name
	subnetwork := "vsa-" + region

	tenantProjectNumber, err := gService.GetTenantProject(consumerVpc, accountName, region)
	if err != nil {
		gcpService.Logger.Errorf("Error finding tenancy unit: %v", err)
		return err
	}

	err = gService.ReleaseSubnetwork(region, tenantProjectNumber, subnetwork)
	if err != nil {
		gcpService.Logger.Errorf("Error Releasing subnetwork: %v", err)
		return err
	}

	return nil
}

func (j *PoolActivity) DeletePoolResources(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := *j.SE

	// Delete LIFs
	if err := deleteLIFs(ctx, se, pool); err != nil {
		return nil, err
	}

	// Delete SVMs
	if err := deleteSVMs(ctx, se, pool); err != nil {
		return nil, err
	}

	// Delete nodes
	if err := deleteNodes(ctx, se, pool); err != nil {
		return nil, err
	}

	// Delete the pool itself from a database
	if err := se.DeletePool(ctx, pool); err != nil {
		return nil, err
	}
	return pool, nil
}

// deletingSVMs updates svm status to deleting.
func deletingSVMs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the svms associated with the pool
	svms, err := se.GetSvmsByPoolID(ctx, pool.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.New("SVM not found")
		}
		return err
	}
	for _, svm := range svms {
		if err = se.DeletingSVM(ctx, svm); err != nil {
			return fmt.Errorf("failed to update SVM record to deleting %s: %w", svm.Name, err)
		}
	}

	return nil
}

// deletingNodes updates nodes status to deleting.
func deletingNodes(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err)
	}

	// Delete each node
	for _, node := range nodes {
		// Delete the node record from the database
		if err := se.DeletingNode(ctx, node); err != nil {
			return fmt.Errorf("failed to delete node record %s: %w", node.Name, err)
		}
	}

	return nil
}

// deleteSVMs deletes all SVMs and their associated database records.
func deleteSVMs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Get SVMs by pool ID
	svms, err := se.GetSvmsByPoolID(ctx, pool.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.New("SVM not found")
		}
		return err
	}

	for _, svm := range svms {
		// Delete the SVM record from the database
		if svm.DeletedAt != nil && svm.DeletedAt.Valid {
			continue
		}
		if err := se.DeleteSVM(ctx, svm); err != nil {
			return fmt.Errorf("failed to delete SVM record %s: %w", pool.Name, err)
		}
	}

	return nil
}

// deleteLIFs deletes LIFs database records associated with the given Nodes.
func deleteLIFs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err)
	}

	// Delete each LIF
	for _, node := range nodes {
		// Retrieve the LIFs associated with the Node
		lif, err := se.GetLifByNodeID(ctx, node.ID, node.AccountID)
		if err != nil {
			return fmt.Errorf("failed to retrieve LIFs for Node %s: %w", node.Name, err)
		}

		if lif.DeletedAt != nil && lif.DeletedAt.Valid {
			continue
		}

		// Delete the LIF record from the database
		if err := se.DeleteLif(ctx, lif); err != nil {
			return fmt.Errorf("failed to delete LIF record %s: %w", lif.Name, err)
		}
	}

	return nil
}

// deleteNodes deletes node database records associated with the given pool.
func deleteNodes(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err)
	}

	// Delete each node
	for _, node := range nodes {
		// Delete the node record from the database
		if err := se.DeleteNode(ctx, node); err != nil {
			return fmt.Errorf("failed to update node record to deleting %s: %w", node.Name, err)
		}
	}

	return nil
}
