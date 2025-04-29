package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

var (
	minQuotaInBytesPool      = env.GetUint64("MIN_QUOTA_IN_BYTES_POOL", 2199023255552) // 2TiB
	createPool               = _createPool
	createPoolAsync          = _createPoolAsync
	validateCreatePoolParams = _validateCreatePoolParams
	nodeUsername             = env.GetString("VSA_NODE_USERNAME", "")
	nodePassword             = env.GetString("VSA_NODE_PASSWORD", "")
	pollInterval             = env.GetUint64("VSA_DEPLOYMENT_POLL_INTERVAL_SEC", 30)
	waitTimeVSADeployment    = env.GetUint64("VSA_DEPLOYMENT_TIMEOUT_MIN", 20)
	homePort                 = env.GetString("VSA_NODE_HOME_PORT", "e0e")
)

const (
	aggregateName  = "aggr1"
	defaultSvmName = "gcnv-default-svm"
	lifNameFormat  = "%s_block_data_lif"
	enableIscsi    = true
)

// CreatePool creates the specified pool and adds it to the list of pools belonging to the specified owner
func (o *Orchestrator) CreatePool(ctx context.Context, params *CreatePoolParams) (*models.Pool, error) {
	return createPool(ctx, o.storage, params)
}

// createPool creates a new pool and triggers asynchronous creation processes.
func _createPool(ctx context.Context, se database.Storage, params *CreatePoolParams) (*models.Pool, error) {
	// Get or create account
	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, err
	}

	// Validate pool creation parameters.
	if err := validateCreatePoolParams(se, params); err != nil {
		return nil, err
	}

	// Prepare the pool record data.
	dbPool := &datamodel.Pool{
		Name:         params.Name,
		Account:      account,
		AccountID:    account.ID,
		VendorID:     params.VendorID,
		Network:      params.VendorSubNetID,
		SizeInBytes:  int64(params.SizeInBytes),
		CoolAccess:   params.CoolAccess,
		Description:  params.Description,
		ServiceLevel: params.ServiceLevel,
		Username:     nodeUsername,
		Password:     nodePassword,
	}

	// Create the pool in the storage engine.
	pool, err := se.CreatePool(ctx, dbPool)
	if err != nil {
		return nil, err
	}

	// Propagate or attach logger from parent context.
	asyncCtx := context.Background()
	logger := utils.GetLoggerFromContext(ctx)
	asyncCtx = context.WithValue(asyncCtx, middleware.ContextSLoggerKey, logger)

	// Launch asynchronous steps in a separate goroutine.
	go func(asyncCtx context.Context, se database.Storage, params *CreatePoolParams, pool *datamodel.Pool) {
		if err = createPoolAsync(asyncCtx, se, params, pool); err != nil {
			logger.Errorf("Asynchronous pool creation error: %v", err)
		}
	}(asyncCtx, se, params, pool)

	return convertDatastorePoolToModel(pool, account.Name), nil
}

// createPoolAsync performs the asynchronous tasks needed to fully configure a pool.
func _createPoolAsync(ctx context.Context, se database.Storage, params *CreatePoolParams, pool *datamodel.Pool) error {
	clusterName := params.AccountName + "_" + params.Name + "_vsa"
	tenancyDetails, err := getTenancyInfo(ctx, params)
	if err != nil {
		return err
	}
	sizeInGB := utils.BytesToGigabytes(params.SizeInBytes)
	vsaCluster, err := common.DeploymentsInsert(ctx, clusterName, params.Region, params.CurrentZone, tenancyDetails.Network, tenancyDetails.SubnetworkName, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject, sizeInGB)
	if err != nil {
		return err
	}

	// Use the primary node to get the provider.
	provider := getProviderByNode(ctx, prepareNodeFromVsaClusterDetails(vsaCluster[0], pool))

	// Wait for nodes and aggregates.
	if err := waitForNodes(ctx, provider, time.Duration(pollInterval)*time.Second, time.Duration(waitTimeVSADeployment)*time.Minute); err != nil {
		return err
	}
	if err := waitForAggregate(ctx, provider, time.Duration(pollInterval)*time.Second, time.Duration(waitTimeVSADeployment)*time.Minute); err != nil {
		return err
	}

	version, err := provider.GetONTAPVersion()
	if err != nil {
		return err
	}

	// Save cluster details.
	clusterDetails := &datamodel.ClusterDetails{
		ExternalName:          clusterName,
		OntapVersion:          version,
		RegionalTenantProject: tenancyDetails.RegionalTenantProject,
		SnHostProject:         tenancyDetails.SnHostProject,
	}

	if err = se.SavePoolWithVsaClusterDetails(ctx, params.Name, params.AccountName, clusterDetails); err != nil {
		return err
	}
	// Persist node details.
	if err := saveNodeDetails(ctx, se, pool, vsaCluster); err != nil {
		return err
	}

	// Create SVM for the pool.
	svm, err := createSvmForPool(ctx, se, pool, provider)
	if err != nil {
		return err
	}

	// Create LIFs for each node.
	if err = createDataLifForSvm(ctx, se, provider, vsaCluster, pool, svm); err != nil {
		return err
	}

	// Get gateway IP from the first node's dataLif.
	gateway := getProxyIP(strings.Split(vsaCluster[0]["dataLif"], "/")[0])
	return createNetworkIpRoute(provider, svm.Name, gateway)
}

func waitForCondition(ctx context.Context, condition func() (bool, error), logMsg string, pollInterval, timeout time.Duration) error {
	logger := utils.GetLoggerFromContext(ctx)
	startTime := time.Now()
	attempt := 0

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

			ready, err := condition()
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

// waitForNodes polls until nodes are up and running.
func waitForNodes(ctx context.Context, provider vsa.Provider, pollInterval, timeout time.Duration) error {
	return waitForCondition(ctx, func() (bool, error) {
		running, err := provider.AreAllNodeUpAndRunning()
		return running, err
	}, "nodes", pollInterval, timeout)
}

// waitForAggregate polls until the aggregate is online.
func waitForAggregate(ctx context.Context, provider vsa.Provider, pollInterval, timeout time.Duration) error {
	return waitForCondition(ctx, func() (bool, error) {
		running, err := provider.IsAggregateOnline(aggregateName)
		return running, err
	}, "aggregate "+aggregateName, pollInterval, timeout)
}

// createSvmForPool creates and persists an SVM using the provider.
func createSvmForPool(ctx context.Context, se database.Storage, pool *datamodel.Pool, provider vsa.Provider) (*datamodel.Svm, error) {
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

// createNetworkIpRoute sets up the network IP route using the provider.
func createNetworkIpRoute(provider vsa.Provider, svmName string, gateway string) error {
	return provider.CreateNetworkIpRoute(vsa.CreateNetworkIPRouteParams{SvmName: svmName, Gateway: gateway})
}

// createDataLifForSvm creates LIFs for each node associated with the given SVM.
func createDataLifForSvm(ctx context.Context, se database.Storage, provider vsa.Provider, cluster []map[string]string, pool *datamodel.Pool, svm *datamodel.Svm) error {
	nodes, err := se.GetNodeByPoolID(ctx, pool.ID)
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

// saveNodeDetails retrieves nodes via the provider and persists them.
func saveNodeDetails(ctx context.Context, se database.Storage, pool *datamodel.Pool, cluster []map[string]string) error {
	if len(cluster) == 0 {
		return errors.New("no cluster details provided")
	}

	for _, details := range cluster {
		node := prepareNodeFromVsaClusterDetails(details, pool)
		provider := getProviderByNode(ctx, node)

		vsaNode, err := provider.GetNodeByName(node.Name)
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", node.Name, err)
		}

		rec := &datamodel.Node{
			Name:            node.Name,
			EndpointAddress: node.EndpointAddress,
			PoolID:          pool.ID,
			State:           models.LifeCycleStateAvailable,
			StateDetails:    models.LifeCycleStateAvailableDetails,
			NodeAttributes:  &datamodel.NodeDetails{ExternalUUID: vsaNode.ExternalUUID, InstanceType: node.InstanceType},
			ZoneName:        node.Zone,
		}

		if _, err = se.CreateNode(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

// getProxyIP returns an IP address with its last octet set to "1".
func getProxyIP(dataLif string) string {
	ip := strings.Split(dataLif, "/")[0]
	octets := strings.Split(ip, ".")
	if len(octets) != 4 {
		return ""
	}
	octets[3] = "1"
	return strings.Join(octets, ".")
}

// prepareNodeFromVsaClusterDetails builds a Node model from the provided cluster details.
func prepareNodeFromVsaClusterDetails(details map[string]string, pool *datamodel.Pool) *models.Node {
	return &models.Node{
		Name:            details["Name"],
		EndpointAddress: details["NodeIp"],
		Username:        pool.Username,
		Password:        pool.Password,
		Zone:            details["Zone"],
		InstanceType:    details["MachineType"],
	}
}

// GetPool gets the specified pool
func (o *Orchestrator) GetPool(ctx context.Context, poolId string) (*models.Pool, error) {
	se := o.storage

	pool, err := se.GetPool(ctx, poolId)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolToModel(pool, pool.Account.Name), nil
}

func _validateCreatePoolParams(se database.Storage, params *CreatePoolParams) error {
	if params.SizeInBytes < minQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr("Given pool size not supported. Pool size can't be less than " + utils.FmtUint64Bytes(minQuotaInBytesPool))
	}

	return nil
}

// getTenancyInfo retrieves the tenant project, network, and subnet information
func getTenancyInfo(ctx context.Context, params *CreatePoolParams) (*tenancyInfo, error) {
	tp, subnet, err := FindTenancyAndGetSubnetwork(ctx, params.VendorSubNetID, params.AccountName, &params.Region)
	if err != nil {
		return nil, err
	}
	snHostProject, network, err := utils.ParseProjectId(subnet.Network)
	if err != nil {
		return nil, err
	}
	return &tenancyInfo{
		RegionalTenantProject: *tp,
		Network:               network,
		SubnetworkName:        subnet.Name,
		SnHostProject:         snHostProject,
	}, nil
}

// GetPoolByVendorID retrieves a pool by its VendorID.
func (o *Orchestrator) GetPoolByVendorID(ctx context.Context, vendorID string) (*models.Pool, error) {
	se := o.storage
	pool, err := se.GetPoolByVendorID(ctx, vendorID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("pool not found")
		}
		return nil, err
	}
	return convertDatastorePoolToModel(pool, pool.Account.Name), nil
}

// CreatePoolParams describes parameters supplied to CreatePool
type CreatePoolParams struct {
	AccountName             string
	Region                  string
	Name                    string
	Description             string
	VendorID                string
	ServiceLevel            string
	QosType                 string
	Tags                    string
	SizeInBytes             uint64
	CoolAccess              bool
	CurrentZone             string
	VendorSubNetID          string
	Zones                   []string
	CustomThroughputMibps   uint64
	HostUUID                string
	CustomPerformanceParams *CustomPerformanceParams
}

type tenancyInfo struct {
	RegionalTenantProject string
	Network               string
	SubnetworkName        string
	SnHostProject         string
}

// CustomPerformanceParams is used to specify the custom performance parameters for a pool
type CustomPerformanceParams struct {
	Enabled    bool
	Throughput float64
	Iops       int64
}

func convertDatastorePoolToModel(pool *datamodel.Pool, accountName string) *models.Pool {
	return &models.Pool{
		BaseModel: models.BaseModel{
			UUID:      pool.UUID,
			CreatedAt: pool.CreatedAt,
			UpdatedAt: pool.UpdatedAt,
			DeletedAt: DeletedAtOrNil(pool.DeletedAt),
		},
		AccountName:    accountName,
		Name:           pool.Name,
		Description:    pool.Description,
		SizeInBytes:    uint64(pool.SizeInBytes),
		State:          pool.State,
		StateDetails:   pool.StateDetails,
		CoolAccess:     pool.CoolAccess,
		VendorSubNetID: pool.Network,
		ServiceLevel:   pool.ServiceLevel,
	}
}

func DeletedAtOrNil(deletedAt *gorm.DeletedAt) *time.Time {
	if deletedAt != nil && deletedAt.Valid {
		return &deletedAt.Time
	}
	return nil
}

func ListPool(ctx context.Context, params gcpgenserver.V1betaDescribePoolParams, orchestrator *Orchestrator) (gcpgenserver.V1betaDescribePoolRes, error) {
	// 1. Prevalidation steps needs to be implemented

	// 2. Create a job in the database
	job, err := orchestrator.storage.CreateJob(ctx, &datamodel.Job{})
	if err != nil {
		return nil, err
	}

	// 3. Create a workflow execution
	retryPolicy := workflowengine.GetRetryPolicy(&workflowengine.RetryPolicyConfig{})
	_, err = orchestrator.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			ID:                    job.ID,
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			RetryPolicy:           retryPolicy,
		},
		workflows.CreatePool,
		params,
	)
	if err != nil {
		return nil, err
	}

	// 3. Implement workflow response processing
	return nil, nil
}
