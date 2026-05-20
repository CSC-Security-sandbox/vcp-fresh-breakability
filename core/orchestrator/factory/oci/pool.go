package oci

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	ociworkflows "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/oci"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/workflowquery"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

const (
	// ociDeploymentHashLen is the number of hash characters used (prefix length + ociDeploymentHashLen ≤ VLM DeploymentID max length).
	ociDeploymentHashLen = 16
	// ociNameMaxLen is the OCI resource name max length.
	ociNameMaxLen = 255
)

var (
	// ociDeploymentPrefix is the OCI deployment name prefix, from OCI_DEPLOYMENT_NAME_PREFIX (default "ocnv-").
	ociDeploymentPrefix         = env.GetString("OCI_DEPLOYMENT_NAME_PREFIX", "ocnv-")
	ociDeploymentNameValidChars = regexp.MustCompile(`[^a-z0-9-]`)

	// ociThroughputThresholdGBps is the exclusive upper bound on per-pool throughput accepted
	// by UpdatePool, expressed in GBps to match the public API contract (ThroughputGBps).
	// Read once at startup from OCI_THROUGHPUT_THRESHOLD_GBPS; default 5 GBps. The orchestrator
	// converts to MiBps internally for comparison against params.TotalThroughputMibps.
	ociThroughputThresholdGBps = env.GetInt("OCI_THROUGHPUT_THRESHOLD_GBPS", 5)

	// ociNodeCapacityMaxTiB is the inclusive upper bound on per-node data-disk size accepted by
	// UpdatePool, expressed in TiB. The public API field is NodeCapacity.sizeInGiB, so the cap
	// is converted to GiB (cap*1024) at the comparison site. Read once at startup from
	// OCI_NODE_CAPACITY_MAX_TIB; default 425 TiB. A request whose sizeInGiB strictly exceeds
	// the GiB-equivalent of this value is rejected as a 400 user-input error before any DB
	// lookup, so an oversized payload fails fast.
	ociNodeCapacityMaxTiB = env.GetInt("OCI_NODE_CAPACITY_MAX_TIB", 425)

	// ociNodeCapacityMinTiB is the inclusive lower bound on per-node data-disk size accepted by
	// UpdatePool, expressed in TiB. The public API field is NodeCapacity.sizeInGiB, so the
	// floor is converted to GiB (floor*1024) at the comparison site. Read once at startup from
	// OCI_NODE_CAPACITY_MIN_TIB; default 2 TiB. A request whose sizeInGiB is strictly below the
	// GiB-equivalent of this value is rejected as a 400 user-input error before any DB lookup.
	ociNodeCapacityMinTiB = env.GetInt("OCI_NODE_CAPACITY_MIN_TIB", 2)
)

// GenerateDeploymentNameFromOCID generates a deployment name from OCID following OCI naming conventions.
// OCI naming: lowercase alphanumeric and hyphens, max OCINameMaxLen chars.
// Format: OCIDeploymentPrefix (OCI_DEPLOYMENT_NAME_PREFIX) + first OCIDeploymentHashLen chars of hash (truncated to ensure uniqueness and compliance).
func GenerateDeploymentNameFromOCID(ocid string) string {
	if ocid == "" {
		return ""
	}
	hash := sha256.Sum256([]byte(ocid))
	hashStr := hex.EncodeToString(hash[:])
	deploymentName := fmt.Sprintf("%s%s", ociDeploymentPrefix, hashStr[:ociDeploymentHashLen])
	deploymentName = strings.ToLower(deploymentName)
	deploymentName = ociDeploymentNameValidChars.ReplaceAllString(deploymentName, "")
	return clampOCIDeploymentNameLength(deploymentName)
}

// clampOCIDeploymentNameLength truncates to OCINameMaxLen when the name exceeds the OCI limit.
func clampOCIDeploymentNameLength(s string) string {
	if len(s) > ociNameMaxLen {
		return s[:ociNameMaxLen]
	}
	return s
}

// prepareOCICreatePoolParams sets OCI-specific params: deployment name from PoolOCID and VendorID when empty.
// Returns the deployment name for logging.
func prepareOCICreatePoolParams(params *commonparams.CreatePoolParams) string {
	deploymentName := GenerateDeploymentNameFromOCID(params.PoolOCID)
	params.DeploymentName = deploymentName
	if params.VendorID == "" {
		params.VendorID = params.PoolOCID
	}
	return deploymentName
}

func preparePool(
	params *commonparams.CreatePoolParams,
	account *datamodel.Account,
	tieringFullnessThreshold int) *datamodel.Pool {
	poolObj := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: utils.RandomUUID(),
		},
		Name:                   params.Name,
		Account:                account,
		AccountID:              account.ID,
		VendorID:               params.VendorID,
		PoolExternalIdentifier: params.PoolOCID,
		Network:                params.VendorSubNetID,
		SizeInBytes:            int64(params.SizeInBytes),
		AllowAutoTiering:       params.AllowAutoTiering,
		Description:            params.Description,
		ServiceLevel:           params.ServiceLevel,
		QosType:                params.QosType,
		LargeCapacity:          params.LargeCapacity,
		ClusterDetails: datamodel.ClusterDetails{
			CompartmentOCID: params.CompartmentOCID,
		},
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:       int64(params.HotTierSizeInBytes),
			EnableHotTierAutoResize:  params.EnableHotTierAutoResize,
			TieringStatus:            datamodel.TieringStatusResumed,
			TieringFullnessThreshold: int64(tieringFullnessThreshold),
		},
		PoolAttributes: &datamodel.PoolAttributes{
			PrimaryZone:   params.PrimaryZone,
			SecondaryZone: params.SecondaryZone,
			MediatorZone:  params.MediatorZone,
			Labels:        params.Labels,
			IsRegionalHA:  params.IsRegionalHA,
			LdapEnabled:   params.LdapEnabled,
			AccountName:   account.Name,
		},
		APIAccessMode: params.Mode,
	}

	if params.CustomPerformanceParams != nil {
		poolObj.PoolAttributes.ThroughputMibps = params.CustomPerformanceParams.ThroughputMibps
		if params.CustomPerformanceParams.Iops != nil {
			poolObj.PoolAttributes.Iops = *params.CustomPerformanceParams.Iops
		}
	}

	if params.KmsConfig != nil {
		poolObj.KmsConfigID = sql.NullInt64{
			Int64: params.KmsConfig.ID,
			Valid: true,
		}
	}

	if params.ActiveDirectoryId != "" && params.ADExistsInVCP {
		poolObj.ActiveDirectoryID = sql.NullInt64{
			Int64: params.ActiveDirectory.ID,
			Valid: true,
		}
	}

	// Use pre-generated deployment name if provided (e.g., from OCID), otherwise generate one
	if params.DeploymentName != "" {
		poolObj.DeploymentName = params.DeploymentName
	}

	switch env.AuthType {
	case env.USER_CERTIFICATE:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      fmt.Sprintf("%s-secret", poolObj.DeploymentName),
			CertificateID: fmt.Sprintf("%s-cert", poolObj.DeploymentName),
			Password:      "",
			AuthType:      env.USER_CERTIFICATE,
			// Username:      fmt.Sprintf("%s%s", userName, VCP_ADMIN_CERT_UN_SUFFIX),
		}
	case env.USERNAME_PWD_SEC_MGR:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      fmt.Sprintf("%s-secret", poolObj.DeploymentName),
			CertificateID: "",
			Password:      "",
			AuthType:      env.USERNAME_PWD_SEC_MGR,
			Username:      env.OCIOntapAdminUsername,
		}
	default:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      "",
			CertificateID: "",
			Password:      env.NodePassword,
			AuthType:      env.USERNAME_PWD,
			Username:      env.OCIOntapAdminUsername,
		}
	}

	return poolObj
}

// CreatePool creates the specified pool and adds it to the list of pools belonging to the specified owner
func (o *OCIOrchestrator) CreatePool(ctx context.Context, params *commonparams.CreatePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	temporal := o.temporal

	// Get or create account (compartment for OCI)
	account, err := common.GetOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to get or create account", "accountName", params.AccountName, "error", err)
		return nil, "", err
	}

	deploymentName := prepareOCICreatePoolParams(params)
	logger.Infof("Generated deployment name from OCID: %s -> %s", params.PoolOCID, deploymentName)

	poolObj := preparePool(params, account, 0)
	dbPool, err := se.CreatingPool(ctx, poolObj)
	if err != nil {
		logger.Error("Failed to create pool in database", "poolName", params.Name, "error", err)
		return nil, "", err
	}
	logger.Infof("Pool created in CREATING state: uuid=%s, name=%s, deploymentName=%s, state=%s", dbPool.UUID, dbPool.Name, dbPool.DeploymentName, dbPool.State)

	defer func() {
		if err != nil {
			common.CleanupPoolOnError(ctx, se, dbPool, err)
		}
	}()

	workflowID := uuid.NewString()

	// Start the OCI pool creation workflow
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		workflowID,
		workflowengine.CustomerTaskQueue,
		ociworkflows.OCICreatePoolWorkflow,
		workflowengine.GetCreatePoolWorkflowRunTimeout(params.LargeCapacity),
		params,
		dbPool,
	)
	if err != nil {
		logger.Error("Failed to start pool create workflow", "workflowID", workflowID, "error", err)
		return nil, "", err
	}
	logger.Infof("OCI pool creation workflow started successfully: workflowID=%s", workflowID)

	poolView := database.ConvertPoolToPoolView(dbPool)
	return common.ConvertDatastorePoolToModel(poolView, account.Name), workflowID, nil
}

// updatePool starts OCIUpdatePoolWorkflow for the given params. Each invocation uses only this
// call's params (size, throughput, etc.); a later call may use a different payload. Validation
// compares the request to the current persisted pool (no shrink).
//
// Allowed entry states are READY and ERROR. ERROR is allowed because after a failed workflow
// ErroredResource flips the pool to ERROR; the client may then retry with the same or a different
// payload. Any in-progress state (UPDATING, CREATING, DELETING, etc.) is rejected with 409
// because an operation is already in flight. DELETED is rejected with 400 because the pool no
// longer exists from the caller's point of view.
//
// Note: a worker crash mid-update can leave the row stuck in UPDATING. With this policy the
// pool will stay 409 until an operator/background reconciler flips it to ERROR.
func (o *OCIOrchestrator) updatePool(ctx context.Context, params *commonparams.UpdatePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	temporal := o.temporal

	if params.PoolExternalIdentifier == "" {
		logger.Error("PoolExternalIdentifier is required", "error", customerrors.NewBadRequestErr("PoolExternalIdentifier is required"))
		return nil, "", customerrors.NewBadRequestErr("PoolExternalIdentifier is required")
	}

	account, err := common.GetAccount(ctx, se, params.AccountName)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err) {
			logger.Error("Account not found", "accountName", params.AccountName, "error", err)
			accountName := params.AccountName
			return nil, "", customerrors.NewNotFoundErr("account", &accountName)
		}
		logger.Error("Failed to get account", "accountName", params.AccountName, "error", err)
		return nil, "", err
	}

	conditions := [][]interface{}{{"pool_external_identifier = ?", params.PoolExternalIdentifier}}
	conditions = append(conditions, []interface{}{"account_id = ?", account.ID})
	poolView, err := se.GetPoolByName(ctx, conditions)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err) {
			logger.Error("Pool not found", "poolExternalIdentifier", params.PoolExternalIdentifier, "error", err)
			return nil, "", customerrors.NewNotFoundErr("pool", &params.PoolExternalIdentifier)
		}
		return nil, "", err
	}

	if err = validateUpdatePoolState(poolView.State, params.PoolExternalIdentifier); err != nil {
		logger.Error("Invalid pool state", "poolExternalIdentifier", params.PoolExternalIdentifier, "error", err)
		return nil, "", err
	}

	pool := database.ConvertPoolViewToPool(poolView)

	if err = validateUpdatePoolSingleAD(pool); err != nil {
		logger.Error("Invalid pool single AD", "poolExternalIdentifier", params.PoolExternalIdentifier, "error", err)
		return nil, "", err
	}

	if err = validateUpdatePoolThroughput(params); err != nil {
		logger.Error("Invalid pool throughput", "poolExternalIdentifier", params.PoolExternalIdentifier, "error", err)
		return nil, "", err
	}

	if err = validateUpdatePoolNodeCapacities(ctx, se, pool, params); err != nil {
		logger.Error("Invalid pool node capacities", "poolExternalIdentifier", params.PoolExternalIdentifier, "error", err)
		return nil, "", err
	}

	pool, err = se.UpdatingPool(ctx, pool)
	if err != nil {
		return nil, "", err
	}

	defer func() {
		if err != nil {
			if _, rbErr := se.ErroredResource(ctx, pool, err.Error()); rbErr != nil {
				logger.Error("Failed to rollback pool to ERROR state", "poolExternalIdentifier", params.PoolExternalIdentifier, "error", rbErr)
			}
		}
	}()

	workflowID := uuid.NewString()
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		workflowID,
		workflowengine.CustomerTaskQueue,
		ociworkflows.OCIUpdatePoolWorkflow,
		workflowengine.GetUpdatePoolWorkflowRunTimeout(false),
		params,
		pool,
	)
	if err != nil {
		logger.Error("Failed to start pool update workflow", "workflowID", workflowID, "error", err)
		return nil, "", err
	}

	logger.Info("OCI pool update workflow started", "workflowID", workflowID, "poolExternalIdentifier", params.PoolExternalIdentifier)

	poolView = database.ConvertPoolToPoolView(pool)
	return common.ConvertDatastorePoolToModel(poolView, account.Name), workflowID, nil
}

// validateUpdatePoolState gates UpdatePool on the persisted lifecycle state of the pool.
//
//   - READY / ERROR     → nil (allowed; ERROR is the post-failure retry path)
//   - DELETED           → 400 (pool is gone, nothing to update)
//   - any in-progress   → 409 (UPDATING, CREATING, DELETING, PREPARING, MIGRATING — an operation
//     is already running for this pool)
//   - anything else     → 409 (defensive catch-all so unexpected states do not silently fall
//     through to the workflow start)
//
// Callers receive a typed customerrors error so the endpoint layer can map it to the correct
// HTTP status via IsBadRequestErr / IsConflictErr.
func validateUpdatePoolState(state, poolExternalIdentifier string) error {
	switch state {
	case models.LifeCycleStateREADY, models.LifeCycleStateError:
		return nil
	case models.LifeCycleStateDeleted:
		return customerrors.NewBadRequestErr(fmt.Sprintf("pool %s is deleted and cannot be updated", poolExternalIdentifier))
	case models.LifeCycleStateUpdating,
		models.LifeCycleStateCreating,
		models.LifeCycleStateDeleting,
		models.LifeCycleStatePreparing,
		models.LifeCycleStateMigrating:
		return customerrors.NewConflictErr(fmt.Sprintf("pool cannot be updated: an operation is already in progress (state=%s)", state))
	default:
		return customerrors.NewConflictErr(fmt.Sprintf("pool cannot be updated in state %s, must be READY or ERROR after a failed update", state))
	}
}

// validateUpdatePoolThroughput enforces the per-pool throughput contract for UpdatePool.
// The check is two-part:
//
//  1. params.TotalThroughputMibps must be non-zero. The endpoint layer already rejects a
//     ThroughputGBps <= 0 in the request (errMsgThroughputGBpsNotPositive); reaching this
//     function with a zero internal MiBps therefore means the caller did not include the
//     throughput field at all, which UpdatePool currently treats as a required field.
//  2. params.TotalThroughputMibps must be strictly less than the configured cap, sourced from
//     OCI_THROUGHPUT_THRESHOLD_GBPS (default 5 GBps). The cap is held in GBps to match the
//     public API contract; we convert to MiBps once here using workflowquery.MiBpsPerGBps so
//     the comparison happens in the same unit as the persisted/orchestrator field.
//
// Returns customerrors.NewBadRequestErr (mapped to HTTP 400 by the endpoint layer) on
// violation, or nil when the throughput is acceptable.
func validateUpdatePoolThroughput(params *commonparams.UpdatePoolParams) error {
	if params.TotalThroughputMibps == 0 {
		return customerrors.NewBadRequestErr("throughputGBps must be non-zero")
	}
	capMibps := int64(float64(ociThroughputThresholdGBps) * workflowquery.MiBpsPerGBps)
	if params.TotalThroughputMibps >= capMibps {
		return customerrors.NewBadRequestErr(
			fmt.Sprintf("throughputGBps must be less than %d GBps", ociThroughputThresholdGBps))
	}
	return nil
}

// validateUpdatePoolSingleAD rejects updates targeting pools that span multiple availability domains.
// OCI pool update is currently supported only for single-AD pools; multi-AD (regional HA) pools
// must be onboarded explicitly. The signal is pool_attributes.is_regional_ha persisted at create time.
// A nil PoolAttributes is treated as single AD (default zero value of IsRegionalHA is false).
func validateUpdatePoolSingleAD(pool *datamodel.Pool) error {
	if pool == nil || pool.PoolAttributes == nil {
		return nil
	}
	if pool.PoolAttributes.IsRegionalHA {
		return customerrors.NewUserInputValidationErr(
			"pool update is supported only for single-AD pools; pool is configured as regional HA (multi-AD)")
	}
	return nil
}

// validateUpdatePoolNodeCapacities verifies, for every entry in params.NodeCapacities, that:
//  1. sizeInGiB falls within the configured per-node bounds [ociNodeCapacityMinTiB,
//     ociNodeCapacityMaxTiB] (sourced from OCI_NODE_CAPACITY_MIN_TIB / OCI_NODE_CAPACITY_MAX_TIB;
//     defaults 2 TiB and 425 TiB respectively, compared as bound*1024 GiB). This is a pure
//     params check and runs before any DB lookup so an out-of-range payload fails fast with
//     a 400.
//  2. node_uuid is a valid node UUID on this pool (must exist in the nodes table for pool.ID),
//     otherwise the whole payload is rejected. This makes "wrong pool" / "stale UUID" attempts
//     fail fast instead of being interpreted downstream.
//  3. sizeInGiB is not smaller than the node's currently-persisted total data-disk size,
//     parsed from pool.VLMConfig. The per-node total is summed across vm.DataDisks[].Size —
//     the same field used by dataDiskTotals in utils/workflowquery/vm_metadata.go — so the
//     comparison is consistent with the size reported by the GetWorkflow endpoint.
//
// Uniqueness of node_uuid within the request is enforced earlier at the endpoint layer
// (validateUpdatePoolNodeCapacityUniqueness), so this function does not duplicate that check.
// HA-pair partner completeness is intentionally NOT enforced here.
//
// Returns:
//   - nil when params.NodeCapacities is empty (caller is updating throughput/DEC only and has
//     not requested any per-node size change), so there is nothing to validate;
//   - a customerrors.NewUserInputValidationErr (→ 400) for user-attributable failures
//     (size below minimum, size-cap exceeded, unknown UUID, shrink attempt);
//   - a plain error (→ 500) for internal failures: DB lookup error, malformed stored VLM
//     config, a nil pool, or an established pool whose VLMConfig is empty (treated as a
//     stored-state invariant violation rather than silently skipping the no-shrink check,
//     which could otherwise let a bad request through unvalidated).
func validateUpdatePoolNodeCapacities(
	ctx context.Context,
	se database.Storage,
	pool *datamodel.Pool,
	params *commonparams.UpdatePoolParams,
) error {
	if pool == nil {
		return fmt.Errorf("validateUpdatePoolNodeCapacities: pool is nil")
	}
	if len(params.NodeCapacities) == 0 {
		return nil
	}

	// Bounds are configured in TiB but the API field is sizeInGiB, so convert TiB→GiB (×1024)
	// at the comparison site.
	minGiB := int64(ociNodeCapacityMinTiB) * 1024
	maxGiB := int64(ociNodeCapacityMaxTiB) * 1024
	for _, nc := range params.NodeCapacities {
		if nc.SizeInGiB < minGiB {
			return customerrors.NewUserInputValidationErr(
				fmt.Sprintf("nodeCapacities.sizeInGiB %d for node_uuid %q is below the configured minimum of %d GiB (%d TiB)",
					nc.SizeInGiB, nc.NodeUUID, minGiB, ociNodeCapacityMinTiB))
		}
		if nc.SizeInGiB > maxGiB {
			return customerrors.NewUserInputValidationErr(
				fmt.Sprintf("nodeCapacities.sizeInGiB %d for node_uuid %q exceeds the configured maximum of %d GiB (%d TiB)",
					nc.SizeInGiB, nc.NodeUUID, maxGiB, ociNodeCapacityMaxTiB))
		}
	}

	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("validateUpdatePoolNodeCapacities: list nodes for pool %q (id=%d): %w", pool.UUID, pool.ID, err)
	}

	nodeByUUID := make(map[string]*datamodel.Node, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if n.UUID != "" {
			nodeByUUID[n.UUID] = n
		}
	}

	for _, nc := range params.NodeCapacities {
		u := strings.TrimSpace(nc.NodeUUID)
		if _, ok := nodeByUUID[u]; !ok {
			return customerrors.NewUserInputValidationErr(
				fmt.Sprintf("nodeCapacities.node_uuid %q is not a valid node for pool %q", nc.NodeUUID, pool.PoolExternalIdentifier))
		}
	}

	if strings.TrimSpace(pool.VLMConfig) == "" {
		return fmt.Errorf("validateUpdatePoolNodeCapacities: pool %q (id=%d) has empty VLM config; cannot verify no-shrink invariant", pool.UUID, pool.ID)
	}

	var cfg vlm.VLMConfig
	if err := json.Unmarshal([]byte(pool.VLMConfig), &cfg); err != nil {
		return fmt.Errorf("validateUpdatePoolNodeCapacities: parse stored VLM config for pool %q (id=%d): %w", pool.UUID, pool.ID, err)
	}

	// Map current per-node data-disk size by node Name. Empty HostName entries are skipped —
	// they cannot be matched back to a node row and so cannot anchor a shrink check.
	// Node.Name is populated from vmConfig.HostName at create time (see activities.SaveNodeDetails),
	// so request membership (keyed by Node.Name → vmCfg.HostName) lines up with stored disk totals.
	sizeByNodeName := make(map[string]int64)
	for _, pair := range cfg.Cloud.HAPairs {
		for _, vmCfg := range []vlm.VMConfig{pair.VM1, pair.VM2} {
			if vmCfg.HostName == "" {
				continue
			}
			var total int64
			for _, d := range vmCfg.DataDisks {
				total += int64(d.Size)
			}
			sizeByNodeName[vmCfg.HostName] = total
		}
	}

	for _, nc := range params.NodeCapacities {
		node := nodeByUUID[strings.TrimSpace(nc.NodeUUID)]
		if node == nil || node.Name == "" {
			continue
		}
		current, ok := sizeByNodeName[node.Name]
		if !ok {
			continue
		}
		if nc.SizeInGiB < current {
			return customerrors.NewUserInputValidationErr(
				fmt.Sprintf("nodeCapacities.sizeInGiB cannot be reduced for node_uuid %q (name=%q): current=%d GiB, requested=%d GiB",
					nc.NodeUUID, node.Name, current, nc.SizeInGiB))
		}
	}

	return nil
}

func (o *OCIOrchestrator) DeletePool(ctx context.Context, params *commonparams.DeletePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	temporal := o.temporal
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)

	// Validate required request fields before database access.
	if params.PoolOCID == "" {
		return nil, "", customerrors.NewBadRequestErr("PoolOCID is required")
	}

	account, err := common.GetAccount(ctx, se, params.AccountName)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err) {
			accountName := params.AccountName
			return nil, "", customerrors.NewNotFoundErr("account", &accountName)
		}
		logger.Error("Failed to get account", "accountName", params.AccountName, "error", err)
		return nil, "", err
	}

	conditions := [][]interface{}{{"pool_external_identifier = ?", params.PoolOCID}}
	conditions = append(conditions, []interface{}{"account_id = ?", account.ID})
	poolView, err := se.GetPoolByName(ctx, conditions)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err) {
			poolOCID := params.PoolOCID
			return nil, "", customerrors.NewNotFoundErr("pool", &poolOCID)
		}
		return nil, "", err
	}

	if utils.IsTransitionalState(poolView.State) {
		return nil, "", customerrors.NewConflictErr(fmt.Sprintf("pool is in transition state and cannot be deleted, state: %s", poolView.State))
	}

	pool := database.ConvertPoolViewToPool(poolView)

	// Update pool state to DELETING
	if err = se.DeletingPool(ctx, pool); err != nil {
		return nil, "", err
	}

	workflowID := uuid.NewString()
	params.PoolID = pool.UUID
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		workflowID,
		workflowengine.CustomerTaskQueue,
		ociworkflows.OCIDeletePoolWorkflow,
		nil,
		params,
		pool,
	)
	if err != nil {
		return nil, "", err
	}

	poolView.State = pool.State
	poolView.StateDetails = pool.StateDetails
	return common.ConvertDatastorePoolToModel(poolView, account.Name), workflowID, nil
}
