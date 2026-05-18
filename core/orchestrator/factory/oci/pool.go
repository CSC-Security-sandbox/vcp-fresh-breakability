package oci

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
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
