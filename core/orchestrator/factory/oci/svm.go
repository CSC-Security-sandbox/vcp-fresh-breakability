package oci

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	commonfactory "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	ociworkflows "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/oci"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

// CreateSvm starts an async workflow to create an SVM in the pool identified by PoolOCID.
// Returns the workflow ID as operationID/workRequestId for polling. Before creating, validates cluster state/capacity,
// SVM name (convention + uniqueness), and IP requirements.
func (o *OCIOrchestrator) CreateSvm(ctx context.Context, params *commonparams.CreateSvmParams) (operationID string, err error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	temporal := o.temporal

	if err := validateCreateSvmParams(params); err != nil {
		return "", err
	}

	account, err := commonfactory.GetOrCreateAccount(ctx, se, strings.TrimSpace(params.AccountName))
	if err != nil {
		logger.Error("Failed to get or create account for SVM create", "accountName", params.AccountName, "error", err)
		return "", err
	}

	if params.IPSpace == "" {
		params.IPSpace = "Default"
	}
	// Default protocols for data LIF calculation (iSCSI enabled by default)
	if !params.EnableIscsi && !params.EnableNfs {
		params.EnableIscsi = true
	}

	deploymentName := GenerateDeploymentNameFromOCID(params.PoolOCID)
	conditions := [][]interface{}{
		{"deployment_name = ?", deploymentName},
		{"account_id = ?", account.ID},
	}
	poolView, err := se.GetPoolByName(ctx, conditions)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", customerrors.NewNotFoundErr("pool not found for PoolOCID", nil)
		}
		return "", err
	}
	pool := database.ConvertPoolViewToPool(poolView)

	switch existing, err := se.GetSvmByExternalIdentifier(ctx, strings.TrimSpace(params.SvmExternalIdentifier), pool.AccountID); {
	case err == nil && existing != nil:
		return "", customerrors.NewConflictErr("svmOCID already exists")
	case err != nil && !customerrors.IsNotFoundErr(err):
		return "", err
	}

	// Pre-create validation: cluster state/capacity, SVM name, IP requirements
	if err := validateCreateSvm(ctx, se, params, pool); err != nil {
		return "", err
	}

	workflowID := uuid.NewString()

	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		workflowID,
		workflowengine.CustomerTaskQueue,
		ociworkflows.OCICreateSVMWorkflow,
		nil, // use default workflow run timeout
		params,
		pool,
	)
	if err != nil {
		logger.Error("Failed to start CreateSVM workflow", "workflowID", workflowID, "error", err)
		return "", err
	}
	// Return workflow ID so caller can use generic workRequest tracking API.
	return workflowID, nil
}

// validateSvmDeletionState ensures the SVM is not already in a transitional lifecycle state.
func validateSvmDeletionState(svm *datamodel.Svm) error {
	switch svm.State {
	case string(models.LifeCycleStateDeleting):
		return customerrors.NewConflictErr("SVM delete is already in progress")
	case string(models.LifeCycleStateCreating):
		return customerrors.NewConflictErr("SVM cannot be deleted while creation is in progress")
	}
	return nil
}

// DeleteSvm starts an async workflow to delete an SVM identified by SvmID.
// Returns the workflow ID so the caller can track the operation via workRequest polling.
func (o *OCIOrchestrator) DeleteSvm(ctx context.Context, params *commonparams.DeleteSvmParams) (operationID string, err error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	temporal := o.temporal

	if err := validateDeleteSvmParams(params); err != nil {
		return "", err
	}

	account, err := commonfactory.GetOrCreateAccount(ctx, se, strings.TrimSpace(params.AccountName))
	if err != nil {
		return "", err
	}

	deploymentName := GenerateDeploymentNameFromOCID(strings.TrimSpace(params.PoolOCID))
	poolConditions := [][]interface{}{
		{"deployment_name = ?", deploymentName},
		{"account_id = ?", account.ID},
	}
	poolView, err := se.GetPoolByName(ctx, poolConditions)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", customerrors.NewNotFoundErr("pool not found for PoolOCID", nil)
		}
		return "", err
	}

	targetSvm, err := se.GetSvmByExternalIdentifier(ctx, strings.TrimSpace(params.SvmID), account.ID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return "", customerrors.NewNotFoundErr("svm not found for svmOCID", nil)
		}
		return "", err
	}
	if targetSvm.PoolID != poolView.ID {
		return "", customerrors.NewNotFoundErr("svm not found for svmOCID", nil)
	}

	if err := validateSvmDeletionState(targetSvm); err != nil {
		return "", err
	}

	pool := database.ConvertPoolViewToPool(poolView)

	workflowID := uuid.NewString()
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		workflowID,
		workflowengine.CustomerTaskQueue,
		ociworkflows.OCIDeleteSVMWorkflow,
		nil,
		params,
		targetSvm,
		pool,
	)
	if err != nil {
		logger.Error("Failed to start DeleteSVM workflow", "workflowID", workflowID, "error", err)
		return "", err
	}
	logger.Info("OCIDeleteSVMWorkflow started", "workflowID", workflowID, "svmOCID", params.SvmID)
	return workflowID, nil
}
