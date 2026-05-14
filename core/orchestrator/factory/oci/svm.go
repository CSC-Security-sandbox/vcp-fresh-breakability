package oci

import (
	"context"
	"errors"
	"strings"
	"time"

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

	exists, err := se.SvmExistsByExternalIdentifier(ctx, strings.TrimSpace(params.SvmExternalIdentifier), pool.AccountID)
	if err != nil {
		logger.Error("Failed to check for existing SVM by OCID", "svmOCID", params.SvmExternalIdentifier, "error", err)
		return "", err
	}
	if exists {
		return "", customerrors.NewConflictErr("svmOCID already exists")
	}
	logger.Info("No existing SVM with same OCID, proceeding to create",
		"svmOCID", params.SvmExternalIdentifier, "poolOCID", params.PoolOCID)

	// Pre-create validation: cluster state/capacity, SVM name, IP requirements.
	if err := validateCreateSvm(ctx, se, params, pool); err != nil {
		return "", err
	}

	svmRow := &datamodel.Svm{
		Name:                  params.Name,
		SvmExternalIdentifier: strings.TrimSpace(params.SvmExternalIdentifier),
		AccountID:             pool.AccountID,
		PoolID:                pool.ID,
	}
	preallocatedSvm, err := se.CreateSvmInCreatingState(ctx, svmRow)
	if err != nil {
		logger.Error("Failed to pre-allocate SVM in CREATING state", "svmOCID", params.SvmExternalIdentifier, "error", err)
		return "", err
	}
	logger.Info("SVM pre-allocated in CREATING state", "svmUUID", preallocatedSvm.UUID, "svmOCID", preallocatedSvm.SvmExternalIdentifier)

	defer func() {
		if err != nil && preallocatedSvm != nil {
			compCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if compErr := se.ErroredSVM(compCtx, preallocatedSvm, models.LifeCycleStateCreationErrorDetails); compErr != nil {
				logger.Error("Failed to mark pre-allocated SVM as ERROR after orchestrator-level failure",
					"svmUUID", preallocatedSvm.UUID, "compensationError", compErr)
			}
		}
	}()

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
		preallocatedSvm,
	)
	if err != nil {
		logger.Error("Failed to start CreateSVM workflow", "workflowID", workflowID, "error", err)
		return "", err
	}
	// Return workflow ID so caller can use generic workRequest tracking API.
	return workflowID, nil
}

// validateSvmDeletionState ensures the SVM is in a state that allows deletion.
func validateSvmDeletionState(svm *datamodel.Svm) error {
	switch svm.State {
	case models.LifeCycleStateDeleted:
		return customerrors.NewNotFoundErr("svm deleted already", nil)
	case models.LifeCycleStateDeleting:
		return customerrors.NewConflictErr("SVM delete is already in progress")
	case models.LifeCycleStateCreating:
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

	// Atomically flip the SVM to DELETING here in the orchestrator
	deletingSvm, err := se.TransitionSvmToDeleting(ctx, targetSvm)
	if err != nil {
		if customerrors.IsConflictErr(err) {
			return "", err
		}
		logger.Error("Failed to transition SVM to DELETING", "svmOCID", params.SvmID, "error", err)
		return "", err
	}
	logger.Info("SVM transitioned to DELETING", "svmUUID", deletingSvm.UUID, "svmOCID", params.SvmID)

	defer func() {
		if err != nil && deletingSvm != nil {
			compCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if compErr := se.ErroredSVM(compCtx, deletingSvm, models.LifeCycleStateDeletionErrorDetails); compErr != nil {
				logger.Error("Failed to mark SVM as ERROR after orchestrator-level delete failure",
					"svmUUID", deletingSvm.UUID, "compensationError", compErr)
			}
		}
	}()

	workflowID := uuid.NewString()
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		workflowID,
		workflowengine.CustomerTaskQueue,
		ociworkflows.OCIDeleteSVMWorkflow,
		nil,
		params,
		deletingSvm,
		pool,
	)
	if err != nil {
		logger.Error("Failed to start DeleteSVM workflow", "workflowID", workflowID, "error", err)
		return "", err
	}
	logger.Info("OCIDeleteSVMWorkflow started", "workflowID", workflowID, "svmOCID", params.SvmID)
	return workflowID, nil
}
