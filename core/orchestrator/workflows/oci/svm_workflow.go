package oci

import (
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// OCICreateSVMResult is the workflow return value, serialized by Temporal and
// extracted by the workRequest polling API to surface SVM metadata on completion.
type OCICreateSVMResult struct {
	Name    string                  `json:"name"`
	SvmOCID string                  `json:"svmOCID"`
	Lifs    []OCICreateSVMLifResult `json:"lifs,omitempty"`
}

type OCICreateSVMLifResult struct {
	Name      string   `json:"name"`
	IP        string   `json:"ipAddress"`
	Node      string   `json:"node"`
	Protocols []string `json:"protocols"`
}

// lifTypeToProtocols maps internal VLM LIF types to the externally-exposed list
// of data protocols served on that LIF.
var lifTypeToProtocols = map[vlm.VSALIFType][]string{
	vlm.LIFTypeSan: {"iscsi", "nvme"},
	vlm.LIFTypeNas: {"nfs", "cifs", "s3"},
}

type ociCreateSVMWorkflow struct {
	workflows.BaseWorkflow
}

var _ workflows.WorkflowInterface = &ociCreateSVMWorkflow{}

// OCICreateSVMWorkflow creates an SVM in an existing OCI pool (cluster) via VLM CreateVSASVM child workflow.
// On success it returns OCICreateSVMResult so the workRequest polling API can surface SVM metadata.
func OCICreateSVMWorkflow(ctx workflow.Context, params *common.CreateSvmParams, pool *datamodel.Pool) (*OCICreateSVMResult, error) {
	wf := new(ociCreateSVMWorkflow)
	log := util.GetLogger(ctx)
	if err := wf.Setup(ctx, params); err != nil {
		return nil, err
	}

	wf.Status = workflows.WorkflowStatusRunning
	result, errRun := wf.Run(ctx, params, pool)
	if errRun != nil {
		log.Errorf("error in ociCreateSVMWorkflow: %v", errRun)
		wf.Status = workflows.WorkflowStatusFailed
		return nil, errRun
	}
	wf.Status = workflows.WorkflowStatusCompleted
	svmResult, ok := result.(*OCICreateSVMResult)
	if !ok {
		return nil, fmt.Errorf("unexpected result type %T from ociCreateSVMWorkflow.Run", result)
	}
	return svmResult, nil
}

func (wf *ociCreateSVMWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	params, ok := input.(*common.CreateSvmParams)
	if !ok {
		return fmt.Errorf("OCICreateSVMWorkflow.Setup: unexpected input type %T, want *common.CreateSvmParams", input)
	}
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = params.AccountName
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	wf.Logger = util.GetLogger(ctx)
	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{ID: wf.ID, Status: wf.Status, CustomerID: wf.CustomerID}, nil
	})
}

func (wf *ociCreateSVMWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	if len(args) < 2 {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCICreateSVMWorkflow.Run: expected 2 args, got %d", len(args)))
	}
	params, ok := args[0].(*common.CreateSvmParams)
	if !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCICreateSVMWorkflow.Run: args[0] has unexpected type %T, want *common.CreateSvmParams", args[0]))
	}
	pool, ok := args[1].(*datamodel.Pool)
	if !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCICreateSVMWorkflow.Run: args[1] has unexpected type %T, want *datamodel.Pool", args[1]))
	}
	logger := util.GetLogger(ctx)
	rollbackManager := common.NewRollbackManager()
	var err error

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError", "NonRetryableErr"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	dbHbCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(dbHeartbeatTimeoutSec)*time.Second)

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackCtx := workflow.WithActivityOptions(disconnectedCtx, ao)
			rollbackManager.ExecuteRollback(rollbackCtx, err)
		}
	}()

	poolActivity := &activities.PoolActivity{}
	svmActivity := &activities.SvmActivity{}

	var vlmConfig *vlm.VLMConfig
	err = workflow.ExecuteActivity(ctx, poolActivity.ParseVlmConfig, pool).Get(ctx, &vlmConfig)
	if err != nil {
		logger.Errorf("Failed to parse VLM config: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	// Pre-allocate the SVM DB row in CREATING right after VLM config is parsed so
	// the SVM is visible to customers for the entire create lifecycle.
	svm := &datamodel.Svm{}
	err = workflow.ExecuteActivity(dbHbCtx, svmActivity.CreateSvmInCreatingState, pool, params.Name, params.SvmExternalIdentifier).Get(dbHbCtx, svm)
	if err != nil {
		logger.Errorf("Failed to pre-allocate SVM in CREATING state: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}
	logger.Infof("SVM pre-allocated in CREATING state: svmName=%s, svmUUID=%s", svm.Name, svm.UUID)

	// State is now CREATING — register the rollback so any subsequent failure flips
	// the DB row to ERROR with a customer-facing reason instead of leaving it stuck.
	rollbackManager.AddActivity(svmActivity.MarkSvmAsErroredForCreation, svm)

	adminPassword := ociOntapAdminPassword
	if pool.PoolCredentials != nil && pool.PoolCredentials.Password != "" {
		adminPassword = pool.PoolCredentials.Password
	}
	credConfig := vlm.OntapCredentials{
		AdminPassword: adminPassword,
		Certificate:   vlm.OntapCertificate{},
	}

	createSVMRequest := &vlm.CreateSVMRequest{
		Name:             params.Name,
		VLMConfig:        *vlmConfig,
		OntapCredentials: credConfig,
	}
	vsaClientWorkflowManager := workflows.GetNewVSAClientWorkflowManager()
	createSVMResponse, vlmErr := vsaClientWorkflowManager.CreateVSASVM(ctx, createSVMRequest)
	if vlmErr != nil {
		err = vlmErr
		logger.Errorf("Failed to create SVM via VLM child workflow: %v", vlmErr)
		return nil, workflows.ConvertToVSAError(vlmErr)
	}
	logger.Infof("SVM created successfully via VLM: %s", params.Name)

	// Register rollback: tear down the SVM we just created on the ONTAP cluster via
	// the VLM delete child workflow so a later DB-persistence failure does not leave
	// orphaned cluster state.
	deleteSVMRequest := &vlm.DeleteSVMRequest{
		Name:             params.Name,
		VLMConfig:        createSVMResponse.VLMConfig,
		OntapCredentials: credConfig,
	}
	vlmWorkerQueue := vlm.GetVLMWorkerQueue(logger, pool.Account.Name)
	rollbackManager.AddWorkflow(vlmWorkerQueue, vlm.DeleteVSASVMWorkflowName, deleteSVMRequest)

	err = workflow.ExecuteActivity(dbHbCtx, svmActivity.SaveSVMAndLifDataWithOCID, pool, createSVMResponse.VLMConfig, params.Name, params.SvmExternalIdentifier).Get(dbHbCtx, svm)
	if err != nil {
		logger.Errorf("Failed to save SVM and LIF data: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}
	logger.Infof("SVM and LIF data saved: svmName=%s, svmUUID=%s", svm.Name, svm.SvmDetails.ExternalUUID)

	result := buildCreateSVMResult(params, svm, &createSVMResponse.VLMConfig)
	return result, nil
}

func buildCreateSVMResult(params *common.CreateSvmParams, svm *datamodel.Svm, vlmCfg *vlm.VLMConfig) *OCICreateSVMResult {
	res := &OCICreateSVMResult{
		Name:    svm.Name,
		SvmOCID: params.SvmExternalIdentifier,
	}

	svmCfg, ok := vlmCfg.Svm[params.Name]
	if !ok {
		return res
	}
	for lifType, lifs := range svmCfg.SVMLIFs {
		protos, ok := lifTypeToProtocols[lifType]
		if !ok {
			continue
		}
		for _, l := range lifs {
			ip := strings.Split(l.IP, "/")[0]
			res.Lifs = append(res.Lifs, OCICreateSVMLifResult{
				Name:      l.Name,
				IP:        ip,
				Node:      l.HomeNode,
				Protocols: protos,
			})
		}
	}
	return res
}

// ---------------------------------------------------------------------------
// OCIDeleteSVMWorkflow
// ---------------------------------------------------------------------------

type ociDeleteSVMWorkflow struct {
	workflows.BaseWorkflow
}

var _ workflows.WorkflowInterface = &ociDeleteSVMWorkflow{}

// OCIDeleteSVMWorkflow soft-deletes an SVM asynchronously so the caller can track progress via workflowId.
// It removes the SVM from the ONTAP cluster via vlm.DeleteVSASVM and then flips the DB row to DELETED.
func OCIDeleteSVMWorkflow(ctx workflow.Context, params *common.DeleteSvmParams, svm *datamodel.Svm, pool *datamodel.Pool) error {
	wf := new(ociDeleteSVMWorkflow)
	log := util.GetLogger(ctx)
	if err := wf.setupDelete(ctx, params); err != nil {
		return err
	}

	wf.Status = workflows.WorkflowStatusRunning
	_, errRun := wf.Run(ctx, params, svm, pool)
	if errRun != nil {
		log.Errorf("error in ociDeleteSVMWorkflow: %v", errRun)
		wf.Status = workflows.WorkflowStatusFailed
		return errRun
	}
	wf.Status = workflows.WorkflowStatusCompleted
	return nil
}

func (wf *ociDeleteSVMWorkflow) setupDelete(ctx workflow.Context, params *common.DeleteSvmParams) error {
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = params.AccountName
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	wf.Logger = util.GetLogger(ctx)
	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{ID: wf.ID, Status: wf.Status, CustomerID: wf.CustomerID}, nil
	})
}

func (wf *ociDeleteSVMWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	params, ok := input.(*common.DeleteSvmParams)
	if !ok {
		return fmt.Errorf("OCIDeleteSVMWorkflow.Setup: unexpected input type %T, want *common.DeleteSvmParams", input)
	}
	return wf.setupDelete(ctx, params)
}

func (wf *ociDeleteSVMWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	if len(args) < 3 {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCIDeleteSVMWorkflow.Run: expected 3 args, got %d", len(args)))
	}
	if _, ok := args[0].(*common.DeleteSvmParams); !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCIDeleteSVMWorkflow.Run: args[0] has unexpected type %T, want *common.DeleteSvmParams", args[0]))
	}
	svm, ok := args[1].(*datamodel.Svm)
	if !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCIDeleteSVMWorkflow.Run: args[1] has unexpected type %T, want *datamodel.Svm", args[1]))
	}
	pool, ok := args[2].(*datamodel.Pool)
	if !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCIDeleteSVMWorkflow.Run: args[2] has unexpected type %T, want *datamodel.Pool", args[2]))
	}
	logger := util.GetLogger(ctx)
	rollbackManager := common.NewRollbackManager()
	var err error

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError", "NonRetryableErr"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	dbHbCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(dbHeartbeatTimeoutSec)*time.Second)

	// Run rollbacks on a disconnected context so they execute even if the parent ctx
	// is cancelled (mirrors OCICreateSVMWorkflow). Any registered rollbacks fire only
	// when err != nil at return time, so a clean success path skips them entirely.
	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackCtx := workflow.WithActivityOptions(disconnectedCtx, ao)
			rollbackManager.ExecuteRollback(rollbackCtx, err)
		}
	}()

	poolActivity := &activities.PoolActivity{}
	svmActivity := &activities.SvmActivity{}

	logger.Infof("Deleting SVM: svmOCID=%s", svm.SvmExternalIdentifier)

	// Step 1: parse VLM config from the pool.
	var vlmConfig *vlm.VLMConfig
	err = workflow.ExecuteActivity(ctx, poolActivity.ParseVlmConfig, pool).Get(ctx, &vlmConfig)
	if err != nil {
		logger.Errorf("Failed to parse VLM config: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	// Step 2: own the DELETING transition inside the workflow so the rollback path is uniform
	err = workflow.ExecuteActivity(dbHbCtx, svmActivity.MarkSvmDeleting, svm).Get(dbHbCtx, nil)
	if err != nil {
		logger.Errorf("Failed to mark SVM as deleting: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	// State is now DELETING — register the rollback so any subsequent failure flips it to ERROR.
	rollbackManager.AddActivity(svmActivity.MarkSvmAsErroredForDeletion, svm)

	// Step 3: remove the SVM from the ONTAP cluster via VLM.
	adminPassword := ociOntapAdminPassword
	if pool.PoolCredentials != nil && pool.PoolCredentials.Password != "" {
		adminPassword = pool.PoolCredentials.Password
	}
	deleteSVMRequest := &vlm.DeleteSVMRequest{
		Name:      svm.Name,
		VLMConfig: *vlmConfig,
		OntapCredentials: vlm.OntapCredentials{
			AdminPassword: adminPassword,
			Certificate:   vlm.OntapCertificate{},
		},
	}
	vsaClientWorkflowManager := workflows.GetNewVSAClientWorkflowManager()
	if _, vlmErr := vsaClientWorkflowManager.DeleteVSASVM(ctx, deleteSVMRequest); vlmErr != nil {
		err = vlmErr
		logger.Errorf("Failed to delete SVM via VLM child workflow: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}
	logger.Infof("SVM removed from cluster via VLM: %s", svm.Name)

	// Step 4: soft-delete the DB row. On failure the deferred rollback above moves the SVM to ERROR.
	err = workflow.ExecuteActivity(dbHbCtx, svmActivity.SoftDeleteSvm, svm).Get(dbHbCtx, nil)
	if err != nil {
		logger.Errorf("Failed to soft-delete SVM: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	logger.Infof("SVM deleted successfully: svmOCID=%s", svm.SvmExternalIdentifier)
	return nil, nil
}
