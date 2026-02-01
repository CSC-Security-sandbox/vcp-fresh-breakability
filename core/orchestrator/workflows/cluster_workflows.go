package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type clusterPeerWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

var _ WorkflowInterface = &clusterPeerWorkflow{}

func AcceptClusterPeerWorkflow(ctx workflow.Context, params *common.ClusterPeerParams, pool *datamodel.Pool) error {
	clusterPeerWF := new(clusterPeerWorkflow)
	err := clusterPeerWF.Setup(ctx, params)
	if err != nil {
		return err
	}
	clusterPeerWF.Status = WorkflowStatusRunning
	err = clusterPeerWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return err
	}
	_, customErr := clusterPeerWF.Run(ctx, params, pool)
	if customErr != nil {
		clusterPeerWF.Status = WorkflowStatusFailed
		err = clusterPeerWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return err
	}
	clusterPeerWF.Status = WorkflowStatusCompleted
	err = clusterPeerWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return err
}

func (wf *clusterPeerWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	acceptClusterPeerParams := input.(*common.ClusterPeerParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = acceptClusterPeerParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *clusterPeerWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.ClusterPeerParams)
	pool := args[1].(*datamodel.Pool)
	clusterPeerActivity := &activities.ClusterPeerActivity{}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(StartToCloseTimeoutForReplicationActivities) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        1,
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var dbNodes []*datamodel.Node
	err := workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   pool.DeploymentName,
		OntapCredentials: pool.PoolCredentials,
	})

	clusterPeer := &common.ClusterPeerParams{}
	err = workflow.ExecuteActivity(ctx, clusterPeerActivity.AcceptClusterPeer, params, node).Get(ctx, &clusterPeer)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	return clusterPeer, nil
}

// ClusterUpgradeWorkflowParams holds the parameters for cluster upgrade workflow
type ClusterUpgradeWorkflowParams struct {
	JobID             string            `json:"jobId"`
	ClusterID         string            `json:"clusterId"`
	Pool              *datamodel.Pool   `json:"pool"`
	TargetVersion     string            `json:"targetVersion"`
	CurrentVersion    string            `json:"currentVersion"`
	VSAImagePath      string            `json:"vsaImagePath"`
	VSAImageName      string            `json:"vsaImageName"`
	MediatorImageName string            `json:"mediatorImageName"`
	ForceUpgrade      bool              `json:"forceUpgrade"`
	Metadata          map[string]string `json:"metadata,omitempty"`
}

// UpgradeContext holds the context and state for the upgrade process
type UpgradeContext struct {
	Params               *ClusterUpgradeWorkflowParams
	Pool                 *datamodel.Pool
	Credentials          *vlm.OntapCredentials
	CurrentVlmConfig     *vlm.VLMConfig
	VlmClient            vlm.VlmWorkflowClient
	ClusterWasDisabled   bool
	NeedsMediatorUpgrade bool
	NeedsVSAUpgrade      bool
}

// UpgradeResult holds the results of the upgrade process
type UpgradeResult struct {
	MediatorUpgradeResponse *vlm.UpdateMediatorResponse
	VSAUpgradeResponse      *vlm.UpgradeVSAClusterDeploymentResponse
	FinalVlmConfig          *vlm.VLMConfig
	Success                 bool
	Error                   error
}
type clusterUpgradeWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// UpdateClusterUpgradeJobStatus updates the status of a cluster upgrade job
func (wf *clusterUpgradeWorkflow) UpdateClusterUpgradeJobStatus(ctx workflow.Context, status, errorMessage string) error {
	if wf.ID == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError,
			errors.New("job uuid cannot be empty"))
	}
	clusterUpgradeActivity := &activities.ClusterUpgradeActivity{}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	})
	var result error
	err := workflow.ExecuteActivity(ctx, clusterUpgradeActivity.UpdateClusterUpgradeJobStatusActivity, wf.ID, status, errorMessage).Get(ctx, &result)
	if err != nil {
		wf.Logger.Error("Failed to update cluster upgrade job status", "jobID", wf.ID, "status", status, "error", err)
		return err
	}
	return result
}

var _ WorkflowInterface = &clusterUpgradeWorkflow{}

// ClusterUpgradeWorkflow is the main workflow for upgrading a VSA cluster
func ClusterUpgradeWorkflow(ctx workflow.Context, params *ClusterUpgradeWorkflowParams) error {
	clusterUpgradeWF := new(clusterUpgradeWorkflow)
	err := clusterUpgradeWF.Setup(ctx, params)
	if err != nil {
		return err
	}
	clusterUpgradeWF.Status = WorkflowStatusRunning

	// Update cluster upgrade job status to IN_PROGRESS
	err = clusterUpgradeWF.UpdateClusterUpgradeJobStatus(ctx, string(models.UpgradeStatusInProgress), "")
	if err != nil {
		return err
	}

	_, customErr := clusterUpgradeWF.Run(ctx, params)
	if customErr != nil {
		clusterUpgradeWF.Status = WorkflowStatusFailed
		// Update cluster upgrade job status to FAILED
		err = clusterUpgradeWF.UpdateClusterUpgradeJobStatus(ctx, string(models.UpgradeStatusFailed), customErr.Error())
		return err
	}

	clusterUpgradeWF.Status = WorkflowStatusCompleted
	// Update cluster upgrade job status to COMPLETED
	err = clusterUpgradeWF.UpdateClusterUpgradeJobStatus(ctx, string(models.UpgradeStatusCompleted), "")
	return err
}

func (wf *clusterUpgradeWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	params := input.(*ClusterUpgradeWorkflowParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = "system" // Cluster upgrade is a system operation
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": wf.ID,
		"customerID": wf.CustomerID,
		"jobID":      params.JobID,
		"clusterID":  params.ClusterID,
	})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

// prepareClusterUpgradeRequestActivity prepares the VLM upgrade request using activities for external API calls
func prepareClusterUpgradeRequestActivity(ctx workflow.Context, upgradeRequest *vlm.UpdateVSAClusterDeploymentRequest, params *ClusterUpgradeWorkflowParams, pool *datamodel.Pool, currentVlmConfig vlm.VLMConfig, credentials vlm.OntapCredentials) error {
	upgradeRequest.VLMConfig = currentVlmConfig

	// Use activity to generate signed URL to avoid workflow deadlock
	commonActivity := &activities.CommonActivities{}
	var signedURL string
	err := workflow.ExecuteActivity(ctx, commonActivity.GenerateVSASignedURLActivity, params.VSAImagePath).Get(ctx, &signedURL)
	if err != nil {
		return fmt.Errorf("failed to generate signed URL for VSA image %s: %w", params.VSAImagePath, err)
	}

	upgradeRequest.OntapUpgrade = vlm.OntapUpgradeConfig{
		OntapUpgradeTargetImageVersion: params.TargetVersion,
		OntapUpgradeImagePath:          signedURL,
		SkipOntapImageVersionMatch:     env.SkipOntapImageVersionMatch,
		RunPreUpgrade:                  true,
	}
	upgradeRequest.OntapCredentials = credentials
	// Set AutoTierThreshold to -1 to signal VLM to skip auto-tiering threshold update.
	// This is a workaround until VLM properly handles the case where object store doesn't exist.
	// Valid threshold values are 0-100, so -1 is used as a sentinel value meaning "do not update".
	upgradeRequest.AutoTierThreshold = -1
	return nil
}

func (wf *clusterUpgradeWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*ClusterUpgradeWorkflowParams)

	wf.Logger.Info("Starting cluster upgrade workflow", "jobID", params.JobID, "clusterID", params.ClusterID, "targetVersion", params.TargetVersion)

	// Pre-upgrade phase: Prepare cluster and determine upgrade needs
	ctx, upgradeContext, err := wf.preUpgradePhase(ctx, params)
	if err != nil {
		wf.Logger.Error("Pre-upgrade phase failed", "jobID", params.JobID, "clusterID", params.ClusterID, "error", err)
		return nil, err
	}

	// Upgrade phase: Perform mediator and VSA upgrades
	upgradeResult, err := wf.upgradePhase(ctx, upgradeContext)
	if err != nil {
		wf.Logger.Error("Upgrade phase failed", "jobID", params.JobID, "clusterID", params.ClusterID, "error", err)
		// Attempt post-upgrade cleanup even on failure
		cleanupErr := wf.postUpgradePhase(ctx, upgradeContext, upgradeResult, err)
		if cleanupErr != nil {
			wf.Logger.Error("Post-upgrade cleanup failed", "jobID", params.JobID, "clusterID", params.ClusterID, "error", cleanupErr)
		}
		return nil, err
	}

	// Post-upgrade phase: License update and power off
	err = wf.postUpgradePhase(ctx, upgradeContext, upgradeResult, nil)
	if err != nil {
		wf.Logger.Error("Post-upgrade phase failed", "jobID", params.JobID, "clusterID", params.ClusterID, "error", err)
		// Don't fail the workflow for post-upgrade issues, just log the error
	}

	wf.Logger.Info("Cluster upgrade workflow completed successfully", "jobID", params.JobID, "clusterID", params.ClusterID)
	return params, nil
}

// preUpgradePhase prepares the cluster for upgrade and determines what needs to be upgraded
func (wf *clusterUpgradeWorkflow) preUpgradePhase(ctx workflow.Context, params *ClusterUpgradeWorkflowParams) (workflow.Context, *UpgradeContext, *vsaerrors.CustomError) {
	wf.Logger.Info("Starting pre-upgrade phase", "jobID", params.JobID, "clusterID", params.ClusterID)

	poolActivities := &activities.PoolActivity{}

	// Set up activity options with timeout
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return ctx, nil, ConvertToVSAError(err)
	}
	activityStartToCloseTimeout, err := time.ParseDuration(StartToCloseTimeoutUpgrade)
	if err != nil {
		return ctx, nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: activityStartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Validate upgrade image digest before proceeding
	if activities.ValidateImageDigestFlag {
		var verified bool
		if err := workflow.ExecuteActivity(ctx, poolActivities.ValidateImageDigest).Get(ctx, &verified); err != nil {
			return ctx, nil, ConvertToVSAError(err)
		}
		if !verified {
			return ctx, nil, ConvertToVSAError(vsaerrors.New("image digest verification failed"))
		}
	}

	// Get credentials and VLM config
	credentials := &vlm.OntapCredentials{}
	err = workflow.ExecuteActivity(ctx, poolActivities.GetOnTapCredentials, params.Pool).Get(ctx, &credentials)
	if err != nil {
		return ctx, nil, ConvertToVSAError(err)
	}

	currentVlmConfig := &vlm.VLMConfig{}
	if err := json.Unmarshal([]byte(params.Pool.VLMConfig), currentVlmConfig); err != nil {
		return ctx, nil, ConvertToVSAError(err)
	}

	// Create VLM client
	vlmClient := vlm.NewVSAClientWorkflowManager()

	// Check if cluster is in DISABLED state (powered off) and handle power operations
	clusterWasDisabled := false
	if params.Pool.State == models.LifeCycleStateDisabled {
		wf.Logger.Info("Cluster is in DISABLED state, will power on before upgrade", "jobID", params.JobID, "clusterID", params.ClusterID)
		clusterWasDisabled = true

		// Power on the cluster
		wf.Logger.Info("Powering on cluster for upgrade", "jobID", params.JobID, "clusterID", params.ClusterID)

		powerOnRequest := &vlm.ClusterPowerOpReq{
			VLMConfig:        *currentVlmConfig,
			OntapCredentials: *credentials,
			Operation:        vlm.ClusterPowerOn,
		}

		err = vlmClient.ClusterPowerOp(ctx, powerOnRequest)
		if err != nil {
			wf.Logger.Error("Failed to power on cluster", "jobID", params.JobID, "clusterID", params.ClusterID, "error", err)
			return ctx, nil, ConvertToVSAError(err)
		}

		wf.Logger.Info("Cluster powered on successfully", "jobID", params.JobID, "clusterID", params.ClusterID)
	}

	// Determine what needs to be upgraded based on current pool build info
	needsMediatorUpgrade := true
	needsVSAUpgrade := true

	if params.Pool.BuildInfo != nil {
		// Check if mediator needs upgrade by comparing image names (mediator uses name, not path)
		if !params.ForceUpgrade && params.Pool.BuildInfo.MediatorBuildImage == params.MediatorImageName {
			needsMediatorUpgrade = false
			wf.Logger.Info("Mediator is already up to date", "jobID", params.JobID, "clusterID", params.ClusterID, "currentBuildImage", params.Pool.BuildInfo.MediatorBuildImage)
		}

		// Check if VSA needs upgrade by comparing build images (VSA uses path)
		if !params.ForceUpgrade && params.Pool.BuildInfo.VSABuildImage == params.VSAImageName {
			needsVSAUpgrade = false
			wf.Logger.Info("VSA is already up to date", "jobID", params.JobID, "clusterID", params.ClusterID, "currentBuildImage", params.Pool.BuildInfo.VSABuildImage)
		}
	}

	upgradeContext := &UpgradeContext{
		Params:               params,
		Pool:                 params.Pool,
		Credentials:          credentials,
		CurrentVlmConfig:     currentVlmConfig,
		VlmClient:            vlmClient,
		ClusterWasDisabled:   clusterWasDisabled,
		NeedsMediatorUpgrade: needsMediatorUpgrade,
		NeedsVSAUpgrade:      needsVSAUpgrade,
	}

	wf.Logger.Info("Pre-upgrade phase completed successfully", "jobID", params.JobID, "clusterID", params.ClusterID, "needsMediatorUpgrade", needsMediatorUpgrade, "needsVSAUpgrade", needsVSAUpgrade, "clusterWasDisabled", clusterWasDisabled)
	return ctx, upgradeContext, nil
}

// upgradePhase performs the actual mediator and VSA upgrades
func (wf *clusterUpgradeWorkflow) upgradePhase(ctx workflow.Context, upgradeContext *UpgradeContext) (*UpgradeResult, *vsaerrors.CustomError) {
	wf.Logger.Info("Starting upgrade phase", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID)

	upgradeResult := &UpgradeResult{
		Success: false,
	}

	poolActivities := &activities.PoolActivity{}

	// Step 1: Upgrade VSA Mediator if needed
	if upgradeContext.NeedsMediatorUpgrade {
		mediatorUpgradeRequest := &vlm.UpdateMediatorRequest{
			VLMConfig: *upgradeContext.CurrentVlmConfig,
			MediatorUpdate: vlm.MediatorUpdateConfig{
				MediatorImageName: upgradeContext.Params.MediatorImageName,
			},
			OntapCredentials: *upgradeContext.Credentials,
		}

		wf.Logger.Info("Starting VSA mediator upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "targetVersion", upgradeContext.Params.TargetVersion)
		mediatorUpgradeResponse, err := upgradeContext.VlmClient.UpgradeVSAMediatorWorkflow(ctx, mediatorUpgradeRequest)
		if err != nil {
			wf.Logger.Error("VLM mediator upgrade workflow failed", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", err)
			customErr := vsaerrors.NewVCPError(vsaerrors.ErrVLMWorkflowError, err)
			upgradeResult.Error = customErr
			return upgradeResult, customErr
		}
		wf.Logger.Info("VSA mediator upgrade completed successfully", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "ontapVersion", mediatorUpgradeResponse)

		// Update VLM config with response from mediator upgrade
		upgradeContext.CurrentVlmConfig = &mediatorUpgradeResponse.VLMConfig
		upgradeResult.MediatorUpgradeResponse = mediatorUpgradeResponse

		// Update build info after successful mediator upgrade
		now := time.Now()
		mediatorBuildInfo := &datamodel.PoolBuildInfo{
			MediatorBuildImage: upgradeContext.Params.MediatorImageName, // Update mediator build image
			OntapVersion:       upgradeContext.Params.TargetVersion,
			BuildTimestamp:     now,
		}

		// Preserve existing VSA build info if pool already has build info
		if upgradeContext.Pool.BuildInfo != nil {
			mediatorBuildInfo.VSABuildImage = upgradeContext.Pool.BuildInfo.VSABuildImage
		}

		// Update both build info and VLM config
		vlmConfigBytes, err := json.Marshal(upgradeContext.CurrentVlmConfig)
		if err != nil {
			wf.Logger.Error("Failed to marshal VLM config after mediator upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", err)
		} else {
			wf.Logger.Infof(string(vlmConfigBytes))
			updates := map[string]interface{}{
				"build_info": mediatorBuildInfo,
				"vlm_config": string(vlmConfigBytes),
			}
			updateErr := workflow.ExecuteActivity(ctx, poolActivities.UpdatePoolFields, upgradeContext.Pool.UUID, updates).Get(ctx, nil)
			if updateErr != nil {
				wf.Logger.Error("Failed to update pool build info and VLM config after mediator upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", updateErr)
				// Don't fail the workflow for this, just log the error
			} else {
				wf.Logger.Info("Pool build info and VLM config updated successfully after mediator upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "mediatorBuildImage", upgradeContext.Params.MediatorImageName)
			}
		}
	} else {
		wf.Logger.Info("Skipping VSA mediator upgrade - already up to date", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID)
	}

	// Step 2: Upgrade VSA Cluster Deployment if needed
	if upgradeContext.NeedsVSAUpgrade {
		upgradeRequest := &vlm.UpdateVSAClusterDeploymentRequest{}

		err := prepareClusterUpgradeRequestActivity(ctx, upgradeRequest, upgradeContext.Params, upgradeContext.Pool, *upgradeContext.CurrentVlmConfig, *upgradeContext.Credentials)
		if err != nil {
			wf.Logger.Error("Failed to prepare cluster upgrade request",
				"jobID", upgradeContext.Params.JobID,
				"clusterID", upgradeContext.Params.ClusterID,
				"vsaImagePath", upgradeContext.Params.VSAImagePath,
				"targetVersion", upgradeContext.Params.TargetVersion,
				"error", err)
			customErr := vsaerrors.NewVCPError(vsaerrors.ErrVLMWorkflowError, err)
			upgradeResult.Error = customErr
			return upgradeResult, customErr
		}

		wf.Logger.Info("Starting VSA cluster deployment upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "targetVersion", upgradeContext.Params.TargetVersion)
		vsaUpgradeResponse, err := upgradeContext.VlmClient.UpgradeVSAClusterDeploymentWorkflow(ctx, upgradeRequest)
		if err != nil {
			wf.Logger.Error("VLM cluster upgrade workflow failed", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", err)
			customErr := vsaerrors.NewVCPError(vsaerrors.ErrVLMWorkflowError, err)
			upgradeResult.Error = customErr
			return upgradeResult, customErr
		}
		wf.Logger.Info("VSA cluster deployment upgrade completed successfully", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "ontapVersion", vsaUpgradeResponse.OntapVersion)

		// Update VLM config with response from VSA upgrade
		upgradeContext.CurrentVlmConfig = &vsaUpgradeResponse.VLMConfig
		upgradeResult.VSAUpgradeResponse = vsaUpgradeResponse

		// Update build info after successful VSA upgrade
		now := time.Now()
		vsaBuildInfo := &datamodel.PoolBuildInfo{
			MediatorBuildImage: upgradeContext.Params.MediatorImageName,
			VSABuildImage:      upgradeContext.Params.VSAImageName, // Update VSA build image
			OntapVersion:       upgradeContext.Params.TargetVersion,
			BuildTimestamp:     now,
		}

		// Preserve existing mediator build info if pool already has build info
		if upgradeContext.Pool.BuildInfo != nil {
			vsaBuildInfo.MediatorBuildImage = upgradeContext.Pool.BuildInfo.MediatorBuildImage
		}

		// Update both build info and VLM config
		vlmConfigBytes, err := json.Marshal(upgradeContext.CurrentVlmConfig)
		if err != nil {
			wf.Logger.Error("Failed to marshal VLM config after VSA upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", err)
		} else {
			updates := map[string]interface{}{
				"build_info": vsaBuildInfo,
				"vlm_config": string(vlmConfigBytes),
			}
			updateErr := workflow.ExecuteActivity(ctx, poolActivities.UpdatePoolFields, upgradeContext.Pool.UUID, updates).Get(ctx, nil)
			if updateErr != nil {
				wf.Logger.Error("Failed to update pool build info and VLM config after VSA upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", updateErr)
				// Don't fail the workflow for this, just log the error
			} else {
				wf.Logger.Info("Pool build info and VLM config updated successfully after VSA upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "vsaBuildImage", upgradeContext.Params.VSAImageName)
			}
			err = wf.updateOntapVersionAfterUpgrade(ctx, upgradeContext, upgradeResult)
			if err != nil {
				wf.Logger.Error("Failed to update ONTAP version after upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", err)
				return nil, ConvertToVSAError(err)
			}
		}
	} else {
		wf.Logger.Info("Skipping VSA cluster deployment upgrade - already up to date", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID)
	}

	upgradeResult.FinalVlmConfig = upgradeContext.CurrentVlmConfig
	upgradeResult.Success = true

	wf.Logger.Info("Upgrade phase completed successfully", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID)
	return upgradeResult, nil
}

// postUpgradePhase handles license updates and power off operations
func (wf *clusterUpgradeWorkflow) postUpgradePhase(ctx workflow.Context, upgradeContext *UpgradeContext, upgradeResult *UpgradeResult, upgradeError error) *vsaerrors.CustomError {
	wf.Logger.Info("Starting post-upgrade phase", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "upgradeSuccess", upgradeResult.Success)

	// Step 1: Update license if upgrade was successful
	if upgradeResult.Success {
		err := wf.updateLicense(ctx, upgradeContext.Params, upgradeResult.FinalVlmConfig, upgradeContext.Credentials, upgradeContext.VlmClient)
		if err != nil {
			wf.Logger.Error("Failed to update license after upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", err)
			// Don't fail the workflow for license update failure, just log the error
		}
	}

	// Step 2: Power off cluster if it was originally disabled
	if upgradeContext.ClusterWasDisabled {
		wf.Logger.Info("Cluster was originally disabled, powering off after upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID)

		powerOffRequest := &vlm.ClusterPowerOpReq{
			VLMConfig:        *upgradeResult.FinalVlmConfig,
			OntapCredentials: *upgradeContext.Credentials,
			Operation:        vlm.ClusterPowerOff,
		}

		err := upgradeContext.VlmClient.ClusterPowerOp(ctx, powerOffRequest)
		if err != nil {
			wf.Logger.Error("Failed to power off cluster after upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "error", err)
			// Don't fail the workflow for power off failure, just log the error
		} else {
			wf.Logger.Info("Cluster powered off successfully after upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID)
		}
	}
	wf.Logger.Info("Post-upgrade phase completed", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID)
	return nil
}

// updateOntapVersionAfterUpgrade gets the ONTAP version after upgrade and saves it to pool cluster details
func (wf *clusterUpgradeWorkflow) updateOntapVersionAfterUpgrade(ctx workflow.Context, upgradeContext *UpgradeContext, upgradeResult *UpgradeResult) error {
	wf.Logger.Info("Getting ONTAP version after upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID)
	commonActivity := &activities.CommonActivities{}
	// Get nodes for the pool
	var dbNodes []*datamodel.Node
	err := workflow.ExecuteActivity(ctx, commonActivity.GetNode, upgradeContext.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return fmt.Errorf("failed to get nodes for pool: %w", err)
	}

	// Create node for provider
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   upgradeContext.Pool.DeploymentName,
		OntapCredentials: upgradeContext.Pool.PoolCredentials,
	})

	// Get ONTAP version
	poolActivities := &activities.PoolActivity{}
	var ontapVersion string
	err = workflow.ExecuteActivity(ctx, poolActivities.GetOntapVersion, node).Get(ctx, &ontapVersion)
	if err != nil {
		return fmt.Errorf("failed to get ONTAP version: %w", err)
	}

	wf.Logger.Info("Retrieved ONTAP version after upgrade", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "ontapVersion", ontapVersion)

	clusterDetail := upgradeContext.Pool.ClusterDetails
	// Update only the ONTAP version in cluster details
	clusterDetail.OntapVersion = ontapVersion

	updates := map[string]interface{}{
		"cluster_details": clusterDetail,
	}

	err = workflow.ExecuteActivity(ctx, poolActivities.UpdatePoolFields, upgradeContext.Pool.UUID, updates).Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to update cluster details with ONTAP version: %w", err)
	}

	wf.Logger.Info("Successfully updated cluster details with ONTAP version", "jobID", upgradeContext.Params.JobID, "clusterID", upgradeContext.Params.ClusterID, "ontapVersion", ontapVersion)
	return nil
}

// updateLicense checks if license update is needed and calls UpdateLicenseWorkflow if required
func (wf *clusterUpgradeWorkflow) updateLicense(ctx workflow.Context, params *ClusterUpgradeWorkflowParams, currentVlmConfig *vlm.VLMConfig, credentials *vlm.OntapCredentials, vlmClient vlm.VlmWorkflowClient) error {
	// TODO: This is a placeholder implementation. The actual environment variable for SecretUri
	// will be added in a different PR by someone else. For now, we assume license update is needed.

	// Get all node management IPs from HA pairs
	var nodeManagementIPs []string

	// Iterate through all HA pairs in the cloud config
	for haPairIndex, haPair := range currentVlmConfig.Cloud.HAPairs {
		// Get node management IP from VM1
		if nodeMgmtLIF, exists := haPair.VM1.SystemLIFs[vlm.LIFTypeNodeMgmt]; exists {
			nodeManagementIPs = append(nodeManagementIPs, nodeMgmtLIF.IP)
			wf.Logger.Info("Found VM1 node management IP", "jobID", params.JobID, "clusterID", params.ClusterID, "haPairIndex", haPairIndex, "vmType", "VM1", "nodeMgmtIP", nodeMgmtLIF.IP)
		}

		// Get node management IP from VM2
		if nodeMgmtLIF, exists := haPair.VM2.SystemLIFs[vlm.LIFTypeNodeMgmt]; exists {
			nodeManagementIPs = append(nodeManagementIPs, nodeMgmtLIF.IP)
			wf.Logger.Info("Found VM2 node management IP", "jobID", params.JobID, "clusterID", params.ClusterID, "haPairIndex", haPairIndex, "vmType", "VM2", "nodeMgmtIP", nodeMgmtLIF.IP)
		}
	}

	if len(nodeManagementIPs) == 0 {
		wf.Logger.Warn("No node management IPs found in VLM config, skipping license update", "jobID", params.JobID, "clusterID", params.ClusterID)
		return nil
	}
	// Update license for each node management IP
	for _, nodeMgmtIP := range nodeManagementIPs {
		// Create UpdateLicenseRequest for this node
		updateLicenseRequest := &vlm.UpdateLicenseRequest{
			OntapLicense: vlm.OntapLicense{
				SecretUri: []string{utils.GetNLFSecretPath()},
			},
			OntapCredentials: *credentials,
			VSAManagementIP:  nodeMgmtIP,
		}

		wf.Logger.Info("Updating ONTAP license for node", "jobID", params.JobID, "clusterID", params.ClusterID, "nodeMgmtIP", nodeMgmtIP)

		err := vlmClient.UpdateLicenseWorkflow(ctx, updateLicenseRequest)
		if err != nil {
			wf.Logger.Error("Failed to update license for node", "jobID", params.JobID, "clusterID", params.ClusterID, "nodeMgmtIP", nodeMgmtIP, "error", err)
			// Continue with other nodes even if one fails
			continue
		}
		wf.Logger.Info("ONTAP license updated successfully for node", "jobID", params.JobID, "clusterID", params.ClusterID, "nodeMgmtIP", nodeMgmtIP)
	}

	wf.Logger.Info("License update process completed for all nodes", "jobID", params.JobID, "clusterID", params.ClusterID, "totalNodes", len(nodeManagementIPs))
	return nil
}
