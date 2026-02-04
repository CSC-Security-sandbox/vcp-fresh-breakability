package workflows

import (
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	hydrationEnabled = env.GetBool("GCP_HYDRATE_ENABLED", true)
)

const (
	CancelQuotaRuleSignalName = "cancel-quota-rule-creation"
)

type quotaRuleCreateWorkflow struct {
	BaseWorkflow
	SE database.Storage
}

var _ WorkflowInterface = &quotaRuleCreateWorkflow{}

// CreateQuotaRuleWorkflow processes quota rule creation requests from a customer.
func CreateQuotaRuleWorkflow(ctx workflow.Context, params *common.CreateQuotaRulesParam, quotaRule *datamodel.QuotaRule) (gcpgenserver.V1betaCreateQuotaRuleRes, error) {
	logger := util.GetLogger(ctx)
	quotaRuleWf := new(quotaRuleCreateWorkflow)
	err := quotaRuleWf.Setup(ctx, params)
	if err != nil {
		logger.Infof("Quota rule workflow setup executed with error: %v", err)
		return nil, err
	}
	// Ensure job is available before updating status
	if err := quotaRuleWf.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
		logger.Infof("Quota rule workflow job state check executed with error: %v", err)
		return nil, ConvertToVSAError(err)
	}
	quotaRuleWf.Status = WorkflowStatusRunning
	err = quotaRuleWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		logger.Infof("Update job status for quota rule executed with error: %v", err)
		return nil, err
	}
	_, customErr := quotaRuleWf.Run(ctx, quotaRule, params.LocationId)
	if customErr != nil {
		logger.Infof("Quota rule workflow run executed with error: %v", customErr)
		quotaRuleWf.Status = WorkflowStatusFailed
		jobUpdateErr := quotaRuleWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if jobUpdateErr != nil {
			logger.Errorf("Failed to update job status to Error for CreateQuotaRuleWorkflow: %v", jobUpdateErr)
			return nil, jobUpdateErr
		}
		return nil, customErr
	}
	quotaRuleWf.Status = WorkflowStatusCompleted
	err = quotaRuleWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		logger.Errorf("Failed to update job status to Done for CreateQuotaRuleWorkflow: %v", err)
	}
	logger.Debug("Create Quota Rule workflow completed successfully")
	return nil, nil
}

// Setup initializes the workflow with the necessary parameters and sets up a query handler for status updates.
func (wf *quotaRuleCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createQuotaRuleParams := input.(*common.CreateQuotaRulesParam)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createQuotaRuleParams.ProjectId
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

// Run executes the quota rule creation workflow, including creating the quota rule on ONTAP and updating its details.
func (wf *quotaRuleCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (result interface{}, returnErr *vsaerrors.CustomError) {
	quotaRule := args[0].(*datamodel.QuotaRule)
	locationId := args[1].(string)
	logger := util.GetLogger(ctx)
	quotaRuleActivity := &activities.QuotaRuleCreateActivity{}
	commonActivity := &activities.QuotaRuleCommonActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 20 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Set up cancellation handler using common framework
	cancellationHandler := common.NewWorkflowCancellationHandler(ctx, CancelQuotaRuleSignalName, quotaRule.UUID, "quota-rule")

	// Defer function to mark the database entry in error state if any error occurs or if cancelled
	defer func() {
		if returnErr != nil || cancellationHandler.IsCancelled() {
			// On error or cancellation, mark quota rule in error state
			if cancellationHandler.IsCancelled() {
				logger.Infof("Quota rule creation cancelled, marking quota rule as aborted: %s", quotaRule.UUID)
				returnErr = ConvertToVSAError(vsaerrors.New("quota rule creation cancelled by delete request"))
			}
			quotaRule.State = models.LifeCycleStateError
			quotaRule.StateDetails = models.LifeCycleStateCreationErrorDetails
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			err2 := workflow.ExecuteActivity(disconnectedCtx, commonActivity.UpdateQuotaRuleState, *quotaRule, false).Get(disconnectedCtx, nil)
			if err2 != nil {
				logger.Errorf("Failed to update quota rule state in DB to error: %v", err2)
			}
		}
	}()
	dbQuotaRule := quotaRule

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		returnErr = cancelErr
		return
	}

	// Fetch volume details as the first activity and perform DP check
	var volumeDetails *datamodel.Volume
	err = workflow.ExecuteActivity(ctx, commonActivity.GetVolumeByID, dbQuotaRule.VolumeID, dbQuotaRule.AccountID).Get(ctx, &volumeDetails)
	if err != nil {
		logger.Errorf("Failed to fetch volume details for DP validation: %v", err)
		returnErr = ConvertToVSAError(err)
		return
	}

	isDataProtection := false
	if volumeDetails != nil && volumeDetails.VolumeAttributes != nil {
		isDataProtection = volumeDetails.VolumeAttributes.IsDataProtection
	}

	if isDataProtection {
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			returnErr = cancelErr
			return
		}
		dbQuotaRule.State = models.LifeCycleStateREADY
		dbQuotaRule.StateDetails = models.LifeCycleStateReadyDetails
		err = workflow.ExecuteActivity(ctx, quotaRuleActivity.CreateQuotaRuleForDataProtectionVolume, dbQuotaRule).Get(ctx, nil)
		if err != nil {
			logger.Errorf("Failed to create quota rule for DP volume: %v", err)
			returnErr = ConvertToVSAError(err)
			return
		}
		return nil, nil
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		returnErr = cancelErr
		return
	}

	// For non-DP volumes, proceed with ONTAP operations
	// Get nodes for the pool and create node structure for provider creation
	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, volumeDetails.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		logger.Errorf("Failed to get nodes for pool: %v", err)
		returnErr = ConvertToVSAError(err)
		return
	}

	// Create node structure for provider - this will be passed to activities
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   volumeDetails.Pool.DeploymentName,
		OntapCredentials: volumeDetails.Pool.PoolCredentials,
	})

	isRQuotaEnabled := quotaRule.RQuota
	// Update RQuota on SVM if required (determined by orchestrator)
	// This must be done before creating the quota rule
	if isRQuotaEnabled {
		// Extract SVM ExternalUUID from volumeDetails
		if volumeDetails.Svm == nil || volumeDetails.Svm.SvmDetails == nil || volumeDetails.Svm.SvmDetails.ExternalUUID == "" {
			logger.Errorf("Volume has no SVM details or ExternalUUID")
			returnErr = ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrInternalServerError, customerrors.NewUserInputValidationErr("Volume has no SVM details or ExternalUUID")))
			return
		}
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			returnErr = cancelErr
			return
		}
		svmExternalUUID := volumeDetails.Svm.SvmDetails.ExternalUUID
		err = workflow.ExecuteActivity(ctx, commonActivity.UpdateRQuotaOnSvm, svmExternalUUID, node, true).Get(ctx, nil)
		if err != nil {
			logger.Errorf("Failed to enable RQuota on SVM: %v", err)
			returnErr = ConvertToVSAError(err)
			return
		}
	}

	// If a default quota already exists, update it instead of creating a new one
	// This handles the case where default quota was auto-created as a side effect of individual quota
	var defaultQuotaUpdateErr error
	if quotaRule.QuotaTarget == "" {
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			returnErr = cancelErr
			return
		}
		defaultQuotaUpdateErr = workflow.ExecuteActivity(ctx, quotaRuleActivity.HandleDefaultQuotaRuleUpdate,
			volumeDetails, node, dbQuotaRule.QuotaType, dbQuotaRule.DiskLimitInKib).Get(ctx, nil)

		// Check the error type
		if defaultQuotaUpdateErr != nil {
			isNotFound := customerrors.IsNotFoundErr(defaultQuotaUpdateErr) ||
				strings.Contains(strings.ToLower(defaultQuotaUpdateErr.Error()), "not found")

			if !isNotFound {
				// NotFoundErr is acceptable - it means no default quota exists, so we'll create one
				returnErr = ConvertToVSAError(defaultQuotaUpdateErr)
				return
			}
		}
	}

	// Skip creation if default quota was successfully updated
	var quotaRuleCreateResponse *vsa.QuotaRuleProviderResponse
	isNotFoundForCreation := customerrors.IsNotFoundErr(defaultQuotaUpdateErr) ||
		(defaultQuotaUpdateErr != nil && strings.Contains(strings.ToLower(defaultQuotaUpdateErr.Error()), "not found"))
	if dbQuotaRule.QuotaTarget != "" || isNotFoundForCreation {
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			returnErr = cancelErr
			return
		}
		err = workflow.ExecuteActivity(ctx, quotaRuleActivity.CreateQuotaRuleOnONTAP,
			node, volumeDetails, dbQuotaRule).Get(ctx, &quotaRuleCreateResponse)
		if err != nil {
			logger.Errorf("Failed to create quota rule in ONTAP: %v", err)
			returnErr = ConvertToVSAError(err)
			return
		}

		if quotaRuleCreateResponse != nil {
			logger.Infof("CreateQuotaRuleOnONTAP response: State=%s, Message=%s",
				quotaRuleCreateResponse.State, quotaRuleCreateResponse.Message)
		}

		// According to user requirements, we always perform quota enable (no isQuotaEnableRequired check)
		// This matches the sample code where quota status check is inside the if block
		var quotaStatus *vsa.QuotaStatus
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			returnErr = cancelErr
			return
		}
		err = workflow.ExecuteActivity(ctx, quotaRuleActivity.GetQuotaStatus, node, volumeDetails).Get(ctx, &quotaStatus)
		if err != nil {
			logger.Errorf("Failed to get quota status: %v", err)
			returnErr = ConvertToVSAError(err)
			return
		}

		logger.Infof("Current quota status after CreateQuotaRule: State=%s, Enabled=%v", quotaStatus.State, quotaStatus.Enabled)

		// Check if quota is OFF (first quota being created)
		if quotaStatus.State == vsa.QuotaStateOff {
			// Enable quota for the first time
			var quotaEnableResp *vsa.JobStatus
			if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
				returnErr = cancelErr
				return
			}
			err = workflow.ExecuteActivity(ctx, commonActivity.HandleQuotaEnableDisable, node, volumeDetails, true).Get(ctx, &quotaEnableResp)
			if err != nil {
				logger.Errorf("Failed to enable quota: %v", err)
				returnErr = ConvertToVSAError(err)
				return
			}

			// Process quota enable response following sample code pattern
			if quotaEnableResp != nil && quotaEnableResp.State == vsa.JobRespFailure {
				// Get SVM name for checking expected failures
				svmName := ""
				if volumeDetails.Svm != nil && volumeDetails.Svm.Name != "" {
					svmName = volumeDetails.Svm.Name
				}

				// Check if the failure is expected (quota status issues or SVM-related)
				if !strings.Contains(quotaEnableResp.Message, vsa.QuotaStatusFailed) &&
					!strings.Contains(quotaEnableResp.Message, svmName) {
					// Unexpected error - return error with helpful message
					errMsg := quotaEnableResp.Message + " - please delete the quota rule and try again"
					logger.Errorf("Quota enable failed: %s", errMsg)
					returnErr = vsaerrors.NewVCPError(vsaerrors.ErrInternalServerError, customerrors.NewUserInputValidationErr(errMsg))
					return
				}
				// Expected error - log as warning but continue (quota may already be enabled)
				logger.Warnf("Quota enable returned expected failure: %s", quotaEnableResp.Message)
			}
		} else {
			// Quota is already ON - check if quota rule creation had resize/activation failures
			// Following sample code pattern: check response and call reinitialization if needed
			logger.Infof("Quota is ON - checking for resize/activation failures in CreateQuotaRule response")
			if quotaRuleCreateResponse != nil && quotaRuleCreateResponse.State == vsa.JobRespFailure {
				logger.Warnf("CreateQuotaRule returned failure state. Message: %s", quotaRuleCreateResponse.Message)
				// Check if failure is due to resize or activation operation
				if strings.Contains(quotaRuleCreateResponse.Message, vsa.ResizeOperationFailed) ||
					strings.Contains(quotaRuleCreateResponse.Message, vsa.ActivationOperationFailed) {
					logger.Infof("Detected resize/activation failure - triggering quota reinitialization")
					// Call QuotaReinitialization activity to handle reinitialization (matches spec Section 7)
					if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
						returnErr = cancelErr
						return
					}
					err = workflow.ExecuteActivity(ctx, commonActivity.QuotaReinitialization,
						node, volumeDetails).Get(ctx, nil)
					if err != nil {
						logger.Errorf("Quota reinitialization failed: %v", err)
						returnErr = ConvertToVSAError(err)
						return
					}
				} else {
					// Other failure - return error
					logger.Errorf("Quota rule creation failed: %s", quotaRuleCreateResponse.Message)
					returnErr = vsaerrors.NewVCPError(vsaerrors.ErrInternalServerError, customerrors.NewUserInputValidationErr(quotaRuleCreateResponse.Message))
					return
				}
			}
		}
	}
	// For default quota update case (else branch), quota status check/enable is skipped (matching sample code behavior)

	// Fetch volume replication details and verify replication state for destination sync
	// These activities are used to sync quota rules to destination volumes in replication scenarios
	var replications []*datamodel.VolumeReplication
	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		returnErr = cancelErr
		return
	}
	err = workflow.ExecuteActivity(ctx, commonActivity.GetVolumeReplication, volumeDetails.ID).Get(ctx, &replications)
	if err != nil {
		logger.Errorf("Failed to fetch volume replication details for destination sync: %v", err)
		returnErr = ConvertToVSAError(err)
		return
	}

	// Check if there are any replications
	if len(replications) == 0 {
		logger.Debugf("No replications found for volume, skipping destination sync")
	} else {
		// At present we can have only 1 replication for a region, so use the first replication
		replication := replications[0]
		if replication == nil || replication.ReplicationAttributes == nil {
			logger.Debugf("Replication is nil or has no attributes, skipping destination sync")
		} else {
			// Verify if this replication is eligible for destination sync
			// This activity validates that replication is eligible for quota sync (MIRRORED or UNINITIALIZED state)
			// Pass LocationId directly - activity will parse region internally
			var isEligible bool
			if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
				returnErr = cancelErr
				return
			}
			err = workflow.ExecuteActivity(ctx, commonActivity.VerifyReplicationState,
				replication, locationId).Get(ctx, &isEligible)
			if err != nil {
				logger.Errorf("Replication state validation failed for destination sync: %v", err)
				returnErr = ConvertToVSAError(err)
				return
			}

			// Sync quota rule only if replication is eligible
			if isEligible {
				// Parse destination project number from RemoteUri for API calls
				destProjectNumber, err := utils.ParseProjectNumberFromURI(replication.RemoteUri)
				if err != nil {
					logger.Errorf("Failed to parse destination project number from RemoteUri: %v, remoteUri: %s", err, replication.RemoteUri)
					returnErr = ConvertToVSAError(vsaerrors.New(fmt.Sprintf("failed to parse destination project number: %v", err)))
					return
				}

				// Create JWT token once for reuse across all destination API calls
				var jwtToken *string
				if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
					returnErr = cancelErr
					return
				}
				err = workflow.ExecuteActivity(ctx, commonActivity.GetSignedDstTokenForQuotaRule, destProjectNumber).Get(ctx, &jwtToken)
				if err != nil {
					logger.Errorf("Failed to get JWT token for destination project %s: %v", destProjectNumber, err)
					returnErr = ConvertToVSAError(err)
					return
				}

				// Create quota rule on destination via internal API
				// Pass destination volume UUID directly instead of fetching full volume details
				var createOperationResult *activities.QuotaRuleOperationResult
				if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
					returnErr = cancelErr
					return
				}
				err = workflow.ExecuteActivity(ctx, quotaRuleActivity.CreateQuotaRuleOnDestination,
					replication.ReplicationAttributes.DestinationVolumeUUID, dbQuotaRule, replication.ReplicationAttributes.DestinationLocation, destProjectNumber, jwtToken).Get(ctx, &createOperationResult)
				if err != nil {
					logger.Errorf("Failed to create quota rule on destination: destinationVolumeUUID=%s, error=%v",
						replication.ReplicationAttributes.DestinationVolumeUUID, err)
					returnErr = ConvertToVSAError(err)
					return
				}

				// Poll for operation completion if async operation was started
				if createOperationResult != nil && createOperationResult.OperationName != "" && !createOperationResult.IsDone {
					logger.Infof("Polling for quota rule creation operation completion on destination: operationName=%s", createOperationResult.OperationName)
					if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
						returnErr = cancelErr
						return
					}
					err = workflow.ExecuteActivity(ctx, commonActivity.DescribeQuotaRuleRemoteJob,
						createOperationResult.OperationName, replication.ReplicationAttributes.DestinationLocation, destProjectNumber, jwtToken).Get(ctx, nil)
					if err != nil {
						logger.Errorf("Failed to wait for quota rule creation on destination: operationName=%s, error=%v",
							createOperationResult.OperationName, err)
						returnErr = ConvertToVSAError(err)
						return
					}
				}

				if hydrationEnabled && createOperationResult != nil && createOperationResult.QuotaRule != nil {
					// Hydrate the quota rule creation to CCFE using the destination quota rule
					if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
						returnErr = cancelErr
						return
					}
					hydrateErr := workflow.ExecuteActivity(ctx, commonActivity.HydrateQuotaRuleCreate,
						createOperationResult.QuotaRule, replication.ReplicationAttributes.DestinationVolumeUUID,
						replication.ReplicationAttributes.DestinationLocation, destProjectNumber).Get(ctx, nil)
					if hydrateErr != nil {
						logger.Errorf("Failed to hydrate quota rule create to CCFE: quotaRuleName=%s, error=%v", createOperationResult.QuotaRule.Name, hydrateErr)
						returnErr = ConvertToVSAError(hydrateErr)
						return
					}
					logger.Infof("Successfully hydrated quota rule create to CCFE: quotaRuleName=%s", createOperationResult.QuotaRule.Name)
				}

				logger.Infof("Successfully synced quota rule to destination: location=%s, volumeUUID=%s",
					replication.ReplicationAttributes.DestinationLocation, replication.ReplicationAttributes.DestinationVolumeUUID)
			} else {
				logger.Debugf("Replication is not eligible for sync: destinationLocation=%s, destinationVolumeUUID=%s",
					replication.ReplicationAttributes.DestinationLocation, replication.ReplicationAttributes.DestinationVolumeUUID)
			}
		}
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		returnErr = cancelErr
		return
	}
	// Update quota rule state (will transition from CREATING to READY if applicable)
	err = workflow.ExecuteActivity(ctx, commonActivity.UpdateQuotaRuleState, *dbQuotaRule, false).Get(ctx, nil)
	if err != nil {
		logger.Errorf("Failed to update quota rule state: %v", err)
		returnErr = ConvertToVSAError(err)
		return
	}

	logger.Infof("Quota rule creation workflow completed successfully")
	return nil, nil
}
