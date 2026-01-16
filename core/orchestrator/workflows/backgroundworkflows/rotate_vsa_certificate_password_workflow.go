package backgroundworkflows

import (
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	// CertificateRotationChildWorkflowTimeout is the timeout for each certificate rotation child workflow
	// Default: 5 minutes
	CertificateRotationChildWorkflowTimeout = time.Duration(env.GetInt("CERTIFICATE_ROTATION_CHILD_WORKFLOW_TIMEOUT_MINUTES", 5)) * time.Minute

	// PasswordRotationChildWorkflowTimeout is the timeout for each password rotation child workflow
	// Default: 5 minutes
	PasswordRotationChildWorkflowTimeout = time.Duration(env.GetInt("PASSWORD_ROTATION_CHILD_WORKFLOW_TIMEOUT_MINUTES", 5)) * time.Minute

	// CertificateRotationBatchSize is the number of certificate rotation child workflows to process in each batch
	// Default: 50 pools per batch
	CertificateRotationBatchSize = env.GetInt("CERTIFICATE_ROTATION_BATCH_SIZE", 50)

	// PasswordRotationBatchSize is the number of password rotation child workflows to process in each batch
	// Default: 50 pools per batch
	PasswordRotationBatchSize = env.GetInt("PASSWORD_ROTATION_BATCH_SIZE", 50)
)

// RotateVsaCertificateAndPasswordWorkflow rotates certificates and passwords used for VSA communication
// This workflow uses the global workflow timeout (WORKFLOW_GLOBAL_TIMEOUT_MINUTES) like other workflows
func RotateVsaCertificateAndPasswordWorkflow(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	
	// Read environment variables at runtime for better testability
	certificateRotationEnabled := env.GetBool("ENABLE_VSA_CERTIFICATE_ROTATION", false)
	passwordRotationEnabled := env.GetBool("ENABLE_VSA_PASSWORD_ROTATION", false)
	authType1PasswordRotationEnabled := env.GetBool("ENABLE_VSA_AUTHTYPE1_PASSWORD_ROTATION", false)
	

	if !certificateRotationEnabled && !passwordRotationEnabled {
		logger.Debug("Both certificate and password rotation disabled. Skipping workflow execution.")
		return nil
	}

	// Set up activity options with retry policy for rotation activities
	// Rotation activities can be retried once (MaximumAttempts = 2: 1 initial attempt + 1 retry)
	// Activity timeout is set to 5 minutes
	retryPolicy, err := workflows.PopulateRotationRetryPolicyParams()
	if err != nil {
		return err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rotateCertificateActivity := &backgroundactivities.RotateVcpToVsaCertificateActivity{}

	// Execute AuthType 1 and AuthType 2 rotations in parallel
	logger.Info("Starting parallel execution of certificate and password rotations")

	// Create futures for parallel execution
	var certRotationFuture workflow.Future
	var passwordRotationFuture workflow.Future

	// Step 1: Certificate Rotation for Auth Type 2 pools (Parallel)
	if certificateRotationEnabled {
		logger.Info("Starting certificate rotation for auth type 2 pools (parallel execution)")
		
		// Get all pools that use certificate authentication
		certRotationFuture = workflow.ExecuteActivity(ctx, rotateCertificateActivity.ListPoolsWithCertificateAuth)
	} else {
		logger.Debug("Certificate rotation disabled. Skipping certificate rotation.")
	}

	// Step 2: Password Rotation for Auth Type 1 pools (Parallel)
	if passwordRotationEnabled && authType1PasswordRotationEnabled {
		logger.Info("Starting password rotation for auth type 1 pools (parallel execution)")
		
		// Get all pools that use password authentication
		passwordRotationFuture = workflow.ExecuteActivity(ctx, rotateCertificateActivity.ListPoolsWithPasswordAuth)
	} else {
		if !passwordRotationEnabled {
			logger.Debug("Password rotation disabled. Skipping password rotation.")
		} else if !authType1PasswordRotationEnabled {
			logger.Info("AuthType USERNAME_PWD_SEC_MGR password rotation disabled via ENABLE_VSA_AUTHTYPE1_PASSWORD_ROTATION=false. Skipping password rotation for AuthType USERNAME_PWD_SEC_MGR pools.")
		}
	}

	// Wait for both parallel operations to complete
	logger.Info("Waiting for parallel rotation operations to complete")

	var certPools []*datamodel.Pool
	var passwordPools []*datamodel.Pool
	var certRotationErr error
	var passwordRotationErr error

	if certRotationFuture != nil {
		certRotationErr = certRotationFuture.Get(ctx, &certPools)
		if certRotationErr != nil {
			logger.Error("ListPoolsWithCertificateAuth failed", "error", certRotationErr)
		} else {
			logger.Info("Certificate pools retrieved successfully", "count", len(certPools))
		}
	}

	if passwordRotationFuture != nil {
		passwordRotationErr = passwordRotationFuture.Get(ctx, &passwordPools)
		if passwordRotationErr != nil {
			logger.Error("ListPoolsWithPasswordAuth failed", "error", passwordRotationErr)
		} else {
			logger.Info("Password pools retrieved successfully", "count", len(passwordPools))
		}
	}

	// Check if any pool listing failed
	if certRotationErr != nil || passwordRotationErr != nil {
		var errorMessages []string
		if certRotationErr != nil {
			errorMessages = append(errorMessages, fmt.Sprintf("certificate pool listing: %v", certRotationErr))
		}
		if passwordRotationErr != nil {
			errorMessages = append(errorMessages, fmt.Sprintf("password pool listing: %v", passwordRotationErr))
		}
		return fmt.Errorf("pool listing operations failed: %s", strings.Join(errorMessages, "; "))
	}

	// Step: Populate missing ca_uri for certificate authentication pools that don't have it
	// Only certificate auth type pools need ca_uri
	if len(certPools) > 0 {
		logger.Info("Populating missing ca_uri for certificate authentication pools from environment variables", "totalPools", len(certPools))
		err = workflow.ExecuteActivity(ctx, rotateCertificateActivity.PopulateMissingCaURI, certPools).Get(ctx, nil)
		if err != nil {
			logger.Warn("Failed to populate missing ca_uri for some pools", "error", err)
			// Don't fail the workflow if ca_uri population fails - this is a best-effort operation
			// Pools will still use environment variables as fallback
		} else {
			logger.Info("Successfully populated missing ca_uri for certificate authentication pools")
		}
	}

	// Track failures for both certificate and password rotations
	certificateRotationFailed := false
	passwordRotationFailed := false

	// Execute certificate rotation workflows in batches using control workflow pattern
	// This ensures only one workflow runs at a time per pool, preventing race conditions
	if len(certPools) > 0 {
		logger.Info("Starting certificate rotation workflows in batches using control workflow pattern", 
			"totalPools", len(certPools), 
			"batchSize", CertificateRotationBatchSize,
			"timeout", CertificateRotationChildWorkflowTimeout)
		
		batchSize := CertificateRotationBatchSize
		if batchSize <= 0 {
			batchSize = 50 // Fallback to default if invalid
		}

		// Initialize control workflow activity
		controlWorkflowActivity := &backgroundactivities.ControlWorkflowActivity{}

		for i := 0; i < len(certPools); i += batchSize {
			end := i + batchSize
			if end > len(certPools) {
				end = len(certPools)
			}
			batch := certPools[i:end]
			batchNumber := (i / batchSize) + 1
			totalBatches := (len(certPools) + batchSize - 1) / batchSize

			logger.Info("Processing certificate rotation batch", 
				"batchNumber", batchNumber, 
				"totalBatches", totalBatches,
				"batchSize", len(batch),
				"poolsRange", fmt.Sprintf("%d-%d", i+1, end))

			// Start all workflows in the current batch through control workflow
			// Each pool's operation is queued through its own control workflow
			var batchFutures []workflow.Future
			var batchPoolUUIDs []string
			for _, pool := range batch {
				logger.Debug("DEBUG: Queuing certificate rotation through control workflow",
					"poolUUID", pool.UUID,
					"poolName", pool.Name,
					"batchNumber", batchNumber)
				// Execute through control workflow activity to ensure sequential execution per pool
				future := workflow.ExecuteActivity(ctx, controlWorkflowActivity.ExecutePoolCertificateRotationSequentially, 
					pool.UUID, CertificateRotationChildWorkflowTimeout)
				batchFutures = append(batchFutures, future)
				batchPoolUUIDs = append(batchPoolUUIDs, pool.UUID)
			}

			// Wait for all workflows in the current batch to be queued before starting the next batch
			for batchIndex, future := range batchFutures {
				err := future.Get(ctx, nil)
				if err != nil {
					certificateRotationFailed = true
					logger.Error("Failed to queue certificate rotation for pool", 
						"poolUUID", batchPoolUUIDs[batchIndex], 
						"batchNumber", batchNumber,
						"error", err)
				}
			}

			logger.Info("Completed certificate rotation batch", 
				"batchNumber", batchNumber, 
				"totalBatches", totalBatches)
		}
	} else {
		logger.Info("No pools with certificate authentication found. Skipping certificate rotation.")
	}

	// Execute password rotation workflows in batches using control workflow pattern
	// This ensures only one workflow runs at a time per pool, preventing race conditions
	if len(passwordPools) > 0 {
		logger.Info("Starting password rotation workflows in batches using control workflow pattern", 
			"totalPools", len(passwordPools), 
			"batchSize", PasswordRotationBatchSize,
			"timeout", PasswordRotationChildWorkflowTimeout)
		
		batchSize := PasswordRotationBatchSize
		if batchSize <= 0 {
			batchSize = 50 // Fallback to default if invalid
		}

		// Initialize control workflow activity
		controlWorkflowActivity := &backgroundactivities.ControlWorkflowActivity{}

		for i := 0; i < len(passwordPools); i += batchSize {
			end := i + batchSize
			if end > len(passwordPools) {
				end = len(passwordPools)
			}
			batch := passwordPools[i:end]
			batchNumber := (i / batchSize) + 1
			totalBatches := (len(passwordPools) + batchSize - 1) / batchSize

			logger.Info("Processing password rotation batch", 
				"batchNumber", batchNumber, 
				"totalBatches", totalBatches,
				"batchSize", len(batch),
				"poolsRange", fmt.Sprintf("%d-%d", i+1, end))

			// Start all workflows in the current batch through control workflow
			// Each pool's operation is queued through its own control workflow
			var batchFutures []workflow.Future
			var batchPoolUUIDs []string
			for _, pool := range batch {
				logger.Debug("DEBUG: Queuing password rotation through control workflow",
					"poolUUID", pool.UUID,
					"poolName", pool.Name,
					"batchNumber", batchNumber)
				// Execute through control workflow activity to ensure sequential execution per pool
				future := workflow.ExecuteActivity(ctx, controlWorkflowActivity.ExecutePoolPasswordRotationSequentially, 
					pool.UUID, PasswordRotationChildWorkflowTimeout)
				batchFutures = append(batchFutures, future)
				batchPoolUUIDs = append(batchPoolUUIDs, pool.UUID)
			}

			// Wait for all workflows in the current batch to be queued before starting the next batch
			for batchIndex, future := range batchFutures {
				err := future.Get(ctx, nil)
				if err != nil {
					passwordRotationFailed = true
					logger.Error("Failed to queue password rotation for pool", 
						"poolUUID", batchPoolUUIDs[batchIndex], 
						"batchNumber", batchNumber,
						"error", err)
				}
			}

			logger.Info("Completed password rotation batch", 
				"batchNumber", batchNumber, 
				"totalBatches", totalBatches)
		}
	} else {
		logger.Info("No pools with password authentication found. Skipping password rotation.")
	}

	// Log summary of rotation operations
	// Note: Parent workflow succeeds if it successfully triggered all child workflows.
	// Individual pool failures are tracked at the child workflow level.
	if certificateRotationFailed || passwordRotationFailed {
		var warningMessages []string
		if certificateRotationFailed {
			warningMessages = append(warningMessages, "certificate rotation failed to queue for one or more pools")
			logger.Warn("Some certificate rotation operations failed to queue", "totalPools", len(certPools))
		} else if len(certPools) > 0 {
			logger.Info("Certificate rotation successfully queued for all auth type 2 pools", "totalPools", len(certPools))
		}
		if passwordRotationFailed {
			warningMessages = append(warningMessages, "password rotation failed to queue for one or more pools")
			logger.Warn("Some password rotation operations failed to queue", "totalPools", len(passwordPools))
		} else if len(passwordPools) > 0 {
			logger.Info("Password rotation successfully queued for all auth type 1 pools", "totalPools", len(passwordPools))
		}
		if len(warningMessages) > 0 {
			logger.Warn("Some rotation operations failed to queue", "warnings", strings.Join(warningMessages, "; "))
		}
	} else {
		if len(certPools) > 0 {
			logger.Info("Certificate rotation successfully queued for all auth type 2 pools", "totalPools", len(certPools))
		}
		if len(passwordPools) > 0 {
			logger.Info("Password rotation successfully queued for all auth type 1 pools", "totalPools", len(passwordPools))
		}
	}

	logger.Info("Certificate and password rotation workflow completed successfully - all child workflows have been triggered")
	return nil
}
