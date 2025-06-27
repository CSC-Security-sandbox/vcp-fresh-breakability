package adminbackgroundjobs

import (
	"context"
	_ "embed"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/jobmanagerworkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflow_engine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

//go:embed admin_background_jobs.json
var data []byte

// adminJobSpecs stores the in-memory admin job specifications (read from admin_background_jobs.json).
// To schedule a new admin job, add it to the admin_background_jobs.json file with the state set to CREATING.
// To update a job's cron expression, modify the cron expression in admin_background_jobs.json and set the state to UPDATING.
// To delete a job, set its state to DELETING in admin_background_jobs.json.
// On application deployment, these in-memory job specs are synchronized with the database and Temporal Server.
// Usage Documentation: https://confluence.ngage.netapp.com/display/VSCP/Guide%3A+How+to+Create+and+Alter+Admin+Job+Schedules
var adminJobSpecs map[string]*datamodel.AdminJobSpec

// LoadJobSpecs loads the admin job specifications from the embedded JSON file into memory.
// It unmarshals the JSON data into the adminJobSpecs map. Returns an error if unmarshalling fails.
func LoadJobSpecs() error {
	err := json.Unmarshal(data, &adminJobSpecs)
	if err != nil {
		return err
	}
	return nil
}

// IsJobSpecRefreshNeeded checks if any admin job specifications need to be created, updated, or marked for deletion
// in the database based on the current in-memory job specs. It compares the loaded job specs with those in storage,
// performs necessary synchronization, and logs actions taken. Returns true if any job specs were updated, along with any error encountered.
func IsJobSpecRefreshNeeded(ctx context.Context, se database.Storage, logger log.Logger) (bool, error) {
	areJobSpecsUpdated := false

	for jobType, latestJobSpec := range adminJobSpecs {
		// To synchronize job specs, only those in CREATING, UPDATING, or DELETING states are considered.
		// A SCHEDULED state indicates the job spec is current and present in memory for reference.
		if latestJobSpec.State != scheduler.JobStatusScheduled {
			logger.Infof("Syncing the job: %s", jobType)
			existingJob, err := se.GetAdminJobSpecByJobType(ctx, jobType)
			if err != nil {
				// If the job spec is not found in the database, we will create it using the latest in-memory specification.
				if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
					logger.Debugf("No existing job spec found for jobType: %s", jobType)
				} else {
					logger.Errorf("Failed to get existing job spec for jobType: %s, error: %v", jobType, err)
					// TODO: Implement alerting or notification logic here if necessary
					continue
				}
			}

			if existingJob == nil {
				latestJobSpec.UUID = uuid.NewString()
				newJobSpec, err := se.CreateAdminJobSpec(ctx, latestJobSpec)
				if err != nil {
					var customErr *vsaerrors.CustomError
					if vsaerrors.As(err, &customErr) {
						// If the error is due to a duplicate key, it means another pod has already created this job spec
						if vsaerrors.Is(customErr.OriginalErr, gorm.ErrDuplicatedKey) {
							continue
						}
					}
					logger.Errorf("Failed to create new admin job spec for jobType: %s, error: %v", jobType, err)
					// TODO: Implement alerting or notification logic here if necessary
					continue
				}
				areJobSpecsUpdated = true
				logger.Infof("Created new admin job spec for jobType: %s, cronExpression: %s", newJobSpec.JobType, newJobSpec.CronExpression)
			} else if latestJobSpec.State == scheduler.JobStatusUpdating && existingJob.CronExpression != latestJobSpec.CronExpression {
				// Currently, only updates to the cron expression of existing job specs are supported.
				// Other types of updates may be handled in the future if needed.
				latestJobSpec.ID = existingJob.ID
				err := se.UpdateAdminJobSpec(ctx, latestJobSpec)
				if err != nil {
					logger.Errorf("Failed to update admin job spec for jobType: %s, error: %v", jobType, err)
					// TODO: Implement alerting or notification logic here if necessary
					continue
				}
				areJobSpecsUpdated = true
				logger.Infof("Updated admin job spec for jobType: %s, cronExpression: %s", jobType, latestJobSpec.CronExpression)
			} else if latestJobSpec.State == scheduler.JobStatusDeleting && existingJob.State != scheduler.JobStatusDeleting && existingJob.State != scheduler.JobStatusDeleted {
				latestJobSpec.ID = existingJob.ID
				err := se.UpdateAdminJobSpec(ctx, latestJobSpec)
				if err != nil {
					logger.Errorf("Failed to update admin job spec to DELETING for jobType: %s, error: %v", jobType, err)
					// TODO: Implement alerting or notification logic here if necessary
					continue
				}
				areJobSpecsUpdated = true
				logger.Infof("Updated admin job spec to DELETING for jobType: %s", jobType)
			}
		}
	}

	return areJobSpecsUpdated, nil
}

// LaunchJobManagerWorkflow initiates the background job manager workflow using the Temporal client.
// The workflow is started in the background-workflows task queue and returns an error if it fails to launch.
func LaunchJobManagerWorkflow(ctx context.Context, temporalClient client.Client, logger log.Logger) error {
	_, err := temporalClient.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:                workflow_engine.BackgroundTaskQueue,
			ID:                       scheduler.JobManagerWorkflowID,
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		},
		jobmanagerworkflows.JobManagerWorkflow,
	)
	if err != nil {
		logger.Errorf("Failed to start background job manager workflow: %v", err)
		return err
	}
	return nil
}
