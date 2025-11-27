package supervisorhandler

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/sde"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// Event represents a supervisor-detected condition for a workflow.
type Event string

const (
	// EventTimeout indicates that a workflow execution timed out.
	EventTimeout Event = "TIMEOUT"
	// WorkflowTimeoutDetail records the error detail that accompanies timeout cleanup.
	WorkflowTimeoutDetail = "temporal workflow timed out"
)

// Handler encapsulates compensating actions for supported job types when a supervisor event is detected.
type Handler interface {
	JobTypes() []models.JobType
	Handle(ctx context.Context, job *datamodel.Job, event Event, storage database.Storage) error
}

// CmekHandler implements Handler for CMEK resources.
type CmekHandler struct{}

// NewCmekHandler returns the default handler that cleans up CMEK-related resources.
func NewCmekHandler() *CmekHandler {
	return &CmekHandler{}
}

// JobTypes enumerates the job types supported by the CMEK handler.
func (h *CmekHandler) JobTypes() []models.JobType {
	return []models.JobType{
		models.JobTypeCreateKmsConfig,
	}
}

// Handle removes KMS configuration artifacts both in SDE and VSA for the job.
func (h *CmekHandler) Handle(ctx context.Context, job *datamodel.Job, event Event, storage database.Storage) error {
	if event != EventTimeout {
		return nil
	}

	correlationID := utils.GetCoRelationIDFromContext(ctx)

	logger := util.GetLogger(ctx).With(log.Fields{
		"jobUUID":                               job.UUID,
		string(middleware.RequestCorrelationID): correlationID,
	})

	if job.JobAttributes == nil || job.JobAttributes.ResourceUUID == "" {
		logger.Warnf("workflow-supervisor-task: job lacks resource UUID; skipping CMEK cleanup")
		return nil
	}

	kmsConfig, err := storage.GetKmsConfig(ctx, job.JobAttributes.ResourceUUID)
	if err != nil {
		if vsaerrors.IsNotFoundErr(err) {
			logger.Warnf("workflow-supervisor-task: kms config already deleted")
			return nil
		}
		return fmt.Errorf("load kms config: %w", err)
	}

	deleteParams := &common.DeleteKmsConfigParams{
		KmsConfigID:    kmsConfig.UUID,
		AccountName:    kmsConfig.CustomerProjectID,
		Region:         kmsConfig.KeyRingLocation,
		XCorrelationID: job.CorrelationID,
	}

	if kmsConfig.KmsAttributes != nil && kmsConfig.KmsAttributes.SdeKmsConfigUUID != "" && deleteParams.AccountName != "" {
		token, tokenErr := auth.GetSignedJwtToken(deleteParams.AccountName)
		if tokenErr != nil {
			logger.Warnf("workflow-supervisor-task: unable to fetch auth token for SDE cleanup: %v", tokenErr)
		} else {
			ctxWithToken := context.WithValue(ctx, middleware.AuthorizationToken, token)
			if _, sdeErr := sde.DeleteSDEKmsConfiguration(ctxWithToken, kmsConfig, deleteParams); sdeErr != nil {
				logger.Warnf("workflow-supervisor-task: SDE cleanup failed: %v", sdeErr)
			}
		}
	} else {
		logger.Debug("workflow-supervisor-task: skipping SDE cleanup (missing attributes)")
	}

	if _, err := storage.DeleteKmsConfig(ctx, kmsConfig.UUID, models.LifeCycleStateError, WorkflowTimeoutDetail); err != nil {
		return fmt.Errorf("delete kms config from VSA: %w", err)
	}

	logger.Infof("workflow-supervisor-task: kms config %s removed from VSA", kmsConfig.UUID)
	return nil
}
