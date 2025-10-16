package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	customValidators "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/validator"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

var (
	createActiveDirectory = _createActiveDirectory
)

const (
	DefaultOrganizationalUnit = "CN=Computers"
)

// _createActiveDirectory orchestrates the creation of an Active Directory resource.
// It validates input, creates a job, and starts the corresponding Temporal workflow.
func _createActiveDirectory(
	ctx context.Context,
	se database.Storage,
	temporal client.Client,
	params *common.CreateActiveDirectoryParams,
) (*models.ActiveDirectory, string, error) {
	logger := util.GetLogger(ctx)

	adValidator := customValidators.NewActiveDirectoryValidator(ctx, se)
	err := adValidator.RegisterValidators()
	if err != nil {
		return nil, "", err
	}
	err = adValidator.ValidateParams(params)
	if err != nil {
		errMsg := strings.Join(func() []string {
			var messages []string
			for _, validationErr := range err.(validator.ValidationErrors) {
				messages = append(messages, validationErr.Translate(adValidator.Translator))
			}
			return messages
		}(), "; ")
		return nil, "", customerrors.NewUserInputValidationErr(errMsg)
	}

	account, err := getOrCreateAccount(ctx, se, params.AccountId)
	if err != nil {
		return nil, "", err
	}

	if params.OrganizationalUnit == "" {
		params.OrganizationalUnit = DefaultOrganizationalUnit
	}

	adUUID := utils.RandomUUID()
	params.CreatedAt = utils.GetTimeNow()
	params.UpdatedAt = utils.GetTimeNow()

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateActiveDirectory),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.ResourceId,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: adUUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	controlWorkflowID := fmt.Sprintf("Account_%d_ActiveDirectory_%s", account.ID, params.ResourceId)
	err = workflowsExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		workflows.CreateActiveDirectoryWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		params,
		adUUID,
		account.ID,
	)
	if err != nil {
		logger.Error("Failed to start create active directory workflow: ", "error", err)
		if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
			logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
		}
		return nil, "", err
	}

	return convertActiveDirectoryParamsToModel(params, adUUID), createdJob.UUID, nil
}

// convertActiveDirectoryParamsToModel maps input params and UUID to a models.ActiveDirectory instance.
func convertActiveDirectoryParamsToModel(params *common.CreateActiveDirectoryParams, uuid string) *models.ActiveDirectory {
	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID:      uuid,
			CreatedAt: params.CreatedAt,
			UpdatedAt: params.UpdatedAt,
		},
		AdName:   params.ResourceId,
		Username: params.Username,
		Password: params.Password,
		Domain:   params.Domain,
		DNS:      params.DNS,
		NetBIOS:  params.NetBIOS,
		State:    models.LifeCycleStateCreating,
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
			OrganizationalUnit:         params.OrganizationalUnit,
			Site:                       params.Site,
			SecurityOperators:          params.SecurityOperators,
			BackupOperators:            params.BackupOperators,
			Administrators:             params.Administrators,
			KdcIP:                      params.KdcIP,
			KdcHostname:                params.KdcHostname,
			AesEncryption:              params.AesEncryption,
			EncryptDCConnections:       params.EncryptDCConnections,
			LdapSigning:                params.LdapSigning,
			AllowLocalNFSUsersWithLdap: params.AllowLocalNFSUsersWithLdap,
			Description:                params.Description,
		},
	}
	return ad
}

// CreateActiveDirectory is the public orchestrator method for creating an Active Directory resource.
func (o *Orchestrator) CreateActiveDirectory(
	ctx context.Context,
	params *common.CreateActiveDirectoryParams,
) (*models.ActiveDirectory, string, error) {
	ad, jobUUID, err := createActiveDirectory(ctx, o.storage, o.temporal, params)
	if err != nil {
		return nil, "", err
	}
	return ad, jobUUID, nil
}
