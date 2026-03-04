package active_directory_activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type ActiveDirectoryDeleteActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

// CheckDeletionAllowedResult holds the result of CheckDeletionAllowed activity
type CheckDeletionAllowedResult struct {
	ADExists        bool
	DeletionAllowed bool
}

// CheckDeletionAllowed checks if Active Directory can be deleted
func (a ActiveDirectoryDeleteActivity) CheckDeletionAllowed(ctx context.Context, params *common.DeleteActiveDirectoryParams) (*CheckDeletionAllowedResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("Starting CheckDeletionAllowed activity", "active_directory_uuid", params.ActiveDirectoryUUID)

	// Check if the Active Directory exists in the database for this account
	ad, err := a.SE.GetActiveDirectoryByUuidAndAccountId(ctx, params.ActiveDirectoryUUID, params.AccountId)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger.Info("Active Directory not found in VCP database", "active_directory_uuid", params.ActiveDirectoryUUID)
			return &CheckDeletionAllowedResult{
				ADExists:        false,
				DeletionAllowed: true,
			}, nil
		}
		logger.Error("Failed to get Active Directory from database", "error", err, "active_directory_uuid", params.ActiveDirectoryUUID)
		return nil, err
	}

	// Check if any SVMs are using this Active Directory
	svms, err := a.SE.GetSVMsUsingActiveDirectory(ctx, ad.ID)
	if err != nil {
		logger.Error("Failed to check SVMs using Active Directory", "error", err, "active_directory_id", ad.ID)
		return nil, err
	}

	if len(svms) > 0 {
		svmNames := make([]string, len(svms))
		for i, svm := range svms {
			svmNames[i] = svm.Name
		}
		logger.Errorf("Active Directory is in use by %d SVM(s): %v", len(svms), svmNames)
		return &CheckDeletionAllowedResult{
			ADExists:        true,
			DeletionAllowed: false,
		}, nil
	}

	// Check if any pools are using this Active Directory
	filter := dbutils.CreateFilterWithConditions(dbutils.NewFilterCondition("active_directory_id", "=", ad.ID))
	pools, err2 := a.SE.ListPools(ctx, filter)
	if err2 != nil {
		logger.Errorf("Failed to check pools using Active Directory error: %v", err2)
		return nil, err2
	}
	if len(pools) > 0 {
		poolNames := make([]string, len(pools))
		for i, pool := range pools {
			poolNames[i] = pool.Name
		}
		logger.Errorf("Active Directory credentials are in use by %d pool(s): %v", len(pools), poolNames)
		return &CheckDeletionAllowedResult{
			ADExists:        true,
			DeletionAllowed: false,
		}, nil
	}

	logger.Info("Active Directory can be deleted", "active_directory_uuid", params.ActiveDirectoryUUID)
	return &CheckDeletionAllowedResult{
		ADExists:        true,
		DeletionAllowed: true,
	}, nil
}

// DeleteVcpActiveDirectory deletes an Active Directory from VCP DB
func (a ActiveDirectoryDeleteActivity) DeleteVcpActiveDirectory(ctx context.Context, params *common.DeleteActiveDirectoryParams) error {
	logger := util.GetLogger(ctx)
	logger.Debug("Starting DeleteVcpActiveDirectory activity", "active_directory_uuid", params.ActiveDirectoryUUID)

	// Check if the Active Directory exists in the database for this account
	ad, err := a.SE.GetActiveDirectoryByUuidAndAccountId(ctx, params.ActiveDirectoryUUID, params.AccountId)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger.Info("Active Directory not found in database, considering it already deleted")
			return nil
		}
		logger.Errorf("Failed to get Active Directory from database. error: %v", err)
		return err
	}

	credentialPath := ad.CredentialPath

	// Delete the Active Directory from the database
	err = a.SE.DeleteActiveDirectory(ctx, params.ActiveDirectoryUUID)
	if err != nil {
		logger.Errorf("Failed to delete Active Directory from database: %v", err)
		return err
	}

	// Delete the password secret from Secret Manager
	gcpService, _ := hyperscaler.GetGCPService(ctx)
	if gcpService != nil && credentialPath != "" {
		projectID := env.SecretManagerProjectID
		err = gcpService.DeleteSecret(projectID, credentialPath)
		if err != nil {
			// Log the error but don't fail the operation
			logger.Warnf("Failed to delete password secret from Secret Manager: %v", err)
		} else {
			logger.Debug("Successfully deleted password secret from Secret Manager")
		}
	}

	logger.Infof("Successfully deleted Active Directory from VCP: %s", params.ActiveDirectoryUUID)
	return nil
}

// DeleteSdeActiveDirectory deletes an Active Directory from SDE/CVP
// Returns error only if it's a real error; treats 404 as success
func (a ActiveDirectoryDeleteActivity) DeleteSdeActiveDirectory(ctx context.Context, params *common.DeleteActiveDirectoryParams) error {
	logger := util.GetLogger(ctx)
	logger.Debug("Starting DeleteSdeActiveDirectory activity")

	jwtToken := utils.GetCVPJWTFromContext(ctx)

	// Create CVP client
	cvpClient := cvp.CreateClient(logger, jwtToken)

	// Prepare the delete parameters
	deleteParams := &active_directories.V1betaDeleteActiveDirectoryParams{
		ProjectNumber:     params.ProjectNumber,
		LocationID:        env.Region,
		ActiveDirectoryID: params.ActiveDirectoryUUID,
	}

	// Call CVP to delete the Active Directory
	resp, err := cvpClient.ActiveDirectories.V1betaDeleteActiveDirectory(deleteParams)
	if err != nil {
		logger.Errorf("Failed to delete Active Directory from CVP: %v", err)

		// Handle different error types
		switch e := err.(type) {
		case *active_directories.V1betaDeleteActiveDirectoryConflict:
			conflictErr := vsaerrors.NewVCPError(vsaerrors.ErrActiveDirectoryDeleteErrorDueToInUseByPool,
				fmt.Errorf("Active Directory deletion conflict: %v", e.Error()))
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(conflictErr)
		case *active_directories.V1betaDeleteActiveDirectoryBadRequest:
			return customerrors.NewUserInputValidationErr(fmt.Sprintf("Bad request when deleting Active Directory: %v", e.Error()))
		case *active_directories.V1betaDeleteActiveDirectoryDefault:
			// Check if it's a 404
			if e.Code() == 404 {
				logger.Info("Active Directory not found at SDE (404), considering it already deleted")
				return nil
			}
			return err
		default:
			return err
		}
	}

	if resp != nil && resp.Payload != nil {
		logger.Infof("Successfully initiated Active Directory deletion in CVP, operation: %v", resp.Payload.Name)
	}

	logger.Infof("Successfully deleted Active Directory from SDE/CVP: %s", params.ActiveDirectoryUUID)
	return nil
}
