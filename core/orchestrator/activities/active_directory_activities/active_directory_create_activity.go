package active_directory_activities

import (
	// Standard library
	"context"
	"gorm.io/gorm"
	"time"

	// Third-party and local
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type ActiveDirectoryCreateActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

var (
	CvpClient           = cvp.CreateClient
	DeleteSecretFromGCP = deleteSecretFromGCP
)

func (a ActiveDirectoryCreateActivity) CreateVcpActiveDirectory(ctx context.Context, adRecord *datamodel.ActiveDirectory) error {
	// As the adRecord with password stored as GCP secret already exists in DB by now, just mark the AD as record and return
	adRecord.State = models.LifeCycleStateREADY
	adRecord.StateDetails = models.LifeCycleStateAvailableDetails
	_, err := a.SE.UpdateActiveDirectory(ctx, adRecord)
	if err != nil {
		return err
	}
	return nil
}

func (a ActiveDirectoryCreateActivity) RollbackActiveDirectory(ctx context.Context, ad *datamodel.ActiveDirectory) error {
	logger := util.GetLogger(ctx)
	if ad == nil {
		return nil
	}

	// Ensure AD state is updated to error regardless of secret deletion outcome
	defer func() {
		ad.State = models.LifeCycleStateError
		ad.StateDetails = models.LifeCycleStateCreationErrorDetails
		ad.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
		if _, updateErr := a.SE.UpdateActiveDirectory(ctx, ad); updateErr != nil {
			logger.Errorf("failed to update AD state during rollback: %v", updateErr)
		}
	}()

	if ad.CredentialPath != "" {
		gcpService, _ := hyperscaler.GetGCPService(ctx)
		err := DeleteSecretFromGCP(ctx, gcpService, ad.CredentialPath)
		if err != nil {
			logger.Errorf("failed to delete secret from GCP during AD creation rollback, err: %v", err)
			return vsaerror.New("failed to delete secret from GCP during AD creation rollback")
		}
	}

	return nil
}

func deleteSecretFromGCP(ctx context.Context, gcpService hyperscaler.GoogleServices, credentialPath string) error {
	logger := util.GetLogger(ctx)

	if gcpService == nil {
		return vsaerror.New("GCP service is nil, cannot delete secret from Secret Manager")
	}

	secret, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, credentialPath)
	if err != nil || secret == nil {
		logger.Infof("secret %s not found in Secret Manager - considering deletion successful", credentialPath)
	}

	if secret != nil {
		err = gcpService.DeleteSecret(env.SecretManagerProjectID, credentialPath)
		if err != nil {
			logger.Errorf("failed to delete password for secretID: %s, err : %v", credentialPath, err)
			return err
		}
	}

	return nil
}

// CreateSdeActiveDirectory PlaceHolder func to hold the SDE AD creation logic
func (a ActiveDirectoryCreateActivity) CreateSdeActiveDirectory(ctx context.Context, params *common.CreateActiveDirectoryParams) error {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.AccountId, params.LocationId, nil)
	body := &cvpModels.ActiveDirectoryV1beta{
		DNS:                        &params.DNS,
		Domain:                     &params.Domain,
		NetBIOS:                    &params.NetBIOS,
		Username:                   &params.Username,
		Password:                   &params.Password,
		ResourceID:                 &params.ResourceId,
		Administrators:             params.Administrators,
		SecurityOperators:          params.SecurityOperators,
		AesEncryption:              &params.AesEncryption,
		AllowLocalNFSUsersWithLdap: &params.AllowLocalNFSUsersWithLdap,
		BackupOperators:            params.BackupOperators,
		Description:                &params.Description,
		EncryptDCConnections:       &params.EncryptDCConnections,
		KdcIP:                      params.KdcIP,
		KdcHostname:                params.KdcHostname,
		Site:                       &params.Site,
		LdapSigning:                &params.LdapSigning,
		OrganizationalUnit:         &params.OrganizationalUnit,
	}
	createParams := &active_directories.V1betaCreateActiveDirectoryParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.AccountId,
		XCorrelationID: &params.XCorrelationId,
		Body:           body,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := CvpClient(logger, jwtToken)
	created, err := cvpClient.ActiveDirectories.V1betaCreateActiveDirectory(createParams)
	if err != nil {
		return err
	}
	if created == nil || created.Payload == nil {
		return customerrors.New("unknown error during the create active directory")
	}
	return nil
}
