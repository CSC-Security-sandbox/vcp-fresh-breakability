package active_directory_activities

import (
	// Standard library
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"

	// Third-party and local
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	logger "golang.org/x/exp/slog"
)

type ActiveDirectoryCreateActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

// ActiveDirectoryGroupBuiltInBackupOperators defines the name of the built-in backup operators group
const ActiveDirectoryGroupBuiltInBackupOperators = `BUILTIN\Backup Operators`

// ActiveDirectoryGroupBuiltInAdministrators defines the name of the built-in administrators group
const ActiveDirectoryGroupBuiltInAdministrators = `BUILTIN\Administrators`

// ActiveDirectorySeSecurityPrivilege defines the name of the SE security privilege
const ActiveDirectorySeSecurityPrivilege = `SeSecurityPrivilege`

var (
	storePasswordSecret = _storePasswordSecret
)

func (a ActiveDirectoryCreateActivity) CreateVcpActiveDirectory(ctx context.Context, params *common.CreateActiveDirectoryParams, adUUID string, accountId int64) (*datamodel.ActiveDirectory, error) {
	ad, adErr := a.SE.GetActiveDirectoryByNameAndAccountID(ctx, params.ResourceId, accountId)
	if ad != nil {
		logger.Debug("Existing Active Directory found with the given name",
			"active_directory_name", params.ActiveDirectoryId)
		return nil, customerrors.NewConflictErr("Active Directory with the given name already exists")
	}

	if adErr != nil {
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(adErr, &customErr) && !customerrors.IsNotFoundErr(customErr.Unwrap()) {
			// propagate the Non-NotFound errors
			return nil, adErr
		}
		logger.Debug("No existing Active Directory found with the given name, proceeding to create a new Active Directory")
	}

	gcpService, _ := hyperscaler.GetGCPService(ctx)
	secretId := generatePasswordSecretId(env.SecretManagerProjectID, params.AccountId, params.ResourceId, env.Region)
	err := storePasswordSecret(gcpService, params.Password, secretId)
	if err != nil {
		return nil, err
	}

	activeDirectory := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: adUUID,
		},
		AdName:         params.ResourceId,
		Username:       params.Username,
		CredentialPath: secretId,
		Domain:         params.Domain,
		DNS:            params.DNS,
		NetBIOS:        params.NetBIOS,
		State:          string(gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY),
		AccountId:      accountId,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: params.OrganizationalUnit,
			Site:               params.Site,
			AdUsers: map[string][]string{
				ActiveDirectoryGroupBuiltInBackupOperators: params.BackupOperators,
				ActiveDirectoryGroupBuiltInAdministrators:  params.Administrators,
				ActiveDirectorySeSecurityPrivilege:         params.SecurityOperators,
			},
			KdcIP:                      params.KdcIP,
			KdcHostname:                params.KdcHostname,
			AesEncryption:              params.AesEncryption,
			EncryptDCConnections:       params.EncryptDCConnections,
			LdapSigning:                params.LdapSigning,
			AllowLocalNFSUsersWithLdap: params.AllowLocalNFSUsersWithLdap,
			Description:                params.Description,
			PrimaryAD:                  true,
		},
	}

	directory, err := a.SE.CreateActiveDirectory(ctx, activeDirectory)
	if err != nil {
		return nil, err
	}

	return directory, nil
}

// CreateSdeActiveDirectory PlaceHolder func to hold the SDE AD creation logic
func (a ActiveDirectoryCreateActivity) CreateSdeActiveDirectory(ctx context.Context, params *common.CreateActiveDirectoryParams) (*models.AggregateDistributionResult, error) {
	return nil, nil
}

func _storePasswordSecret(gcpService hyperscaler.GoogleServices, password string, secretID string) error {
	var secret *hyperscalermodels.CustomSecret
	projectID := env.SecretManagerProjectID
	secret, err := gcpService.CreateSecret(projectID, env.Region, secretID, password)
	if err != nil {
		return err
	}
	common.AddToUserAuthCache(secretID, secret.SecretVersion.Value)
	return nil
}

func generatePasswordSecretId(secretManagerProjectID string, accountID string, adName string, region string) string {
	data := fmt.Sprintf("%s-%s-%s-%s", secretManagerProjectID, accountID, adName, region)
	hash := sha256.Sum256([]byte(data))
	return "gcnv-" + hex.EncodeToString(hash[:8])[:15]
}
