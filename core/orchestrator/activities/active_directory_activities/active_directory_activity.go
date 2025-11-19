package active_directory_activities

import (
	"context"
	"errors"
	"fmt"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	adHelper "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	logmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type ActiveDirectoryActivity struct {
	SE database.Storage
}

// GetActiveDirectoryForPool retrieves the Active Directory configuration associated with the pool ID.
func (a ActiveDirectoryActivity) GetActiveDirectoryForPool(ctx context.Context, poolID int64) (*vsa.ActiveDirectory, error) {
	logger := util.GetLogger(ctx)
	activeDirectory, err := a.SE.GetActiveDirectoryForPoolByPoolID(ctx, poolID)
	if err != nil {
		logger.Error("Failed to fetch Active Directory for the pool", "poolID", poolID, "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if activeDirectory == nil {
		logger.Error("Active Directory not found for the pool", "poolID", poolID)
		return nil, errors.New("active directory not found for the pool")
	}

	if activeDirectory.ActiveDirectoryAttributes == nil {
		logger.Error("Active Directory attributes missing", "adUUID", activeDirectory.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("active directory attributes not populated"))
	}

	if activeDirectory.CredentialPath == "" {
		logger.Error("Active Directory credential path missing", "adUUID", activeDirectory.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("active directory credential path is empty"))
	}

	passwordSecret, err := adHelper.GetPasswordSecret(ctx, activeDirectory.CredentialPath)
	if err != nil {
		logger.Error("Failed to fetch Active Directory password", "adUUID", activeDirectory.UUID, "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if passwordSecret == nil || passwordSecret.SecretVersion == nil {
		logger.Error("Password secret or secret version is nil", "adUUID", activeDirectory.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("password secret fetch unsuccessful"))
	}
	password := passwordSecret.SecretVersion.Value
	encryptedPassword, err := utils.EncryptPassword(logmiddleware.Secret(password))
	if err != nil {
		logger.Error("failed to encrypt AD password", "error", err.Error())
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to encrypt AD password: %w", err))
	}

	attributes := activeDirectory.ActiveDirectoryAttributes
	ad := &vsa.ActiveDirectory{
		UUID:                    activeDirectory.UUID,
		Domain:                  activeDirectory.Domain,
		DNS:                     activeDirectory.DNS,
		NetBIOS:                 activeDirectory.NetBIOS,
		Username:                activeDirectory.Username,
		Password:                logmiddleware.Secret(*encryptedPassword),
		ManagedAD:               &attributes.ManagedAD,
		PrimaryAD:               &attributes.PrimaryAD,
		OrganizationalUnit:      attributes.OrganizationalUnit,
		Site:                    &attributes.Site,
		Users:                   attributes.AdUsers,
		AesEncryption:           &attributes.AesEncryption,
		LdapOverTLS:             &attributes.LdapOverTLS,
		EncryptDCConnections:    &attributes.EncryptDCConnections,
		ServerRootCaCertificate: &attributes.ServerRootCaCertificate,
		LdapSigning:             &attributes.LdapSigning,
	}

	return ad, nil
}
