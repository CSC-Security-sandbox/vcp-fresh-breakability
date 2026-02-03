package helper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	vcpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	StorePasswordSecret                    = storePasswordSecret
	DeleteSecretFromGCP                    = deletePasswordSecret
	GetPasswordSecret                      = getPasswordSecret
	ConvertCVPActiveDirectoryV1BetaToModel = convertCVPActiveDirectoryV1BetaToModel
	CompareADStateHierarchy                = compareADStateHierarchy
	StringToActiveDirectoryState           = stringToActiveDirectoryState
	GetStatePriority                       = getStatePriority
	// Define the state hierarchy once, in priority order (highest to lowest)
	ActiveDirectoryStateHierarchy = []gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState{
		gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateUPDATING,
		gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateERROR,
		gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateINUSE,
		gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY,
		// Add more states here in priority order as needed
	}
)

func GeneratePasswordSecretId(secretManagerProjectID string, accountID string, adName string, region string) string {
	data := fmt.Sprintf("%s-%s-%s-%s", secretManagerProjectID, accountID, adName, region)
	hash := sha256.Sum256([]byte(data))
	return "gcnv-" + hex.EncodeToString(hash[:8])[:15]
}

func storePasswordSecret(ctx context.Context, password string, secretID string) error {
	logger := util.GetLogger(ctx)

	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		logger.Error("Failed to get GCP service", "error", err)
		return err
	}

	existingSecret, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, secretID)
	if err != nil {
		logger.Error("Failed to check existing secret", "secretID", secretID, "error", err)
		return err
	}

	// Only create secret if it doesn't exist
	if existingSecret == nil {
		projectID := env.SecretManagerProjectID
		_, err := gcpService.CreateSecret(projectID, env.Region, secretID, password)
		if err != nil {
			logger.Error("Failed to create secret", "secretID", secretID, "error", err)
			return err
		}
		logger.Info("Successfully created new secret", "secretID", secretID)
	} else {
		logger.Info("Secret already exists, skipping creation", "secretID", secretID)
	}

	return nil
}

func deletePasswordSecret(ctx context.Context, gcpService hyperscaler.GoogleServices, credentialPath string) error {
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

func getPasswordSecret(ctx context.Context, secretID string) (*hyperscalermodels.CustomSecret, error) {
	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		return nil, err
	}
	secret, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, secretID)
	if err != nil || secret == nil || secret.SecretVersion == nil {
		return nil, fmt.Errorf("failed to get secret for project: %s, secretID: %s, err: %s", env.SecretManagerProjectID, secretID, err)
	}
	return secret, nil
}

// compareADStateHierarchy evaluates and updates the primary Active Directory state based on the hierarchy of two input AD states.
// It prioritizes states according to activeDirectoryStateHierarchy (e.g., "UPDATING" > "ERROR" > "INUSE").
func compareADStateHierarchy(sdeAD, vcpAD *vcpModels.ActiveDirectory) {
	if sdeAD == nil || vcpAD == nil {
		return
	}

	// Convert string states to gcpgenserver enum format for comparison
	sdeState := stringToActiveDirectoryState(sdeAD.State)
	vcpState := stringToActiveDirectoryState(vcpAD.State)

	sdePriority := getStatePriority(sdeState)
	vcpPriority := getStatePriority(vcpState)

	// Select the state with higher priority (lower index)
	var selectedState string

	// If both states are not in hierarchy, keep the original sdeAD state
	if sdePriority == -1 && vcpPriority == -1 {
		return
	}

	// If one state is not in hierarchy, use the other
	if sdePriority == -1 {
		selectedState = vcpAD.State
	} else if vcpPriority == -1 {
		selectedState = sdeAD.State
	} else if sdePriority <= vcpPriority {
		selectedState = sdeAD.State
	} else {
		selectedState = vcpAD.State
	}

	// Update the sdeAD state with the selected state
	sdeAD.State = selectedState
}

// stringToActiveDirectoryState converts string state to gcpgenserver enum format
func stringToActiveDirectoryState(state string) gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState {
	switch state {
	case "CREATING":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateCREATING
	case "READY":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY
	case "UPDATING":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateUPDATING
	case "IN_USE":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateINUSE
	case "DELETING":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateDELETING
	case "ERROR":
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateERROR
	default:
		return gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateSTATEUNSPECIFIED
	}
}

// getStatePriority returns the priority index of a state (lower index = higher priority)
// Returns -1 if state is not in the hierarchy
func getStatePriority(state gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState) int {
	for i, hierarchyState := range ActiveDirectoryStateHierarchy {
		if state == hierarchyState {
			return i
		}
	}
	return -1 // State not found in hierarchy
}

// ConvertCVPActiveDirectoryV1BetaToModel converts CVP client's models.ActiveDirectoryV1beta to vcpModels.ActiveDirectory
func convertCVPActiveDirectoryV1BetaToModel(adV1Beta *models.ActiveDirectoryV1beta) *vcpModels.ActiveDirectory {
	if adV1Beta == nil {
		return nil
	}

	// Convert state from CVP format to VCP format
	state := "READY" // Default state
	if adV1Beta.ActiveDirectoryState != "" {
		state = adV1Beta.ActiveDirectoryState
	}

	stateDetails := "Active Directory is ready"
	if adV1Beta.ActiveDirectoryStateDetails != "" {
		stateDetails = adV1Beta.ActiveDirectoryStateDetails
	}

	ad := &vcpModels.ActiveDirectory{
		State:        state,
		StateDetails: stateDetails,
	}

	// Set UUID from ActiveDirectoryID
	ad.UUID = adV1Beta.ActiveDirectoryID

	// Set required fields with pointer dereference
	if adV1Beta.ResourceID != nil {
		ad.AdName = *adV1Beta.ResourceID
	}
	if adV1Beta.Username != nil {
		ad.Username = *adV1Beta.Username
	}
	if adV1Beta.Password != nil {
		ad.Password = *adV1Beta.Password
	}
	if adV1Beta.Domain != nil {
		ad.Domain = *adV1Beta.Domain
	}
	if adV1Beta.DNS != nil {
		ad.DNS = *adV1Beta.DNS
	}
	if adV1Beta.NetBIOS != nil {
		ad.NetBIOS = *adV1Beta.NetBIOS
	}

	// Set timestamps
	if !adV1Beta.CreatedAt.IsZero() {
		ad.CreatedAt = time.Time(adV1Beta.CreatedAt)
	}
	if !adV1Beta.UpdatedAt.IsZero() {
		ad.UpdatedAt = time.Time(adV1Beta.UpdatedAt)
	}
	if adV1Beta.DeletedAt != nil && !adV1Beta.DeletedAt.IsZero() {
		deletedAt := time.Time(*adV1Beta.DeletedAt)
		ad.DeletedAt = &deletedAt
	}

	// Convert attributes
	ad.ActiveDirectoryAttributes = &vcpModels.ActiveDirectoryAttributes{
		BackupOperators:   adV1Beta.BackupOperators,
		SecurityOperators: adV1Beta.SecurityOperators,
		Administrators:    adV1Beta.Administrators,
	}

	if adV1Beta.OrganizationalUnit != nil {
		ad.ActiveDirectoryAttributes.OrganizationalUnit = *adV1Beta.OrganizationalUnit
	}
	if adV1Beta.Site != nil {
		ad.ActiveDirectoryAttributes.Site = *adV1Beta.Site
	}
	// KdcIP and KdcHostname are plain strings in CVP model, not pointers
	if adV1Beta.KdcIP != "" {
		ad.ActiveDirectoryAttributes.KdcIP = adV1Beta.KdcIP
	}
	if adV1Beta.KdcHostname != "" {
		ad.ActiveDirectoryAttributes.KdcHostname = adV1Beta.KdcHostname
	}
	if adV1Beta.AesEncryption != nil {
		ad.ActiveDirectoryAttributes.AesEncryption = *adV1Beta.AesEncryption
	}
	if adV1Beta.EncryptDCConnections != nil {
		ad.ActiveDirectoryAttributes.EncryptDCConnections = *adV1Beta.EncryptDCConnections
	}
	if adV1Beta.LdapSigning != nil {
		ad.ActiveDirectoryAttributes.LdapSigning = *adV1Beta.LdapSigning
	}
	if adV1Beta.AllowLocalNFSUsersWithLdap != nil {
		ad.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap = *adV1Beta.AllowLocalNFSUsersWithLdap
	}
	if adV1Beta.Description != nil {
		ad.ActiveDirectoryAttributes.Description = *adV1Beta.Description
	}

	return ad
}

// ConvertUpdateParamsToDescribeParams converts common.UpdateActiveDirectoryParams to gcpgenserver.V1betaDescribeActiveDirectoryParams
func ConvertUpdateParamsToDescribeParams(updateParams *common.UpdateActiveDirectoryParams, projectNumber string) (*common.GetADParams, error) {
	if updateParams.XCorrelationId == "" {
		return nil, vsaerror.New("Correlation ID is empty")
	}

	return &common.GetADParams{
		ProjectNumber: projectNumber,
		LocationID:    updateParams.LocationId,
		CorrelationID: updateParams.XCorrelationId,
		UUID:          updateParams.ActiveDirectoryId,
	}, nil
}
