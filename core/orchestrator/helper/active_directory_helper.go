package helper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	StorePasswordSecret = storePasswordSecret
	DeleteSecretFromGCP = deletePasswordSecret
	GetPasswordSecret   = getPasswordSecret
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
