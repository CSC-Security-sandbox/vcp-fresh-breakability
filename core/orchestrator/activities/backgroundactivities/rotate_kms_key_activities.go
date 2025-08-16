package backgroundactivities

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	getGcpService                        = hyperscaler2.GetGCPService
	gcpServiceCreateServiceAccountKey    = kms_activities.GcpServiceCreateServiceAccountKey
	deleteServiceAccountKeysExcludingKey = kms_activities.DeleteServiceAccountKeysExcludingKey
	listPoolsByKmsConfigId               = ListPoolsByKmsConfigId
	syncKeyWithOntap                     = _syncKeyWithOntap
	extractKeyID                         = _extractKeyID
)

const (
	serviceNameCmek = "cmek"
	keyPrefix       = "/keys/"
)

type RotateKmsSAKeyActivity struct {
	SE database.Storage
}

func (a *RotateKmsSAKeyActivity) ListKmsConfigs(ctx context.Context) ([]*datamodel.KmsConfig, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	conditions := [][]interface{}{
		{"state in ?", []string{string(gcpserver.KmsConfigV1betaKmsStateINUSE), string(gcpserver.KmsConfigV1betaKmsStateREADY)}}}
	kmsConfigs, err := se.GetMultipleKmsConfigs(ctx, conditions)
	if err != nil {
		logger.Errorf("Failed to list kms service accounts: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return kmsConfigs, nil
}

// GetKmsConfigServiceAccount retrieves the service account for a specific KMS config
func (a *RotateKmsSAKeyActivity) GetKmsConfigServiceAccount(ctx context.Context, kmsConfigID string) (*datamodel.ServiceAccount, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	// Get KMS config by ID with service account preloaded
	kmsConfig, err := se.GetKmsConfigByUUID(ctx, kmsConfigID)
	if err != nil {
		logger.Errorf("Failed to get KMS config: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	if kmsConfig.ServiceAccountID == nil || *kmsConfig.ServiceAccountID == 0 {
		logger.Error("No service account associated with KMS config", "kmsConfigID", kmsConfigID)
		return nil, errors.New("no service account associated with KMS config")
	}

	// Return the service account that should be preloaded
	if kmsConfig.ServiceAccount == nil {
		logger.Error("Service account not preloaded for KMS config", "kmsConfigID", kmsConfigID)
		return nil, errors.New("service account not found for KMS config")
	}

	return kmsConfig.ServiceAccount, nil
}

// GetKmsConfig retrieves a KMS config by its ID
func (a *RotateKmsSAKeyActivity) GetKmsConfig(ctx context.Context, kmsConfigID string) (*datamodel.KmsConfig, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	kmsConfig, err := se.GetKmsConfigByUUID(ctx, kmsConfigID)
	if err != nil {
		logger.Errorf("Failed to get KMS config: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return kmsConfig, nil
}

// RotateServiceAccountKey rotates the service account key for a given service account.
func (a *RotateKmsSAKeyActivity) RotateServiceAccountKey(ctx context.Context, serviceAccount *datamodel.ServiceAccount, kmsConfig *datamodel.KmsConfig) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	// Extract the keyID from the service account key
	saKeyId, err := extractKeyID(serviceAccount.ServiceAccountPasswordLocation)
	if err != nil {
		return err
	}
	existingKey := "projects/" + kmsConfig.KeyProjectID + "/serviceAccounts/" + serviceAccount.ServiceAccountEmail + keyPrefix + saKeyId

	gcpService, err := getGcpService(ctx)
	if err != nil {
		return err
	}

	serviceAccountKey, err := gcpServiceCreateServiceAccountKey(gcpService, ctx, serviceAccount.ServiceAccountEmail)
	if err != nil {
		return err
	}
	keyToExclude := serviceAccountKey.Name

	secretPassword, err := utils.EncryptPassword(log.Secret(serviceAccountKey.PrivateKeyData))
	if err != nil {
		logger.Errorf("Failed to encrypt service account key: %v", err)
		return err
	}
	err = kms_activities.AccessCryptoKey(ctx, kmsConfig, *secretPassword)
	if err != nil {
		logger.Errorf("Failed to access crypto key: %v", err)
		return err
	}

	pools, err := listPoolsByKmsConfigId(ctx, kmsConfig.ID, se)
	if err != nil {
		logger.Errorf("Failed to list pools by kms config id: %v", err)
		return err
	}

	// First sync the new key with all ONTAP clusters to validate it works
	for _, pool := range pools {
		if err = syncKeyWithOntap(ctx, se, serviceAccountKey.PrivateKeyData, serviceAccount.ServiceAccountPasswordLocation, pool); err != nil {
			return err
		}
	}

	// Only if all ONTAP sync operations succeed, update the database
	_, err = se.UpdateServiceAccountEmailAndKey(ctx, serviceAccount.UUID, serviceAccount.ServiceAccountEmail, serviceAccountKey.PrivateKeyData)
	if err != nil {
		logger.Errorf("Failed to update service account key: %v", err)
		return err
	}

	defer func() {
		if err != nil {
			keyToExclude = existingKey
			logger.Errorf("Failed to rotate kms key: %v", err)
		}
		// Delete the stale service account keys
		err = deleteServiceAccountKeysExcludingKey(ctx, gcpService, serviceAccount.ServiceAccountEmail, keyToExclude)
		if err != nil {
			logger.Errorf("Failed to delete service account old keys %s: %v", serviceAccountKey.Name, err)
			return
		}
	}()

	return nil
}

func ListPoolsByKmsConfigId(ctx context.Context, kmsConfigId int64, se database.Storage) ([]*datamodel.Pool, error) {
	logger := util.GetLogger(ctx)

	filter := dbutils.CreateFilterWithConditions(dbutils.NewFilterCondition("kms_config_id", "=", kmsConfigId),
		dbutils.NewFilterCondition("state", "!=", gcpserver.PoolV1betaStoragePoolStateERROR))
	poolViews, err := se.ListPools(ctx, filter)
	if err != nil {
		logger.Errorf("Failed to list pools: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	var pools []*datamodel.Pool
	for _, poolView := range poolViews {
		pools = append(pools, database.ConvertPoolViewToPool(poolView))
	}
	return pools, nil
}

func _syncKeyWithOntap(ctx context.Context, se database.Storage, newServiceAccountKey string, oldServiceAccountKey string, pool *datamodel.Pool) error {
	logger := util.GetLogger(ctx)

	svm, err := se.GetSvmForPoolID(ctx, pool.ID)
	if err != nil {
		return err
	}

	provider, err := GetOntapRestProviderForPool(ctx, se, pool)
	if err != nil {
		logger.Errorf("Failed to get ONTAP rest provider: %v", err)
		return err
	}

	sa, err := base64.StdEncoding.DecodeString(newServiceAccountKey)
	if err != nil {
		logger.Errorf("Failed to decode new service account key: %v", err)
		return err
	}

	serviceAccountKeySecret := log.Secret(sa)

	// Update ONTAP with new service account key
	_, _, err = provider.ModifyGcpKms(svm.SvmDetails.ExternalKmsConfigUUID, &serviceAccountKeySecret)
	if err != nil {
		return err
	}

	// Check the KMS configuration using the provider i.e ONTAP REST client on vsa cluster
	_, err = provider.IsGcpKmsReachable(vsa.GetKmsConfigParams{
		ExternalKmsConfigID: svm.SvmDetails.ExternalKmsConfigUUID,
	})

	if err != nil {
		// If new key fails reachability check, revert to old key
		logger.Warnf("New KMS key failed reachability check, reverting to old key. Error: %v", err)

		oldSa, decodeErr := utils.DecryptAndDecodeCredentials(oldServiceAccountKey)
		if decodeErr != nil {
			return err
		}

		oldServiceAccountKeySecret := log.Secret(oldSa)

		// Revert ONTAP to old service account key
		_, _, revertErr := provider.ModifyGcpKms(svm.SvmDetails.ExternalKmsConfigUUID, &oldServiceAccountKeySecret)
		if revertErr != nil {
			logger.Errorf("Failed to revert ONTAP to old service account key: %v", revertErr)
			// Return original error even if revert failed
		} else {
			logger.Infof("Successfully reverted ONTAP to old service account key for svm: %s", svm.UUID)
		}
		return err
	}
	logger.Infof(fmt.Sprintf("CMEK_KEY_ROTATION : Checked reachability for svm : %s on ontap", svm.UUID))

	return nil
}

func _extractKeyID(serviceAccountKey string) (string, error) {
	decryptKey, err := utils.DecryptPassword(log.Secret(serviceAccountKey))
	if err != nil {
		return "", errors.New("failed to decrypt service account key")
	}
	credentialsDecoded, err := base64.StdEncoding.DecodeString(*decryptKey)
	if err != nil {
		return "", err
	}
	var credentialsMap map[string]interface{}
	if err = json.Unmarshal(credentialsDecoded, &credentialsMap); err != nil {
		return "", fmt.Errorf("failed to unmarshal credentials: %v", err)
	}

	// Extract values from the map
	keyID, ok := credentialsMap["private_key_id"].(string)
	if !ok {
		return "", fmt.Errorf("key not found or not a string")
	}
	return keyID, nil
}
