package kms_activities

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

const (
	EncryptingState                     = "encrypting"
	EncryptedState                      = "encrypted"
	PollWaitIntervalForVolumeEncryption = 30
)

// MigrateSdeKmsConfigActivity initiates migration of a CMEK policy in SDE
func (kmsActivity *KmsConfigActivity) MigrateSdeKmsConfigActivity(ctx context.Context, params *common.MigrateKmsConfigParams) (*kms_configurations.V1betaEncryptVolumesAccepted, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetAuthTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	migrateKmsConfigParams := &kms_configurations.V1betaEncryptVolumesParams{
		KmsConfigID:    params.SdeUUID,
		LocationID:     params.LocationID,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &xCorrelationID,
	}

	cvpResponse, cvpErr := cvpClient.KmsConfigurations.V1betaEncryptVolumes(migrateKmsConfigParams)
	if cvpErr != nil {
		logger.Error("Error migrating KMS configuration: ", cvpErr)
		return nil, temporal.NewNonRetryableApplicationError("Error migrating SDE KMS Configuration", "DescribeOperationError", cvpErr)
	}
	if cvpResponse == nil || cvpResponse.Payload == nil {
		return nil, errors.New("Error encountered during SDE CMEK migration: CVP response is empty")
	}
	return cvpResponse, nil
}

// PollMigrateSdeKmsConfigActivity polls the SDE KMS migration operation until it is done.
func (j *KmsConfigActivity) PollMigrateSdeKmsConfigActivity(ctx context.Context, params *common.MigrateKmsConfigParams, response *kms_configurations.V1betaEncryptVolumesAccepted) error {
	if response == nil || response.Payload == nil {
		return temporal.NewNonRetryableApplicationError("Error migrating SDE KMS Configuration", "DescribeOperationError", errors.New("SDE CMEK migration error"))
	}

	if !*response.Payload.Done {
		payload, err := GetResponseforPollCvpOperation(ctx, response.Payload.Name, params.ProjectNumber, params.LocationID)
		if err != nil {
			return err
		}
		response.Payload = payload
	}

	// SDE shall update the state of the CMEK policy
	return nil
}

// MigrateVsaPoolActivity migrates one VSA pool over to EKM
func (j *KmsConfigActivity) MigrateVsaPoolActivity(ctx context.Context, volumes []*datamodel.Volume, node *models.Node) error {
	se := j.SE
	logger := util.GetLogger(ctx)
	var volumeMigrationFailed, volumeMigrationComplete bool

	provider, err := activities.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Unable to get provider for pool - GetProviderByNode failure: %s", err.Error())
		return err
	}

	for _, volume := range volumes {
		getVolumeParams := vsa.GetVolumeParams{VolumeName: volume.Name}
		if volume.VolumeAttributes != nil {
			getVolumeParams.UUID = volume.VolumeAttributes.ExternalUUID
		} else {
			logger.Errorf("External UUID not present in Volume attributes of volume %s in VSA", volume.Name)
			volumeMigrationFailed = true
			continue
		}
		if volume.Svm != nil {
			getVolumeParams.SvmName = volume.Svm.Name
		} else {
			logger.Errorf("SVM name not present in Volume data-model of volume %s in VSA", volume.Name)
			volumeMigrationFailed = true
			continue
		}

		getEncryptionStatus, errStatus := provider.GetVolumeEncryptionStatus(getVolumeParams)
		if errStatus != nil {
			logger.Errorf("Failed to get encryption status for volume %s, aborting encyrption... Error: %s", volume.Name, errStatus.Error())
			volumeMigrationFailed = true
			continue
		}
		if getEncryptionStatus == nil {
			logger.Errorf("Failed to get encryption status for volume %s, aborting encyrption", volume.Name)
			volumeMigrationFailed = true
			continue
		}
		if *getEncryptionStatus.Encryption.State == EncryptedState {
			continue
		} else if *getEncryptionStatus.Encryption.State != EncryptingState {
			// Encrypt volume
			errEnableEncryption := provider.UpdateVolumeEnableEncryption(vsa.UpdateVolumeParams{
				UUID:             getVolumeParams.UUID,
				EncryptionEnable: true,
			})
			if errEnableEncryption != nil {
				logger.Errorf("Failed to initiate encryption of volume %s in VSA: %s", volume.Name, errEnableEncryption.Error())
				volumeMigrationFailed = true
				continue
			}
		}

		stateOfVol := volume.State
		stateDetailsOfVol := volume.StateDetails
		errUpdateVol := se.UpdateVolumeFields(ctx, volume.UUID, map[string]interface{}{
			"state":         models.LifeCycleStateUpdating,
			"state_details": models.LifeCycleStateVolMigratingDetails,
		})
		if errUpdateVol != nil {
			logger.Errorf("Unable to set state of volume to updating %s before encryption", volume.Name)
			continue
		}

		var volArrayForTimeout []*datamodel.Volume
		volArrayForTimeout = append(volArrayForTimeout, volume)
		volumePollTimeout := utils.DetermineStartToCloseTimeoutBasedOnUsedSize(volArrayForTimeout)
		pollTimeout := time.After(time.Duration(volumePollTimeout) * time.Minute)
		for !volumeMigrationFailed && !volumeMigrationComplete {
			select {
			case <-pollTimeout:
				logger.Errorf("Polling timed out for volume %s", volume.Name)
				volumeMigrationFailed = true
			default:
				getEncryptionResponse, errEncryptStatus := provider.GetVolumeEncryptionStatus(getVolumeParams)
				if errEncryptStatus != nil {
					logger.Errorf("Failed to get encryption status for volume %s during polling: %s", volume.Name, errEncryptStatus.Error())
					volumeMigrationFailed = true
					continue
				}
				if getEncryptionResponse != nil {
					switch *getEncryptionResponse.Encryption.State {
					case EncryptingState:
						logger.Infof("Volume encryption ongoing for %s ...", volume.Name)
						time.Sleep(time.Duration(PollWaitIntervalForVolumeEncryption) * time.Second)
					case EncryptedState:
						logger.Infof("Volume encryption completed for %s ...", volume.Name)
						volumeMigrationComplete = true
					default:
						logger.Errorf("Unexpected encryption state for volume %s during polling: %s", volume.Name, *getEncryptionResponse.Encryption.State)
						volumeMigrationFailed = true
					}
				} else {
					logger.Errorf("Failed to retrieve encryption status for volume %s during polling", volume.Name)
					volumeMigrationFailed = true
				}
			}
		}
		errUpdateVol = se.UpdateVolumeFields(ctx, volume.UUID, map[string]interface{}{
			"state":         stateOfVol,
			"state_details": stateDetailsOfVol,
		})
		if errUpdateVol != nil {
			logger.Errorf("Unable to reset state and state-details of volume %s after encryption", volume.Name)
		}
	}

	if volumeMigrationFailed {
		return temporal.NewNonRetryableApplicationError("Encryption failed for one/some of the volumes", "CmekVolumeMigrationError", errors.New("Volume encryption failure"))
	}
	return nil
}

// CompleteKmsMigrationActivity updates KmsConfig State of VCP KmsConfig (if it exists) using the results from the Verify operation
func (j *KmsConfigActivity) CompleteKmsMigrationActivity(ctx context.Context, kmsConfigUUID string) error {
	se := j.SE

	kmsConfig, err := se.GetKmsConfig(ctx, kmsConfigUUID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return nil
		}
		return err
	}

	isHealthy := true
	healthError := ""
	err = AccessCryptoKey(ctx, se, kmsConfig)
	if err != nil {
		isHealthy = false
		healthError = err.Error()
	}

	kmsConfigInUse, err := isKmsConfigInUse(ctx, se, kmsConfig)
	if err != nil {
		return err
	}

	errUpdateHealth := UpdateKmsConfigHealth(ctx, se, kmsConfig, isHealthy, healthError, kmsConfigInUse)
	if errUpdateHealth != nil {
		return errUpdateHealth
	}

	return nil
}
