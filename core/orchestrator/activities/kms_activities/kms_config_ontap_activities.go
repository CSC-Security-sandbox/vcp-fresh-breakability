package kms_activities

import (
	"context"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
)

func (j *KmsConfigActivity) ConfigureKmsForSvmActivity(ctx context.Context, svm *datamodel.Svm, node *coreModels.Node, params commonparams.CreatePoolParams) (*datamodel.Svm, error) {
	se := j.SE
	provider, err := activities.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if provider == nil {
		return nil, errors.New("provider not found")
	}
	if params.KmsConfigId == "" {
		return svm, nil
	}
	// Fetch the KMS config from the database
	kmsConfig, err := se.GetKmsConfig(ctx, params.KmsConfigId)
	if err != nil {
		return nil, err
	}

	// Decode the base64 encoded key
	decodedKey, err := utils.DecryptAndDecodeCredentials(kmsConfig.ServiceAccount.ServiceAccountPasswordLocation)
	if err != nil {
		return nil, err
	}

	// Create the KMS configuration using the provider i.e ONTAP REST client on vsa cluster
	res, err := provider.CreateKmsConfig(vsa.CreateKmsConfigParams{
		SvmName:           svm.Name,
		KeyName:           kmsConfig.KeyName,
		KeyRingLocation:   kmsConfig.KeyRingLocation,
		KeyRingName:       kmsConfig.KeyRing,
		ProjectID:         kmsConfig.KeyProjectID,                          // project id of keyfull path
		Credentials:       nillable.ToPointer(strfmt.Password(decodedKey)), // Use the long-term service account of SDE i.e., VCP service account Key
		PrivilegedAccount: kmsConfig.KmsAttributes.SdeServiceAccountEmail,  // Use SDE service account email for privileged operations i.e., impersonation
	})
	if err != nil {
		return nil, err
	}

	// Update the SVM with the KMS configuration IDs
	updatedSvm, err := se.UpdateSvmWithKmsConfigIDs(ctx, svm, kmsConfig.UUID, res.ExternalUUID)
	if err != nil {
		return nil, err
	}

	_, err = se.UpdateKmsConfigState(ctx, kmsConfig.UUID, coreModels.LifeCycleStateInUse, coreModels.LifeCycleStateInUseDetails)
	if err != nil {
		return nil, err
	}

	return updatedSvm, nil
}

func (j *KmsConfigActivity) CheckVsaKmsConfigReachableActivity(ctx context.Context, svm *datamodel.Svm, node *coreModels.Node) error {
	provider, err := activities.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if provider == nil {
		return errors.New("provider not found")
	}

	// Check the KMS configuration using the provider i.e ONTAP REST client on vsa cluster
	_, err = provider.IsGcpKmsReachable(vsa.GetKmsConfigParams{
		ExternalKmsConfigID: svm.SvmDetails.ExternalKmsConfigUUID,
	})

	if err != nil {
		if strings.Contains(err.Error(), "permission_denied") {
			return errors.New("GCP KMS key is not reachable from ONTAP - Service account lacks permission, retrying again")
		}
		if strings.Contains(err.Error(), "Invalid JWT Signature") || strings.Contains(err.Error(), "InvalidJWTSignature") {
			return errors.New("GCP KMS key is not reachable from ONTAP - Failed to establish connectivity" +
				" with the cloud key management service, retrying again")
		}
		return temporal.NewNonRetryableApplicationError("GCP KMS key is not reachable from VSA Clusters", ErrTypeKmsConfigNotReachableVsaCluster, err)
	}
	return err
}
