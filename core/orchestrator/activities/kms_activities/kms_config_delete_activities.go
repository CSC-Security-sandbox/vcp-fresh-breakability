package kms_activities

import (
	"context"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/sde"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	sdeDeleteSDEKmsConfiguration = sde.DeleteSDEKmsConfiguration
	describeSDEJob               = sde.DescribeSDEJob
)

func (a *KmsConfigActivity) DeleteSDEKmsConfig(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (*string, error) {
	logger := util.GetLogger(ctx)

	resp, err := sdeDeleteSDEKmsConfiguration(ctx, kmsConfig, params)
	if err != nil {
		return nil, err
	}
	logger.Debug("KmsConfig:%s delete sent to sde", kmsConfig.ResourceID)

	operation, ok := resp.(*gcpserver.OperationV1beta)
	if ok {
		return &strings.Split(operation.Name.Value, "/")[7], nil
	}
	return nil, nil
}

func (a *KmsConfigActivity) DescribeSDEDeleteJob(ctx context.Context, jobUuid *string, params *common.DeleteKmsConfigParams) error {
	if nillable.IsNilOrEmpty(jobUuid) {
		return nil
	}
	return describeSDEJob(ctx, *jobUuid, params.Region, params.AccountName, params.XCorrelationID)
}

// DisableKmsServiceAccount updates the KMS ServiceAccounts as disabled in the database.
func (j *KmsConfigActivity) DisableKmsServiceAccount(ctx context.Context, kmsConfig *datamodel.KmsConfig) error {
	se := j.SE
	_, err := se.UpdateServiceAccountState(ctx, kmsConfig.ServiceAccount.UUID, models.LifeCycleStateDisabled, models.LifeCycleStateDisabledDetails)
	return err
}

func (a *KmsConfigActivity) DeleteKmsConfig(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	_, err := se.DeleteKmsConfig(ctx, params.KmsConfigID, models.LifeCycleStateDeleted, models.LifeCycleStateDeletedDetails)
	if err != nil {
		return err
	}
	logger.Debug("KmsConfig:%s deleted successfully in the db", kmsConfig.Name)

	return nil
}
