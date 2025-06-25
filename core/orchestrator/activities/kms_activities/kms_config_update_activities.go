package kms_activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/sde"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	sdeUpdateSDEKmsConfiguration = sde.UpdateSDEKmsConfiguration
)

func (a *KmsConfigActivity) UpdateSDEKmsConfig(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.UpdateKmsConfigParams) error {
	logger := util.GetLogger(ctx)

	_, err := sdeUpdateSDEKmsConfiguration(ctx, kmsConfig, params)
	if err != nil {
		return err
	}
	logger.Debug("KmsConfig:%s update successfully in the sde", params.Name)

	return nil
}

func (a *KmsConfigActivity) UpdateKmsConfig(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.UpdateKmsConfigParams) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	if params.KeyName != "" {
		kmsConfig.KeyName = params.KeyName
	}
	if params.KeyRing != "" {
		kmsConfig.KeyRing = params.KeyRing
	}
	if params.KeyRingLocation != "" {
		kmsConfig.KeyRingLocation = params.KeyRingLocation
	}
	if params.KeyProjectID != "" {
		kmsConfig.KeyProjectID = params.KeyProjectID
	}
	if params.Name != "" {
		kmsConfig.Name = params.Name
	}
	if !nillable.IsNilOrEmpty(params.Description) {
		kmsConfig.Description = *params.Description
	}

	_, err := se.UpdateKmsConfigState(ctx, kmsConfig.UUID, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
	if err != nil {
		return err
	}
	logger.Debug("KmsConfig:%s update successfully in the db", kmsConfig.Name)

	return nil
}

func (a *KmsConfigActivity) UpdateKmsConfigState(ctx context.Context, kmsConfig *datamodel.KmsConfig, state, stateDetails string) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	_, err := se.UpdateKmsConfigState(ctx, kmsConfig.UUID, state, stateDetails)
	if err != nil {
		return err
	}
	logger.Debug("KmsConfig state:%s update successfully in the db", kmsConfig.Name)

	return nil
}
