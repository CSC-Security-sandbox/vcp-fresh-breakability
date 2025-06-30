package kms_activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
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
	updateFields := make(map[string]interface{})

	if params.KeyName != "" {
		updateFields["key_name"] = params.KeyName
	}
	if params.KeyRing != "" {
		updateFields["key_ring"] = params.KeyRing
	}
	if params.KeyRingLocation != "" {
		updateFields["key_ring_location"] = params.KeyRingLocation
	}
	if params.KeyProjectID != "" {
		updateFields["key_project_id"] = params.KeyProjectID
	}
	if params.Name != "" {
		updateFields["name"] = params.Name
	}
	if !nillable.IsNilOrEmpty(params.Description) {
		updateFields["description"] = *params.Description
	}

	updateFields["state"] = models.LifeCycleStateREADY
	updateFields["state_details"] = models.LifeCycleStateAvailableDetails
	err := se.UpdateKmsConfig(ctx, kmsConfig.UUID, updateFields)
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
