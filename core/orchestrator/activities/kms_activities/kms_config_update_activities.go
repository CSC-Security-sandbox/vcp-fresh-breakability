package kms_activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

func UpdateKmsConfig(se database.Storage, ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.UpdateKmsConfigParams) error {
	logger := util.GetLogger(ctx)
	updateFields := make(map[string]interface{})

	if params.ResourceID != "" {
		updateFields["resource_id"] = params.ResourceID
	}
	if params.Description != nil {
		updateFields["description"] = *params.Description
	}
	if params.KeyName != "" {
		updateFields["key_name"] = params.KeyName
		updateFields["key_ring"] = params.KeyRing
		updateFields["key_ring_location"] = params.KeyRingLocation
		updateFields["key_project_id"] = params.KeyProjectID
		updateFields["state"] = models.LifeCycleStateCreated
	}
	err := se.UpdateKmsConfig(ctx, kmsConfig.UUID, updateFields)
	if err != nil {
		return err
	}
	logger.Debug("KmsConfig:%s update successfully in the db", kmsConfig.Name)

	return nil
}

func (a *KmsConfigActivity) UpdateKmsConfigState(ctx context.Context, kmsConfig *datamodel.KmsConfig, state, stateDetails string) error {
	activity.RecordHeartbeat(ctx, "Starting UpdateKmsConfigState activity")
	defer activity.RecordHeartbeat(ctx, "Finished UpdateKmsConfigState activity")
	logger := util.GetLogger(ctx)
	se := a.SE

	activity.RecordHeartbeat(ctx, "Updating KMS configuration state to %s", state)
	_, err := se.UpdateKmsConfigState(ctx, kmsConfig.UUID, state, stateDetails)
	if err != nil {
		return err
	}
	logger.Debug("KmsConfig state:%s update successfully in the db", kmsConfig.Name)

	return nil
}
