package activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

// VolumePerformanceGroupActivity handles VPG-related activities
type VolumePerformanceGroupActivity struct {
	SE database.Storage
}

// CreateVPGInDB creates a Volume Performance Group in the database
func (a *VolumePerformanceGroupActivity) CreateVPGInDB(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) (*datamodel.VolumePerformanceGroup, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Creating VPG in database")

	createdVPG, err := a.SE.CreateVolumePerformanceGroup(ctx, vpg)
	if err != nil {
		logger.Error("Failed to create VPG in database", "error", err, "vpg_name", vpg.Name)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("VPG created in database", "vpg_id", createdVPG.UUID, "vpg_name", createdVPG.Name)
	return createdVPG, nil
}

// CreateQoSPolicyInONTAP creates a QoS policy in ONTAP and returns the policy ID (UUID/name)
// This is called before creating the VPG in the database so the VPG can be created with the QPG UUID already set
func (a *VolumePerformanceGroupActivity) CreateQoSPolicyInONTAP(
	ctx context.Context,
	vpg *datamodel.VolumePerformanceGroup,
	pool *datamodel.PoolView,
	node *models.Node,
) (string, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Creating QoS policy in ONTAP")

	// Get SVM for the pool
	svm, err := a.SE.GetSvmForPoolID(ctx, pool.ID)
	if err != nil {
		logger.Error("Failed to get SVM for QoS policy creation", "error", err, "pool_id", pool.ID)
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Get provider for the node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Error("Failed to get provider for QoS policy creation", "error", err)
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Create the QoS policy in ONTAP
	createQosParams := vsa.CreateQoSGroupPolicyParams{
		Name:          vpg.Name,
		SvmName:       svm.Name,
		MaxThroughput: vpg.ThroughputMibps,
		MaxIOPS:       vpg.Iops,
		IsShared:      nillable.GetBoolPtr(vpg.IsShared),
	}

	activity.RecordHeartbeat(ctx, fmt.Sprintf("Creating QoS policy: %s", vpg.Name))
	qosPolicyResp, err := provider.CreateQoSGroupPolicy(createQosParams)
	if err != nil {
		logger.Error("Failed to create QoS policy in ONTAP", "error", err, "vpg_name", vpg.Name)
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("QoS policy created in ONTAP", "qos_policy", qosPolicyResp.Name, "vpg_name", vpg.Name)
	return qosPolicyResp.Name, nil
}

// DeleteQoSPolicyInONTAP deletes a QoS policy from ONTAP by policy name.
func (a *VolumePerformanceGroupActivity) DeleteQoSPolicyInONTAP(
	ctx context.Context,
	qosPolicyID string,
	pool *datamodel.PoolView,
	node *models.Node,
) error {
	logger := util.GetLogger(ctx)
	if qosPolicyID == "" {
		return nil
	}

	// Get SVM for the pool
	svm, err := a.SE.GetSvmForPoolID(ctx, pool.ID)
	if err != nil {
		logger.Error("Failed to get SVM for QoS policy deletion", "error", err, "pool_id", pool.ID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Get provider for the node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Error("Failed to get provider for QoS policy deletion", "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	deleteQosParams := vsa.DeleteQoSGroupPolicyParams{
		Name:    qosPolicyID,
		SvmName: svm.Name,
	}
	err = provider.DeleteQoSGroupPolicy(deleteQosParams)
	if err != nil {
		if utilErrors.IsNotFoundErr(err) {
			logger.Debug("QoS policy already deleted", "policy_name", qosPolicyID)
			return nil
		}
		logger.Error("Failed to delete QoS policy from ONTAP", "policy_name", qosPolicyID, "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("Deleted QoS policy from ONTAP", "policy_name", qosPolicyID)
	return nil
}
