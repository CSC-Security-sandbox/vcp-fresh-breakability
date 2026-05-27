package replicationActivities

import (
	"context"
	"fmt"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type StopVolumeReplicationActivity struct {
	SE database.Storage
}

func (a *StopVolumeReplicationActivity) GetSrcBasePathStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	if result.Event.ReplicationModel.ReplicationAttributes.SourceLocation == RemoteRegionCustomer {
		return result, nil
	}
	srcBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

func (a *StopVolumeReplicationActivity) GetDstBasePathStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	if result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation == RemoteRegionCustomer {
		return result, nil
	}
	dstBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

func (a *StopVolumeReplicationActivity) GetSignedSrcTokenStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	srcJwt, err := GetSignedToken(ctx, *result.SrcProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.SrcJwtToken = srcJwt
	return result, nil
}

func (a *StopVolumeReplicationActivity) GetSignedDstTokenStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	if *result.SrcProjectNumber == *result.DstProjectNumber {
		result.DstJwtToken = result.SrcJwtToken
		return result, nil
	}
	dstJwt, err := GetSignedToken(ctx, *result.DstProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.DstJwtToken = dstJwt
	return result, nil
}

func (a *StopVolumeReplicationActivity) StopReplicationOnDestination(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("stopReplicationOnDestination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.CorrelationID),
	}
	stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
		Force: googleproxyclient.NewOptBool(result.Event.ForceStop),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopReplication, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		result.DstReplication = r
		result.JobId = &r.Jobs[0].JobId.Value
		return result, nil
	case *googleproxyclient.V1betaInternalStopVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalStopVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalStopVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalStopVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalStopVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New("unknown response type"))
	}
}

func (a *StopVolumeReplicationActivity) DescribeDestJobStop(ctx context.Context, result *replication.StopReplicationResult) error {
	err := activities.DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation, result.Event.XCorrelationID)
	if err != nil {
		return err
	}
	return nil
}

func (a *StopVolumeReplicationActivity) HandleHybridReplicationStopWhenGcnvIsSrc(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("HandleHybridReplicationWhenGcnvIsSrc")

	// Query the replication from database
	dbReplication, err := a.SE.GetVolumeReplication(ctx, result.Event.ReplicationModel.UUID)
	if err != nil {
		logger.Errorf("Failed to get replication from database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
	}

	// Ensure HybridReplicationAttributes exists
	if dbReplication.HybridReplicationAttributes == nil {
		dbReplication.HybridReplicationAttributes = &datamodel.HybridReplicationAttribute{}
	}

	// Generate commands for stopping replication
	if dbReplication.ReplicationAttributes != nil {
		extOntapPath := getPath(dbReplication.ReplicationAttributes.DestinationSvmName, dbReplication.ReplicationAttributes.DestinationVolumeName)
		gcnvPath := getPath(dbReplication.ReplicationAttributes.SourceSvmName, dbReplication.ReplicationAttributes.SourceVolumeName)
		commands := []string{
			"# Please run the following command once on your ONTAP system.",
			fmt.Sprintf("snapmirror break -source-path %s -destination-path %s", gcnvPath, extOntapPath),
			"# If ran successfully, MirrorState will say Broken-Off. Please check by running:",
			fmt.Sprintf("snapmirror show -source-path %s -destination-path %s", gcnvPath, extOntapPath),
		}
		dbReplication.HybridReplicationAttributes.HybridReplicationUserCommands = commands
	}

	// Update the replication in database
	err = a.SE.UpdateVolumeReplication(ctx, dbReplication)
	if err != nil {
		logger.Errorf("Failed to update replication in database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err)
	}

	logger.Infof("Successfully updated HybridReplicationUserCommands for replication %s", dbReplication.UUID)
	return result, nil
}

// getPath returns the path of an ONTAP snapmirror relationship in a <svm_name>:<volume_name> format
func getPath(svmName, volumeName string) string {
	return fmt.Sprintf("%s:%s", svmName, volumeName)
}

func (a *StopVolumeReplicationActivity) SetHybridReplicationVariablesStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	logger := util.GetLogger(ctx)
	if result.DbVolReplication != nil && result.DbVolReplication.HybridReplicationAttributes != nil {
		logger.Infof("Replication is a hybrid replication")
		result.IsHybridReplicationVolume = true
	}
	if replication.IsSrcForHybridReplication(result.DbVolReplication) {
		result.IsSrcForHybridReplication = true
	}
	return result, nil
}
