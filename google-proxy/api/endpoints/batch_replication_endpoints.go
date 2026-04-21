package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	cvpBatch "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/batch"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var fetchBatchReplicationsFromCVPFn = fetchBatchReplicationsFromCVP

func (h Handler) V1betaBatchListReplications(ctx context.Context, req *gcpgenserver.ReplicationURIListV1beta, params gcpgenserver.V1betaBatchListReplicationsParams) (gcpgenserver.V1betaBatchListReplicationsRes, error) {
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaBatchListReplicationsBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	if req == nil {
		return &gcpgenserver.V1betaBatchListReplicationsBadRequest{
			Code:    float64(http.StatusBadRequest),
			Message: "replicationUris is required and must have at least 1 item",
		}, nil
	}
	replicationURIs := req.GetReplicationUris()
	if len(replicationURIs) == 0 {
		return &gcpgenserver.V1betaBatchListReplicationsBadRequest{
			Code:    float64(http.StatusBadRequest),
			Message: "replicationUris is required and must have at least 1 item",
		}, nil
	}
	if len(replicationURIs) > env.MaxBatchReplicationURIs {
		msg := fmt.Sprintf("replicationUris in body should have at most %d items", env.MaxBatchReplicationURIs)
		code := float64(http.StatusBadRequest)
		return &gcpgenserver.V1betaBatchListReplicationsBadRequest{
			Code:    code,
			Message: msg,
		}, nil
	}
	if err := validateBatchReplicationURIList(replicationURIs, params.LocationId); err != nil {
		return &gcpgenserver.V1betaBatchListReplicationsBadRequest{
			Code:    float64(http.StatusBadRequest),
			Message: err.Error(),
		}, nil
	}

	fieldSet := buildReplicationFieldSet(params.Fields)
	if cvp.CVP_HOST == "" {
		return h.batchListReplicationsVCPOnly(ctx, params, replicationURIs, fieldSet)
	}
	return h.batchListReplicationsParallel(ctx, params, replicationURIs, fieldSet)
}

func (h Handler) batchListReplicationsParallel(ctx context.Context, params gcpgenserver.V1betaBatchListReplicationsParams, replicationURIs []string, fieldSet map[string]bool) (gcpgenserver.V1betaBatchListReplicationsRes, error) {
	logger := util.GetLogger(ctx)

	var (
		vcpReplications []gcpgenserver.BatchReplicationV1beta
		vcpErr          error
		cvpReplications []gcpgenserver.BatchReplicationV1beta
		cvpErr          error
		wg              sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		vcpReplications, vcpErr = h.getBatchReplicationsFromVCP(ctx, params, replicationURIs, fieldSet)
	}()
	go func() {
		defer wg.Done()
		cvpReplications, cvpErr = fetchBatchReplicationsFromCVPFn(ctx, params, replicationURIs, fieldSet)
	}()
	wg.Wait()

	if vcpErr != nil && cvpErr != nil {
		logger.Error("Both VCP and CVP batch replication queries failed", "vcpError", vcpErr.Error(), "cvpError", cvpErr.Error())
		return &gcpgenserver.V1betaBatchListReplicationsInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error while getting replications from both systems",
		}, nil
	}
	if vcpErr != nil {
		logger.Warn("VCP batch replication query failed, returning CVP results only", "error", vcpErr.Error())
	}
	if cvpErr != nil {
		logger.Warn("CVP batch replication query failed, returning VCP results only", "error", cvpErr.Error())
	}

	allReplications := append(vcpReplications, cvpReplications...)
	return &gcpgenserver.V1betaBatchListReplicationsOK{Replications: allReplications}, nil
}

func (h Handler) batchListReplicationsVCPOnly(ctx context.Context, params gcpgenserver.V1betaBatchListReplicationsParams, replicationURIs []string, fieldSet map[string]bool) (gcpgenserver.V1betaBatchListReplicationsRes, error) {
	replications, err := h.getBatchReplicationsFromVCP(ctx, params, replicationURIs, fieldSet)
	if err != nil {
		return &gcpgenserver.V1betaBatchListReplicationsInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error while getting replications",
		}, nil
	}
	return &gcpgenserver.V1betaBatchListReplicationsOK{Replications: replications}, nil
}

func (h Handler) getBatchReplicationsFromVCP(ctx context.Context, params gcpgenserver.V1betaBatchListReplicationsParams, replicationURIs []string, fieldSet map[string]bool) ([]gcpgenserver.BatchReplicationV1beta, error) {
	getReplicationParams := commonparams.GetMultipleReplicationsParams{
		ReplicationURIs: replicationURIs,
		LocationId:      params.LocationId,
	}
	if params.XCorrelationID.IsSet() {
		getReplicationParams.XCorrelationID = params.XCorrelationID.Value
	}

	vcpReplications, err := h.Orchestrator.GetBatchReplications(ctx, getReplicationParams)
	if err != nil {
		return nil, err
	}
	results := make([]gcpgenserver.BatchReplicationV1beta, 0, len(vcpReplications))
	for _, repl := range vcpReplications {
		results = append(results, convertCommonToBatchReplication(repl, fieldSet))
	}
	return results, nil
}

func fetchBatchReplicationsFromCVP(ctx context.Context, params gcpgenserver.V1betaBatchListReplicationsParams, replicationURIs []string, fieldSet map[string]bool) ([]gcpgenserver.BatchReplicationV1beta, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	cvpParams := cvpBatch.NewV1betaBatchListReplicationsParamsWithContext(ctx)
	applyBatchCvpListCommonParams(cvpParams, params.LocationId, batchListFieldStrings(params.Fields), params.XCorrelationID)
	cvpParams.SetBody(&cvpmodels.ReplicationURIListV1beta{ReplicationUris: replicationURIs})

	cvpResponse, err := cvpClient.Batch.V1betaBatchListReplications(cvpParams)
	if err != nil {
		return nil, fmt.Errorf("CVP batch list replications failed: %w", err)
	}

	results := make([]gcpgenserver.BatchReplicationV1beta, 0)
	if cvpResponse == nil || cvpResponse.Payload == nil {
		return results, nil
	}
	repls := cvpResponse.Payload.Replications
	for _, cvpReplication := range repls {
		if cvpReplication != nil {
			results = append(results, convertCVPBatchReplicationToGCPBatchReplication(cvpReplication, fieldSet))
		}
	}
	return results, nil
}

func buildReplicationFieldSet(fields []gcpgenserver.V1betaBatchListReplicationsFieldsItem) map[string]bool {
	if len(fields) == 0 {
		return nil
	}
	set := make(map[string]bool, len(fields))
	for _, f := range fields {
		set[string(f)] = true
	}
	return set
}

func validateBatchReplicationURIList(replicationURIs []string, locationID string) error {
	for _, uri := range replicationURIs {
		m, err := utils.CFFEURIToMap(uri)
		if err != nil {
			return err
		}
		if m["locations"] != locationID {
			return fmt.Errorf("replicationURIs locationId in body does not match locationId in parameter")
		}
	}
	return nil
}

func convertCommonToBatchReplication(repl commonparams.ReplicationV1beta, fieldSet map[string]bool) gcpgenserver.BatchReplicationV1beta {
	b := gcpgenserver.BatchReplicationV1beta{}
	if repl.ReplicationId != nil {
		b.ReplicationId = gcpgenserver.NewOptNilString(*repl.ReplicationId)
	}
	if fieldSet == nil {
		return b
	}
	if fieldSet["created"] && repl.Created != nil {
		b.Created = gcpgenserver.NewOptNilDateTime(*repl.Created)
	}
	if fieldSet["resourceId"] && repl.ResourceId != nil {
		b.ResourceId = gcpgenserver.NewOptNilString(*repl.ResourceId)
	}
	if fieldSet["state"] && repl.State != nil {
		b.State = gcpgenserver.NewOptBatchReplicationV1betaState(gcpgenserver.BatchReplicationV1betaState(*repl.State))
	}
	if fieldSet["stateDetails"] && repl.StateDetails != nil {
		b.StateDetails = gcpgenserver.NewOptNilString(*repl.StateDetails)
	}
	if fieldSet["stateDetailsCode"] && repl.StateDetailsCode != nil {
		b.StateDetailsCode = gcpgenserver.NewOptNilInt32(*repl.StateDetailsCode)
	}
	if fieldSet["role"] && repl.Role != nil {
		b.Role = gcpgenserver.NewOptBatchReplicationV1betaRole(gcpgenserver.BatchReplicationV1betaRole(*repl.Role))
	}
	if fieldSet["replicationSchedule"] && repl.ReplicationSchedule != nil {
		b.ReplicationSchedule = gcpgenserver.NewOptBatchReplicationV1betaReplicationSchedule(gcpgenserver.BatchReplicationV1betaReplicationSchedule(*repl.ReplicationSchedule))
	}
	if fieldSet["mirrorState"] && repl.MirrorState != nil {
		b.MirrorState = gcpgenserver.NewOptBatchReplicationV1betaMirrorState(gcpgenserver.BatchReplicationV1betaMirrorState(*repl.MirrorState))
	}
	if fieldSet["description"] && repl.Description != nil {
		b.Description = gcpgenserver.NewOptNilString(*repl.Description)
	}
	if fieldSet["labels"] && repl.Labels != nil {
		b.Labels = gcpgenserver.NewOptNilBatchReplicationV1betaLabels(gcpgenserver.BatchReplicationV1betaLabels(repl.Labels))
	}
	if fieldSet["source"] && repl.Source != nil {
		source := gcpgenserver.BatchReplicationV1betaSource{}
		if repl.Source.VolumeName != nil {
			source.VolumeName = gcpgenserver.NewOptString(*repl.Source.VolumeName)
		}
		if repl.Source.VolumeId != nil {
			source.VolumeId = gcpgenserver.NewOptString(*repl.Source.VolumeId)
		}
		b.Source = gcpgenserver.NewOptNilBatchReplicationV1betaSource(source)
	}
	if fieldSet["destination"] && repl.Destination != nil {
		destination := gcpgenserver.BatchReplicationV1betaDestination{}
		if repl.Destination.VolumeName != nil {
			destination.VolumeName = gcpgenserver.NewOptString(*repl.Destination.VolumeName)
		}
		if repl.Destination.VolumeId != nil {
			destination.VolumeId = gcpgenserver.NewOptString(*repl.Destination.VolumeId)
		}
		b.Destination = gcpgenserver.NewOptNilBatchReplicationV1betaDestination(destination)
	}
	if fieldSet["healthy"] && repl.Healthy != nil {
		b.Healthy = gcpgenserver.NewOptNilBool(*repl.Healthy)
	}
	if fieldSet["transferStats"] && repl.TransferStats != nil {
		stats := gcpgenserver.BatchReplicationV1betaTransferStats{}
		if repl.TransferStats.TotalTransferBytes != nil {
			stats.TotalTransferBytes = gcpgenserver.NewOptFloat64(*repl.TransferStats.TotalTransferBytes)
		}
		if repl.TransferStats.TotalTransferTimeSecs != nil {
			stats.TotalTransferTimeSecs = gcpgenserver.NewOptFloat64(*repl.TransferStats.TotalTransferTimeSecs)
		}
		if repl.TransferStats.LastTransferSize != nil {
			stats.LastTransferSize = gcpgenserver.NewOptFloat64(*repl.TransferStats.LastTransferSize)
		}
		if repl.TransferStats.LastTransferError != nil {
			stats.LastTransferError = gcpgenserver.NewOptString(*repl.TransferStats.LastTransferError)
		}
		if repl.TransferStats.LastTransferDuration != nil {
			stats.LastTransferDuration = gcpgenserver.NewOptFloat64(*repl.TransferStats.LastTransferDuration)
		}
		if repl.TransferStats.LastTransferEndTime != nil {
			stats.LastTransferEndTime = gcpgenserver.NewOptDateTime(*repl.TransferStats.LastTransferEndTime)
		}
		if repl.TransferStats.TotalProgress != nil {
			stats.TotalProgress = gcpgenserver.NewOptFloat64(*repl.TransferStats.TotalProgress)
		}
		if repl.TransferStats.ProgressLastUpdated != nil {
			stats.ProgressLastUpdated = gcpgenserver.NewOptDateTime(*repl.TransferStats.ProgressLastUpdated)
		}
		if repl.TransferStats.LagTime != nil {
			stats.LagTime = gcpgenserver.NewOptFloat64(*repl.TransferStats.LagTime)
		}
		b.TransferStats = gcpgenserver.NewOptNilBatchReplicationV1betaTransferStats(stats)
	}
	if fieldSet["destinationVolumeParameters"] && repl.DestinationVolumeParameters != nil {
		params := gcpgenserver.DestinationVolumeParametersV1beta{
			StoragePool: repl.DestinationVolumeParameters.StoragePool,
		}
		if repl.DestinationVolumeParameters.VolumeId != nil {
			params.VolumeId = gcpgenserver.NewOptString(*repl.DestinationVolumeParameters.VolumeId)
		}
		if repl.DestinationVolumeParameters.ShareName != nil {
			params.ShareName = gcpgenserver.NewOptString(*repl.DestinationVolumeParameters.ShareName)
		}
		if repl.DestinationVolumeParameters.Description != nil {
			params.Description = gcpgenserver.NewOptString(*repl.DestinationVolumeParameters.Description)
		}
		if repl.DestinationVolumeParameters.TieringPolicy != nil {
			t := gcpgenserver.TieringPolicyV1beta{}
			if repl.DestinationVolumeParameters.TieringPolicy.TierAction != nil {
				t.TierAction = gcpgenserver.NewOptNilTieringPolicyV1betaTierAction(gcpgenserver.TieringPolicyV1betaTierAction(*repl.DestinationVolumeParameters.TieringPolicy.TierAction))
			}
			if repl.DestinationVolumeParameters.TieringPolicy.CoolingThresholdDays != nil {
				t.CoolingThresholdDays = gcpgenserver.NewOptNilInt32(*repl.DestinationVolumeParameters.TieringPolicy.CoolingThresholdDays)
			}
			if repl.DestinationVolumeParameters.TieringPolicy.HotTierBypassModeEnabled != nil {
				t.HotTierBypassModeEnabled = gcpgenserver.NewOptNilBool(*repl.DestinationVolumeParameters.TieringPolicy.HotTierBypassModeEnabled)
			}
			params.TieringPolicy = gcpgenserver.NewOptTieringPolicyV1beta(t)
		}
		if repl.DestinationVolumeParameters.ThroughputMibps != nil {
			params.ThroughputMibps = gcpgenserver.NewOptNilFloat64(*repl.DestinationVolumeParameters.ThroughputMibps)
		}
		if repl.DestinationVolumeParameters.Iops != nil {
			params.Iops = gcpgenserver.NewOptNilInt64(*repl.DestinationVolumeParameters.Iops)
		}
		if repl.DestinationVolumeParameters.VolumePerformanceGroupId != nil {
			params.VolumePerformanceGroupId = gcpgenserver.NewOptNilString(*repl.DestinationVolumeParameters.VolumePerformanceGroupId)
		}
		b.DestinationVolumeParameters = gcpgenserver.NewOptNilBatchReplicationV1betaDestinationVolumeParameters(
			gcpgenserver.BatchReplicationV1betaDestinationVolumeParameters(params))
	} else if fieldSet["destinationVolumeParameters"] {
		// Only include destinationVolumeParameters when we can derive a schema-valid storagePool URI.
		derivedStoragePool := deriveStoragePoolURIFromReplication(repl)
		if derivedStoragePool != "" {
			derived := gcpgenserver.DestinationVolumeParametersV1beta{
				StoragePool: derivedStoragePool,
			}
			if repl.Destination != nil && repl.Destination.VolumeName != nil {
				_, _, volumeName := parseVolumeURI(*repl.Destination.VolumeName)
				if volumeName != "" {
					derived.VolumeId = gcpgenserver.NewOptString(volumeName)
				}
			}
			if repl.Description != nil {
				derived.Description = gcpgenserver.NewOptString(*repl.Description)
			}
			b.DestinationVolumeParameters = gcpgenserver.NewOptNilBatchReplicationV1betaDestinationVolumeParameters(
				gcpgenserver.BatchReplicationV1betaDestinationVolumeParameters(derived))
		}
	}
	if fieldSet["clusterLocation"] && repl.ClusterLocation != nil {
		b.ClusterLocation = gcpgenserver.NewOptNilString(*repl.ClusterLocation)
	}
	if fieldSet["hybridReplicationType"] && repl.HybridReplicationType != nil {
		b.HybridReplicationType = gcpgenserver.NewOptBatchReplicationV1betaHybridReplicationType(gcpgenserver.BatchReplicationV1betaHybridReplicationType(*repl.HybridReplicationType))
	}
	if fieldSet["hybridPeeringDetails"] && repl.HybridPeeringDetails != nil {
		hybrid := gcpgenserver.HybridPeeringV1beta{}
		if repl.HybridPeeringDetails.SubnetIp != nil {
			hybrid.SubnetIp = gcpgenserver.NewOptString(*repl.HybridPeeringDetails.SubnetIp)
		}
		if repl.HybridPeeringDetails.Command != nil {
			hybrid.Command = gcpgenserver.NewOptString(*repl.HybridPeeringDetails.Command)
		}
		if repl.HybridPeeringDetails.Passphrase != nil {
			hybrid.Passphrase = gcpgenserver.NewOptString(*repl.HybridPeeringDetails.Passphrase)
		}
		if repl.HybridPeeringDetails.CommandExpiryTime != nil {
			hybrid.CommandExpiryTime = gcpgenserver.NewOptDateTime(*repl.HybridPeeringDetails.CommandExpiryTime)
		}
		if repl.HybridPeeringDetails.PeerVolumeName != nil {
			hybrid.PeerVolumeName = gcpgenserver.NewOptString(*repl.HybridPeeringDetails.PeerVolumeName)
		}
		if repl.HybridPeeringDetails.PeerClusterName != nil {
			hybrid.PeerClusterName = gcpgenserver.NewOptString(*repl.HybridPeeringDetails.PeerClusterName)
		}
		if repl.HybridPeeringDetails.PeerSvmName != nil {
			hybrid.PeerSvmName = gcpgenserver.NewOptString(*repl.HybridPeeringDetails.PeerSvmName)
		}
		b.HybridPeeringDetails = gcpgenserver.NewOptNilBatchReplicationV1betaHybridPeeringDetails(
			gcpgenserver.BatchReplicationV1betaHybridPeeringDetails(hybrid))
	}
	if fieldSet["hybridReplicationUserCommands"] && repl.HybridReplicationUserCommands != nil {
		b.HybridReplicationUserCommands = gcpgenserver.NewOptNilBatchReplicationV1betaHybridReplicationUserCommands(
			gcpgenserver.BatchReplicationV1betaHybridReplicationUserCommands(gcpgenserver.HybridReplicationUserCommandsV1beta{
				Commands: repl.HybridReplicationUserCommands.Commands,
			}))
	}
	ensureBatchReplicationRequestedFields(&b, fieldSet)
	return b
}

func convertCVPTransferStatsToGCP(ts *cvpmodels.TransferStatsV1beta) gcpgenserver.BatchReplicationV1betaTransferStats {
	stats := gcpgenserver.BatchReplicationV1betaTransferStats{}
	stats.TotalTransferBytes = gcpgenserver.NewOptFloat64(ts.TotalTransferBytes)
	stats.TotalTransferTimeSecs = gcpgenserver.NewOptFloat64(ts.TotalTransferTimeSecs)
	stats.LastTransferSize = gcpgenserver.NewOptFloat64(ts.LastTransferSize)
	if ts.LastTransferError != "" {
		stats.LastTransferError = gcpgenserver.NewOptString(ts.LastTransferError)
	}
	stats.LastTransferDuration = gcpgenserver.NewOptFloat64(ts.LastTransferDuration)
	if ts.LastTransferEndTime != nil {
		stats.LastTransferEndTime = gcpgenserver.NewOptDateTime(time.Time(*ts.LastTransferEndTime))
	}
	stats.TotalProgress = gcpgenserver.NewOptFloat64(ts.TotalProgress)
	if ts.ProgressLastUpdated != nil {
		stats.ProgressLastUpdated = gcpgenserver.NewOptDateTime(time.Time(*ts.ProgressLastUpdated))
	}
	stats.LagTime = gcpgenserver.NewOptFloat64(ts.LagTime)
	return stats
}

func convertCVPDestinationVolumeParametersToGCP(cvp *cvpmodels.DestinationVolumeParametersV1beta) gcpgenserver.BatchReplicationV1betaDestinationVolumeParameters {
	params := gcpgenserver.DestinationVolumeParametersV1beta{}
	if cvp.StoragePool != nil {
		params.StoragePool = *cvp.StoragePool
	}
	if cvp.VolumeID != "" {
		params.VolumeId = gcpgenserver.NewOptString(cvp.VolumeID)
	}
	if cvp.ShareName != "" {
		params.ShareName = gcpgenserver.NewOptString(cvp.ShareName)
	}
	if cvp.Description != nil {
		params.Description = gcpgenserver.NewOptString(*cvp.Description)
	}
	if cvp.TieringPolicy != nil {
		t := gcpgenserver.TieringPolicyV1beta{}
		if cvp.TieringPolicy.TierAction != nil {
			t.TierAction = gcpgenserver.NewOptNilTieringPolicyV1betaTierAction(gcpgenserver.TieringPolicyV1betaTierAction(*cvp.TieringPolicy.TierAction))
		}
		if cvp.TieringPolicy.CoolingThresholdDays != nil {
			t.CoolingThresholdDays = gcpgenserver.NewOptNilInt32(*cvp.TieringPolicy.CoolingThresholdDays)
		}
		if cvp.TieringPolicy.HotTierBypassModeEnabled != nil {
			t.HotTierBypassModeEnabled = gcpgenserver.NewOptNilBool(*cvp.TieringPolicy.HotTierBypassModeEnabled)
		}
		params.TieringPolicy = gcpgenserver.NewOptTieringPolicyV1beta(t)
	}
	return gcpgenserver.BatchReplicationV1betaDestinationVolumeParameters(params)
}

func convertCVPBatchReplicationToGCPBatchReplication(cvpReplication *cvpmodels.BatchReplicationV1beta, fieldSet map[string]bool) gcpgenserver.BatchReplicationV1beta {
	b := gcpgenserver.BatchReplicationV1beta{
		ReplicationId: gcpgenserver.NewOptNilString(cvpReplication.ReplicationID),
	}
	if fieldSet == nil {
		return b
	}
	if fieldSet["created"] && cvpReplication.Created != nil {
		t := time.Time(*cvpReplication.Created)
		b.Created = gcpgenserver.NewOptNilDateTime(t)
	}
	if fieldSet["resourceId"] && cvpReplication.ResourceID != nil {
		b.ResourceId = gcpgenserver.NewOptNilString(*cvpReplication.ResourceID)
	}
	if fieldSet["state"] {
		st := cvpReplication.State
		if st == "" {
			st = string(gcpgenserver.BatchReplicationV1betaStateSTATEUNSPECIFIED)
		}
		b.State = gcpgenserver.NewOptBatchReplicationV1betaState(gcpgenserver.BatchReplicationV1betaState(st))
	}
	if fieldSet["stateDetails"] && cvpReplication.StateDetails != nil {
		b.StateDetails = gcpgenserver.NewOptNilString(*cvpReplication.StateDetails)
	}
	if fieldSet["stateDetailsCode"] {
		b.StateDetailsCode = gcpgenserver.NewOptNilInt32(cvpReplication.StateDetailsCode)
	}
	if fieldSet["role"] && cvpReplication.Role != nil {
		b.Role = gcpgenserver.NewOptBatchReplicationV1betaRole(gcpgenserver.BatchReplicationV1betaRole(*cvpReplication.Role))
	}
	if fieldSet["replicationSchedule"] && cvpReplication.ReplicationSchedule != "" {
		b.ReplicationSchedule = gcpgenserver.NewOptBatchReplicationV1betaReplicationSchedule(gcpgenserver.BatchReplicationV1betaReplicationSchedule(cvpReplication.ReplicationSchedule))
	}
	if fieldSet["mirrorState"] && cvpReplication.MirrorState != "" {
		b.MirrorState = gcpgenserver.NewOptBatchReplicationV1betaMirrorState(gcpgenserver.BatchReplicationV1betaMirrorState(cvpReplication.MirrorState))
	}
	if fieldSet["description"] && cvpReplication.Description != nil {
		b.Description = gcpgenserver.NewOptNilString(*cvpReplication.Description)
	}
	if fieldSet["labels"] && cvpReplication.Labels != nil {
		b.Labels = gcpgenserver.NewOptNilBatchReplicationV1betaLabels(gcpgenserver.BatchReplicationV1betaLabels(cvpReplication.Labels))
	}
	if fieldSet["source"] && cvpReplication.Source != nil {
		source := gcpgenserver.BatchReplicationV1betaSource{}
		if cvpReplication.Source.VolumeName != "" {
			source.VolumeName = gcpgenserver.NewOptString(cvpReplication.Source.VolumeName)
		}
		if cvpReplication.Source.VolumeID != "" {
			source.VolumeId = gcpgenserver.NewOptString(cvpReplication.Source.VolumeID)
		}
		b.Source = gcpgenserver.NewOptNilBatchReplicationV1betaSource(source)
	}
	if fieldSet["destination"] && cvpReplication.Destination != nil {
		destination := gcpgenserver.BatchReplicationV1betaDestination{}
		if cvpReplication.Destination.VolumeName != "" {
			destination.VolumeName = gcpgenserver.NewOptString(cvpReplication.Destination.VolumeName)
		}
		if cvpReplication.Destination.VolumeID != "" {
			destination.VolumeId = gcpgenserver.NewOptString(cvpReplication.Destination.VolumeID)
		}
		b.Destination = gcpgenserver.NewOptNilBatchReplicationV1betaDestination(destination)
	}
	if fieldSet["healthy"] && cvpReplication.Healthy != nil {
		b.Healthy = gcpgenserver.NewOptNilBool(*cvpReplication.Healthy)
	}
	if fieldSet["transferStats"] && cvpReplication.TransferStats != nil {
		b.TransferStats = gcpgenserver.NewOptNilBatchReplicationV1betaTransferStats(convertCVPTransferStatsToGCP(cvpReplication.TransferStats))
	}
	if fieldSet["destinationVolumeParameters"] && cvpReplication.DestinationVolumeParameters != nil && cvpReplication.DestinationVolumeParameters.StoragePool != nil {
		b.DestinationVolumeParameters = gcpgenserver.NewOptNilBatchReplicationV1betaDestinationVolumeParameters(
			convertCVPDestinationVolumeParametersToGCP(cvpReplication.DestinationVolumeParameters))
	}
	if fieldSet["clusterLocation"] && cvpReplication.ClusterLocation != nil {
		b.ClusterLocation = gcpgenserver.NewOptNilString(*cvpReplication.ClusterLocation)
	}
	if fieldSet["hybridReplicationType"] && cvpReplication.HybridReplicationType != nil {
		b.HybridReplicationType = gcpgenserver.NewOptBatchReplicationV1betaHybridReplicationType(gcpgenserver.BatchReplicationV1betaHybridReplicationType(*cvpReplication.HybridReplicationType))
	}
	if fieldSet["hybridPeeringDetails"] && cvpReplication.HybridPeeringDetails != nil {
		h := cvpReplication.HybridPeeringDetails
		hybrid := gcpgenserver.HybridPeeringV1beta{}
		if h.SubnetIP != "" {
			hybrid.SubnetIp = gcpgenserver.NewOptString(h.SubnetIP)
		}
		if h.Command != "" {
			hybrid.Command = gcpgenserver.NewOptString(h.Command)
		}
		if h.Passphrase != nil {
			hybrid.Passphrase = gcpgenserver.NewOptString(*h.Passphrase)
		}
		if h.CommandExpiryTime != nil {
			hybrid.CommandExpiryTime = gcpgenserver.NewOptDateTime(time.Time(*h.CommandExpiryTime))
		}
		if h.PeerVolumeName != nil {
			hybrid.PeerVolumeName = gcpgenserver.NewOptString(*h.PeerVolumeName)
		}
		if h.PeerClusterName != nil {
			hybrid.PeerClusterName = gcpgenserver.NewOptString(*h.PeerClusterName)
		}
		if h.PeerSvmName != nil {
			hybrid.PeerSvmName = gcpgenserver.NewOptString(*h.PeerSvmName)
		}
		b.HybridPeeringDetails = gcpgenserver.NewOptNilBatchReplicationV1betaHybridPeeringDetails(
			gcpgenserver.BatchReplicationV1betaHybridPeeringDetails(hybrid))
	}
	if fieldSet["hybridReplicationUserCommands"] && cvpReplication.HybridReplicationUserCommands != nil {
		b.HybridReplicationUserCommands = gcpgenserver.NewOptNilBatchReplicationV1betaHybridReplicationUserCommands(
			gcpgenserver.BatchReplicationV1betaHybridReplicationUserCommands(gcpgenserver.HybridReplicationUserCommandsV1beta{
				Commands: cvpReplication.HybridReplicationUserCommands.Commands,
			}))
	}
	ensureBatchReplicationRequestedFields(&b, fieldSet)
	return b
}

func ensureBatchReplicationRequestedFields(b *gcpgenserver.BatchReplicationV1beta, fieldSet map[string]bool) {
	if fieldSet == nil {
		return
	}
	if fieldSet["created"] && !b.Created.Set {
		b.Created = gcpgenserver.NewOptNilDateTime(time.Time{})
	}
	if fieldSet["resourceId"] && !b.ResourceId.Set {
		b.ResourceId = gcpgenserver.NewOptNilString("")
	}
	if fieldSet["state"] && !b.State.Set {
		b.State = gcpgenserver.NewOptBatchReplicationV1betaState(gcpgenserver.BatchReplicationV1betaStateSTATEUNSPECIFIED)
	}
	if fieldSet["stateDetails"] && !b.StateDetails.Set {
		b.StateDetails = gcpgenserver.NewOptNilString("")
	}
	if fieldSet["stateDetailsCode"] && !b.StateDetailsCode.Set {
		b.StateDetailsCode = gcpgenserver.NewOptNilInt32(0)
	}
	if fieldSet["role"] && !b.Role.Set {
		b.Role = gcpgenserver.NewOptBatchReplicationV1betaRole(gcpgenserver.BatchReplicationV1betaRoleSOURCE)
	}
	if fieldSet["replicationSchedule"] && !b.ReplicationSchedule.Set {
		b.ReplicationSchedule = gcpgenserver.NewOptBatchReplicationV1betaReplicationSchedule(gcpgenserver.BatchReplicationV1betaReplicationScheduleEVERY10MINUTES)
	}
	if fieldSet["mirrorState"] && !b.MirrorState.Set {
		b.MirrorState = gcpgenserver.NewOptBatchReplicationV1betaMirrorState(gcpgenserver.BatchReplicationV1betaMirrorStateUNINITIALIZED)
	}
	if fieldSet["description"] && !b.Description.Set {
		b.Description = gcpgenserver.NewOptNilString("")
	}
	if fieldSet["labels"] && !b.Labels.Set {
		b.Labels = gcpgenserver.NewOptNilBatchReplicationV1betaLabels(gcpgenserver.BatchReplicationV1betaLabels{})
	}
	if fieldSet["source"] && !b.Source.Set {
		b.Source = gcpgenserver.NewOptNilBatchReplicationV1betaSource(gcpgenserver.BatchReplicationV1betaSource{})
	}
	if fieldSet["destination"] && !b.Destination.Set {
		b.Destination = gcpgenserver.NewOptNilBatchReplicationV1betaDestination(gcpgenserver.BatchReplicationV1betaDestination{})
	}
	if fieldSet["healthy"] && !b.Healthy.Set {
		b.Healthy = gcpgenserver.NewOptNilBool(false)
	}
	if fieldSet["transferStats"] && !b.TransferStats.Set {
		b.TransferStats = gcpgenserver.NewOptNilBatchReplicationV1betaTransferStats(gcpgenserver.BatchReplicationV1betaTransferStats{})
	}
	if fieldSet["destinationVolumeParameters"] && !b.DestinationVolumeParameters.Set {
		b.DestinationVolumeParameters = gcpgenserver.NewOptNilBatchReplicationV1betaDestinationVolumeParameters(
			gcpgenserver.BatchReplicationV1betaDestinationVolumeParameters{})
	}
	if fieldSet["clusterLocation"] && !b.ClusterLocation.Set {
		b.ClusterLocation = gcpgenserver.NewOptNilString("")
	}
	if fieldSet["hybridReplicationType"] && !b.HybridReplicationType.Set {
		b.HybridReplicationType = gcpgenserver.NewOptBatchReplicationV1betaHybridReplicationType(gcpgenserver.BatchReplicationV1betaHybridReplicationTypeHYBRIDREPLICATIONTYPEUNSPECIFIED)
	}
	if fieldSet["hybridPeeringDetails"] && !b.HybridPeeringDetails.Set {
		b.HybridPeeringDetails = gcpgenserver.NewOptNilBatchReplicationV1betaHybridPeeringDetails(gcpgenserver.BatchReplicationV1betaHybridPeeringDetails{})
	}
	if fieldSet["hybridReplicationUserCommands"] && !b.HybridReplicationUserCommands.Set {
		b.HybridReplicationUserCommands = gcpgenserver.NewOptNilBatchReplicationV1betaHybridReplicationUserCommands(
			gcpgenserver.BatchReplicationV1betaHybridReplicationUserCommands{Commands: []string{}})
	}
}

func parseVolumeURI(volumeURI string) (project string, location string, volume string) {
	parts := strings.Split(volumeURI, "/")
	// Expected: projects/{project}/locations/{location}/volumes/{volume}
	if len(parts) < 6 {
		return "", "", ""
	}
	return parts[1], parts[3], parts[5]
}

func deriveStoragePoolURIFromReplication(repl commonparams.ReplicationV1beta) string {
	if repl.Destination != nil && repl.Destination.VolumeName != nil {
		project, location, _ := parseVolumeURI(*repl.Destination.VolumeName)
		if project != "" && location != "" {
			return fmt.Sprintf("projects/%s/locations/%s/storagePools/-", project, location)
		}
	}
	return ""
}
