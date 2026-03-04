package api

import (
	"context"
	"strings"
	"time"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

// V1GetMultipleReplicationsByExternalUUID handles requests to get multiple replications by external UUID
func (h Handler) V1GetMultipleReplicationsByExternalUUID(ctx context.Context, params oasgenserver.V1GetMultipleReplicationsByExternalUUIDParams) (oasgenserver.V1GetMultipleReplicationsByExternalUUIDRes, error) {
	// Parse the comma-separated external	 UUIDs
	uuidStrings := strings.Split(params.ExternalUuids, ",")
	externalUUIDs := make([]string, len(uuidStrings))
	for i, uuid := range uuidStrings {
		externalUUIDs[i] = strings.TrimSpace(uuid)
	}

	// Convert boolean IncludeSourceEndpoints to string EndpointType for orchestrator
	var endpointType string
	if params.IncludeSourceEndpoints.IsSet() && params.IncludeSourceEndpoints.Value {
		endpointType = "src"
	} else {
		endpointType = "dst" // default behavior
	}

	// Create orchestrator params
	orchParams := commonparams.GetMultipleReplicationsByExternalUUIDParams{
		ExternalUUIDs: externalUUIDs,
		EndpointType:  endpointType,
	} // Call orchestrator function
	replications, err := h.Orchestrator.GetMultipleReplicationsByExternalUUID(ctx, orchParams)
	if err != nil {
		return nil, err
	}

	// Convert common types to core-api types
	var coreReplications []oasgenserver.ReplicationV1
	for _, replication := range replications {
		coreReplication := convertCommonReplicationV1betaToCoreReplication(replication)
		coreReplications = append(coreReplications, coreReplication)
	}

	// Return the response
	return &oasgenserver.V1GetMultipleReplicationsByExternalUUIDOK{
		Replications: coreReplications,
	}, nil
}

// convertCommonReplicationV1betaToCoreReplication converts a commonparams.ReplicationV1beta to oasgenserver.ReplicationV1
func convertCommonReplicationV1betaToCoreReplication(commonReplication commonparams.ReplicationV1beta) oasgenserver.ReplicationV1 {
	getString := func(s *string) string {
		if s != nil {
			return *s
		}
		return ""
	}
	getTime := func(t *time.Time) time.Time {
		if t != nil {
			return *t
		}
		return time.Time{}
	}
	getState := func(s *string) oasgenserver.ReplicationV1State {
		if s != nil {
			return mapGcpStateToCore(*s)
		}
		return oasgenserver.ReplicationV1StateSTATEUNSPECIFIED
	}

	return oasgenserver.ReplicationV1{
		ReplicationId: oasgenserver.OptString{
			Value: getString(commonReplication.ReplicationId),
			Set:   commonReplication.ReplicationId != nil,
		},
		ResourceId: oasgenserver.OptString{
			Value: getString(commonReplication.ResourceId),
			Set:   commonReplication.ResourceId != nil,
		},
		Description: oasgenserver.OptNilString{
			Value: getString(commonReplication.Description),
			Set:   commonReplication.Description != nil,
		},
		State: oasgenserver.OptReplicationV1State{
			Value: getState(commonReplication.State),
			Set:   commonReplication.State != nil,
		},
		StateDetails: oasgenserver.OptString{
			Value: getString(commonReplication.StateDetails),
			Set:   commonReplication.StateDetails != nil,
		},
		Created: oasgenserver.OptDateTime{
			Value: getTime(commonReplication.Created),
			Set:   commonReplication.Created != nil,
		},
	}
}

// convertGcpGenServerToCoreReplication converts a gcpgenserver.ReplicationV1beta to oasgenserver.ReplicationV1
func convertGcpGenServerToCoreReplication(gcpReplication gcpgenserver.ReplicationV1beta) oasgenserver.ReplicationV1 {
	return oasgenserver.ReplicationV1{
		ReplicationId: oasgenserver.OptString{
			Value: gcpReplication.GetReplicationId().Or(""),
			Set:   gcpReplication.GetReplicationId().IsSet(),
		},
		ResourceId: oasgenserver.OptString{
			Value: gcpReplication.GetResourceId().Or(""),
			Set:   gcpReplication.GetResourceId().IsSet(),
		},
		Description: oasgenserver.OptNilString{
			Value: gcpReplication.GetDescription().Or(""),
			Set:   gcpReplication.GetDescription().IsSet(),
		},
		State: oasgenserver.OptReplicationV1State{
			Value: mapGcpStateEnumToCore(gcpReplication.GetState().Or(gcpgenserver.ReplicationV1betaStateSTATEUNSPECIFIED)),
			Set:   gcpReplication.GetState().IsSet(),
		},
		StateDetails: oasgenserver.OptString{
			Value: gcpReplication.GetStateDetails().Or(""),
			Set:   gcpReplication.GetStateDetails().IsSet(),
		},
		Created: oasgenserver.OptDateTime{
			Value: gcpReplication.GetCreated().Or(time.Time{}),
			Set:   gcpReplication.GetCreated().IsSet(),
		},
	}
}

// mapGcpStateToCore maps GCP state strings to core API state
func mapGcpStateToCore(gcpState string) oasgenserver.ReplicationV1State {
	switch gcpState {
	case "STATE_UNSPECIFIED":
		return oasgenserver.ReplicationV1StateSTATEUNSPECIFIED
	case "CREATING":
		return oasgenserver.ReplicationV1StateCREATING
	case "READY":
		return oasgenserver.ReplicationV1StateREADY
	case "UPDATING":
		return oasgenserver.ReplicationV1StateUPDATING
	case "DELETING":
		return oasgenserver.ReplicationV1StateDELETING
	case "ERROR":
		return oasgenserver.ReplicationV1StateERROR
	default:
		return oasgenserver.ReplicationV1StateSTATEUNSPECIFIED
	}
}

// mapGcpStateEnumToCore maps GCP state enum to core API state
func mapGcpStateEnumToCore(gcpState gcpgenserver.ReplicationV1betaState) oasgenserver.ReplicationV1State {
	switch gcpState {
	case gcpgenserver.ReplicationV1betaStateSTATEUNSPECIFIED:
		return oasgenserver.ReplicationV1StateSTATEUNSPECIFIED
	case gcpgenserver.ReplicationV1betaStateCREATING:
		return oasgenserver.ReplicationV1StateCREATING
	case gcpgenserver.ReplicationV1betaStateREADY:
		return oasgenserver.ReplicationV1StateREADY
	case gcpgenserver.ReplicationV1betaStateUPDATING:
		return oasgenserver.ReplicationV1StateUPDATING
	case gcpgenserver.ReplicationV1betaStateDELETING:
		return oasgenserver.ReplicationV1StateDELETING
	case gcpgenserver.ReplicationV1betaStateERROR:
		return oasgenserver.ReplicationV1StateERROR
	default:
		return oasgenserver.ReplicationV1StateSTATEUNSPECIFIED
	}
}
