package api

import (
	"context"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func (h Handler) V1betaCreateReplication(ctx context.Context, req *gcpgenserver.ReplicationCreateV1beta, params gcpgenserver.V1betaCreateReplicationParams) (gcpgenserver.V1betaCreateReplicationRes, error) {
	// TODO implement me
	return &gcpgenserver.OperationV1beta{}, nil
}
