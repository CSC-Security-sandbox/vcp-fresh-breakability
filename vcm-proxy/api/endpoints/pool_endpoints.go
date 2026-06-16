package api

import (
	"context"

	vcmserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcm-proxy/api/vcm-servergen"
)

// CreatePool provisions the ONTAP pool via the VCM orchestrator + VLM.
func (h Handler) CreatePool(ctx context.Context, req *vcmserver.CreatePoolRequest) (vcmserver.CreatePoolRes, error) {
	// TODO: implement CreatePool
	return &vcmserver.CreatePoolNotImplemented{
		Code:    501,
		Message: "CreatePool TODO: implementation pending method decision",
	}, nil
}

// GetPool returns the current state of a pool.
func (h Handler) GetPool(ctx context.Context, params vcmserver.GetPoolParams) (vcmserver.GetPoolRes, error) {
	// TODO: implement GetPool
	return &vcmserver.GetPoolNotImplemented{
		Code:    501,
		Message: "GetPool TODO: implementation pending method decision",
	}, nil
}
