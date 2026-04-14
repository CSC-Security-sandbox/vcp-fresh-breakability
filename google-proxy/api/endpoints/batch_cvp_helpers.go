package api

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type batchCvpListParams interface {
	SetLocationID(string)
	SetFields([]string)
	SetXCorrelationID(*string)
}

// cvpClientFromContext builds a CVP API client using the logger and JWT from ctx.
func cvpClientFromContext(ctx context.Context) cvpapi.Cvp {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	return createClient(logger, jwtToken)
}

// batchListFieldStrings converts typed batch field enums to plain strings for the CVP client.
func batchListFieldStrings[T ~string](fields []T) []string {
	if len(fields) == 0 {
		return nil
	}
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, string(f))
	}
	return out
}

// applyBatchCvpListCommonParams sets location, optional fields filter, and optional correlation ID on CVP batch list params.
func applyBatchCvpListCommonParams(p batchCvpListParams, locationID string, fieldStrs []string, xCorr gcpgenserver.OptString) {
	p.SetLocationID(locationID)
	if len(fieldStrs) > 0 {
		p.SetFields(fieldStrs)
	}
	if xCorr.IsSet() {
		v := xCorr.Value
		p.SetXCorrelationID(&v)
	}
}
