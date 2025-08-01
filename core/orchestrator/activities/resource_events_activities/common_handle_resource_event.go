package resource_events_activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	createClient   = cvp.CreateClient
	getSignedToken = auth.GetSignedJwtToken
)

func pollCvpOperationForWorkflow(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*models.OperationV1beta, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Polling for operation %s", operationParams.OperationID)
	operationResponse, err := cvpClient.Async.V1betaDescribeOperation(operationParams)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDescribingSDEJob, err)
	}

	return operationResponse.Payload, nil
}
