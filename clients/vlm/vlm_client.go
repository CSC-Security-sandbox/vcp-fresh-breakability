package vlm

import (
	"context"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	vlmClient "netapp.com/vsa/lifecycle-manager/pkg/vlmclient"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

type ClientFactory interface {
	VSAClusterDeployUpdate(ctx context.Context, deploymentUpdateParams *vlmconfig.DeploymentUpdateParams) error
}

type Client struct {
	vlmClient *vlmClient.VLMClient
	traceLog  log.Logger
}

func NewClient(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) *Client {
	newVLMClient, err := vlmClient.NewVLMClient(ctx, vlmConfig)
	if err != nil {
		return nil
	}
	return &Client{
		vlmClient: newVLMClient,
		traceLog:  logger,
	}
}

func (c *Client) VSAClusterDeployUpdate(ctx context.Context, deploymentUpdateParams *vlmconfig.DeploymentUpdateParams) error {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return vsaerrors.NewVCPError(vsaerrors.ErrVLMClientInitializationError, nil)
	}
	err := c.vlmClient.UpdateVSAClusterDeployment(ctx, *deploymentUpdateParams)
	if err != nil {
		c.traceLog.Errorf("Error updating VSA cluster deployment: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterUpdateError, err)
	}
	return nil
}
