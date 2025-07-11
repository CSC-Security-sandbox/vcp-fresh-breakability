package vlm

import (
	"context"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	vlmClient "netapp.com/vsa/lifecycle-manager/pkg/vlmclient"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

type ClientFactory interface {
	VSAClusterDeployUpdate(ctx context.Context, credentials vlmconfig.OntapCredentials, currentVlmConfig *vlmconfig.VLMConfig, newVlmConfig *vlmconfig.VLMConfig) (*vlmconfig.VLMConfig, error)
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

func (c *Client) VSAClusterDeployUpdate(ctx context.Context, credentials vlmconfig.OntapCredentials, currentVlmConfig *vlmconfig.VLMConfig, newVlmConfig *vlmconfig.VLMConfig) (*vlmconfig.VLMConfig, error) {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrVLMClientInitializationError, nil)
	}

	updateVSAClusterDeploymentRequest := vlmconfig.UpdateVSAClusterDeploymentRequest{
		VLMConfig:        currentVlmConfig,
		OntapCredentials: credentials,
		SPConfig:         newVlmConfig.Deployment.SPConfig,
		NumHAPair:        newVlmConfig.Deployment.NumHAPair,
	}
	updateVSAClusterDeploymentResponse, err := c.vlmClient.UpdateVSAClusterDeployment(ctx, updateVSAClusterDeploymentRequest)
	if err != nil {
		c.traceLog.Errorf("Error updating VSA cluster deployment: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterUpdateError, err)
	}
	return updateVSAClusterDeploymentResponse.VLMConfig, nil
}
