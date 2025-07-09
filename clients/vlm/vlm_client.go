package vlm

import (
	"context"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	vlmClient "netapp.com/vsa/lifecycle-manager/pkg/vlmclient"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

type ClientFactory interface {
	VSAClusterDeployCreate(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) error
	VSASVMCreate(ctx context.Context, svmConfig *vlmconfig.SVMConfigParams) error
	VSAClusterDeploymentDelete(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) error
	VSAClusterDeployGet(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) (*vlmconfig.VLMConfig, error)
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

func (c *Client) VSAClusterDeployCreate(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) error {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return vsaerrors.NewVCPError(vsaerrors.ErrVLMClientInitializationError, nil)
	}
	err := c.vlmClient.CreateVSAClusterDeployment(ctx, vlmConfig)
	if err != nil {
		c.traceLog.Errorf("Error creating VSA cluster deployment: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterCreateError, err)
	}
	return nil
}
func (c *Client) VSAClusterDeployGet(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) (*vlmconfig.VLMConfig, error) {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrVLMClientInitializationError, nil)
	}
	vlmConfig, err := c.vlmClient.GetVSAClusterDeployment(ctx, vlmConfig)
	if err != nil {
		c.traceLog.Errorf("Error getting VSA cluster deployment: %v", err)
		return nil, err
	}
	return vlmConfig, nil
}

func (c *Client) VSAClusterDeploymentDelete(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) error {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return vsaerrors.NewVCPError(vsaerrors.ErrVLMClientInitializationError, nil)
	}
	err := c.vlmClient.DeleteVSAClusterDeployment(ctx, vlmConfig)
	if err != nil {
		c.traceLog.Errorf("Error deleting VSA cluster deployment: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterDeleteError, err)
	}
	return nil
}

func (c *Client) VSASVMCreate(ctx context.Context, svmConfig *vlmconfig.SVMConfigParams) error {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return vsaerrors.NewVCPError(vsaerrors.ErrVLMClientInitializationError, nil)
	}
	err := c.vlmClient.CreateVSASVM(ctx, *svmConfig)
	if err != nil {
		c.traceLog.Errorf("Error creating VSA SVM: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrCreatingSVM, err)
	}
	return nil
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
